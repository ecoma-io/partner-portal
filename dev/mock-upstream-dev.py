#!/usr/bin/env python3
"""A dev-only OpenAI-shaped upstream that can also fail on purpose.

`deploy/mock-upstream.py` answers exactly one shape: a 200 with a fixed usage
block, because `deploy/smoke-test.sh` asserts on deterministic token counts and
must never see anything else. That is the right constraint for a smoke test and
the wrong one for looking at the dashboard — a ledger where every request
succeeded shows one line of every state in the UI, and none of the other three.

So this is a *separate* file rather than a flag on the smoke fixture. The smoke
fixture is proof that the deployment path works, and its value comes from
changing nothing; teaching it to fail on request would make it a worse proof and
would couple two fixtures that have no reason to move together.

Its only job is to make every state the dashboard can render reachable from a
terminal, at zero cost:

  x-dev-fail: <status>     return that HTTP status with an OpenAI-shaped error
                           body. Recorded as `failed`, with the upstream's own
                           error message on the request row.
  x-dev-fail: close        close the socket without answering. Exercises the
                           `BAD_GATEWAY` path (handler.rs:241) — a transport
                           failure rather than a reported one.
  x-dev-fail: hang         sleep past the client's timeout. Exercises
                           `GATEWAY_TIMEOUT` (handler.rs:220). Slow on purpose:
                           only use it when testing drain, not in a load loop.
  x-dev-fail: no-usage     a clean 200 stream that never sends a usage event.
                           This is *not* a failure: it is invariant 3, "unavailable
                           is not zero" — the request succeeds, the tokens stay
                           NULL, and the dashboard's unavailable-usage card is the
                           thing under review.

Sizes and timings are drawn, not fixed. A fixture that always answers "11
prompt tokens, 7 completion tokens, four frames" makes every number in the
dashboard look correct while proving nothing: the interesting shapes are the
outliers — a request ten times the median, a cached prompt, a stream that sits
silent for ten seconds and answers in one frame. A chart of constants shows you
the chart. Every duration this fixture draws sits in the 15–26s band (see
STREAM_SECONDS for why that band and not a short-heavy tail).
`deploy/mock-upstream.py` keeps its fixed values on purpose, because
`deploy/smoke-test.sh` asserts on them; the two fixtures are separate files for
exactly this reason.

Deliberately stdlib-only and single-file, same as the smoke fixture, so it starts
with nothing but `python3` — no image, no install, no network.

Usage:
  python3 dev/mock-upstream-dev.py [--port 9100] [--seed N]
"""

import argparse
import json
import random
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODELS = ["gpt-4o", "gpt-4o-mini", "claude-sonnet-4-20250514", "gpt-4.1"]

# Request sizes, in tokens. Weighted towards the small end because that is what
# a real caller mostly sends, with a long tail so a chart of an hour has
# something to show at the top. The first request is fixed to the values the
# smoke fixture uses, so a fresh ledger has a row that matches the deployment
# path exactly and can be compared against it.
#
# Cached prompt tokens are a fraction of the prompt, never more than it, and
# never reported at all for a minority of requests — a NULL cached count is a
# different dashboard state from a zero one, and only a fixture that sometimes
# omits the field can produce it.
PROMPT_TOKENS = [11, 28, 64, 145, 320, 780, 1600]
COMPLETION_TOKENS = [7, 19, 48, 112, 260, 640]
CACHED_TOKEN_CHOICES = [0.0, 0.15, 0.4, 0.75, 1.0]
# ~30% of requests report no cache information at all.
OMIT_CACHE_PROBABILITY = 0.3

# How long an answer takes, in seconds.
#
# The brief for this fixture is a dashboard review of *long* requests: every
# duration is drawn uniformly from a band whose floor is fifteen seconds. A
# short-heavy distribution was tried first (a 2.8s mean with a tail to 19s) and
# it filled the ledger with rows a duration chart renders as one flat blob near
# zero — the opposite of what the loop is for. Within the band the draw is
# uniform, because once every request is long, biasing the middle of the band
# would just move the blob.
#
# The band's ceiling is set by the config, not by taste. `dev/
# partner-portal.dev.yaml` sets `timeout_secs: 30`, and that is both an overall
# deadline and an idle deadline between frames, so a draw past it becomes a
# `GATEWAY_TIMEOUT` — a real state, but one this fixture must produce on
# request (`x-dev-fail: hang`), not by accident on its own draws. Twenty-six
# seconds plus the proxy's and curl's overhead stays under it with room to
# spare.
STREAM_SECONDS = (15.0, 26.0)
# A non-stream answer is a single write, so its duration is the provider's
# think time. It gets the same band: the brief is about request durations, and
# it does not distinguish streamed from not.
THINK_SECONDS = (15.0, 26.0)
# The share of the total spent before the first token. A long wait with no
# output is a different chart from a slow drip of tokens, and the dashboard has
# columns for both, so the split is drawn too.
FIRST_TOKEN_SHARE = (0.12, 0.55)
FRAME_COUNTS = [1, 2, 3, 4, 6, 9, 14]

# One generator for the whole process, shared by every thread: a per-request
# `random.Random()` seeded from the clock would make two requests arriving in
# the same microsecond identical, and `random`'s global is already
# thread-safe. The lock costs nothing next to an HTTP round trip.
_rng = random.Random()
_rng_lock = threading.Lock()

# One counter per outcome, printed on SIGINT. A load script that only sees
# "200 OK" cannot tell a provider that answered from one the proxy synthesised.
_counters = {
    "chat": 0,
    "responses": 0,
    "models": 0,
    "failed": 0,
    "streamed": 0,
    "no_usage": 0,
    "hangs": 0,
    "closed": 0,
}
_lock = threading.Lock()
_started = time.monotonic()


def bump(name: str) -> None:
    with _lock:
        _counters[name] += 1


def weighted(rng: random.Random, values: list, decay: float = 1.5) -> object:
    """A draw from `values`, biased towards the front.

    The lists are ordered small-to-large, so weighting the earlier entries more
    heavily gives the shape a real workload has: mostly small requests and a
    thin tail of large ones. A uniform draw over those lists would be almost
    entirely large requests, which is the opposite of the truth.

    `decay` is how steeply each later entry is penalised. Sizes and frame
    counts decay at 1.5, which puts a fifth of the requests in the largest
    bucket. Durations are not on this path — see `STREAM_SECONDS` for why they
    draw uniformly from a band instead.
    """
    count = len(values)
    weights = [decay ** (count - 1 - i) for i in range(count)]
    return rng.choices(values, weights=weights, k=1)[0]


class Shape:
    """The numbers one answer reports: how big, how cached, how long.

    Drawn once per request and passed to whichever response the path produces,
    so a stream and a non-stream of the same request could not disagree — and,
    in a fixture, a disagreement would look exactly like a metering bug.
    """

    def __init__(self, rng: random.Random, *, stream: bool):
        self.prompt = weighted(rng, PROMPT_TOKENS)
        self.completion = weighted(rng, COMPLETION_TOKENS)
        self.cached = (
            None
            if rng.random() < OMIT_CACHE_PROBABILITY
            else int(self.prompt * rng.choice(CACHED_TOKEN_CHOICES))
        )
        self.frames = weighted(rng, FRAME_COUNTS)
        if not stream:
            self.think = rng.uniform(*THINK_SECONDS)
            self.first_token = 0.0
            self.frame_gap = 0.0
            return

        # The total is drawn from the band and the pacing derived from it, so
        # the longest answer this fixture can produce is the top of
        # STREAM_SECONDS — see the comment there on the 30s ceiling.
        self.think = 0.0
        total = rng.uniform(*STREAM_SECONDS)
        self.first_token = total * rng.uniform(*FIRST_TOKEN_SHARE)
        # The first frame lands after first_token; the frames share what is
        # left of the total *exactly*, so the stream lands on `total` whatever
        # the frame count. A one-frame draw therefore holds the line silent for
        # the whole remainder — up to about 23s — which is precisely the
        # long-silent upstream the idle deadline exists to police, and it stays
        # legal because the remainder is bounded by the band. (The earlier
        # per-gap cap is gone: against a 15s floor it truncated every
        # one-frame, long-remainder draw to below the floor, which is how a
        # stream asked to be long came back as a 12s one.)
        self.frame_gap = max((total - self.first_token) / self.frames, 0.01)

    def usage(self, input_key: str, output_key: str) -> dict:
        block = {
            input_key: self.prompt,
            output_key: self.completion,
            "total_tokens": self.prompt + self.completion,
        }
        if self.cached is not None:
            # The spelling differs by endpoint; src/proxy/usage.rs reads
            # prompt_tokens_details for chat and input_tokens_details for
            # responses, and a fixture that sent one name for both would leave
            # one endpoint's cached column permanently NULL.
            details = (
                "prompt_tokens_details"
                if input_key == "prompt_tokens"
                else "input_tokens_details"
            )
            block[details] = {"cached_tokens": self.cached}
        return block


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    # Any failure the client asked for, as a mode string. Read once per request.
    def _failure_mode(self) -> str | None:
        mode = self.headers.get("x-dev-fail")
        return mode.strip().lower() if mode else None

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        # bytes, not str: `http.server`'s send_header encodes with latin-1
        # unless the value is already bytes, and a plain str of digits is
        # fine — but json.dumps of a body containing non-ASCII would make the
        # *body* header set fail. The length is computed from the encoded
        # bytes for the same reason.
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status: int, message: str, code: str) -> None:
        # The shape an OpenAI-compatible client already knows how to read, and
        # the shape the proxy hands to the caller unchanged
        # (handler.rs::forward_upstream_error). A fixture that returned a bare
        # string would make the proxy's error passthrough look untested.
        self._send(
            status,
            {"error": {"message": message, "type": "invalid_request_error", "code": code}},
        )

    def do_GET(self) -> None:  # noqa: N802 - the base class names these
        if self.path.startswith("/v1/models"):
            bump("models")
            self._send(
                200,
                {
                    "object": "list",
                    "data": [{"id": m, "object": "model", "owned_by": "mock"} for m in MODELS],
                },
            )
            return

        if self.path == "/__count":
            with _lock:
                snapshot = dict(_counters)
            self._send(
                200,
                {
                    "uptime_secs": round(time.monotonic() - _started, 1),
                    "counters": snapshot,
                },
            )
            return

        if self.path == "/healthz":
            self._send(200, {"status": "ok"})
            return

        self._send_error(404, f"unknown path {self.path}", "not_found")

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            return json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return {}

    def _maybe_fail(self, request: dict) -> bool:
        """Honour x-dev-fail. Returns True when the request was answered.

        `request` is the already-parsed body: the request stream is consumed
        once, here in `_body`, and re-reading it would block.
        """
        mode = self._failure_mode()
        if not mode:
            return False

        if mode == "hang":
            bump("hangs")
            # Longer than the proxy's connect/response timeout in any sane dev
            # config, so the *proxy* gives up rather than this fixture.
            time.sleep(3600)
            return True

        # An injected failure that answers instantly would draw a row with a
        # near-zero duration in the dashboard's duration chart — a dot at zero
        # next to the 15–26s band everything else sits in, and exactly the
        # "0s requests" artefact this fixture exists to prevent. So a failure
        # costs the same think time an answer would have: an upstream that
        # queued a request for twenty seconds and then refused it is a real
        # shape, and it keeps the chart honest.
        def fail_after_thinking() -> None:
            with _rng_lock:
                delay = _rng.uniform(*THINK_SECONDS)
            time.sleep(delay)

        if mode == "close":
            bump("closed")
            # Close the connection without a status line. The proxy must turn
            # this into a recorded failure rather than a dropped request.
            fail_after_thinking()
            self.close_connection = True
            try:
                self.connection.close()
            except OSError:
                pass
            return True

        if mode == "no-usage":
            # Not a failure — a successful stream that reports no usage. It only
            # means anything when the caller asked for a stream: a non-stream
            # response with no usage block would be a different (and much
            # rarer) case, so it is refused rather than silently answered
            # normally, which would make the header look like it did nothing.
            if not request.get("stream"):
                bump("failed")
                fail_after_thinking()
                self._send_error(
                    400,
                    "x-dev-fail: no-usage requires a streaming request",
                    "mock_bad_request",
                )
                return True
            return False

        try:
            status = int(mode)
        except ValueError:
            status = 500

        bump("failed")
        fail_after_thinking()
        self._send_error(
            status,
            f"mock upstream was asked to fail with {status}",
            "mock_injected_failure",
        )
        return True

    def do_POST(self) -> None:  # noqa: N802
        request = self._body()
        if self._maybe_fail(request):
            return

        if self.path.startswith("/v1/chat/completions"):
            bump("chat")
            model = request.get("model") or MODELS[0]
            with _rng_lock:
                shape = Shape(_rng, stream=bool(request.get("stream")))
                reply_id = f"chatcmpl-dev-{_rng.randrange(10**9):09d}"
            if request.get("stream"):
                self._chat_stream(model, reply_id, shape, no_usage=bool(self.headers.get("x-dev-fail")))
                return
            time.sleep(shape.think)
            self._send(
                200,
                {
                    "id": reply_id,
                    "object": "chat.completion",
                    "created": int(time.time()),
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": "ok"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": shape.usage("prompt_tokens", "completion_tokens"),
                },
            )
            return

        if self.path.startswith("/v1/responses"):
            bump("responses")
            model = request.get("model") or MODELS[0]
            with _rng_lock:
                shape = Shape(_rng, stream=bool(request.get("stream")))
                reply_id = f"resp-dev-{_rng.randrange(10**9):09d}"
            if request.get("stream"):
                self._responses_stream(model, reply_id, shape, no_usage=bool(self.headers.get("x-dev-fail")))
                return
            time.sleep(shape.think)
            self._send(
                200,
                {
                    "id": reply_id,
                    "object": "response",
                    "model": model,
                    "output": [],
                    "usage": shape.usage("input_tokens", "output_tokens"),
                },
            )
            return

        self._send_error(404, f"unknown path {self.path}", "not_found")

    # --- streaming -----------------------------------------------------------
    #
    # Frames are written and flushed one at a time with a small gap, because the
    # thing under review is a dashboard that has to update while a stream is
    # still open. A fixture that emits all six frames in one write would make
    # every stream look instantaneous.

    def _open_stream(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

    def _frame(self, payload: dict, gap: float) -> None:
        self.wfile.write(f"data: {json.dumps(payload)}\n\n".encode())
        self.wfile.flush()
        if gap > 0:
            time.sleep(gap)

    def _chat_stream(self, model: str, reply_id: str, shape: Shape, *, no_usage: bool) -> None:
        bump("streamed")
        self._open_stream()
        # The first frame is delayed by its own draw, so TTFT is a real
        # measurement here and not the sum of an arbitrary number of identical
        # gaps. Every later frame is one gap.
        time.sleep(shape.first_token)
        for i in range(shape.frames):
            self._frame(
                {
                    "id": reply_id,
                    "object": "chat.completion.chunk",
                    "model": model,
                    "choices": [{"index": 0, "delta": {"content": f"token-{i}"}}],
                },
                shape.frame_gap,
            )

        if no_usage:
            # End cleanly with no usage event and no [DONE] sentinel — the
            # fixture in tests/common/mod.rs::chat_stream_without_usage. The
            # request is a success; the tokens are simply absent, and the
            # dashboard must show NULL rather than 0.
            bump("no_usage")
            return

        self._frame(
            {
                "id": reply_id,
                "object": "chat.completion.chunk",
                "model": model,
                "choices": [],
                "usage": shape.usage("prompt_tokens", "completion_tokens"),
            },
            0.0,
        )
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

    def _responses_stream(self, model: str, reply_id: str, shape: Shape, *, no_usage: bool) -> None:
        bump("streamed")
        self._open_stream()
        time.sleep(shape.first_token)
        for i in range(shape.frames):
            self._frame(
                {
                    "type": "response.output_text.delta",
                    "item_id": f"msg_{i}",
                    "delta": f"token-{i}",
                },
                shape.frame_gap,
            )
        if no_usage:
            bump("no_usage")
            return
        self._frame(
            {
                "type": "response.completed",
                "response": {
                    "id": reply_id,
                    "model": model,
                    "usage": shape.usage("input_tokens", "output_tokens"),
                },
            },
            0.0,
        )

    def log_message(self, fmt: str, *args) -> None:
        # One line per request, so the terminal running this fixture shows the
        # traffic the proxy produced — including the ones it injected.
        elapsed = time.monotonic() - _started
        sys.stdout.write(f"mock-upstream-dev +{elapsed:7.1f}s  {fmt % args}\n")
        sys.stdout.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="seed the draws; without it every run is different",
    )
    args = parser.parse_args()

    if args.seed is not None:
        _rng.seed(args.seed)

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.daemon_threads = True
    print(
        f"mock-upstream-dev listening on http://127.0.0.1:{args.port}\n"
        f"  models: {', '.join(MODELS)}\n"
        f"  sizes, cache hits, frame counts and timings are drawn per request"
        f"{f' (seed {args.seed})' if args.seed is not None else ''}\n"
        f"  inject a failure with the x-dev-fail header: "
        f"500 | 503 | close | hang | no-usage",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nmock-upstream-dev stopping", flush=True)
    finally:
        with _lock:
            print(f"mock-upstream-dev counters: {_counters}", flush=True)
        server.server_close()


if __name__ == "__main__":
    main()
