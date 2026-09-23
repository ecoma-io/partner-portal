#!/usr/bin/env python3
"""A minimal OpenAI-compatible upstream, for exercising the deploy stack.

This exists so `deploy/smoke-test.sh` can run the real proxy, the real ledger and
the real reverse proxy without an upstream credential or an outbound call. It is
a test fixture: nothing in a production deployment runs it.

It answers three things the proxy cares about:

  GET  /v1/models             a model list (proxied unmetered)
  POST /v1/chat/completions   a completion with a `usage` block, which is what
                              the proxy parses to meter the request
  GET  /__count               how many inference requests have been seen

`/__count` is the load-bearing part of the smoke test: the proxy is supposed to
forward each client request exactly once, so the upstream's own counter must
equal the number of requests the client made and the number of rows the ledger
recorded. Two counters that agree and one that does not is how a duplication or
a silent drop is told apart from a client-side error.

Deliberately stdlib-only and single-file: it must start in a bare python image
with no package install.
"""

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODEL = "mock-model"
# Deterministic token counts: the smoke test asserts on the ledger's totals, and
# a random number there would make a real mismatch indistinguishable from noise.
PROMPT_TOKENS = 11
COMPLETION_TOKENS = 7

_lock = threading.Lock()
_inference_requests = 0


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - the base class names these
        if self.path == "/__count":
            with _lock:
                count = _inference_requests
            self._send(200, {"inference_requests": count})
            return

        if self.path.startswith("/v1/models"):
            self._send(
                200,
                {
                    "object": "list",
                    "data": [{"id": MODEL, "object": "model", "owned_by": "mock"}],
                },
            )
            return

        self._send(404, {"error": {"message": f"unknown path {self.path}"}})

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            request = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            request = {}

        if self.path.startswith("/v1/chat/completions"):
            global _inference_requests
            with _lock:
                _inference_requests += 1

            # The requested model is echoed back so a mismatch between what the
            # client asked for and what the ledger recorded is visible in the
            # dashboard, not hidden by the fixture.
            model = request.get("model") or MODEL
            self._send(
                200,
                {
                    "id": "chatcmpl-mock",
                    "object": "chat.completion",
                    "created": 0,
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": "ok"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": PROMPT_TOKENS,
                        "completion_tokens": COMPLETION_TOKENS,
                        "total_tokens": PROMPT_TOKENS + COMPLETION_TOKENS,
                    },
                },
            )
            return

        if self.path.startswith("/v1/responses"):
            with _lock:
                _inference_requests += 1
            self._send(
                200,
                {
                    "id": "resp-mock",
                    "object": "response",
                    "model": request.get("model") or MODEL,
                    "output": [],
                    "usage": {
                        "input_tokens": PROMPT_TOKENS,
                        "output_tokens": COMPLETION_TOKENS,
                        "total_tokens": PROMPT_TOKENS + COMPLETION_TOKENS,
                    },
                },
            )
            return

        self._send(404, {"error": {"message": f"unknown path {self.path}"}})

    def log_message(self, fmt: str, *args) -> None:
        # One line per request on stdout, so `docker compose logs` shows the
        # upstream's own view of the traffic the edge produced.
        sys.stdout.write("mock-upstream: " + (fmt % args) + "\n")
        sys.stdout.flush()


def main() -> None:
    port = int(os.environ.get("PORT", "9000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"mock-upstream listening on 0.0.0.0:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
