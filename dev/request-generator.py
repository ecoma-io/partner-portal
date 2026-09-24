#!/usr/bin/env python3
r"""A request generator for the dev load loop: one varied request per line.

`scripts/dev-load.sh` used to draw a model with `awk rand()` and build every
body by hand. That produced a ledger where every row looked the same — same
endpoint, same stream flag, one short user message — which is a poor thing to
look at while reviewing a dashboard, because a chart of constant data shows you
the chart and nothing about the data.

So the variety is generated rather than sampled: endpoint, stream flag, model,
consumer, message count, system prompts, and payload size all vary per request.

The one non-obvious part is the anti-repeat. Drawing a model uniformly at random
still repeats: with four models, two in a row happens a quarter of the time and
two pairs in a row a sixteenth, and on a dashboard a model that appears three
times running reads as *stuck* — the exact artefact under review. So a draw is
made from everything except the last value, which makes a consecutive repeat
impossible while leaving every model equally likely otherwise.

Stdlib only, for the same reason as the other dev fixtures: `python3`, no
install. One request per line on stdout, flushed, so the shell can read it a
line at a time without deadlocking on a full pipe.

The line is four tab-separated fields — key, path, stream, body — not a JSON
object the shell has to take apart. That is a deliberate choice: the body is
still JSON, but the shell never parses it. `json.dumps` escapes every control
character, so a tab inside the body is the two characters `\` and `t` and
cannot collide with the separator; and because the body is the last field, a
POSIX `read` with `IFS` set to tab puts the whole of it — spaces and all —
into one variable. The alternative was three `sed` calls per request to peel
a JSON envelope apart, which is both slower and wrong in a way that only shows
up on some lines.

Usage:
  python3 dev/request-generator.py <worker> <seq> <key> <beta-key> <force-stream>
"""

import json
import random
import sys

MODELS = ["gpt-4o", "gpt-4o-mini", "gpt-4.1", "claude-sonnet-4-20250514"]
# The proxy's real paths, not the ledger's endpoint names. Endpoint::from_path
# compares against these, so `/v1/chat_completions` would not match a single
# path and every request would 404.
ENDPOINTS = ["/v1/chat/completions", "/v1/responses"]
ROLES = ["system", "user", "assistant", "tool"]

# The fixture never reads these. What matters is that the shape varies: a real
# client's payload is not one short sentence, and a token count that never moves
# is not a number anyone can trust the dashboard to display.
FILLER = [
    "summarise the incident report in three bullet points",
    "rewrite this paragraph so a new hire can follow it",
    "what changed between the two revisions, and does it matter?",
    "draft a short reply declining the meeting politely",
    "explain this error message to someone who has never seen a stack trace",
    "list the risks of storing this without encryption",
    "rewrite the release notes for a customer who is not technical",
    "turn these requirements into testable statements",
    "find the two places where this contradicts the design document",
    "what would break if the ordering guarantee were relaxed?",
    "write a migration that does not need a maintenance window",
    "name the three assumptions this makes without saying so",
    "compare the two approaches and say which is cheaper to operate",
    "produce a rollback plan for this change",
    "explain the tradeoff you would make on a single-node deployment",
    "spot the bug in this snippet and say why it only appears under load",
]

# A stream is a minority of requests: most callers still ask for one response.
STREAM_PROBABILITY = 0.35


class NoRepeat:
    """A random draw that will not return the same value twice running.

    The exclusion is per-drawer, so an endpoint and a model are each
    anti-repeated without either one constraining the other.
    """

    def __init__(self, values, weights=None):
        self.values = values
        self.weights = list(weights) if weights else [1] * len(values)
        self.last = None

    def pick(self, rng):
        options = [
            (v, w) for v, w in zip(self.values, self.weights) if v != self.last
        ]
        total = sum(w for _, w in options)
        threshold = rng.uniform(0, total)
        running = 0.0
        for value, weight in options:
            running += weight
            if threshold <= running:
                self.last = value
                return value
        self.last = options[-1][0]
        return self.last


def build_messages(rng):
    """A conversation of a plausible length, with system prompts when asked."""
    messages = []
    # A short exchange is the common case; a long one exists, and a generator
    # that only ever produces short ones never exercises a large payload.
    for index in range(rng.choice([1, 1, 2, 2, 3, 4, 6, 9])):
        role = rng.choice(ROLES)
        if role == "tool":
            messages.append(
                {"role": "tool", "tool_call_id": f"call_{rng.randrange(10**6)}",
                 "content": rng.choice(FILLER)}
            )
        else:
            messages.append({"role": role, "content": rng.choice(FILLER)})

    # System prompts are their own axis: a client may prepend none, one, or
    # several, and that changes the prompt-token count the dashboard shows.
    for _ in range(rng.choice([0, 0, 0, 1, 1, 2, 3])):
        messages.insert(
            0,
            {
                "role": "system",
                "content": rng.choice(FILLER),
                # A prefix is how a caller pins a shared preamble; keeping it
                # means the body is not just a shuffled list of the same roles.
                **({"name": rng.choice(["policy", "style", "tools"])} if rng.random() < 0.4 else {}),
            },
        )
    return messages


def build_input(rng, messages):
    """The Responses API accepts three shapes of `input`, and real clients use
    all of them. Sending only one would exercise a code path no caller takes."""
    shape = rng.choice(["string", "parts", "items"])
    if shape == "string":
        return rng.choice(FILLER)
    if shape == "parts":
        return [{"type": "input_text", "text": rng.choice(FILLER)}]

    # The `items` shape is built from the conversation, but a conversation of
    # only tool calls has no item this shape accepts — and an empty `input` is
    # not a request any client makes, so fall back rather than send one.
    items = [
        {"role": m["role"], "content": m["content"]}
        for m in messages
        if m["role"] in ("user", "assistant", "system")
    ]
    return items or [{"role": "user", "content": rng.choice(FILLER)}]


def main() -> None:
    if len(sys.argv) != 6:
        sys.exit("usage: request-generator.py <worker> <seq> <key> <beta-key> <force-stream>")

    worker, seq, key, beta_key, force_stream = sys.argv[1:6]
    force_stream = force_stream == "1"
    seq = int(seq)

    model = NoRepeat(MODELS)
    # No weights on the endpoint: with two values, "not the one just used"
    # already fixes the split at 50/50, and a weight would be a knob that
    # cannot do what it says. Chat dominating real traffic is a property of the
    # caller, not of this fixture.
    endpoint = NoRepeat(ENDPOINTS)

    while True:
        # Seeded per request from the worker and a counter, so a run can be
        # reproduced from two numbers and two workers never draw alike.
        rng = random.Random(f"{worker}:{seq}")
        seq += 1

        model_id = model.pick(rng)
        path = endpoint.pick(rng)

        # --stream forces every request to be a stream, so the flag is only
        # drawn when it was not.
        stream = True if force_stream else rng.random() < STREAM_PROBABILITY

        # A minority goes to the second consumer, so the dashboard's consumer
        # filter has a real split to show. A manager view over one consumer is a
        # filter that proves nothing.
        request_key = beta_key if rng.random() < 0.2 else key

        messages = build_messages(rng)
        body = {"model": model_id}
        if stream:
            body["stream"] = True
        if rng.random() < 0.35:
            body["max_tokens"] = rng.choice([64, 256, 1024, 4096])
        if rng.random() < 0.25:
            body["temperature"] = round(rng.uniform(0.0, 1.0), 2)

        if path == "/v1/chat/completions":
            body["messages"] = messages
        else:
            body["input"] = build_input(rng, messages)

        # `stream_options` is chat-completions-only: the Responses API always
        # emits its terminal usage event and has no flag to suppress it. A
        # request carrying chat's spelling to a Responses endpoint is one no
        # real client makes.
        if stream and path == "/v1/chat/completions" and rng.random() < 0.5:
            body["stream_options"] = {"include_usage": True}

        try:
            print(
                "\t".join(
                    (request_key, path, "1" if stream else "0", json.dumps(body))
                ),
                flush=True,
            )
        except BrokenPipeError:
            # The reader went away — dev-down, or a Ctrl-C in the load script.
            # Exiting quietly is right: a traceback on stderr would read as the
            # generator having failed rather than having been finished with.
            return


if __name__ == "__main__":
    main()
