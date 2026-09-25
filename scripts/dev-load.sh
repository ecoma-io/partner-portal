#!/bin/sh
#
# dev-load.sh — a steady trickle of traffic at the dev proxy, with failures
# mixed in, so the dashboard has something to show without costing anything.
#
# The defaults are thirty successful requests then one failure, asked for at two
# requests per second. The ratio is the one thing a reviewer asked for and it is
# exact, not statistical: each worker counts its own slots, so a run of 310
# requests is ten failures, whatever the pace. The pace, in turn, is bounded by
# the fixture, not by this script — every request the dev stub answers takes
# 15–26 seconds on purpose (see `STREAM_SECONDS` in `dev/mock-upstream-dev.py`),
# and no pool this script is willing to spawn can carry two requests a second of
# twenty-second requests. It sizes the pool to the best it can do and says what
# that is rather than pretending.
#
# The point is to exercise every state the dashboard can render, not to load
# test. For throughput, `scripts/bench-scale.sh` drives the real writer and
# reports percentiles; this drives the real HTTP path and is meant to be
# watched.
#
# POSIX sh, not bash. That costs the usual conveniences and buys the important
# one: this runs wherever a POSIX shell exists, not only where bash is
# installed. There are no arrays and no $RANDOM here — the choices come from
# `awk`, which the rest of the toolchain already needs.
#
# Usage:
#   scripts/dev-load.sh                 # 2 rps asked, 30:1 success:failure
#   scripts/dev-load.sh --rps 5         # five a second (see workers, below)
#   scripts/dev-load.sh --rps 5 --stream # five a second, all streaming (15–26s each)
#   scripts/dev-load.sh --pattern 3:1   # three successes per failure
#   scripts/dev-load.sh --no-failures   # successes only
#   scripts/dev-load.sh --stream        # every request is a stream
#   scripts/dev-load.sh --duration 60   # stop after a minute instead of forever
#
# Ctrl-C stops it. It touches neither the proxy, nor the stub, nor the ledger.

set -u

usage() {
  cat <<'EOF'
Usage: scripts/dev-load.sh [options]

Drives steady traffic at the dev proxy, with failures mixed in. Ctrl-C stops.

  --rps N         requests per second (default 2)
  --pattern N:M   successes per failure (default 30:1)
  --no-failures   successes only
  --stream        send every request as a stream
  --duration N    stop after N seconds instead of running forever

Environment: DEV_PROXY, DEV_UPSTREAM, DEV_KEY, DEV_BETA_KEY
EOF
}

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd) || exit 1
PROXY="${DEV_PROXY:-http://127.0.0.1:8080}"
UPSTREAM="${DEV_UPSTREAM:-http://127.0.0.1:9100}"
KEY="${DEV_KEY:-dev-key}"
BETA_KEY="${DEV_BETA_KEY:-dev-key-beta}"

RPS=2
PATTERN="30:1"
FAILURES=1
STREAM=0
DURATION=0

while [ $# -gt 0 ]; do
  case "$1" in
    --rps) RPS="${2:?--rps needs a number}"; shift 2 ;;
    --pattern) PATTERN="${2:?--pattern needs N:M}"; shift 2 ;;
    --no-failures) FAILURES=0; shift ;;
    --stream) STREAM=1; shift ;;
    --duration) DURATION="${2:?--duration needs seconds}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "$PATTERN" in
  *:*) OK_COUNT=${PATTERN%%:*}; FAIL_COUNT=${PATTERN##*:} ;;
  *) OK_COUNT=$PATTERN; FAIL_COUNT=1 ;;
esac

is_uint() { printf '%s' "$1" | grep -Eq '^[0-9]+$'; }

if ! is_uint "$RPS" || [ "$RPS" -lt 1 ]; then
  echo "--rps must be a whole number, at least 1" >&2
  exit 2
fi
if ! is_uint "$OK_COUNT" || [ "$OK_COUNT" -lt 1 ]; then
  echo "--pattern needs at least one success per cycle, e.g. 5:1" >&2
  exit 2
fi
if ! is_uint "$DURATION"; then
  echo "--duration must be a whole number of seconds" >&2
  exit 2
fi

# --- preflight --------------------------------------------------------------
# Failing here with one clear line beats a script that appears to run and
# produces nothing but connection errors.

if ! curl -fsS --max-time 3 "$PROXY/healthz" >/dev/null 2>&1; then
  cat >&2 <<EOF
The dev proxy is not answering at $PROXY.

  Start it with:  scripts/dev-up.sh
  Or point elsewhere: DEV_PROXY=http://host:port scripts/dev-load.sh
EOF
  exit 1
fi

if ! curl -fsS --max-time 3 "$UPSTREAM/healthz" >/dev/null 2>&1; then
  cat >&2 <<EOF
The dev stub is not answering at $UPSTREAM, so every request would be a
connection failure — a state, but not an interesting one to look at.

  scripts/dev-up.sh starts it, or set DEV_UPSTREAM.
EOF
  exit 1
fi

if [ ! -f "$REPO_ROOT/dev/request-generator.py" ]; then
  echo "dev-load: dev/request-generator.py is missing — this repository is" >&2
  echo "  missing a dev fixture it needs to draw varied requests." >&2
  exit 1
fi

if ! curl -fsS --max-time 3 -H "Authorization: Bearer $KEY" \
    "$PROXY/api/dashboard/summary?range=24h" >/dev/null 2>&1; then
  cat >&2 <<EOF
The proxy is up but refused the key "$KEY".
  Check the keys in dev/partner-portal.dev.yaml
EOF
  exit 1
fi

# --- how many concurrent workers -------------------------------------------
#
# The rate is set by where a request actually starts, not by how long the
# script sleeps. A loop that waits a fixed interval *and then* makes the
# request delivers one request per (interval + latency), so a 0.5s interval and
# a 0.62s request arrive at 0.9 rps under a requested 2 — a load script that
# silently delivers a fraction of what it was asked for is worse than one that
# refuses.
#
# So the wait is (share of the interval − measured request time), and the
# measured time is what curl already reports.
#
# Workers are spent for one reason: a worker is blocked for the whole duration
# of the request it is making, so a request rate is a concurrency requirement.
#
# The number to size against is the worker's cycle, and a cycle is
#
#     E[max(request, share)]     where share = workers / rate
#
# — not the mean request, and not max(mean, share) either. Both of those were
# tried and both under-size the pool, for the same reason: the request times
# have a tail, and a long request sets the length of every cycle it lands in.
# Averaging first hides exactly the requests that dominate.
#
# With the current fixture the tail is the whole distribution — every request
# runs 15–26s, so every sample dominates every share the pool cap allows, the
# cycle is the mean request time (about 21s plus overhead) and the pool
# delivers workers / cycle whatever the asked rate. That is the honest ceiling:
# two requests a second of twenty-second requests is forty concurrent
# requests, and this script's cap is far below that on purpose.
#
# There is a second cost to a large pool, and it is the reason not to simply
# ask for a hundred workers: the share is workers/rate, so doubling the workers
# doubles the pause, which pushes more requests into the max() and lengthens
# every cycle. A pool past the point where share exceeds the request time buys
# nothing and then costs, and the fix is fewer workers paced harder.
#
# So the sizing is a search, not a formula: take the worker count with the
# highest implied delivery, ties going to the smaller pool, and say what that
# delivery is. It was once "smallest pool that clears the rate under a
# max-share cap", and the cap broke it twice in one week: first it left
# WORKERS at 1 when it disqualified every candidate — one twenty-second
# request at a time, a pattern failure every ten minutes — and after the
# fixture's durations rose past fifteen seconds it kept picking a pool at half
# the delivery of a bigger one, because the cap ranked a share it liked above
# a rate it was not delivering. E[max] already prices a long share (a pool
# whose share outlasts its requests pays for the pause in its own cycle), so
# the cap bought nothing the arithmetic does not, and what it cost was the
# rate.
#
# The request-time distribution comes from the fixture, not from a guess — it
# moves whenever `dev/mock-upstream-dev.py` does, and a run that reported 4 rps
# while delivering 1.8 was this going stale.
#
# The cap is 8. Past that this is a benchmark rather than something to watch,
# and `scripts/bench-scale.sh` is the tool for that.

# Measured on 2026-09-25 with the short-heavy fixture this script used to
# sample: the difference between a worker's cycle and the stub's own duration
# was curl, the counter append and the pacing arithmetic — about 0.44s per
# request, paid by every worker on every cycle. Against the current 15–26s
# band it is noise, but it is still real time a request occupies its worker,
# so it stays inside the max() rather than being earned back by pacing.
WORKER_OVERHEAD_SECONDS=0.44

# The fixture's own draws, sampled once: 4000 request times at the shape this
# run will use, drawn from the same `Shape` the upstream will draw from, so the
# sizing sees the tail rather than the mean. One Python start-up per run, not
# per request.
SAMPLE=$(python3 -c '
import importlib.util, random, sys
spec = importlib.util.spec_from_file_location("mock", "dev/mock-upstream-dev.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
rng = random.Random(20260925)
# A streamed request has think == 0.0 and its duration is first-token plus the
# frame gaps; a non-streamed one has all of it in think. Sampling both from the
# same Shape the upstream uses is the whole point — a cycle sized on one branch
# of that conditional is a cycle sized on half the traffic, which is exactly
# what delivered a fifth of the rate while reporting full.
#
# Each sample is the stub duration plus the per-request overhead the worker
# pays (curl, the counter append, the pacing arithmetic): `took` as the worker
# actually experiences it. The overhead is added inside the max() by the awk
# below, on the same term the max is computed over.
force = sys.argv[1] == "1"
out = []
for _ in range(4000):
    streaming = force or rng.random() < 0.35
    s = m.Shape(rng, stream=streaming)
    if streaming:
        out.append(s.first_token + s.frames * s.frame_gap)
    else:
        out.append(s.think)
print(" ".join("%.4f" % t for t in out))
' "$STREAM")

# Smallest pool that can carry the rate.
#
# The cycle of a worker is E[max(took, share)]: it waits out the remainder of
# its share when the request is short, and when the request is longer than the
# share it can only send the next one as soon as this one returns. Averaging
# first and taking the max afterwards would be wrong for two reasons — the
# average hides the long tail (a 9-second stream dominates its worker for nine
# seconds), and the share is *derived* from the pool so every extra worker
# makes it longer. But both are handled by the expression itself: E[max] is
# what a pool actually delivers, measured rather than guessed.
#
# Measured on a real run (the fixture's earlier, short-heavy distribution):
# with a 2.0s share, eight samples took 0.62, 5.77, 9.01, 0.38, 0.17, 0.07,
# 0.16, 0.17 seconds, so E[max(t, 2.0)] is 3.37s and two workers deliver 0.59
# rps — exactly the 0.6 measured. The mean of those takes is 2.0s and would
# have predicted 1.0, which is precisely the lie a mean tells about a tailed
# distribution.
#
# So the pool is the count with the highest E[max(took, w/r)]-derived delivery
# — which, when the fixture draws 15–26s requests, is the largest one — and
# the script prints what it will actually deliver rather than what was asked.
# The overhead (curl, the counter append, the pacing arithmetic) is real time
# a request occupies its worker, so it is part of `took`, not something earned
# back by pacing.
WORKERS=1
_cycle=0
_share=0
_best_delivered=0
_w=1
while [ "$_w" -le 8 ]; do
  _share=$(awk -v w="$_w" -v r="$RPS" 'BEGIN { printf "%.3f", w / r }')
  _cycle=$(printf '%s\n' $SAMPLE | awk -v s="$_share" -v o="$WORKER_OVERHEAD_SECONDS" '{
    t = $1 + o
    if (t > s) sum += t; else sum += s
  } END { printf "%.4f", sum / NR }')
  _delivered=$(awk -v w="$_w" -v c="$_cycle" 'BEGIN { printf "%.4f", w / c }')
  # Take the best delivery; a tie keeps the smaller pool, because the loop
  # counts upward and only a strictly better count replaces the incumbent.
  # Two pools that deliver the same rate look the same on the dashboard, and
  # the smaller one spreads its starts more evenly.
  if [ "$(awk -v d="$_delivered" -v b="$_best_delivered" 'BEGIN { print (d > b + 0.001) ? 1 : 0 }')" = "1" ]; then
    _best_delivered=$_delivered
    WORKERS=$_w
  fi
  _w=$((_w + 1))
done
_share=$(awk -v w="$WORKERS" -v r="$RPS" 'BEGIN { printf "%.3f", w / r }')
_cycle=$(printf '%s\n' $SAMPLE | awk -v s="$_share" -v o="$WORKER_OVERHEAD_SECONDS" '{
  t = $1 + o
  if (t > s) sum += t; else sum += s
} END { printf "%.4f", sum / NR }')

# What the pool will actually reach, whether or not it clears the rate: the same
# cycle arithmetic with the worker count that survived. Reported because a rate
# a script promised and did not deliver is worse than one it said it could not.
DELIVERED_RPS=$(awk -v w="$WORKERS" -v c="$_cycle" 'BEGIN { printf "%.2f", w / c }')

if [ "$(awk -v d="$DELIVERED_RPS" -v r="$RPS" 'BEGIN { print (d < r - 0.05) ? 1 : 0 }')" = "1" ]; then
  echo "dev-load: --rps $RPS cannot be reached; the fixture's requests run 15-26s," >&2
  echo "  so the best $WORKERS workers deliver ~${DELIVERED_RPS} rps (cycle ~${_cycle}s)." >&2
  echo "  Expect less than asked for; for throughput use scripts/bench-scale.sh." >&2
fi

# The drain bound used at the end of a run, derived from the same sample the
# sizing used: a worker that has just picked up the longest request the fixture
# draws must be allowed to finish it, or the stop kills it mid-flight and the
# ledger keeps an `in_flight` row that the next restart resolves to
# `interrupted` — a failure the run never made, sitting in the dashboard as a
# fake one. Longest sample, plus the per-request overhead, plus a margin;
# measured in tenths of a second because that is the drain loop's tick.
DRAIN_TICKS=$(printf '%s\n' $SAMPLE | awk -v o="$WORKER_OVERHEAD_SECONDS" '
  { t = $1 + o; if (t > m) m = t }
  END { printf "%d", (m + 4) * 10 }')

# Each worker's share of the interval, i.e. the gap between one request
# starting and the next starting. The share the sizing settled on, floor
# included — the sizing measured against this number, so the worker loop must
# use the same one and not a recomputed w/r.
#
# The share is bounded below so a slow rate does not stretch the pause: one
# worker at 0.5 rps would otherwise sleep two seconds between requests, which
# reads as stalls rather than traffic on a dashboard that refreshes every
# second. A share of 0.25s means the worker paces to at most four requests a
# second and the rate comes from the pool, not from a pause no one can watch.
WORKER_INTERVAL=$(awk -v s="$_share" 'BEGIN { if (s < 0.25) s = 0.25; printf "%.3f", s }')

# How many requests a worker draws from the generator at a time. Large enough
# that the restart is rare at any rate this script offers, small enough that the
# file stays tiny.
GEN_BATCH=200

# --- the traffic ------------------------------------------------------------

# Every request's shape — model, endpoint, stream, message count, system
# prompts — is drawn by dev/request-generator.py, which is a separate file
# because assembling varied JSON in POSIX sh is not possible and doing it in
# awk is unreadable. What that generator guarantees, and what this script
# therefore does not have to do:
#
#   * no two consecutive requests share a model, or an endpoint. Drawing
#     uniformly at random still repeats — a quarter of the time at 4 models —
#     and on a dashboard a model appearing three times running reads as stuck,
#     which is the artefact under review.
#   * both endpoints appear, both stream and non-stream, and the payload varies
#     in size. A generator that only sent short chat requests made every chart
#     in the dashboard a flat line.
#
# Failures are still decided here rather than in the generator, because the
# ratio is the one thing a person asked for: --pattern N:M. Mixing it in would
# make the ratio depend on a draw nobody can see.

# Failures cycle through three shapes, because each exercises a different path
# through the proxy and each lands differently on the dashboard:
#
#   500 / 503  the upstream answered with an error. Recorded as `failed`, with
#              the upstream's own error message on the request row.
#   close      the upstream vanished. Also `failed`, but through the transport
#              path (the proxy's BAD_GATEWAY) rather than a reported status.
#   no-usage   a *successful* stream with no usage event. Recorded as
#              `completed` with NULL tokens — invariant 3, and the only one of
#              these that is not a failure at all.
#
# no-usage only applies to streaming requests: the case it reproduces is a
# stream that ends without a usage event, which is the only way a successful
# request ends up with NULL tokens. Without --stream the stub refuses it with a
# 400, so it leaves the cycle and the other three carry the failures alone.
#
# `hang` is deliberately absent: it blocks for an hour by design and would
# consume the worker's request budget. To see GATEWAY_TIMEOUT, send one by hand:
#
#   curl -H 'x-dev-fail: hang' -H "Authorization: Bearer dev-key" \
#        -H 'content-type: application/json' -d '{"model":"gpt-4o","messages":[]}' \
#        http://127.0.0.1:8080/v1/chat/completions
if [ "$STREAM" = "1" ]; then
  FAILURE_MODES="500 503 close no-usage"
else
  FAILURE_MODES="500 503 close"
fi

# Counters live in a directory, one file per worker. A worker only ever appends
# to its own, so there is no shared write and no lock — which is what POSIX sh
# gives you instead of an associative array, and it happens to be the faster
# arrangement anyway.
COUNTERS_DIR=$(mktemp -d "${TMPDIR:-/tmp}/dev-load.XXXXXX") || exit 1
STOP_FILE="$COUNTERS_DIR/stop"

WORKER_PIDS=""

cleanup() {
  : > "$STOP_FILE"
  # The workers notice the stop file within one slice of their interval and
  # exit on their own; the wait is bounded so Ctrl-C is never a hang. `kill -0`
  # is the test: it is true while any of them is alive. The bound is the same
  # derived drain the clean stop uses — the fixture's requests run 15–26s, so
  # a Ctrl-C that killed a worker mid-request would leave the ledger an
  # `in_flight` row the run never earned.
  #
  # No generator to signal: each one finishes a bounded batch and exits, so
  # there is no process left behind by a run that was cut short.
  i=0
  while [ "$i" -lt "$DRAIN_TICKS" ]; do
    alive=0
    for p in $WORKER_PIDS; do
      if kill -0 "$p" 2>/dev/null; then alive=1; break; fi
    done
    [ "$alive" = "0" ] && break
    i=$((i + 1))
    sleep 0.1
  done
  kill $WORKER_PIDS 2>/dev/null
  rm -rf "$COUNTERS_DIR"
}

trap 'echo; echo "dev-load: stopping."; cleanup; exit 0' INT TERM

# The loop one worker runs. It ends when the stop file appears, so Ctrl-C does
# not depend on a signal reaching a subshell.
worker() {
  _id="$1"
  _count_file="$COUNTERS_DIR/count.$_id"
  : > "$_count_file"

  # One long-lived generator per worker, rather than a fresh Python process per
  # request: the anti-repeat has to remember the previous model, so it has to be
  # the same process across requests, and a `python3` fork per request would also
  # dominate a 35ms request.
  _gen="$COUNTERS_DIR/gen.$_id"

  # The generator is asked for a bounded batch of requests, not for an endless
  # stream, and re-launched when the batch runs out.
  #
  # The obvious alternative — a FIFO, so the generator blocks until the next
  # request is read — does not work, and it fails quietly enough to be worth
  # recording. A writer that runs forever on a pipe is killed by SIGPIPE the
  # moment the reader goes away, and Python's default is to die on that signal
  # without raising: measured here, the generator served 8 requests and exited
  # silently, so the run reported zero after the first one with nothing in the
  # log to say why. A batch file has no such failure mode — the generator
  # finishes what it was asked for and exits, and the next batch starts when the
  # next request is due.
  #
  # The batch is 200 requests, which at the highest rate this script offers is
  # still a minute of traffic: the restart is rare enough to be invisible, and
  # the file stays small enough that reading a line from it is instant.
  _gen_batch() {
    "$REPO_ROOT/dev/request-generator.py" "$_id" "$1" "$KEY" "$BETA_KEY" "$STREAM" \
      2>/dev/null | head -n "$GEN_BATCH" > "$_gen"
  }

  _gen_seq=0
  _gen_batch "$_gen_seq"
  _gen_seq=$((_gen_seq + 1))

  # The descriptor is held open and drained with `read <&3` for one line at a
  # time. That is not a convenience: `read < "$_gen"` would reopen the file on
  # every iteration and start again at byte zero, sending the first request
  # forever. POSIX `read` cannot be told to use a descriptor and cannot be given
  # a timeout — dash rejects `-t` outright and `-u` is a bashism — so the
  # descriptor is shared through the loop's own file descriptor and a
  # regeneration replaces the file, not the descriptor.
  exec 3< "$_gen"
  _gen_left=$(( $(wc -l < "$_gen") ))

  while [ ! -f "$STOP_FILE" ]; do
    if [ "$_gen_left" -le 0 ]; then
      # A new batch, a new file, and therefore a new descriptor: the old one is
      # at end-of-file and reading it again would return nothing forever.
      exec 3<&-
      _gen_batch "$_gen_seq"
      _gen_seq=$((_gen_seq + 1))
      _gen_left=$(wc -l < "$_gen")
      [ "$_gen_left" -gt 0 ] || {
        echo "dev-load: the generator produced nothing for worker $_id" >&2
        break
      }
      exec 3< "$_gen"
    fi

    # Four tab-separated fields, body last. `IFS` is a literal tab and the
    # first three variables are the only ones named, so the body — which
    # contains spaces, quotes and braces — lands whole in _body and keeps them
    # all, and the trailing field is not split on its own spaces.
    #
    # A line the generator did not write comes back with empty fields rather
    # than with a half-filled request, so the field count is checked instead of
    # trusting that something was matched.
    _stream=""
    IFS="$(printf '\t')" read -r _key _path _stream _body <&3 || break
    _gen_left=$((_gen_left - 1))

    if [ -z "$_key" ] || [ -z "$_path" ] || [ -z "$_body" ]; then
      echo "dev-load: could not read a generated request; stopping." >&2
      echo "  worker $_id read a line with fewer than four fields" >&2
      echo "  (key='$_key' path='$_path' stream='$_stream')" >&2
      break
    fi

    # The request is counted as sent *before* it leaves, so the pattern is
    # global rather than per-worker. A per-worker counter was the first design
    # and it is wrong on this fixture: a worker only starts a request every
    # ~21s, so with the default 30:1 the first failure needed one worker to
    # reach its 31st request — eleven minutes of nothing, then a burst of
    # eight failures arriving together, which on a dashboard reads as an
    # incident, not a ratio. Counting every worker's request in one place puts
    # a failure every OK_COUNT+FAIL_COUNT requests regardless of how many
    # workers there are.
    #
    # The count is `grep -c` over small files, and two workers racing between
    # append and count can both see the same total — a slot doubled or skipped
    # once in a while, worth a fraction of a percent, against a lock that
    # could wedge the pool if a worker died holding it.
    printf 'sent\n' >> "$_count_file"
    _sent_total=$(cat "$COUNTERS_DIR"/count.* 2>/dev/null | grep -c '^sent$')
    # This request's place in the global cycle: OK_COUNT successes, then the
    # failure slots, then round again.
    _slot=$(( (_sent_total - 1) % (OK_COUNT + FAIL_COUNT) ))

    _mode=""
    if [ "$FAILURES" = "1" ] && [ "$_slot" -ge "$OK_COUNT" ]; then
      # Walk the list in order, so the mix of failure shapes is even rather
      # than random: over a session you see all of them, not whichever came up
      # most often. The index is derived from the same global total as the
      # slot — which failure number this is, divided across every worker — so
      # the mix cycles 500, 503, close, ... globally instead of every worker
      # starting its own count back at 500.
      _mode=$(awk -v list="$FAILURE_MODES" \
        -v k="$(( (_sent_total - 1 - OK_COUNT) / (OK_COUNT + FAIL_COUNT) ))" \
        'BEGIN { n = split(list, a, " "); print a[k % n + 1] }')
    fi

    # `no-usage` is only meaningful for a stream — the stub refuses it otherwise
    # with a 400 — so a non-stream request in a failure slot falls back to the
    # first always-applicable mode rather than injecting something that would
    # fail for a different reason than the one under test.
    if [ "$_mode" = "no-usage" ] && [ "$STREAM" != "1" ]; then
      case "$_body" in
        *'"stream":true'*) : ;;
        *) _mode="500" ;;
      esac
    fi

    # The failure header is only sent when there is a failure to inject: an
    # empty x-dev-fail reads as "no mode" in the stub, but sending it at all
    # is a lie about the request that shows up in a proxy's access log.
    # curl reports both the status and how long the request took, in one call.
    # The elapsed time is what the wait below is measured against, so the rate
    # is held whether the request was instant or slow.
    #
    # The max-time is above the proxy's own 30s deadline, not below it: this
    # fixture's requests run 15–26s, and a client that gave up at twenty
    # seconds would turn every success slot that drew a long request into a
    # counted failure — which is exactly how a 30:1 pattern stopped being one.
    # If a request is going to be cut, the proxy's deadline cuts it and the
    # ledger records the timeout it actually got.
    if [ -n "$_mode" ]; then
      _result=$(curl -s -o /dev/null -w '%{http_code} %{time_total}' --max-time 35 \
        -H "Authorization: Bearer $_key" \
        -H 'content-type: application/json' \
        -H "x-dev-fail: $_mode" \
        -d "$_body" \
        "$PROXY$_path" 2>/dev/null)
    else
      _result=$(curl -s -o /dev/null -w '%{http_code} %{time_total}' --max-time 35 \
        -H "Authorization: Bearer $_key" \
        -H 'content-type: application/json' \
        -d "$_body" \
        "$PROXY$_path" 2>/dev/null)
    fi

    _code=${_result%% *}
    _took=${_result##* }
    # curl on a connection failure writes "000 0.000000", and a truncated
    # response leaves _result empty. Either way this is a failure worth
    # counting rather than an arithmetic error worth crashing over.
    [ -n "$_took" ] || _took=0

    if [ "$_code" = "200" ]; then
      if [ "$_mode" = "no-usage" ]; then
        printf 'ok nu\n' >> "$_count_file"
      else
        printf 'ok\n' >> "$_count_file"
      fi
    else
      printf 'fail\n' >> "$_count_file"
    fi

    # Sleep for the interval minus what the request just cost. A request slower
    # than the interval gets no sleep at all: the worker is already behind, and
    # the honest way to catch up is to send the next one immediately.
    #
    # The wait is taken in short slices rather than one long one, so a stop is
    # noticed within a fraction of a second instead of at the end of it.
    #
    # The wait is taken in 0.02s slices so a stop is noticed promptly rather
    # than at the end of a whole interval, and the loop bound is a count of
    # slices — an integer, because `[ "$a" -lt "$b" ]` cannot compare decimals
    # and dash answers "Illegal number: 0.454" instead of trying.
    #
    # Rounded rather than truncated, and converted to slices rather than to
    # hundredths: a wait of 0.956s is 48 slices, and counting it as 96
    # hundredths would sleep twice as long as asked.
    _slices=$(awk -v iv="$WORKER_INTERVAL" -v took="$_took" \
      'BEGIN {
         n = (iv - took) / 0.02
         if (n > 0) { printf "%d", (n + 0.5) } else { print 0 }
       }')
    _waited=0
    while [ "$_waited" -lt "$_slices" ] && [ ! -f "$STOP_FILE" ]; do
      _waited=$((_waited + 1))
      sleep 0.02
    done
  done
}

tally() {
  # One awk over every worker's file: fewer processes than grep-per-category,
  # and one place where the four numbers are defined. Every line is one
  # request — there is no header to skip, and an earlier `FNR == 1 { next }`
  # here silently dropped the first request of every worker, which made a
  # correct 42-sent run report 34 and left the pattern looking wrong by
  # exactly the worker count.
  awk '
    $1 == "ok" && $2 == "nu" { nu++; next }
    $1 == "ok"                   { ok++; next }
    $1 == "fail"                 { fail++ }
    END { printf "%d %d %d %d", ok, fail, nu, ok + fail + nu }
  ' "$COUNTERS_DIR"/count.* 2>/dev/null
}

echo "dev-load: $RPS rps, pattern $OK_COUNT:$FAIL_COUNT, stream=$STREAM"
if [ "$WORKERS" -gt 1 ]; then
  echo "dev-load: $WORKERS workers, cycle ~${_cycle}s, reaching ~${DELIVERED_RPS} rps"
fi
echo "dev-load: proxy $PROXY, key $KEY"
echo "dev-load: Ctrl-C to stop. The ledger is left as it is."
echo

WORKER_PIDS=""
i=1
while [ "$i" -le "$WORKERS" ]; do
  worker "$i" &
  WORKER_PIDS="$WORKER_PIDS $!"
  i=$((i + 1))
done

started_at=$(date +%s)
last_line=0

while :; do
  if [ "$DURATION" -gt 0 ] && [ $(( $(date +%s) - started_at )) -ge "$DURATION" ]; then
    break
  fi

  # With no --duration this loop would otherwise never end, so it also stops if
  # every worker has gone. A worker cannot exit on its own — it loops until the
  # stop file appears — so this means something killed them, and continuing
  # would print a tally that never changes.
  alive=0
  for p in $WORKER_PIDS; do
    if kill -0 "$p" 2>/dev/null; then alive=1; break; fi
  done
  if [ "$alive" = "0" ]; then
    echo
    echo "dev-load: every worker exited; stopping." >&2
    break
  fi

  sleep 1
  counts=$(tally)
  set -- $counts
  if [ "$4" -ne "$last_line" ]; then
    last_line=$4
    printf '\r  %5d sent   %5d ok   %5d failed   %3d no-usage   %4ds   ' \
      "$4" "$1" "$2" "$3" "$(( $(date +%s) - started_at ))"
  fi
done

echo
# A worker stops starting requests the moment the stop file appears, but the
# request in its hand is still open — and the fixture's requests run 15–26s.
# Killing a worker mid-request leaves the ledger with a row stuck at
# `in_flight`, which the next proxy restart resolves to `interrupted`
# (invariant 2 does its job, but the ledger then shows a failure the run never
# caused). So the wait is for the workers to finish what they hold, and the
# bound is derived, not fixed: the longest request in the sample the sizing
# drew, plus overhead and margin — see DRAIN_TICKS above. The loop exits as
# soon as they are all gone.
: > "$STOP_FILE"
_waited=0
while [ "$_waited" -lt "$DRAIN_TICKS" ]; do
  _busy=0
  for p in $WORKER_PIDS; do
    if kill -0 "$p" 2>/dev/null; then _busy=1; break; fi
  done
  [ "$_busy" = "0" ] && break
  _waited=$((_waited + 1))
  sleep 0.1
done

counts=$(tally)
set -- $counts
echo
echo "dev-load: $4 sent   $1 ok   $2 failed   ($3 no-usage)"
echo
echo "  dashboard  $PROXY/        login with '$KEY', or 'dev-manager' for all consumers"
echo "  look at    the status filter set to 'failed', and the unavailable-usage card"

kill $WORKER_PIDS 2>/dev/null
rm -rf "$COUNTERS_DIR"
exit 0
