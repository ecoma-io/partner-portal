#!/usr/bin/env bash
#
# End-to-end smoke test for the two-instance deploy stack.
#
# It starts the real stack (two proxies, one shared SQLite ledger, an nginx edge
# and a stub upstream), drives real traffic through the edge, and then checks the
# accounting from three independent directions:
#
#   1. the upstream's own request counter      — was each request forwarded once?
#   2. the ledger, via the dashboard API       — was each request recorded once,
#                                                with the right usage?
#   3. each instance's own committed counter   — did the shared ledger receive the
#                                                sum of both writers?
#
# All three agreeing is what "no loss, no duplication" means here. Any two of
# them can agree while the third is wrong, which is why all three are checked.
#
# It also stops an instance and asserts that the drain sequence in src/main.rs ran
# to completion ("metering pipeline drained and committed") rather than being cut
# short by a SIGKILL.
#
# And, because the per-key model gate (docs/adr/0012) is part of the deployment
# path, it asserts the gate's quiet direction too: a request for a model the key
# does not list is refused 404 `model_not_found` before the upstream is
# contacted, and leaves every counter above untouched.
#
# Finally, because the key set is a database (docs/adr/0014) and not a line in the
# config, it exercises the operations that replaced the config rewrite this script
# used to perform on itself: `partner-portal keygen` seeds the key into the shared
# volume before the stack starts; the manager password reaches the admin surface
# while the partner key is refused there; a `PATCH` narrows the key's model list
# and the very next request against that instance is refused by the gate, with no
# restart and no waiting; widening it serves again; and the key still
# authenticates after an instance restart, because it lives on the volume rather
# than in the file both instances mount.
#
# Usage:
#   PARTNER_PORTAL_IMAGE=partner-portal:local ./smoke-test.sh
#
# Requires: docker with compose v2, curl, python3. Leaves the stack running.

set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

IMAGE="${PARTNER_PORTAL_IMAGE:-partner-portal:local}"
COMPOSE=(docker compose -f docker-compose.yml --profile smoke)

# Host ports, defaulting to the ones deploy/docker-compose.yml documents and
# overridable so the stack can run on a machine that already listens on 8080.
EDGE_PORT="${EDGE_PORT:-8080}"
PORTAL_A_PORT="${PORTAL_A_PORT:-8081}"
PORTAL_B_PORT="${PORTAL_B_PORT:-8082}"
export EDGE_PORT PORTAL_A_PORT PORTAL_B_PORT

# Which file inside ./config the instances load. This must be the smoke config:
# the production one points at the real api.openai.com, so a stack that loaded it
# would reject every request here *and* would have been one restart away from
# making live calls.
PARTNER_PORTAL_CONFIG_FILE="${PARTNER_PORTAL_CONFIG_FILE:-partner-portal.smoke.yaml}"
export PARTNER_PORTAL_CONFIG_FILE

# The volume the instances share, and the database on it (docs/adr/0014: the
# ledger holds the partner keys as well as the usage). Named exactly as
# docker-compose.yml names it and overridable the same way rolling-update.sh
# overrides it, so the key this script seeds is the key the stack loads.
DATA_VOLUME="${PARTNER_PORTAL_DATA_VOLUME:-partner-portal-deploy_portal-data}"
DB_PATH="/var/lib/partner-portal/partner-portal.db"

# The secret the key hashes are keyed with. It must not change between runs —
# this script seeds into a volume that outlives it, and a hash written under one
# secret is not verifiable under another — so it is a fixed fixture here, exactly
# like `smoke-upstream-key`, and overridable for a CI job that supplies its own.
# compose refuses to start without it (`:?` in docker-compose.yml), which is why
# it is exported rather than merely set.
API_KEY_SECRET="${PARTNER_PORTAL_API_KEY_SECRET:-smoke-api-key-secret-not-a-real-one!!}"
export PARTNER_PORTAL_API_KEY_SECRET="$API_KEY_SECRET"

EDGE="http://127.0.0.1:${EDGE_PORT}"
PORTAL_A="http://127.0.0.1:${PORTAL_A_PORT}"
PORTAL_B="http://127.0.0.1:${PORTAL_B_PORT}"
# Longer than KEY_PREFIX_LEN on purpose: a real key is `pp_` plus 43 characters,
# and a key *shorter* than the prefix length is stored whole in `key_prefix`, which
# would make "the listing does not contain the secret" impossible to assert — the
# prefix would be the secret.
KEY="smoke-key-please-replace"
# The consumer the seeded key belongs to, and the manager password the smoke
# config sets. Neither is a credential the deployment ships with: both are
# fixtures for this script, like `smoke-upstream-key`.
KEY_CONSUMER="partner-smoke"
MANAGER="smoke-manager"
# Unique per run, so a leftover ledger from an earlier run cannot be mistaken for
# traffic produced by this one.
MODEL="smoke-$(date +%s)"
# The traffic-switch burst gets its own name off the same stamp: its three rows
# must stay separable from the main burst's in every ledger query below.
SWITCH_MODEL="$MODEL-switch"
# The model the admin-API section below is about: in the key's list when the stack
# starts, dropped by a PATCH and added back by another, and never in the same
# query as the two above — its rows are counted on their own.
GATE_MODEL="$MODEL-gate"
# Refused by the gate: in no allow-list, so the smoke proves the refusal without
# ever being able to reach the upstream under it.
REFUSED_MODEL="$MODEL-not-allowed"
REQUESTS=20
PROMPT_TOKENS=11
COMPLETION_TOKENS=7

# /healthz and /readyz are probed with curl; the bodies and status codes are
# written to files rather than captured in command substitution, because a
# subshell assignment would not survive back into this shell.
BODY_FILE="$(mktemp)"
STATUS_FILE="$(mktemp)"

# The file that decides which instances the edge routes to, and a copy of it to
# restore on exit.
#
# It is now the *only* repository file this script edits: the config the
# instances load is not amended any more, because the key's model list is a row
# in the database and is changed through the admin API (docs/adr/0014). Nothing
# writes `config/`, so nothing has to be restored there.
UPSTREAM_CONF="nginx/upstream.d/upstream.conf"
UPSTREAM_CONF_BACKUP="$(mktemp)"
cp "$UPSTREAM_CONF" "$UPSTREAM_CONF_BACKUP"

failures=0
say() { printf '\n=== %s\n' "$*"; }
ok() { printf '  ok   %s\n' "$*"; }
bad() {
    printf '  FAIL %s\n' "$*"
    failures=$((failures + 1))
}
info() { printf '  info %s\n' "$*"; }

expect_eq() {
    local what="$1" want="$2" got="$3"
    if [ "$want" = "$got" ]; then
        ok "$what: $got"
    else
        bad "$what: expected $want, got $got"
    fi
}

# curl a URL into $BODY_FILE, its status code into $STATUS_FILE.
fetch() {
    local url="$1"
    shift
    curl -sS -o "$BODY_FILE" -w '%{http_code}' "$@" "$url" >"$STATUS_FILE"
}
status() { cat "$STATUS_FILE"; }
body() { cat "$BODY_FILE"; }

json_get() {
    # A dotted path, small enough that jq is not a dependency.
    python3 -c '
import json, sys
doc = json.load(sys.stdin)
for key in sys.argv[1].split("."):
    if key == "":
        continue
    doc = doc[int(key)] if key.isdigit() else doc[key]
print(doc)
' "$1"
}

# The upstream's counter, read from inside the compose network — the mock is
# deliberately not published on the host.
mock_count() {
    "${COMPOSE[@]}" exec -T mock-upstream python -c '
import json, urllib.request
print(json.load(urllib.request.urlopen("http://127.0.0.1:9000/__count"))["inference_requests"])
' | tr -d '\r'
}

# The state of the smoke key's row, as stored: `active`, `revoked`, or `absent`
# when there is no row (or no database yet). Read through a throwaway container on
# the shared volume, read-only, so the assertion comes from the ledger the
# instances write rather than from an API that filters.
#
# `< 12 characters is the whole prefix`: the smoke key is shorter than
# KEY_PREFIX_LEN, so the stored prefix *is* the key — which is what makes it
# findable here, and is also why a real deployment's keys are generated long.
key_state() {
    docker run --rm \
        -v "$DATA_VOLUME:/data" \
        python:3.13-alpine python -c '
import sqlite3, sys
try:
    conn = sqlite3.connect("file:/data/partner-portal.db?mode=ro", uri=True, timeout=10)
    row = conn.execute(
        "SELECT status FROM api_keys WHERE key_prefix = ? LIMIT 1", (sys.argv[1],)
    ).fetchone()
except sqlite3.Error:
    print("absent")
    sys.exit(0)
print("absent" if row is None else row[0])
' "${KEY:0:12}"
}

# The ledger *records* an instance reports it has committed since it started.
# Cumulative for the life of the process, so it only means anything as a delta
# taken around the traffic this test generates — and a *restart* resets it, which
# is why a counter that went backwards is reported as a finding of its own rather
# than as arithmetic that happens to be negative.
#
# An instance that is not answering is diagnosed here instead of failing as an
# empty string three checks later: a curl that dies mid-pipeline takes the whole
# script with it under `set -e`, and a suite that exits 1 with no explanation
# costs whoever reads the log far more than this line does.
ledger_records() {
    local url="$1" body
    if ! body="$(curl -fsS --max-time 10 "$url/readyz")"; then
        printf 'ledger_records: %s/readyz did not answer: the instance is down or restarting\n' "$url" >&2
        return 1
    fi
    printf '%s\n' "$body" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["ledger_committed"])'
}

cleanup() {
    # The traffic-switch check rewrites the upstream that is in rotation, so the
    # repository copy is restored even if the script fails part-way: a failed run
    # must not leave the deployment pointing at one instance. Nothing else in the
    # repository is written by this run.
    cp "$UPSTREAM_CONF_BACKUP" "$UPSTREAM_CONF"
    rm -f "$BODY_FILE" "$STATUS_FILE" "$UPSTREAM_CONF_BACKUP"
}
trap cleanup EXIT

# total_requests, total_input_tokens, total_output_tokens — the rollup the
# dashboard reads (a different table and a different query path than the raw
# request rows). Read as a delta around the burst below so rows from an earlier
# run in the same ledger cannot skew the comparison.
dashboard_totals() {
    curl -sS -H "Authorization: Bearer $KEY" "$EDGE/api/dashboard/summary?range=24h" \
        | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(d["total_requests"], d["total_input_tokens"], d["total_output_tokens"])
'
}

# The worker processes the edge is running right now, by pid.
#
# `nginx -s reload` is asynchronous and returns immediately: the master spawns
# the new generation and tells the old one to finish and exit, and until that has
# happened a request is still handled by an *old* worker with the configuration
# from before the edit. A check that counts requests after a switch therefore has
# to wait for the old generation to retire, or it measures the transition rather
# than the switch. That wait is polled, not slept: a fixed delay long enough on an
# idle machine is short enough on a loaded CI runner, which is the same defect in
# a hat.
edge_workers() {
    docker exec partner-portal-edge pgrep -P 1 nginx 2>/dev/null | sort | tr '\n' ' '
}

# Take an instance out of, or put it back into, rotation — the traffic switch
# that rolling-update.sh performs, exercised here by the check that proves it
# works. `down` is nginx's own mark: the server line stays exactly where it is
# (the record of which slot is which), and the loader simply stops selecting it.
#
# The edit is applied through the existing inode (write a temporary, then
# truncate-and-write the real file) rather than with `sed -i` or `mv`, both of
# which replace the file. The container sees the file through a directory mount,
# so a replacement would still be picked up — but the property this deployment
# relies on should hold even if that mount is ever narrowed to a single file,
# where a replacement leaves nginx reading the inode it already had.
rotation() {
    local state="$1" instance="$2" tmp
    # The workers that must be gone before the edited file is the configuration
    # being served. Read before the edit; the pids are stable across a reload.
    local old_workers
    old_workers=" $(edge_workers) "
    tmp="$(mktemp)"
    case "$state" in
    down) sed "s|^\( *server ${instance}:8080 resolve\)|\1 down|" "$UPSTREAM_CONF" >"$tmp" ;;
    up) sed "s|^\( *server ${instance}:8080 resolve\) down|\1|" "$UPSTREAM_CONF" >"$tmp" ;;
    *)
        echo "rotation: state must be down or up, got $state" >&2
        rm -f "$tmp"
        return 1
        ;;
    esac
    cat "$tmp" >"$UPSTREAM_CONF"
    rm -f "$tmp"
    # Graceful: workers finish in-flight requests against the old configuration
    # before exiting, so a switch cannot cut a streaming response in half. Its
    # own notice line ("signal process started") is stderr noise on success, so
    # it is held back and shown only when the reload actually fails.
    local reload_output
    if ! reload_output="$(docker exec partner-portal-edge nginx -s reload 2>&1)"; then
        printf '%s\n' "$reload_output" >&2
        return 1
    fi

    # Wait for the reload to take effect, observed rather than assumed: the old
    # generation is gone, so every worker that can still accept a connection has
    # read the edited file. The bound is a bound, not a delay — a reload with no
    # long-lived request to finish retires its workers in milliseconds, and five
    # seconds of wall clock is only spent when the reload genuinely did not take.
    local attempt current pid retired
    for attempt in $(seq 1 100); do
        retired=1
        current="$(edge_workers)"
        if [ -z "${current// /}" ]; then
            # No workers at all: mid-reload, or the edge is going down. Not a
            # state to serve traffic checks from.
            retired=0
        else
            for pid in $current; do
                case "$old_workers" in
                *" $pid "*) retired=0 ;;
                esac
            done
        fi
        if [ "$retired" = 1 ]; then
            return 0
        fi
        sleep 0.05
    done
    echo "rotation: the edge is still running a pre-switch worker 5s after the reload" >&2
    return 1
}

# ---------------------------------------------------------------------------
say "Preflight"
# ---------------------------------------------------------------------------
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "image $IMAGE not found; build it first:" >&2
    echo "  docker build -t $IMAGE ." >&2
    exit 1
fi
ok "image $IMAGE present"

# The instances must start with the key set this run expects, not pick it up
# mid-run: the model gate (docs/adr/0012) reads the allow-list, and a burst sent
# before the snapshot refresh lands would 404. Taking the stack down first makes
# the start deterministic whether or not a previous run left one behind — without
# `-v`: the volume survives, which is exactly why MODEL is unique per run.
#
# Down before the key is seeded, too: the seed belongs to a stopped deployment,
# and an instance that is running would load a half-built key set for no reason.
"${COMPOSE[@]}" down --remove-orphans >/dev/null

# The key this run authenticates with, seeded into the shared volume with the
# image's own `keygen` — the first-run step deploy/README.md documents, and the
# reason the smoke config carries no `keys:` block (docs/adr/0014). It is the
# same binary the deployment runs, which is also what makes this a test of the
# image's provisioning path.
#
# It runs before any container has mounted the volume, and that is deliberate: on
# first creation Docker seeds an empty named volume with the image's own directory
# ownership (uid 10001, mode 0750), and only a container that has
# /var/lib/partner-portal in its image can do that. A reader started first would
# create the volume root-owned and the deployment would then fail to open its own
# database.
#
# The key is issued with exactly the models the deployment serves plus this run's,
# in one call: the bundled mock answers one model, and the run's own unique names
# are not knowable to a hot reload, so they are part of the key rather than a file
# edit. The `PATCH` section below then changes that list through the API — the
# operation this script exists to exercise.
if seed_report="$(docker run --rm \
    -v "$DATA_VOLUME:/var/lib/partner-portal" \
    -e "PARTNER_PORTAL_API_KEY_SECRET=$API_KEY_SECRET" \
    "$IMAGE" keygen \
    --database "$DB_PATH" \
    --name partner-smoke \
    --consumer-id "$KEY_CONSUMER" \
    --plaintext "$KEY" \
    --allowed-model mock-model \
    --allowed-model "$MODEL" \
    --allowed-model "$SWITCH_MODEL" \
    --allowed-model "$GATE_MODEL" 2>&1)"; then
    ok "seeded the smoke key into $DATA_VOLUME with $IMAGE's keygen"
else
    # The volume outlives this script, so a second run finds the key already
    # there — and a UNIQUE failure on the stored hash is how that surfaces. The
    # state is read rather than assumed, because a *revoked* key collides the
    # same way and means something else entirely, and neither `keygen` nor the
    # operator can undo it: the hash is the same hash.
    seeded_state="$(key_state)"
    if [ "$seeded_state" = "active" ]; then
        ok "the smoke key is already in $DATA_VOLUME (left alone)"
    else
        printf '%s\n' "$seed_report" >&2
        bad "could not seed the smoke key: it is '$seeded_state' in $DATA_VOLUME"
        exit 1
    fi
fi

# `--wait` blocks on the compose healthcheck, which probes /readyz rather than
# /healthz: an instance that is up but not ready to receive traffic is not up for
# this purpose.
"${COMPOSE[@]}" up -d --wait --wait-timeout 180 >/dev/null
ok "stack started"

# ---------------------------------------------------------------------------
say "Probes on each instance directly (bypassing the edge)"
# ---------------------------------------------------------------------------
for pair in "portal-a:$PORTAL_A" "portal-b:$PORTAL_B"; do
    name="${pair%%:*}"
    url="${pair#*:}"

    fetch "$url/healthz"
    expect_eq "$name /healthz status" "200" "$(status)"
    expect_eq "$name /healthz status field" "ok" "$(body | json_get status)"

    fetch "$url/readyz"
    expect_eq "$name /readyz status" "200" "$(status)"
    expect_eq "$name /readyz ready" "True" "$(body | json_get ready)"

    fetch "$url/version"
    expect_eq "$name /version status" "200" "$(status)"
    info "$name /version schema_version=$(body | json_get schema_version)"
done

# ---------------------------------------------------------------------------
say "The edge routes to the instances"
# ---------------------------------------------------------------------------
# Checked before anything else goes through it, and reported separately: an edge
# that is up but not routing (a stale listen port, a configuration that loaded
# the wrong file) otherwise shows up as a confusing failure deep inside the
# authentication or accounting checks, which are about the instances, not the edge.
edge_ok=""
for _ in $(seq 1 30); do
    if curl -fsS --max-time 5 "$EDGE/healthz" >/dev/null 2>&1; then
        edge_ok=yes
        break
    fi
    sleep 1
done
if [ -n "$edge_ok" ]; then
    ok "edge $EDGE answers /healthz"
else
    bad "edge $EDGE did not answer /healthz"
    "${COMPOSE[@]}" logs --no-log-prefix edge 2>&1 | tail -20
fi

# ---------------------------------------------------------------------------
say "Authentication"
# ---------------------------------------------------------------------------
fetch "$EDGE/api/me"
expect_eq "unauthenticated /api/me is rejected" "401" "$(status)"

fetch "$EDGE/api/me" -H "Authorization: Bearer not-a-key"
expect_eq "unknown key is rejected" "401" "$(status)"

fetch "$EDGE/api/me" -H "Authorization: Bearer $KEY"
expect_eq "authenticated /api/me" "200" "$(status)"

# ---------------------------------------------------------------------------
say "Through the edge: $REQUESTS inference requests, 1 unmetered discovery call"
# ---------------------------------------------------------------------------
upstream_before="$(mock_count)"
rollup_before="$(dashboard_totals)"
committed_before_a="$(ledger_records "$PORTAL_A")"
committed_before_b="$(ledger_records "$PORTAL_B")"

for i in $(seq 1 "$REQUESTS"); do
    code="$(curl -sS -o /dev/null -w '%{http_code}' \
        -X POST "$EDGE/v1/chat/completions" \
        -H "Authorization: Bearer $KEY" \
        -H 'Content-Type: application/json' \
        -d "{\"model\":\"$MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"ping $i\"}]}")"
    if [ "$code" != "200" ]; then
        bad "inference request $i through the edge returned $code"
    fi
done
ok "$REQUESTS inference requests through the edge"

# /v1/models is authenticated and proxied but deliberately not metered
# (src/proxy/handler.rs). It is here to prove the discovery path works, and to
# make the ledger assertion below meaningful: the inference requests, and nothing
# else, must appear there.
fetch "$EDGE/v1/models" -H "Authorization: Bearer $KEY"
expect_eq "/v1/models through the edge" "200" "$(status)"
expect_eq "/v1/models returns the upstream model list" "mock-model" \
    "$(body | json_get data.0.id)"

upstream_after="$(mock_count)"
expect_eq "upstream saw exactly $REQUESTS inference requests" \
    "$REQUESTS" "$((upstream_after - upstream_before))"

# ---------------------------------------------------------------------------
say "The ledger recorded every request exactly once"
# ---------------------------------------------------------------------------
# The writer batches on a 1s timeout, so the last row can still be in flight when
# the last response returns. Give it a moment rather than racing it.
sleep 2

fetch "$EDGE/api/dashboard/requests?range=24h&limit=200&model=$MODEL" \
    -H "Authorization: Bearer $KEY"
expect_eq "ledger query" "200" "$(status)"

if body | python3 -c '
import json, sys

expected = int(sys.argv[1])
prompt_tokens = int(sys.argv[2])
completion_tokens = int(sys.argv[3])
rows = json.load(sys.stdin)["data"]

problems = []
if len(rows) != expected:
    problems.append(f"expected {expected} ledger rows, found {len(rows)}")

ids = [r["request_id"] for r in rows]
if len(set(ids)) != len(ids):
    problems.append("duplicate request_id in the ledger")

completed = [r for r in rows if r["request_status"] == "completed"]
if len(completed) != len(rows):
    statuses = sorted({r["request_status"] for r in rows})
    problems.append(f"not every row completed: {statuses}")

wrong_usage = [
    r for r in rows
    if r["input_tokens"] != prompt_tokens or r["output_tokens"] != completion_tokens
]
if wrong_usage:
    problems.append(f"{len(wrong_usage)} row(s) recorded the wrong token usage")

if problems:
    print("  FAIL " + "; ".join(problems))
    sys.exit(1)

print(f"  ok   exactly {expected} rows, {len(set(ids))} distinct request_ids, "
      f"all completed, usage {prompt_tokens}/{completion_tokens} on every row")
' "$REQUESTS" "$PROMPT_TOKENS" "$COMPLETION_TOKENS"; then
    :
else
    failures=$((failures + 1))
fi

# The same burst, read back through the dashboard's rollup. A different table and
# a different query path from the rows above, so a row that was written but never
# rolled up — or rolled up twice — is visible here even while the row query is
# satisfied.
rollup_after="$(dashboard_totals)"
read -r requests_before tokens_in_before tokens_out_before <<<"$rollup_before"
read -r requests_after tokens_in_after tokens_out_after <<<"$rollup_after"
expect_eq "rollup counted this burst's requests" \
    "$REQUESTS" "$((requests_after - requests_before))"
expect_eq "rollup counted this burst's input tokens" \
    "$((REQUESTS * PROMPT_TOKENS))" "$((tokens_in_after - tokens_in_before))"
expect_eq "rollup counted this burst's output tokens" \
    "$((REQUESTS * COMPLETION_TOKENS))" "$((tokens_out_after - tokens_out_before))"

fetch "$EDGE/api/dashboard/models?range=24h" -H "Authorization: Bearer $KEY"
if body | python3 -c '
import json, sys
sys.exit(0 if sys.argv[1] in json.load(sys.stdin)["models"] else 1)
' "$MODEL"; then
    ok "the model rollup lists this run's model"
else
    bad "the model rollup does not list $MODEL"
fi

# Each instance counts the ledger *records* it committed: an accept and a
# finalize per request, or one record when the writer collapses the pair into a
# single batch. It is a record count, not a request count, so the invariant is a
# bound rather than an equality — fewer than one record per request would mean a
# request metered nowhere, more than two would mean a record committed twice.
committed_after_a="$(ledger_records "$PORTAL_A")"
committed_after_b="$(ledger_records "$PORTAL_B")"
# A counter that went backwards means the instance restarted inside the burst.
# Said explicitly, because otherwise it surfaces only as a negative delta that
# fails the bound below with no hint of why — and because an instance that
# restarts on its own is a finding regardless of what the ledger shows.
if [ "$committed_after_a" -lt "$committed_before_a" ] \
    || [ "$committed_after_b" -lt "$committed_before_b" ]; then
    bad "an instance restarted during the burst (its committed-record counter went backwards)"
fi
committed_a=$((committed_after_a - committed_before_a))
committed_b=$((committed_after_b - committed_before_b))
total_committed=$((committed_a + committed_b))
if [ "$total_committed" -ge "$REQUESTS" ] && [ "$total_committed" -le "$((REQUESTS * 2))" ]; then
    ok "committed ledger records for this burst: $total_committed, within [$REQUESTS, $((REQUESTS * 2))]"
else
    bad "committed ledger records for this burst: $total_committed, outside [$REQUESTS, $((REQUESTS * 2))]"
fi
info "portal-a committed $committed_a, portal-b committed $committed_b of them"

# The gate's quiet direction, where it matters: the refusal happens before the
# upstream is contacted and before any record is written, so the upstream counter
# sits exactly where the burst left it and the ledger checks above stay true
# afterwards.
fetch "$EDGE/v1/chat/completions" \
    -X POST \
    -H "Authorization: Bearer $KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"model\":\"$REFUSED_MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"refused\"}]}"
expect_eq "a model outside allowed_models is refused" "404" "$(status)"
expect_eq "the refusal names the gate" "model_not_found" "$(body | json_get error.code)"
expect_eq "the refused model never reached the upstream" "$REQUESTS" "$(mock_count)"

# ---------------------------------------------------------------------------
say "The admin surface, and the key it manages (docs/adr/0014)"
# ---------------------------------------------------------------------------
# The key set is a table now, not a line in the config both instances mount, so
# the operations that replace editing that file are exercised against the live
# deployment: the manager password reaches the surface and a partner key does not,
# no response ever carries a plaintext, and a PATCH is honoured by the instance
# that served it on the very next request.

fetch "$PORTAL_A/api/admin/api-keys"
expect_eq "unauthenticated /api/admin/api-keys is rejected" "401" "$(status)"

fetch "$PORTAL_A/api/admin/api-keys" -H "Authorization: Bearer $KEY"
expect_eq "a partner key is refused the admin surface" "403" "$(status)"
expect_eq "the refusal names the credential it wants" "manager_required" \
    "$(body | json_get error.code)"

# The listing an operator reads: the prefix identifies the key, and the secret is
# not in the response at all. Asserted on the body just fetched rather than on a
# second request, so the two checks below are about one response.
fetch "$PORTAL_A/api/admin/api-keys" -H "Authorization: Bearer $MANAGER"
expect_eq "the manager password reaches the admin surface" "200" "$(status)"
if body | grep -qF -- "$KEY"; then
    bad "the key listing contains the plaintext key"
else
    ok "the key listing contains the prefix ${KEY:0:12} and not the secret"
fi
if body | grep -q 'key_hash'; then
    bad "the key listing leaks the stored hash"
else
    ok "the key listing does not leak the stored hash"
fi

if ! key_id="$(body | python3 -c '
import json, sys
rows = json.load(sys.stdin)
for row in rows:
    if row["key_prefix"] == sys.argv[1]:
        print(row["id"])
        break
else:
    sys.exit(f"no api key with prefix {sys.argv[1]} in the listing")
' "${KEY:0:12}")"; then
    bad "the manager listing has no key with prefix ${KEY:0:12}"
    exit 1
fi

# Through portal-a directly, not through the edge: a mutation is visible to the
# instance that served it on the next request (the store reloads its snapshot
# inside the same call), and to a sibling within one refresh interval. Asserting
# the sibling here would be asserting a timing bound rather than an operation —
# tests/e2e/api_key_refresh.rs is where that bound is measured.
fetch "$PORTAL_A/api/admin/api-keys/$key_id" \
    -X PATCH \
    -H "Authorization: Bearer $MANAGER" \
    -H 'Content-Type: application/json' \
    -d '{"allowed_models":["mock-model"]}'
expect_eq "PATCH narrows the key's model list" "200" "$(status)"
expect_eq "the PATCH response carries the list it set" "['mock-model']" \
    "$(body | json_get allowed_models)"

# The gate, in its quiet direction again — but this time the model is one the key
# *did* list, a moment ago. A refusal here is the proof that the PATCH took effect
# on this instance with no restart and no waiting.
gate_upstream_before="$(mock_count)"
fetch "$PORTAL_A/v1/chat/completions" \
    -X POST \
    -H "Authorization: Bearer $KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"model\":\"$GATE_MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"gate\"}]}"
expect_eq "a model the key no longer lists is refused" "404" "$(status)"
expect_eq "the refusal names the gate" "model_not_found" "$(body | json_get error.code)"
expect_eq "the dropped model never reached the upstream" \
    "$gate_upstream_before" "$(mock_count)"

fetch "$EDGE/api/dashboard/requests?range=24h&limit=200&model=$GATE_MODEL" \
    -H "Authorization: Bearer $KEY"
expect_eq "the refused model minted no ledger row" "0" \
    "$(body | python3 -c 'import json, sys; print(len(json.load(sys.stdin)["data"]))')"

fetch "$PORTAL_A/api/admin/api-keys/$key_id" \
    -X PATCH \
    -H "Authorization: Bearer $MANAGER" \
    -H 'Content-Type: application/json' \
    -d "{\"allowed_models\":[\"mock-model\",\"$MODEL\",\"$SWITCH_MODEL\",\"$GATE_MODEL\"]}"
expect_eq "PATCH widens it again" "200" "$(status)"

fetch "$PORTAL_A/v1/chat/completions" \
    -X POST \
    -H "Authorization: Bearer $KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"model\":\"$GATE_MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"gate again\"}]}"
expect_eq "the widened model is served again" "200" "$(status)"

# The writer batches on a 1s timeout, so wait for it rather than racing it — and
# then count: exactly one row, from the one accepted request above. The refused
# one is the interesting half, and it is already accounted for: it minted nothing.
sleep 2
fetch "$EDGE/api/dashboard/requests?range=24h&limit=200&model=$GATE_MODEL" \
    -H "Authorization: Bearer $KEY"
expect_eq "the accepted probe is the model's only ledger row" "1" \
    "$(body | python3 -c '
import json, sys
rows = json.load(sys.stdin)["data"]
done = [r for r in rows if r["request_status"] == "completed"]
print(len(rows) if len(done) == len(rows) else f"{len(rows)} rows, {len(done)} completed")
')"

# ---------------------------------------------------------------------------
say "SIGTERM drains the metering pipeline instead of discarding it"
# ---------------------------------------------------------------------------
"${COMPOSE[@]}" stop portal-a >/dev/null
if "${COMPOSE[@]}" logs --no-log-prefix portal-a 2>/dev/null \
    | grep -q "metering pipeline drained and committed"; then
    ok "portal-a logged a completed drain on SIGTERM"
else
    bad "portal-a did not log a completed drain; the metering queue may have been dropped"
fi

# ---------------------------------------------------------------------------
say "Traffic switch: the survivor takes the load"
# ---------------------------------------------------------------------------
# portal-a is stopped, but its container still exists and `portal-a` is still a
# line in the upstream, so nothing moves the traffic until the edit is made. It
# has to be made, too: a stopped container's address is a black hole rather than
# a closed port — Docker removes the network endpoint, so a connection to it
# hangs in connect and ends as a 504 rather than failing fast, and nginx does not
# retry (`proxy_next_upstream off`). That is why a switch has to be live *before*
# an instance is stopped (what the settle step in rolling-update.sh buys), and why
# rotation() waits for the reload to take effect instead of for a fixed delay.
# Anything other than a 200 here means the switch is not doing the work.
switch_before="$(mock_count)"
if rotation down portal-a; then
    ok "portal-a marked down in the edge's upstream, and the workers that still routed to it have retired"
else
    bad "could not take portal-a out of rotation"
fi

switched=0
for i in 1 2 3; do
    code="$(curl -sS -o /dev/null -w '%{http_code}' \
        -X POST "$EDGE/v1/chat/completions" \
        -H "Authorization: Bearer $KEY" \
        -H 'Content-Type: application/json' \
        -d "{\"model\":\"$SWITCH_MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"ping $i\"}]}")"
    [ "$code" = "200" ] && switched=$((switched + 1))
done
expect_eq "requests through the edge with portal-a out of rotation" "3" "$switched"

switch_after="$(mock_count)"
expect_eq "upstream saw the 3 requests sent during the switch" \
    "3" "$((switch_after - switch_before))"

sleep 2
fetch "$EDGE/api/dashboard/requests?range=24h&limit=200&model=$SWITCH_MODEL" \
    -H "Authorization: Bearer $KEY"
if body | python3 -c '
import json, sys
rows = json.load(sys.stdin)["data"]
sys.exit(0 if len(rows) == 3 and all(r["request_status"] == "completed" for r in rows) else 1)
'; then
    ok "the survivor metered all 3 requests into the shared ledger"
else
    bad "the 3 requests during the switch are not all in the ledger"
fi

# Put it back, and prove the file the operator edits is back to both instances.
if rotation up portal-a; then
    if grep -q ' down' "$UPSTREAM_CONF"; then
        bad "the upstream still marks an instance down after the switch back"
    else
        ok "both instances are back in rotation"
    fi
else
    bad "could not put portal-a back into rotation"
fi

"${COMPOSE[@]}" start portal-a >/dev/null
for _ in $(seq 1 60); do
    if curl -fsS "$PORTAL_A/readyz" >/dev/null 2>&1; then break; fi
    sleep 1
done
fetch "$PORTAL_A/readyz"
expect_eq "portal-a is ready again after restart" "200" "$(status)"

# The restarted instance authenticates with the same key, because the key is a row
# on the shared volume rather than something the old process held in memory or in
# the file both instances mount (docs/adr/0014). Asked of the restarted instance
# directly: through the edge the request could land on the peer, which never
# stopped and would answer either way.
fetch "$PORTAL_A/api/me" -H "Authorization: Bearer $KEY"
expect_eq "the restarted instance accepts the same key" "200" "$(status)"

# The restarted instance must be reading the same ledger, not a fresh one: the
# rows written before it restarted are still there, under the same model.
fetch "$EDGE/api/dashboard/requests?range=24h&limit=200&model=$MODEL" \
    -H "Authorization: Bearer $KEY"
if body | python3 -c '
import json, sys
rows = json.load(sys.stdin)["data"]
sys.exit(0 if len(rows) == int(sys.argv[1]) else 1)
' "$REQUESTS"; then
    ok "the restarted instance still sees the $REQUESTS rows written before it stopped"
else
    bad "the ledger changed across the restart"
fi

# ---------------------------------------------------------------------------
say "Neither instance restarted on its own during this run"
# ---------------------------------------------------------------------------
# A finding in its own right, and the only check here that would catch a process
# that exits cleanly on a timer. The drain path commits on the way out, so an
# instance that shuts itself down every thirty seconds still loses no records —
# the ledger checks above can pass on such a build whenever the run's timing
# happens not to overlap a restart. What it does lose is connection continuity:
# requests that land in the window fail at the edge, with no retry behind them.
#
# `RestartCount` counts restarts the *engine* performed, so this measures the
# self-exit case exactly: the `compose stop` / `compose start` this script
# performs on portal-a does not increment it. The window is this run's wall
# clock — long enough to see a loop on the order of the drain bound, not a
# detector for every possible one.
for svc in portal-a portal-b; do
    instance_container="$("${COMPOSE[@]}" ps -q "$svc")"
    restarts="$(docker inspect --format '{{.RestartCount}}' "$instance_container")"
    expect_eq "$svc did not exit on its own" "0" "$restarts"
done

# ---------------------------------------------------------------------------
say "Summary"
# ---------------------------------------------------------------------------
if [ "$failures" -eq 0 ]; then
    echo "  all checks passed"
    echo
    echo "  edge        $EDGE"
    echo "  portal-a    $PORTAL_A"
    echo "  portal-b    $PORTAL_B"
    echo "  dashboard   $EDGE/ (API under /api/dashboard/*, Bearer $KEY)"
    exit 0
fi

echo "  $failures check(s) failed"
exit 1
