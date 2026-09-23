#!/usr/bin/env bash
#
# Smoke test for a single built image: start it exactly as the deployment does,
# prove it serves, and prove it shuts down without dropping metered usage.
#
# This is the check the CI `container` job runs, and the same one an operator can
# run against a tag before promoting it. It is deliberately not a compose test —
# the two-instance stack has its own (deploy/smoke-test.sh) — so that a failure
# here is a failure of the image, not of a stack around it.
#
# What it asserts, in order:
#   * the image's runtime contract: non-root uid, exec-form entrypoint, healthcheck
#     defined, the hardening the compose files apply is survivable (read-only root
#     filesystem, all capabilities dropped, no new privileges)
#   * /healthz answers 200 (liveness; no database consulted)
#   * /readyz answers 200 with ready=true (the metering writer is committing)
#   * /version reports a schema version (what a rolling update compares)
#   * authentication is enforced: 401 without a key, 401 with a wrong one, 200 with
#     the configured one
#   * /v1/models is proxied to the upstream and answers
#   * /v1/chat/completions is proxied *and metered*: the ledger row exists, with the
#     token usage the upstream reported
#   * the dashboard is embedded: GET / returns the built index, not the
#     "built without a dashboard" placeholder, and a hashed asset is served
#   * SIGTERM drains the metering pipeline ("metering pipeline drained and
#     committed") before the process exits, and the ledger survives on the volume
#
# Usage:
#   scripts/docker-smoke-test.sh [image]        # default: partner-portal:test
#
# Requires: docker, curl, python3 (for the stub upstream and JSON assertions).

set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

IMAGE="${1:-partner-portal:test}"
# Deliberately outside the range deploy/smoke-test.sh uses (18080-18082): that
# script leaves its two-instance stack running when it finishes, so a CI job that
# runs both must not have them fight over a port. Overridable either way.
HOST_PORT="${HOST_PORT:-18090}"
MOCK_PORT="${MOCK_PORT:-19000}"
KEY="smoke-key"
UPSTREAM_MODEL="mock-model"
CONTAINER="partner-portal-smoke-$$"
MOCK_CONTAINER="partner-portal-mock-$$"
VOLUME="partner-portal-smoke-data-$$"
NETWORK="partner-portal-smoke-net-$$"
WORK="$(mktemp -d)"

failures=0
ok() { printf '  ok   %s\n' "$*"; }
bad() {
    printf '  FAIL %s\n' "$*"
    failures=$((failures + 1))
}
say() { printf '\n=== %s\n' "$*"; }
info() { printf '  info %s\n' "$*"; }

expect_eq() {
    local what="$1" want="$2" got="$3"
    if [ "$want" = "$got" ]; then
        ok "$what: $got"
    else
        bad "$what: expected $want, got $got"
    fi
}

BODY="$WORK/body"
STATUS="$WORK/status"
# Written to files rather than captured, so the status survives out of the
# subshell a command substitution would create.
fetch() {
    local url="$1"
    shift
    curl -sS --max-time 15 -o "$BODY" -w '%{http_code}' "$@" "$url" >"$STATUS"
}
status() { cat "$STATUS"; }
body() { cat "$BODY"; }

json_get() {
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

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    docker rm -f "$MOCK_CONTAINER" >/dev/null 2>&1 || true
    docker volume rm "$VOLUME" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
    rm -rf "$WORK"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
say "The image's runtime contract"
# ---------------------------------------------------------------------------
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "image $IMAGE not found; build it first:" >&2
    echo "  docker build -t $IMAGE ." >&2
    exit 1
fi

expect_eq "entrypoint is exec form" '["/usr/local/bin/partner-portal"]' \
    "$(docker image inspect --format '{{json .Config.Entrypoint}}' "$IMAGE")"

expect_eq "configured user is not root" "10001:10001" \
    "$(docker image inspect --format '{{.Config.User}}' "$IMAGE")"

if docker image inspect --format '{{json .Config.Healthcheck}}' "$IMAGE" \
    | grep -q PARTNER_PORTAL_HEALTH_URL; then
    ok "image healthcheck is defined and parameterised by PARTNER_PORTAL_HEALTH_URL"
else
    bad "image has no healthcheck, or one that ignores PARTNER_PORTAL_HEALTH_URL"
fi

expect_eq "healthcheck default points at liveness" \
    "http://127.0.0.1:8080/healthz" \
    "$(docker image inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$IMAGE" \
        | sed -n 's/^PARTNER_PORTAL_HEALTH_URL=//p')"

expect_eq "config default is the documented mount point" \
    "/etc/partner-portal/config.yaml" \
    "$(docker image inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$IMAGE" \
        | sed -n 's/^PARTNER_PORTAL_CONFIG=//p')"

expect_eq "declared volume" '["/var/lib/partner-portal"]' \
    "$(docker image inspect --format '{{json .Config.Volumes}}' "$IMAGE" | python3 -c 'import json,sys; print(json.dumps(sorted(json.load(sys.stdin).keys())))')"

expect_eq "exposed port" "['8080/tcp']" \
    "$(docker image inspect --format '{{json .Config.ExposedPorts}}' "$IMAGE" | python3 -c 'import json,sys; print(sorted(json.load(sys.stdin).keys()))')"

# ---------------------------------------------------------------------------
say "Starting the stub upstream and the container"
# ---------------------------------------------------------------------------
# The upstream runs as a container on a network shared with the proxy, which is
# also how deploy/docker-compose.yml wires it. Container-to-host networking was
# the alternative and is not used: `host.docker.internal` needs
# `--add-host …:host-gateway` and then depends on the host's firewall and on the
# daemon's bridge, and a smoke test that fails on a hardened host is a smoke test
# that gets skipped.
docker network create "$NETWORK" >/dev/null

docker run -d --name "$MOCK_CONTAINER" \
    --network "$NETWORK" \
    --read-only --tmpfs /tmp \
    --cap-drop ALL --security-opt no-new-privileges \
    -p "127.0.0.1:$MOCK_PORT:9000" \
    -v "$(pwd)/deploy/mock-upstream.py:/app/mock-upstream.py:ro" \
    -e PORT=9000 \
    python:3.13-alpine python /app/mock-upstream.py >/dev/null

for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:$MOCK_PORT/__count" >/dev/null 2>&1; then break; fi
    sleep 0.5
done
if curl -fsS "http://127.0.0.1:$MOCK_PORT/__count" >/dev/null 2>&1; then
    ok "stub upstream serving on 127.0.0.1:$MOCK_PORT"
else
    bad "stub upstream did not start"
    docker logs "$MOCK_CONTAINER" 2>&1 | tail -20
    exit 1
fi

# The deployment's own smoke config, with the upstream repointed at the stub.
sed "s|http://mock-upstream:9000|http://$MOCK_CONTAINER:9000|" \
    deploy/config/partner-portal.smoke.yaml >"$WORK/config.yaml"

# Started with the hardening the compose files apply, so this test covers the
# deployment's actual shape: read-only root filesystem, every capability dropped,
# no new privileges, non-root user, a writable /tmp only.
docker run -d --name "$CONTAINER" \
    --network "$NETWORK" \
    --read-only --tmpfs /tmp \
    --cap-drop ALL --security-opt no-new-privileges \
    -p "127.0.0.1:$HOST_PORT:8080" \
    -v "$WORK/config.yaml:/etc/partner-portal/config.yaml:ro" \
    -v "$VOLUME:/var/lib/partner-portal" \
    "$IMAGE" >/dev/null

for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:$HOST_PORT/healthz" >/dev/null 2>&1; then break; fi
    if ! docker inspect --format '{{.State.Running}}' "$CONTAINER" | grep -q true; then
        bad "container exited during startup"
        docker logs "$CONTAINER" 2>&1 | tail -20
        exit 1
    fi
    sleep 1
done
expect_eq "container is up under read_only + cap_drop ALL" "true" \
    "$(docker inspect --format '{{.State.Running}}' "$CONTAINER")"

BASE="http://127.0.0.1:$HOST_PORT"

# ---------------------------------------------------------------------------
say "Liveness, readiness, identity"
# ---------------------------------------------------------------------------
fetch "$BASE/healthz"
expect_eq "/healthz" "200" "$(status)"
expect_eq "/healthz status field" "ok" "$(body | json_get status)"

fetch "$BASE/readyz"
expect_eq "/readyz" "200" "$(status)"
expect_eq "/readyz ready field" "True" "$(body | json_get ready)"

fetch "$BASE/version"
expect_eq "/version" "200" "$(status)"
info "/version schema_version=$(body | json_get schema_version) commit=$(body | json_get commit)"

# ---------------------------------------------------------------------------
say "Authentication"
# ---------------------------------------------------------------------------
fetch "$BASE/api/me"
expect_eq "no key is rejected" "401" "$(status)"

fetch "$BASE/api/me" -H "Authorization: Bearer wrong-key"
expect_eq "wrong key is rejected" "401" "$(status)"

fetch "$BASE/api/me" -H "Authorization: Bearer $KEY"
expect_eq "configured key is accepted" "200" "$(status)"

# ---------------------------------------------------------------------------
say "Proxying and metering"
# ---------------------------------------------------------------------------
fetch "$BASE/v1/models" -H "Authorization: Bearer $KEY"
expect_eq "/v1/models through the proxy" "200" "$(status)"
expect_eq "/v1/models answers with the upstream model list" "$UPSTREAM_MODEL" \
    "$(body | json_get data.0.id)"

MODEL="smoke-image-$(date +%s)"
fetch "$BASE/v1/chat/completions" \
    -X POST \
    -H "Authorization: Bearer $KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"model\":\"$MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}"
expect_eq "/v1/chat/completions through the proxy" "200" "$(status)"

# The ledger writer batches on a 1s timeout; the row is durable but may not be
# queryable within the same millisecond the response returns.
sleep 2
fetch "$BASE/api/dashboard/requests?range=24h&limit=10&model=$MODEL" \
    -H "Authorization: Bearer $KEY"
expect_eq "ledger query" "200" "$(status)"

if body | python3 -c '
import json, sys

rows = json.load(sys.stdin)["data"]
if len(rows) != 1:
    print(f"  FAIL expected exactly 1 ledger row, found {len(rows)}")
    sys.exit(1)

row = rows[0]
status = row["request_status"]
input_tokens = row["input_tokens"]
output_tokens = row["output_tokens"]

if status != "completed":
    print(f"  FAIL request_status is {status}, expected completed")
    sys.exit(1)
if (input_tokens, output_tokens) != (11, 7):
    print(f"  FAIL usage recorded as {input_tokens}/{output_tokens}, expected 11/7")
    sys.exit(1)

print(f"  ok   1 ledger row, completed, usage {input_tokens}/{output_tokens}")
'; then
    :
else
    failures=$((failures + 1))
fi

# ---------------------------------------------------------------------------
say "The dashboard is embedded, not a placeholder"
# ---------------------------------------------------------------------------
fetch "$BASE/"
expect_eq "GET /" "200" "$(status)"
if body | grep -qi '<div id="app">'; then
    ok "GET / serves the built dashboard"
else
    bad "GET / does not look like the built dashboard (placeholder page?)"
fi

# Vite emits content-hashed assets; the index names them. Pulling one out of the
# index and fetching it proves the asset table was embedded, not just index.html.
asset="$(body | grep -o '/assets/[A-Za-z0-9._-]*\.js' | head -1)"
if [ -n "$asset" ]; then
    fetch "$BASE$asset"
    expect_eq "hashed asset $asset" "200" "$(status)"
    expect_eq "hashed asset is served as an immutable, content-addressed URL" \
        "public, max-age=31536000, immutable" \
        "$(curl -sSI "$BASE$asset" | tr -d '\r' | awk 'tolower($1)=="cache-control:"{ $1=""; print substr($0,2)}')"
else
    bad "index.html names no /assets/*.js bundle"
fi

# ---------------------------------------------------------------------------
say "SIGTERM drains the metering pipeline"
# ---------------------------------------------------------------------------
docker stop --time 30 "$CONTAINER" >/dev/null
if docker logs "$CONTAINER" 2>&1 | grep -q "metering pipeline drained and committed"; then
    ok "the drain sequence completed on SIGTERM"
else
    bad "no completed drain in the logs; the metering queue may have been dropped"
    docker logs "$CONTAINER" 2>&1 | tail -20
fi

# The ledger must be on the volume, not inside the container: a database written
# into the image layer disappears at the next deploy, taking the usage with it.
if docker run --rm -v "$VOLUME:/data" --entrypoint /bin/sh "$IMAGE" \
    -c 'test -s /data/partner-portal.db'; then
    ok "the ledger persists on the declared volume"
else
    bad "no ledger on the volume after shutdown"
fi

# ---------------------------------------------------------------------------
if [ "$failures" -eq 0 ]; then
    printf '\nall checks passed\n'
    exit 0
fi
printf '\n%s check(s) failed\n' "$failures"
exit 1
