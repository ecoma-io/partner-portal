#!/usr/bin/env bash
#
# Rolling update of one instance slot on a single VPS.
#
# The deployment has two instances of the same binary behind the edge, sharing
# one SQLite ledger (deploy/docker-compose.yml). An update replaces one slot at a
# time; the other slot keeps serving. Run it twice — once per slot — to move the
# whole deployment onto a new digest.
#
# The phases are the ones documented in .github/workflows/release.yml:
#
#   1  backup      online SQLite backup of the shared ledger, verified
#   2  preflight   image present, compose config valid, edge and peer healthy,
#                  disk free, backup written
#   3  migration   the new build's schema_version vs the one the running
#                  generation reports, before anything is replaced
#   4  start new   the slot is taken out of rotation, the old container is
#                  stopped (SIGTERM, so it drains) and the new build takes its
#   8  old drain   place — see the note on the order below
#   5  health      GET /healthz on the new container
#   6  readiness   GET /readyz until ready: the gate for receiving traffic
#   7  switch      the slot goes back into the edge's rotation
#   9  verify      requests through the edge, checked against the ledger
#  10  cleanup     backups pruned, the rollback command printed
#
# Why phases 4 and 8 trade places here: the two slots are fixed, each with its
# own published port, and a slot holds one container at a time. A new instance
# cannot be started alongside the old one in the same slot, so the old container
# is drained first and the new one starts in the vacated place. The property the
# documented order exists to protect is unchanged — an instance that is not
# healthy and ready never receives traffic, because the slot is out of rotation
# for the whole replacement and only goes back in at phase 7.
#
# Crash recovery does not need the peer quiesced (src/ledger/instance.rs): a row
# is recovered only when the instance that wrote it is provably gone, so the live
# peer's in-flight rows are never touched by the new instance's startup pass.
#
# Rollback
#   A new instance that is not healthy AND ready never receives traffic, so a
#   failed update leaves the peer serving and the previous image untouched. The
#   script prints the exact rollback command (this script, the previous digest)
#   at phase 10. Traffic is reverted, never an image mutated in place.
#
# Usage
#   PARTNER_PORTAL_IMAGE is ignored: --image is what is deployed.
#
#   ./rolling-update.sh --instance a --image ghcr.io/owner/partner-portal@sha256:...
#   ./rolling-update.sh --instance b --image partner-portal:local --dry-run
#
# Requires: docker with compose v2, curl, python3, and python3 on the host for
# the JSON assertions. Leaves the stack running.

set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
IMAGE=""
SLOT=""
SETTLE_SECS=5
READY_TIMEOUT=120
STOP_TIMEOUT=60
KEEP_BACKUPS=5
VERIFY_REQUESTS=3
MIN_FREE_MIB=1024
DRY_RUN=""

usage() {
    cat <<'EOF'
Rolling update of one instance slot (a or b) on a single VPS.

  --instance a|b          which slot to replace                        (required)
  --image REF             image to deploy, digest preferred            (required)
  --settle SECONDS        wait after taking the slot out of rotation      (5)
  --ready-timeout SECONDS how long to wait for /readyz                   (120)
  --stop-timeout SECONDS  docker stop grace for the old container         (60)
  --keep-backups N        ledger backups to retain                         (5)
  --verify-requests N     requests through the edge at phase 9, each of
                          which is a real, billed upstream call           (3)
  --min-free-mib N        abort if the docker root has less free space  (1024)
  --dry-run               print what would happen, change nothing
  -h, --help              this text

Environment: EDGE_PORT, PORTAL_A_PORT, PORTAL_B_PORT,
PARTNER_PORTAL_CONFIG_FILE, PARTNER_PORTAL_DATA_VOLUME — the same overrides
deploy/docker-compose.yml documents.
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
    --instance) SLOT="${2:-}"; shift 2 ;;
    --image) IMAGE="${2:-}"; shift 2 ;;
    --settle) SETTLE_SECS="${2:-}"; shift 2 ;;
    --ready-timeout) READY_TIMEOUT="${2:-}"; shift 2 ;;
    --stop-timeout) STOP_TIMEOUT="${2:-}"; shift 2 ;;
    --keep-backups) KEEP_BACKUPS="${2:-}"; shift 2 ;;
    --verify-requests) VERIFY_REQUESTS="${2:-}"; shift 2 ;;
    --min-free-mib) MIN_FREE_MIB="${2:-}"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h | --help) usage; exit 0 ;;
    *)
        echo "unknown argument: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
done

[ -n "$IMAGE" ] || {
    echo "--image is required" >&2
    exit 2
}
case "$SLOT" in
a | b) ;;
*)
    echo "--instance must be a or b" >&2
    exit 2
    ;;
esac

# ---------------------------------------------------------------------------
# The deployment this script operates on
# ---------------------------------------------------------------------------
EDGE_PORT="${EDGE_PORT:-8080}"
PORTAL_A_PORT="${PORTAL_A_PORT:-8081}"
PORTAL_B_PORT="${PORTAL_B_PORT:-8082}"
export EDGE_PORT PORTAL_A_PORT PORTAL_B_PORT

PARTNER_PORTAL_CONFIG_FILE="${PARTNER_PORTAL_CONFIG_FILE:-partner-portal.yaml}"
export PARTNER_PORTAL_CONFIG_FILE

DATA_VOLUME="${PARTNER_PORTAL_DATA_VOLUME:-partner-portal-deploy_portal-data}"
COMPOSE=(docker compose -f docker-compose.yml)
UPSTREAM_CONF="nginx/upstream.d/upstream.conf"
BACKUP_DIR="$PWD/backups"
EDGE="http://127.0.0.1:${EDGE_PORT}"

if [ "$SLOT" = a ]; then
    SLOT_PORT="$PORTAL_A_PORT"
    PEER_PORT="$PORTAL_B_PORT"
else
    SLOT_PORT="$PORTAL_B_PORT"
    PEER_PORT="$PORTAL_A_PORT"
fi
SLOT_CONTAINER="partner-portal-${SLOT}"
PEER_CONTAINER="partner-portal-$([ "$SLOT" = a ] && echo b || echo a)"
SLOT_URL="http://127.0.0.1:${SLOT_PORT}"
PEER_URL="http://127.0.0.1:${PEER_PORT}"
# How the edge addresses the two slots: the compose service name, which is what
# the containers are resolvable as on the compose network. It is not the same as
# the container name, and the upstream file is keyed by this one.
SLOT_UPSTREAM="portal-${SLOT}"
PEER_UPSTREAM="portal-$([ "$SLOT" = a ] && echo b || echo a)"

PREFLIGHT_CONTAINER="partner-portal-preflight-$$"

failures=0
say() { printf '\n=== %s\n' "$*"; }
ok() { printf '  ok   %s\n' "$*"; }
info() { printf '  info %s\n' "$*"; }
warn() { printf '  warn %s\n' "$*"; }
die() {
    printf '  FAIL %s\n' "$*" >&2
    exit 1
}
run() {
    if [ -n "$DRY_RUN" ]; then
        printf '  dry  %s\n' "$*"
        return 0
    fi
    "$@"
}

BODY_FILE="$(mktemp)"
STATUS_FILE="$(mktemp)"
cleanup() {
    docker rm -f "$PREFLIGHT_CONTAINER" >/dev/null 2>&1 || true
    rm -f "$BODY_FILE" "$STATUS_FILE"
}
trap cleanup EXIT

fetch() {
    local url="$1"
    shift
    curl -sS --max-time 10 -o "$BODY_FILE" -w '%{http_code}' "$@" "$url" >"$STATUS_FILE"
}
status() { cat "$STATUS_FILE"; }
body() { cat "$BODY_FILE"; }

json_field() { # url, field
    curl -sS --max-time 5 "$1" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)[sys.argv[1]])' "$2"
}

free_port() {
    python3 -c 'import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()'
}

# The schema version the ledger itself records (ledger_meta.schema_version).
# Read from the database rather than from a running instance's /version: what the
# migration gate is about is the file both instances write to, and an instance
# that has been replaced reports what it *would* write, not what is on disk.
ledger_schema_version() {
    docker run --rm -v "$DATA_VOLUME:/data" python:3.13-alpine python -c '
import sqlite3, sys
conn = sqlite3.connect("file:/data/partner-portal.db?mode=ro", uri=True, timeout=10)
try:
    row = conn.execute(
        "SELECT value FROM ledger_meta WHERE key = ?", ("schema_version",)
    ).fetchone()
except sqlite3.Error as e:
    sys.exit(f"the ledger has no readable schema version: {e}")
print(row[0] if row else 0)
'
}

# The first configured partner key. Phases 7 and 9 speak to the deployment the
# way a client does, which means presenting a key — the same file the instances
# read, so no credential is duplicated into this script or into its environment.
partner_key() {
    python3 -c 'import re, sys
for line in open("config/" + sys.argv[1]):
    m = re.match(r"\s*-\s*key:\s*[\"\x27]?([^\"\x27#]+)", line)
    if m:
        print(m.group(1).strip())
        break
else:
    sys.exit("no key found in config/" + sys.argv[1])' "$PARTNER_PORTAL_CONFIG_FILE"
}

# Take a slot out of, or put it back into, rotation, then reload the edge. The
# edit goes through the existing inode (write a temporary, then truncate-and-write
# the real file) rather than with `sed -i` or `mv`, both of which replace it: the
# container reads the file through a directory mount, but the property should
# hold even if that mount is ever narrowed to a single file.
rotation() {
    local state="$1" instance="$2" tmp out
    tmp="$(mktemp)"
    case "$state" in
    down) sed "s|^\( *server ${instance}:8080 resolve\)|\1 down|" "$UPSTREAM_CONF" >"$tmp" ;;
    up) sed "s|^\( *server ${instance}:8080 resolve\) down|\1|" "$UPSTREAM_CONF" >"$tmp" ;;
    *)
        rm -f "$tmp"
        die "rotation: state must be down or up, got $state"
        ;;
    esac
    if [ -n "$DRY_RUN" ]; then
        info "dry  would mark $instance $state in $UPSTREAM_CONF and reload the edge"
        rm -f "$tmp"
        return 0
    fi
    cat "$tmp" >"$UPSTREAM_CONF"
    rm -f "$tmp"
    if ! out="$(docker exec partner-portal-edge nginx -s reload 2>&1)"; then
        printf '%s\n' "$out" >&2
        die "the edge did not reload"
    fi
}

in_rotation() { # instance -> "yes"/"no"
    if grep -q "^ *server $1:8080 resolve down" "$UPSTREAM_CONF"; then
        echo no
    else
        echo yes
    fi
}

wait_for() { # url, seconds; 200 within the deadline, nothing about the body
    local url="$1" timeout="$2"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if curl -fsS --max-time 5 "$url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

wait_ready() { # url, seconds; /readyz answering 200 with ready=true
    local url="$1" timeout="$2"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if curl -fsS --max-time 5 "$url/readyz" 2>/dev/null \
            | python3 -c 'import json, sys
try:
    sys.exit(0 if json.load(sys.stdin).get("ready") is True else 1)
except Exception:
    sys.exit(1)' 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# ---------------------------------------------------------------------------
say "0. This update"
# ---------------------------------------------------------------------------
info "slot          $SLOT ($SLOT_CONTAINER, $SLOT_URL)"
info "peer          $PEER_CONTAINER ($PEER_URL)"
info "edge          $EDGE"
info "image         $IMAGE"
[ -n "$DRY_RUN" ] && warn "dry run: nothing will be changed"

# ---------------------------------------------------------------------------
say "1. Backup the ledger"
# ---------------------------------------------------------------------------
# Taken with SQLite's online backup API, not `cp`: the ledger is in WAL mode with
# two live writers, and a file copy of a database mid-commit is a copy that may
# not open. The backup connection is opened read-write because a read-only
# connection cannot always map the WAL's shared memory; it is a reader as far as
# the writers are concerned.
BACKUP_NAME="partner-portal-$(date -u +%Y%m%dT%H%M%SZ).db"
run mkdir -p "$BACKUP_DIR"
if [ -z "$DRY_RUN" ]; then
    backup_report="$(
        docker run --rm -i \
            -v "$DATA_VOLUME:/data" \
            -v "$BACKUP_DIR:/backup" \
            -e BACKUP_NAME="$BACKUP_NAME" \
            python:3.13-alpine python - <<'PY'
import os, sqlite3, sys

src = sqlite3.connect("file:/data/partner-portal.db?mode=rw", uri=True, timeout=10)
dst = sqlite3.connect(os.path.join("/backup", os.environ["BACKUP_NAME"]))
with dst:
    src.backup(dst)

integrity = dst.execute("PRAGMA integrity_check").fetchone()[0]
rows = dst.execute("SELECT count(*) FROM usage_records").fetchone()[0]
dst.close()
src.close()
print(integrity, rows)
PY
    )" || die "the ledger backup failed"
    read -r integrity rows <<<"$backup_report"
    [ "$integrity" = "ok" ] || die "the backup does not pass integrity_check ($integrity)"
    if [ "$rows" = "0" ]; then
        warn "$BACKUP_NAME is a valid backup of an empty ledger"
    else
        ok "$BACKUP_NAME: integrity ok, $rows usage record(s)"
    fi
fi

# ---------------------------------------------------------------------------
say "2. Preflight"
# ---------------------------------------------------------------------------
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    case "$IMAGE" in
    */* | *:*) run docker pull "$IMAGE" || die "cannot pull $IMAGE" ;;
    *) die "image $IMAGE is not present locally and is not a pullable reference" ;;
    esac
fi
ok "image present"

run "${COMPOSE[@]}" config -q || die "docker-compose.yml is not valid"
ok "compose configuration is valid"

docker inspect --format '{{.State.Running}}' "$SLOT_CONTAINER" 2>/dev/null \
    | grep -q true || die "$SLOT_CONTAINER is not running; this script replaces a running slot"
ok "$SLOT_CONTAINER is running"

docker inspect --format '{{.State.Running}}' "$PEER_CONTAINER" 2>/dev/null \
    | grep -q true || die "$PEER_CONTAINER is not running; there would be nothing serving during the update"
fetch "$PEER_URL/readyz" >/dev/null
[ "$(status)" = "200" ] || die "$PEER_CONTAINER is not serving traffic; fix it before updating $SLOT"
ok "$PEER_CONTAINER is up and ready: the deployment has a survivor"

fetch "$EDGE/healthz" >/dev/null
[ "$(status)" = "200" ] || die "the edge at $EDGE is not answering"
ok "the edge is answering"

docker_root="$(docker info --format '{{.DockerRootDir}}')"
free_mib="$(df -Pk "$docker_root" | awk 'NR==2 {print int($4/1024)}')"
if [ -n "$free_mib" ]; then
    [ "$free_mib" -ge "$MIN_FREE_MIB" ] \
        || die "only ${free_mib}MiB free on $docker_root; the preflight floor is ${MIN_FREE_MIB}MiB"
    ok "${free_mib}MiB free on $docker_root"
else
    warn "could not read free space on $docker_root"
fi

info "$SLOT_UPSTREAM is currently $(in_rotation "$SLOT_UPSTREAM") in rotation"

# ---------------------------------------------------------------------------
say "3. Migration compatibility"
# ---------------------------------------------------------------------------
# The new build is started once against a scratch database — no volume, so it
# cannot touch the ledger — for two reasons: it proves the image starts with this
# configuration at all, and it reports the schema version it would write. An
# older binary against a newer schema is the combination that corrupts a ledger.
ledger_schema="$(ledger_schema_version)" || die "could not read the ledger's schema version"
run docker rm -f "$PREFLIGHT_CONTAINER" >/dev/null 2>&1 || true
preflight_port="$(free_port)"
if [ -z "$DRY_RUN" ]; then
    # The scratch database lives on a tmpfs over the declared data directory: the
    # real volume is deliberately not mounted, so this container cannot read or
    # write the ledger no matter what its configuration says. The tmpfs options
    # are explicit because the default mode for a `--tmpfs` mount is not the
    # 1777 of /tmp — an instance running as uid 10001 gets a directory it cannot
    # create a database in, and fails at startup for a reason that has nothing to
    # do with the image being tested.
    docker run -d --rm --name "$PREFLIGHT_CONTAINER" \
        -v "$PWD/config:/etc/partner-portal:ro" \
        -e "PARTNER_PORTAL_CONFIG=/etc/partner-portal/$PARTNER_PORTAL_CONFIG_FILE" \
        --tmpfs /var/lib/partner-portal:rw,mode=0770,uid=10001,gid=10001 \
        -p "127.0.0.1:${preflight_port}:8080" \
        "$IMAGE" >/dev/null || die "the new image did not start"

    if ! wait_for "http://127.0.0.1:${preflight_port}/healthz" 30; then
        docker logs "$PREFLIGHT_CONTAINER" 2>&1 | tail -20
        die "the new image does not become healthy with $PARTNER_PORTAL_CONFIG_FILE"
    fi
    new_schema="$(json_field "http://127.0.0.1:${preflight_port}/version" schema_version)"
    docker rm -f "$PREFLIGHT_CONTAINER" >/dev/null 2>&1 || true
    ok "the new image starts with this configuration (scratch database)"
else
    new_schema="$ledger_schema"
    info "dry  would start the new image against a scratch database and read /version"
fi

# What each running generation believes, for the record. A deployment mid-rollout
# legitimately has two answers here — one slot migrated, the other not yet — and
# the ledger's own number is the one the gate above used.
for pair in "a:$PORTAL_A_PORT" "b:$PORTAL_B_PORT"; do
    slot_name="${pair%%:*}"
    slot_url="http://127.0.0.1:${pair#*:}"
    if reported="$(json_field "$slot_url/version" schema_version 2>/dev/null)"; then
        info "portal-$slot_name reports schema_version $reported"
    else
        info "portal-$slot_name is not answering /version"
    fi
done
info "the ledger records schema_version $ledger_schema; the new build writes $new_schema"

if [ "$new_schema" -lt "$ledger_schema" ]; then
    die "the new build is older than the ledger schema ($new_schema < $ledger_schema); deploying it would run an old binary against a migrated ledger"
fi
if [ "$new_schema" -gt "$ledger_schema" ]; then
    warn "the new build migrates the ledger ($ledger_schema -> $new_schema), by add-only migration"
    warn "a running old build keeps working through it, but it cannot be rolled back to:"
    warn "a binary that understands schema_version=$ledger_schema must not be given a ledger at $new_schema"
fi
ok "the new build is compatible with the running ledger"

# ---------------------------------------------------------------------------
say "4. Replace the instance in slot $SLOT (phases 4 and 8)"
# ---------------------------------------------------------------------------
previous_image="$(docker inspect --format '{{.Config.Image}}' "$SLOT_CONTAINER" 2>/dev/null || echo unknown)"
previous_image_id="$(docker inspect --format '{{.Image}}' "$SLOT_CONTAINER" 2>/dev/null || echo unknown)"
info "replacing $previous_image ($previous_image_id)"

# Out of rotation first: the edge stops sending new work before the container
# moves, so the requests still in flight are the ones already accepted.
rotation down "$SLOT_UPSTREAM"
if [ -z "$DRY_RUN" ]; then
    [ "$(in_rotation "$SLOT_UPSTREAM")" = "no" ] || die "$SLOT_UPSTREAM is still in rotation"
fi
ok "$SLOT_UPSTREAM out of rotation"

# The reload is graceful and finishes in-flight requests against the previous
# configuration; the settle window is the time for them to land. It is a window,
# not a guarantee: a client holding a long stream open can outlive it. The cost of
# being wrong is bounded — that request is drained by the SIGTERM below, or
# recovered as `interrupted` if the container is killed past its grace period.
info "settling for ${SETTLE_SECS}s before the container moves"
run sleep "$SETTLE_SECS"

# SIGTERM, with a grace period longer than the container's own stop_grace_period:
# the drain sequence commits every queued ledger record before the process exits,
# and a SIGKILL mid-drain is the one failure this whole product exists to avoid.
run docker stop -t "$STOP_TIMEOUT" "$SLOT_CONTAINER" >/dev/null
if [ -z "$DRY_RUN" ]; then
    if docker logs "$SLOT_CONTAINER" 2>&1 | grep -q "metering pipeline drained and committed"; then
        ok "the old container drained its metering queue on SIGTERM"
    else
        warn "no completed drain in the old container's logs; check whether it was killed mid-drain"
    fi
fi

# Compose recreates the stopped container from the same service definition with
# the new image, so the replacement keeps the deployment's hardening, mounts and
# ports rather than being hand-started with a second, drifting set of flags.
PARTNER_PORTAL_IMAGE="$IMAGE"
export PARTNER_PORTAL_IMAGE
run "${COMPOSE[@]}" up -d --no-deps --force-recreate "portal-${SLOT}" || die "the new container did not start"
ok "the new build is running in slot $SLOT"

# ---------------------------------------------------------------------------
say "5. Health"
# ---------------------------------------------------------------------------
if [ -z "$DRY_RUN" ]; then
    wait_for "$SLOT_URL/healthz" "$READY_TIMEOUT" \
        || {
            docker logs "$SLOT_CONTAINER" 2>&1 | tail -20
            die "the new instance never became healthy; slot $SLOT stays out of rotation"
        }
    fetch "$SLOT_URL/healthz"
    ok "/healthz is 200 (status=$(body | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])'))"
fi

# ---------------------------------------------------------------------------
say "6. Readiness"
# ---------------------------------------------------------------------------
if [ -z "$DRY_RUN" ]; then
    wait_ready "$SLOT_URL" "$READY_TIMEOUT" \
        || {
            docker logs "$SLOT_CONTAINER" 2>&1 | tail -20
            die "the new instance never became ready; slot $SLOT stays out of rotation, and the peer is still serving"
        }
    fetch "$SLOT_URL/readyz"
    ok "ready, ledger_ready, queue depth: $(body | python3 -c 'import json,sys
d = json.load(sys.stdin)
print(d["ready"], d["ledger_ready"], d["ledger_queue_depth"])')"
fi

# ---------------------------------------------------------------------------
say "7. Traffic switch"
# ---------------------------------------------------------------------------
rotation up "$SLOT_UPSTREAM"
if [ -z "$DRY_RUN" ]; then
    wait_for "$EDGE/healthz" 30 || die "the edge did not recover after the switch"
fi
ok "$SLOT_UPSTREAM is back in rotation and the edge is serving"

# ---------------------------------------------------------------------------
say "9. Verify"
# ---------------------------------------------------------------------------
# Through the edge, not against the container: what is being verified is the
# deployment the client sees. Each of these is a real, billed upstream call, which
# is why the count is an option rather than a constant.
if [ "$VERIFY_REQUESTS" -gt 0 ] && [ -z "$DRY_RUN" ]; then
    probe_model="rolling-update-$(date +%s)"
    key="$(partner_key)"
    verified=0
    for i in $(seq 1 "$VERIFY_REQUESTS"); do
        code="$(curl -sS --max-time 60 -o /dev/null -w '%{http_code}' \
            -X POST "$EDGE/v1/chat/completions" \
            -H "Authorization: Bearer $key" \
            -H 'Content-Type: application/json' \
            -d "{\"model\":\"$probe_model\",\"messages\":[{\"role\":\"user\",\"content\":\"rolling update check $i\"}]}")"
        [ "$code" = "200" ] && verified=$((verified + 1))
    done
    [ "$verified" -eq "$VERIFY_REQUESTS" ] \
        || die "$verified of $VERIFY_REQUESTS requests through the edge succeeded"
    ok "$VERIFY_REQUESTS requests through the edge returned 200"

    sleep 2
    rows="$(curl -sS --max-time 10 -H "Authorization: Bearer $key" \
        "$EDGE/api/dashboard/requests?range=1h&limit=100&model=$probe_model" \
        | python3 -c 'import json, sys
rows = json.load(sys.stdin)["data"]
done = sum(1 for r in rows if r["request_status"] == "completed")
print(f"{len(rows)} {done}")')"
    read -r ledger_rows completed_rows <<<"$rows"
    if [ "$ledger_rows" -eq "$VERIFY_REQUESTS" ] && [ "$completed_rows" -eq "$VERIFY_REQUESTS" ]; then
        ok "the ledger recorded all $VERIFY_REQUESTS of them as completed"
    else
        die "the ledger has $ledger_rows row(s) for the probe model, $completed_rows completed; expected $VERIFY_REQUESTS"
    fi
else
    info "no probe requests sent (--verify-requests $VERIFY_REQUESTS)"
fi

# ---------------------------------------------------------------------------
say "10. Cleanup"
# ---------------------------------------------------------------------------
if [ -z "$DRY_RUN" ] && [ -d "$BACKUP_DIR" ]; then
    # Oldest first, beyond the retention count. `ls -t` is the ordering the
    # operator sees in the directory; the files are named with a UTC timestamp, so
    # the two agree.
    pruned=0
    while read -r stale; do
        [ -n "$stale" ] || continue
        rm -f "$BACKUP_DIR/$stale"
        pruned=$((pruned + 1))
    done < <(ls -t "$BACKUP_DIR" 2>/dev/null | tail -n "+$((KEEP_BACKUPS + 1))")
    info "$pruned old backup(s) pruned, $KEEP_BACKUPS retained in $BACKUP_DIR"
fi

printf '\n'
ok "slot $SLOT is running $IMAGE"
info "the peer ($PEER_CONTAINER) is still on its previous build; run this script again with --instance $([ "$SLOT" = a ] && echo b || echo a) to finish the rollout"
info "rollback: ./rolling-update.sh --instance $SLOT --image $previous_image"
info "  (the previous image object is $previous_image_id; nothing was mutated in place)"
exit 0
