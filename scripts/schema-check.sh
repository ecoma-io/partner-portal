#!/usr/bin/env bash
#
# Start a freshly built image against a database that does not exist yet, and
# check that what it creates is what this source tree declares.
#
# There is no migration runner: `src/ledger/schema.sql` is embedded in the binary
# and applied on every startup as an idempotent batch, and the version it stamps
# is `SCHEMA_VERSION` in `src/ledger/mod.rs`. That makes two failures possible and
# both of them silent:
#
#   * a schema file that no longer applies cleanly to an *empty* database. Every
#     other test in the tree starts from a database that some earlier run already
#     created, or from a schema the test itself applied, so this is the only place
#     the first-ever start is exercised.
#   * a version constant that was not raised alongside the schema it describes, or
#     raised without the schema — the ledger records the constant, so a mismatch
#     is a rolling update comparing two numbers that do not describe the same
#     file.
#
# The expected version is read from the source rather than written here, so a
# deliberate bump does not have to be made twice and an accidental one cannot pass
# by being made twice.
#
# Usage:
#   scripts/schema-check.sh [image]         # default: partner-portal:test
#
# Requires: docker, curl (inside the image), python3 (for the source check and the
# SQLite read). Run from the repository root.

set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

IMAGE="${1:-partner-portal:test}"
CONTAINER="partner-portal-schema-check-$$"
VOLUME="partner-portal-schema-check-data-$$"
# Inside the image, and the value its WORKDIR/volume was built around.
DB_PATH="partner-portal.db"
# A fixture, and only ever the fixture: the image is started with a tmpfs-free
# empty volume, so this secret never touches anything that outlives the check.
API_KEY_SECRET="schema-check-secret-not-a-real-one!!"

failures=0
ok() { printf '  ok   %s\n' "$*"; }
bad() {
    printf '  FAIL %s\n' "$*"
    failures=$((failures + 1))
}

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    docker volume rm "$VOLUME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "image $IMAGE not found; build it first:" >&2
    echo "  docker build -t $IMAGE ." >&2
    exit 1
fi

# The version this source tree declares. An empty result is a failure, not a
# skipped check: the pattern and the constant are the same line of code.
expected="$(sed -n 's/^pub const SCHEMA_VERSION: u32 = \([0-9]\+\);/\1/p' src/ledger/mod.rs)"
if [ -z "$expected" ]; then
    echo "could not read SCHEMA_VERSION from src/ledger/mod.rs" >&2
    exit 1
fi
ok "src/ledger/mod.rs declares schema_version $expected"

# The volume is removed first so this is always a first-ever start: a leftover
# with a database in it would make the check pass without exercising anything.
docker volume rm "$VOLUME" >/dev/null 2>&1 || true

# Started with the hardening the deployment applies — read-only root filesystem,
# every capability dropped, no new privileges — because a schema that can only be
# created by a privileged process is a schema the deployment cannot create. The
# database is on the volume, so a read-only root filesystem does not interfere
# with it, which is itself worth knowing.
docker run -d --name "$CONTAINER" \
    --read-only --tmpfs /tmp \
    --cap-drop ALL --security-opt no-new-privileges \
    -v "$(pwd)/deploy/config:/etc/partner-portal:ro" \
    -v "$VOLUME:/var/lib/partner-portal" \
    -e PARTNER_PORTAL_CONFIG=/etc/partner-portal/partner-portal.smoke.yaml \
    -e "PARTNER_PORTAL_API_KEY_SECRET=$API_KEY_SECRET" \
    "$IMAGE" >/dev/null

# Probed with `docker exec` rather than a published port: the image ships curl for
# its healthcheck, this runs as the image's own user, and a check that needs no
# host port cannot collide with a CI job or with a stack another script left up.
started=""
for _ in $(seq 1 60); do
    if docker exec "$CONTAINER" curl -fsS http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
        started=yes
        break
    fi
    if ! docker inspect --format '{{.State.Running}}' "$CONTAINER" | grep -q true; then
        break
    fi
    sleep 1
done
if [ -z "$started" ]; then
    bad "the image did not become healthy against an empty database"
    docker logs "$CONTAINER" 2>&1 | tail -30
    exit 1
fi
ok "the image started against a database that did not exist"

reported="$(docker exec "$CONTAINER" curl -fsS http://127.0.0.1:8080/version \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["schema_version"])')"
if [ "$reported" = "$expected" ]; then
    ok "/version reports schema_version $reported"
else
    bad "/version reports schema_version $reported, the source declares $expected"
fi

# Stopped before the file is read: a running instance is still writing WAL frames,
# and while a read-only connection would see a consistent snapshot, the point here
# is what the file holds once the process that created it is gone — which is the
# state a backup, a restore or a rolling update meets.
docker stop "$CONTAINER" >/dev/null

# `-i` is load-bearing: this reads its program from stdin, and `docker run`
# attaches stdin only when asked, so without it the reader gets an empty program,
# exits 0 and reports nothing — a green check that read no database at all. The
# emptiness guard below is the second half of the same defence.
if ! read_report="$(docker run --rm -i -v "$VOLUME:/data" python:3.13-alpine python - "$DB_PATH" <<'PY'
import sqlite3, sys

conn = sqlite3.connect(f"file:/data/{sys.argv[1]}?mode=ro", uri=True, timeout=10)
tables = {
    row[0]
    for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
}
missing = [t for t in ("api_keys", "usage_records", "usage_hourly", "ledger_meta") if t not in tables]
if missing:
    sys.exit(f"a fresh database is missing {missing}; it has {sorted(tables)}")

row = conn.execute("SELECT value FROM ledger_meta WHERE key = ?", ("schema_version",)).fetchone()
print(row[0] if row else "unset")
PY
)"; then
    bad "the database could not be read from a second container"
    exit 1
fi
if [ -z "$read_report" ]; then
    bad "the reader produced no output, so nothing about the file was checked"
    exit 1
fi

if [ "$read_report" = "$expected" ]; then
    ok "the database holds api_keys, usage_records, usage_hourly and ledger_meta"
    ok "ledger_meta records schema_version $read_report"
else
    bad "a fresh database reports '$read_report' for ledger_meta.schema_version; expected $expected"
fi

if [ "$failures" -eq 0 ]; then
    printf '\nall checks passed\n'
    exit 0
fi
printf '\n%s check(s) failed\n' "$failures"
exit 1
