#!/bin/sh
#
# dev-restart.sh — restart the proxy, and nothing else.
#
# This is the loop's inner turn, and it is why it exists as its own script
# rather than a flag on dev-up.sh. The three processes have genuinely different
# lifetimes:
#
#   proxy      changes when you edit Rust. It has to stop, drain and come back.
#   dashboard  changes when you edit a .vue. Vite has already done that — HMR
#              pushes it to the browser, no process involved. Restarting vite
#              would throw away a warm dependency graph to achieve nothing.
#   stub       has no source to edit. It is a fixture.
#
# So this stops the proxy the way production does — SIGTERM, readiness down,
# accepted work finished, metering committed, database closed — and starts it
# again. A `--hard` flag additionally replaces the stub, which is only useful
# after editing dev/mock-upstream-dev.py itself.
#
# The ledger is not touched. Usage you generated is usage, and a restart that
# wiped it would quietly invalidate whatever you were looking at.
#
# Usage:
#   scripts/dev-restart.sh              # restart the proxy (the common case)
#   scripts/dev-restart.sh --stub       # also replace the mock upstream
#   scripts/dev-restart.sh --ledger     # print the ledger summary afterwards
#
# Ctrl-C at the prompt while it waits leaves the proxy down and the other two
# running; scripts/dev-up.sh brings it back.

set -u

usage() {
  cat <<'EOF'
Usage: scripts/dev-restart.sh [--stub] [--ledger]

Restarts the dev proxy, draining it properly. The vite dev server and the mock
upstream are left alone — HMR already handles .vue edits, and the stub has no
source of its own to reload.

  --stub     also replace the mock upstream (use after editing the fixture)
  --ledger   print a one-line ledger summary when it comes back up
EOF
}

. "$(dirname "$0")/dev-lib.sh" || exit 1

WITH_STUB=0
WITH_LEDGER=0
while [ $# -gt 0 ]; do
  case "$1" in
    --stub) WITH_STUB=1; shift ;;
    --ledger) WITH_LEDGER=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [ ! -f "$PIDS_FILE" ] || [ -z "$(pids_alive)" ]; then
  echo "dev-restart: the stack is not running." >&2
  echo "  scripts/dev-up.sh" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"

# --- down -------------------------------------------------------------------

# The proxy's drain takes about as long as its configured shutdown_grace_secs:
# readiness fails first so a balancer would stop sending, then accepted work
# finishes, then the queue commits. dev-lib.sh's stop_by_name waits for that
# rather than assuming it.
if pids_entry proxy >/dev/null 2>&1; then
  echo "dev-restart: stopping the proxy (readiness down, draining)…"
  stop_by_name proxy
  echo "dev-restart: proxy stopped."
else
  echo "dev-restart: no proxy was running; starting one."
fi

if [ "$WITH_STUB" = "1" ]; then
  echo "dev-restart: replacing the mock upstream…"
  stop_by_name stub
fi

# --- up ---------------------------------------------------------------------

echo "dev-restart: building and starting…"

if [ "$WITH_STUB" = "1" ]; then
  if ! start_stub; then
    exit 1
  fi
fi

if ! start_proxy; then
  exit 1
fi

# The dashboard is checked, not started. A restart is not the moment to notice
# that vite died three edits ago, but it should still say so rather than leave
# a developer wondering why their HMR stopped working.
if pids_entry dashboard >/dev/null 2>&1 && ! port_busy "$DASHBOARD_PORT"; then
  echo "dev-restart: the vite dev server is not answering on $DASHBOARD_PORT." >&2
  echo "  scripts/dev-up.sh starts it. Your .vue changes will not hot-reload." >&2
fi

echo
echo "  proxy back on http://127.0.0.1:$PROXY_PORT/"
if pids_entry dashboard >/dev/null 2>&1; then
  echo "  dashboard still on http://127.0.0.1:$(dashboard_port)/   (untouched)"
fi
echo "  log   tail -f $LOG_DIR/proxy.log"
echo

if [ "$WITH_LEDGER" = "1" ]; then
  # Read the SQLite file rather than the dashboard API: the API is not a
  # faithful view of every column — NULL versus 0, in_flight rows — and this
  # is the same rule the integration tests follow.
  #
  # Python rather than the sqlite3 CLI, which is not installed everywhere. The
  # stub already requires python3, so this adds no dependency to the loop.
  db="$RUN_DIR/partner-portal-dev.db"
  if [ -f "$db" ]; then
    python3 - "$db" <<'PY' 2>/dev/null || echo "  ledger   (could not read $db)"
import sqlite3, sys

db = sqlite3.connect("file:" + sys.argv[1] + "?mode=ro", uri=True)
rows = db.execute(
    "SELECT request_status, usage_status, COUNT(*) FROM usage_records "
    "GROUP BY request_status, usage_status ORDER BY 3 DESC"
).fetchall()
print("  ledger   " + "  ".join(f"{r}:{u}={n}" for r, u, n in rows))
PY
  fi
fi

exit 0
