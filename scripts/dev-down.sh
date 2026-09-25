#!/bin/sh
#
# dev-down.sh — stop everything scripts/dev-up.sh started, and nothing else.
#
# The pids file is the inventory; dev-lib.sh does the signalling. Nothing here
# matches on a command line, so a `partner-portal` you started by hand for
# something else is not this script's business.
#
# Order is reverse start order on purpose. The stub goes before the proxy, so
# the proxy is never in a position to accept a request it cannot forward, and
# the proxy goes before the dashboard, so nothing is still sending it API
# calls while it drains.
#
# Usage:
#   scripts/dev-down.sh              # stop the stack
#   scripts/dev-down.sh --purge      # stop it and delete the dev ledger
#
# The ledger is kept by default. --purge deletes it, which is the one way to
# start a dev session from an empty dashboard.

set -u

usage() {
  cat <<'EOF'
Usage: scripts/dev-down.sh [--purge]

Stops the processes scripts/dev-up.sh started, by the process group it recorded.

  --purge   also delete the dev ledger (target/dev/partner-portal-dev.db)
EOF
}

. "$(dirname "$0")/dev-lib.sh" || exit 1

PURGE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --purge) PURGE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [ ! -f "$PIDS_FILE" ]; then
  echo "dev-down: the stack is not running."
  if [ "$PURGE" = "1" ]; then
    rm -f "$RUN_DIR"/partner-portal-dev.db*
    echo "dev-down: purged the dev ledger."
  fi
  exit 0
fi

# Anything named here is started by name, and a name that is not running is
# not an error — dev-restart.sh removes the proxy's line on every restart, so
# "not running" is the normal state for a proxy that is already down.
stopped=0
for name in dashboard proxy stub; do
  if pids_entry "$name" >/dev/null 2>&1; then
    stop_by_name "$name"
    stopped=$((stopped + 1))
  fi
done

rm -f "$PIDS_FILE"

# A port still bound after this means something outlived its group, which is
# worth saying out loud: the next dev-up will refuse to start because of it.
for p in "$PROXY_PORT" "$STUB_PORT" "$DASHBOARD_PORT"; do
  if port_busy "$p"; then
    echo "dev-down: port $p is still bound after stopping $stopped process(es)." >&2
    echo "  If that is not yours, find it with: ss -ltnp \"sport = :$p\"" >&2
  fi
done

if [ "$PURGE" = "1" ]; then
  rm -f "$RUN_DIR"/partner-portal-dev.db*
  echo "dev-down: purged the dev ledger."
fi

if [ "$stopped" = "0" ]; then
  echo "dev-down: the stack is not running."
fi

exit 0
