#!/bin/sh
#
# dev-up.sh — start the three processes a dev loop needs, and stop being the
# thing you have to remember.
#
#   1. dev/mock-upstream-dev.py   the stub upstream, on 9100
#   2. the proxy                   a native binary, on 8080
#   3. the dashboard               vite dev server, on 5173
#
# Native, not containerised, and that is the whole point. A Docker-based dev
# loop pays for its isolation on every source change: the dashboard is baked
# into the image at compile time (the Dockerfile copies dashboard/dist into the
# binary), and the release profile is `lto = "thin"` with `codegen-units = 1`,
# so a one-line Rust edit costs a full image rebuild measured in minutes. Here,
# a Rust edit is a restart (scripts/dev-restart.sh), and a `.vue` edit is an
# HMR update — no build between the edit and the browser, and no `docker
# build` anywhere in the loop.
#
# What this gives up, deliberately: the container's own hardening (read-only
# root, capabilities dropped) and the multi-instance topology. Those are what
# `deploy/` and `.github/workflows/ci.yml` are for, and neither needs to be on
# the path between a keystroke and a screenshot.
#
# The ledger is kept at target/dev/partner-portal-dev.db and is *not* reset:
# the load script is meant to build up a history worth paging through. Pass
# --reset to start clean.
#
# POSIX sh, not bash: /bin/sh is dash on Debian, and a script that only runs
# under bash is a script that only runs on the machine where it was written.
#
# Usage:
#   scripts/dev-up.sh                   # start everything, return the prompt
#   scripts/dev-up.sh --reset           # wipe the dev ledger first
#   scripts/dev-up.sh --no-dashboard    # skip the vite dev server
#   scripts/dev-up.sh --foreground      # stay attached; Ctrl-C stops everything
#
# Then, in another terminal:  scripts/dev-load.sh
# Restart the proxy:         scripts/dev-restart.sh
# Stop everything:           scripts/dev-down.sh

set -u

usage() {
  cat <<'EOF'
Usage: scripts/dev-up.sh [--reset] [--no-dashboard] [--foreground]

Starts the dev stack: the stub upstream, the proxy, and the vite dev server.
All native, no image build. Waits for each to answer before starting the next.

  --reset          delete the dev ledger first (target/dev/partner-portal-dev.db)
  --no-dashboard   do not start vite; the built bundle is still served at :8080
  --foreground     stay attached; Ctrl-C stops the whole stack
EOF
}

. "$(dirname "$0")/dev-lib.sh" || exit 1

RESET=0
WITH_DASHBOARD=1
FOREGROUND=0

while [ $# -gt 0 ]; do
  case "$1" in
    --reset) RESET=1; shift ;;
    --no-dashboard) WITH_DASHBOARD=0; shift ;;
    --foreground) FOREGROUND=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# --- preflight --------------------------------------------------------------

for f in "$DEV_CONFIG" "$STUB"; do
  if [ ! -f "$f" ]; then
    echo "missing $f — this repository is missing its dev fixtures" >&2
    exit 1
  fi
done

# Refuse to start a second copy rather than racing the first for the ports. A
# dev loop that quietly runs two proxies against one ledger is worse than one
# that refuses.
if [ -f "$PIDS_FILE" ]; then
  running=$(pids_alive | tr '\n' ' ')
  if [ -n "$running" ]; then
    echo "dev-up: already running: $running" >&2
    echo "  scripts/dev-down.sh   stop it" >&2
    echo "  scripts/dev-restart.sh  just restart the proxy" >&2
    exit 1
  fi
  # Every entry named a process that is gone, so the file is just cleanup.
  rm -f "$PIDS_FILE"
fi

for p in "$STUB_PORT" "$PROXY_PORT"; do
  if port_busy "$p"; then
    echo "dev-up: port $p is already in use." >&2
    echo "  Something else holds it, or a previous run did not stop cleanly." >&2
    exit 1
  fi
done

if ! command -v python3 >/dev/null 2>&1; then
  echo "dev-up: python3 is required for the stub" >&2
  exit 1
fi

# The dashboard dev server is a convenience; its absence must not block the
# other two, because the proxy also serves the built bundle at /.
if [ "$WITH_DASHBOARD" = "1" ]; then
  if ! command -v pnpm >/dev/null 2>&1; then
    echo "dev-up: pnpm not found — starting without the vite dev server." >&2
    echo "  The dashboard is still at http://127.0.0.1:$PROXY_PORT/ (built bundle)." >&2
    WITH_DASHBOARD=0
  elif [ ! -d dashboard/node_modules ]; then
    echo "dev-up: dashboard/node_modules is missing — running pnpm install once." >&2
    pnpm --dir dashboard install || { echo "dev-up: install failed" >&2; exit 1; }
  fi
fi

mkdir -p "$LOG_DIR"
if [ "$RESET" = "1" ]; then
  rm -f "$RUN_DIR"/partner-portal-dev.db*
fi

: > "$PIDS_FILE"

# --- the stack, in order -----------------------------------------------------

# Each is started only once the previous one answers, so a failure names the
# thing that failed rather than surfacing later as a confusing 502.

if ! start_stub; then
  "$REPO_ROOT/scripts/dev-down.sh" >/dev/null 2>&1
  exit 1
fi

if ! start_proxy; then
  "$REPO_ROOT/scripts/dev-down.sh" >/dev/null 2>&1
  exit 1
fi

dashboard_started=0
if [ "$WITH_DASHBOARD" = "1" ]; then
  if start_dashboard; then
    dashboard_started=1
  else
    # Not fatal: the other two are up and the proxy serves the built bundle.
    echo "  The other two are running; the built bundle is at :$PROXY_PORT/" >&2
  fi
fi

# --- report -----------------------------------------------------------------

echo
echo "  dev stack up"
echo
if [ "$dashboard_started" = "1" ]; then
  echo "    dashboard   http://127.0.0.1:$(dashboard_port)/       (vite, HMR — edit a .vue and it reloads)"
fi
echo "    proxy       http://127.0.0.1:$PROXY_PORT/        (the built bundle, same API)"
echo "    stub        http://127.0.0.1:$STUB_PORT/        (the upstream; x-dev-fail to make it fail)"
echo
echo "    log in with"
echo "      dev-key         consumer 'acme'        (or dev-key-2 — same view, by design)"
echo "      dev-key-beta    consumer 'beta'"
echo "      dev-manager     every consumer        (ADR 0013)"
echo
echo "    load        scripts/dev-load.sh"
echo "    restart     scripts/dev-restart.sh     (after a Rust edit; the dashboard keeps HMR)"
echo "    logs        tail -f $LOG_DIR/proxy.log"
echo "    stop        scripts/dev-down.sh"
echo

if [ "$FOREGROUND" = "1" ]; then
  echo "  --foreground: staying attached. Ctrl-C stops everything."
  echo
  trap '"$REPO_ROOT/scripts/dev-down.sh"; exit 0' INT TERM
  wait
else
  echo "  detached. Watch with: tail -f $LOG_DIR/*.log"
fi
