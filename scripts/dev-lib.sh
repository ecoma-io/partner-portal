#!/bin/sh
#
# dev-lib.sh — process management shared by dev-up.sh, dev-down.sh and
# dev-restart.sh. Sourced, never executed:
#
#   . "$(dirname "$0")/dev-lib.sh"
#
# It exists because none of this is ten lines, and all of it has to be
# identical in every script that touches a process. Three copies of the group
# handling would be three places to get it wrong, and two of them already were:
# dash rejects a `--` marker before a group id, and a signal written without its
# dash is read as a pid — so it signals nothing and the script believes it
# succeeded.
#
# What it knows, all of it learned by running it:
#
#   * Every process gets its own process group and the *group* is what gets
#     recorded. `pnpm --dir dashboard dev` is a chain — pnpm → sh → node/vite →
#     esbuild — whose leader exits within a second while four members go on
#     holding port 5173. Keying on the pid sees a corpse, skips it, and leaves
#     the port bound, so the next dev-up refuses to start.
#   * A group is signalled with the shell's own builtin, as `kill -TERM -<gid>`.
#     The setuid /bin/kill from coreutils fails silently under a sandbox, and
#     dash rejects `kill -TERM -- -<gid>` with "Illegal number: -".
#   * Where setsid is missing there is no separate group, so the pid is
#     recorded instead and marked `p`. Signalling a pid that is really this
#     script's own group would take the script down with it.
#
# The pids file is the whole inventory. One line per process:
#
#     <id> <g|p> <name>          id: the group, or the pid when mode is p
#                               g: signal the process group
#                               p: signal the single pid
#
# Nothing here matches on a command line. A `partner-portal` you started by
# hand is not this script's business, and a stale line whose process is gone is
# simply cleaned up.

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd) || return 1
cd "$REPO_ROOT" || return 1

RUN_DIR="$REPO_ROOT/target/dev"
LOG_DIR="$RUN_DIR/logs"
PIDS_FILE="$RUN_DIR/pids"
DEV_CONFIG="dev/partner-portal.dev.yaml"
STUB="dev/mock-upstream-dev.py"

STUB_PORT="${DEV_STUB_PORT:-9100}"
PROXY_PORT="${DEV_PROXY_PORT:-8080}"
DASHBOARD_PORT="${DEV_DASHBOARD_PORT:-5173}"

# Sourced scripts share $0 with their caller, so the messages say which script
# the developer actually ran.
SCRIPT_NAME=$(basename "$0")

# setsid is Linux, not POSIX. Where it is missing the group behaviour degrades
# to signalling one pid, which is enough for the stub and the proxy (both of
# which exec into their real program) and not enough for pnpm.
HAVE_SETSID=0
command -v setsid >/dev/null 2>&1 && HAVE_SETSID=1

# --- signals and liveness ----------------------------------------------------

# group_signal <signal> <gid>
group_signal() {
  kill "-$1" "-$2" 2>/dev/null || kill "-$1" "$2" 2>/dev/null
}

# group_alive <gid> — liveness is a question about the group, never the bare
# id. After a group's leader exits no process has that pid, so `kill -0 <gid>`
# answers "gone" while four members are still running.
group_alive() {
  kill -0 "-$1" 2>/dev/null || kill -0 "$1" 2>/dev/null
}

# target_signal <mode> <id> <signal>
target_signal() {
  if [ "$1" = "g" ]; then
    group_signal "$3" "$2"
  else
    kill "-$3" "$2" 2>/dev/null
  fi
}

# target_alive <mode> <id>
target_alive() {
  if [ "$1" = "g" ]; then
    group_alive "$2"
  else
    kill -0 "$2" 2>/dev/null
  fi
}

# port_busy <port> — `ss` is not present everywhere; where it is missing the
# bind attempt is the real check and this just reports "free".
port_busy() {
  command -v ss >/dev/null 2>&1 || return 1
  ss -ltn "sport = :$1" 2>/dev/null | tail -n +2 | grep -q .
}

# --- the inventory -----------------------------------------------------------

# pids_list — one name per line, in start order.
pids_list() {
  [ -f "$PIDS_FILE" ] || return 0
  awk 'NF >= 3 { print $3 }' "$PIDS_FILE"
}

# pids_alive — the names of the recorded processes that are actually still
# running. Liveness has to be asked about the group, not the bare id, so this
# is the same question stop_by_name would ask; a file full of names alone
# cannot distinguish a live stack from a crashed one.
pids_alive() {
  [ -f "$PIDS_FILE" ] || return 0
  while read -r id mode name; do
    [ -n "${id:-}" ] || continue
    target_alive "$mode" "$id" && echo "$name"
  done < "$PIDS_FILE"
}

# pids_entry <name> — "<id> <mode>" for a named process, or nothing.
pids_entry() {
  [ -f "$PIDS_FILE" ] || return 1
  awk -v n="$1" '$3 == n { print $1, $2; exit }' "$PIDS_FILE"
}

# pids_forget <name> — drop a line, so the next dev-down does not chase a
# corpse. The rewrite goes through a temporary file because a half-written
# pids file is an inventory that lies about what is running.
pids_forget() {
  [ -f "$PIDS_FILE" ] || return 0
  _pf_tmp="$PIDS_FILE.tmp.$$"
  awk -v n="$1" '$3 != n' "$PIDS_FILE" > "$_pf_tmp" 2>/dev/null
  mv "$_pf_tmp" "$PIDS_FILE"
}

# --- starting ----------------------------------------------------------------

# log_mark <name> — a timestamped divider, so a restart does not silently
# append to the previous run's log and leave the two indistinguishable.
log_mark() {
  printf '\n=== %s: %s restarted at %s ===\n' \
    "$1" "$SCRIPT_NAME" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >> "$LOG_DIR/$2.log"
}

# spawn <name> <logfile> <command...> — start a command in its own process
# group, append its output to <logfile>, and record it. Echoes "<id> <mode>".
#
# With setsid the process leads its own group, so the pid it gets is already
# the group id and there is nothing to look up. Without it, the child shares
# this script's group and the pid is the only usable handle — which is why the
# mode is recorded alongside it.
spawn() {
  _sp_name="$1"
  _sp_log="$2"
  shift 2
  if [ "$HAVE_SETSID" = "1" ]; then
    setsid "$@" >> "$_sp_log" 2>&1 &
    _sp_mode=g
  else
    "$@" >> "$_sp_log" 2>&1 &
    _sp_mode=p
  fi
  _sp_pid=$!
  echo "$_sp_pid $_sp_mode $_sp_name" >> "$PIDS_FILE"
  echo "$_sp_pid $_sp_mode"
}

# wait_for_url <url> <mode> <id> <tenths> — poll rather than sleep, so a fast
# machine is not made to wait and a slow one is not raced. Exits early when
# the process dies: a thing that exited will never answer.
wait_for_url() {
  _wf_url="$1"
  _wf_mode="$2"
  _wf_id="$3"
  _wf_tries="$4"
  _wf_i=0
  while [ "$_wf_i" -lt "$_wf_tries" ]; do
    if curl -fsS --max-time 1 "$_wf_url" >/dev/null 2>&1; then
      return 0
    fi
    target_alive "$_wf_mode" "$_wf_id" || return 1
    _wf_i=$((_wf_i + 1))
    sleep 0.25
  done
  return 1
}

# start_stub — the dev upstream on 9100.
start_stub() {
  set -- $(spawn stub "$LOG_DIR/stub.log" python3 "$STUB" --port "$STUB_PORT") || return 1
  if wait_for_url "http://127.0.0.1:$STUB_PORT/healthz" "$2" "$1" 40; then
    return 0
  fi
  echo "$SCRIPT_NAME: the stub never came up on $STUB_PORT. Log:" >&2
  tail -20 "$LOG_DIR/stub.log" >&2
  stop_by_name stub
  return 1
}

# start_proxy — a debug `cargo run`, not the release binary. The loop is the
# point: the release profile's thin LTO is the right trade for a shipping
# artifact and the wrong one between two saves. Run from the repository root
# so the relative database path in the dev config lands in target/dev/.
#
# The 240-tenth timeout is generous because the first run compiles the crate.
# The wait exits as soon as the process dies, so a build failure surfaces in
# seconds rather than after the full timeout.
start_proxy() {
  PARTNER_PORTAL_CONFIG="$DEV_CONFIG"
  export PARTNER_PORTAL_CONFIG
  # The listen address is an environment property, not a config field
  # (src/config/listen.rs): the dev proxy binds loopback only, and the port is
  # stated here, next to the health probe below, rather than twice.
  PARTNER_PORTAL_LISTEN="127.0.0.1:$PROXY_PORT"
  export PARTNER_PORTAL_LISTEN
  set -- $(spawn proxy "$LOG_DIR/proxy.log" cargo run --quiet --bin partner-portal) || return 1
  if wait_for_url "http://127.0.0.1:$PROXY_PORT/healthz" "$2" "$1" 240; then
    return 0
  fi
  echo "$SCRIPT_NAME: the proxy never became healthy. Last lines of its log:" >&2
  echo >&2
  tail -30 "$LOG_DIR/proxy.log" >&2
  echo >&2
  echo "  (a first run compiles the crate — run it again once that is cached)" >&2
  stop_by_name proxy
  return 1
}

# start_dashboard — the vite dev server, where a .vue edit is an HMR update
# rather than a restart.
#
# Probed on `localhost`, not 127.0.0.1: vite binds the IPv6 loopback
# ([::1]:5173) on this machine, and a probe to 127.0.0.1 never reaches it. The
# report at the end says `localhost` for the same reason — it is the name that
# works in a browser.
start_dashboard() {
  set -- $(spawn dashboard "$LOG_DIR/dashboard.log" pnpm --dir dashboard dev --port "$DASHBOARD_PORT") || return 1
  if wait_for_url "http://localhost:$DASHBOARD_PORT/" "$2" "$1" 60; then
    return 0
  fi
  echo "$SCRIPT_NAME: vite did not come up on $DASHBOARD_PORT. Log:" >&2
  tail -20 "$LOG_DIR/dashboard.log" >&2
  stop_by_name dashboard
  return 1
}

# dashboard_port — vite moves the port itself when the requested one is taken,
# so the port is read back out of its log rather than reported from the request.
dashboard_port() {
  _dp=$(sed -n 's|.*localhost:\([0-9][0-9]*\).*|\1|p' "$LOG_DIR/dashboard.log" 2>/dev/null | head -1)
  [ -n "$_dp" ] || _dp="$DASHBOARD_PORT"
  echo "$_dp"
}

# --- stopping ----------------------------------------------------------------

# stop_by_name <name> — signal one recorded process and wait for it to go.
#
# The proxy's shutdown is a real sequence — readiness fails, accepted work
# finishes, the queue commits, the database closes — and a SIGKILL partway
# through would discard metered usage, which is precisely the failure this
# product exists to avoid. It therefore gets 15 seconds, which covers the
# configured shutdown_grace_secs of 5 plus the drain itself. A process that
# needs more is a process that is wedged, and the report says so.
stop_by_name() {
  _sb_name="$1"
  _sb_line=$(pids_entry "$_sb_name") || return 0
  [ -n "$_sb_line" ] || return 0
  set -- $_sb_line
  _sb_id="$1"
  _sb_mode="$2"

  if ! target_alive "$_sb_mode" "$_sb_id"; then
    pids_forget "$_sb_name"
    return 0
  fi

  target_signal "$_sb_mode" "$_sb_id" TERM

  _sb_i=0
  while [ "$_sb_i" -lt 60 ]; do
    target_alive "$_sb_mode" "$_sb_id" || break
    _sb_i=$((_sb_i + 1))
    sleep 0.25
  done

  if target_alive "$_sb_mode" "$_sb_id"; then
    echo "$SCRIPT_NAME: $_sb_name (group $_sb_id) did not stop on SIGTERM; sending SIGKILL." >&2
    target_signal "$_sb_mode" "$_sb_id" KILL
  fi

  pids_forget "$_sb_name"
  return 0
}
