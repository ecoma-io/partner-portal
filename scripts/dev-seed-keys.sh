#!/bin/sh
#
# dev-seed-keys.sh — write the dev loop's partner API keys into the dev ledger.
#
# Why this exists at all: partner API keys are rows in SQLite (ADR 0014), not
# lines in `dev/partner-portal.dev.yaml`. So a dev loop that wants a key you can
# *type* has to create one, and the only way to register a memorable plaintext
# rather than a generated one is `partner-portal keygen --plaintext`. This
# script is that call, three times, for the three keys the loop is documented
# to have.
#
# It runs before the proxy starts — `keygen` applies the schema itself, so the
# database does not have to exist yet — which means the proxy's first snapshot
# already contains the keys and no login ever races the seed. `dev-up.sh` calls
# it; running it by hand is equally fine and is a no-op for keys already there.
#
# POSIX sh, like every other script here. It is *executed*, not sourced: unlike
# `dev-lib.sh` it does not manage processes, it just uses the shared variable
# definitions.

set -u

usage() {
  cat <<'EOF'
Usage: scripts/dev-seed-keys.sh

Writes the three dev keys (dev-key, dev-key-2 for consumer 'acme' and
dev-key-beta for consumer 'beta') into target/dev/partner-portal-dev.db, unless
they are already there. Prints what it wrote.

The database and the hashing secret are the ones scripts/dev-lib.sh defines, so
the seed and the proxy always agree. No key ever appears in a YAML file.
EOF
}

. "$(dirname "$0")/dev-lib.sh" || exit 1

case "${1:-}" in
  -h|--help) usage; exit 0 ;;
  "") ;;
  *) echo "$SCRIPT_NAME: unknown argument: $1" >&2; usage >&2; exit 2 ;;
esac

mkdir -p "$RUN_DIR"

if [ ! -f "$DEV_CONFIG" ]; then
  echo "$SCRIPT_NAME: missing $DEV_CONFIG — run this from the repository root" >&2
  exit 1
fi

# key_state <plaintext> — prints one of:
#
#   active    a live key with this plaintext's prefix is already in the ledger
#   inactive  the row exists but is not active (revoked, or expired)
#   absent    nothing matches; the key can be issued
#
# A read-only connection, and a missing file or table is `absent` rather than an
# error: on the first run neither exists, and "nothing there yet" is exactly the
# state that means "seed it".
key_state() {
  python3 - "$DEV_DB" "$1" <<'PY' 2>/dev/null
import sqlite3, sys

path, plaintext = sys.argv[1], sys.argv[2]
prefix = plaintext[:12]

try:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    row = conn.execute(
        "SELECT status FROM api_keys WHERE key_prefix = ? LIMIT 1", (prefix,)
    ).fetchone()
except sqlite3.Error:
    print("absent")
    sys.exit(0)

if row is None:
    print("absent")
elif row[0] == "active":
    print("active")
else:
    print("inactive")
PY
}

# seed <plaintext> <name> <consumer_id>
#
# The plaintext goes on the command line, which for a *dev* key is the point:
# these three strings are published in this file, in `dev-load.sh`'s defaults and
# in the `dev-up.sh` banner. A real deployment's first key is issued with
# `--plaintext -` so the value never reaches the process table.
seed() {
  _seed_key="$1"
  _seed_name="$2"
  _seed_consumer="$3"

  # One --allowed-model per model, because the flag is repeatable and the
  # allow-list is a list rather than a string.
  _seed_models=""
  for _m in $DEV_MODELS; do
    _seed_models="$_seed_models --allowed-model $_m"
  done

  # stderr is captured and stdout discarded: `keygen` prints the plaintext on
  # stdout (which is the one we already know) and its report on stderr, and the
  # report is noise here — this script prints its own summary. It is echoed back
  # only when the command fails, because then the report is the explanation.
  _seed_out=$(PARTNER_PORTAL_CONFIG="$DEV_CONFIG" \
    cargo run --quiet --bin partner-portal -- keygen \
      --name "$_seed_name" \
      --consumer-id "$_seed_consumer" \
      --plaintext "$_seed_key" \
      $_seed_models 2>&1 >/dev/null) || {
        echo "$SCRIPT_NAME: could not issue $_seed_name" >&2
        printf '%s\n' "$_seed_out" >&2
        return 1
      }
}

# seed_one <plaintext> <name> <consumer_id>
seed_one() {
  _state=$(key_state "$1")

  case "$_state" in
    active)
      echo "  = $1  already in the ledger, left alone"
      ;;
    inactive)
      # Re-issuing the same plaintext would collide with the existing row's
      # hash, and a UNIQUE failure would be the confusing message. Say what
      # happened and what to do instead.
      echo >&2
      echo "$SCRIPT_NAME: $1 exists but is not active (revoked or expired)." >&2
      echo "  Its plaintext cannot be re-issued — the stored hash is the same." >&2
      echo "  Start clean with:  scripts/dev-up.sh --reset" >&2
      return 1
      ;;
    absent)
      if seed "$1" "$2" "$3"; then
        echo "  + $1  issued as '$2' for consumer '$3'"
      else
        return 1
      fi
      ;;
    *)
      # An unreadable state is *not* "nothing there". Seeding on top of an
      # unknown state is how a second run turns into a UNIQUE failure whose
      # message says nothing about the cause.
      echo "$SCRIPT_NAME: could not read the ledger state of $1" >&2
      echo "  (python3 is needed to read $DEV_DB before writing to it)" >&2
      return 1
      ;;
  esac
}

echo "dev keys in $DEV_DB"
seed_one "$DEV_KEY" "dev-acme" "acme" || exit 1
seed_one "$DEV_KEY_2" "dev-acme-rotated" "acme" || exit 1
seed_one "$DEV_BETA_KEY" "dev-beta" "beta" || exit 1
echo
echo "  log in at the dashboard with any of them, or '$DEV_KEY' for scripts/dev-load.sh."
