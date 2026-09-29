#!/bin/sh
#
# dev-seed-keys.sh — write the dev loop's partners and API keys into the dev
# ledger.
#
# Why this exists at all: partner API keys are rows in SQLite (ADR 0014), not
# lines in `dev/partner-portal.dev.yaml`. So a dev loop that wants a key you can
# *type* has to create one, and the only way to register a memorable plaintext
# rather than a generated one is `partner-portal keygen --plaintext`. This
# script is that call, once per partner, for the two keys the loop is documented
# to have.
#
# One key per partner, not one per consumer plus a spare: `keygen` writes the
# partner record before the key, and a second run for a consumer that already
# has one is refused — which is the same fact the database enforces with
# `idx_api_keys_one_active_per_consumer`. Rotation is a revoke-then-issue, and
# nothing in a dev loop needs to demonstrate it.
#
# The partner record is the other half of this script's job, and the reason it
# passes `--model` at all: `partner_models` is the single authority for which
# models a partner may call *and* what each costs (ADR 0015). A dev loop with
# keys but no prices has partners whose requests are refused, or — worse, if the
# prices were invented as zero — partners who are served for free, which is a
# bug the local loop would then teach nobody about.
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

Writes the two dev partners and their keys (dev-key for consumer 'acme' on an
invoice account, dev-key-beta for consumer 'beta' on a reconciliation one) into
target/dev/partner-portal-dev.db, unless they are already there. Prints what it
wrote.

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

# model_count <consumer_id> — how many models the partner may call. `-` when the
# table or the partner row is not there at all, which is what a database the
# billing layer has never opened looks like.
#
# Read for one reason: an *active* key is the one state this script leaves
# alone, and a key that predates `partner_models` is active and unusable —
# every request through it is refused, because a model with no price is a model
# the partner cannot call (ADR 0015). Silence there would look like the seed
# having done its job.
model_count() {
  python3 - "$DEV_DB" "$1" <<'PY' 2>/dev/null
import sqlite3, sys

path, consumer = sys.argv[1], sys.argv[2]

try:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    row = conn.execute(
        "SELECT COUNT(*) FROM partner_models WHERE consumer_id = ?", (consumer,)
    ).fetchone()
except sqlite3.Error:
    print("-")
    sys.exit(0)

print(row[0] if row else "-")
PY
}

# seed <plaintext> <name> <consumer_id> <billing_mode>
#
# The plaintext goes on the command line, which for a *dev* key is the point:
# these two strings are published in this file, in `dev-load.sh`'s defaults and
# in the `dev-up.sh` banner. A real deployment's first key is issued with
# `--plaintext -` so the value never reaches the process table.
seed() {
  _seed_key="$1"
  _seed_name="$2"
  _seed_consumer="$3"
  _seed_mode="$4"

  # One --model per model, because the flag is repeatable and a model without
  # one is a model the partner cannot call.
  _seed_models=""
  for _m in $DEV_MODELS; do
    _seed_models="$_seed_models --model $_m"
  done

  # stderr is captured and stdout discarded: `keygen` prints the plaintext on
  # stdout (which is the one we already know) and its report on stderr, and the
  # report is noise here — this script prints its own summary. It is echoed back
  # only when the command fails, because then the report is the explanation.
  #
  # No --billing-email: the dev loop does not send mail, and the dev config has
  # `billing.email.enabled: false` for the same reason. A partner with no
  # address still gets its statements issued and filed — which is the behaviour
  # worth having locally, since it is the one an operator meets on the day the
  # relay is down.
  _seed_out=$(PARTNER_PORTAL_CONFIG="$DEV_CONFIG" \
    cargo run --quiet --bin partner-portal -- keygen \
      --name "$_seed_name" \
      --consumer-id "$_seed_consumer" \
      --billing-mode "$_seed_mode" \
      --plaintext "$_seed_key" \
      $_seed_models 2>&1 >/dev/null) || {
        echo "$SCRIPT_NAME: could not issue $_seed_name" >&2
        printf '%s\n' "$_seed_out" >&2
        return 1
      }
}

# seed_one <plaintext> <name> <consumer_id> <billing_mode>
seed_one() {
  _state=$(key_state "$1")

  case "$_state" in
    active)
      echo "  = $1  already in the ledger, left alone"
      # ...but "left alone" only helps if what is there is usable. A key issued
      # before the billing layer exists in a database whose `partner_models` is
      # empty for this consumer, and an empty price list means every request is
      # refused. The seed is the only place that can say so before the loop
      # starts and the 403s look like a bug in the proxy.
      _count=$(model_count "$3")
      if [ "$_count" = "0" ] || [ "$_count" = "-" ]; then
        echo >&2
        echo "$SCRIPT_NAME: consumer '$3' has no priced models, so '$1' can call" >&2
        echo "  nothing — a model with no price is a model the partner cannot call" >&2
        echo "  (ADR 0015). This database predates the billing layer; seed it again:" >&2
        echo "    scripts/dev-up.sh --reset" >&2
        return 1
      fi
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
      if seed "$1" "$2" "$3" "$4"; then
        echo "  + $1  issued as '$2' for consumer '$3' ($4)"
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

echo "dev partners and keys in $DEV_DB"
seed_one "$DEV_KEY" "dev-acme" "acme" "invoice" || exit 1
seed_one "$DEV_BETA_KEY" "dev-beta" "beta" "reconciliation" || exit 1
echo
echo "  models, priced in dollars per million tokens (ADR 0015):"
for _model in $DEV_MODELS; do
  _name=${_model%%:*}
  _rest=${_model#*:}
  _in=${_rest%%:*}
  _rest=${_rest#*:}
  _cached=${_rest%%:*}
  _out=${_rest#*:}
  echo "    $_name  input \$$_in, cached \$$_cached, output \$$_out"
done
echo
echo "  log in at the dashboard with either key, or '$DEV_KEY' for scripts/dev-load.sh."
