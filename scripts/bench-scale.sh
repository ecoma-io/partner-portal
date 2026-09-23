#!/usr/bin/env bash
#
# bench-scale.sh — runnable end-to-end scale benchmark for the metering pipeline.
#
# Drives `examples/bench_scale.rs` in release mode: it writes N synthetic usage
# records through the real LedgerWriter (accept + finalize per request), across a
# realistic multi-day span and a realistic model/consumer mix, then measures the
# dashboard queries, the keyset-vs-OFFSET pagination cost and the retention sweep.
# The harness itself prints the report; this script only sets the knobs, guards
# the machine (disk space, release profile) and tees the output to a log.
#
# Usage:
#   scripts/bench-scale.sh                 # full scale: 6,000,000 rows (~60 days)
#   scripts/bench-scale.sh smoke           # 100,000 rows   — quick wiring check
#   scripts/bench-scale.sh million         # 1,000,000 rows — CI-sized run
#   scripts/bench-scale.sh full            # 6,000,000 rows — same as no argument
#   scripts/bench-scale.sh <N>             # N rows, e.g. `scripts/bench-scale.sh 250000`
#
# Any BENCH_* variable can be overridden in the environment; explicit env wins
# over the tier default. See examples/bench_scale.rs for the full list:
#
#   BENCH_ROWS                 rows to insert            (default 6,000,000)
#   BENCH_SPAN_DAYS            span of created_at values (default 75 = 60 live + 15 expired)
#   BENCH_CONCURRENCY          concurrent producer tasks (default 128)
#   BENCH_BATCH_SIZE           writer micro-batch size   (default 100)
#   BENCH_BATCH_TIMEOUT_MS     writer batch window       (default 10)
#   BENCH_QUEUE_SIZE           bounded ingest queue      (default 10,000)
#   BENCH_CONSUMERS            distinct consumer keys    (default 24)
#   BENCH_RETENTION_DAYS       retention cutoff          (default 60)
#   BENCH_RETENTION_BATCH      rows per retention batch  (default 2,000)
#   BENCH_RETENTION_MAX_BATCHES batch budget             (default 500)
#   BENCH_QUERY_REPEATS        repeats per query         (default 5)
#   BENCH_PAGE_DEPTHS          keyset/OFFSET depths      (default "0,10000,50000")
#   BENCH_SEED                 synthetic data seed       (default 42)
#   BENCH_DB_DIR               scratch root              (default target/bench-scale)
#   BENCH_KEEP                 keep the database         (default: deleted)
#
# Examples:
#   BENCH_KEEP=1 BENCH_CONCURRENCY=256 scripts/bench-scale.sh million
#   BENCH_ROWS=2000000 BENCH_BATCH_SIZE=500 scripts/bench-scale.sh
#
# The database is written under $BENCH_DB_DIR (default target/bench-scale/), i.e.
# on the same filesystem as the repository — never in $TMPDIR, which is often a
# small tmpfs and would measure RAM instead of the disk the product uses.

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
cd "$repo_root"

tier=${1:-full}
case "$tier" in
  smoke)   tier_rows=100000 ;;
  million) tier_rows=1000000 ;;
  full)    tier_rows=6000000 ;;
  ''|*[!0-9]*) echo "bench-scale.sh: unknown tier '$tier' (use smoke|million|full or a row count)" >&2; exit 2 ;;
  *)       tier_rows=$tier ;;
esac

# Explicit BENCH_ROWS in the environment overrides the tier default.
export BENCH_ROWS=${BENCH_ROWS:-$tier_rows}

# --- preflight --------------------------------------------------------------

if ! command -v cargo >/dev/null 2>&1; then
  echo "bench-scale.sh: cargo not found on PATH" >&2
  exit 127
fi

# Measured in this repository at ~540 bytes/record (raw row + hourly rollup row +
# indexes) on a 60-day span. Budget 800 B/row so the guard covers the WAL and the
# scratch copy the retention sweep may leave behind.
need_mb=$(( BENCH_ROWS * 800 / 1000000 + 64 ))
scratch_root=${BENCH_DB_DIR:-$repo_root/target/bench-scale}
mkdir -p "$scratch_root"
avail_mb=$(df -Pm -- "$scratch_root" | awk 'NR==2 {print $4}')
if [ -n "$avail_mb" ] && [ "$avail_mb" -lt "$need_mb" ]; then
  echo "bench-scale.sh: needs ~${need_mb} MiB free on $(df -Pm -- "$scratch_root" | awk 'NR==2 {print $6}'), only ${avail_mb} MiB available" >&2
  echo "  (set BENCH_DB_DIR elsewhere, or lower BENCH_ROWS)" >&2
  exit 3
fi

echo "=== bench-scale: ${BENCH_ROWS} rows, tier '$tier' ==="
echo "repo:      $repo_root"
echo "scratch:   $scratch_root  (${avail_mb} MiB free, need ~${need_mb} MiB)"
echo "cpus:      $(nproc)  mem: $(awk '/MemTotal/ {printf "%.1f GiB", $2/1048576}' /proc/meminfo)"
echo "load:      $(cut -d' ' -f1-3 /proc/loadavg)"
echo "profile:   release (cargo --release, Cargo.toml [profile.release])"

log_dir=$scratch_root/logs
mkdir -p "$log_dir"
log=$log_dir/bench-scale-$(date -u +%Y%m%dT%H%M%SZ).log
echo "log:       $log"
echo

start=$(date +%s)
set +e
cargo run --release --offline --example bench_scale 2>&1 | tee "$log"
status=${PIPESTATUS[0]}
set -e
end=$(date +%s)

echo
echo "=== bench-scale: exit ${status}, wall $(date -u -d "@$((end - start))" +%H:%M:%S), log $log ==="
exit "$status"
