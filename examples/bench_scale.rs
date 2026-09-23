//! End-to-end scale benchmark for the metering pipeline.
//!
//! Inserts N synthetic usage records through the **real** [`LedgerWriter`] path
//! (bounded queue -> single writer task -> micro-batch -> one transaction per
//! batch that inserts the raw row and upserts the hourly rollup), then measures
//! the dashboard queries and a retention sweep against the resulting database.
//!
//! The default N is 6,000,000 records — roughly 60 days of traffic at 100k
//! requests/day — and the default time span is 75 days so that ~15 days of data
//! is already past the 60-day retention cutoff and the retention phase has real
//! work to do.
//!
//! # What it prints
//!
//! * the environment it ran in (CPU, cores, RAM, database path, filesystem and
//!   whether the backing device reports itself as rotational),
//! * insert-phase wall time, rows/sec, per-record ack latency percentiles, DB
//!   and WAL size, bytes per row, and the process RSS high-water mark
//!   (`VmHWM` from `/proc/self/status`),
//! * dashboard query latency: the summary aggregate, the 30-day hourly
//!   timeseries, and a page of the request list by keyset pagination compared
//!   against the naive `OFFSET` equivalent — both measured, at the same logical
//!   page depth,
//! * the retention sweep: wall time, rows deleted, space freed before and after
//!   an extra `incremental_vacuum`, and the resulting free-page count.
//!
//! # Running
//!
//! ```text
//! cargo run --release --example bench_scale
//! BENCH_ROWS=1000000 cargo run --release --example bench_scale
//! BENCH_KEEP=1 cargo run --release --example bench_scale   # keep the database
//! ```
//!
//! `scripts/bench-scale.sh` is a thin wrapper with the same knobs. All knobs:
//! `BENCH_ROWS`, `BENCH_SPAN_DAYS`, `BENCH_CONCURRENCY`, `BENCH_BATCH_SIZE`,
//! `BENCH_BATCH_TIMEOUT_MS`, `BENCH_QUEUE_SIZE`, `BENCH_CONSUMERS`,
//! `BENCH_RETENTION_DAYS`, `BENCH_RETENTION_BATCH`, `BENCH_RETENTION_MAX_BATCHES`,
//! `BENCH_QUERY_REPEATS`, `BENCH_PAGE_DEPTHS`, `BENCH_SEED`, `BENCH_DB_DIR`,
//! `BENCH_KEEP`.

use parking_lot::Mutex;
use partner_portal::ledger::retention::{self, DEFAULT_BATCH_SIZE, DEFAULT_MAX_BATCHES};
use partner_portal::ledger::timefmt;
use partner_portal::ledger::writer::{LedgerWriter, LedgerWriterConfig, format_hour};
use partner_portal::ledger::{Endpoint, RequestRecord, Usage, configure_sqlite, init_schema};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

// --------------------------------------------------------------------- knobs

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

struct Cfg {
    rows: usize,
    span_days: i64,
    concurrency: usize,
    batch_size: usize,
    batch_timeout_ms: u64,
    queue_size: usize,
    consumers: usize,
    retention_days: u32,
    retention_batch: usize,
    retention_max_batches: u32,
    query_repeats: usize,
    page_depths: Vec<i64>,
    seed: u64,
    keep: bool,
    db_dir: PathBuf,
}

impl Cfg {
    fn from_env() -> Self {
        let depths = env_string("BENCH_PAGE_DEPTHS", "0,10000,50000")
            .split(',')
            .filter_map(|s| s.trim().parse::<i64>().ok())
            .collect::<Vec<_>>();
        let root = match std::env::var("BENCH_DB_DIR") {
            Ok(dir) => PathBuf::from(dir),
            Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("bench-scale"),
        };

        Self {
            rows: env_usize("BENCH_ROWS", 6_000_000),
            span_days: env_i64("BENCH_SPAN_DAYS", 75),
            concurrency: env_usize("BENCH_CONCURRENCY", 128),
            batch_size: env_usize("BENCH_BATCH_SIZE", 100),
            batch_timeout_ms: env_u64("BENCH_BATCH_TIMEOUT_MS", 10),
            queue_size: env_usize("BENCH_QUEUE_SIZE", 10_000),
            consumers: env_usize("BENCH_CONSUMERS", 24),
            retention_days: env_u64("BENCH_RETENTION_DAYS", 60) as u32,
            retention_batch: env_usize("BENCH_RETENTION_BATCH", DEFAULT_BATCH_SIZE),
            retention_max_batches: env_u64(
                "BENCH_RETENTION_MAX_BATCHES",
                DEFAULT_MAX_BATCHES as u64,
            ) as u32,
            query_repeats: env_usize("BENCH_QUERY_REPEATS", 5),
            page_depths: if depths.is_empty() { vec![0] } else { depths },
            seed: env_u64("BENCH_SEED", 42),
            keep: std::env::var("BENCH_KEEP").is_ok(),
            db_dir: root,
        }
    }
}

// ---------------------------------------------------------------- utilities

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// `VmHWM` (peak resident set size) in KiB, from `/proc/self/status`.
fn peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .ok();
        }
    }
    None
}

fn current_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .ok();
        }
    }
    None
}

fn page_stats(conn: &Connection) -> (i64, i64, i64) {
    let page_count: i64 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap_or(0);
    let freelist: i64 = conn
        .query_row("PRAGMA freelist_count", [], |r| r.get(0))
        .unwrap_or(0);
    let page_size: i64 = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap_or(0);
    (page_count, freelist, page_size)
}

/// Fold the WAL back into the main database so file sizes reflect real bytes.
fn checkpoint(conn: &Connection) {
    let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap_or(-1)
}

// --------------------------------------------------------------- hardware

/// Best-effort description of the CPU, memory and the device backing `db_dir`.
///
/// Nothing here is assumed from the environment: the CPU model is read from
/// `/proc/cpuinfo`, memory from `/proc/meminfo`, and the backing device from
/// `/proc/mounts` plus sysfs. When sysfs cannot answer (containers, exotic
/// filesystems) the report says so rather than guessing.
fn hardware_report(db_dir: &Path) -> Vec<String> {
    let mut out = Vec::new();

    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let cpu_model = cpuinfo
        .lines()
        .find(|l| l.starts_with("model name"))
        .and_then(|l| l.split_once(':'))
        .map(|(_, v)| v.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let logical = cpuinfo
        .lines()
        .filter(|l| l.starts_with("processor"))
        .count();
    let physical = cpuinfo
        .lines()
        .filter(|l| l.starts_with("cpu cores"))
        .filter_map(|l| {
            l.split_once(':')
                .and_then(|(_, v)| v.trim().parse::<usize>().ok())
        })
        .max()
        .unwrap_or(0);
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    out.push(format!(
        "cpu: {cpu_model} ({logical} logical / {physical} physical cores, {threads} available)"
    ));

    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let mem = |key: &str| -> Option<f64> {
        meminfo
            .lines()
            .find(|l| l.starts_with(key))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<f64>().ok())
            .map(|kib| kib / (1024.0 * 1024.0))
    };
    out.push(format!(
        "memory: {:.1} GiB total, {:.1} GiB available at start",
        mem("MemTotal:").unwrap_or(0.0),
        mem("MemAvailable:").unwrap_or(0.0)
    ));

    let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
    let db_dir = std::fs::canonicalize(db_dir).unwrap_or_else(|_| db_dir.to_path_buf());
    let mut best: Option<(usize, String, String, String)> = None;
    for line in mounts.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 4 {
            continue;
        }
        let mp = PathBuf::from(f[1]);
        if db_dir.starts_with(&mp) {
            let len = mp.as_os_str().len();
            if best.as_ref().is_none_or(|(l, _, _, _)| len > *l) {
                best = Some((len, f[0].to_string(), f[2].to_string(), f[3].to_string()));
            }
        }
    }
    match best {
        Some((_, device, fstype, opts)) => {
            let name = PathBuf::from(&device)
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            let (rotational, model) = block_device_info(&name);
            out.push(format!(
                "database: {} on {} ({device}, {fstype}, rw opts: {})",
                db_dir.display(),
                name,
                opts
            ));
            match (rotational, model) {
                (Some(0), Some(model)) => out.push(format!(
                    "backing device: {model} on {device}, rotational=0 (non-rotational: SSD/NVMe)"
                )),
                (Some(1), Some(model)) => out.push(format!(
                    "backing device: {model} on {device}, rotational=1 (spinning disk)"
                )),
                (Some(0), None) => out.push(format!(
                    "backing device: {device}, rotational=0 (non-rotational)"
                )),
                (Some(1), None) => out.push(format!("backing device: {device}, rotational=1")),
                (Some(other), _) => out.push(format!(
                    "backing device: {device}, queue/rotational={other} (unexpected value)"
                )),
                (None, _) => out.push(format!(
                    "backing device: {device}: type could NOT be determined from sysfs \
                     (queue/rotational unreadable); do not assume SSD"
                )),
            }
        }
        None => out.push(format!(
            "database: {} (mount point could not be resolved from /proc/mounts)",
            db_dir.display()
        )),
    }

    out
}

/// `(queue/rotational, device model)` for a block device or one of its
/// partitions, walking up to the parent device. `None` when sysfs cannot say.
fn block_device_info(name: &str) -> (Option<i64>, Option<String>) {
    let Ok(link) = std::fs::canonicalize(format!("/sys/class/block/{name}")) else {
        return (None, None);
    };
    let mut rotational = None;
    let mut model = None;
    let mut cur: Option<&Path> = Some(link.as_path());
    while let Some(dir) = cur {
        if rotational.is_none() {
            rotational = std::fs::read_to_string(dir.join("queue/rotational"))
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok());
        }
        if model.is_none() {
            model = std::fs::read_to_string(dir.join("device/model"))
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());
        }
        if rotational.is_some() && model.is_some() {
            break;
        }
        cur = dir.parent();
    }
    (rotational, model)
}

// ------------------------------------------------------- latency histogram

const HIST_BUCKETS: usize = 4_601;
const HIST_OVERFLOW: usize = 4_600;

#[derive(Clone)]
struct LatencyHist {
    buckets: Vec<u32>,
    count: u64,
    sum_ns: u128,
}

impl Default for LatencyHist {
    fn default() -> Self {
        Self {
            buckets: vec![0; HIST_BUCKETS],
            count: 0,
            sum_ns: 0,
        }
    }
}

fn bucket_index(us: u64) -> usize {
    match us {
        0..=999 => us as usize,
        1_000..=9_999 => 1_000 + ((us - 1_000) / 10) as usize,
        10_000..=99_999 => 1_900 + ((us - 10_000) / 100) as usize,
        100_000..=999_999 => 2_800 + ((us - 100_000) / 1_000) as usize,
        1_000_000..=9_999_999 => 3_700 + ((us - 1_000_000) / 10_000) as usize,
        _ => HIST_OVERFLOW,
    }
}

fn bucket_upper_us(idx: usize) -> u64 {
    match idx {
        0..=999 => idx as u64 + 1,
        1_000..=1_899 => 1_000 + (idx - 1_000) as u64 * 10 + 10,
        1_900..=2_799 => 10_000 + (idx - 1_900) as u64 * 100 + 100,
        2_800..=3_699 => 100_000 + (idx - 2_800) as u64 * 1_000 + 1_000,
        3_700..=4_599 => 1_000_000 + (idx - 3_700) as u64 * 10_000 + 10_000,
        _ => 10_000_000,
    }
}

impl LatencyHist {
    fn record(&mut self, d: Duration) {
        let us = d.as_micros().min(u128::from(u32::MAX)) as u64;
        let idx = bucket_index(us);
        self.buckets[idx] = self.buckets[idx].saturating_add(1);
        self.count += 1;
        self.sum_ns += d.as_nanos();
    }

    fn merge(&mut self, other: &LatencyHist) {
        for (a, b) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *a = a.saturating_add(*b);
        }
        self.count += other.count;
        self.sum_ns += other.sum_ns;
    }

    fn percentile(&self, p: f64) -> Duration {
        if self.count == 0 {
            return Duration::ZERO;
        }
        let target = ((p * self.count as f64).ceil() as u64).max(1);
        let mut cum = 0u64;
        for (idx, &c) in self.buckets.iter().enumerate() {
            cum += u64::from(c);
            if cum >= target {
                return Duration::from_micros(bucket_upper_us(idx));
            }
        }
        Duration::from_secs(10)
    }

    fn mean(&self) -> Duration {
        if self.count == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos((self.sum_ns / u128::from(self.count)) as u64)
    }

    fn count(&self) -> u64 {
        self.count
    }
}

#[derive(Clone, Default)]
struct Meter {
    accept: LatencyHist,
    finalize: LatencyHist,
}

impl Meter {
    fn merge(&mut self, other: &Meter) {
        self.accept.merge(&other.accept);
        self.finalize.merge(&other.finalize);
    }
}

fn ms(d: Duration) -> String {
    format!("{:.3}", d.as_secs_f64() * 1_000.0)
}

fn pct(hist: &LatencyHist, label: &str) -> String {
    format!(
        "{label}: p50={} p95={} p99={} mean={} (n={})",
        ms(hist.percentile(0.50)),
        ms(hist.percentile(0.95)),
        ms(hist.percentile(0.99)),
        ms(hist.mean()),
        hist.count()
    )
}

// ------------------------------------------------------------ the workload

const MODELS: [&str; 8] = [
    "gpt-4o",
    "gpt-4o-mini",
    "gpt-4.1",
    "gpt-4.1-mini",
    "o4-mini",
    "o3",
    "gpt-4-turbo",
    "gpt-3.5-turbo",
];

/// Build the record for record number `seq` of `total`, in the `in_flight`
/// state, the way the proxy does before contacting the upstream.
fn build_record(
    seq: usize,
    total: usize,
    base: OffsetDateTime,
    span_secs: i64,
    consumers: &[String],
    seed: u64,
) -> RequestRecord {
    let h = mix((seq as u64) ^ seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let step = span_secs as f64 / total.max(1) as f64;

    // Timestamps advance monotonically with sub-millisecond jitter, which is
    // what live traffic looks like: the ledger is appended to in time order.
    let created = base
        + time::Duration::seconds((seq as f64 * step) as i64)
        + time::Duration::microseconds((h % 20_000) as i64);

    let consumer = consumers[(h as usize / 3) % consumers.len()].clone();
    let model = MODELS[(h as usize / 7) % MODELS.len()].to_string();
    let streaming = h % 10 < 3;
    // `/v1/models` is deliberately not metered, so only these two endpoints can
    // appear in the ledger.
    let endpoint = if h % 10 < 9 {
        Endpoint::ChatCompletions
    } else {
        Endpoint::Responses
    };

    let mut record = RequestRecord::new(
        uuid::Uuid::now_v7().to_string(),
        consumer,
        model,
        endpoint,
        streaming,
    );
    record.created_at = created;
    record
}

/// Move a record to a terminal state: ~2% upstream failures, ~2% with usage the
/// provider never reported (NULL tokens, not zeros), the rest completed.
fn terminalize(record: &mut RequestRecord, seq: usize, seed: u64) {
    let h = mix((seq as u64) ^ seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xABCD_EF01);
    let duration_ms = 50 + (h % 4_000);
    let streaming = record.streaming;

    match h % 100 {
        0..=1 => record.fail(Some(502), "upstream returned 502".to_string(), duration_ms),
        2..=3 => record.complete(200, Usage::default(), duration_ms),
        _ => {
            let input = 200 + (h % 4_000);
            let output = 20 + (h / 7) % 1_200;
            let cached = if h % 5 == 0 { Some(input / 4) } else { None };
            record.complete(
                200,
                Usage::new(Some(input), Some(output), cached),
                duration_ms,
            );
        }
    }

    if streaming {
        record.set_ttft(10 + h % 500);
    }
}

// -------------------------------------------------------------- insert phase

struct InsertResult {
    wall: Duration,
    meter: Meter,
    committed: u64,
}

async fn insert_phase(
    writer: Arc<LedgerWriter>,
    db_path: PathBuf,
    cfg: &Cfg,
    consumers: Arc<Vec<String>>,
    base: OffsetDateTime,
) -> InsertResult {
    let counter = Arc::new(AtomicUsize::new(0));
    let span_secs = cfg.span_days * 86_400;
    let rows = cfg.rows;

    let done = Arc::new(AtomicBool::new(false));
    let monitor = {
        let counter = Arc::clone(&counter);
        let done = Arc::clone(&done);
        let db_path = db_path.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let mut last = 0usize;
            while !done.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(10)).await;
                if done.load(Ordering::Relaxed) {
                    break;
                }
                let n = counter.load(Ordering::Relaxed).min(rows);
                let recent = (n - last) as f64 / 10.0;
                last = n;
                println!(
                    "  [progress] {n} / {rows} rows ({:.1}%), {recent:.0} rows/s recent, \
                     wall {:.0}s, db {:.0} MiB",
                    n as f64 * 100.0 / rows.max(1) as f64,
                    started.elapsed().as_secs_f64(),
                    mib(file_size(&db_path))
                );
            }
        })
    };

    let started = Instant::now();
    let mut handles = Vec::with_capacity(cfg.concurrency);
    for _ in 0..cfg.concurrency {
        let writer = Arc::clone(&writer);
        let counter = Arc::clone(&counter);
        let consumers = Arc::clone(&consumers);
        let seed = cfg.seed;
        handles.push(tokio::spawn(async move {
            let mut meter = Meter::default();
            loop {
                let seq = counter.fetch_add(1, Ordering::Relaxed);
                if seq >= rows {
                    break;
                }

                let mut record = build_record(seq, rows, base, span_secs, &consumers, seed);

                let t = Instant::now();
                writer
                    .accept(record.clone())
                    .await
                    .expect("accept must commit");
                meter.accept.record(t.elapsed());

                terminalize(&mut record, seq, seed);

                let t = Instant::now();
                writer.finalize(record).await.expect("finalize must commit");
                meter.finalize.record(t.elapsed());
            }
            meter
        }));
    }

    let mut meter = Meter::default();
    for handle in handles {
        meter.merge(&handle.await.expect("producer task panicked"));
    }
    let wall = started.elapsed();

    done.store(true, Ordering::Relaxed);
    let _ = monitor.await;

    let committed = writer.committed_total();
    writer.shutdown().await;

    InsertResult {
        wall,
        meter,
        committed,
    }
}

/// Insert `rows` fresh records whose `created_at` is recent, so they are not
/// retention-eligible, through the real writer.
///
/// Run after a retention sweep, this measures whether the pages the sweep freed
/// are *reused* by new traffic (the freelist) or whether the file grows afresh —
/// i.e. whether steady-state disk usage is bounded by live data or by cumulative
/// traffic. Nothing about that is assumed here: the caller compares the growth
/// against the fresh-database bytes/record measured in the insert phase.
async fn reuse_probe(
    writer: Arc<LedgerWriter>,
    rows: usize,
    concurrency: usize,
    consumers: Arc<Vec<String>>,
    seed: u64,
) -> (Duration, u64) {
    let counter = Arc::new(AtomicUsize::new(0));
    let probe_seed = seed ^ 0x00A5_5A5A;
    let base = OffsetDateTime::now_utc() - time::Duration::minutes(30);
    let span_secs: i64 = 1_800; // half an hour of recent, live traffic

    let started = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let writer = Arc::clone(&writer);
        let counter = Arc::clone(&counter);
        let consumers = Arc::clone(&consumers);
        handles.push(tokio::spawn(async move {
            loop {
                let seq = counter.fetch_add(1, Ordering::Relaxed);
                if seq >= rows {
                    break;
                }
                let mut record = build_record(seq, rows, base, span_secs, &consumers, probe_seed);
                writer
                    .accept(record.clone())
                    .await
                    .expect("probe accept must commit");
                terminalize(&mut record, seq, probe_seed);
                writer
                    .finalize(record)
                    .await
                    .expect("probe finalize must commit");
            }
        }));
    }
    for handle in handles {
        handle.await.expect("probe producer panicked");
    }
    let wall = started.elapsed();
    let committed = writer.committed_total();
    writer.shutdown().await;
    (wall, committed)
}

// ------------------------------------------------------------- query phase

struct QueryMeasurement {
    label: String,
    runs: Vec<Duration>,
    /// Identity of each row returned by the last run, so two pages can be
    /// compared for equivalence rather than merely by row count.
    items: Vec<String>,
}

impl QueryMeasurement {
    fn min(&self) -> Duration {
        self.runs.iter().copied().min().unwrap_or_default()
    }
    fn median(&self) -> Duration {
        let mut v = self.runs.clone();
        v.sort();
        v.get(v.len() / 2).copied().unwrap_or_default()
    }
    fn max(&self) -> Duration {
        self.runs.iter().copied().max().unwrap_or_default()
    }
    fn rows(&self) -> usize {
        self.items.len()
    }
}

// -------------------------------------------------------------------- main

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(|n| n.get().min(16))
                .unwrap_or(4),
        )
        .enable_all()
        .build()
        .expect("build tokio runtime")
}

fn main() {
    let cfg = Cfg::from_env();
    let rt = runtime();

    let run_id = format!(
        "run-{}-{}",
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    );
    let dir = cfg.db_dir.join(&run_id);
    std::fs::create_dir_all(&dir).expect("create benchmark directory");
    let db_path = dir.join("ledger.db");

    println!("================================================================================");
    println!(" partner-portal metering scale benchmark");
    println!("================================================================================");
    for line in hardware_report(&dir) {
        println!("{line}");
    }
    println!(
        "rows={} span={}d consumers={} concurrency={} batch_size={} batch_timeout={}ms \
         queue={} seed={}",
        cfg.rows,
        cfg.span_days,
        cfg.consumers,
        cfg.concurrency,
        cfg.batch_size,
        cfg.batch_timeout_ms,
        cfg.queue_size,
        cfg.seed
    );
    println!("database: {}", db_path.display());
    println!();

    // ---- setup ----
    // The writer spawns its task on the runtime, so it is constructed from
    // inside the runtime context.
    let (conn, writer) = rt.block_on(async {
        let c = Connection::open(&db_path).expect("open database");
        configure_sqlite(&c).expect("configure sqlite");
        init_schema(&c).expect("init schema");

        let conn = Arc::new(Mutex::new(c));
        let writer = Arc::new(LedgerWriter::new(
            Arc::clone(&conn),
            LedgerWriterConfig {
                queue_size: cfg.queue_size,
                batch_size: cfg.batch_size,
                batch_timeout_ms: cfg.batch_timeout_ms,
                // A production instance always stamps its owner on every
                // accepted row, so the measured row carries the real payload.
                instance_id: Some("bench-scale".to_string()),
            },
        ));
        (conn, writer)
    });

    let consumers = Arc::new(
        (0..cfg.consumers)
            .map(|i| format!("consumer-{i:02}"))
            .collect::<Vec<String>>(),
    );
    let base = OffsetDateTime::now_utc() - time::Duration::days(cfg.span_days);

    // ---- insert ----
    println!(
        "[insert] writing {} records through the real LedgerWriter path...",
        cfg.rows
    );
    let insert = rt.block_on(insert_phase(
        Arc::clone(&writer),
        db_path.clone(),
        &cfg,
        Arc::clone(&consumers),
        base,
    ));

    let write_conn = conn.lock();
    let wal_bytes_before_checkpoint = file_size(&db_path.with_extension("db-wal"));
    let size_before_checkpoint = file_size(&db_path);
    checkpoint(&write_conn);
    let size_after_checkpoint = file_size(&db_path);
    let wal_after_checkpoint = file_size(&db_path.with_extension("db-wal"));
    let (page_count, freelist, page_size) = page_stats(&write_conn);

    println!();
    println!("[insert] results");
    println!("  wall time:            {:.2} s", insert.wall.as_secs_f64());
    println!(
        "  throughput:           {:.0} records/s ({} ops committed, 2 ops per record: accept + finalize)",
        cfg.rows as f64 / insert.wall.as_secs_f64(),
        insert.committed
    );
    if (insert.committed as f64) < cfg.rows as f64 {
        println!(
            "  note:                 {} records collapsed (accept superseded by finalize in the \
             same batch), so fewer than 2 rows/s of ops were written",
            cfg.rows as f64 * 2.0 - insert.committed as f64
        );
    }
    println!("  {}", pct(&insert.meter.accept, "accept ack  "));
    println!("  {}", pct(&insert.meter.finalize, "finalize ack"));
    println!(
        "  db file:              {:.1} MiB before checkpoint, {:.1} MiB after ({:.0} MiB WAL before, {:.1} MiB after)",
        mib(size_before_checkpoint),
        mib(size_after_checkpoint),
        mib(wal_bytes_before_checkpoint),
        mib(wal_after_checkpoint)
    );
    println!(
        "  pages:                {page_count} pages x {page_size} B, {freelist} free ({:.1} MiB free)",
        mib(freelist as u64 * page_size as u64)
    );
    println!(
        "  bytes per record:     {:.0} B on disk after checkpoint ({:.1} MiB / {} records)",
        size_after_checkpoint as f64 / cfg.rows as f64,
        mib(size_after_checkpoint),
        cfg.rows
    );
    println!(
        "  per 1,000,000 rows:   {:.1} MiB (measured at this row count)",
        mib(size_after_checkpoint) / (cfg.rows as f64 / 1_000_000.0)
    );
    match peak_rss_kib() {
        Some(kib) => println!(
            "  RSS high-water:       {:.1} MiB (VmHWM), {:.1} MiB current (VmRSS)",
            kib as f64 / 1024.0,
            current_rss_kib().unwrap_or(0) as f64 / 1024.0
        ),
        None => println!("  RSS high-water:       unavailable (/proc/self/status unreadable)"),
    }
    drop(write_conn);

    // ---- queries ----
    let now = timefmt::now();
    let window_days: i64 = 30;
    let start = now - time::Duration::days(window_days);
    let start_ts = timefmt::format_ts(start);
    let end_ts = timefmt::format_ts(now);
    let start_hour = format_hour(start);
    let end_hour = format_hour(now);
    let consumer = consumers[0].clone();

    let reader = {
        let c = Connection::open(&db_path).expect("open reader");
        configure_sqlite(&c).expect("configure reader");
        c
    };

    let rows_in_window: i64 = reader
        .query_row(
            "SELECT COUNT(*) FROM usage_records WHERE consumer_id = ?1 AND created_at >= ?2 AND created_at < ?3",
            rusqlite::params![consumer, start_ts, end_ts],
            |r| r.get(0),
        )
        .unwrap_or(-1);

    println!();
    println!("[queries] dashboard queries over the full dataset");
    println!("  window: last {window_days} days ({start_ts} .. {end_ts})");
    println!(
        "  consumer: {consumer}, {rows_in_window} rows in window, repeats: {}",
        cfg.query_repeats
    );
    println!();

    let mut results: Vec<QueryMeasurement> = Vec::new();

    // 1. summary aggregate (hourly rollup) — same SQL as
    //    GET /api/dashboard/summary.
    let summary_sql = r#"
        SELECT COALESCE(SUM(request_count), 0), COALESCE(SUM(success_count), 0),
               COALESCE(SUM(failure_count), 0), COALESCE(SUM(total_input_tokens), 0),
               COALESCE(SUM(total_output_tokens), 0), COALESCE(SUM(total_cached_tokens), 0),
               CASE WHEN SUM(request_count) > 0 THEN SUM(total_duration_ms) * 1.0 / SUM(request_count) ELSE NULL END,
               CASE WHEN SUM(ttft_count) > 0 THEN SUM(total_ttft_ms) * 1.0 / SUM(ttft_count) ELSE NULL END,
               CASE WHEN SUM(request_count) > 0 THEN SUM(success_count) * 1.0 / SUM(request_count) ELSE 0.0 END
        FROM usage_hourly
        WHERE consumer_id = ?1 AND hour >= ?2 AND hour <= ?3 AND (?4 IS NULL OR model = ?4)
    "#;
    results.push(time_query_manual(
        &reader,
        "summary (rollup aggregate)",
        summary_sql,
        cfg.query_repeats,
        |stmt| {
            stmt.query_row(
                rusqlite::params![consumer, start_hour, end_hour, Option::<String>::None],
                |row| {
                    let n: i64 = row.get(0)?;
                    Ok(n)
                },
            )
        },
    ));

    // 2. summary's secondary raw count (same endpoint, second query).
    let unavailable_sql = r#"
        SELECT COUNT(*) FROM usage_records
        WHERE consumer_id = ?1 AND created_at >= ?2 AND created_at < ?3
          AND (?4 IS NULL OR model = ?4)
          AND usage_status <> 'available' AND request_status <> 'in_flight'
    "#;
    results.push(time_query_manual(
        &reader,
        "summary unavailable-usage count (raw)",
        unavailable_sql,
        cfg.query_repeats,
        |stmt| {
            stmt.query_row(
                rusqlite::params![consumer, start_ts, end_ts, Option::<String>::None],
                |row| {
                    let n: i64 = row.get(0)?;
                    Ok(n)
                },
            )
        },
    ));

    // 3. hourly timeseries over the window.
    let ts_sql = r#"
        SELECT hour, SUM(request_count), SUM(success_count), SUM(failure_count),
               SUM(total_input_tokens), SUM(total_output_tokens), SUM(total_cached_tokens)
        FROM usage_hourly
        WHERE consumer_id = ?1 AND hour >= ?2 AND hour <= ?3 AND (?4 IS NULL OR model = ?4)
        GROUP BY hour ORDER BY hour ASC
    "#;
    results.push(time_query_rows(
        &reader,
        "timeseries (rollup, GROUP BY hour)",
        ts_sql,
        cfg.query_repeats,
        |stmt| {
            let rows = stmt.query(rusqlite::params![
                consumer,
                start_hour,
                end_hour,
                Option::<String>::None
            ])?;
            rows.mapped(|row| row.get::<_, String>(0)).collect()
        },
    ));

    // 4/5. request list: keyset pagination vs the naive OFFSET equivalent at
    //      the same logical page depth. The cursor for a deep keyset page is
    //      obtained once, untimed, with the OFFSET query — so both queries are
    //      compared on exactly the same page, and the OFFSET cost is reported
    //      separately as its own measurement.
    const PAGE: i64 = 50;
    let list_select = "SELECT id, request_id, created_at, model, endpoint, streaming, http_status, \
                       request_status, usage_status, input_tokens, output_tokens, cached_tokens, \
                       duration_ms, ttft_ms, error_message FROM usage_records";
    let list_where = "WHERE consumer_id = ?1 AND created_at >= ?2 AND created_at < ?3 \
                      AND (?4 IS NULL OR model = ?4) AND (?5 IS NULL OR request_status = ?5)";

    let keyset_sql = format!(
        "{list_select} {list_where} AND (created_at, id) < (?6, ?7) \
         ORDER BY created_at DESC, id DESC LIMIT ?8"
    );
    let offset_sql =
        format!("{list_select} {list_where} ORDER BY created_at DESC, id DESC LIMIT ?6 OFFSET ?7");
    let first_page_sql =
        format!("{list_select} {list_where} ORDER BY created_at DESC, id DESC LIMIT ?6");

    // Page 1 by keyset (no cursor), i.e. what the dashboard serves first.
    results.push(time_query_rows(
        &reader,
        "requests page 1 (keyset, no cursor)",
        &first_page_sql,
        cfg.query_repeats,
        |stmt| {
            let rows = stmt.query(rusqlite::params![
                consumer,
                start_ts,
                end_ts,
                Option::<String>::None,
                Option::<String>::None,
                PAGE
            ])?;
            rows.mapped(|row| Ok(row.get::<_, i64>(0)?.to_string()))
                .collect()
        },
    ));

    // `offset` here is the row offset of the *first row of the page*. The keyset
    // cursor is the row immediately before it (offset - 1), which is exactly
    // what the dashboard hands back as `next_cursor`, so both queries return the
    // same 50 rows and can be compared directly.
    let mut pairs: Vec<(usize, usize)> = Vec::new();
    for offset in cfg.page_depths.clone() {
        if offset <= 0 {
            continue;
        }
        // Untimed: fetch the cursor row at offset - 1.
        let cursor = reader
            .query_row(
                &format!(
                    "{list_select} {list_where} ORDER BY created_at DESC, id DESC LIMIT 1 OFFSET ?6"
                ),
                rusqlite::params![
                    consumer,
                    start_ts,
                    end_ts,
                    Option::<String>::None,
                    Option::<String>::None,
                    offset - 1
                ],
                |row| Ok((row.get::<_, String>(2)?, row.get::<_, i64>(0)?)),
            )
            .ok();

        let Some((cursor_created, cursor_id)) = cursor else {
            println!("  (page offset {offset} is past the end of the window; skipped)");
            continue;
        };

        let keyset_index = results.len();
        results.push(time_query_rows(
            &reader,
            &format!("requests keyset @ offset {offset}"),
            &keyset_sql,
            cfg.query_repeats,
            |stmt| {
                let rows = stmt.query(rusqlite::params![
                    consumer,
                    start_ts,
                    end_ts,
                    Option::<String>::None,
                    Option::<String>::None,
                    cursor_created,
                    cursor_id,
                    PAGE
                ])?;
                rows.mapped(|row| {
                    Ok(format!(
                        "{}|{}",
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(0)?
                    ))
                })
                .collect()
            },
        ));

        let offset_index = results.len();
        results.push(time_query_rows(
            &reader,
            &format!("requests OFFSET {offset} (naive)"),
            &offset_sql,
            cfg.query_repeats,
            |stmt| {
                let rows = stmt.query(rusqlite::params![
                    consumer,
                    start_ts,
                    end_ts,
                    Option::<String>::None,
                    Option::<String>::None,
                    PAGE,
                    offset
                ])?;
                rows.mapped(|row| {
                    Ok(format!(
                        "{}|{}",
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(0)?
                    ))
                })
                .collect()
            },
        ));
        pairs.push((keyset_index, offset_index));
    }

    println!(
        "  {:<44} {:>10} {:>10} {:>10} {:>8}",
        "query", "min ms", "p50 ms", "max ms", "rows"
    );
    println!("  {}", "-".repeat(86));
    for r in &results {
        println!(
            "  {:<44} {:>10} {:>10} {:>10} {:>8}",
            r.label,
            ms(r.min()),
            ms(r.median()),
            ms(r.max()),
            r.rows()
        );
    }
    println!(
        "  (n={} runs each; min/p50/max are over runs, not rows; query params: consumer_id={consumer}, \
         30-day window, model=NULL, status=NULL, limit={PAGE})",
        cfg.query_repeats
    );

    // The two pagination strategies must return the same page, or the
    // comparison would be measuring two different queries.
    for (keyset_index, offset_index) in pairs {
        let k = &results[keyset_index];
        let o = &results[offset_index];
        println!(
            "  equivalence check: {} vs {} -> same rows: {}",
            k.label,
            o.label,
            k.items == o.items
        );
    }

    // ---- retention ----
    println!();
    println!(
        "[retention] sweeping records older than {} days",
        cfg.retention_days
    );

    let write_conn = conn.lock();
    checkpoint(&write_conn);
    let size_before = file_size(&db_path);
    let (pages_before, freelist_before, page_size) = page_stats(&write_conn);
    let rows_before = count(&write_conn, "SELECT COUNT(*) FROM usage_records");
    let hourly_before = count(&write_conn, "SELECT COUNT(*) FROM usage_hourly");
    println!(
        "  before: {:.1} MiB, {rows_before} raw rows, {hourly_before} rollup rows, \
         {freelist_before} free pages, WAL {:.1} MiB",
        mib(size_before),
        mib(file_size(&db_path.with_extension("db-wal")))
    );
    drop(write_conn);

    let mut sweep_time = Duration::ZERO;
    let mut sweeps = 0u32;
    let mut deleted_raw = 0u64;
    let mut deleted_hourly = 0u64;
    let mut hit_budget = false;
    loop {
        let stats = retention::run_retention(
            &conn,
            cfg.retention_days,
            cfg.retention_batch,
            cfg.retention_max_batches,
        )
        .expect("retention sweep");
        sweep_time += Duration::from_millis(stats.duration_ms);
        sweeps += 1;
        deleted_raw += stats.raw_deleted;
        deleted_hourly += stats.hourly_deleted;
        if stats.hit_budget {
            hit_budget = true;
        }
        println!(
            "  sweep {sweeps}: deleted {} raw + {} rollup rows in {} ms{}",
            stats.raw_deleted,
            stats.hourly_deleted,
            stats.duration_ms,
            if stats.hit_budget {
                " (slice budget reached, continuing)"
            } else {
                ""
            }
        );
        if stats.total_deleted() == 0 || !stats.hit_budget || sweeps >= 32 {
            break;
        }
    }

    let write_conn = conn.lock();
    checkpoint(&write_conn);
    let size_after_sweeps = file_size(&db_path);
    let (pages_after_sweeps, freelist_after_sweeps, _) = page_stats(&write_conn);
    let rows_after = count(&write_conn, "SELECT COUNT(*) FROM usage_records");
    let hourly_after = count(&write_conn, "SELECT COUNT(*) FROM usage_hourly");
    let wal_after_sweeps = file_size(&db_path.with_extension("db-wal"));

    // Is the space returned to the filesystem by the sweep's own bounded
    // incremental_vacuum, or does it need another pass? Measure, do not assume:
    // repeat the very same statement and record what each pass returns.
    const EXTRA_PASSES: u32 = 5;
    let mut freed_by_extra_passes = 0i64;
    let mut last_freelist = freelist_after_sweeps;
    for _ in 0..EXTRA_PASSES {
        write_conn
            .execute_batch("PRAGMA incremental_vacuum(8192);")
            .expect("incremental_vacuum(8192)");
        checkpoint(&write_conn);
        let (_, fl, _) = page_stats(&write_conn);
        freed_by_extra_passes += last_freelist.saturating_sub(fl);
        last_freelist = fl;
    }
    let size_after_extra_passes = file_size(&db_path);
    let (_, freelist_after_extra_passes, _) = page_stats(&write_conn);

    write_conn
        .execute_batch("PRAGMA incremental_vacuum;")
        .expect("full incremental_vacuum");
    checkpoint(&write_conn);
    let size_after_full_vacuum = file_size(&db_path);
    let (pages_final, freelist_final, _) = page_stats(&write_conn);
    drop(write_conn);

    println!();
    println!("[retention] results");
    println!(
        "  sweeps:               {sweeps} (hit slice budget: {hit_budget}), total sweep time {:.3} s",
        sweep_time.as_secs_f64()
    );
    println!(
        "  deleted:              {deleted_raw} raw rows, {deleted_hourly} rollup rows ({:.0} rows/s of sweep)",
        deleted_raw as f64 / sweep_time.as_secs_f64().max(1e-9)
    );
    println!(
        "  rows:                 {rows_before} -> {rows_after} raw, {hourly_before} -> {hourly_after} rollup"
    );
    println!(
        "  db file:              {} KiB before -> {} KiB after sweeps -> {} KiB after \
         {EXTRA_PASSES} extra incremental_vacuum(8192) passes -> {} KiB after a final \
         incremental_vacuum",
        size_before as f64 / 1024.0,
        size_after_sweeps as f64 / 1024.0,
        size_after_extra_passes as f64 / 1024.0,
        size_after_full_vacuum as f64 / 1024.0
    );
    println!(
        "  space freed:          {} KiB by the sweeps themselves, {} KiB more by the extra passes, \
         {} KiB total",
        size_before.saturating_sub(size_after_sweeps) as f64 / 1024.0,
        size_after_sweeps.saturating_sub(size_after_full_vacuum) as f64 / 1024.0,
        size_before.saturating_sub(size_after_full_vacuum) as f64 / 1024.0
    );
    println!(
        "  bytes freed per row:  {:.1} B",
        size_before.saturating_sub(size_after_full_vacuum) as f64 / deleted_raw.max(1) as f64
    );
    println!(
        "  free pages:           {freelist_before} before -> {freelist_after_sweeps} after sweeps -> \
         {freelist_after_extra_passes} after {EXTRA_PASSES} extra passes -> {freelist_final} after the \
         final incremental_vacuum ({page_size} B pages)"
    );
    println!("  pages in file:        {pages_before} -> {pages_after_sweeps} -> {pages_final}");
    println!(
        "  incremental_vacuum:   the sweep's own single statement left {freelist_after_sweeps} free \
         pages; {EXTRA_PASSES} further statements of the same kind returned \
         {freed_by_extra_passes} pages ({:.2} pages per statement)",
        freed_by_extra_passes as f64 / EXTRA_PASSES as f64
    );
    println!("  WAL after sweeps:     {:.1} MiB", mib(wal_after_sweeps));
    if size_after_full_vacuum < size_after_sweeps {
        println!(
            "  -> the per-sweep incremental_vacuum left pages behind; repeating it does return \
             space to the filesystem, at ~{:.1} page(s) per statement",
            freed_by_extra_passes as f64 / EXTRA_PASSES as f64
        );
    } else {
        println!(
            "  -> the per-sweep incremental_vacuum already returned the reclaimable space; the \
             extra passes did not shrink the file further"
        );
    }
    println!(
        "  -> {freelist_final} pages ({:.1} MiB) remain free in the file: they are reusable by new \
         inserts (measured below) but are not returned to the filesystem",
        mib(freelist_final as u64 * page_size as u64)
    );

    // ---- reuse probe: does new traffic reuse what retention freed? ----
    println!();
    let probe_rows = (cfg.rows / 10).clamp(1_000, 200_000);
    println!(
        "[reuse] inserting {probe_rows} fresh, retention-ineligible records through the same writer"
    );
    let (pages_before_probe, freelist_before_probe, _) = {
        let g = conn.lock();
        checkpoint(&g);
        page_stats(&g)
    };
    let size_before_probe = file_size(&db_path);
    let (probe_wall, probe_committed) = rt.block_on(async {
        let probe_writer = Arc::new(LedgerWriter::new(
            Arc::clone(&conn),
            LedgerWriterConfig {
                queue_size: cfg.queue_size,
                batch_size: cfg.batch_size,
                batch_timeout_ms: cfg.batch_timeout_ms,
                instance_id: Some("bench-scale".to_string()),
            },
        ));
        reuse_probe(
            probe_writer,
            probe_rows,
            cfg.concurrency,
            Arc::clone(&consumers),
            cfg.seed,
        )
        .await
    });
    let (pages_after_probe, freelist_after_probe, _) = {
        let g = conn.lock();
        checkpoint(&g);
        page_stats(&g)
    };
    let size_after_probe = file_size(&db_path);
    let grew = size_after_probe.saturating_sub(size_before_probe);
    let fresh_bytes_per_record = size_after_checkpoint as f64 / cfg.rows as f64;
    println!(
        "  wall time:            {:.2} s ({:.0} records/s)",
        probe_wall.as_secs_f64(),
        probe_rows as f64 / probe_wall.as_secs_f64().max(1e-9)
    );
    println!("  ops committed:        {probe_committed}");
    println!(
        "  db file:              {} KiB -> {} KiB (grew {} KiB for {probe_rows} records)",
        size_before_probe as f64 / 1024.0,
        size_after_probe as f64 / 1024.0,
        grew as f64 / 1024.0
    );
    println!(
        "  growth per record:    {:.0} B/record, against {:.0} B/record when the same payloads \
         were written into a fresh database",
        grew as f64 / probe_rows as f64,
        fresh_bytes_per_record
    );
    println!(
        "  free pages:           {freelist_before_probe} -> {freelist_after_probe} \
         ({pages_before_probe} -> {pages_after_probe} pages in file)"
    );
    if grew as f64 / probe_rows as f64 <= fresh_bytes_per_record / 2.0 {
        println!(
            "  -> new traffic reuses the pages retention freed: the file does not grow by the \
             fresh-write cost, so steady-state size tracks live data, not cumulative traffic"
        );
    } else {
        println!(
            "  -> new traffic did NOT reuse the freed pages at the fresh-write rate: the file grew \
             almost as if the space had never been freed"
        );
    }

    // ---- cleanup ----
    println!();
    if cfg.keep {
        println!("[cleanup] BENCH_KEEP=1: kept {}", dir.display());
    } else {
        drop(reader);
        drop(conn);
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => println!("[cleanup] removed {}", dir.display()),
            Err(e) => println!("[cleanup] could not remove {}: {e}", dir.display()),
        }
    }
}

/// Time a query whose closure binds a single row.
fn time_query_manual<F>(
    conn: &Connection,
    label: &str,
    sql: &str,
    repeats: usize,
    mut run: F,
) -> QueryMeasurement
where
    F: FnMut(&mut rusqlite::Statement) -> rusqlite::Result<i64>,
{
    let mut runs = Vec::with_capacity(repeats);
    let mut last = 0i64;
    for _ in 0..repeats {
        let mut stmt = conn.prepare(sql).expect("prepare query");
        let t = Instant::now();
        last = run(&mut stmt).expect("run query");
        runs.push(t.elapsed());
    }
    // A single-row aggregate has no ids; keep the scalar for the printout.
    QueryMeasurement {
        label: label.to_string(),
        runs,
        items: vec![last.to_string()],
    }
}

/// Time a query that returns many rows, keeping the ids of the last run.
fn time_query_rows<F>(
    conn: &Connection,
    label: &str,
    sql: &str,
    repeats: usize,
    mut run: F,
) -> QueryMeasurement
where
    F: FnMut(&mut rusqlite::Statement) -> rusqlite::Result<Vec<String>>,
{
    let mut runs = Vec::with_capacity(repeats);
    let mut items = Vec::new();
    for i in 0..repeats {
        let mut stmt = conn.prepare(sql).expect("prepare query");
        let t = Instant::now();
        let out = run(&mut stmt).expect("run query");
        runs.push(t.elapsed());
        if i == repeats - 1 {
            items = out;
        }
    }
    QueryMeasurement {
        label: label.to_string(),
        runs,
        items,
    }
}
