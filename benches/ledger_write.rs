//! Criterion benchmarks for the metering write path.
//!
//! Everything here goes through the **real** [`LedgerWriter`] API — the bounded
//! queue, the single writer task, the micro-batch and the one transaction per
//! batch that inserts the raw row and upserts the hourly rollup. No benchmark
//! issues raw SQL against a connection, because the thing under test is the
//! pipeline, not SQLite.
//!
//! # What is measured
//!
//! | group | question |
//! |---|---|
//! | `ledger_ingest_throughput` | records/sec through `accept()` + `finalize()` at batch sizes 1/10/100/1000 |
//! | `ledger_rollup_upsert` | cost of the `ON CONFLICT DO UPDATE` rollup when every record lands in one bucket vs spread across hours/models/consumers |
//! | `ledger_retention_sweep` | cost of one bounded retention sweep over a large table |
//! | *(printed report)* | commit-ack latency distribution p50/p95/p99 as seen by a producer awaiting the writer's ack |
//!
//! # Shape of the workload
//!
//! Producers are `tokio` tasks that behave like the proxy's request handlers:
//! `accept()` (awaited, because the record must be durable before the upstream
//! is contacted), then `finalize()` (awaited). Two ops per record. No simulated
//! upstream latency is inserted, so the numbers are the *metering pipeline's*
//! ceiling, not end-to-end request throughput.
//!
//! # Running
//!
//! ```text
//! cargo bench --bench ledger_write                       # full run (a few minutes)
//! cargo bench --bench ledger_write -- --list             # discover the groups
//! cargo bench --bench ledger_write -- --test             # one iteration per bench (smoke)
//! cargo bench --bench ledger_write -- --sample-size 10 --measurement-time 3
//! ```
//!
//! Environment knobs (all optional):
//! `BENCH_KEEP=1` keeps the scratch databases, `BENCH_DB_DIR=<path>` moves them
//! (the default is `target/bench-tmp`, because `/tmp` is often a tmpfs),
//! `BENCH_LATENCY_ROWS` changes the sample count of the printed latency report.
//!
//! Release profile only: `cargo bench` builds with the `bench` profile, which
//! inherits `[profile.release]` (thin LTO, `codegen-units = 1`) and turns debug
//! assertions off.

use criterion::{BenchmarkId, Criterion, Throughput};
use parking_lot::Mutex;
use partner_portal::ledger::writer::{LedgerWriter, LedgerWriterConfig};
use partner_portal::ledger::{Endpoint, RequestRecord, Usage, configure_sqlite, init_schema};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

// --------------------------------------------------------------------- knobs

/// Criterion samples per benchmark. 10 keeps the whole suite in a few minutes
/// while still producing a confidence interval; raise it for a release report.
const SAMPLES: usize = 10;
const MEASURE_SECS: u64 = 5;
const WARMUP_SECS: u64 = 2;

/// Batch window used by every benchmark unless stated otherwise. This is
/// [`LedgerWriterConfig::default`]'s value. The shipped config file defaults to
/// 1000 ms instead (see `DatabaseConfig::batch_timeout_ms`); the window is the
/// dominant latency term when the offered rate is below `batch_size`, which the
/// printed latency report makes explicit.
const BATCH_WINDOW_MS: u64 = 10;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn scratch_root() -> PathBuf {
    match std::env::var("BENCH_DB_DIR") {
        Ok(dir) => PathBuf::from(dir),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("bench-tmp"),
    }
}

/// A scratch directory that is removed when it goes out of scope, unless
/// `BENCH_KEEP=1`. The databases live here rather than in `/tmp` because `/tmp`
/// is frequently a tmpfs, which would make "disk" measurements meaningless.
struct Scratch {
    dir: PathBuf,
}

static SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

impl Scratch {
    fn new(tag: &str) -> Self {
        let uniq = SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = scratch_root().join(format!("{tag}-{}-{uniq}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        Self { dir }
    }

    fn db_path(&self) -> PathBuf {
        self.dir.join("ledger.db")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if std::env::var("BENCH_KEEP").is_ok() {
            eprintln!("BENCH_KEEP=1: kept {}", self.dir.display());
            return;
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ------------------------------------------------------------------ database

fn open_db(path: &Path) -> Arc<Mutex<Connection>> {
    let conn = Connection::open(path).expect("open scratch db");
    configure_sqlite(&conn).expect("configure sqlite");
    init_schema(&conn).expect("init schema");
    Arc::new(Mutex::new(conn))
}

fn new_writer(conn: Arc<Mutex<Connection>>, batch_size: usize, window_ms: u64) -> LedgerWriter {
    LedgerWriter::new(
        conn,
        LedgerWriterConfig {
            queue_size: env_usize("BENCH_QUEUE_SIZE", 10_000),
            batch_size,
            batch_timeout_ms: window_ms,
            // Production always stamps an owning instance on each accepted row;
            // the benchmark does the same so the stored row has its real shape.
            instance_id: Some("bench".to_string()),
        },
    )
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(|n| n.get().min(8))
                .unwrap_or(4),
        )
        .enable_all()
        .build()
        .expect("build tokio runtime")
}

// ------------------------------------------------------- latency histogram

/// Log-linear latency histogram, microsecond resolution below 1 ms.
///
/// Percentiles are reported as the upper bound of the bucket that contains
/// them, so they are conservative, never optimistic.
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

fn fmt_ms(d: Duration) -> String {
    format!("{:.3}", d.as_secs_f64() * 1_000.0)
}

// ------------------------------------------------------------ the workload

const CONSUMERS: [&str; 8] = [
    "acme",
    "globex",
    "initech",
    "umbrella",
    "hooli",
    "stark",
    "wayne",
    "cyberdyne",
];

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

/// How the `created_at` / consumer / model dimensions of the workload vary.
#[derive(Clone, Copy)]
enum Shape {
    /// Live traffic: timestamps advance monotonically with a sub-millisecond
    /// jitter, and the consumer/model/endpoint/streaming mix is realistic.
    Live {
        base: OffsetDateTime,
        span_secs: i64,
    },
    /// Every record lands in exactly one `(hour, consumer, model, endpoint,
    /// streaming)` bucket: the cheapest possible rollup.
    SameBucket { base: OffsetDateTime },
    /// One consumer/model, but timestamps spread across `span_secs` so the
    /// rollup fans out over many hour buckets.
    SpreadHours {
        base: OffsetDateTime,
        span_secs: i64,
    },
    /// Hours, models and consumers all vary: the widest the rollup index gets.
    SpreadAll {
        base: OffsetDateTime,
        span_secs: i64,
    },
}

impl Shape {
    fn live() -> Self {
        Shape::Live {
            base: OffsetDateTime::now_utc() - time::Duration::days(75),
            span_secs: 75 * 86_400,
        }
    }
}

#[derive(Clone, Copy)]
struct Job {
    records: usize,
    concurrency: usize,
    shape: Shape,
    /// `true` = full request lifecycle (`accept` then `finalize`). `false` =
    /// the single-batch fast path, where a terminal record is written directly.
    with_accept: bool,
    tag: u64,
}

/// Deterministic per-record payload hash (SplitMix64 finalizer).
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Build the record for slot `seq` of a `total`-record job, in `in_flight`.
fn record_for(shape: Shape, seq: usize, total: usize, tag: u64) -> RequestRecord {
    let h = mix((seq as u64) ^ tag.rotate_left(17) ^ 0x51ED_2701);
    let total = total.max(1) as f64;

    let (created, consumer, model, streaming, endpoint) = match shape {
        Shape::Live { base, span_secs } => {
            let step = span_secs as f64 / total;
            let secs = (seq as f64 * step) as i64;
            let jitter_us = (h % 20_000) as i64; // up to 20 ms of reordering
            (
                base + time::Duration::seconds(secs) + time::Duration::microseconds(jitter_us),
                CONSUMERS[(h as usize / 3) % CONSUMERS.len()],
                MODELS[(h as usize / 7) % MODELS.len()],
                h % 10 < 3,
                if h % 10 < 9 {
                    Endpoint::ChatCompletions
                } else {
                    Endpoint::Responses
                },
            )
        }
        Shape::SameBucket { base } => (
            base,
            CONSUMERS[0],
            MODELS[0],
            false,
            Endpoint::ChatCompletions,
        ),
        Shape::SpreadHours { base, span_secs } => {
            let step = span_secs as f64 / total;
            let secs = (seq as f64 * step) as i64;
            (
                base + time::Duration::seconds(secs),
                CONSUMERS[0],
                MODELS[0],
                false,
                Endpoint::ChatCompletions,
            )
        }
        Shape::SpreadAll { base, span_secs } => {
            let step = span_secs as f64 / total;
            let secs = (seq as f64 * step) as i64;
            (
                base + time::Duration::seconds(secs),
                CONSUMERS[(h as usize / 3) % CONSUMERS.len()],
                MODELS[(h as usize / 7) % MODELS.len()],
                h % 10 < 3,
                if h % 10 < 9 {
                    Endpoint::ChatCompletions
                } else {
                    Endpoint::Responses
                },
            )
        }
    };

    let mut record = RequestRecord::new(
        uuid::Uuid::now_v7().to_string(),
        consumer.to_string(),
        model.to_string(),
        endpoint,
        streaming,
    );
    record.created_at = created;
    record
}

/// Move a record to a terminal state, with a realistic outcome mix:
/// ~2% upstream failures, ~2% with usage the provider never reported (NULL
/// tokens rather than fabricated zeros), the rest completed.
fn terminalize(record: &mut RequestRecord, seq: usize, tag: u64) {
    let h = mix((seq as u64) ^ tag.rotate_left(17) ^ 0x51ED_2701);
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

/// One producer task's view of the workload.
async fn produce(writer: Arc<LedgerWriter>, job: Job) -> Meter {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(job.concurrency);

    for _ in 0..job.concurrency {
        let writer = Arc::clone(&writer);
        let counter = Arc::clone(&counter);
        handles.push(tokio::spawn(async move {
            let mut meter = Meter::default();
            loop {
                let seq = counter.fetch_add(1, Ordering::Relaxed);
                if seq >= job.records {
                    break;
                }

                let mut record = record_for(job.shape, seq, job.records, job.tag);

                if job.with_accept {
                    let t = Instant::now();
                    writer
                        .accept(record.clone())
                        .await
                        .expect("accept must commit");
                    meter.accept.record(t.elapsed());
                }

                terminalize(&mut record, seq, job.tag);

                let t = Instant::now();
                writer.finalize(record).await.expect("finalize must commit");
                meter.finalize.record(t.elapsed());
            }
            meter
        }));
    }

    let mut total = Meter::default();
    for handle in handles {
        total.merge(&handle.await.expect("producer task panicked"));
    }
    total
}

// ------------------------------------------------------------- benchmarks

/// Records/sec of the ingest path at several batch sizes.
///
/// Concurrency is raised with the batch size so the writer can actually fill a
/// batch: a batch cannot be larger than the number of ops in flight, and with a
/// 10 ms window the window becomes the throughput limiter once `batch_size`
/// exceeds concurrency. That is a real property of the design, and it is why
/// the 1000-row batch does not simply scale.
fn ingest_throughput(c: &mut Criterion) {
    let rt = Arc::new(runtime());
    let mut group = c.benchmark_group("ledger_ingest_throughput");
    group.sample_size(SAMPLES);
    group.measurement_time(Duration::from_secs(MEASURE_SECS));
    group.warm_up_time(Duration::from_secs(WARMUP_SECS));

    // (batch_size, concurrency, records per iteration)
    let configs: [(usize, usize, usize); 4] = [
        (1, 32, 1_000),
        (10, 64, 4_000),
        (100, 200, 20_000),
        (1_000, 512, 20_000),
    ];

    for (batch, concurrency, records) in configs {
        group.throughput(Throughput::Elements(records as u64));
        let rt = Arc::clone(&rt);
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch),
            &(batch, concurrency, records),
            |b, &(batch, concurrency, records)| {
                let rt = Arc::clone(&rt);
                b.iter_custom(move |iters| {
                    let rt = Arc::clone(&rt);
                    rt.block_on(async move {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let scratch = Scratch::new("ingest");
                            let conn = open_db(&scratch.db_path());
                            let writer = Arc::new(new_writer(conn, batch, BATCH_WINDOW_MS));
                            let job = Job {
                                records,
                                concurrency,
                                shape: Shape::live(),
                                with_accept: true,
                                tag: SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed),
                            };

                            let t = Instant::now();
                            let _ = produce(Arc::clone(&writer), job).await;
                            total += t.elapsed();

                            writer.shutdown().await;
                        }
                        total
                    })
                });
            },
        );
    }
    group.finish();
}

/// Cost of the hourly rollup upsert: one hot bucket vs many.
///
/// Uses the single-batch fast path (`finalize` for a record that has no accept
/// row), which is the production path for a request whose accept and finalize
/// land in the same batch. Every record still inserts a raw row, so what
/// changes between the three benchmarks is only the rollup index fan-out.
fn rollup_upsert(c: &mut Criterion) {
    let rt = Arc::new(runtime());
    let mut group = c.benchmark_group("ledger_rollup_upsert");
    group.sample_size(SAMPLES);
    group.measurement_time(Duration::from_secs(MEASURE_SECS));
    group.warm_up_time(Duration::from_secs(WARMUP_SECS));

    const RECORDS: usize = 20_000;
    const CONCURRENCY: usize = 256;

    let base = OffsetDateTime::now_utc() - time::Duration::days(60);
    let shapes: [(&str, Shape); 3] = [
        ("same_bucket", Shape::SameBucket { base }),
        (
            "spread_60d_hours",
            Shape::SpreadHours {
                base,
                span_secs: 60 * 86_400,
            },
        ),
        (
            "spread_60d_hours_models_consumers",
            Shape::SpreadAll {
                base,
                span_secs: 60 * 86_400,
            },
        ),
    ];

    group.throughput(Throughput::Elements(RECORDS as u64));
    for (name, shape) in shapes {
        let rt = Arc::clone(&rt);
        group.bench_with_input(BenchmarkId::from_parameter(name), &name, |b, _| {
            let rt = Arc::clone(&rt);
            b.iter_custom(move |iters| {
                let rt = Arc::clone(&rt);
                rt.block_on(async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let scratch = Scratch::new("rollup");
                        let conn = open_db(&scratch.db_path());
                        let writer = Arc::new(new_writer(conn, 100, BATCH_WINDOW_MS));
                        let job = Job {
                            records: RECORDS,
                            concurrency: CONCURRENCY,
                            shape,
                            with_accept: false,
                            tag: SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed),
                        };

                        let t = Instant::now();
                        let _ = produce(Arc::clone(&writer), job).await;
                        total += t.elapsed();

                        writer.shutdown().await;
                    }
                    total
                })
            });
        });
    }
    group.finish();
}

/// One bounded retention sweep over a table with ~200k rows spanning 120 days,
/// so half of it is past a 60-day cutoff.
///
/// The table is rebuilt before each timed iteration and the rebuild is *not*
/// included in the measurement (`iter_custom` times only the sweep). The sweep
/// itself is the production call: sliced deletes with the production defaults
/// (2000 rows per slice, 500 slices max) plus the bounded
/// `PRAGMA incremental_vacuum` at the end.
fn retention_sweep(c: &mut Criterion) {
    const ROWS: usize = 200_000;
    const ELIGIBLE_ROWS: usize = ROWS / 2; // 60 of the 120 days
    const CONCURRENCY: usize = 256;

    let rt = Arc::new(runtime());
    let mut group = c.benchmark_group("ledger_retention_sweep");
    group.sample_size(SAMPLES);
    group.measurement_time(Duration::from_secs(MEASURE_SECS));
    group.warm_up_time(Duration::from_secs(WARMUP_SECS));
    group.throughput(Throughput::Elements(ELIGIBLE_ROWS as u64));

    let base = OffsetDateTime::now_utc() - time::Duration::days(120);
    group.bench_function("60d_cutoff_over_200k_rows", |b| {
        let rt = Arc::clone(&rt);
        b.iter_custom(move |iters| {
            let rt = Arc::clone(&rt);
            rt.block_on(async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let scratch = Scratch::new("retention");
                    let conn = open_db(&scratch.db_path());
                    let writer = Arc::new(new_writer(Arc::clone(&conn), 100, BATCH_WINDOW_MS));
                    let job = Job {
                        records: ROWS,
                        concurrency: CONCURRENCY,
                        shape: Shape::Live {
                            base,
                            span_secs: 120 * 86_400,
                        },
                        with_accept: false,
                        tag: SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed),
                    };
                    let _ = produce(Arc::clone(&writer), job).await;
                    writer.shutdown().await;

                    // --- timed region: exactly one production sweep ---
                    let t = Instant::now();
                    let stats = partner_portal::ledger::retention::run_retention(
                        &conn,
                        60,
                        partner_portal::ledger::retention::DEFAULT_BATCH_SIZE,
                        partner_portal::ledger::retention::DEFAULT_MAX_BATCHES,
                    )
                    .expect("retention sweep");
                    total += t.elapsed();

                    debug_assert_eq!(stats.raw_deleted as usize, ELIGIBLE_ROWS);
                }
                total
            })
        });
    });
    group.finish();
}

// ------------------------------------------------- printed latency report

/// Per-record commit-ack latency as observed by a producer awaiting the
/// writer's ack, for every batch size.
///
/// Criterion reports a mean and a median; the product's question is about the
/// tail (a request's accept must commit before the upstream is contacted), so
/// this prints the full p50/p95/p99 distribution instead. It runs before the
/// criterion groups and takes a couple of seconds.
fn ack_latency_report() {
    let rows = env_usize("BENCH_LATENCY_ROWS", 2_000);
    let concurrency = env_usize("BENCH_LATENCY_CONCURRENCY", 32);
    let rt = runtime();

    println!();
    println!("=== ledger ack latency distribution (real writer path) ===");
    println!(
        "records per config: {rows}, producers: {concurrency}, batch window: {BATCH_WINDOW_MS} ms"
    );
    println!(
        "{:>6}  {:>10}  {:>8}  {:>8}  {:>8}  {:>10}  {:>8}  {:>8}  {:>8}  {:>10}",
        "batch",
        "rows/s",
        "acc p50",
        "acc p95",
        "acc p99",
        "acc mean",
        "fin p50",
        "fin p95",
        "fin p99",
        "fin mean"
    );
    println!("{}", "-".repeat(110));

    for batch in [1usize, 10, 100, 1_000] {
        let job = Job {
            records: rows,
            concurrency,
            shape: Shape::live(),
            with_accept: true,
            tag: SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed),
        };

        // The writer spawns its task on the runtime, so it must be constructed
        // from inside the runtime context.
        let (meter, elapsed) = rt.block_on(async move {
            let scratch = Scratch::new("latency");
            let conn = open_db(&scratch.db_path());
            let writer = Arc::new(new_writer(conn, batch, BATCH_WINDOW_MS));

            let t = Instant::now();
            let meter = produce(Arc::clone(&writer), job).await;
            let elapsed = t.elapsed();

            writer.shutdown().await;
            (meter, elapsed)
        });

        let rate = rows as f64 / elapsed.as_secs_f64();
        println!(
            "{:>6}  {:>10.0}  {:>8}  {:>8}  {:>8}  {:>10}  {:>8}  {:>8}  {:>8}  {:>10}",
            batch,
            rate,
            fmt_ms(meter.accept.percentile(0.50)),
            fmt_ms(meter.accept.percentile(0.95)),
            fmt_ms(meter.accept.percentile(0.99)),
            fmt_ms(meter.accept.mean()),
            fmt_ms(meter.finalize.percentile(0.50)),
            fmt_ms(meter.finalize.percentile(0.95)),
            fmt_ms(meter.finalize.percentile(0.99)),
            fmt_ms(meter.finalize.mean()),
        );
        println!(
            "        samples: accept={} finalize={}",
            meter.accept.count(),
            meter.finalize.count()
        );
    }
    println!("(latencies in ms; percentiles are the upper bound of the containing");
    println!(" histogram bucket, so they are never optimistic)");
    println!();
}

// -------------------------------------------------------------------- main

criterion::criterion_group!(
    ledger_benches,
    ingest_throughput,
    rollup_upsert,
    retention_sweep
);

fn main() {
    // `--list` must not pay for the latency report.
    let listing = std::env::args().any(|a| a == "--list");
    if !listing {
        ack_latency_report();
    }
    ledger_benches();
    Criterion::default().configure_from_args().final_summary();
}
