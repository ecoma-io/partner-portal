//! Read-path probe: cost of opening a fresh reader connection per query.
//!
//! The dashboard does `pool.read(...)`, which opens a NEW connection and runs
//! `configure_sqlite` on it for EVERY query (`src/ledger/pool.rs::reader`).
//! The scale benchmark (§5 of docs/performance.md) reuses ONE reader for all
//! its queries, so it excludes this per-query open cost. This probe measures it.
//!
//! It creates a scratch database in target/, configures+schemas it, inserts a
//! modest number of rows so the file is nontrivial, then times, in isolation:
//!   (a) plain `Connection::open`
//!   (b) open + configure_sqlite (the dashboard's per-query cost)
//!   (c) the full dashboard pattern: open + configure + one summary query
//!     reading `usage_hourly` for one consumer
//!   (d) same as (c) but on a REUSED connection (what §5 measures)
//! so the per-query overhead of the fresh-connection design is visible.
//!
//! Run:  cargo run --release --example read_probe
#![allow(clippy::print_stdout)]

use partner_portal::ledger::{configure_sqlite, init_schema};
use rusqlite::{Connection, params};
use std::path::PathBuf;
use std::time::Instant;

fn timing<F: FnMut()>(iters: usize, mut f: F) -> (f64, f64) {
    // warmup
    f();
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let s = Instant::now();
        f();
        samples.push(s.elapsed().as_secs_f64() * 1000.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (samples[iters / 2], samples[iters - 1])
}

fn main() {
    let scratch = PathBuf::from("target/read-probe");
    std::fs::create_dir_all(&scratch).expect("create scratch");
    let db_path = scratch.join("read-probe.db");
    let _ = std::fs::remove_file(&db_path);

    let mut c = Connection::open(&db_path).expect("open");
    configure_sqlite(&c).expect("configure");
    init_schema(&c).expect("schema");

    // Insert 200k rows for one consumer across a few hours so the summary query
    // has something to scan. Not timed. One transaction: this is a data-setup
    // concern, not the subject of the measurement.
    let tx = c.transaction().expect("begin tx");
    let mut stmt = tx
        .prepare(
            "INSERT INTO usage_records
             (request_id, consumer_id, model, endpoint, streaming,
              http_status, request_status, usage_status,
              input_tokens, output_tokens, cached_tokens,
              duration_ms, ttft_ms, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        )
        .expect("prepare insert");
    for i in 0..200_000i64 {
        stmt.execute(params![
            format!("req-{i}"),
            "consumer-00",
            "gpt-4o",
            "chat_completions",
            0i64,
            200i64,
            "completed",
            "available",
            100i64,
            100i64,
            0i64,
            1000i64,
            200i64,
            format!("2026-09-23T{:02}:00:00.000000000Z", (i % 24)),
        ])
        .expect("insert");
    }
    drop(stmt);
    tx.commit().expect("commit tx");
    drop(c);

    let summary_sql = "SELECT COUNT(*), COALESCE(SUM(total_input_tokens), 0), \
                       COALESCE(SUM(total_output_tokens), 0), \
                       COALESCE(AVG(total_duration_ms), 0) \
                       FROM usage_hourly WHERE consumer_id = ?1";
    let sum_row = "SELECT 1 FROM usage_hourly WHERE consumer_id = ?1 LIMIT 1";

    // (d) reused connection: what §5 measures.
    let reused = Connection::open(&db_path).expect("open");
    configure_sqlite(&reused).expect("configure");
    let (reused_p50, reused_p99) = timing(2000, || {
        let _: i64 = reused
            .query_row(summary_sql, params!["consumer-00"], |r| r.get(0))
            .expect("query");
    });

    // (a) plain open.
    let (plain_p50, plain_p99) = timing(2000, || {
        let conn = Connection::open(&db_path).expect("open");
        drop(conn);
    });

    // (b) open + configure (no query).
    let (config_p50, config_p99) = timing(2000, || {
        let conn = Connection::open(&db_path).expect("open");
        configure_sqlite(&conn).expect("configure");
        drop(conn);
    });

    // (c) full dashboard pattern: open + configure + one summary query.
    let (open_query_p50, open_query_p99) = timing(2000, || {
        let conn = Connection::open(&db_path).expect("open");
        configure_sqlite(&conn).expect("configure");
        let _: i64 = conn
            .query_row(summary_sql, params!["consumer-00"], |r| r.get(0))
            .expect("query");
    });

    // The SQLite meta-table count configure_sqlite runs per open.
    let (meta_p50, meta_p99) = timing(2000, || {
        let conn = Connection::open(&db_path).expect("open");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM sqlite_master", [], |r| r.get(0))
            .expect("count");
    });

    let db_size = std::fs::metadata(&db_path)
        .map(|m| m.len() / (1024 * 1024))
        .unwrap_or(0);
    println!("read-path probe");
    println!(
        "  database: {} ({db_size} MiB, 200000 raw rows)",
        db_path.display()
    );
    println!("  per-operation p50 / p99 (ms, 2000 iters):");
    println!("    plain Connection::open                : {plain_p50:.4} / {plain_p99:.4}");
    println!("    open + configure_sqlite               : {config_p50:.4} / {config_p99:.4}");
    println!(
        "    configure_sqlite alone (delta)        : {:.4} / {:.4}",
        config_p50 - plain_p50,
        config_p99 - plain_p99
    );
    println!("    sqlite_master count (part of configure): {meta_p50:.4} / {meta_p99:.4}");
    println!(
        "    open + configure + summary QUERY      : {open_query_p50:.4} / {open_query_p99:.4}"
    );
    println!("    REUSED conn + same summary query (§5) : {reused_p50:.4} / {reused_p99:.4}");
    println!(
        "    per-query open overhead (open_query - reused): {:.4} / {:.4}",
        open_query_p50 - reused_p50,
        open_query_p99 - reused_p99
    );

    let _ = sum_row;
    let _ = std::fs::remove_dir_all(&scratch);
}
