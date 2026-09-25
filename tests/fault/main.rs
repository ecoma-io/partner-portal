//! Fault-injection suite: what the ledger must guarantee when the process, the
//! database, or the metering pipeline misbehaves.
//!
//! ```text
//!   cargo test --test fault
//! ```
//!
//! These tests are slower than the integration suite by nature — they kill
//! processes, hold database locks, and wait for retry backoff — but each one is
//! bounded and none of them sleeps "long enough": every wait is a condition with
//! a deadline.

#[path = "../common/mod.rs"]
mod common;

mod db_busy;
mod kill_mid_request;
mod queue_saturation;
mod readiness;
mod term_drain;
mod writer_failure;
