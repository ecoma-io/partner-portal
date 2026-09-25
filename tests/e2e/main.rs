//! End-to-end tests.
//!
//! These drive the **real binary** as a child process — real signals, real
//! SQLite file, real HTTP — because the behaviour they cover is defined by what
//! the process does at the operating-system boundary.
//!
//! ```text
//!   cargo test --test e2e -- --test-threads=1
//! ```
//!
//! Run them serially: each test spawns several processes and drives real
//! traffic, and letting them overlap makes failures harder to attribute.
//!
//! | File | What it proves |
//! |---|---|
//! | `rolling_update` | two instances writing one database lose and duplicate nothing |
//! | `hot_reload` | a config change lands within ~1s; a broken one is refused |
//! | `dashboard_isolation` | a key sees only its own usage; the client cannot choose its identity |
//! | `cross_instance_sse` | an instance notifies about a write another process made |
//! | `shutdown` | a graceful shutdown completes and commits everything it accepted |
//!
//! The files share the harness in `harness.rs`. `#![allow(dead_code)]` is on it
//! because each test file uses a different subset of it.

mod harness;

mod cross_instance_sse;
mod dashboard_isolation;
mod hot_reload;
mod rolling_update;
mod shutdown;
