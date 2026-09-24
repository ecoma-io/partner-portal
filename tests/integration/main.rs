//! Integration tests: the real binary as a child process, a mock upstream on an
//! ephemeral port, and the ledger read back from SQLite.
//!
//! ```text
//!   cargo test --test integration
//! ```
//!
//! Each module owns one surface. Run a single module with a filter, e.g.
//! `cargo test --test integration -- auth::`.

#[path = "../common/mod.rs"]
mod common;

mod auth;
mod dashboard;
mod hot_reload;
mod ledger_durability;
mod manager;
mod model_allow_list;
mod proxy_chat_completions;
mod sse;
