//! Partner Portal — a lightweight OpenAI-compatible reverse proxy.
//!
//! One upstream, three endpoints, local key authentication, and a durable usage
//! ledger with an embedded dashboard. It is deliberately **not** a
//! general-purpose LLM gateway: there is no routing, no multi-provider failover
//! and no request transformation.
//!
//! # The invariants this crate exists to uphold
//!
//! 1. **Metering is never dropped.** The ledger's queue applies backpressure
//!    rather than discarding; SQLite contention is retried, not skipped.
//! 2. **Every accepted request reaches a terminal state.** A record is durably
//!    `in_flight` before the upstream is contacted and resolved to `completed`,
//!    `failed` or `interrupted` afterwards — including by crash recovery.
//! 3. **Unavailable is not zero.** Usage the provider never reported stays
//!    `NULL`; nothing is fabricated to make a total look complete.
//! 4. **Raw and rollup agree.** Both are written in one transaction, and each
//!    request is rolled up exactly once.
//! 5. **Streaming stays incremental.** Bodies are forwarded frame by frame; the
//!    response is never buffered to recover usage.
//! 6. **Shutdown drains.** Readiness fails, in-flight work finishes, the metering
//!    pipeline drains and commits, and only then does the database close.
//! 7. **Dashboard data is key-scoped.** Consumer identity comes from the
//!    authenticated credential, never from the request. The one deliberate
//!    widening is the configured `manager:` password, which sees every
//!    consumer and narrows only through the `consumers=` filter (ADR 0011,
//!    ADR 0013); a deployment that never configures one is byte-for-byte the
//!    ADR 0008 behaviour.

pub mod admin;
pub mod auth;
pub mod config;
pub mod dashboard;
pub mod ledger;
pub mod proxy;
pub mod telemetry;
pub mod web;

pub use config::Config;
