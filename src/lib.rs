//! Partner Portal - Lightweight OpenAI-compatible reverse proxy
//!
//! This crate provides a minimal reverse proxy that:
//! - Proxies requests to a single OpenAI-compatible upstream
//! - Performs local API-key authentication with upstream key replacement
//! - Records usage metrics to SQLite with 60-day retention
//! - Provides an embedded Vue dashboard for usage inspection

pub mod admin;
pub mod auth;
pub mod config;
pub mod dashboard;
pub mod ledger;
pub mod proxy;
pub mod telemetry;

pub use config::Config;
