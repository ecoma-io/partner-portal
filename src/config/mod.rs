//! Configuration management with hot-reload support
//!
//! Configuration is loaded from a YAML file and hot-reloaded every second.
//! Invalid configurations are rejected without affecting the current config.

mod hot_reload;
mod listen;
mod loader;
mod types;

pub use hot_reload::{ConfigSnapshot, HotReloader};
pub use listen::{DEFAULT_LISTEN_ADDR, LISTEN_ENV, listen_addr, parse_listen_addr};
pub use loader::{ConfigError, ConfigLoader};
pub use types::{
    Config, DatabaseConfig, KeyConfig, ServerConfig, UpstreamConfig, redact_credentials,
};
