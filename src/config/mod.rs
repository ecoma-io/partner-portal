//! Configuration management with hot-reload support
//!
//! Configuration is loaded from a YAML file and hot-reloaded every second.
//! Invalid configurations are rejected without affecting the current config.

mod hot_reload;
mod listen;
mod loader;
mod types;

/// Environment variable that names the configuration file to read.
///
/// One variable, read by the server *and* by `partner-portal keygen`, because
/// both have to agree on which database they are talking about — a utility that
/// found its configuration differently from the server would issue keys into a
/// file the server never opens.
pub const CONFIG_ENV: &str = "PARTNER_PORTAL_CONFIG";

pub use hot_reload::{ConfigSnapshot, HotReloader};
pub use listen::{DEFAULT_LISTEN_ADDR, LISTEN_ENV, listen_addr, parse_listen_addr};
pub use loader::{ConfigError, ConfigLoader};
pub use types::{Config, DatabaseConfig, ServerConfig, UpstreamConfig, redact_credentials};
