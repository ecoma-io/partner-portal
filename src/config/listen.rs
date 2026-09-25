//! The listen address: an environment property, deliberately not config.
//!
//! A port is a property of the *deployment* — it must match the container
//! runtime's port mapping, the health-check URL and whatever proxies in front,
//! none of which read this process's YAML. Keeping it in the config file put
//! one half of that contract in a place the other halves cannot see, so
//! changing it meant editing two files in lockstep with nothing holding them
//! together. `PARTNER_PORTAL_LISTEN` follows the same pattern as
//! `PARTNER_PORTAL_CONFIG`: one variable, set where the deployment is
//! described (a compose file, a unit file, a shell), with a default that is
//! right for a bare process.

/// Environment variable holding the listen address.
pub const LISTEN_ENV: &str = "PARTNER_PORTAL_LISTEN";

/// The listen address used when [`LISTEN_ENV`] is unset or empty.
pub const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:8080";

/// Parse a listen address from an optional raw value.
///
/// `None` — and an empty or whitespace-only value, so a compose file may set
/// the variable empty — resolves to [`DEFAULT_LISTEN_ADDR`]. Anything else must
/// parse as a `SocketAddr`; the error names the variable *and* echoes the
/// rejected value, because "the port is wrong" without "which value I read" is
/// not something an operator can act on. This is a startup abort, never a
/// fall-back: binding somewhere the operator did not mean is worse than not
/// starting.
pub fn parse_listen_addr(raw: Option<&str>) -> Result<std::net::SocketAddr, String> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return DEFAULT_LISTEN_ADDR
            .parse()
            .map_err(|e| format!("{DEFAULT_LISTEN_ADDR:?} must parse as a default: {e}"));
    };
    raw.parse()
        .map_err(|e| format!("{LISTEN_ENV} is not a valid listen address {raw:?}: {e}"))
}

/// Read [`LISTEN_ENV`] from the process environment and parse it.
pub fn listen_addr() -> Result<std::net::SocketAddr, String> {
    parse_listen_addr(std::env::var(LISTEN_ENV).ok().as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_listen_addr_defaults_when_unset() {
        assert_eq!(
            parse_listen_addr(None).unwrap(),
            DEFAULT_LISTEN_ADDR.parse().unwrap()
        );
    }

    /// An empty value is "unset with extra steps" — a compose file that maps
    /// the variable from an undefined shell variable produces exactly this.
    #[test]
    fn test_parse_listen_addr_defaults_on_empty() {
        for raw in ["", "   "] {
            assert_eq!(
                parse_listen_addr(Some(raw)).unwrap(),
                DEFAULT_LISTEN_ADDR.parse().unwrap(),
                "a {raw:?} value must mean the default"
            );
        }
    }

    #[test]
    fn test_parse_listen_addr_accepts_a_valid_value() {
        let addr = parse_listen_addr(Some("127.0.0.1:9100")).unwrap();
        assert_eq!(addr.to_string(), "127.0.0.1:9100");
    }

    /// The startup-abort path: garbage must be refused with the variable name
    /// and the value in the error, so the operator can see what was read and
    /// from where.
    #[test]
    fn test_parse_listen_addr_rejects_garbage_with_the_value_in_the_error() {
        let err = parse_listen_addr(Some("nope")).unwrap_err();
        assert!(
            err.contains(LISTEN_ENV),
            "the error must name the variable: {err}"
        );
        assert!(err.contains("nope"), "the error must echo the value: {err}");
    }
}
