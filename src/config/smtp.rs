//! The SMTP credential: an environment property, deliberately not config.
//!
//! # Why it is not in `billing.email`
//!
//! Everything else about the mail relay is in the YAML — host, port, sender —
//! because those are properties of the deployment's configuration. The secret is
//! not. A password in that file is a password in a file that gets committed to a
//! repository, pasted into a support thread and printed by `config.example.yaml`;
//! the file's permissions would be its only boundary, and the reload watcher
//! re-reads it every second.
//!
//! So it follows [`crate::config::listen`]: one variable per value, set where the
//! deployment is described (a compose file, a systemd unit, a secret manager's
//! environment injection), read once at start-up, held in memory, and never
//! written to the database, the config file or a log line.
//!
//! # Why the username travels with the password
//!
//! They are one credential. Reading them independently is how a deployment ends
//! up authenticating as the wrong account — the password rotated and the username
//! not, or the two set in different places — and the failure looks like "the
//! relay rejected us", which points at the password. They are read together and
//! an incomplete pair is a start-up abort that names both variables.

/// Environment variable holding the SMTP username.
pub const SMTP_USERNAME_ENV: &str = "PARTNER_PORTAL_SMTP_USERNAME";

/// Environment variable holding the SMTP password.
pub const SMTP_PASSWORD_ENV: &str = "PARTNER_PORTAL_SMTP_PASSWORD";

/// A username and password to authenticate to the relay with.
///
/// `Debug` is written by hand for the same reason [`crate::config::types::UpstreamConfig`]'s
/// is: this type exists to carry a secret, so a derived `Debug` would put it into
/// any `{:?}` of a value that holds one — including a panic message.
#[derive(Clone, PartialEq, Eq)]
pub struct SmtpCredentials {
    username: String,
    password: String,
}

impl SmtpCredentials {
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    /// The password, for the one caller that has to send it.
    ///
    /// Named for what it invites: every other accessor of this type reveals a
    /// non-secret, and this one does not.
    pub fn password(&self) -> &str {
        &self.password
    }
}

impl std::fmt::Debug for SmtpCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmtpCredentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Resolve the credential from the two raw values.
///
/// Both unset — or set but empty, which is what a compose file produces when it
/// maps an undefined shell variable — is `Ok(None)`: an anonymous relay, which is
/// the normal shape of a sidecar on the same host and is why this is not an
/// error. An SMTP server that needs no credential is not a misconfiguration.
///
/// Half a credential is an error, and it names both variables rather than the one
/// that is missing: the operator set one of them somewhere and needs to find the
/// other, and the message must not reveal which value was present.
pub fn parse_credentials(
    username: Option<&str>,
    password: Option<&str>,
) -> Result<Option<SmtpCredentials>, String> {
    let username = username.map(str::trim).filter(|v| !v.is_empty());
    let password = password.map(str::trim).filter(|v| !v.is_empty());

    match (username, password) {
        (None, None) => Ok(None),
        (Some(u), Some(p)) => Ok(Some(SmtpCredentials::new(u.to_string(), p.to_string()))),
        _ => Err(format!(
            "{SMTP_USERNAME_ENV} and {SMTP_PASSWORD_ENV} must be set together: \
             an SMTP credential is one value, and half of one cannot authenticate"
        )),
    }
}

/// Read both variables from the process environment.
pub fn credentials_from_env() -> Result<Option<SmtpCredentials>, String> {
    parse_credentials(
        std::env::var(SMTP_USERNAME_ENV).ok().as_deref(),
        std::env::var(SMTP_PASSWORD_ENV).ok().as_deref(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_both_variables_set_produce_a_credential() {
        let creds = parse_credentials(Some("portal@acme.test"), Some("s3cret"))
            .unwrap()
            .expect("both set");
        assert_eq!(creds.username(), "portal@acme.test");
        assert_eq!(creds.password(), "s3cret");
    }

    /// The sidecar case: a relay on the same host that wants no credential at
    /// all. Not an error, and not a partially-configured credential either.
    #[test]
    fn test_neither_variable_is_an_anonymous_relay_not_a_failure() {
        assert_eq!(parse_credentials(None, None).unwrap(), None);
        // An empty value is "unset with extra steps" — a compose file that maps
        // the variable from an undefined shell variable produces exactly this.
        assert_eq!(parse_credentials(Some(""), Some("")).unwrap(), None);
        assert_eq!(parse_credentials(Some("  "), Some(""),).unwrap(), None);
    }

    /// Half a credential never authenticates, so it is refused rather than
    /// silently downgraded to an anonymous send — which would be a rejected
    /// message, or worse, an accepted one from the wrong account.
    #[test]
    fn test_half_a_credential_is_refused_and_names_both_variables() {
        for (user, pass) in [
            (Some("portal@acme.test"), None),
            (None, Some("s3cret")),
            (Some("portal@acme.test"), Some("")),
            (Some(""), Some("s3cret")),
        ] {
            let err =
                parse_credentials(user, pass).expect_err("half a credential must not be accepted");
            assert!(err.contains(SMTP_USERNAME_ENV), "{err}");
            assert!(err.contains(SMTP_PASSWORD_ENV), "{err}");
        }
    }

    /// The credential is never rendered by `Debug`, which is what any `{:?}` of
    /// a config-adjacent struct will use.
    #[test]
    fn test_the_password_is_never_rendered() {
        let creds = SmtpCredentials::new("portal@acme.test".into(), "hunter2-hunter2".into());
        let rendered = format!("{creds:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(rendered.contains("portal@acme.test"), "{rendered}");
    }
}
