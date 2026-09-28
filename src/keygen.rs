//! `partner-portal keygen` — first-run provisioning of a partner API key.
//!
//! # The gap this closes
//!
//! Keys live in the database (ADR 0014) and the only runtime surface that mints
//! one is `POST /api/admin/api-keys`, which answers to the manager password. An
//! operator who has the config file can therefore always reach the admin API —
//! access was never the problem. What a fresh deployment lacks is a *convenient*
//! first step: a shell has no way to authenticate, compare, and issue without
//! either reaching for a JSON client or trusting a bootstrap key handed to it
//! through the environment.
//!
//! So this is a one-shot CLI subcommand, and deliberately nothing more. It is
//! **not** part of the runtime architecture: the server never invokes it, it
//! holds no state, and nothing about serving a request depends on it having run.
//! An operator runs it once per key, against the same database and the same
//! secret the server uses — named by the configuration file, or by `--database`
//! when there is no server to share a configuration with.
//!
//! # The secret is half the operation
//!
//! `api_keys.key_hash` is `HMAC-SHA256(PARTNER_PORTAL_API_KEY_SECRET, plaintext)`
//! — see [`crate::apikeys`]. A key issued under a different secret than the
//! server holds is a string that authenticates nothing, and the failure is
//! silent until a partner reports a 401. That is why this reads the secret from
//! the same environment variable the server does, and refuses to run without it
//! rather than falling back to anything.
//!
//! # Output contract
//!
//! The plaintext is printed to **stdout and nowhere else** — one line, so
//! `KEY=$(partner-portal keygen ...)` works in a script. Everything an operator
//! reads (the id, the name, the prefix, the warning about the secret) goes to
//! stderr, so capturing the key cannot capture a file that also names it. This
//! is the same rule the admin API follows: the plaintext exists in exactly one
//! response, once.

use std::fmt::Write as _;
use std::path::Path;

use crate::apikeys::{ApiKeyStore, load_secret};
use crate::config::{CONFIG_ENV, ConfigLoader};
use crate::ledger::LedgerPool;

/// The subcommand word that selects this mode.
pub const COMMAND: &str = "keygen";

/// Exit code for a misuse of the command line — distinct from a failure, so a
/// script can tell "you called it wrong" from "it did not work".
pub const EXIT_USAGE: i32 = 2;

/// Where the plaintext comes from.
///
/// An enum rather than an `Option<String>` plus a flag: there is exactly one
/// choice here, and two fields would let them disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plaintext {
    /// Mint a new one. The default, and what every real key uses: 256 bits of
    /// OS entropy, so the credential's strength is not the operator's decision.
    Generated,
    /// Register a plaintext the operator supplied, exactly as given.
    ///
    /// Two legitimate reasons, both outside the request path: migrating a key
    /// that used to live in `config.yaml` without re-issuing it to the partner,
    /// and writing a memorable key into a development fixture. See
    /// [`ApiKeyStore::create_with_plaintext`].
    Supplied(String),
    /// `--plaintext -`: read one line from stdin.
    ///
    /// Preferred over the argument form for a real key, because a command line
    /// is readable by every user on the machine (`/proc/<pid>/cmdline`) and by
    /// the shell's history file.
    FromStdin,
}

/// What the operator asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub name: String,
    pub consumer_id: String,
    pub allowed_models: Vec<String>,
    pub plaintext: Plaintext,
    /// `--database <PATH>`, when the operator named a file directly.
    ///
    /// `Some` means the configuration file is not read at all, so there is no
    /// question of which of the two wins.
    pub database: Option<std::path::PathBuf>,
}

/// Why a command line was refused.
#[derive(Debug, PartialEq, Eq)]
pub enum UsageError {
    /// An argument the command does not know.
    UnknownArgument(String),
    /// A flag that needs a value was given without one.
    MissingValue(&'static str),
    /// A required flag was not given.
    Missing(&'static str),
    /// A repeatable flag was given an empty value.
    Blank(&'static str),
}

impl std::fmt::Display for UsageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UsageError::UnknownArgument(arg) => {
                write!(f, "unrecognised argument `{arg}`")
            }
            UsageError::MissingValue(flag) => write!(f, "`{flag}` needs a value"),
            UsageError::Missing(flag) => write!(f, "`{flag}` is required"),
            UsageError::Blank(flag) => write!(f, "`{flag}` must not be blank"),
        }
    }
}

impl std::error::Error for UsageError {}

/// Whether this invocation is the subcommand.
///
/// `args` is the full argument vector including `argv[0]`, so the subcommand
/// word is at index 1. Anything else — no arguments, a flag, an unknown word —
/// is the server, because the server is what this binary is and an argument
/// parser that guessed otherwise would turn a typo into a provisioning run.
pub fn requested(args: &[String]) -> bool {
    args.get(1).map(String::as_str) == Some(COMMAND)
}

/// The help text, printed for `keygen --help` and for a usage error.
pub fn usage() -> String {
    let mut out = String::new();
    let _ = writeln!(out, "usage: partner-portal {COMMAND} [OPTIONS]");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Issue one partner API key into the ledger database and print it once."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "Options:");
    let _ = writeln!(
        out,
        "  --name <NAME>              a label for the key, shown in the listing"
    );
    let _ = writeln!(
        out,
        "  --consumer-id <ID>         the consumer_id this key identifies as"
    );
    let _ = writeln!(
        out,
        "  --allowed-model <MODEL>    a model the key may call; repeatable, and at"
    );
    let _ = writeln!(out, "                             least one is required");
    let _ = writeln!(
        out,
        "  --plaintext <VALUE|->      register this plaintext instead of"
    );
    let _ = writeln!(
        out,
        "                             generating one; `-` reads a line from stdin."
    );
    let _ = writeln!(
        out,
        "                             For migrating and for fixtures"
    );
    let _ = writeln!(
        out,
        "  --database <PATH>          the ledger file. Without it the database comes"
    );
    let _ = writeln!(
        out,
        "                             from the configuration file (`{CONFIG_ENV}`,"
    );
    let _ = writeln!(
        out,
        "                             else config.yaml) — the same one the server"
    );
    let _ = writeln!(
        out,
        "                             reads. With it, no configuration file is read"
    );
    let _ = writeln!(out, "  -h, --help                 print this text");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "The hashing secret comes from PARTNER_PORTAL_API_KEY_SECRET and must be the"
    );
    let _ = writeln!(
        out,
        "secret the server runs with, or the key issued here authenticates nowhere."
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "There is no `--all-models`: the allow-list is the credential's authority"
    );
    let _ = writeln!(
        out,
        "(ADR 0012), and a generator that invented one would be granting access nobody"
    );
    let _ = writeln!(out, "asked for.");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "`--plaintext` exists for two jobs, and neither of them is issuing a key to a"
    );
    let _ = writeln!(
        out,
        "partner. It registers a plaintext you already have — so a deployment moving"
    );
    let _ = writeln!(
        out,
        "off the config file can keep its partners' existing keys working instead of"
    );
    let _ = writeln!(
        out,
        "rotating every one of them — and it is what scripts/dev-seed-keys.sh uses to"
    );
    let _ = writeln!(
        out,
        "write the dev loop's memorable keys. A supplied plaintext is as strong as you"
    );
    let _ = writeln!(
        out,
        "made it, so for a real deployment prefer generating: a generated key carries"
    );
    let _ = writeln!(out, "256 bits and cannot be chosen badly.");
    out
}

/// Parse the arguments that follow the subcommand word.
///
/// Hand-rolled rather than `clap`: this is one subcommand with five flags, all
/// of them taking a value, and `AGENTS.md` asks for a reason before a dependency
/// is added.
pub fn parse(args: &[String]) -> Result<Request, UsageError> {
    let mut name: Option<String> = None;
    let mut consumer_id: Option<String> = None;
    let mut allowed_models: Vec<String> = Vec::new();
    let mut plaintext = Plaintext::Generated;
    let mut database: Option<std::path::PathBuf> = None;

    let mut rest = args.iter().skip(1);
    // Skip the subcommand word itself.
    rest.next();

    while let Some(arg) = rest.next() {
        let mut value_for = |flag: &'static str| -> Result<String, UsageError> {
            rest.next().cloned().ok_or(UsageError::MissingValue(flag))
        };

        match arg.as_str() {
            "--name" => {
                let value = value_for("--name")?;
                if value.trim().is_empty() {
                    return Err(UsageError::Blank("--name"));
                }
                name = Some(value);
            }
            "--consumer-id" => {
                let value = value_for("--consumer-id")?;
                if value.trim().is_empty() {
                    return Err(UsageError::Blank("--consumer-id"));
                }
                consumer_id = Some(value);
            }
            "--allowed-model" => {
                let value = value_for("--allowed-model")?;
                if value.trim().is_empty() {
                    return Err(UsageError::Blank("--allowed-model"));
                }
                allowed_models.push(value);
            }
            "--database" => {
                let value = value_for("--database")?;
                if value.trim().is_empty() {
                    return Err(UsageError::Blank("--database"));
                }
                database = Some(std::path::PathBuf::from(value));
            }
            "--plaintext" => {
                let value = value_for("--plaintext")?;
                // `-` is the stdin form, not a plaintext called "-".
                plaintext = if value == "-" {
                    Plaintext::FromStdin
                } else {
                    if value.trim().is_empty() {
                        return Err(UsageError::Blank("--plaintext"));
                    }
                    Plaintext::Supplied(value)
                };
            }
            other => return Err(UsageError::UnknownArgument(other.to_string())),
        }
    }

    if allowed_models.is_empty() {
        return Err(UsageError::Missing("--allowed-model"));
    }

    Ok(Request {
        // A key with no label is still a key; an unnamed one is what an operator
        // gets for not choosing. The consumer is required because it is the
        // isolation boundary (invariant 7) and has no sensible default: a
        // guessed consumer_id is a key that attributes usage to the wrong
        // partner.
        name: name.unwrap_or_else(|| "unnamed".to_string()),
        consumer_id: consumer_id.ok_or(UsageError::Missing("--consumer-id"))?,
        allowed_models,
        plaintext,
        database,
    })
}

/// Resolve a [`Plaintext`] to the actual value, reading stdin when asked.
///
/// The newline is stripped and nothing else: a value read from a file or a pipe
/// arrives with its terminator, and keeping it would store a credential that
/// the partner's `Authorization: Bearer …` cannot reproduce. Interior and
/// trailing spaces are kept — the hashing secret is not trimmed either, and
/// quietly altering a credential is the one thing this must not do.
pub fn resolve_plaintext(source: &Plaintext) -> Result<Option<String>, KeygenError> {
    match source {
        Plaintext::Generated => Ok(None),
        Plaintext::Supplied(value) => Ok(Some(value.clone())),
        Plaintext::FromStdin => {
            let mut line = String::new();
            let read = std::io::stdin()
                .read_line(&mut line)
                .map_err(KeygenError::Stdin)?;
            if read == 0 {
                return Err(KeygenError::EmptyStdin);
            }
            Ok(Some(line.strip_suffix('\n').unwrap_or(&line).to_string()))
        }
    }
}

/// Issue the key and return the plaintext to print, having written the
/// operator's report to stderr.
pub fn run(db_path: &Path, secret: Vec<u8>, request: &Request) -> Result<String, KeygenError> {
    // `LedgerPool::new` applies `schema.sql`, so a database that does not exist
    // yet is created here rather than being an error: a first run against a
    // fresh volume is the normal case.
    let pool = LedgerPool::new(db_path.to_path_buf()).map_err(|source| KeygenError::Database {
        path: db_path.to_path_buf(),
        source,
    })?;
    let store = ApiKeyStore::new(std::sync::Arc::new(pool), secret);

    let supplied = resolve_plaintext(&request.plaintext)?;
    let supplied_by_operator = supplied.is_some();
    let (row, plaintext) = match supplied {
        Some(plaintext) => {
            let row = store
                .create_with_plaintext(
                    &request.name,
                    &request.consumer_id,
                    request.allowed_models.clone(),
                    None,
                    &plaintext,
                )
                .map_err(KeygenError::Store)?;
            (row, plaintext)
        }
        None => store
            .create(
                &request.name,
                &request.consumer_id,
                request.allowed_models.clone(),
                None,
            )
            .map_err(KeygenError::Store)?,
    };

    let mut report = String::new();
    let _ = writeln!(report, "issued key #{}", row.id);
    let _ = writeln!(report, "  name:        {}", row.name);
    let _ = writeln!(report, "  consumer_id: {}", row.consumer_id);
    let _ = writeln!(report, "  key_prefix:  {}", row.key_prefix);
    let _ = writeln!(report, "  models:      {}", row.allowed_models.join(", "));
    let _ = writeln!(report, "  database:    {}", db_path.display());

    if supplied_by_operator {
        // Two things an operator registering a chosen plaintext has to know,
        // and neither is visible from outside.
        let _ = writeln!(report);
        let _ = writeln!(
            report,
            "This key's plaintext was supplied, not generated: it is exactly as \
             strong as the value you gave it."
        );
        if row.key_prefix.len() < crate::apikeys::KEY_PREFIX_LEN {
            let _ = writeln!(
                report,
                "It is shorter than {} characters, so the whole of it is stored in \
                 `key_prefix` and appears in every listing.",
                crate::apikeys::KEY_PREFIX_LEN
            );
        }
    }

    let _ = writeln!(report);
    let _ = writeln!(
        report,
        "This is the only time the key is shown. Store it in the partner's secret \
         manager now; it cannot be read back from the database or from any endpoint."
    );

    eprint!("{report}");
    Ok(plaintext)
}

/// The database path a configuration file names.
///
/// Relative paths are resolved by SQLite against the process's working
/// directory, which for a CLI is the directory the operator ran it from — not
/// the one the server runs in. Reported in the output so the operator can see
/// which file was written.
pub fn database_path(config_path: &Path) -> Result<std::path::PathBuf, KeygenError> {
    let config = ConfigLoader::from_file(config_path).map_err(|source| KeygenError::Config {
        path: config_path.to_path_buf(),
        source: Box::new(source),
    })?;
    Ok(std::path::PathBuf::from(&config.database.path))
}

/// Why provisioning failed.
#[derive(Debug)]
pub enum KeygenError {
    /// The configuration file could not be read or parsed.
    Config {
        path: std::path::PathBuf,
        source: Box<crate::config::ConfigError>,
    },
    /// The database could not be opened or its schema applied.
    Database {
        path: std::path::PathBuf,
        source: crate::ledger::SchemaError,
    },
    /// The key could not be written.
    Store(crate::apikeys::ApiKeyError),
    /// `--plaintext -` was given and stdin could not be read.
    Stdin(std::io::Error),
    /// `--plaintext -` was given and stdin was at end of input.
    EmptyStdin,
}

impl std::fmt::Display for KeygenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeygenError::Config { path, source } => {
                write!(f, "could not read {}: {source}", path.display())
            }
            KeygenError::Database { path, source } => {
                write!(f, "could not open {}: {source}", path.display())
            }
            KeygenError::Store(source) => write!(f, "could not issue the key: {source}"),
            KeygenError::Stdin(source) => {
                write!(f, "--plaintext -: could not read stdin: {source}")
            }
            KeygenError::EmptyStdin => write!(
                f,
                "--plaintext -: stdin was empty. Pipe the key in with \
                 `printf %s \"$KEY\" |` or a here-string; a trailing newline is \
                 stripped, nothing else"
            ),
        }
    }
}

impl std::error::Error for KeygenError {}

/// Resolve the hashing secret, with the message an operator needs.
pub fn load_hashing_secret() -> Result<Vec<u8>, String> {
    load_secret().map_err(|e| {
        format!(
            "{e}. The key issued here is hashed with that secret, so it must be the \
             one the server runs with"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(rest: &[&str]) -> Vec<String> {
        let mut v = vec!["partner-portal".to_string(), COMMAND.to_string()];
        v.extend(rest.iter().map(|s| s.to_string()));
        v
    }

    #[test]
    fn test_the_subcommand_is_recognised_only_as_the_first_word() {
        assert!(requested(&args(&[])));
        assert!(requested(&args(&["--name", "x", "--consumer-id", "y"])));
        // No arguments is the server, which is what this binary is. So is a
        // flag, so `partner-portal --help` does not provision anything.
        assert!(!requested(&["partner-portal".to_string()]));
        assert!(!requested(&[
            "partner-portal".to_string(),
            "--help".to_string()
        ]));
        assert!(!requested(&[
            "partner-portal".to_string(),
            "keygenn".to_string()
        ]));
    }

    #[test]
    fn test_an_allow_list_is_required_because_there_is_no_all_models_value() {
        // The property worth pinning: this command cannot mint a credential with
        // unbounded authority, and it cannot mint one that authenticates but is
        // allowed nothing either — both would be surprising in opposite ways.
        let err = parse(&args(&["--name", "alice", "--consumer-id", "acme"])).unwrap_err();
        assert_eq!(err, UsageError::Missing("--allowed-model"));
        assert!(usage().contains("--allowed-model"));
    }

    #[test]
    fn test_a_consumer_is_required_and_never_defaulted() {
        // consumer_id is the isolation boundary (invariant 7). A default would
        // silently attribute one partner's usage to another.
        let err = parse(&args(&["--allowed-model", "gpt-4o"])).unwrap_err();
        assert_eq!(err, UsageError::Missing("--consumer-id"));

        // The name, by contrast, is a label and has a stated default.
        let request = parse(&args(&[
            "--consumer-id",
            "acme",
            "--allowed-model",
            "gpt-4o",
        ]))
        .unwrap();
        assert_eq!(request.name, "unnamed");
        assert_eq!(request.consumer_id, "acme");
    }

    #[test]
    fn test_flags_parse_in_any_order_and_models_repeat() {
        let request = parse(&args(&[
            "--allowed-model",
            "gpt-4o",
            "--consumer-id",
            "acme",
            "--name",
            "alice",
            "--allowed-model",
            "gpt-4o-mini",
        ]))
        .unwrap();
        assert_eq!(
            request,
            Request {
                name: "alice".to_string(),
                consumer_id: "acme".to_string(),
                allowed_models: vec!["gpt-4o".to_string(), "gpt-4o-mini".to_string()],
                // The default, asserted here so a bug that made `--plaintext`
                // the default would be caught: every real key is generated.
                plaintext: Plaintext::Generated,
                // And no `--database` means the configuration file is the
                // source, which is what makes `partner-portal keygen` find the
                // same file the server runs against.
                database: None,
            }
        );
    }

    #[test]
    fn test_a_named_database_is_taken_verbatim() {
        let request = parse(&args(&[
            "--database",
            "/var/lib/partner-portal/partner-portal.db",
            "--consumer-id",
            "acme",
            "--allowed-model",
            "gpt-4o",
        ]))
        .unwrap();
        assert_eq!(
            request.database,
            Some(std::path::PathBuf::from(
                "/var/lib/partner-portal/partner-portal.db"
            ))
        );
    }

    #[test]
    fn test_a_supplied_plaintext_is_taken_exactly_and_a_dash_means_stdin() {
        // The two forms, and the one that must not be confused with a value:
        // `-` selects the stdin path rather than registering a key called "-".
        let given = parse(&args(&[
            "--consumer-id",
            "acme",
            "--allowed-model",
            "gpt-4o",
            "--plaintext",
            "dev-key",
        ]))
        .unwrap();
        assert_eq!(given.plaintext, Plaintext::Supplied("dev-key".to_string()));

        let piped = parse(&args(&[
            "--consumer-id",
            "acme",
            "--allowed-model",
            "gpt-4o",
            "--plaintext",
            "-",
        ]))
        .unwrap();
        assert_eq!(piped.plaintext, Plaintext::FromStdin);
    }

    #[test]
    fn test_a_bad_command_line_is_refused_rather_than_guessed() {
        assert_eq!(
            parse(&args(&["--consumer-id"])),
            Err(UsageError::MissingValue("--consumer-id"))
        );
        assert_eq!(
            parse(&args(&["--nope", "x"])),
            Err(UsageError::UnknownArgument("--nope".to_string()))
        );
        assert_eq!(
            parse(&args(&["--consumer-id", "  ", "--allowed-model", "gpt-4o"])),
            Err(UsageError::Blank("--consumer-id"))
        );
        assert_eq!(
            parse(&args(&["--consumer-id", "acme", "--allowed-model", " "])),
            Err(UsageError::Blank("--allowed-model"))
        );
        // A value that looks like a flag is taken as the value, not as a flag:
        // `--name --consumer` would otherwise be a confusing partial parse.
        assert_eq!(
            parse(&args(&["--name", "--consumer-id", "--consumer-id", "acme"])),
            Err(UsageError::Missing("--allowed-model"))
        );
    }

    #[test]
    fn test_a_key_issued_by_keygen_is_stored_hashed_and_authenticates() {
        // The whole point of the command, end to end against a real database:
        // it creates the schema if absent, writes a row whose stored form is not
        // the plaintext, and the plaintext it printed is the one that works.
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("ledger.db");
        let secret = b"a-keygen-test-secret-of-32-bytes!!".to_vec();

        let request = parse(&args(&[
            "--name",
            "first",
            "--consumer-id",
            "acme",
            "--allowed-model",
            "gpt-4o",
        ]))
        .unwrap();
        let plaintext = run(&db_path, secret.clone(), &request).unwrap();

        assert!(plaintext.starts_with(crate::apikeys::KEY_PREFIX));
        assert_eq!(plaintext.len(), crate::apikeys::KEY_PREFIX.len() + 43);

        // Reopened the way the server would, with the same secret. `refresh`
        // is the start-up load — authentication reads the snapshot, not SQL, so
        // a store that has not loaded one authenticates nobody.
        let store = ApiKeyStore::new(
            std::sync::Arc::new(LedgerPool::new(db_path.clone()).unwrap()),
            secret,
        );
        store.refresh().unwrap();
        let auth = store
            .authenticate(&plaintext)
            .expect("the printed key must authenticate");
        assert_eq!(auth.consumer_id, "acme");
        assert_eq!(auth.name, "first");
        assert_eq!(auth.allowed_models, vec!["gpt-4o".to_string()]);

        // And the plaintext is not in the file it was written to.
        for suffix in ["", "-wal", "-shm"] {
            let path = std::path::PathBuf::from(format!("{}{suffix}", db_path.display()));
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            assert!(
                !bytes
                    .windows(plaintext.len())
                    .any(|w| w == plaintext.as_bytes()),
                "the plaintext reached {}",
                path.display()
            );
        }
    }

    #[test]
    fn test_a_key_issued_under_another_secret_does_not_authenticate() {
        // The operational foot-gun the module doc names: keygen and the server
        // must share the secret, and when they do not, the symptom is a 401
        // rather than an error anywhere. Pinned so the doc is not the only place
        // that says so.
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("ledger.db");

        let request = parse(&args(&[
            "--consumer-id",
            "acme",
            "--allowed-model",
            "gpt-4o",
        ]))
        .unwrap();
        let plaintext = run(
            &db_path,
            b"the-secret-keygen-ran-with-32-bytes".to_vec(),
            &request,
        )
        .unwrap();

        let server = ApiKeyStore::new(
            std::sync::Arc::new(LedgerPool::new(db_path).unwrap()),
            b"the-secret-the-server-has-32-bytes!".to_vec(),
        );
        assert_eq!(
            server.refresh().unwrap(),
            1,
            "the row is there, so the refusal below is about the hash and not \
             about a missing key"
        );
        assert!(server.authenticate(&plaintext).is_none());
    }
}
