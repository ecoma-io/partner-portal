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
use crate::billing::partner::BillingMode;
use crate::billing::pricing::{PricePerMillion, PricingSnapshot};
use crate::billing::store::{BillingStore, NewPartner};
use crate::billing::{DEFAULT_PAYMENT_TERMS_MINUTES, ModelPrice};
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
    /// The partner's commercial configuration, written before the key.
    pub partner: NewPartner,
    /// The models the partner may call, with their prices. Never empty.
    pub models: Vec<ModelPrice>,
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
    /// `--model` was not `NAME:INPUT:CACHED:OUTPUT`, or a price was not a
    /// number.
    BadModel { value: String, reason: String },
    /// `--billing-mode` was not one of the two modes.
    BadBillingMode(String),
    /// `--payment-terms` was not a non-negative whole number of minutes.
    BadPaymentTerms(String),
    /// Two `--model` flags named the same model.
    DuplicateModel(String),
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
            UsageError::BadModel { value, reason } => {
                write!(f, "--model {value:?}: {reason}")
            }
            UsageError::BadBillingMode(mode) => write!(
                f,
                "--billing-mode {mode:?} is not a mode; expected `invoice` or `reconciliation`"
            ),
            UsageError::BadPaymentTerms(value) => write!(
                f,
                "--payment-terms {value:?} is not a whole number of minutes"
            ),
            UsageError::DuplicateModel(model) => {
                write!(f, "--model {model:?} was given more than once")
            }
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
    let _ = writeln!(out, "  --model <NAME:IN:CACHED:OUT>");
    let _ = writeln!(
        out,
        "                             a model the partner may call and its price in"
    );
    let _ = writeln!(
        out,
        "                             dollars per million tokens: input, cached"
    );
    let _ = writeln!(
        out,
        "                             input, output. Repeatable, and at least one"
    );
    let _ = writeln!(
        out,
        "                             is required — a model with no price is a"
    );
    let _ = writeln!(
        out,
        "                             model the partner cannot call"
    );
    let _ = writeln!(
        out,
        "  --billing-mode <MODE>      `invoice` (default) or `reconciliation`. An"
    );
    let _ = writeln!(
        out,
        "                             invoice partner owes money and is suspended"
    );
    let _ = writeln!(
        out,
        "                             when a complete statement passes its due"
    );
    let _ = writeln!(
        out,
        "                             date; a reconciliation partner is statemented"
    );
    let _ = writeln!(
        out,
        "                             for the settlement record and owes nothing"
    );
    let _ = writeln!(
        out,
        "  --billing-email <ADDRESS>  where statements are sent. Optional: without"
    );
    let _ = writeln!(
        out,
        "                             it statements are still issued and simply not"
    );
    let _ = writeln!(out, "                             emailed");
    let _ = writeln!(
        out,
        "  --payment-terms <MINUTES>  how long after a period closes payment is due."
    );
    let _ = writeln!(
        out,
        "                             Default {DEFAULT_PAYMENT_TERMS_MINUTES} (twelve hours)"
    );
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
        "There is no `--all-models`: which models a partner may call and what each"
    );
    let _ = writeln!(
        out,
        "costs are one list (ADR 0012 as amended by 0015), and a generator that"
    );
    let _ = writeln!(
        out,
        "invented one would be granting access nobody asked for."
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "The partner record is written here too, and writing it is what tells the"
    );
    let _ = writeln!(
        out,
        "worker who to bill. A second run for a consumer that already exists is"
    );
    let _ = writeln!(
        out,
        "refused rather than reconfiguring their models and prices from a command"
    );
    let _ = writeln!(
        out,
        "line; use the admin API to change an existing partner."
    );
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
    let mut models: Vec<ModelPrice> = Vec::new();
    let mut billing_mode = BillingMode::Invoice;
    let mut billing_email = String::new();
    let mut payment_terms_minutes = DEFAULT_PAYMENT_TERMS_MINUTES;
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
            "--model" => {
                let value = value_for("--model")?;
                let entry = parse_model(&value)?;
                if models.iter().any(|m| m.model == entry.model) {
                    return Err(UsageError::DuplicateModel(entry.model));
                }
                models.push(entry);
            }
            "--billing-mode" => {
                let value = value_for("--billing-mode")?;
                billing_mode =
                    BillingMode::parse(value.trim()).ok_or(UsageError::BadBillingMode(value))?;
            }
            "--billing-email" => {
                // Blank is allowed and means "no address on file": statements
                // are still issued and simply not emailed. It is not the same
                // as omitting the flag, and it is not an error.
                billing_email = value_for("--billing-email")?;
            }
            "--payment-terms" => {
                let value = value_for("--payment-terms")?;
                payment_terms_minutes = value
                    .trim()
                    .parse::<i64>()
                    .ok()
                    .filter(|n| *n >= 0)
                    .ok_or(UsageError::BadPaymentTerms(value))?;
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

    if models.is_empty() {
        return Err(UsageError::Missing("--model"));
    }

    // A key with no label is still a key; an unnamed one is what an operator
    // gets for not choosing. The consumer is required because it is the
    // isolation boundary (invariant 7) and has no sensible default: a guessed
    // consumer_id is a key that attributes usage to the wrong partner.
    let name = name.unwrap_or_else(|| "unnamed".to_string());
    let consumer_id = consumer_id.ok_or(UsageError::Missing("--consumer-id"))?;

    Ok(Request {
        partner: NewPartner {
            consumer_id: consumer_id.clone(),
            name: name.clone(),
            billing_email: billing_email.trim().to_string(),
            billing_mode,
            payment_terms_minutes,
        },
        name,
        consumer_id,
        models,
        plaintext,
        database,
    })
}

/// Parse `NAME:INPUT:CACHED:OUTPUT`, where each price is dollars per million
/// tokens.
///
/// One flag rather than four, because the name and its three prices are one
/// fact about one model (ADR 0015) and a syntax that let them be given
/// separately would let a model end up in the list with no price behind it —
/// which is precisely the "callable at a price nobody configured" state the
/// single table exists to make impossible.
///
/// The name may not contain a colon: model names from every provider in use are
/// `[A-Za-z0-9._-]`-ish, and a delimiter that can appear inside the first field
/// makes the parse ambiguous rather than merely inconvenient.
fn parse_model(value: &str) -> Result<ModelPrice, UsageError> {
    let bad = |reason: &str| UsageError::BadModel {
        value: value.to_string(),
        reason: reason.to_string(),
    };

    let parts: Vec<&str> = value.split(':').collect();
    let [model, input, cached, output] = parts.as_slice() else {
        return Err(bad(
            "expected NAME:INPUT:CACHED:OUTPUT, with the prices in dollars per \
             million tokens",
        ));
    };
    let model = model.trim();
    if model.is_empty() {
        return Err(bad("the model name is blank"));
    }
    let price = |text: &str, which: &str| -> Result<PricePerMillion, UsageError> {
        PricePerMillion::parse(text.trim()).map_err(|e| bad(&format!("{which} price: {e}")))
    };
    Ok(ModelPrice {
        model: model.to_string(),
        prices: PricingSnapshot::new(
            price(input, "input")?,
            price(cached, "cached input")?,
            price(output, "output")?,
        ),
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
    let pool = std::sync::Arc::new(pool);
    let store = ApiKeyStore::new(std::sync::Arc::clone(&pool), secret);
    let billing = BillingStore::new(pool);

    // The partner record first, then the prices, then the key. In that order
    // because the key is the thing that authenticates, and a key that exists
    // before its partner row is a credential that resolves to no model list and
    // no billing mode — briefly true, and visible to anyone watching the
    // snapshot refresh.
    //
    // A partner that already exists is refused rather than overwritten. This
    // command provisions; it does not reconfigure. Re-running it to add a key to
    // an existing partner would silently replace their prices with whatever was
    // on this command line, and the command line is not where a customer's
    // contract lives.
    let partner = billing.create_partner(request.partner.clone())?;
    billing.replace_models(&partner.consumer_id, &request.models)?;

    let supplied = resolve_plaintext(&request.plaintext)?;
    let supplied_by_operator = supplied.is_some();
    let (row, plaintext) = match supplied {
        Some(plaintext) => {
            let row = store
                .create_with_plaintext(&request.name, &request.consumer_id, None, &plaintext)
                .map_err(KeygenError::Store)?;
            (row, plaintext)
        }
        None => store
            .create(&request.name, &request.consumer_id, None)
            .map_err(KeygenError::Store)?,
    };

    let mut report = String::new();
    let _ = writeln!(report, "issued key #{}", row.id);
    let _ = writeln!(report, "  name:        {}", row.name);
    let _ = writeln!(report, "  consumer_id: {}", row.consumer_id);
    let _ = writeln!(report, "  key_prefix:  {}", row.key_prefix);
    let _ = writeln!(report, "  billing:     {}", partner.billing_mode);
    if partner.billing_email.is_empty() {
        let _ = writeln!(
            report,
            "  billing_email: (none — statements are issued and not emailed)"
        );
    } else {
        let _ = writeln!(report, "  billing_email: {}", partner.billing_email);
    }
    if partner.billing_mode.owes_payment() {
        let _ = writeln!(
            report,
            "  payment due: {} minutes after each day closes",
            partner.payment_terms_minutes
        );
    } else {
        let _ = writeln!(
            report,
            "  payment due: n/a (reconciliation — no deadline, never suspended)"
        );
    }
    let _ = writeln!(report, "  models:");
    for entry in &request.models {
        let (input, cached, output) = entry.prices.as_tuple();
        let _ = writeln!(
            report,
            "    {:<28} in {:<10} cached {:<10} out {}",
            entry.model,
            PricePerMillion::new(input).to_decimal_string(),
            PricePerMillion::new(cached).to_decimal_string(),
            PricePerMillion::new(output).to_decimal_string()
        );
    }
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
    /// The partner record or its prices could not be written.
    Billing(crate::billing::BillingError),
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
            KeygenError::Billing(source) => {
                write!(f, "could not record the partner: {source}")
            }
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

impl From<crate::billing::BillingError> for KeygenError {
    fn from(error: crate::billing::BillingError) -> Self {
        KeygenError::Billing(error)
    }
}

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

    /// The three prices the task specification names, written the way an
    /// operator writes them, parsed to the exact micro-USD integers the product
    /// stores. Fixed point is the whole reason the money is integers, so the
    /// conversion is pinned against literal expected values rather than against
    /// a round trip.
    #[test]
    fn test_prices_parse_into_exact_micro_usd() {
        let request = parse(&args(&[
            "--consumer-id",
            "acme",
            "--model",
            "gpt-4o:0.095:0.0475:0.475",
            "--model",
            "cheap:0.002375:0:0",
        ]))
        .unwrap();

        let (input, cached, output) = request.models[0].prices.as_tuple();
        assert_eq!(
            (input, cached, output),
            (95_000, 47_500, 475_000),
            "$0.095/M, $0.0475/M and $0.475/M are 95000, 47500 and 475000 micro-USD"
        );

        let (input, cached, output) = request.models[1].prices.as_tuple();
        assert_eq!(
            (input, cached, output),
            (2_375, 0, 0),
            "$0.002375/M is 2375 micro-USD, and a literal 0 is a price of nothing"
        );
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

    /// A price list is required, and there is no way to ask for every model.
    ///
    /// The property worth pinning: this command cannot mint a credential with
    /// unbounded authority, and it cannot mint one that authenticates but is
    /// allowed nothing either — both would be surprising in opposite ways. It
    /// also cannot list a model with no price behind it, because a model the
    /// partner may call at a price nobody set is the state the single table
    /// exists to make impossible.
    #[test]
    fn test_a_price_list_is_required_and_there_is_no_all_models_value() {
        let err = parse(&args(&["--name", "alice", "--consumer-id", "acme"])).unwrap_err();
        assert_eq!(err, UsageError::Missing("--model"));
        assert!(usage().contains("--model"));

        assert_eq!(
            parse(&args(&["--consumer-id", "acme", "--all-models", "gpt-4o"])),
            Err(UsageError::UnknownArgument("--all-models".to_string())),
            "the flag that would grant every model must not exist"
        );
    }

    #[test]
    fn test_a_consumer_is_required_and_never_defaulted() {
        // consumer_id is the isolation boundary (invariant 7). A default would
        // silently attribute one partner's usage to another.
        let err = parse(&args(&["--model", "gpt-4o:0.095:0.095:0.475"])).unwrap_err();
        assert_eq!(err, UsageError::Missing("--consumer-id"));

        // The name, by contrast, is a label and has a stated default.
        let request = parse(&args(&[
            "--consumer-id",
            "acme",
            "--model",
            "gpt-4o:0.095:0.095:0.475",
        ]))
        .unwrap();
        assert_eq!(request.name, "unnamed");
        assert_eq!(request.consumer_id, "acme");
        // And the partner record is written from the same values, so the row
        // and the key cannot disagree about who this is.
        assert_eq!(request.partner.consumer_id, "acme");
        assert_eq!(request.partner.name, "unnamed");
    }

    #[test]
    fn test_flags_parse_in_any_order_and_models_repeat() {
        let request = parse(&args(&[
            "--model",
            "gpt-4o:0.095:0.0475:0.475",
            "--consumer-id",
            "acme",
            "--name",
            "alice",
            "--model",
            "gpt-4o-mini:0.015:0.0075:0.06",
        ]))
        .unwrap();
        assert_eq!(
            request,
            Request {
                name: "alice".to_string(),
                consumer_id: "acme".to_string(),
                partner: NewPartner {
                    consumer_id: "acme".to_string(),
                    name: "alice".to_string(),
                    // No `--billing-email` is an empty address on file, which
                    // is not the same as an address nobody set: statements are
                    // still issued and simply not emailed.
                    billing_email: String::new(),
                    billing_mode: BillingMode::Invoice,
                    payment_terms_minutes: DEFAULT_PAYMENT_TERMS_MINUTES,
                },
                models: vec![
                    ModelPrice {
                        model: "gpt-4o".to_string(),
                        prices: PricingSnapshot::new(
                            PricePerMillion::new(95_000),
                            PricePerMillion::new(47_500),
                            PricePerMillion::new(475_000),
                        ),
                    },
                    ModelPrice {
                        model: "gpt-4o-mini".to_string(),
                        prices: PricingSnapshot::new(
                            PricePerMillion::new(15_000),
                            PricePerMillion::new(7_500),
                            PricePerMillion::new(60_000),
                        ),
                    },
                ],
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

    /// The two billing modes are the whole commercial contract, so the flag is
    /// pinned on both sides: `invoice` owes and can be suspended, and
    /// `reconciliation` is not an invoice with the deadline turned off — it has
    /// no deadline and never owes.
    #[test]
    fn test_the_billing_mode_and_terms_are_taken_from_the_flags() {
        let invoiced = parse(&args(&[
            "--consumer-id",
            "acme",
            "--model",
            "gpt-4o:0.095:0.095:0.475",
            "--billing-mode",
            "invoice",
            "--billing-email",
            "billing@acme.test",
            "--payment-terms",
            "1440",
        ]))
        .unwrap();
        assert_eq!(invoiced.partner.billing_mode, BillingMode::Invoice);
        assert!(invoiced.partner.billing_mode.owes_payment());
        assert_eq!(invoiced.partner.billing_email, "billing@acme.test");
        assert_eq!(invoiced.partner.payment_terms_minutes, 1440);

        let reconciled = parse(&args(&[
            "--consumer-id",
            "globex",
            "--model",
            "gpt-4o:0.095:0.095:0.475",
            "--billing-mode",
            "reconciliation",
        ]))
        .unwrap();
        assert_eq!(reconciled.partner.billing_mode, BillingMode::Reconciliation);
        assert!(
            !reconciled.partner.billing_mode.owes_payment(),
            "a reconciliation partner is statemented for the record and owes nothing"
        );
        // The terms are still carried — they are simply never consulted,
        // because the mode is what decides whether there is a deadline at all.
        assert_eq!(
            reconciled.partner.payment_terms_minutes,
            DEFAULT_PAYMENT_TERMS_MINUTES
        );
    }

    #[test]
    fn test_a_named_database_is_taken_verbatim() {
        let request = parse(&args(&[
            "--database",
            "/var/lib/partner-portal/partner-portal.db",
            "--consumer-id",
            "acme",
            "--model",
            "gpt-4o:0.095:0.095:0.475",
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
            "--model",
            "gpt-4o:0.095:0.095:0.475",
            "--plaintext",
            "dev-key",
        ]))
        .unwrap();
        assert_eq!(given.plaintext, Plaintext::Supplied("dev-key".to_string()));

        let piped = parse(&args(&[
            "--consumer-id",
            "acme",
            "--model",
            "gpt-4o:0.095:0.095:0.475",
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
            parse(&args(&[
                "--consumer-id",
                "  ",
                "--model",
                "gpt-4o:0.095:0.095:0.475"
            ])),
            Err(UsageError::Blank("--consumer-id"))
        );
        assert_eq!(
            parse(&args(&["--consumer-id", "acme", "--model", " "])),
            Err(UsageError::BadModel {
                value: " ".to_string(),
                reason: "expected NAME:INPUT:CACHED:OUTPUT, with the prices in \
                         dollars per million tokens"
                    .to_string(),
            })
        );
        // A value that looks like a flag is taken as the value, not as a flag:
        // `--name --consumer` would otherwise be a confusing partial parse.
        assert_eq!(
            parse(&args(&["--name", "--consumer-id", "--consumer-id", "acme"])),
            Err(UsageError::Missing("--model"))
        );
    }

    /// The model syntax has one shape and every way of getting it wrong is an
    /// error with a reason. A parse that guessed — a missing price defaulted to
    /// zero, say — would create exactly the free-model state the pricing table
    /// exists to prevent.
    #[test]
    fn test_a_malformed_model_is_refused_with_a_reason() {
        let bad = |value: &str| {
            parse(&args(&["--consumer-id", "acme", "--model", value]))
                .expect_err(&format!("{value:?} must not parse"))
        };

        // Wrong arity, in both directions.
        assert!(
            bad("gpt-4o")
                .to_string()
                .contains("NAME:INPUT:CACHED:OUTPUT")
        );
        assert!(bad("gpt-4o:0.095").to_string().contains("CACHED:OUTPUT"));
        assert!(
            bad("gpt-4o:0.095:0.095:0.475:extra")
                .to_string()
                .contains("CACHED:OUTPUT")
        );
        // A blank name.
        assert!(bad(":0.095:0.095:0.475").to_string().contains("blank"));
        // A price that is not a number, named as the component that is wrong.
        assert!(
            bad("gpt-4o:cheap:0.095:0.475")
                .to_string()
                .contains("input")
        );
        assert!(
            bad("gpt-4o:0.095:0.095:0.475x")
                .to_string()
                .contains("output")
        );
        // A negative price is refused here as well as by the schema check: a
        // negative price would make a statement credit the partner money.
        assert!(matches!(bad("gpt-4o:-1:0:0"), UsageError::BadModel { .. }));
    }

    #[test]
    fn test_the_remaining_flags_are_validated_not_guessed() {
        let good = |rest: &[&str]| {
            let mut v = vec![
                "--consumer-id",
                "acme",
                "--model",
                "gpt-4o:0.095:0.095:0.475",
            ];
            v.extend_from_slice(rest);
            parse(&args(&v))
        };

        assert_eq!(
            good(&["--billing-mode", "prepaid"]).unwrap_err(),
            UsageError::BadBillingMode("prepaid".to_string())
        );
        assert_eq!(
            good(&["--payment-terms", "-1"]).unwrap_err(),
            UsageError::BadPaymentTerms("-1".to_string())
        );
        assert_eq!(
            good(&["--payment-terms", "soon"]).unwrap_err(),
            UsageError::BadPaymentTerms("soon".to_string())
        );
        // Two prices for one model: the second would either lose silently or
        // win arbitrarily, and neither is a price the partner agreed to.
        assert_eq!(
            good(&["--model", "gpt-4o:0.015:0.015:0.06",]).unwrap_err(),
            UsageError::DuplicateModel("gpt-4o".to_string())
        );
        // A blank `--database` names no file, so it is refused rather than
        // treated as "the default".
        assert_eq!(
            good(&["--database", " "]).unwrap_err(),
            UsageError::Blank("--database")
        );
    }

    /// The prices as written in the report are the prices that were stored.
    ///
    /// The operator reads the report to confirm what they just configured, so a
    /// report that rendered a different number than the database holds would be
    /// a lie in the one place a person checks their work.
    #[test]
    fn test_the_report_renders_the_prices_that_were_stored() {
        let request = parse(&args(&[
            "--consumer-id",
            "acme",
            "--model",
            "gpt-4o:0.095:0.0475:0.475",
        ]))
        .unwrap();

        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("ledger.db");
        run(
            &db_path,
            b"a-keygen-test-secret-of-32-bytes!!".to_vec(),
            &request,
        )
        .unwrap();

        let pool = std::sync::Arc::new(LedgerPool::new(db_path).unwrap());
        let billing = BillingStore::new(pool);
        let stored = billing.models("acme").unwrap();
        assert_eq!(stored.len(), 1);
        let (input, cached, output) = stored[0].prices.as_tuple();
        assert_eq!((input, cached, output), (95_000, 47_500, 475_000));
        assert_eq!(
            PricePerMillion::new(input).to_decimal_string(),
            "0.095",
            "the report and the database must render the same price"
        );
        assert_eq!(PricePerMillion::new(cached).to_decimal_string(), "0.0475");
        assert_eq!(PricePerMillion::new(output).to_decimal_string(), "0.475");
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
            "--billing-email",
            "billing@acme.test",
            "--model",
            "gpt-4o:0.095:0.095:0.475",
            "--model",
            "gpt-4o-mini:0.015:0.015:0.06",
        ]))
        .unwrap();
        let plaintext = run(&db_path, secret.clone(), &request).unwrap();

        assert!(plaintext.starts_with(crate::apikeys::KEY_PREFIX));
        assert_eq!(plaintext.len(), crate::apikeys::KEY_PREFIX.len() + 43);

        // Reopened the way the server would, with the same secret. `refresh`
        // is the start-up load — authentication reads the snapshot, not SQL, so
        // a store that has not loaded one authenticates nobody.
        let pool = std::sync::Arc::new(LedgerPool::new(db_path.clone()).unwrap());
        let store = ApiKeyStore::new(std::sync::Arc::clone(&pool), secret);
        store.refresh().unwrap();
        let auth = store
            .authenticate(&plaintext)
            .expect("the printed key must authenticate");
        assert_eq!(auth.consumer_id, "acme");
        assert_eq!(auth.key_name, "first");
        assert_eq!(
            auth.allowed_models(),
            vec!["gpt-4o".to_string(), "gpt-4o-mini".to_string()],
            "the models the operator priced are the models the key may call"
        );
        assert_eq!(auth.pricing_for("gpt-4o").unwrap().input.as_i64(), 95_000);
        assert_eq!(
            auth.pricing_for("gpt-4o-mini").unwrap().output.as_i64(),
            60_000
        );
        assert!(!auth.allows("gpt-5"), "and nothing else");

        // The partner record is the thing the statement worker bills, and it
        // was written by this command rather than by a second one.
        let billing = BillingStore::new(pool);
        let partner = billing
            .get_partner("acme")
            .unwrap()
            .expect("the partner record must exist");
        assert_eq!(partner.billing_mode, BillingMode::Invoice);
        assert_eq!(partner.billing_email, "billing@acme.test");
        assert_eq!(partner.payment_terms_minutes, DEFAULT_PAYMENT_TERMS_MINUTES);

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

    /// A second run for a partner that already exists is refused.
    ///
    /// This command provisions; it does not reconfigure. Re-running it would
    /// otherwise replace a partner's prices with whatever was on this command
    /// line, and the command line is not where a customer's contract lives.
    /// The refusal has to leave the first key working — a failed second run that
    /// broke the first would be worse than the overwrite it prevented.
    #[test]
    fn test_a_second_run_for_an_existing_partner_is_refused() {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("ledger.db");
        let secret = b"a-keygen-test-secret-of-32-bytes!!".to_vec();

        let first = parse(&args(&[
            "--consumer-id",
            "acme",
            "--model",
            "gpt-4o:0.095:0.095:0.475",
        ]))
        .unwrap();
        let plaintext = run(&db_path, secret.clone(), &first).unwrap();

        let again = parse(&args(&[
            "--consumer-id",
            "acme",
            "--model",
            "gpt-4o:9.999:9.999:9.999",
        ]))
        .unwrap();
        let err = run(&db_path, secret.clone(), &again)
            .expect_err("provisioning an existing partner must be refused");
        assert!(
            matches!(err, KeygenError::Billing(crate::billing::BillingError::PartnerExists(ref id)) if id == "acme"),
            "expected PartnerExists(acme), got {err:?}"
        );

        // The prices are the ones the first run set, and the first key still
        // authenticates: a refused run changed nothing.
        let pool = std::sync::Arc::new(LedgerPool::new(db_path).unwrap());
        let billing = BillingStore::new(std::sync::Arc::clone(&pool));
        let stored = billing.models("acme").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].prices.input.as_i64(), 95_000);

        let store = ApiKeyStore::new(pool, secret);
        store.refresh().unwrap();
        assert!(store.authenticate(&plaintext).is_some());
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
            "--model",
            "gpt-4o:0.095:0.095:0.475",
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
