//! Statement email: a hand-written SMTP client, and the message it sends.
//!
//! # Why this is not a mail crate
//!
//! It is a `MAIL FROM`/`RCPT TO`/`DATA` conversation, a dot-stuffing rule and a
//! `STARTTLS` upgrade — a few hundred lines that can be read, tested against a
//! scripted server, and debugged from the log. A mail crate brings a MIME
//! builder, an address parser, a transport stack and a TLS backend to a product
//! that sends one plain-text message to one operator-configured relay, and
//! `AGENTS.md` asks for a reason before a dependency is added. The one thing
//! that is genuinely not hand-writable — TLS — is the one thing this file does
//! borrow (rustls, under the same feature the upstream connector uses).
//!
//! # What is never in a message
//!
//! Not the partner's API key, not the upstream credential, not a request body,
//! none of them. A statement carries the partner's own model names, token
//! counts and amounts, all of which the partner already knows, plus the
//! operator's own from-address. There is no field in [`Message`] through which a
//! secret could arrive, and [`statement_message`] is the only constructor the
//! worker uses.
//!
//! The relay *password* is a different matter, and it is the one place this file
//! has a rule worth stating: **it is never sent down a cleartext socket.** A
//! relay that offers `STARTTLS` gets an encrypted session; one that does not is
//! given no credentials at all, and the send fails with a reason an operator can
//! read. `AUTH` over plaintext is precisely the disclosure this product refuses
//! to put in a log line.
//!
//! # At-least-once, and why that is the right target
//!
//! The worker claims a statement with a lease, sends, and records the outcome. A
//! crash between the send and the commit duplicates the message; a failure
//! before it re-sends later with a backoff. A duplicate is a nuisance and a
//! missing invoice is a business failure, so the ordering is deliberate — and
//! the statement is durable before any of this runs, which is what keeps email a
//! *courtesy on top of the statement* rather than a step the statement depends
//! on. Nothing here can change an amount.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::billing::pricing::MicroUsd;
use crate::billing::store::{Statement, StatementLine};
use crate::config::{EmailConfig, SmtpCredentials};

/// How long to wait for the TCP connection itself.
///
/// Short: a relay that is not answering is not going to, and the retry backoff
/// is the mechanism for trying again. A send that blocks for minutes holds a
/// lease and a `spawn_blocking` thread for no benefit.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait for a reply on an established connection.
///
/// Generous next to a normal SMTP exchange (milliseconds) because the `DATA`
/// reply comes *after* the relay has accepted and possibly queued the message,
/// and that is where a slow relay is slow. It bounds one read, so a relay that
/// dribbles can still exceed the total — deliberately: a relay that is making
/// progress is not a relay to abort.
const IO_TIMEOUT: Duration = Duration::from_secs(30);

/// The largest reply, or reply line, this client will read.
///
/// A greeting, an `EHLO` capability list and an error text are all far smaller.
/// The bound exists because the alternative is an unbounded buffer driven by a
/// remote peer, which is the shape of a memory bug.
const MAX_REPLY_BYTES: usize = 8 * 1024;

/// How much of a relay's reply is kept as the recorded failure reason.
///
/// The text lands in `daily_statements.email_last_error`, which an operator
/// reads. An SMTP reply can be long; a novel is not more informative than its
/// first sentence.
const MAX_ERROR_CHARS: usize = 500;

/// Where and how to send, resolved from the config file and the environment.
///
/// The host, the port and the sender come from `billing.email`; the credential
/// comes from the environment and is never in the file (ADR 0015, and
/// [`crate::config::smtp`] for why).
#[derive(Debug, Clone)]
pub struct Settings {
    pub host: String,
    pub port: u16,
    pub from_address: String,
    pub credentials: Option<SmtpCredentials>,
}

impl Settings {
    /// The relay as the deployment configured it.
    pub fn from_config(email: &EmailConfig, credentials: Option<SmtpCredentials>) -> Self {
        Self {
            host: email.smtp_host.trim().to_string(),
            port: email.smtp_port,
            from_address: email.from_address.trim().to_string(),
            credentials,
        }
    }

    /// Whether the pair (host, from) is complete enough to attempt a send.
    ///
    /// The config's own validation refuses `enabled: true` without them, so this
    /// is a second reading of the same fact for a caller holding a `Settings`
    /// from somewhere else. It returns `false` rather than erroring: "there is
    /// no relay configured" is a state the worker handles by not emailing, not a
    /// failure to record against a statement.
    pub fn is_complete(&self) -> bool {
        !self.host.is_empty() && !self.from_address.is_empty()
    }
}

/// One message: the headers this client writes, and a body it does not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub from: String,
    pub to: String,
    pub subject: String,
    pub body: String,
}

/// Why a message could not be built or sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError {
    /// The relay could not be reached at all.
    Connect(String),
    /// A header carries a newline, which would let a value taken from a
    /// partner's record forge a second header or a second recipient.
    InvalidHeader { field: &'static str, reason: String },
    /// Credentials are configured and the session is not encrypted.
    CredentialsWithoutTls,
    /// Credentials are configured, this build has no TLS, and there is no
    /// encrypted channel to authenticate over.
    NoTlsInBuild,
    /// The relay refused a command. `code` is the SMTP reply code, `text` the
    /// relay's own words.
    Refused {
        command: &'static str,
        code: u16,
        text: String,
    },
    /// The conversation did not go the way SMTP says it goes.
    Protocol(String),
    /// The socket failed mid-conversation.
    Io(String),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Connect(e) => write!(f, "could not reach the mail relay: {e}"),
            SendError::InvalidHeader { field, reason } => {
                write!(f, "{field} is not a usable header: {reason}")
            }
            SendError::CredentialsWithoutTls => write!(
                f,
                "the relay does not offer STARTTLS, so the SMTP credential was not sent; \
                 enable encryption on the relay, or unset PARTNER_PORTAL_SMTP_USERNAME and \
                 PARTNER_PORTAL_SMTP_PASSWORD"
            ),
            SendError::NoTlsInBuild => write!(
                f,
                "SMTP credentials are configured but this build has no TLS; build with \
                 --features hyper-rustls, or run the relay without a credential"
            ),
            SendError::Refused {
                command,
                code,
                text,
            } => {
                write!(f, "the relay refused {command}: {code} {text}")
            }
            SendError::Protocol(e) => write!(f, "unexpected SMTP conversation: {e}"),
            SendError::Io(e) => write!(f, "the mail relay connection failed: {e}"),
        }
    }
}

impl std::error::Error for SendError {}

impl Message {
    /// Build a message, refusing to produce one whose headers could be forged.
    ///
    /// `to` and `from` come from configuration and from a partner's record, and
    /// a newline in either is header injection: a `to` of
    /// `victim@example.com\r\nBcc: everyone@else` would be sent, by this client,
    /// to everybody. The check is here rather than only at the API because this
    /// is the last place that can refuse, and the only place that knows what a
    /// header is.
    pub fn new(
        from: impl Into<String>,
        to: impl Into<String>,
        subject: impl Into<String>,
        body: impl Into<String>,
    ) -> Result<Self, SendError> {
        let message = Self {
            from: from.into(),
            to: to.into(),
            subject: subject.into(),
            body: body.into(),
        };
        check_header("from", &message.from)?;
        check_header("to", &message.to)?;
        check_header("subject", &message.subject)?;
        for (field, value) in [("to", &message.to), ("from", &message.from)] {
            if !value.contains('@') {
                return Err(SendError::InvalidHeader {
                    field,
                    reason: "the address has no @ in it".to_string(),
                });
            }
        }
        Ok(message)
    }

    /// The message as it goes on the wire: headers, a blank line, the body, and
    /// every line CRLF-terminated.
    ///
    /// Public because the `DATA` phase is the one part of the conversation whose
    /// exact bytes matter — a bare LF is not a line ending to an SMTP server —
    /// and because a test asserts on it without a socket.
    pub fn wire_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let mut header = |name: &str, value: &str| {
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(b": ");
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        };
        header("From", &self.from);
        header("To", &self.to);
        header("Subject", &self.subject);
        header("Date", &rfc5322_date(time::OffsetDateTime::now_utc()));
        header("MIME-Version", "1.0");
        header("Content-Type", "text/plain; charset=utf-8");
        // `8bit` rather than `7bit`: a partner's name is whatever an operator
        // typed, and a statement that mangles it is worse than one that needs
        // 8BITMIME — which every relay in use advertises. `7bit` would be a
        // claim about the body that this client cannot make.
        header("Content-Transfer-Encoding", "8bit");
        header("Auto-Submitted", "auto-generated");
        out.extend_from_slice(b"\r\n");
        for line in normalised_lines(&self.body) {
            out.extend_from_slice(line.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        out
    }
}

/// Refuse a header value that could become two headers.
fn check_header(field: &'static str, value: &str) -> Result<(), SendError> {
    let bad = |reason: &str| {
        Err(SendError::InvalidHeader {
            field,
            reason: reason.to_string(),
        })
    };
    if value.is_empty() {
        return bad("it is empty");
    }
    if value.contains('\r') || value.contains('\n') {
        return bad("it carries a line break");
    }
    if value.chars().any(|c| c.is_control()) {
        return bad("it carries a control character");
    }
    Ok(())
}

/// A body split into lines, with the line endings this client will write.
///
/// Interior `\r` counts as a line ending too: a body that arrived with CRLF
/// already must not produce a `\r\r\n` on the wire, which is a blank line to
/// some parsers and a corrupted character to others.
fn normalised_lines(body: &str) -> Vec<String> {
    body.replace("\r\n", "\n")
        .replace('\r', "\n")
        .split('\n')
        .map(str::to_string)
        .collect()
}

/// The `DATA` payload, dot-stuffed as RFC 5321 requires.
///
/// A body line that is exactly `.` ends the message; a line that *starts* with
/// `.` has it doubled, and the relay removes the extra one. Without this, a
/// statement containing a lone dot would be truncated at the relay — silently,
/// with the rest of its text read as SMTP commands.
fn stuffed(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 16);
    let mut at_line_start = true;
    for byte in bytes {
        if at_line_start && *byte == b'.' {
            out.push(b'.');
        }
        out.push(*byte);
        at_line_start = *byte == b'\n';
    }
    out
}

/// `Fri, 27 Sep 2026 09:15:00 +0000`, the shape RFC 5322 asks for.
///
/// Written by hand because `time`'s well-known format is a *parsing* helper with
/// a fixed ASCII rendering, and this needs a spelled-out month and a two-digit
/// day. Ten lines, no dependency.
fn rfc5322_date(now: time::OffsetDateTime) -> String {
    const DAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let weekday = now.weekday().number_days_from_monday() as usize;
    let month = now.month() as usize - 1;
    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} +0000",
        DAYS.get(weekday).copied().unwrap_or("Mon"),
        now.day(),
        MONTHS.get(month).copied().unwrap_or("Jan"),
        now.year(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

/// The statement, as the partner reads it.
///
/// Every figure comes from the statement row and its lines — the amount is
/// `total_amount_micro_usd`, not a sum recomputed here, because the row is the
/// number the partner is asked to pay and two arithmetics is one too many.
///
/// The amount is written with all six of its micro-dollar digits and the token
/// counts are the ones the provider reported. Nothing is rounded for
/// presentation: a partner comparing this message with the dashboard must not
/// have to guess which of the two is approximate.
pub fn statement_message(
    statement: &Statement,
    lines: &[StatementLine],
    partner_name: &str,
    from_address: &str,
    recipient: &str,
) -> Result<Message, SendError> {
    let reconciliation = statement
        .billing_mode_value()
        .is_some_and(|m| !m.owes_payment());
    let subject = if reconciliation {
        format!(
            "Settlement statement for {}: {}",
            statement.billing_date,
            statement.total()
        )
    } else {
        match &statement.due_at {
            Some(due) => format!(
                "Statement for {}: {} due {}",
                statement.billing_date,
                statement.total(),
                short_instant(due)
            ),
            None => format!(
                "Statement for {}: {}",
                statement.billing_date,
                statement.total()
            ),
        }
    };

    let mut body = String::new();
    body.push_str(&format!("Hello {partner_name},\n\n"));
    body.push_str(&format!(
        "Here is your usage statement for {date}, covering {start} to {end}.\n\n",
        date = statement.billing_date,
        start = short_instant(&statement.period_start),
        end = short_instant(&statement.period_end),
    ));

    if lines.is_empty() {
        // Not an error and not silent: a day with no billable usage is a real
        // statement with a real total, and an operator reading it should know
        // that is what happened rather than wonder at an empty table.
        body.push_str("No billable usage was recorded for this day.\n\n");
    } else {
        body.push_str(&format!(
            "{:<32} {:>10} {:>10} {:>12} {:>14}\n",
            "model", "input", "cached", "output", "amount"
        ));
        for line in lines {
            body.push_str(&format!(
                "{:<32} {:>10} {:>10} {:>12} {:>14}\n",
                line.model,
                grouped(line.input_tokens),
                grouped(line.cached_input_tokens),
                grouped(line.output_tokens),
                MicroUsd::from_i64(line.total_cost_micro_usd),
            ));
        }
        let sum = |f: fn(&StatementLine) -> i64| lines.iter().map(f).sum::<i64>();
        body.push_str(&format!(
            "\n{:<32} {:>10} {:>10} {:>12} {:>14}\n\n",
            "total",
            grouped(sum(|l| l.input_tokens)),
            grouped(sum(|l| l.cached_input_tokens)),
            grouped(sum(|l| l.output_tokens)),
            statement.total(),
        ));
    }

    if statement.incomplete_usage_count > 0 {
        // Stated, never hidden. An invoice that omits work it could not measure
        // and says nothing is the one a partner is right to be unhappy about;
        // this count is why the column exists on the statement.
        body.push_str(&format!(
            "{count} request(s) that day could not be measured in full — the provider did not \
             report their usage, or no complete price was configured for the model — and are \
             therefore not charged. They are counted here so the total above is not read as \
             complete when it is not. No suspension follows from that count.\n\n",
            count = statement.incomplete_usage_count,
        ));
    }

    if reconciliation {
        body.push_str(
            "This is a reconciliation statement, for the settlement record. There is no payment \
             due on it and no deadline attached to it.\n\n",
        );
    } else {
        match &statement.due_at {
            Some(due) => body.push_str(&format!(
                "Payment of {amount} is due by {due}. If it is not recorded by then, this \
                 account's API key is suspended until it is, and requests are refused with a \
                 billing error in the meantime.\n\n",
                amount = statement.total(),
                due = readable_instant(due),
            )),
            None => body.push_str(
                "Payment is due. The deadline for this statement is set when it is issued.\n\n",
            ),
        }
    }

    body.push_str(&format!("Statement reference: {}\n\n", statement.id));
    body.push_str(
        "This message was generated automatically. If a figure here looks wrong, reply to it \
         before paying — a statement is the record, and it is not adjusted after it is sent.\n",
    );

    Message::new(from_address, recipient, subject, body)
}

/// `2026-09-27T00:00:00.000000000Z` as `2026-09-27 00:00 UTC`.
///
/// The stored form is fixed-width and precise, which is right for a database and
/// unreadable in a sentence. A timestamp that cannot be read is rendered
/// verbatim rather than guessed at: a wrong date on an invoice is worse than an
/// ugly one.
fn short_instant(stored: &str) -> String {
    match stored.split_once('T') {
        Some((date, rest)) => match rest.split_once('.') {
            Some((time, _)) => format!("{date} {time} UTC"),
            None => format!("{date} {rest}"),
        },
        None => stored.to_string(),
    }
}

/// The same rendering, named for the place it is used: a deadline a partner has
/// to act on.
fn readable_instant(stored: &str) -> String {
    short_instant(stored)
}

/// Group a count in threes, so `1234567` reads as `1,234,567`.
///
/// Token counts are the one figure on a statement a partner checks by eye, and
/// six unbroken digits is where that stops being possible.
fn grouped(n: i64) -> String {
    let digits = n.unsigned_abs().to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 { format!("-{out}") } else { out }
}

/// Send one message, blocking.
///
/// Blocking on purpose: the caller runs this on `spawn_blocking`, which is how a
/// `TcpStream` conversation belongs in an async server. A hand-rolled async SMTP
/// state machine would be the same code with a `poll` in front of it.
pub fn send(settings: &Settings, message: &Message) -> Result<(), SendError> {
    if !settings.is_complete() {
        return Err(SendError::Protocol(
            "no relay is configured (billing.email.smtp_host and from_address are required)"
                .to_string(),
        ));
    }

    let addr = format!("{}:{}", settings.host, settings.port);
    let mut addrs = addr
        .to_socket_addrs()
        .map_err(|e| SendError::Connect(format!("{addr}: {e}")))?;
    let socket = addrs
        .next()
        .ok_or_else(|| SendError::Connect(format!("{addr}: no address resolved")))?;
    let stream = TcpStream::connect_timeout(&socket, CONNECT_TIMEOUT)
        .map_err(|e| SendError::Connect(format!("{addr}: {e}")))?;
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .map_err(|e| SendError::Connect(e.to_string()))?;
    stream
        .set_write_timeout(Some(IO_TIMEOUT))
        .map_err(|e| SendError::Connect(e.to_string()))?;
    // Nagle off: the exchange is small and latency-bound, and waiting to
    // coalesce a 30-byte command with nothing behind it is pure delay.
    let _ = stream.set_nodelay(true);

    let mut session = Session::new(Box::new(stream));
    session.expect(220, "greeting")?;
    session.hello(&settings.from_address)?;

    // STARTTLS, if the relay offers it and this build can do it. Both halves
    // matter: a relay that offers it and refuses a cleartext `AUTH` needs the
    // upgrade, and a credential must not go out unencrypted.
    let encrypted = if session.supports("STARTTLS") {
        session.starttls(&settings.host)?;
        true
    } else {
        false
    };

    if let Some(credentials) = &settings.credentials {
        if !encrypted {
            // Refused, not downgraded. A password down a cleartext socket is a
            // credential disclosed, and a failed send that says so is
            // recoverable: the operator fixes the relay and the retry delivers
            // the statement.
            return Err(SendError::CredentialsWithoutTls);
        }
        session.authenticate(credentials)?;
    }

    session.command(
        250,
        "MAIL FROM",
        &format!("MAIL FROM:<{}>", settings.from_address),
    )?;
    session.command(250, "RCPT TO", &format!("RCPT TO:<{}>", message.to))?;
    session.command(354, "DATA", "DATA")?;
    session.write_all(&stuffed(&message.wire_bytes()))?;
    session.write_all(b".\r\n")?;
    session.expect(250, "end of DATA")?;
    // `QUIT` is polite and not load-bearing: the message is committed at the end
    // of `DATA`, so a relay that closes the connection first has still accepted
    // it. Its failure is therefore not reported.
    let _ = session.command(221, "QUIT", "QUIT");
    Ok(())
}

/// A live SMTP conversation.
///
/// The wire lives in an `Option` for one reason: `STARTTLS` *replaces* it, and
/// the replacement is a different object. Taking it is how the upgrade is
/// expressed in the type system rather than by swapping in a placeholder.
struct Session {
    wire: Option<Box<dyn Wire>>,
    /// Bytes read from the relay and not yet consumed, with how many of them
    /// have been. Its own buffer rather than a `BufReader` because the buffer
    /// has to outlive the socket it came from across the TLS upgrade — and
    /// anything left in it at that moment is a protocol violation this can see.
    buffer: Vec<u8>,
    consumed: usize,
    /// The capability list from the last `EHLO`.
    capabilities: Vec<String>,
    /// Whether the session is encrypted. A field rather than an inference,
    /// because it is a security property a later refactor must not lose.
    encrypted: bool,
}

/// Anything this client can speak SMTP over: a socket, or a socket under TLS.
trait Wire: Read + Write + Send {}
impl<T: Read + Write + Send> Wire for T {}

/// One parsed reply.
#[derive(Debug, PartialEq, Eq)]
struct Reply {
    code: u16,
    text: String,
}

impl Session {
    fn new(wire: Box<dyn Wire>) -> Self {
        Self {
            wire: Some(wire),
            buffer: Vec::new(),
            consumed: 0,
            capabilities: Vec::new(),
            encrypted: false,
        }
    }

    fn wire_mut(&mut self) -> Result<&mut Box<dyn Wire>, SendError> {
        self.wire
            .as_mut()
            .ok_or_else(|| SendError::Protocol("the session lost its connection".to_string()))
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), SendError> {
        let wire = self.wire_mut()?;
        wire.write_all(bytes)
            .map_err(|e| SendError::Io(e.to_string()))?;
        wire.flush().map_err(|e| SendError::Io(e.to_string()))
    }

    /// Send one command and require one particular reply code.
    fn command(
        &mut self,
        expect_code: u16,
        command: &'static str,
        line: &str,
    ) -> Result<Reply, SendError> {
        let mut wire = Vec::with_capacity(line.len() + 2);
        wire.extend_from_slice(line.as_bytes());
        wire.extend_from_slice(b"\r\n");
        self.write_all(&wire)?;
        self.expect(expect_code, command)
    }

    fn expect(&mut self, expect_code: u16, command: &'static str) -> Result<Reply, SendError> {
        let reply = self.read_reply()?;
        // RCPT is the one command with two success codes — 250 accepted, 251
        // will forward — and both mean the recipient will get the message.
        if reply.code == expect_code || (expect_code == 250 && reply.code == 251) {
            Ok(reply)
        } else {
            Err(SendError::Refused {
                command,
                code: reply.code,
                text: reply.text,
            })
        }
    }

    /// Read one line, waiting for more bytes when the buffer runs out.
    fn read_line(&mut self) -> Result<String, SendError> {
        loop {
            if let Some(offset) = self.buffer[self.consumed..]
                .iter()
                .position(|b| *b == b'\n')
            {
                let end = self.consumed + offset + 1;
                let line = String::from_utf8_lossy(&self.buffer[self.consumed..end]).into_owned();
                self.consumed = end;
                return Ok(line);
            }
            // Nothing complete is buffered. Drop what has been consumed and read
            // more; a line that has grown past the cap without a newline is a
            // peer that is not speaking SMTP.
            if self.consumed > 0 {
                self.buffer.drain(..self.consumed);
                self.consumed = 0;
            }
            if self.buffer.len() > MAX_REPLY_BYTES {
                return Err(SendError::Protocol(format!(
                    "the relay sent more than {MAX_REPLY_BYTES} bytes without ending a line"
                )));
            }
            let mut chunk = [0u8; 1024];
            let read = match self.wire_mut()?.read(&mut chunk) {
                Ok(read) => read,
                // A read timeout is an error, and a `TcpStream` read that timed
                // out is not necessarily broken — but the conversation is
                // abandoned anyway, and the retry backoff is what recovers it.
                Err(e) => return Err(SendError::Io(e.to_string())),
            };
            if read == 0 {
                return Err(SendError::Protocol(
                    "the relay closed the connection mid-reply".to_string(),
                ));
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }

    /// Read one reply, following multi-line continuations to their end.
    ///
    /// `250-first line` / `250 second line`: the digits repeat and the separator
    /// is what distinguishes them. Reading only the first line of an `EHLO`
    /// would lose every capability but the first — which is exactly how a client
    /// concludes a relay does not support `STARTTLS` and then sends a password
    /// in the clear.
    fn read_reply(&mut self) -> Result<Reply, SendError> {
        let mut code_text = String::new();
        let mut text = String::new();
        loop {
            let line = self.read_line()?;
            let trimmed = line.trim_end_matches(['\r', '\n']);
            if trimmed.len() < 3 || !trimmed.as_bytes()[..3].iter().all(u8::is_ascii_digit) {
                return Err(SendError::Protocol(format!(
                    "the relay sent a line that is not a reply: {trimmed:?}"
                )));
            }
            if code_text.is_empty() {
                code_text = trimmed[..3].to_string();
            } else if trimmed[..3] != code_text {
                return Err(SendError::Protocol(format!(
                    "the reply changed code mid-message: {trimmed:?}"
                )));
            }
            text.push_str(trimmed.get(4..).unwrap_or(""));
            match trimmed.as_bytes().get(3) {
                Some(b'-') => text.push('\n'),
                Some(b' ') => break,
                _ => {
                    return Err(SendError::Protocol(format!(
                        "the relay sent an unseparated reply line: {trimmed:?}"
                    )));
                }
            }
        }
        let code: u16 = code_text.parse().map_err(|_| {
            SendError::Protocol(format!("reply code {code_text:?} is not a number"))
        })?;
        Ok(Reply {
            code,
            text: bounded(&text),
        })
    }

    /// `EHLO`, and remember what the relay said it can do.
    ///
    /// The name given is the domain of the sender's own address, which is a name
    /// the operator owns and which a relay is entitled to resolve. A relay that
    /// refuses `EHLO` gets no `HELO` fallback: the fallback is RFC 5321's
    /// degraded mode, it carries no capability list, and guessing at what a
    /// relay supports is how this client would end up sending a credential in
    /// the clear. A relay that cannot do `EHLO` cannot do `STARTTLS` either.
    fn hello(&mut self, from_address: &str) -> Result<(), SendError> {
        let domain = from_address
            .rsplit_once('@')
            .map(|(_, d)| d)
            .filter(|d| !d.is_empty())
            .unwrap_or("localhost");
        let reply = self.command(250, "EHLO", &format!("EHLO {domain}"))?;
        // The first line is the relay's greeting ("mail.example.com Hello"), not
        // a capability: reading it as one would see `mail.example.com`.
        self.capabilities = reply
            .text
            .lines()
            .skip(1)
            .filter_map(|line| line.split_whitespace().next())
            .map(str::to_string)
            .collect();
        Ok(())
    }

    /// Whether the relay advertised `capability` in its `EHLO` reply.
    ///
    /// Case-insensitive: SMTP keywords are, and a case difference must not
    /// decide whether a password is allowed onto the wire.
    fn supports(&self, capability: &str) -> bool {
        self.capabilities
            .iter()
            .any(|c| c.eq_ignore_ascii_case(capability))
    }

    /// Upgrade the connection with `STARTTLS`.
    ///
    /// Anything already buffered is a failure, not a detail: it would be a
    /// plaintext byte the relay sent *after* its `220`, injected into what both
    /// sides now treat as an encrypted channel. The buffer is therefore checked
    /// before the socket is taken, and the session is abandoned if it has
    /// anything in it.
    fn starttls(&mut self, host: &str) -> Result<(), SendError> {
        self.command(220, "STARTTLS", "STARTTLS")?;
        if self.buffer.len() != self.consumed {
            return Err(SendError::Protocol(
                "the relay sent data after its STARTTLS reply, before the handshake".to_string(),
            ));
        }
        self.buffer.clear();
        self.consumed = 0;
        let wire = self
            .wire
            .take()
            .ok_or_else(|| SendError::Protocol("the session lost its connection".to_string()))?;
        self.wire = Some(upgrade(wire, host)?);
        self.encrypted = true;
        // A fresh `EHLO` after the handshake: the capability list of a cleartext
        // session is not that of an encrypted one, and `STARTTLS` must not still
        // be advertised on a session that is already upgraded.
        self.capabilities.clear();
        let reply = self.command(250, "EHLO", "EHLO")?;
        self.capabilities = reply
            .text
            .lines()
            .skip(1)
            .filter_map(|line| line.split_whitespace().next())
            .map(str::to_string)
            .collect();
        Ok(())
    }

    /// `AUTH PLAIN`, the one mechanism implemented.
    ///
    /// `PLAIN` because every submission relay offers it and `LOGIN` is the
    /// handshake that exists to work around a relay that does not. The
    /// initial-response form is used; a relay that answers `334` (asking for the
    /// response separately) is answered once, and one that answers `334` again
    /// is refused by `expect` rather than looped at.
    fn authenticate(&mut self, credentials: &SmtpCredentials) -> Result<(), SendError> {
        let mut raw = Vec::new();
        raw.push(0);
        raw.extend_from_slice(credentials.username().as_bytes());
        raw.push(0);
        raw.extend_from_slice(credentials.password().as_bytes());
        let encoded = base64(&raw);
        let reply = self.command(235, "AUTH", &format!("AUTH PLAIN {encoded}"));
        match reply {
            Err(SendError::Refused { code: 334, .. }) => {
                self.command(235, "AUTH", &encoded).map(|_| ())
            }
            other => other.map(|_| ()),
        }
    }
}

/// Truncate at a character boundary, and say that it happened.
fn bounded(text: &str) -> String {
    if text.chars().count() <= MAX_ERROR_CHARS {
        return text.to_string();
    }
    let mut out: String = text.chars().take(MAX_ERROR_CHARS).collect();
    out.push('…');
    out
}

/// Standard base64 with padding (RFC 4648 §4), for `AUTH PLAIN`.
///
/// Twenty lines against a dependency, tested against the RFC's own vectors. An
/// `SmtpCredentials` value is the only thing this ever encodes.
fn base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = chunk.get(1).map_or(0, |b| u32::from(*b));
        let b2 = chunk.get(2).map_or(0, |b| u32::from(*b));
        let triple = (b0 << 16) | (b1 << 8) | b2;
        for shift in [18, 12, 6, 0] {
            let index = ((triple >> shift) & 0x3f) as usize;
            out.push(ALPHABET[index] as char);
        }
        // Padding replaces the characters that carry no input byte: the last two
        // for a one-byte tail, the last one for a two-byte tail.
        match chunk.len() {
            1 => out.truncate(out.len() - 2),
            2 => out.truncate(out.len() - 1),
            _ => {}
        }
        for _ in chunk.len()..3 {
            out.push('=');
        }
    }
    out
}

/// Wrap a socket in TLS, or explain why this build cannot.
#[cfg(feature = "hyper-rustls")]
fn upgrade(wire: Box<dyn Wire>, host: &str) -> Result<Box<dyn Wire>, SendError> {
    use std::sync::Arc;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| SendError::Protocol(format!("TLS configuration: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();

    let server_name = rustls::pki_types::ServerName::try_from(host.to_string()).map_err(|e| {
        SendError::Protocol(format!("{host:?} is not a usable TLS server name: {e}"))
    })?;
    let conn = rustls::ClientConnection::new(Arc::new(config), server_name)
        .map_err(|e| SendError::Protocol(format!("TLS session: {e}")))?;

    // `StreamOwned` drives the handshake on the first read or write, and the
    // certificate chain is verified there — so a relay whose certificate does
    // not match its name, or is not trusted, fails the send rather than being
    // authenticated to.
    Ok(Box::new(rustls::StreamOwned::new(conn, wire)))
}

/// A build without TLS cannot authenticate, and says so rather than silently
/// downgrading to a cleartext credential.
#[cfg(not(feature = "hyper-rustls"))]
fn upgrade(_wire: Box<dyn Wire>, _host: &str) -> Result<Box<dyn Wire>, SendError> {
    Err(SendError::NoTlsInBuild)
}

/// A scripted SMTP relay, for the tests in this crate that need one to talk to.
///
/// It exists at module scope rather than inside `mod tests` because the worker's
/// tests need it too: the at-least-once bookkeeping in
/// [`crate::billing::worker`] is only meaningful if a send really happened, and
/// the honest way to arrange that is a socket a real `send` can reach rather
/// than a substituted sender that would let the two halves disagree.
#[cfg(test)]
pub(crate) mod test_smtp {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::Duration;

    /// How the relay should behave, so a test can drive the client down one
    /// path.
    #[derive(Clone)]
    pub(crate) struct Relay {
        /// Lines of the `EHLO` reply after the greeting line.
        pub capabilities: Vec<&'static str>,
        pub rcpt_code: u16,
        pub rcpt_text: &'static str,
    }

    impl Default for Relay {
        fn default() -> Self {
            Self {
                capabilities: Vec::new(),
                rcpt_code: 250,
                rcpt_text: "2.1.5 OK",
            }
        }
    }

    /// A relay listening on an ephemeral port, with what it heard so far.
    pub(crate) struct Running {
        port: u16,
        commands: Arc<Mutex<Vec<String>>>,
        data: Arc<Mutex<Vec<String>>>,
        thread: Option<JoinHandle<()>>,
    }

    impl Running {
        /// Start a relay. It accepts one connection, which is all a single
        /// `send` makes.
        pub(crate) fn start(relay: Relay) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("an ephemeral port");
            let port = listener.local_addr().expect("the bound address").port();
            let commands = Arc::new(Mutex::new(Vec::new()));
            let data = Arc::new(Mutex::new(Vec::new()));
            let heard_commands = Arc::clone(&commands);
            let heard_data = Arc::clone(&data);

            let thread = std::thread::spawn(move || {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                // A timeout as well as EOF: a client that dies without closing
                // would otherwise leave this thread, and the joining test, stuck.
                let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
                let mut writer = stream.try_clone().expect("a writer for the same socket");
                let mut reader = BufReader::new(stream);
                write_reply(&mut writer, 220, &["mail.test ESMTP ready"]);

                let mut in_data = false;
                loop {
                    let mut line = String::new();
                    match reader.read_line(&mut line) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                    let line = line.trim_end_matches(['\r', '\n']).to_string();

                    if in_data {
                        if line == "." {
                            in_data = false;
                            write_reply(&mut writer, 250, &["2.0.0 queued as 1A2B3C"]);
                            continue;
                        }
                        heard_data.lock().unwrap().push(line);
                        continue;
                    }

                    heard_commands.lock().unwrap().push(line.clone());
                    let verb = line.split_whitespace().next().unwrap_or("").to_uppercase();
                    match verb.as_str() {
                        "EHLO" => {
                            let mut texts = vec!["mail.test Hello"];
                            texts.extend(relay.capabilities.iter().copied());
                            write_reply(&mut writer, 250, &texts);
                        }
                        "STARTTLS" => write_reply(&mut writer, 220, &["2.0.0 Ready to start TLS"]),
                        "AUTH" => {
                            write_reply(&mut writer, 235, &["2.7.0 Authentication successful"])
                        }
                        "MAIL" => write_reply(&mut writer, 250, &["2.1.0 OK"]),
                        "RCPT" => write_reply(&mut writer, relay.rcpt_code, &[relay.rcpt_text]),
                        "DATA" => {
                            in_data = true;
                            write_reply(&mut writer, 354, &["End data with <CR><LF>.<CR><LF>"]);
                        }
                        "QUIT" => {
                            write_reply(&mut writer, 221, &["2.0.0 Bye"]);
                            break;
                        }
                        // `HELO` included: this client must never send it, and a
                        // test that let it succeed would not notice if it did.
                        other => write_reply(
                            &mut writer,
                            500,
                            &[&format!("5.5.1 {other} not recognised")],
                        ),
                    }
                }
            });

            Self {
                port,
                commands,
                data,
                thread: Some(thread),
            }
        }

        pub(crate) fn port(&self) -> u16 {
            self.port
        }

        /// The commands the relay was sent, in order.
        pub(crate) fn commands(&self) -> Vec<String> {
            self.commands.lock().unwrap().clone()
        }

        /// The lines of the message body the relay received.
        pub(crate) fn data(&self) -> Vec<String> {
            self.data.lock().unwrap().clone()
        }

        /// Both transcripts as they stand, without waiting for the relay.
        ///
        /// A test that asserts **nothing** was sent has to use this and not
        /// [`Running::finish`]: the relay thread is still parked in `accept`,
        /// waiting for a conversation the test is claiming never happened, and
        /// joining it would hang until the process ends. Call this after the
        /// code under test has returned — the client sends `EHLO` the moment it
        /// connects, so a connection that happened is already recorded.
        pub(crate) fn heard(&self) -> (Vec<String>, Vec<String>) {
            (self.commands(), self.data())
        }

        /// Wait for the conversation to finish and hand back both transcripts.
        pub(crate) fn finish(mut self) -> (Vec<String>, Vec<String>) {
            if let Some(thread) = self.thread.take() {
                thread.join().expect("the relay thread finishes");
            }
            (self.commands(), self.data())
        }
    }

    fn write_reply(writer: &mut std::net::TcpStream, code: u16, texts: &[&str]) {
        for (index, text) in texts.iter().enumerate() {
            let separator = if index + 1 == texts.len() { ' ' } else { '-' };
            let _ = write!(writer, "{code}{separator}{text}\r\n");
        }
        let _ = writer.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    use crate::billing::pricing::{PricePerMillion, PricingSnapshot};
    use crate::billing::store::{Statement, StatementLine};

    // ---------------------------------------------------------------------
    // The message, without a socket
    // ---------------------------------------------------------------------

    #[test]
    fn test_base64_matches_the_rfc4648_vectors() {
        // The vectors from the RFC itself, including the two padding cases and
        // the empty input. An `AUTH PLAIN` line that is off by a character is a
        // relay replying `535` and an operator reading "authentication failed"
        // with nothing wrong on their side.
        for (input, expected) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(input.as_bytes()), expected, "base64({input:?})");
        }
    }

    #[test]
    fn test_dot_stuffing_only_touches_a_dot_at_the_start_of_a_line() {
        // A lone `.` ends the message; a line beginning with one has it doubled
        // and the relay removes the copy.
        assert_eq!(stuffed(b".\r\n"), b"..\r\n");
        assert_eq!(stuffed(b".hidden\r\n"), b"..hidden\r\n");
        assert_eq!(
            stuffed(b"first\r\n.\r\nlast\r\n"),
            b"first\r\n..\r\nlast\r\n"
        );
        // Not at the start of a line: a price, an ellipsis, a decimal point.
        assert_eq!(stuffed(b"$0.056250\r\n"), b"$0.056250\r\n");
        assert_eq!(stuffed(b"a.b\r\n"), b"a.b\r\n");
    }

    /// The end of a body line is where stuffing is decided, so it has to survive
    /// a body whose final line has no newline of its own — which is what
    /// `wire_bytes` produces for a statement.
    #[test]
    fn test_dot_stuffing_sees_the_line_boundaries_wire_bytes_writes() {
        let message = Message::new("billing@acme.test", "p@acme.test", "S", ".\n.again").unwrap();
        let raw = message.wire_bytes();
        let stuffed = stuffed(&raw);
        let text = String::from_utf8(stuffed).expect("ASCII throughout");
        assert!(text.contains("\r\n..\r\n"), "{text}");
        assert!(text.contains("\r\n..again\r\n"), "{text}");
        // The only unstuffed line-start dot is none of them.
        assert!(!text.ends_with("\r\n.\r\n"), "{text}");
    }

    #[test]
    fn test_wire_bytes_carry_the_headers_and_crlf_line_endings() {
        let message = Message::new(
            "billing@acme.test",
            "partner@acme.test",
            "Hello",
            "one\ntwo",
        )
        .unwrap();
        let text = String::from_utf8(message.wire_bytes()).expect("ASCII throughout");
        for header in [
            "From: billing@acme.test\r\n",
            "To: partner@acme.test\r\n",
            "Subject: Hello\r\n",
            "MIME-Version: 1.0\r\n",
            "Content-Type: text/plain; charset=utf-8\r\n",
        ] {
            assert!(text.contains(header), "{header:?} missing from {text}");
        }
        // Headers, a blank line, then the body — the order that makes the first
        // body line the first body line.
        let (headers, body) = text.split_once("\r\n\r\n").expect("a header block");
        assert!(headers.contains("Subject: Hello"));
        assert_eq!(body, "one\r\ntwo\r\n");
        // Not one bare LF anywhere: to an SMTP server that is not a line ending.
        assert!(!text.contains('\n') || text.matches('\n').count() == text.matches("\r\n").count());
    }

    #[test]
    fn test_a_header_value_that_could_forge_a_recipient_is_refused() {
        // A `to` with a newline in it is a second header, and a second recipient
        // the operator never chose. This is the last place that can refuse.
        for (to, needle) in [
            ("victim@acme.test\r\nBcc: everyone@else.test", "line break"),
            ("victim@acme.test\nBcc: everyone@else.test", "line break"),
            ("victim@acme.test\u{0}", "control character"),
            ("", "empty"),
        ] {
            match Message::new("billing@acme.test", to, "S", "B") {
                Err(SendError::InvalidHeader {
                    field: "to",
                    reason,
                }) => {
                    assert!(reason.contains(needle), "{to:?} gave {reason:?}")
                }
                other => panic!("{to:?} was accepted as {other:?}"),
            }
        }
    }

    #[test]
    fn test_an_address_that_is_not_an_address_is_refused() {
        // A relay will reject this anyway; refusing here means the failure names
        // the partner record that holds it rather than arriving as a `501`.
        assert!(matches!(
            Message::new("billing@acme.test", "not-an-address", "S", "B"),
            Err(SendError::InvalidHeader { field: "to", .. })
        ));
        assert!(matches!(
            Message::new("no-at-sign", "partner@acme.test", "S", "B"),
            Err(SendError::InvalidHeader { field: "from", .. })
        ));
    }

    #[test]
    fn test_the_rendered_date_is_rfc5322_shaped() {
        let rendered = rfc5322_date(time::macros::datetime!(2026-09-27 09:05:03 UTC));
        assert_eq!(rendered, "Sun, 27 Sep 2026 09:05:03 +0000");
    }

    #[test]
    fn test_a_relay_that_is_not_configured_is_refused_before_anything_is_opened() {
        let settings = Settings {
            host: String::new(),
            port: 587,
            from_address: "billing@acme.test".into(),
            credentials: None,
        };
        assert!(!settings.is_complete());
        let message = Message::new("billing@acme.test", "p@acme.test", "S", "B").unwrap();
        match send(&settings, &message) {
            Err(SendError::Protocol(text)) => assert!(text.contains("smtp_host"), "{text}"),
            other => panic!("expected a protocol error, got {other:?}"),
        }
    }

    // ---------------------------------------------------------------------
    // The protocol, over an in-memory wire
    // ---------------------------------------------------------------------

    fn session_with(script: &str) -> Session {
        Session::new(Box::new(Cursor::new(script.as_bytes().to_vec())))
    }

    /// The capability list is the whole reason `EHLO` is read as a multi-line
    /// reply. Reading only the greeting line is how a client concludes that a
    /// relay does not support `STARTTLS` and then sends the password anyway.
    #[test]
    fn test_a_capability_after_the_first_line_is_found() {
        let mut session = session_with(
            "220 mail.test ready\r\n250-mail.test Hello\r\n250-8BITMIME\r\n250 STARTTLS\r\n",
        );
        session
            .expect(220, "greeting")
            .expect("a well-formed greeting");

        let reply = session
            .command(250, "EHLO", "EHLO acme.test")
            .expect("a well-formed EHLO reply");
        session.capabilities = reply
            .text
            .lines()
            .skip(1)
            .filter_map(|line| line.split_whitespace().next())
            .map(str::to_string)
            .collect();

        assert!(session.supports("STARTTLS"));
        // Case is not a capability: a relay that writes `starttls` is offering
        // the same thing, and a case difference must not license a cleartext
        // password.
        assert!(session.supports("starttls"));
        assert!(session.supports("8BITMIME"));
        assert!(!session.supports("AUTH"));
        // The greeting line is not a capability. Read as one, a relay named
        // `STARTTLS...` would be a relay whose whole capability list is its own
        // hostname.
        assert!(!session.supports("mail.test"));
        assert!(!session.supports("Hello"));
    }

    /// `hello()` does the skip itself, so the same property is asserted through
    /// the real path rather than by repeating the parsing in the test.
    #[test]
    fn test_hello_records_the_capabilities_and_not_the_greeting() {
        let mut session = session_with(
            "220 mail.test ready\r\n250-mail.test Hello\r\n250-AUTH PLAIN LOGIN\r\n250 SMTPUTF8\r\n",
        );
        session.expect(220, "greeting").unwrap();
        session.hello("billing@acme.test").unwrap();

        assert_eq!(session.capabilities, vec!["AUTH", "SMTPUTF8"]);
        assert!(session.supports("auth"));
        assert!(session.supports("SMTPUTF8"));
        assert!(!session.supports("STARTTLS"));
    }

    #[test]
    fn test_a_malformed_reply_is_a_protocol_error_not_a_wrong_answer() {
        for (script, needle) in [
            // The separator after the code is what says "more follows"; without
            // one the line is not a reply at all.
            ("220 ready\r\n250text\r\n", "unseparated"),
            // A code that changes mid-message would make the code meaningless.
            ("220 ready\r\n250-first\r\n251 second\r\n", "changed code"),
            ("220 ready\r\nnonsense\r\n", "not a reply"),
            // A bare code with neither a space nor a hyphen after it is not a
            // reply: the separator is what tells the client whether more follows.
            ("220 ready\r\n250\r\n", "unseparated"),
        ] {
            let mut session = session_with(script);
            session
                .expect(220, "greeting")
                .expect("the greeting is well formed");
            match session.read_reply() {
                Err(SendError::Protocol(text)) => {
                    assert!(text.contains(needle), "{script:?} gave {text:?}")
                }
                other => panic!("{script:?} gave {other:?}"),
            }
        }
    }

    /// A peer that never ends a line must not grow a buffer until the process
    /// dies. The bound is the whole reason `read_line` does not use `read_to_string`.
    #[test]
    fn test_a_reply_line_that_never_ends_is_bounded() {
        let mut script = b"220 ready\r\n".to_vec();
        script.extend_from_slice(&vec![b'a'; MAX_REPLY_BYTES + 1]);
        let mut session = Session::new(Box::new(Cursor::new(script)));
        session.expect(220, "greeting").unwrap();
        match session.read_reply() {
            Err(SendError::Protocol(text)) => {
                assert!(text.contains("without ending a line"), "{text}")
            }
            other => panic!("expected a protocol error, got {other:?}"),
        }
    }

    #[test]
    fn test_a_connection_that_ends_mid_reply_is_not_read_as_success() {
        // The relay accepted the connection and said nothing else. A client that
        // treated EOF as an empty reply would proceed to send the credential.
        let mut session = session_with("");
        assert!(matches!(
            session.expect(220, "greeting"),
            Err(SendError::Protocol(text)) if text.contains("closed the connection")
        ));
    }

    #[test]
    fn test_an_error_text_from_a_relay_is_truncated_with_a_marker() {
        let long = "x".repeat(MAX_ERROR_CHARS + 50);
        let truncated = bounded(&long);
        assert_eq!(truncated.chars().count(), MAX_ERROR_CHARS + 1);
        assert!(truncated.ends_with('…'));
        assert_eq!(bounded("short"), "short");
    }

    // ---------------------------------------------------------------------
    // The protocol, over a socket
    // ---------------------------------------------------------------------

    use super::test_smtp::{Relay, Running};

    /// What the relay heard, and what `send` made of it.
    struct Driven {
        result: Result<(), SendError>,
        commands: Vec<String>,
        data: Vec<String>,
    }

    impl Driven {
        /// Every line the relay was sent, commands and message body together.
        fn everything(&self) -> Vec<String> {
            self.commands
                .iter()
                .chain(self.data.iter())
                .cloned()
                .collect()
        }
    }

    /// Run one `send()` against a scripted relay.
    fn drive(relay: Relay, credentials: Option<SmtpCredentials>, body: &str) -> Driven {
        let running = Running::start(relay);
        let settings = Settings {
            host: "127.0.0.1".into(),
            port: running.port(),
            from_address: "billing@acme.test".into(),
            credentials,
        };
        let message =
            Message::new("billing@acme.test", "partner@acme.test", "Statement", body).unwrap();
        let result = send(&settings, &message);
        let (commands, data) = running.finish();
        Driven {
            result,
            commands,
            data,
        }
    }

    #[test]
    fn test_the_dialogue_runs_in_order_and_ends_the_message() {
        let driven = drive(Relay::default(), None, "Hello and welcome\n");
        assert_eq!(driven.result, Ok(()));
        // The order is the protocol: a `DATA` before a `RCPT` is a relay
        // rejecting the message, and a `QUIT` before the terminating dot is a
        // send that never happened.
        assert_eq!(
            driven.commands,
            vec![
                "EHLO acme.test",
                "MAIL FROM:<billing@acme.test>",
                "RCPT TO:<partner@acme.test>",
                "DATA",
                "QUIT",
            ]
        );
        assert!(driven.data.contains(&"Hello and welcome".to_string()));
        // The body the relay received is the message's own headers and text,
        // which is what makes the transcript a faithful view of the send.
        assert!(driven.data.iter().any(|l| l == "Subject: Statement"));
        assert!(driven.data.iter().any(|l| l == "To: partner@acme.test"));
    }

    /// Nothing about an anonymous relay is authenticated, and the transcript is
    /// the evidence: no `AUTH`, and therefore no credential anywhere.
    #[test]
    fn test_an_anonymous_relay_gets_no_authentication_at_all() {
        let driven = drive(Relay::default(), None, "Hello\n");
        assert_eq!(driven.result, Ok(()));
        assert!(
            !driven.everything().iter().any(|l| l.starts_with("AUTH")),
            "{:?}",
            driven.commands
        );
    }

    #[test]
    fn test_a_refused_recipient_is_reported_with_the_relays_own_words() {
        let driven = drive(
            Relay {
                rcpt_code: 550,
                rcpt_text: "5.1.1 no such user here",
                ..Relay::default()
            },
            None,
            "Hello\n",
        );
        assert_eq!(
            driven.result,
            Err(SendError::Refused {
                command: "RCPT TO",
                code: 550,
                text: "5.1.1 no such user here".to_string(),
            })
        );
        // The message never reached `DATA`, so nothing was handed over that the
        // relay could have delivered.
        assert!(driven.data.is_empty());
        assert!(!driven.commands.iter().any(|l| l == "DATA"));
    }

    #[test]
    fn test_the_body_the_relay_receives_is_dot_stuffed() {
        // A statement whose text ends a line with a dot must not end the message
        // early — the relay would read the rest of the statement as SMTP
        // commands, and the failure would be a truncated invoice.
        let driven = drive(Relay::default(), None, "balance: $0.056250\n.\nthanks\n");
        assert_eq!(driven.result, Ok(()));
        assert!(driven.data.contains(&"..".to_string()), "{:?}", driven.data);
        assert!(
            driven.data.contains(&"thanks".to_string()),
            "{:?}",
            driven.data
        );
    }

    /// The credential is the one secret this module handles, and a relay that
    /// will not encrypt is given neither it nor the message.
    #[test]
    fn test_a_credential_is_refused_rather_than_sent_to_a_relay_without_starttls() {
        let credentials = SmtpCredentials::new("portal@acme.test".into(), "hunter2-hunter2".into());
        let driven = drive(Relay::default(), Some(credentials), "Hello\n");

        assert_eq!(driven.result, Err(SendError::CredentialsWithoutTls));
        let everything = driven.everything().join("\n");
        assert!(!everything.contains("AUTH"), "{everything}");
        assert!(!everything.contains("hunter2"), "{everything}");
        assert!(!everything.contains("portal@acme.test"), "{everything}");
        // And the message was not sent either: an unauthenticated send to a
        // relay that wanted authentication is a bounced invoice, not a
        // delivered one.
        assert!(driven.data.is_empty());
        assert!(!driven.commands.iter().any(|l| l == "DATA"));
    }

    /// With no TLS in the build, a relay that offers `STARTTLS` is still not a
    /// channel to authenticate over: the upgrade cannot happen, so the send
    /// fails with a reason rather than continuing in the clear.
    #[cfg(not(feature = "hyper-rustls"))]
    #[test]
    fn test_a_credential_without_tls_in_the_build_fails_and_sends_nothing() {
        let credentials = SmtpCredentials::new("portal@acme.test".into(), "hunter2-hunter2".into());
        let driven = drive(
            Relay {
                capabilities: vec!["8BITMIME", "STARTTLS"],
                ..Relay::default()
            },
            Some(credentials),
            "Hello\n",
        );

        assert_eq!(driven.result, Err(SendError::NoTlsInBuild));
        // The relay offered the upgrade and the client asked for it — and then
        // stopped. A client that carried on would have written the password into
        // a cleartext socket.
        assert_eq!(
            driven.commands,
            vec!["EHLO acme.test", "STARTTLS"],
            "the send must stop at the failed upgrade"
        );
        let everything = driven.everything().join("\n");
        assert!(!everything.contains("AUTH"), "{everything}");
        assert!(!everything.contains("hunter2"), "{everything}");
    }

    // ---------------------------------------------------------------------
    // What the partner reads
    // ---------------------------------------------------------------------

    fn statement(mode: &str, total: i64, due_at: Option<&str>, incomplete: i64) -> Statement {
        Statement {
            id: 41,
            consumer_id: "acme".to_string(),
            billing_date: "2026-09-27".to_string(),
            billing_mode: mode.to_string(),
            currency: crate::billing::CURRENCY.to_string(),
            period_start: "2026-09-27T00:00:00.000000000Z".to_string(),
            period_end: "2026-09-28T00:00:00.000000000Z".to_string(),
            billing_cutoff_at: "2026-09-28T00:05:00.000000000Z".to_string(),
            total_amount_micro_usd: total,
            incomplete_usage_count: incomplete,
            due_at: due_at.map(str::to_string),
            paid_at: None,
            paid_by: None,
            payment_reference: None,
            payment_note: None,
            email_sent_at: None,
            email_attempts: 0,
            email_last_error: None,
            email_next_retry_at: None,
            created_at: "2026-09-28T00:05:00.000000000Z".to_string(),
            updated_at: "2026-09-28T00:05:00.000000000Z".to_string(),
        }
    }

    fn line(model: &str, input: i64, cached: i64, output: i64, total: i64) -> StatementLine {
        StatementLine {
            id: 1,
            statement_id: 41,
            model: model.to_string(),
            prices: PricingSnapshot::new(
                PricePerMillion::new(2_500_000),
                PricePerMillion::new(1_250_000),
                PricePerMillion::new(10_000_000),
            ),
            request_count: 3,
            input_tokens: input,
            cached_input_tokens: cached,
            uncached_input_tokens: input - cached,
            output_tokens: output,
            input_cost_micro_usd: 0,
            cached_input_cost_micro_usd: 0,
            output_cost_micro_usd: 0,
            total_cost_micro_usd: total,
        }
    }

    fn render(statement: &Statement, lines: &[StatementLine]) -> Message {
        statement_message(
            statement,
            lines,
            "Acme",
            "billing@portal.test",
            "partner@acme.test",
        )
        .expect("a renderable statement")
    }

    #[test]
    fn test_an_invoice_statement_carries_the_amount_and_the_deadline() {
        let statement = statement("invoice", 56_250, Some("2026-09-28T12:00:00.000000000Z"), 0);
        let message = render(
            &statement,
            &[line("gpt-4o", 1_000_000, 250_000, 12_345, 56_250)],
        );

        assert_eq!(
            message.subject,
            "Statement for 2026-09-27: $0.056250 due 2026-09-28 12:00:00 UTC"
        );
        assert!(message.body.contains("gpt-4o"), "{}", message.body);
        // Grouped digits: a partner checks these by eye, and six unbroken digits
        // is where that stops working.
        assert!(message.body.contains("1,000,000"), "{}", message.body);
        assert!(message.body.contains("250,000"), "{}", message.body);
        assert!(message.body.contains("12,345"), "{}", message.body);
        // The amount appears in the table, in the total row and in the deadline
        // sentence — and it is the statement's own total every time.
        assert!(
            message.body.matches("$0.056250").count() >= 3,
            "{}",
            message.body
        );
        assert!(
            message.body.contains("2026-09-27 00:00:00 UTC"),
            "{}",
            message.body
        );
        assert!(
            message.body.contains("2026-09-28 00:00:00 UTC"),
            "{}",
            message.body
        );
        assert!(
            message
                .body
                .contains("Payment of $0.056250 is due by 2026-09-28 12:00:00 UTC"),
            "{}",
            message.body
        );
        assert!(message.body.contains("Statement reference: 41"));
        // A complete statement says nothing about incomplete usage: the sentence
        // exists to stop a total being misread, and there is nothing to misread.
        assert!(!message.body.contains("could not be measured"));
    }

    /// The total is the row's, not a sum the renderer recomputed. Two
    /// arithmetics is one too many, and the row is the number the partner is
    /// asked to pay.
    #[test]
    fn test_the_rendered_total_is_the_statements_own_amount() {
        // A line whose own cost and the statement's total deliberately disagree:
        // the total row must be the statement's number and the model row must be
        // the line's, and neither may be derived from the other here. A renderer
        // that summed the lines would show a total the partner is not being
        // asked for.
        let statement = statement(
            "invoice",
            999_999,
            Some("2026-09-28T12:00:00.000000000Z"),
            0,
        );
        let message = render(&statement, &[line("gpt-4o", 1_000_000, 0, 0, 1)]);
        let total_row = message
            .body
            .lines()
            .find(|l| l.starts_with("total"))
            .expect("a total row");
        assert!(total_row.contains("$0.999999"), "{total_row}");
        let model_row = message
            .body
            .lines()
            .find(|l| l.starts_with("gpt-4o"))
            .expect("a model row");
        assert!(model_row.contains("$0.000001"), "{model_row}");
    }

    #[test]
    fn test_a_reconciliation_statement_asks_for_nothing() {
        // A real amount on a statement with no obligation: the settlement record
        // the specification requires, and not an invoice in disguise. Modelled
        // as a zero price or a year-9999 deadline it would read as one.
        let statement = statement("reconciliation", 95_000, None, 0);
        let message = render(&statement, &[line("gpt-4o", 1_000_000, 0, 0, 95_000)]);

        assert!(
            message
                .subject
                .starts_with("Settlement statement for 2026-09-27")
        );
        assert!(message.body.contains("$0.095000"), "{}", message.body);
        assert!(
            message.body.contains("reconciliation statement"),
            "{}",
            message.body
        );
        assert!(!message.body.contains("is due by"), "{}", message.body);
        assert!(!message.body.contains("suspended"), "{}", message.body);
    }

    #[test]
    fn test_an_incomplete_day_is_stated_and_says_it_suspends_nothing() {
        let statement = statement("invoice", 56_250, Some("2026-09-28T12:00:00.000000000Z"), 7);
        let message = render(&statement, &[line("gpt-4o", 1_000_000, 0, 12_345, 56_250)]);

        assert!(message.body.contains("7 request(s)"), "{}", message.body);
        assert!(message.body.contains("not charged"), "{}", message.body);
        assert!(
            message.body.contains("No suspension follows"),
            "{}",
            message.body
        );
    }

    #[test]
    fn test_a_day_with_no_billable_usage_says_so_rather_than_showing_an_empty_table() {
        let statement = statement("invoice", 0, Some("2026-09-28T12:00:00.000000000Z"), 0);
        let message = render(&statement, &[]);

        assert!(
            message.body.contains("No billable usage was recorded"),
            "{}",
            message.body
        );
        assert!(!message.body.contains("model"), "{}", message.body);
    }

    /// The operator's own annotations — who marked it paid, their note, the last
    /// relay error — are not part of what a partner is shown, and a renderer that
    /// widened to `SELECT *` would leak them the first time one contained
    /// something worth not sending.
    #[test]
    fn test_the_operators_notes_do_not_ride_along_on_the_statement() {
        let mut statement = statement("invoice", 56_250, Some("2026-09-28T12:00:00.000000000Z"), 0);
        statement.paid_at = Some("2026-09-28T13:00:00.000000000Z".to_string());
        statement.paid_by = Some("manager:john".to_string());
        statement.payment_reference = Some("SENTINEL-REFERENCE".to_string());
        statement.payment_note = Some("SENTINEL-NOTE".to_string());
        statement.email_last_error = Some("SENTINEL-RELAY-ERROR".to_string());
        let message = render(&statement, &[line("gpt-4o", 1, 0, 1, 56_250)]);

        for sentinel in [
            "SENTINEL-REFERENCE",
            "SENTINEL-NOTE",
            "SENTINEL-RELAY-ERROR",
            "manager:john",
        ] {
            assert!(
                !message.body.contains(sentinel),
                "{sentinel} reached the partner"
            );
            assert!(
                !message.subject.contains(sentinel),
                "{sentinel} reached the subject"
            );
        }
    }

    #[test]
    fn test_the_statement_is_sent_to_the_partner_and_from_the_operator() {
        let statement = statement("invoice", 1, Some("2026-09-28T12:00:00.000000000Z"), 0);
        let message = render(&statement, &[]);
        assert_eq!(message.to, "partner@acme.test");
        assert_eq!(message.from, "billing@portal.test");
        // And the envelope the relay is given is the same pair, so a partner
        // cannot be shown one sender and receive another.
        let driven = drive(
            Relay::default(),
            None,
            String::from_utf8_lossy(&message.wire_bytes()).as_ref(),
        );
        assert_eq!(driven.result, Ok(()));
        assert!(
            driven
                .commands
                .contains(&"MAIL FROM:<billing@acme.test>".to_string())
        );
        assert!(
            driven
                .commands
                .contains(&"RCPT TO:<partner@acme.test>".to_string())
        );
    }

    #[test]
    fn test_counts_are_grouped_in_threes() {
        assert_eq!(grouped(0), "0");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(1_000), "1,000");
        assert_eq!(grouped(12_345), "12,345");
        assert_eq!(grouped(1_234_567), "1,234,567");
    }
}
