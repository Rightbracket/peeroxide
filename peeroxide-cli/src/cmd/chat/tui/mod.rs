//! Interactive terminal UI for `peeroxide chat join`.
//!
//! Two implementations of [`ChatUi`] are provided:
//!
//! - [`line::LineUi`]: byte-compatible with the historical behaviour —
//!   line-oriented stdin, `println!`/`eprintln!` to stdout/stderr. Used when
//!   stdout is not a TTY, when `--line-mode` is passed, or when
//!   `PEEROXIDE_LINE_MODE=1` is set in the environment.
//! - [`interactive::InteractiveUi`]: full TTY mode with a status bar pinned at
//!   the bottom of the terminal, multi-line input area, slash commands, and
//!   chat history flowing through a scroll region above.
//!
//! Pick one via [`make_ui`]. Callers (i.e. `join.rs`) interact only through
//! the [`ChatUi`] trait, so the two implementations are interchangeable.

pub mod commands;
pub mod input;
pub mod interactive;
pub mod line;
pub mod status;
pub mod terminal;

use std::collections::HashSet;
use std::io::IsTerminal;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::cmd::chat::display::DisplayMessage;

pub use commands::SlashCommand;
pub use status::{DhtActivityGuard, RecvFetchGuard, StatusState};

/// Cheap-to-clone handle used by spawned background tasks (publisher, reader,
/// nexus refresh, friend refresh, post.rs helpers) to surface a user-visible
/// system notice — e.g. `"  nexus published (seq=…)"` or `"warning: feed
/// mutable_put failed: …"`.
///
/// Notices flow into a `mpsc::UnboundedSender<String>`; the main loop in
/// `join.rs` drains the corresponding receiver and forwards each line through
/// [`ChatUi::render_system`]. This keeps spawned tasks free of `ChatUi`
/// references and makes it impossible for a background task to accidentally
/// write directly into the terminal at the wrong cursor position (which would
/// land on top of the interactive UI's input area).
///
/// In line mode the round-trip is byte-equivalent to the historical
/// `eprintln!` because `LineUi::render_system` is itself an `eprintln!`.
#[derive(Clone)]
pub struct NoticeSink {
    tx: tokio::sync::mpsc::UnboundedSender<String>,
}

impl NoticeSink {
    pub fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    /// Send a notice. Silently drops if the receiver has been closed — there
    /// is no value in panicking from a background task on a UI teardown race.
    pub fn send(&self, line: impl Into<String>) {
        let _ = self.tx.send(line.into());
    }

    /// Equivalent to `send(format!(...))` but spelled to match the `eprintln!`
    /// call sites it's replacing for grep-ability.
    pub fn notify(&self, line: impl Into<String>) {
        self.send(line);
    }
}

mod notice_global {
    //! Process-wide notice sink for code paths that can't easily take a
    //! `NoticeSink` parameter (probe traces deep inside helpers, etc.).
    //!
    //! Set once at session start by `join::run` (via [`super::install_global_notice_sink`]) and
    //! never replaced. Concurrent calls from spawned tasks are safe — the
    //! underlying `UnboundedSender` is `Clone + Send + Sync`.

    use std::sync::OnceLock;

    static GLOBAL: OnceLock<super::NoticeSink> = OnceLock::new();

    pub fn install(sink: super::NoticeSink) {
        // `set` returns Err if already initialized; we just leave the first
        // one in place. That matches the "one session per process" model.
        let _ = GLOBAL.set(sink);
    }

    pub fn try_get() -> Option<&'static super::NoticeSink> {
        GLOBAL.get()
    }
}

/// Register `sink` as the process-wide notice channel. Idempotent: the first
/// caller wins; subsequent calls are no-ops (the session model is one chat
/// loop per process). Used by deep helpers that emit probe / warning lines
/// without taking a `NoticeSink` parameter.
pub fn install_global_notice_sink(sink: NoticeSink) {
    notice_global::install(sink);
}

/// Emit a single system-notice line. If a global sink has been registered
/// (i.e. we're inside a `chat join` session), route through it so the
/// interactive UI can paint the line into the scroll region. Otherwise
/// fall back to `eprintln!` — that preserves behaviour for standalone
/// subcommands and for tests that don't construct a `ChatUi`.
pub fn emit_notice(line: impl Into<String>) {
    let line = line.into();
    match notice_global::try_get() {
        Some(sink) => sink.send(line),
        None => eprintln!("{line}"),
    }
}

/// One unit of user input from the UI. `Message` and `Command` are produced by
/// the input handler; `Eof` and `Interrupt` are signals from the terminal.
#[derive(Debug)]
pub enum UiInput {
    /// User typed and submitted a chat message.
    Message(String),
    /// User typed a slash command (e.g. `/quit`, `/ignore alice`).
    Command(SlashCommand),
    /// stdin reached EOF (e.g. piped input completed).
    Eof,
    /// Ctrl-C or equivalent interrupt.
    Interrupt,
}

/// Shared local-only state that survives across input lines: who the user is
/// currently ignoring (consulted by the reader task before forwarding inbound
/// messages to the display).
pub type IgnoreSet = Arc<RwLock<HashSet<[u8; 32]>>>;

/// Common surface that `join.rs` uses to interact with the user, regardless of
/// whether we're in line mode or interactive TUI mode.
pub trait ChatUi: Send {
    /// Render an inbound (or self-echoed) chat message.
    fn render_message(&self, msg: &DisplayMessage);

    /// Render a system notice (`*** ...`, debug log, probe trace).
    fn render_system(&self, line: &str);

    /// Snapshot of observable status counters. Updated by publisher / reader /
    /// dht-poll task; consumed by the status bar renderer.
    fn status(&self) -> Arc<StatusState>;

    /// Shared ignore set. The reader task should consult this before
    /// forwarding a message to `render_message`.
    fn ignore_set(&self) -> IgnoreSet;

    /// Wait for the next input event from the user.
    ///
    /// Returns `None` once the input source is permanently closed (the UI is
    /// shutting down). Callers should treat this as terminal.
    fn next_input(&mut self) -> futures::future::BoxFuture<'_, Option<UiInput>>;

    /// Tear down the UI cleanly. After this returns, the terminal must be in
    /// a usable state (cursor visible, raw mode disabled, scroll region reset).
    fn shutdown(self: Box<Self>) -> futures::future::BoxFuture<'static, ()>;
}

/// Options controlling which `ChatUi` is constructed.
#[derive(Debug, Clone)]
pub struct UiOptions {
    /// Force line mode regardless of whether stdout is a TTY (`--line-mode`).
    pub force_line_mode: bool,
    /// Channel name to display on the status bar.
    pub channel_name: String,
    /// Profile name for `/friend` and `/unfriend` resolution.
    pub profile_name: String,
}

/// Build the appropriate `ChatUi` implementation based on the runtime
/// environment and command-line flags.
///
/// Picks `InteractiveUi` only when **both** stdout and stdin are TTYs and the
/// user hasn't opted out via `--line-mode` / `PEEROXIDE_LINE_MODE`. Falls back
/// to `LineUi` on any error setting up the interactive renderer.
///
/// Stdin must also be a TTY because interactive mode reads keystrokes via
/// `crossterm::event::EventStream`, which polls the controlling terminal —
/// when stdin is a pipe or redirected file the event reader returns errors
/// or stalls, so a pipeline like `cat msgs.txt | peeroxide chat join …`
/// would fail. Auto-detecting non-TTY stdin and falling back to line mode
/// lets such pipelines just work without needing `--line-mode`.
pub fn make_ui(opts: UiOptions) -> Box<dyn ChatUi> {
    let stdout_is_tty = std::io::stdout().is_terminal();
    let stdin_is_tty = std::io::stdin().is_terminal();
    let want_interactive = !opts.force_line_mode && stdout_is_tty && stdin_is_tty;
    if want_interactive {
        match interactive::InteractiveUi::new(&opts) {
            Ok(ui) => return Box::new(ui),
            Err(e) => {
                eprintln!("*** interactive UI unavailable ({e}); falling back to line mode");
            }
        }
    }
    Box::new(line::LineUi::new(opts))
}
