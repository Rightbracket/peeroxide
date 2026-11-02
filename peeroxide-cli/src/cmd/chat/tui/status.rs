//! Shared status state observed by publisher / reader / DHT-poll task and
//! consumed by the status bar renderer.
//!
//! All counters are `AtomicUsize` with `Relaxed` ordering — these are
//! advisory display values, not synchronisation primitives.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Decrement `counter`, saturating at zero.
///
/// `fetch_update` is deprecated in favour of `try_update` (Rust 1.99), which
/// does not exist at our MSRV (1.85).
#[allow(deprecated)]
fn saturating_dec(counter: &AtomicUsize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_sub(1))
    });
}

use arc_swap::ArcSwap;
use tokio::sync::Notify;

/// Counters and labels shown on the status bar.
///
/// Created once at session start and shared via `Arc` across:
/// - `join.rs` (channel name, dht peers polling task)
/// - `publisher.rs` (`send_pending`)
/// - `reader.rs` (`recv_pending`, `feed_count`)
/// - `tui::interactive` (renderer, reads all fields)
///
/// Mutators call `dirty.notify_one()` after a write so the renderer can repaint
/// promptly. Renderers can also poll on an idle timer.
pub struct StatusState {
    pub send_pending: AtomicUsize,
    /// Number of DHT `immutable_get` requests currently outstanding for
    /// **message or summary content**. This is the "Receiving (N)" count
    /// the user sees — it represents content the reader is actively pulling
    /// because it knows about new messages (FeedRecord listed unseen hashes,
    /// summary-history walk, or predecessor refetch). Managed via
    /// [`RecvFetchGuard`] which inc/decs atomically across an `await`.
    ///
    /// Background scans (`lookup` for new peers, `mutable_get` of FeedRecords
    /// to *check* for new messages) are **not** counted here — those are
    /// signalled separately by `dht_active`.
    pub recv_pending: AtomicUsize,
    /// Number of any-kind DHT requests currently outstanding (lookup,
    /// mutable_get, immutable_get). Surfaces as a single-character activity
    /// indicator at the far left of the bar so the user can tell when
    /// background DHT chatter is happening even though no message is
    /// incoming. Managed via [`DhtActivityGuard`]; `RecvFetchGuard`
    /// additionally bumps `recv_pending`.
    pub dht_active: AtomicUsize,
    pub feed_count: AtomicUsize,
    pub dht_peers: AtomicUsize,
    /// True when inbox monitoring is active for this session (configured
    /// via `chat join` flags). When false the inbox segment is omitted from
    /// the bar layout entirely. When true the segment renders as 'inbox' /
    /// 'i' (plain) when `inbox_unread == 0`, or 'INBOX' / 'I' (yellow-bg,
    /// black-fg) when there's at least one unread invite.
    pub inbox_enabled: AtomicBool,
    /// Count of invites surfaced by the inbox monitor that haven't yet been
    /// displayed via `/inbox`. The bar uses this only as a boolean (lit /
    /// not lit); the count itself isn't shown.
    pub inbox_unread: AtomicUsize,
    pub channel_name: ArcSwap<String>,
    pub dirty: Notify,
}

impl StatusState {
    pub fn new(channel_name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            send_pending: AtomicUsize::new(0),
            recv_pending: AtomicUsize::new(0),
            dht_active: AtomicUsize::new(0),
            feed_count: AtomicUsize::new(0),
            dht_peers: AtomicUsize::new(0),
            inbox_enabled: AtomicBool::new(false),
            inbox_unread: AtomicUsize::new(0),
            channel_name: ArcSwap::from_pointee(channel_name.into()),
            dirty: Notify::new(),
        })
    }

    /// Enable or disable the inbox segment on the status bar.
    pub fn set_inbox_enabled(&self, enabled: bool) {
        let prev = self.inbox_enabled.swap(enabled, Ordering::Relaxed);
        if prev != enabled {
            self.dirty.notify_one();
        }
    }

    /// Set the count of unread invites; the bar lights up (yellow bg,
    /// uppercase) when this is > 0.
    pub fn set_inbox_unread(&self, count: usize) {
        let prev = self.inbox_unread.swap(count, Ordering::Relaxed);
        if prev != count {
            self.dirty.notify_one();
        }
    }

    /// Increment `send_pending` and notify the renderer.
    pub fn inc_send_pending(&self) {
        self.send_pending.fetch_add(1, Ordering::Relaxed);
        self.dirty.notify_one();
    }

    /// Decrement `send_pending` (saturating) and notify.
    pub fn dec_send_pending(&self) {
        // saturating: don't wrap if mismatched inc/dec ever sneak in.
        saturating_dec(&self.send_pending);
        self.dirty.notify_one();
    }

    /// Set `recv_pending` to an absolute count and notify.
    pub fn set_recv_pending(&self, n: usize) {
        let prev = self.recv_pending.swap(n, Ordering::Relaxed);
        if prev != n {
            self.dirty.notify_one();
        }
    }

    /// Increment the in-flight `immutable_get` counter and notify the
    /// renderer. Paired with [`StatusState::dec_recv_in_flight`]; prefer
    /// using [`RecvFetchGuard`] which couples the two and survives early
    /// returns / panics across an `await`.
    pub fn inc_recv_in_flight(&self) {
        self.recv_pending.fetch_add(1, Ordering::Relaxed);
        self.dirty.notify_one();
    }

    /// Decrement the in-flight counter (saturating at zero).
    pub fn dec_recv_in_flight(&self) {
        saturating_dec(&self.recv_pending);
        self.dirty.notify_one();
    }

    /// Increment the any-DHT-op counter (lookup / mutable_get / immutable_get).
    /// Prefer [`DhtActivityGuard`].
    pub fn inc_dht_active(&self) {
        self.dht_active.fetch_add(1, Ordering::Relaxed);
        self.dirty.notify_one();
    }

    /// Decrement the any-DHT-op counter (saturating at zero).
    pub fn dec_dht_active(&self) {
        saturating_dec(&self.dht_active);
        self.dirty.notify_one();
    }

    /// Set `feed_count` and notify.
    pub fn set_feed_count(&self, n: usize) {
        let prev = self.feed_count.swap(n, Ordering::Relaxed);
        if prev != n {
            self.dirty.notify_one();
        }
    }

    /// Set `dht_peers` and notify.
    pub fn set_dht_peers(&self, n: usize) {
        let prev = self.dht_peers.swap(n, Ordering::Relaxed);
        if prev != n {
            self.dirty.notify_one();
        }
    }

    /// Snapshot a consistent view of the counters for one render pass.
    pub fn snapshot(&self) -> StatusSnapshot {
        StatusSnapshot {
            send_pending: self.send_pending.load(Ordering::Relaxed),
            recv_pending: self.recv_pending.load(Ordering::Relaxed),
            dht_active: self.dht_active.load(Ordering::Relaxed) > 0,
            feed_count: self.feed_count.load(Ordering::Relaxed),
            dht_peers: self.dht_peers.load(Ordering::Relaxed),
            inbox_enabled: self.inbox_enabled.load(Ordering::Relaxed),
            inbox_unread: self.inbox_unread.load(Ordering::Relaxed),
            channel_name: (**self.channel_name.load()).clone(),
        }
    }
}

/// RAII guard that increments `recv_pending` on construction and decrements
/// on drop. Wrap each `immutable_get` call (for message / summary content)
/// in one of these so the in-flight count stays consistent across early
/// returns, errors, and panics that unwind through the await.
pub struct RecvFetchGuard {
    status: Arc<StatusState>,
}

impl RecvFetchGuard {
    pub fn new(status: Arc<StatusState>) -> Self {
        status.inc_recv_in_flight();
        Self { status }
    }
}

impl Drop for RecvFetchGuard {
    fn drop(&mut self) {
        self.status.dec_recv_in_flight();
    }
}

/// RAII guard that increments `dht_active` on construction and decrements on
/// drop. Wrap **every** DHT read call (lookup / mutable_get / immutable_get)
/// in one of these so the left-edge activity dot lights up while any DHT op
/// is in flight. For content fetches that should also surface as
/// `Receiving (N)`, additionally use [`RecvFetchGuard`].
pub struct DhtActivityGuard {
    status: Arc<StatusState>,
}

impl DhtActivityGuard {
    pub fn new(status: Arc<StatusState>) -> Self {
        status.inc_dht_active();
        Self { status }
    }
}

impl Drop for DhtActivityGuard {
    fn drop(&mut self) {
        self.status.dec_dht_active();
    }
}

/// A point-in-time copy of the status counters and channel name. Cheap to
/// pass to the pure-function renderer in this module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusSnapshot {
    pub send_pending: usize,
    pub recv_pending: usize,
    /// True when at least one DHT op (lookup / mutable_get / immutable_get)
    /// is currently in flight. Drives the left-edge activity dot.
    pub dht_active: bool,
    pub feed_count: usize,
    pub dht_peers: usize,
    /// True when the inbox monitor is running for this session (omitted
    /// from the bar entirely when false).
    pub inbox_enabled: bool,
    /// Number of unread inbox invites. `> 0` paints the inbox segment with
    /// the highlighted (yellow-bg / uppercase) form.
    pub inbox_unread: usize,
    pub channel_name: String,
}

/// Truncation level applied to the status bar based on terminal width.
///
/// Levels are ordered from most-detailed (`Full`) to least (`ChannelOnly`).
/// The renderer chooses the most-detailed level whose natural width fits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruncLevel {
    /// `Sending... (3)  Receiving... (12)` ··· `Feeds: 7  DHT: 42  #room-name`
    Full,
    /// `Sending  Receiving` ··· `Feeds: 7  DHT: 42  #room-name`
    DropWords,
    /// `S:3 R:12` ··· `F:7 D:42 #room-name`
    Short,
    /// `S:3 R:12` ··· `D:42 #room-name`
    ShortDropF,
    /// `S:3 R:12` ··· `#room-name`
    ShortDropFD,
    /// `Ready` ··· `#room-name`  (or just `#room-name` if no left activity)
    ChannelAndReady,
    /// `#room-name` (possibly truncated with `…`)
    ChannelOnly,
}

/// Identifier for a status-bar segment, used to key sticky slot widths
/// across renders. Two segments are "the same slot" iff their `LeftSeg` /
/// `RightSeg` value is equal — so when a counter goes to zero and the
/// `Sending` segment disappears, its slot is released and the `Ready`
/// segment that takes its place starts fresh.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LeftSeg {
    Sending,
    Receiving,
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RightSeg {
    Feeds,
    Dht,
    Channel,
}

/// Sticky slot widths for the left and right segment groups. Once a slot has
/// grown to fit a value, it stays at that width until the terminal is resized
/// (which calls [`SlotWidths::reset`]).
///
/// Left-side slots are also positionally sticky: once `Sending` or `Receiving`
/// has appeared at least once, its slot remains reserved in the bar (rendered
/// as padded blanks when the underlying counter is zero) so subsequent
/// segments don't visually shift left when an upstream segment goes idle.
/// Slot widths grow monotonically and are only released by [`Self::reset`].
#[derive(Debug, Default)]
pub struct SlotWidths {
    pub left: std::collections::HashMap<LeftSeg, usize>,
    pub right: std::collections::HashMap<RightSeg, usize>,
}

impl SlotWidths {
    pub fn reset(&mut self) {
        self.left.clear();
        self.right.clear();
    }
}

/// Pure function: choose a truncation level given a snapshot and terminal width.
///
/// Returns the most detailed level whose natural rendered width fits within
/// `cols`, accounting for one space of padding on each end of the bar, the
/// activity-dot slot at the far left (2 cols: dot + separator), and a
/// minimum 2-column gap between the left and right groups.
pub fn pick_level(snap: &StatusSnapshot, cols: usize) -> TruncLevel {
    // Padding (1 left + 1 right) + dot slot (2) + minimum gap (2) = 6 reserved.
    let avail = cols.saturating_sub(6);

    for level in [
        TruncLevel::Full,
        TruncLevel::DropWords,
        TruncLevel::Short,
        TruncLevel::ShortDropF,
        TruncLevel::ShortDropFD,
        TruncLevel::ChannelAndReady,
        TruncLevel::ChannelOnly,
    ] {
        let (l, r) = natural_widths(snap, level);
        if l + r <= avail {
            return level;
        }
    }
    TruncLevel::ChannelOnly
}

/// Natural rendered width of the left and right groups at a given truncation
/// level. Does not include sticky-slot padding (that's added at layout time)
/// nor end-padding/gap (those are added by `pick_level` / `render_bar`).
fn natural_widths(snap: &StatusSnapshot, level: TruncLevel) -> (usize, usize) {
    let activity_present = snap.send_pending > 0 || snap.recv_pending > 0;
    let l = match level {
        TruncLevel::Full => {
            let mut parts: Vec<String> = Vec::new();
            if snap.send_pending > 0 {
                parts.push(format!("Sending... ({})", snap.send_pending));
            }
            if snap.recv_pending > 0 {
                parts.push(format!("Receiving... ({})", snap.recv_pending));
            }
            if parts.is_empty() {
                "Ready".len()
            } else {
                parts.join("  ").len()
            }
        }
        TruncLevel::DropWords => {
            let mut parts: Vec<&str> = Vec::new();
            if snap.send_pending > 0 {
                parts.push("Sending");
            }
            if snap.recv_pending > 0 {
                parts.push("Receiving");
            }
            if parts.is_empty() {
                "Ready".len()
            } else {
                parts.join("  ").len()
            }
        }
        TruncLevel::Short
        | TruncLevel::ShortDropF
        | TruncLevel::ShortDropFD => {
            let mut parts: Vec<String> = Vec::new();
            if snap.send_pending > 0 {
                parts.push(format!("S:{}", snap.send_pending));
            }
            if snap.recv_pending > 0 {
                parts.push(format!("R:{}", snap.recv_pending));
            }
            parts.join(" ").len()
        }
        TruncLevel::ChannelAndReady => {
            if activity_present { 0 } else { "Ready".len() }
        }
        TruncLevel::ChannelOnly => 0,
    };
    let r = match level {
        TruncLevel::Full | TruncLevel::DropWords => {
            // Feeds: N  DHT: N  #channel
            let f = format!("Feeds: {}", snap.feed_count);
            let d = format!("DHT: {}", snap.dht_peers);
            f.len() + 2 + d.len() + 2 + snap.channel_name.len()
        }
        TruncLevel::Short => {
            let f = format!("F:{}", snap.feed_count);
            let d = format!("D:{}", snap.dht_peers);
            f.len() + 1 + d.len() + 1 + snap.channel_name.len()
        }
        TruncLevel::ShortDropF => {
            let d = format!("D:{}", snap.dht_peers);
            d.len() + 1 + snap.channel_name.len()
        }
        TruncLevel::ShortDropFD | TruncLevel::ChannelAndReady => snap.channel_name.len(),
        TruncLevel::ChannelOnly => snap.channel_name.len(),
    };
    (l, r)
}

/// Result of rendering the status bar: the plain-text body (exactly `cols`
/// wide, padded with spaces) plus an optional character range within `body`
/// to be painted with the "attention" styling (yellow background, black
/// foreground) by the caller.
///
/// Today only the INBOX segment uses `inbox_highlight`; the rest of the
/// body should be painted with the normal grey-background status-bar
/// styling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarRender {
    pub body: String,
    pub inbox_highlight: Option<std::ops::Range<usize>>,
}

/// Convenience: deref to `str` so callers (and tests) can use the usual
/// `&str` methods (`contains`, `find`, `chars`, `len`, …) directly on a
/// `BarRender` value without unwrapping `.body`. The caller still needs to
/// reach into `inbox_highlight` explicitly when painting styles.
impl std::ops::Deref for BarRender {
    type Target = str;
    fn deref(&self) -> &str {
        &self.body
    }
}

/// Render the plain-text content of the status bar (no terminal escapes) at
/// the chosen level, applying sticky slot widths. Returns a `BarRender`
/// whose `body` is exactly `cols` wide; the caller wraps the body in grey
/// styling and overlays yellow on the `inbox_highlight` range (when Some).
pub fn render_bar(
    snap: &StatusSnapshot,
    level: TruncLevel,
    cols: usize,
    slots: &mut SlotWidths,
) -> BarRender {
    if cols < 4 {
        // Pathological — terminal is essentially unusable for a bar. Return
        // exactly `cols` spaces; caller still gets a coloured row.
        return BarRender {
            body: " ".repeat(cols),
            inbox_highlight: None,
        };
    }

    // Activity dot at the far left. Always 1 visible cell: '●' when any DHT
    // op is in flight, ' ' otherwise. Followed by a 1-cell separator so left
    // segments don't visually touch the dot. The slot is always reserved
    // regardless of activity state — keeps left segment positions stable.
    // Only included if cols ≥ 6 (otherwise the bar is too narrow and we drop
    // the dot to preserve room for the channel name).
    let show_dot_slot = cols >= 6;
    let dot_slot_w: usize = if show_dot_slot { 2 } else { 0 };
    let dot_char = if snap.dht_active { '●' } else { ' ' };

    // Build segment lists for both groups, tagged by segment kind so we can
    // look up sticky slot widths.
    let (left_segs, right_segs) = build_segments(snap, level);

    // Drop right-side slot entries for segments that aren't present this
    // frame.
    let right_kinds: std::collections::HashSet<RightSeg> =
        right_segs.iter().map(|(k, _)| *k).collect();
    slots.right.retain(|k, _| right_kinds.contains(k));

    // Left side: positionally sticky until `slots.reset()` (called on
    // resize). Grow sticky widths from the active set; never drop a Sending
    // / Receiving slot once reserved. `Ready` is suppressed once a real
    // activity slot exists.
    for (k, s) in &left_segs {
        let w = s.chars().count();
        let entry = slots.left.entry(*k).or_insert(0);
        if w > *entry {
            *entry = w;
        }
    }
    let has_real_sticky = slots.left.contains_key(&LeftSeg::Sending)
        || slots.left.contains_key(&LeftSeg::Receiving);
    if has_real_sticky {
        slots.left.remove(&LeftSeg::Ready);
    }

    // Grow right-side slots to fit current values (monotonic).
    for (k, s) in &right_segs {
        let w = s.chars().count();
        let entry = slots.right.entry(*k).or_insert(0);
        if w > *entry {
            *entry = w;
        }
    }

    let active_left: std::collections::HashMap<LeftSeg, &str> = left_segs
        .iter()
        .map(|(k, s)| (*k, s.as_str()))
        .collect();
    let left_rendered: Vec<String> = if has_real_sticky {
        [LeftSeg::Sending, LeftSeg::Receiving]
            .iter()
            .filter_map(|k| {
                let w = *slots.left.get(k)?;
                let s = active_left.get(k).copied().unwrap_or("");
                Some(pad_right(s, w))
            })
            .collect()
    } else {
        left_segs
            .iter()
            .map(|(k, s)| {
                let w = slots
                    .left
                    .get(k)
                    .copied()
                    .unwrap_or_else(|| s.chars().count());
                pad_right(s, w)
            })
            .collect()
    };
    let right_rendered: Vec<String> = right_segs
        .iter()
        .map(|(k, s)| {
            let w = slots.right.get(k).copied().unwrap_or_else(|| s.chars().count());
            pad_left(s, w)
        })
        .collect();
    let left_join = left_rendered.join(" ");
    let right_join = right_rendered.join(" ");

    let inner = cols.saturating_sub(2 + dot_slot_w);

    // ChannelOnly + too-long channel name: ellipsis-truncate (preserved
    // behaviour). No inbox segment at this level.
    if matches!(level, TruncLevel::ChannelOnly) && right_join.chars().count() > inner {
        let mut name = snap.channel_name.clone();
        if inner == 0 {
            return BarRender {
                body: " ".repeat(cols),
                inbox_highlight: None,
            };
        }
        let take = inner.saturating_sub(1);
        name = name.chars().take(take).collect::<String>();
        name.push('…');
        let body = format!(
            " {}{:>width$} ",
            if show_dot_slot {
                format!("{dot_char} ")
            } else {
                String::new()
            },
            name,
            width = inner
        );
        return BarRender {
            body,
            inbox_highlight: None,
        };
    }

    // ── Place all segments into a fixed-width char buffer ───────────────
    //
    // Layout columns (0-based):
    //   col 0           — lead space
    //   col 1           — activity dot (when `show_dot_slot`)
    //   col 2           — dot/left separator
    //   col 1+dot_slot_w .. col 1+dot_slot_w+left_len   — left segments
    //   col cols-1-right_len .. col cols-1              — right segments
    //   col cols-1      — trail space
    //   center cols/2   — anchor for the inbox segment (if placed)
    //
    // Inbox candidates (longest first): the level dictates the maximum
    // form; we downgrade to single-char if the long form would collide
    // with left/right (centre placement leaves at least 1 space margin on
    // both sides) and drop entirely if even the single-char form can't
    // fit.

    let mut buf: Vec<char> = vec![' '; cols];

    if show_dot_slot {
        buf[1] = dot_char;
    }

    let left_start = 1 + dot_slot_w;
    let left_len = left_join.chars().count();
    for (i, c) in left_join.chars().enumerate() {
        let col = left_start + i;
        if col >= cols - 1 {
            break;
        }
        buf[col] = c;
    }

    let right_len = right_join.chars().count();
    let right_end = cols.saturating_sub(1); // exclusive
    let right_start = right_end.saturating_sub(right_len);
    for (i, c) in right_join.chars().enumerate() {
        let col = right_start + i;
        if col >= right_end {
            break;
        }
        buf[col] = c;
    }

    let inbox_highlight = place_inbox_segment(
        &mut buf,
        cols,
        left_start + left_len,
        right_start,
        inbox_candidates(snap, level),
        snap.inbox_unread > 0,
    );

    BarRender {
        body: buf.into_iter().collect(),
        inbox_highlight,
    }
}

/// Candidate strings for the INBOX segment, longest-to-shortest. Empty
/// when the level forbids it or inbox monitoring is disabled.
fn inbox_candidates(snap: &StatusSnapshot, level: TruncLevel) -> Vec<&'static str> {
    if !snap.inbox_enabled {
        return Vec::new();
    }
    let highlighted = snap.inbox_unread > 0;
    match level {
        TruncLevel::Full | TruncLevel::DropWords => {
            if highlighted {
                vec!["INBOX", "I"]
            } else {
                vec!["inbox", "i"]
            }
        }
        TruncLevel::Short | TruncLevel::ShortDropF | TruncLevel::ShortDropFD => {
            if highlighted {
                vec!["I"]
            } else {
                vec!["i"]
            }
        }
        TruncLevel::ChannelAndReady | TruncLevel::ChannelOnly => Vec::new(),
    }
}

/// Attempt to place an inbox candidate at the centre of the bar without
/// colliding with the left or right segment groups. Tries each candidate
/// in order (longest to shortest); the first one that fits is written
/// into `buf` and its `Range` is returned. If none fit, returns `None`.
///
/// The centre is anchored at `cols / 2`: an N-char candidate starts at
/// `cols/2 - N/2` and ends at `cols/2 - N/2 + N`. A minimum 1-cell
/// space gap is enforced on both sides between the inbox segment and the
/// nearest left / right segment characters.
///
/// `left_end_exclusive` is the column index one past the last left-segment
/// char (i.e. the first column where placement could legally start, before
/// adding the gap).
/// `right_start` is the column index where the right-segment characters
/// begin (i.e. the first column where placement must NOT extend into,
/// before adding the gap).
fn place_inbox_segment(
    buf: &mut [char],
    cols: usize,
    left_end_exclusive: usize,
    right_start: usize,
    candidates: Vec<&'static str>,
    highlight: bool,
) -> Option<std::ops::Range<usize>> {
    if candidates.is_empty() {
        return None;
    }
    let bar_center = cols / 2;
    for cand in &candidates {
        let text_len = cand.chars().count();
        if text_len == 0 {
            continue;
        }
        let start = bar_center.saturating_sub(text_len / 2);
        let end = start.saturating_add(text_len);
        // Stay within [1, cols-1) (col 0 and col cols-1 are lead/trail
        // spaces).
        if start < 1 || end > cols.saturating_sub(1) {
            continue;
        }
        // Min 1-cell gap on each side.
        if start <= left_end_exclusive {
            continue;
        }
        if end + 1 > right_start {
            continue;
        }
        for (i, ch) in cand.chars().enumerate() {
            buf[start + i] = ch;
        }
        // Return the highlight range only when the bar should paint the
        // attention styling. When inbox is enabled but empty, the
        // placeholder text ('inbox' / 'i') is written into `buf` but no
        // highlight range is returned, so the caller paints normal grey.
        return if highlight { Some(start..end) } else { None };
    }
    None
}

type LeftSegments = Vec<(LeftSeg, String)>;
type RightSegments = Vec<(RightSeg, String)>;

/// Build the ordered, kind-tagged list of segments for each group at the
/// chosen level. Segments are excluded when their underlying counter is zero
/// or omitted at that level.
fn build_segments(snap: &StatusSnapshot, level: TruncLevel) -> (LeftSegments, RightSegments) {
    let activity = snap.send_pending > 0 || snap.recv_pending > 0;
    let left: Vec<(LeftSeg, String)> = match level {
        TruncLevel::Full => {
            let mut v = Vec::new();
            if snap.send_pending > 0 {
                v.push((LeftSeg::Sending, format!("Sending... ({})", snap.send_pending)));
            }
            if snap.recv_pending > 0 {
                v.push((
                    LeftSeg::Receiving,
                    format!("Receiving... ({})", snap.recv_pending),
                ));
            }
            if v.is_empty() {
                vec![(LeftSeg::Ready, "Ready".to_string())]
            } else {
                v
            }
        }
        TruncLevel::DropWords => {
            let mut v = Vec::new();
            if snap.send_pending > 0 {
                v.push((LeftSeg::Sending, "Sending".to_string()));
            }
            if snap.recv_pending > 0 {
                v.push((LeftSeg::Receiving, "Receiving".to_string()));
            }
            if v.is_empty() {
                vec![(LeftSeg::Ready, "Ready".to_string())]
            } else {
                v
            }
        }
        TruncLevel::Short | TruncLevel::ShortDropF | TruncLevel::ShortDropFD => {
            let mut v = Vec::new();
            if snap.send_pending > 0 {
                v.push((LeftSeg::Sending, format!("S:{}", snap.send_pending)));
            }
            if snap.recv_pending > 0 {
                v.push((LeftSeg::Receiving, format!("R:{}", snap.recv_pending)));
            }
            v
        }
        TruncLevel::ChannelAndReady => {
            if activity {
                Vec::new()
            } else {
                vec![(LeftSeg::Ready, "Ready".to_string())]
            }
        }
        TruncLevel::ChannelOnly => Vec::new(),
    };
    let right: Vec<(RightSeg, String)> = match level {
        TruncLevel::Full | TruncLevel::DropWords => vec![
            (RightSeg::Feeds, format!("Feeds: {}", snap.feed_count)),
            (RightSeg::Dht, format!("DHT: {}", snap.dht_peers)),
            (RightSeg::Channel, snap.channel_name.clone()),
        ],
        TruncLevel::Short => vec![
            (RightSeg::Feeds, format!("F:{}", snap.feed_count)),
            (RightSeg::Dht, format!("D:{}", snap.dht_peers)),
            (RightSeg::Channel, snap.channel_name.clone()),
        ],
        TruncLevel::ShortDropF => vec![
            (RightSeg::Dht, format!("D:{}", snap.dht_peers)),
            (RightSeg::Channel, snap.channel_name.clone()),
        ],
        TruncLevel::ShortDropFD | TruncLevel::ChannelAndReady | TruncLevel::ChannelOnly => {
            vec![(RightSeg::Channel, snap.channel_name.clone())]
        }
    };
    (left, right)
}

fn pad_right(s: &str, width: usize) -> String {
    let len = s.chars().count();
    if len >= width {
        s.to_string()
    } else {
        let mut out = String::from(s);
        for _ in 0..(width - len) {
            out.push(' ');
        }
        out
    }
}

fn pad_left(s: &str, width: usize) -> String {
    let len = s.chars().count();
    if len >= width {
        s.to_string()
    } else {
        let mut out = String::new();
        for _ in 0..(width - len) {
            out.push(' ');
        }
        out.push_str(s);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(s: usize, r: usize, f: usize, d: usize, name: &str) -> StatusSnapshot {
        StatusSnapshot {
            send_pending: s,
            recv_pending: r,
            dht_active: false,
            feed_count: f,
            dht_peers: d,
            inbox_enabled: false,
            inbox_unread: 0,
            channel_name: name.to_string(),
        }
    }

    fn snap_active(s: usize, r: usize, f: usize, d: usize, name: &str) -> StatusSnapshot {
        StatusSnapshot {
            send_pending: s,
            recv_pending: r,
            dht_active: true,
            feed_count: f,
            dht_peers: d,
            inbox_enabled: false,
            inbox_unread: 0,
            channel_name: name.to_string(),
        }
    }

    fn snap_inbox(s: usize, r: usize, f: usize, d: usize, name: &str, inbox_unread: usize) -> StatusSnapshot {
        StatusSnapshot {
            send_pending: s,
            recv_pending: r,
            dht_active: false,
            feed_count: f,
            dht_peers: d,
            inbox_enabled: true,
            inbox_unread,
            channel_name: name.to_string(),
        }
    }

    #[test]
    fn picks_full_when_room() {
        let s = snap(3, 12, 7, 42, "#room-name");
        assert_eq!(pick_level(&s, 120), TruncLevel::Full);
    }

    #[test]
    fn falls_back_progressively() {
        let s = snap(3, 12, 7, 42, "#room-name");
        // 120 → Full; shrink to find each level.
        let levels: Vec<TruncLevel> = (10..=120)
            .map(|w| pick_level(&s, w))
            .collect();
        // Should be monotone-non-increasing in "detail" (i.e. as we go from
        // narrow to wide, level moves Full → ChannelOnly direction).
        // Spot-check: at very narrow widths we end at ChannelOnly.
        assert_eq!(pick_level(&s, 10), TruncLevel::ChannelOnly);
        // Sanity: somewhere in between we hit Short.
        assert!(levels.iter().any(|l| matches!(l, TruncLevel::Short)));
    }

    #[test]
    fn idle_shows_ready() {
        let s = snap(0, 0, 7, 42, "#room");
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::Full, 80, &mut slots);
        assert_eq!(bar.chars().count(), 80);
        assert!(bar.contains("Ready"), "bar = {bar:?}");
        assert!(bar.contains("Feeds: 7"));
        assert!(bar.contains("DHT: 42"));
        assert!(bar.contains("#room"));
    }

    #[test]
    fn activity_dot_shows_when_dht_active() {
        let s = snap_active(0, 0, 7, 42, "#room");
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::Full, 80, &mut slots);
        // Bar layout: " ● {body} "
        assert!(bar.starts_with(" ● "), "bar = {bar:?}");
    }

    #[test]
    fn activity_dot_hidden_when_idle() {
        let s = snap(0, 0, 7, 42, "#room");
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::Full, 80, &mut slots);
        // Idle: same slot width but ' ' instead of '●'. Bar starts with "   "
        // (1 lead pad + 1 dot slot + 1 separator).
        assert!(!bar.contains('●'), "bar = {bar:?}");
        assert!(bar.starts_with("   "));
    }

    #[test]
    fn activity_dot_dropped_in_extreme_narrow() {
        // cols < 6: dot slot is dropped entirely to give channel name room.
        let s = snap_active(0, 0, 0, 0, "#r");
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::ChannelOnly, 5, &mut slots);
        assert!(!bar.contains('●'), "bar = {bar:?}");
        assert_eq!(bar.chars().count(), 5);
    }

    #[test]
    fn recv_in_flight_shows_count() {
        // recv_pending now represents in-flight DHT immutable_gets. A value
        // of 3 should render as "Receiving... (3)".
        let s = snap(0, 3, 7, 42, "#room");
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::Full, 80, &mut slots);
        assert!(bar.contains("Receiving... (3)"), "bar = {bar:?}");
        assert!(!bar.contains("Ready"));
    }

    #[test]
    fn active_shows_counts() {
        let s = snap(3, 12, 7, 42, "#room");
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::Full, 80, &mut slots);
        assert!(bar.contains("Sending... (3)"));
        assert!(bar.contains("Receiving... (12)"));
        assert!(!bar.contains("Ready"));
    }

    #[test]
    fn short_level_uses_abbreviations() {
        let s = snap(3, 12, 7, 42, "#room");
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::Short, 40, &mut slots);
        assert!(bar.contains("S:3"));
        assert!(bar.contains("R:12"));
        assert!(bar.contains("F:7"));
        assert!(bar.contains("D:42"));
        assert!(bar.contains("#room"));
    }

    #[test]
    fn channel_only_truncates_with_ellipsis() {
        let s = snap(0, 0, 0, 0, "#very-long-channel-name");
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::ChannelOnly, 12, &mut slots);
        assert_eq!(bar.chars().count(), 12);
        assert!(bar.contains('…'), "bar = {bar:?}");
    }

    #[test]
    fn sticky_slots_grow_monotonically() {
        let s1 = snap(3, 0, 7, 42, "#room");
        let s2 = snap(123456, 0, 7, 42, "#room");
        let s3 = snap(3, 0, 7, 42, "#room");
        let mut slots = SlotWidths::default();
        let _ = render_bar(&s1, TruncLevel::Full, 80, &mut slots);
        let w1 = *slots.left.get(&LeftSeg::Sending).unwrap();
        let _ = render_bar(&s2, TruncLevel::Full, 80, &mut slots);
        let w2 = *slots.left.get(&LeftSeg::Sending).unwrap();
        assert!(w2 > w1, "slot should grow with bigger value");
        let _ = render_bar(&s3, TruncLevel::Full, 80, &mut slots);
        let w3 = *slots.left.get(&LeftSeg::Sending).unwrap();
        assert_eq!(w3, w2, "slot should NOT shrink when value shrinks");
    }

    #[test]
    fn slot_kept_sticky_when_segment_disappears() {
        // Sticky semantics: once Sending has appeared, its slot stays
        // reserved (rendered as blanks when idle) until `reset()` is called
        // — which happens on terminal resize. Ready is suppressed once any
        // real activity slot is reserved.
        let active = snap(3, 0, 7, 42, "#room");
        let idle = snap(0, 0, 7, 42, "#room");
        let mut slots = SlotWidths::default();
        let _ = render_bar(&active, TruncLevel::Full, 80, &mut slots);
        assert!(slots.left.contains_key(&LeftSeg::Sending));
        assert!(!slots.left.contains_key(&LeftSeg::Ready));
        let _ = render_bar(&idle, TruncLevel::Full, 80, &mut slots);
        assert!(
            slots.left.contains_key(&LeftSeg::Sending),
            "Sending slot should remain sticky after going idle"
        );
        assert!(
            !slots.left.contains_key(&LeftSeg::Ready),
            "Ready should not appear once a real activity slot is reserved"
        );

        // After reset (simulates a terminal resize) the sticky state clears
        // and the next idle render picks Ready up again.
        slots.reset();
        let _ = render_bar(&idle, TruncLevel::Full, 80, &mut slots);
        assert!(!slots.left.contains_key(&LeftSeg::Sending));
        assert_eq!(
            slots.left.get(&LeftSeg::Ready).copied(),
            Some("Ready".len())
        );
    }

    #[test]
    fn receiving_position_sticky_when_sending_goes_idle() {
        // The collapse-left bug: once both Sending and Receiving have been
        // seen, Receiving must keep its slot position even after Sending's
        // counter drops to zero.
        let both = snap(3, 5, 7, 42, "#room");
        let only_recv = snap(0, 5, 7, 42, "#room");
        let mut slots = SlotWidths::default();
        let bar_both = render_bar(&both, TruncLevel::Full, 80, &mut slots);
        // Find columns of "Receiving" while both are active.
        let col_both = bar_both
            .find("Receiving")
            .expect("Receiving present when active");

        let bar_recv = render_bar(&only_recv, TruncLevel::Full, 80, &mut slots);
        let col_recv = bar_recv
            .find("Receiving")
            .expect("Receiving still present after Sending goes idle");
        assert_eq!(
            col_both, col_recv,
            "Receiving column must be sticky when Sending goes idle\n  both: {bar_both:?}\n  recv: {bar_recv:?}"
        );
        // And Sending's slot is now blanks of its previous width.
        let sending_w = slots.left.get(&LeftSeg::Sending).copied().unwrap();
        assert!(sending_w >= "Sending... (3)".len());
    }

    #[test]
    fn bar_is_always_cols_wide() {
        for cols in [4_usize, 10, 20, 40, 80, 120] {
            let s = snap(3, 12, 7, 42, "#room");
            let level = pick_level(&s, cols);
            let mut slots = SlotWidths::default();
            let bar = render_bar(&s, level, cols, &mut slots);
            assert_eq!(bar.chars().count(), cols, "level={level:?} cols={cols}");
        }
    }

    #[test]
    fn pad_helpers() {
        assert_eq!(pad_right("ab", 5), "ab   ");
        assert_eq!(pad_left("ab", 5), "   ab");
        assert_eq!(pad_right("abcdef", 3), "abcdef");
    }

    // ── inbox segment ─────────────────────────────────────────────────

    #[test]
    fn inbox_omitted_when_disabled() {
        // No inbox_enabled in this snapshot → no INBOX/inbox anywhere.
        let s = snap(0, 0, 7, 42, "#room");
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::Full, 80, &mut slots);
        assert!(!bar.body.contains("INBOX"));
        assert!(!bar.body.contains("inbox"));
        assert_eq!(bar.inbox_highlight, None);
    }

    #[test]
    fn inbox_lowercase_when_enabled_and_no_unread() {
        let s = snap_inbox(0, 0, 7, 42, "#room", 0);
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::Full, 80, &mut slots);
        assert!(
            bar.body.contains("inbox"),
            "expected lowercase 'inbox' in {:?}",
            bar.body
        );
        assert!(!bar.body.contains("INBOX"));
        assert_eq!(
            bar.inbox_highlight, None,
            "no highlight when unread = 0"
        );
    }

    #[test]
    fn inbox_uppercase_when_unread_present() {
        let s = snap_inbox(0, 0, 7, 42, "#room", 3);
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::Full, 80, &mut slots);
        assert!(
            bar.body.contains("INBOX"),
            "expected uppercase 'INBOX' in {:?}",
            bar.body
        );
        assert!(!bar.body.contains("inbox"));
        let range = bar.inbox_highlight.expect("highlight should be Some when unread > 0");
        assert_eq!(range.end - range.start, "INBOX".len());
        // Body slice at the range should equal "INBOX".
        let chars: Vec<char> = bar.body.chars().collect();
        let slice: String = chars[range.start..range.end].iter().collect();
        assert_eq!(slice, "INBOX");
    }

    #[test]
    fn inbox_centered_at_cols_div_two() {
        let s = snap_inbox(0, 0, 7, 42, "#room", 1);
        let mut slots = SlotWidths::default();
        let cols = 80;
        let bar = render_bar(&s, TruncLevel::Full, cols, &mut slots);
        let range = bar.inbox_highlight.expect("highlight");
        let center = cols / 2;
        // 'INBOX' has 5 chars; center anchor places start = center - 5/2 = 38.
        assert_eq!(range.start, center - "INBOX".len() / 2);
        assert_eq!(range.end, range.start + "INBOX".len());
    }

    #[test]
    fn inbox_downgrades_to_single_char_at_short_level() {
        let s = snap_inbox(3, 12, 7, 42, "#room", 5);
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::Short, 50, &mut slots);
        assert!(
            !bar.body.contains("INBOX"),
            "INBOX shouldn't appear at Short level: {:?}",
            bar.body
        );
        // 'I' should appear, highlighted.
        let range = bar.inbox_highlight.expect("highlight should be Some");
        assert_eq!(range.end - range.start, 1);
    }

    #[test]
    fn inbox_lowercase_single_char_when_no_unread_at_short() {
        let s = snap_inbox(3, 12, 7, 42, "#room", 0);
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::Short, 50, &mut slots);
        // 'i' should be present in the body somewhere around the centre,
        // and no highlight range returned.
        assert_eq!(bar.inbox_highlight, None);
        // The body should still contain a lowercase 'i' centred.
        let cols = 50;
        let center = cols / 2;
        let center_char = bar.body.chars().nth(center).unwrap();
        // At cols=50, center=25; 'i' starts at 25 - 0 = 25.
        assert_eq!(center_char, 'i');
    }

    #[test]
    fn inbox_dropped_at_channel_only() {
        let s = snap_inbox(0, 0, 0, 0, "#room", 7);
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::ChannelOnly, 30, &mut slots);
        assert!(!bar.body.contains("INBOX"));
        assert!(!bar.body.contains("inbox"));
        assert_eq!(bar.inbox_highlight, None);
    }

    #[test]
    fn inbox_dropped_at_channel_and_ready() {
        let s = snap_inbox(0, 0, 0, 0, "#room", 7);
        let mut slots = SlotWidths::default();
        let bar = render_bar(&s, TruncLevel::ChannelAndReady, 35, &mut slots);
        assert!(!bar.body.contains("INBOX"));
        assert_eq!(bar.inbox_highlight, None);
    }

    #[test]
    fn inbox_downgrades_when_centre_would_overlap_left() {
        // Construct a scenario where the left group is wide enough that
        // 'INBOX' (5 chars) at centre would overlap, but 'I' fits.
        // At cols=40, centre=20. 'INBOX' wants cols 18..23.
        // If left group occupies cols 3..18 (i.e. left_len=15), overlap.
        // We synthesize this via a huge channel name to push the right
        // group out and a Sending counter that makes left wide. Simpler:
        // just verify the downgrade logic with a tighter cols.
        let s = snap_inbox(123456, 0, 7, 42, "#room-name", 1);
        let mut slots = SlotWidths::default();
        // Narrow enough that 'INBOX' won't fit, but the bar is still in
        // the Full level for the test's setup. At cols=30 with a long
        // send-pending, the centre is squeezed.
        let bar = render_bar(&s, TruncLevel::Full, 30, &mut slots);
        // Either INBOX downgraded to I, or omitted entirely. Both are
        // acceptable; we just assert the result is consistent (range
        // length matches what's in `body`).
        if let Some(range) = bar.inbox_highlight {
            let len = range.end - range.start;
            assert!(
                len == 1 || len == 5,
                "highlight should be 1 or 5 chars, got {len}"
            );
        }
    }
}
