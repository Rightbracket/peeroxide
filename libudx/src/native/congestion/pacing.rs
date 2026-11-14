//! Token bucket that spreads a stream's packets over time.
//!
//! Port of the pacing in C libudx's `udx.c`: `update_pacing_time`,
//! `stream_may_send`, and the token accounting in `send_stream_packets`.

/// Tokens a new stream starts with, in bytes. C: `UDX_INIT_PACING_RATE`.
const INITIAL_TOKENS: u64 = 25_000;

/// How long the stream waits for new tokens once the bucket is empty.
pub(crate) const REFILL_INTERVAL_MS: u64 = 1;

/// The bytes a stream may send before the next refill.
///
/// Each millisecond that passes is worth the pacing rate in tokens. A
/// packet may be sent while any token is left, and takes its size out of
/// the bucket, so a stream sends about one millisecond's share of the rate
/// each millisecond, however rarely it is woken.
#[derive(Debug, Clone)]
pub(crate) struct TokenBucket {
    available: u64,
    /// The millisecond the bucket was last refilled in.
    last_refill_ms: u64,
}

impl TokenBucket {
    /// Creates a bucket that holds the starting tokens.
    pub(crate) fn new(now_ms: u64) -> Self {
        Self {
            available: INITIAL_TOKENS,
            last_refill_ms: now_ms,
        }
    }

    /// Grants the tokens for the milliseconds that have passed since the last
    /// refill. Tokens left from before are dropped, as they are in C, so the
    /// bucket never holds more than the time since the last refill is worth.
    pub(crate) fn refill(&mut self, now_ms: u64, rate_bytes_per_ms: u64) {
        if now_ms > self.last_refill_ms {
            let elapsed_ms = now_ms - self.last_refill_ms;
            self.available = elapsed_ms.saturating_mul(rate_bytes_per_ms);
            self.last_refill_ms = now_ms;
        }
    }

    /// Whether a packet may be sent now.
    pub(crate) fn has_tokens(&self) -> bool {
        self.available > 0
    }

    /// Takes a sent packet's bytes out of the bucket. Returns whether that
    /// emptied it, in which case nothing more may be sent before a refill.
    pub(crate) fn consume(&mut self, bytes: u64) -> bool {
        self.available = self.available.saturating_sub(bytes);
        self.available == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_bucket_holds_the_starting_tokens() {
        let mut bucket = TokenBucket::new(0);
        assert!(bucket.has_tokens());
        assert!(!bucket.consume(INITIAL_TOKENS - 1));
        assert!(bucket.consume(1));
        assert!(!bucket.has_tokens());
    }

    #[test]
    fn a_packet_larger_than_the_tokens_left_empties_the_bucket() {
        let mut bucket = TokenBucket::new(0);
        bucket.refill(1, 500);
        assert!(bucket.has_tokens());
        assert!(bucket.consume(1_200));
        assert!(!bucket.has_tokens());
    }

    #[test]
    fn each_millisecond_grants_the_rate() {
        let mut bucket = TokenBucket::new(0);
        bucket.consume(INITIAL_TOKENS);

        bucket.refill(1, 3_000);
        assert!(!bucket.consume(2_999));
        assert!(bucket.consume(1));

        // Three milliseconds without a refill are worth three times the rate.
        bucket.refill(4, 3_000);
        assert!(!bucket.consume(8_999));
        assert!(bucket.consume(1));
    }

    #[test]
    fn a_refill_within_the_same_millisecond_grants_nothing() {
        let mut bucket = TokenBucket::new(5);
        bucket.consume(INITIAL_TOKENS);
        bucket.refill(5, 3_000);
        assert!(!bucket.has_tokens());
    }

    #[test]
    fn a_refill_replaces_the_tokens_left_over() {
        let mut bucket = TokenBucket::new(0);
        // The starting tokens are not added to the refill.
        bucket.refill(1, 1_000);
        assert!(bucket.consume(1_000));
    }
}
