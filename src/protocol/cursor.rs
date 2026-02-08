// Cursor generation for long-poll/SSE responses.
//
// Cursors are opaque strings that clients echo back in subsequent
// requests via the `cursor` query parameter. They enable CDN request
// collapsing by identifying the stream position.
//
// Cursors are monotonically increasing decimal integers (digits only)
// using a snowflake-style encoding: the upper 42 bits hold milliseconds
// since a custom epoch (2024-01-01), the lower 10 bits hold a sequence
// counter. This gives ~139 years of range with 1024 unique values per
// millisecond, all fitting within JavaScript's MAX_SAFE_INTEGER (2^53-1).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Custom epoch: 2024-01-01T00:00:00Z in milliseconds since Unix epoch.
const CUSTOM_EPOCH_MS: u64 = 1_704_067_200_000;

/// Number of bits reserved for the within-millisecond sequence.
const SEQ_BITS: u32 = 10;

/// Tracks the last generated value to guarantee monotonicity even when
/// the system clock drifts backward or multiple calls occur within the
/// same millisecond.
static LAST_VALUE: AtomicU64 = AtomicU64::new(0);

/// Generate a monotonically increasing cursor value.
///
/// Returns a decimal string of digits that is guaranteed to be
/// strictly greater than any previously generated cursor within
/// this process lifetime. The value encodes wall-clock time so
/// cursors from different process restarts are also ordered.
///
/// The `_next_offset` parameter is accepted for API compatibility
/// but the cursor value is independent of it.
///
/// # Panics
///
/// Panics if the system clock is before the Unix epoch.
#[must_use]
pub fn generate(_next_offset: &crate::protocol::offset::Offset) -> String {
    #[allow(clippy::cast_possible_truncation)] // millis since epoch fits in u64 until year 584556
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as u64;

    let ts = now_ms.saturating_sub(CUSTOM_EPOCH_MS);
    let base = ts << SEQ_BITS;

    // CAS loop to ensure strict monotonicity
    loop {
        let last = LAST_VALUE.load(Ordering::Relaxed);
        let next = if base > last {
            base // new millisecond, start at seq 0
        } else {
            last + 1 // same or earlier ms, just increment
        };

        if LAST_VALUE
            .compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return next.to_string();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::offset::Offset;

    #[test]
    fn test_cursor_is_digits_only() {
        let offset = Offset::new(3, 26);
        let cursor = generate(&offset);
        assert!(cursor.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn test_cursor_is_monotonic() {
        let offset = Offset::new(0, 0);
        let c1: u64 = generate(&offset).parse().unwrap();
        let c2: u64 = generate(&offset).parse().unwrap();
        assert!(c2 > c1);
    }

    #[test]
    fn test_cursor_fits_in_js_max_safe_integer() {
        let offset = Offset::new(0, 0);
        let cursor: u64 = generate(&offset).parse().unwrap();
        // JavaScript Number.MAX_SAFE_INTEGER = 2^53 - 1
        assert!(cursor <= (1_u64 << 53) - 1);
    }

    #[test]
    fn test_cursor_encodes_time() {
        let offset = Offset::new(0, 0);
        let cursor: u64 = generate(&offset).parse().unwrap();
        // Extract timestamp portion: should be reasonable (> 0, i.e. after 2024)
        let ts = cursor >> SEQ_BITS;
        assert!(ts > 0, "timestamp portion should be positive");
    }
}
