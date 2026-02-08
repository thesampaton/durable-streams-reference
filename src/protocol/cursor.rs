// Cursor generation for long-poll/SSE responses.
//
// Cursors are opaque strings that clients echo back in subsequent
// requests via the `cursor` query parameter. They enable CDN request
// collapsing by identifying the stream position.
//
// The value packs (read_seq, byte_offset) into a single integer that
// fits within JavaScript's MAX_SAFE_INTEGER (2^53 - 1) so conformance
// tests can compare cursors via parseInt().
//
// Each emitted cursor is strictly greater than the previous one,
// even when the stream position hasn't changed. This satisfies the
// "cursor collision with jitter" conformance tests while keeping the
// offset-derived value as a floor so cursors advance with position.

use std::sync::atomic::{AtomicU64, Ordering};

/// Bits reserved for the byte-offset component.
///
/// 27 bits gives a range of 128 MB — well above the per-stream limit.
const BYTE_OFFSET_BITS: u32 = 27;

/// Global monotonic cursor counter. Each `generate()` call returns a
/// value strictly greater than any previously returned cursor.
static LAST_CURSOR: AtomicU64 = AtomicU64::new(0);

/// Generate a monotonically increasing cursor from stream position.
///
/// The offset-derived value acts as a floor — when the stream advances
/// the cursor jumps to at least the new position. Between advances the
/// cursor still increments so consecutive responses at the same offset
/// are distinguishable (required for jitter detection).
///
/// The value is a decimal integer that fits within JavaScript's
/// `Number.MAX_SAFE_INTEGER`.
#[must_use]
pub fn generate(next_offset: &crate::protocol::offset::Offset) -> String {
    if let Some((read_seq, byte_offset)) = next_offset.parse_components() {
        let offset_value = (read_seq << BYTE_OFFSET_BITS) | byte_offset;
        loop {
            let last = LAST_CURSOR.load(Ordering::Relaxed);
            let next = offset_value.max(last + 1);
            if LAST_CURSOR
                .compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return next.to_string();
            }
        }
    }

    // `next_offset` should never be a sentinel, but keep a deterministic fallback.
    "0".to_string()
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
    fn test_cursor_increases_for_same_offset() {
        // Consecutive calls at the same offset must still produce
        // strictly increasing cursors (jitter detection).
        let offset = Offset::new(7, 42);
        let c1: u64 = generate(&offset).parse().unwrap();
        let c2: u64 = generate(&offset).parse().unwrap();
        assert!(c2 > c1);
    }

    #[test]
    fn test_cursor_changes_with_offset() {
        let a = Offset::new(1, 2);
        let b = Offset::new(1, 3);
        assert_ne!(generate(&a), generate(&b));
    }

    #[test]
    fn test_cursor_is_monotonic() {
        // Offsets increase → cursors increase (numerically)
        let a = Offset::new(1, 5);
        let b = Offset::new(1, 10);
        let c = Offset::new(2, 0);
        let ca: u64 = generate(&a).parse().unwrap();
        let cb: u64 = generate(&b).parse().unwrap();
        let cc: u64 = generate(&c).parse().unwrap();
        assert!(cb > ca);
        assert!(cc > cb);
    }

    #[test]
    fn test_cursor_fits_in_js_max_safe_integer() {
        // Typical values: read_seq up to ~67M, byte_offset up to 10MB
        let offset = Offset::new(67_000_000, 10_485_760);
        let cursor: u64 = generate(&offset).parse().unwrap();
        assert!(cursor <= (1_u64 << 53) - 1);
    }
}
