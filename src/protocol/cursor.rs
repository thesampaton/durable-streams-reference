// Cursor generation for long-poll/SSE responses.
//
// Cursors are opaque strings that clients echo back in subsequent
// requests via the `cursor` query parameter. They enable CDN request
// collapsing by identifying the stream position.

use crate::protocol::offset::Offset;

/// Generate a cursor value from the stream's current next offset.
///
/// The cursor is the string representation of `next_offset`.
/// Clients MUST treat it as opaque and MUST NOT parse it.
#[must_use]
pub fn generate(next_offset: &Offset) -> String {
    next_offset.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_from_offset() {
        let offset = Offset::new(3, 26);
        let cursor = generate(&offset);
        assert_eq!(cursor, "0000000000000003_000000000000001a");
    }

    #[test]
    fn test_cursor_from_start() {
        let offset = Offset::new(0, 0);
        let cursor = generate(&offset);
        assert_eq!(cursor, "0000000000000000_0000000000000000");
    }
}
