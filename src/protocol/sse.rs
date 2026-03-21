// SSE (Server-Sent Events) protocol types and helpers.
//
// Defines the control event payload structure and event builders
// for the SSE read mode (PROTOCOL.md §5.8).
//
// We format SSE frames manually (without axum's Event::data) to
// produce `data:<value>` without the optional space after the colon,
// which the conformance test suite's raw-text assertions require.

use base64::Engine;
use bytes::Bytes;
use serde::Serialize;

/// JSON payload for `event: control` SSE events.
///
/// Field names use camelCase per PROTOCOL.md §5.8.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlPayload {
    /// Current tail position (always present).
    pub stream_next_offset: String,
    /// Cursor for CDN collapsing (present when stream is open).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_cursor: Option<String>,
    /// True when client has caught up with all available data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub up_to_date: Option<bool>,
    /// True when stream is closed and all data has been sent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_closed: Option<bool>,
}

/// Format an `event: control` SSE frame from a typed payload.
///
/// Returns a raw SSE frame string ready to be written to the wire.
///
/// # Panics
///
/// Panics if `ControlPayload` fails to serialize, which should never happen
/// since all fields are simple strings/booleans.
#[must_use]
pub fn format_control_frame(payload: &ControlPayload) -> String {
    let json =
        serde_json::to_string(payload).expect("ControlPayload serialization should not fail");
    format!("event: control\ndata:{json}\n\n")
}

/// Format an `event: data` SSE frame for a single stored message.
///
/// If the stream content type is binary (per `is_binary_content_type`),
/// the data is base64-encoded. For JSON content types, the message is
/// wrapped in a JSON array. Otherwise it's sent as UTF-8 text.
///
/// For JSON streams with multiple messages from a single read, prefer
/// `format_data_frames` which batches them into a single SSE event.
///
/// Multi-line data is split across multiple `data:` lines per the SSE spec.
/// All line ending forms (`\r\n`, `\r`, `\n`) are treated as boundaries to
/// prevent CRLF injection attacks — each segment gets its own `data:` prefix.
#[must_use]
pub fn format_data_frame(data: &Bytes, is_binary: bool, is_json: bool) -> String {
    let text = if is_binary {
        base64::engine::general_purpose::STANDARD.encode(data)
    } else if is_json {
        // Wrap JSON data in an array per conformance spec
        let raw = String::from_utf8_lossy(data);
        format!("[{raw}]")
    } else {
        // Safety: for text/* content types the data is expected to be
        // valid UTF-8. If it's not, we use lossy conversion which
        // replaces invalid sequences with the replacement character.
        String::from_utf8_lossy(data).into_owned()
    };

    format_raw_data_frame(&text)
}

/// Format `event: data` SSE frames for a batch of messages from a single read.
///
/// For JSON streams, all messages are batched into a single `event: data` frame
/// containing one JSON array — maintaining the 1:1 data-to-control relationship
/// the spec requires (PROTOCOL.md §5.8). Without batching, the client would
/// concatenate separate `[...][...]` arrays, producing invalid JSON.
///
/// For binary/text streams, each message gets its own `event: data` frame.
///
/// Returns an empty string if `messages` is empty.
#[must_use]
pub fn format_data_frames(messages: &[Bytes], is_binary: bool, is_json: bool) -> String {
    if messages.is_empty() {
        return String::new();
    }

    if is_json {
        // Batch all messages into a single JSON array in one event: data frame
        let mut array_content = String::new();
        for (i, msg) in messages.iter().enumerate() {
            if i > 0 {
                array_content.push(',');
            }
            let raw = String::from_utf8_lossy(msg);
            array_content.push_str(&raw);
        }
        let text = format!("[{array_content}]");
        format_raw_data_frame(&text)
    } else {
        let mut result = String::new();
        for msg in messages {
            result.push_str(&format_data_frame(msg, is_binary, false));
        }
        result
    }
}

/// Format a raw text payload as an `event: data` SSE frame.
///
/// Splits on all line-ending forms (`\r\n`, `\r`, `\n`) to prevent CRLF
/// injection — each segment gets its own `data:` prefix.
fn format_raw_data_frame(text: &str) -> String {
    let mut frame = String::from("event: data\n");
    for line in split_lines(text) {
        frame.push_str("data:");
        frame.push_str(line);
        frame.push('\n');
    }
    frame.push('\n');
    frame
}

/// Split text on all line-ending forms: `\r\n`, `\r`, and `\n`.
///
/// This is stricter than Rust's `str::lines()` which doesn't handle
/// bare `\r` as a line terminator.
fn split_lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\r' {
            lines.push(&text[start..i]);
            // Consume \r\n as a single line ending
            if i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                i += 2;
            } else {
                i += 1;
            }
            start = i;
        } else if bytes[i] == b'\n' {
            lines.push(&text[start..i]);
            i += 1;
            start = i;
        } else {
            i += 1;
        }
    }
    // Remaining text after last line ending
    lines.push(&text[start..]);
    lines
}

/// Format an SSE keep-alive comment frame.
#[must_use]
pub fn format_keepalive_frame() -> &'static str {
    ":\n\n"
}

/// Determine if a content type requires base64 encoding in SSE mode.
///
/// Returns `false` for `text/*` and `application/json` (UTF-8 safe).
/// Returns `true` for everything else (binary).
///
/// Per PROTOCOL.md §5.8: "For streams with content-type: text/* or
/// application/json, data events carry UTF-8 text directly. For streams
/// with any other content-type (binary streams), servers MUST automatically
/// base64-encode data events."
#[must_use]
pub fn is_binary_content_type(ct: &str) -> bool {
    if ct.starts_with("text/") {
        return false;
    }
    if ct == "application/json" {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_binary_text_types() {
        assert!(!is_binary_content_type("text/plain"));
        assert!(!is_binary_content_type("text/html"));
        assert!(!is_binary_content_type("text/csv"));
    }

    #[test]
    fn test_is_binary_json() {
        assert!(!is_binary_content_type("application/json"));
    }

    #[test]
    fn test_is_binary_octet_stream() {
        assert!(is_binary_content_type("application/octet-stream"));
    }

    #[test]
    fn test_is_binary_protobuf() {
        assert!(is_binary_content_type("application/x-protobuf"));
    }

    #[test]
    fn test_is_binary_ndjson() {
        // application/ndjson is not text/* or application/json, so it's binary
        assert!(is_binary_content_type("application/ndjson"));
    }

    #[test]
    fn test_control_payload_serializes_camel_case() {
        let payload = ControlPayload {
            stream_next_offset: "abc_123".to_string(),
            stream_cursor: Some("cursor1".to_string()),
            up_to_date: Some(true),
            stream_closed: None,
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"streamNextOffset\""));
        assert!(json.contains("\"streamCursor\""));
        assert!(json.contains("\"upToDate\""));
        assert!(!json.contains("\"streamClosed\""));
    }

    #[test]
    fn test_control_payload_skips_none_fields() {
        let payload = ControlPayload {
            stream_next_offset: "offset1".to_string(),
            stream_cursor: None,
            up_to_date: None,
            stream_closed: Some(true),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"streamNextOffset\""));
        assert!(!json.contains("\"streamCursor\""));
        assert!(!json.contains("\"upToDate\""));
        assert!(json.contains("\"streamClosed\":true"));
    }

    #[test]
    fn test_format_data_frame_text() {
        let data = Bytes::from("hello world");
        let frame = format_data_frame(&data, false, false);
        assert_eq!(frame, "event: data\ndata:hello world\n\n");
    }

    #[test]
    fn test_format_data_frame_multiline_lf() {
        let data = Bytes::from("line1\nline2\nline3");
        let frame = format_data_frame(&data, false, false);
        assert!(frame.contains("data:line1\n"));
        assert!(frame.contains("data:line2\n"));
        assert!(frame.contains("data:line3\n"));
    }

    #[test]
    fn test_format_data_frame_multiline_crlf() {
        let data = Bytes::from("line1\r\nline2\r\nline3");
        let frame = format_data_frame(&data, false, false);
        assert!(frame.contains("data:line1\n"));
        assert!(frame.contains("data:line2\n"));
        assert!(frame.contains("data:line3\n"));
        // No \r should appear in the output
        assert!(!frame.contains('\r'));
    }

    #[test]
    fn test_format_data_frame_multiline_cr() {
        let data = Bytes::from("line1\rline2\rline3");
        let frame = format_data_frame(&data, false, false);
        assert!(frame.contains("data:line1\n"));
        assert!(frame.contains("data:line2\n"));
        assert!(frame.contains("data:line3\n"));
        assert!(!frame.contains('\r'));
    }

    #[test]
    fn test_format_data_frame_json() {
        let data = Bytes::from(r#"{"id":1}"#);
        let frame = format_data_frame(&data, false, true);
        assert!(frame.contains(r#"data:[{"id":1}]"#));
    }

    #[test]
    fn test_format_data_frame_binary() {
        let data = Bytes::from(vec![0x01, 0x02, 0x03]);
        let frame = format_data_frame(&data, true, false);
        assert!(frame.contains("data:AQID"));
    }

    #[test]
    fn test_format_control_frame() {
        let payload = ControlPayload {
            stream_next_offset: "test".to_string(),
            stream_cursor: None,
            up_to_date: Some(true),
            stream_closed: None,
        };
        let frame = format_control_frame(&payload);
        assert!(frame.starts_with("event: control\n"));
        assert!(frame.contains("data:"));
        assert!(frame.ends_with("\n\n"));
    }

    #[test]
    fn test_split_lines_lf() {
        assert_eq!(split_lines("a\nb\nc"), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_split_lines_crlf() {
        assert_eq!(split_lines("a\r\nb\r\nc"), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_split_lines_cr() {
        assert_eq!(split_lines("a\rb\rc"), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_split_lines_mixed() {
        assert_eq!(split_lines("a\nb\rc\r\nd"), vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_split_lines_empty_segments() {
        // Consecutive line endings produce empty segments
        assert_eq!(split_lines("a\n\nb"), vec!["a", "", "b"]);
        assert_eq!(split_lines("a\r\rb"), vec!["a", "", "b"]);
    }

    #[test]
    fn test_format_data_frames_json_batches_into_single_event() {
        let messages = vec![
            Bytes::from(r#"{"id":1}"#),
            Bytes::from(r#"{"id":2}"#),
            Bytes::from(r#"{"id":3}"#),
        ];
        let frame = format_data_frames(&messages, false, true);
        // Should produce exactly one `event: data` with all messages in one array
        assert_eq!(
            frame.matches("event: data").count(),
            1,
            "JSON batch should produce exactly one SSE data event"
        );
        assert!(
            frame.contains(r#"data:[{"id":1},{"id":2},{"id":3}]"#),
            "JSON batch should contain all messages in one array"
        );
    }

    #[test]
    fn test_format_data_frames_json_single_message() {
        let messages = vec![Bytes::from(r#"{"id":1}"#)];
        let frame = format_data_frames(&messages, false, true);
        assert_eq!(frame.matches("event: data").count(), 1);
        assert!(frame.contains(r#"data:[{"id":1}]"#));
    }

    #[test]
    fn test_format_data_frames_text_emits_per_message() {
        let messages = vec![Bytes::from("hello"), Bytes::from("world")];
        let frame = format_data_frames(&messages, false, false);
        // Non-JSON should produce one event: data per message
        assert_eq!(
            frame.matches("event: data").count(),
            2,
            "Text batch should produce one SSE data event per message"
        );
    }

    #[test]
    fn test_format_data_frames_empty() {
        let result = format_data_frames(&[], false, true);
        assert!(result.is_empty());
    }
}
