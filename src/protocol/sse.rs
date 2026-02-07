// SSE (Server-Sent Events) protocol types and helpers.
//
// Defines the control event payload structure and event builders
// for the SSE read mode (PROTOCOL.md §5.8).

use axum::response::sse::Event;
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

/// Build an `event: control` SSE event from a typed payload.
///
/// # Panics
///
/// Panics if `ControlPayload` fails to serialize, which should never happen
/// since all fields are simple strings/booleans.
pub fn build_control_event(payload: &ControlPayload) -> Event {
    let json =
        serde_json::to_string(payload).expect("ControlPayload serialization should not fail");
    Event::default().event("control").data(json)
}

/// Build an `event: data` SSE event for a single stored message.
///
/// If the stream content type is binary (per `is_binary_content_type`),
/// the data is base64-encoded. Otherwise it's sent as UTF-8 text.
pub fn build_data_event(data: &Bytes, is_binary: bool) -> Event {
    let text = if is_binary {
        base64::engine::general_purpose::STANDARD.encode(data)
    } else {
        // Safety: for text/* and application/json content types the data
        // is expected to be valid UTF-8. If it's not, we use lossy conversion
        // which replaces invalid sequences with the replacement character.
        String::from_utf8_lossy(data).into_owned()
    };
    Event::default().event("data").data(text)
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
    fn test_build_data_event_text() {
        let data = Bytes::from("hello world");
        let _event = build_data_event(&data, false);
        // Event is opaque; we verify it doesn't panic for text
    }

    #[test]
    fn test_build_data_event_binary() {
        let data = Bytes::from(vec![0x01, 0x02, 0x03]);
        let _event = build_data_event(&data, true);
        // Event is opaque; we verify it doesn't panic for binary
    }

    #[test]
    fn test_build_control_event_does_not_panic() {
        let payload = ControlPayload {
            stream_next_offset: "test".to_string(),
            stream_cursor: None,
            up_to_date: Some(true),
            stream_closed: None,
        };
        let _event = build_control_event(&payload);
    }
}
