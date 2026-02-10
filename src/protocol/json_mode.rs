use crate::protocol::error::{Error, Result};
use bytes::{BufMut, Bytes, BytesMut};
use serde_json::Value;

/// Process JSON data for append: validate and flatten arrays
///
/// If the input is a JSON array, returns each element as a separate message.
/// If the input is a single JSON value, returns it as one message.
/// Empty arrays return `Ok(vec![])` — callers decide whether that's an error.
///
/// # Errors
///
/// Returns `Error::InvalidJson` if input is not valid JSON.
///
/// # Panics
///
/// Panics if serializing validated JSON back to bytes fails, which should never happen.
pub fn process_append(data: &[u8]) -> Result<Vec<Bytes>> {
    // Parse JSON
    let value: Value =
        serde_json::from_slice(data).map_err(|e| Error::InvalidJson(e.to_string()))?;

    if let Value::Array(arr) = value {
        // Empty arrays return empty vec — caller decides policy
        if arr.is_empty() {
            return Ok(vec![]);
        }

        // Flatten array: each element becomes a separate message
        let mut messages = Vec::with_capacity(arr.len());
        for element in arr {
            let json_bytes =
                serde_json::to_vec(&element).expect("serializing validated JSON should not fail");
            messages.push(Bytes::from(json_bytes));
        }
        Ok(messages)
    } else {
        // Single value: return as-is
        let json_bytes =
            serde_json::to_vec(&value).expect("serializing validated JSON should not fail");
        Ok(vec![Bytes::from(json_bytes)])
    }
}

/// Wrap messages in JSON array for read.
///
/// Takes a collection of JSON message bytes and wraps them in a JSON array.
/// If the input is empty, returns an empty array `[]`.
///
/// # Errors
///
/// This function currently does not return errors and always returns `Ok`.
pub fn wrap_read(messages: &[Bytes]) -> Result<Bytes> {
    wrap_read_iter(messages.iter())
}

/// Wrap JSON messages (iterator form) in a JSON array for read responses.
///
/// Accepts any iterator of `&Bytes` to avoid intermediate allocations
/// in handler hot paths.
///
/// # Errors
///
/// This function currently does not return errors and always returns `Ok`.
pub fn wrap_read_iter<'a, I>(messages: I) -> Result<Bytes>
where
    I: IntoIterator<Item = &'a Bytes>,
{
    // Stored JSON messages are validated on write; build the array directly
    // to avoid per-message parse + re-serialize in read hot paths.
    let iter = messages.into_iter();
    let (lo, _) = iter.size_hint();
    // Rough estimate: '[' + ']' + per-message avg ~64 bytes + commas
    let mut out = BytesMut::with_capacity(2 + lo * 65);
    out.put_u8(b'[');
    let mut wrote_any = false;
    for msg in iter {
        if wrote_any {
            out.put_u8(b',');
        }
        out.extend_from_slice(msg);
        wrote_any = true;
    }
    out.put_u8(b']');
    Ok(out.freeze())
}

/// Check if a content type is JSON
///
/// Returns true if the normalized content type is "application/json".
#[must_use]
pub fn is_json_content_type(content_type: &str) -> bool {
    content_type == "application/json"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_append_single_value() {
        let data = br#"{"event":"click","x":100}"#;
        let result = process_append(data).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Bytes::from(r#"{"event":"click","x":100}"#));
    }

    #[test]
    fn test_process_append_array_flattening() {
        let data = br#"[{"a":1},{"b":2}]"#;
        let result = process_append(data).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Bytes::from(r#"{"a":1}"#));
        assert_eq!(result[1], Bytes::from(r#"{"b":2}"#));
    }

    #[test]
    fn test_process_append_empty_array_returns_empty_vec() {
        let data = b"[]";
        let result = process_append(data).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_process_append_invalid_json() {
        let data = b"{invalid}";
        let result = process_append(data);

        assert!(matches!(result, Err(Error::InvalidJson(_))));
    }

    #[test]
    fn test_process_append_nested_arrays_preserved() {
        let data = br#"[{"tags":["a","b"]},{"items":[1,2,3]}]"#;
        let result = process_append(data).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Bytes::from(r#"{"tags":["a","b"]}"#));
        assert_eq!(result[1], Bytes::from(r#"{"items":[1,2,3]}"#));
    }

    #[test]
    fn test_wrap_read_empty() {
        let messages: Vec<Bytes> = vec![];
        let result = wrap_read(&messages).unwrap();

        assert_eq!(result, Bytes::from("[]"));
    }

    #[test]
    fn test_wrap_read_single_message() {
        let messages = vec![Bytes::from(r#"{"a":1}"#)];
        let result = wrap_read(&messages).unwrap();

        assert_eq!(result, Bytes::from(r#"[{"a":1}]"#));
    }

    #[test]
    fn test_wrap_read_multiple_messages() {
        let messages = vec![
            Bytes::from(r#"{"a":1}"#),
            Bytes::from(r#"{"b":2}"#),
            Bytes::from(r#"{"c":3}"#),
        ];
        let result = wrap_read(&messages).unwrap();

        assert_eq!(result, Bytes::from(r#"[{"a":1},{"b":2},{"c":3}]"#));
    }

    #[test]
    fn test_is_json_content_type() {
        assert!(is_json_content_type("application/json"));
        assert!(!is_json_content_type("text/plain"));
        assert!(!is_json_content_type("application/xml"));
    }
}
