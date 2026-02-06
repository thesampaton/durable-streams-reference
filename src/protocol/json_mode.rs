use crate::protocol::error::{Error, Result};
use bytes::Bytes;
use serde_json::Value;

/// Process JSON data for append: validate and flatten arrays
///
/// If the input is a JSON array, returns each element as a separate message.
/// If the input is a single JSON value, returns it as one message.
/// Empty arrays are rejected with an error.
///
/// # Errors
///
/// Returns `Error::InvalidJson` if:
/// - Input is not valid JSON
/// - Input is an empty array
///
/// # Panics
///
/// Panics if serializing validated JSON back to bytes fails, which should never happen.
pub fn process_append(data: &[u8]) -> Result<Vec<Bytes>> {
    // Parse JSON
    let value: Value =
        serde_json::from_slice(data).map_err(|e| Error::InvalidJson(e.to_string()))?;

    if let Value::Array(arr) = value {
        // Reject empty arrays
        if arr.is_empty() {
            return Err(Error::InvalidJson(
                "empty arrays are not permitted".to_string(),
            ));
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

/// Wrap messages in JSON array for read
///
/// Takes a collection of JSON message bytes and wraps them in a JSON array.
/// If the input is empty, returns an empty array `[]`.
///
/// # Errors
///
/// Returns `Error::InvalidJson` if any message is not valid JSON.
///
/// # Panics
///
/// Panics if serializing the final array fails, which should never happen.
pub fn wrap_read(messages: &[Bytes]) -> Result<Bytes> {
    if messages.is_empty() {
        // Empty stream returns empty array
        return Ok(Bytes::from_static(b"[]"));
    }

    // Parse each message as JSON and collect into array
    let mut values = Vec::with_capacity(messages.len());
    for msg in messages {
        let value: Value = serde_json::from_slice(msg)
            .map_err(|e| Error::InvalidJson(format!("stored message is not valid JSON: {e}")))?;
        values.push(value);
    }

    // Wrap in array and serialize
    let array = Value::Array(values);
    let json_bytes =
        serde_json::to_vec(&array).expect("serializing validated JSON array should not fail");

    Ok(Bytes::from(json_bytes))
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
    fn test_process_append_empty_array_rejected() {
        let data = b"[]";
        let result = process_append(data);

        assert!(matches!(result, Err(Error::InvalidJson(_))));
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
