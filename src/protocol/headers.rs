use crate::protocol::error::{Error, Result};
use chrono::{DateTime, Utc};

/// Protocol header names
pub mod names {
    pub const STREAM_TTL: &str = "Stream-TTL";
    pub const STREAM_EXPIRES_AT: &str = "Stream-Expires-At";
    pub const STREAM_CLOSED: &str = "Stream-Closed";
    pub const STREAM_NEXT_OFFSET: &str = "Stream-Next-Offset";
    pub const STREAM_UP_TO_DATE: &str = "Stream-Up-To-Date";
    pub const STREAM_CURSOR: &str = "Stream-Cursor";
    pub const STREAM_SEQ: &str = "Stream-Seq";
    pub const PRODUCER_ID: &str = "Producer-Id";
    pub const PRODUCER_EPOCH: &str = "Producer-Epoch";
    pub const PRODUCER_SEQ: &str = "Producer-Seq";
    pub const PRODUCER_EXPECTED_SEQ: &str = "Producer-Expected-Seq";
    pub const PRODUCER_RECEIVED_SEQ: &str = "Producer-Received-Seq";
}

/// Parse TTL header value
///
/// Validates that the TTL is a valid unsigned integer with no leading zeros,
/// no decimal points, and no scientific notation.
///
/// # Errors
///
/// Returns `Error::InvalidTtl` if the value has leading zeros, decimals,
/// scientific notation, is negative, or is not a valid integer.
pub fn parse_ttl(value: &str) -> Result<u64> {
    let trimmed = value.trim();

    // Reject empty
    if trimmed.is_empty() {
        return Err(Error::InvalidTtl("empty value".to_string()));
    }

    // Reject leading zeros (except "0" itself)
    if trimmed.len() > 1 && trimmed.starts_with('0') {
        return Err(Error::InvalidTtl(format!(
            "leading zeros not allowed: '{trimmed}'"
        )));
    }

    // Reject decimals
    if trimmed.contains('.') {
        return Err(Error::InvalidTtl(format!(
            "decimal values not allowed: '{trimmed}'"
        )));
    }

    // Reject scientific notation
    if trimmed.contains('e') || trimmed.contains('E') {
        return Err(Error::InvalidTtl(format!(
            "scientific notation not allowed: '{trimmed}'"
        )));
    }

    // Reject negative
    if trimmed.starts_with('-') {
        return Err(Error::InvalidTtl(format!(
            "negative values not allowed: '{trimmed}'"
        )));
    }

    // Parse as u64
    trimmed
        .parse::<u64>()
        .map_err(|e| Error::InvalidTtl(format!("invalid integer '{trimmed}': {e}")))
}

/// Parse Expires-At header value (ISO 8601 timestamp)
///
/// # Errors
///
/// Returns `Error::InvalidHeader` if the value is not a valid ISO 8601 timestamp.
pub fn parse_expires_at(value: &str) -> Result<DateTime<Utc>> {
    value
        .parse::<DateTime<Utc>>()
        .map_err(|e| Error::InvalidHeader {
            header: names::STREAM_EXPIRES_AT.to_string(),
            reason: format!("invalid ISO 8601 timestamp: {e}"),
        })
}

/// Parse boolean header value
///
/// Accepts "true" (case-insensitive) as true, anything else as false.
#[must_use]
pub fn parse_bool(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("true")
}

/// Normalize content type for storage and comparison
///
/// Lowercases the content type and strips charset parameter.
/// Example: "text/plain; charset=utf-8" → "text/plain"
#[must_use]
pub fn normalize_content_type(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ttl_valid() {
        assert_eq!(parse_ttl("0").unwrap(), 0);
        assert_eq!(parse_ttl("1").unwrap(), 1);
        assert_eq!(parse_ttl("3600").unwrap(), 3600);
        assert_eq!(parse_ttl("86400").unwrap(), 86400);
        assert_eq!(parse_ttl("  100  ").unwrap(), 100); // whitespace ok
    }

    #[test]
    fn test_parse_ttl_invalid() {
        // Leading zeros
        assert!(parse_ttl("01").is_err());
        assert!(parse_ttl("00").is_err());
        assert!(parse_ttl("0123").is_err());

        // Decimals
        assert!(parse_ttl("1.5").is_err());
        assert!(parse_ttl("3600.0").is_err());

        // Scientific notation
        assert!(parse_ttl("1e3").is_err());
        assert!(parse_ttl("1E3").is_err());

        // Negative
        assert!(parse_ttl("-1").is_err());

        // Empty
        assert!(parse_ttl("").is_err());
        assert!(parse_ttl("  ").is_err());

        // Non-numeric
        assert!(parse_ttl("abc").is_err());
    }

    #[test]
    fn test_parse_bool() {
        assert!(parse_bool("true"));
        assert!(parse_bool("TRUE"));
        assert!(parse_bool("True"));
        assert!(parse_bool("  true  "));

        assert!(!parse_bool("false"));
        assert!(!parse_bool("1"));
        assert!(!parse_bool(""));
        assert!(!parse_bool("yes"));
    }

    #[test]
    fn test_normalize_content_type() {
        assert_eq!(normalize_content_type("text/plain"), "text/plain");
        assert_eq!(normalize_content_type("TEXT/PLAIN"), "text/plain");
        assert_eq!(
            normalize_content_type("text/plain; charset=utf-8"),
            "text/plain"
        );
        assert_eq!(
            normalize_content_type("application/json;charset=utf-8"),
            "application/json"
        );
        assert_eq!(normalize_content_type("  TEXT/PLAIN  "), "text/plain");
    }
}
