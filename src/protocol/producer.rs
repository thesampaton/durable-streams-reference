use super::error::{Error, Result};
use super::headers::names;
use axum::http::HeaderMap;

/// Parsed producer headers from a POST request.
///
/// All three headers (Producer-Id, Producer-Epoch, Producer-Seq) must be
/// present together or absent together. This struct represents the "all present"
/// case after validation.
#[derive(Debug, Clone)]
pub struct ProducerHeaders {
    /// Stable producer identifier (non-empty string)
    pub id: String,
    /// Session epoch (monotonically non-decreasing, reset on restart)
    pub epoch: u64,
    /// Per-epoch sequence number (monotonically increasing, starts at 0)
    pub seq: u64,
}

/// Parse producer headers from an HTTP request.
///
/// Returns `Ok(None)` if no producer headers are present.
/// Returns `Ok(Some(headers))` if all three are present and valid.
///
/// # Errors
///
/// Returns `Err(InvalidProducerState)` if headers are partial, empty, or malformed.
pub fn parse_producer_headers(headers: &HeaderMap) -> Result<Option<ProducerHeaders>> {
    let id = headers
        .get(names::PRODUCER_ID)
        .and_then(|v| v.to_str().ok());
    let epoch = headers
        .get(names::PRODUCER_EPOCH)
        .and_then(|v| v.to_str().ok());
    let seq = headers
        .get(names::PRODUCER_SEQ)
        .and_then(|v| v.to_str().ok());

    match (id, epoch, seq) {
        // No producer headers — non-producer append
        (None, None, None) => Ok(None),

        // All present — validate values
        (Some(id), Some(epoch_str), Some(seq_str)) => {
            if id.is_empty() {
                return Err(Error::InvalidProducerState(
                    "Producer-Id must not be empty".to_string(),
                ));
            }

            let epoch = parse_non_negative_int(epoch_str, "Producer-Epoch")?;
            let seq = parse_non_negative_int(seq_str, "Producer-Seq")?;

            Ok(Some(ProducerHeaders {
                id: id.to_string(),
                epoch,
                seq,
            }))
        }

        // Partial headers — error
        _ => Err(Error::InvalidProducerState(
            "Producer-Id, Producer-Epoch, and Producer-Seq must all be provided together"
                .to_string(),
        )),
    }
}

/// Maximum safe integer value (2^53 - 1), matching JavaScript's
/// `Number.MAX_SAFE_INTEGER` for cross-platform compatibility.
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Parse a non-negative integer from a header value string.
///
/// Rejects leading zeros (except "0" itself), negative numbers,
/// decimals, scientific notation, and values exceeding 2^53-1.
fn parse_non_negative_int(value: &str, header_name: &str) -> Result<u64> {
    // Reject leading zeros (except "0" itself)
    if value.len() > 1 && value.starts_with('0') {
        return Err(Error::InvalidProducerState(format!(
            "{header_name} must be a non-negative integer, got: {value}"
        )));
    }

    let n = value.parse::<u64>().map_err(|_| {
        Error::InvalidProducerState(format!(
            "{header_name} must be a non-negative integer, got: {value}"
        ))
    })?;

    if n > MAX_SAFE_INTEGER {
        return Err(Error::InvalidProducerState(format!(
            "{header_name} must not exceed 2^53-1, got: {value}"
        )));
    }

    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    fn headers_with(id: &str, epoch: &str, seq: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(names::PRODUCER_ID, id.parse().unwrap());
        h.insert(names::PRODUCER_EPOCH, epoch.parse().unwrap());
        h.insert(names::PRODUCER_SEQ, seq.parse().unwrap());
        h
    }

    #[test]
    fn test_no_headers_returns_none() {
        let h = HeaderMap::new();
        assert!(parse_producer_headers(&h).unwrap().is_none());
    }

    #[test]
    fn test_all_valid_headers() {
        let h = headers_with("prod-1", "0", "0");
        let result = parse_producer_headers(&h).unwrap().unwrap();
        assert_eq!(result.id, "prod-1");
        assert_eq!(result.epoch, 0);
        assert_eq!(result.seq, 0);
    }

    #[test]
    fn test_large_values() {
        let h = headers_with("prod-1", "9007199254740991", "9007199254740991");
        let result = parse_producer_headers(&h).unwrap().unwrap();
        assert_eq!(result.epoch, 9_007_199_254_740_991);
        assert_eq!(result.seq, 9_007_199_254_740_991);
    }

    #[test]
    fn test_partial_headers_id_only() {
        let mut h = HeaderMap::new();
        h.insert(names::PRODUCER_ID, "prod-1".parse().unwrap());
        assert!(matches!(
            parse_producer_headers(&h),
            Err(Error::InvalidProducerState(_))
        ));
    }

    #[test]
    fn test_partial_headers_missing_seq() {
        let mut h = HeaderMap::new();
        h.insert(names::PRODUCER_ID, "prod-1".parse().unwrap());
        h.insert(names::PRODUCER_EPOCH, "0".parse().unwrap());
        assert!(matches!(
            parse_producer_headers(&h),
            Err(Error::InvalidProducerState(_))
        ));
    }

    #[test]
    fn test_empty_producer_id() {
        let h = headers_with("", "0", "0");
        assert!(matches!(
            parse_producer_headers(&h),
            Err(Error::InvalidProducerState(_))
        ));
    }

    #[test]
    fn test_non_integer_epoch() {
        let h = headers_with("prod-1", "abc", "0");
        assert!(matches!(
            parse_producer_headers(&h),
            Err(Error::InvalidProducerState(_))
        ));
    }

    #[test]
    fn test_non_integer_seq() {
        let h = headers_with("prod-1", "0", "3.5");
        assert!(matches!(
            parse_producer_headers(&h),
            Err(Error::InvalidProducerState(_))
        ));
    }

    #[test]
    fn test_negative_epoch() {
        let h = headers_with("prod-1", "-1", "0");
        assert!(matches!(
            parse_producer_headers(&h),
            Err(Error::InvalidProducerState(_))
        ));
    }

    #[test]
    fn test_leading_zeros_rejected() {
        let h = headers_with("prod-1", "01", "0");
        assert!(matches!(
            parse_producer_headers(&h),
            Err(Error::InvalidProducerState(_))
        ));
    }

    #[test]
    fn test_exceeds_max_safe_integer_rejected() {
        // 2^53 = 9007199254740992, one above the limit
        let h = headers_with("prod-1", "9007199254740992", "0");
        assert!(matches!(
            parse_producer_headers(&h),
            Err(Error::InvalidProducerState(_))
        ));

        let h = headers_with("prod-1", "0", "9007199254740992");
        assert!(matches!(
            parse_producer_headers(&h),
            Err(Error::InvalidProducerState(_))
        ));
    }

    #[test]
    fn test_scientific_notation_rejected() {
        let h = headers_with("prod-1", "1e5", "0");
        assert!(matches!(
            parse_producer_headers(&h),
            Err(Error::InvalidProducerState(_))
        ));
    }
}
