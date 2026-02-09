use crate::protocol::error::{Error, Result};
use std::fmt;
use std::str::FromStr;

/// Offset newtype with validated format
///
/// Format: `{read_seq:016x}_{byte_offset:016x}` (zero-padded lowercase hex)
/// Sentinels: "-1" (stream start), "now" (tail/live)
///
/// Offsets are lexicographically ordered and must be monotonically increasing
/// within a stream. The format guarantees lexicographic ordering equals
/// temporal ordering.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(String);

impl Offset {
    /// Sentinel value for stream start
    pub const START: &'static str = "-1";

    /// Sentinel value for stream tail/live mode
    pub const NOW: &'static str = "now";

    /// Create a new offset from read sequence and byte offset
    ///
    /// Generates the canonical offset format: `{read_seq:016x}_{byte_offset:016x}`
    #[must_use]
    pub fn new(read_seq: u64, byte_offset: u64) -> Self {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut raw = [0u8; 33];

        for (i, slot) in raw[..16].iter_mut().enumerate() {
            let shift = (15 - i) * 4;
            *slot = HEX[((read_seq >> shift) & 0xF) as usize];
        }
        raw[16] = b'_';
        for (i, slot) in raw[17..].iter_mut().enumerate() {
            let shift = (15 - i) * 4;
            *slot = HEX[((byte_offset >> shift) & 0xF) as usize];
        }

        let mut s = String::with_capacity(33);
        for &b in &raw {
            s.push(char::from(b));
        }
        Self(s)
    }

    /// Create the stream start sentinel
    #[must_use]
    pub fn start() -> Self {
        Self(Self::START.to_string())
    }

    /// Create the tail/now sentinel
    #[must_use]
    pub fn now() -> Self {
        Self(Self::NOW.to_string())
    }

    /// Check if this is the start sentinel
    #[must_use]
    pub fn is_start(&self) -> bool {
        self.0 == Self::START
    }

    /// Check if this is the now/tail sentinel
    #[must_use]
    pub fn is_now(&self) -> bool {
        self.0 == Self::NOW
    }

    /// Check if this is a sentinel value (start or now)
    #[must_use]
    pub fn is_sentinel(&self) -> bool {
        self.is_start() || self.is_now()
    }

    /// Get the raw offset string
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parse the offset into (`read_seq`, `byte_offset`) components
    ///
    /// Returns `None` for sentinel values.
    #[must_use]
    pub fn parse_components(&self) -> Option<(u64, u64)> {
        if self.is_sentinel() {
            return None;
        }

        let (read_seq_raw, byte_offset_raw) = self.0.split_once('_')?;

        let read_seq = u64::from_str_radix(read_seq_raw, 16).ok()?;
        let byte_offset = u64::from_str_radix(byte_offset_raw, 16).ok()?;

        Some((read_seq, byte_offset))
    }
}

impl FromStr for Offset {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        // Allow sentinels
        if s == Self::START || s == Self::NOW {
            return Ok(Self(s.to_string()));
        }

        // Validate format: {hex}_{hex}
        let Some((read_seq_raw, byte_offset_raw)) = s.split_once('_') else {
            return Err(Error::InvalidOffset(format!(
                "Expected format 'read_seq_byte_offset', got '{s}'"
            )));
        };

        // Validate both parts are exactly 16 hex digits
        for (i, part) in [read_seq_raw, byte_offset_raw].into_iter().enumerate() {
            if part.len() != 16 {
                return Err(Error::InvalidOffset(format!(
                    "Expected 16 hex digits for part {}, got {} digits in '{s}'",
                    i + 1,
                    part.len()
                )));
            }

            // Validate hex digits (lowercase only for canonical form)
            if !part.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(Error::InvalidOffset(format!(
                    "Invalid hex character in part {} of '{s}'",
                    i + 1
                )));
            }

            // Ensure lowercase (canonical form)
            if part.chars().any(|c| c.is_ascii_uppercase()) {
                return Err(Error::InvalidOffset(format!(
                    "Offset must use lowercase hex digits: '{s}'"
                )));
            }
        }

        // Validate parseable
        if u64::from_str_radix(read_seq_raw, 16).is_err()
            || u64::from_str_radix(byte_offset_raw, 16).is_err()
        {
            return Err(Error::InvalidOffset(format!(
                "Failed to parse hex values in '{s}'"
            )));
        }

        Ok(Self(s.to_string()))
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Offset> for String {
    fn from(offset: Offset) -> Self {
        offset.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_new() {
        let offset = Offset::new(0, 0);
        assert_eq!(offset.as_str(), "0000000000000000_0000000000000000");

        let offset = Offset::new(1, 42);
        assert_eq!(offset.as_str(), "0000000000000001_000000000000002a");

        let offset = Offset::new(u64::MAX, u64::MAX);
        assert_eq!(offset.as_str(), "ffffffffffffffff_ffffffffffffffff");
    }

    #[test]
    fn test_offset_sentinels() {
        let start = Offset::start();
        assert!(start.is_start());
        assert!(start.is_sentinel());
        assert!(!start.is_now());
        assert_eq!(start.as_str(), "-1");

        let now = Offset::now();
        assert!(now.is_now());
        assert!(now.is_sentinel());
        assert!(!now.is_start());
        assert_eq!(now.as_str(), "now");
    }

    #[test]
    fn test_offset_parse_valid() {
        let offset: Offset = "0000000000000000_0000000000000000".parse().unwrap();
        assert_eq!(offset.as_str(), "0000000000000000_0000000000000000");

        let offset: Offset = "0000000000000001_000000000000002a".parse().unwrap();
        assert_eq!(offset.as_str(), "0000000000000001_000000000000002a");

        let offset: Offset = "-1".parse().unwrap();
        assert!(offset.is_start());

        let offset: Offset = "now".parse().unwrap();
        assert!(offset.is_now());
    }

    #[test]
    fn test_offset_parse_invalid() {
        // Wrong separator
        assert!(
            "0000000000000000-0000000000000000"
                .parse::<Offset>()
                .is_err()
        );

        // Too short
        assert!("000_000".parse::<Offset>().is_err());

        // Too long
        assert!(
            "00000000000000000_0000000000000000"
                .parse::<Offset>()
                .is_err()
        );

        // Uppercase (not canonical)
        assert!(
            "000000000000000A_0000000000000000"
                .parse::<Offset>()
                .is_err()
        );

        // Invalid hex
        assert!(
            "000000000000000g_0000000000000000"
                .parse::<Offset>()
                .is_err()
        );

        // Missing underscore
        assert!(
            "00000000000000000000000000000000"
                .parse::<Offset>()
                .is_err()
        );

        // Extra parts
        assert!(
            "0000000000000000_0000000000000000_0000000000000000"
                .parse::<Offset>()
                .is_err()
        );
    }

    #[test]
    fn test_offset_ordering() {
        let offset1 = Offset::new(0, 0);
        let offset2 = Offset::new(0, 1);
        let offset3 = Offset::new(1, 0);
        let offset4 = Offset::new(1, 1);

        assert!(offset1 < offset2);
        assert!(offset2 < offset3);
        assert!(offset3 < offset4);
        assert!(offset1 < offset4);

        // Lexicographic ordering equals temporal ordering
        assert_eq!(offset1.as_str() < offset2.as_str(), offset1 < offset2);
    }

    #[test]
    fn test_offset_parse_components() {
        let offset = Offset::new(42, 100);
        let (read_seq, byte_offset) = offset.parse_components().unwrap();
        assert_eq!(read_seq, 42);
        assert_eq!(byte_offset, 100);

        // Sentinels return None
        assert!(Offset::start().parse_components().is_none());
        assert!(Offset::now().parse_components().is_none());
    }

    #[test]
    fn test_offset_display() {
        let offset = Offset::new(1, 2);
        assert_eq!(format!("{offset}"), "0000000000000001_0000000000000002");

        let start = Offset::start();
        assert_eq!(format!("{start}"), "-1");

        let now = Offset::now();
        assert_eq!(format!("{now}"), "now");
    }
}
