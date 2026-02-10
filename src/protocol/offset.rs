use crate::protocol::error::{Error, Result};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

/// Offset value with validated format.
///
/// Canonical format: `{read_seq:016x}_{byte_offset:016x}`.
/// Sentinels: `-1` (stream start), `now` (tail/live).
#[derive(Debug, Clone)]
pub enum Offset {
    Start,
    Now,
    Concrete {
        read_seq: u64,
        byte_offset: u64,
        raw: [u8; 33],
    },
}

impl Offset {
    /// Sentinel value for stream start
    pub const START: &'static str = "-1";

    /// Sentinel value for stream tail/live mode
    pub const NOW: &'static str = "now";

    /// Create a new offset from read sequence and byte offset.
    #[must_use]
    pub fn new(read_seq: u64, byte_offset: u64) -> Self {
        Self::Concrete {
            read_seq,
            byte_offset,
            raw: encode_offset(read_seq, byte_offset),
        }
    }

    /// Create the stream start sentinel
    #[must_use]
    pub fn start() -> Self {
        Self::Start
    }

    /// Create the tail/now sentinel
    #[must_use]
    pub fn now() -> Self {
        Self::Now
    }

    /// Check if this is the start sentinel
    #[must_use]
    pub fn is_start(&self) -> bool {
        matches!(self, Self::Start)
    }

    /// Check if this is the now/tail sentinel
    #[must_use]
    pub fn is_now(&self) -> bool {
        matches!(self, Self::Now)
    }

    /// Check if this is a sentinel value (start or now)
    #[must_use]
    pub fn is_sentinel(&self) -> bool {
        matches!(self, Self::Start | Self::Now)
    }

    /// Get the canonical offset string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Start => Self::START,
            Self::Now => Self::NOW,
            Self::Concrete { raw, .. } => {
                // SAFETY: `raw` is always constructed from ASCII hex digits + `_`.
                unsafe { std::str::from_utf8_unchecked(raw) }
            }
        }
    }

    /// Parse the offset into (`read_seq`, `byte_offset`) components.
    ///
    /// Returns `None` for sentinel values.
    #[must_use]
    pub fn parse_components(&self) -> Option<(u64, u64)> {
        match self {
            Self::Concrete {
                read_seq,
                byte_offset,
                ..
            } => Some((*read_seq, *byte_offset)),
            Self::Start | Self::Now => None,
        }
    }
}

impl FromStr for Offset {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s == Self::START {
            return Ok(Self::Start);
        }
        if s == Self::NOW {
            return Ok(Self::Now);
        }

        let bytes = s.as_bytes();
        if bytes.len() != 33 || bytes[16] != b'_' {
            return Err(Error::InvalidOffset(format!(
                "Expected format 'read_seq_byte_offset', got '{s}'"
            )));
        }

        for (idx, b) in bytes.iter().copied().enumerate() {
            if idx == 16 {
                continue;
            }
            if b.is_ascii_uppercase() {
                return Err(Error::InvalidOffset(format!(
                    "Offset must use lowercase hex digits: '{s}'"
                )));
            }
            if !b.is_ascii_digit() && !(b'a'..=b'f').contains(&b) {
                let part_num = if idx < 16 { 1 } else { 2 };
                return Err(Error::InvalidOffset(format!(
                    "Invalid hex character in part {part_num} of '{s}'"
                )));
            }
        }

        let read_seq = decode_hex_16(&bytes[..16])
            .ok_or_else(|| Error::InvalidOffset(format!("Failed to parse hex values in '{s}'")))?;
        let byte_offset = decode_hex_16(&bytes[17..])
            .ok_or_else(|| Error::InvalidOffset(format!("Failed to parse hex values in '{s}'")))?;

        Ok(Self::Concrete {
            read_seq,
            byte_offset,
            raw: encode_offset(read_seq, byte_offset),
        })
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<Offset> for String {
    fn from(offset: Offset) -> Self {
        offset.as_str().to_string()
    }
}

impl PartialEq for Offset {
    fn eq(&self, other: &Self) -> bool {
        match (self.parse_components(), other.parse_components()) {
            (Some(a), Some(b)) => a == b,
            _ => self.as_str() == other.as_str(),
        }
    }
}

impl Eq for Offset {}

impl PartialOrd for Offset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Offset {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.parse_components(), other.parse_components()) {
            (Some((a_rs, a_bo)), Some((b_rs, b_bo))) => (a_rs, a_bo).cmp(&(b_rs, b_bo)),
            _ => self.as_str().cmp(other.as_str()),
        }
    }
}

impl Hash for Offset {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Some((read_seq, byte_offset)) = self.parse_components() {
            0u8.hash(state);
            read_seq.hash(state);
            byte_offset.hash(state);
        } else {
            1u8.hash(state);
            self.as_str().hash(state);
        }
    }
}

fn encode_offset(read_seq: u64, byte_offset: u64) -> [u8; 33] {
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

    raw
}

fn decode_hex_16(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 16 {
        return None;
    }

    let mut value = 0u64;
    for b in bytes {
        let digit = match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            _ => return None,
        };
        value = (value << 4) | u64::from(digit);
    }
    Some(value)
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
    fn test_offset_sentinel_ordering() {
        let start = Offset::start();
        let now = Offset::now();
        let zero = Offset::new(0, 0);
        let mid = Offset::new(5, 10);

        // "-1" < "0000..." (ASCII '-' < '0')
        assert!(start < zero);
        assert!(start < mid);

        // "now" > "0000..." (ASCII 'n' > '0')
        assert!(now > zero);
        assert!(now > mid);

        // Sentinels are not equal to each other
        assert_ne!(start, now);

        // Same sentinel is equal to itself
        assert_eq!(Offset::start(), Offset::start());
        assert_eq!(Offset::now(), Offset::now());
    }

    #[test]
    fn test_offset_equality_and_hash() {
        use std::collections::HashSet;

        let a = Offset::new(1, 2);
        let b = Offset::new(1, 2);
        let c = Offset::new(1, 3);

        assert_eq!(a, b);
        assert_ne!(a, c);

        // Equal offsets must produce the same hash (HashSet insertion)
        let mut set = HashSet::new();
        set.insert(a.as_str().to_string());
        assert!(set.contains(b.as_str()));

        // Sentinels hash consistently
        let mut set2 = HashSet::new();
        set2.insert(Offset::start().as_str().to_string());
        set2.insert(Offset::now().as_str().to_string());
        assert_eq!(set2.len(), 2);
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
