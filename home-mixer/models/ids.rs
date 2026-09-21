//! Pipeline identity types (U4).
//!
//! Internal business identities are registry-allocated Snowflake `u64` values
//! (production guarantees `1..=i64::MAX`). External proto and Redis boundaries
//! still carry 24-char lowercase hex ObjectIds; adapters map between the two
//! through `crate::id::IdentityReader` or `crate::id::IdentityIngress`.
//! Timestamps, counters, thresholds,
//! and request-local IDs stay `u64`.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

/// 12-byte external identity. Field is private so callers cannot forge a
/// layout. Only external boundaries, the Redis-compatible key space, and the
/// padded test resolver handle `ObjectId`; the domain model uses `u64`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ObjectId([u8; 12]);

pub type PostId = u64;

/// A member ("皮"), and the only identity this pipeline knows. Accounts do not
/// exist here: one account owns several members, and recall, relations and
/// authorship are all member-to-member.
///
/// Internally this is the member's Snowflake ID allocated by the ID Registry.
/// This is mrpyq's `member_id` / `MemberRef.id`. Do not confuse it with mrpyq's
/// own `user_id`, which is not an identity but one half of `member_key`
/// (`{user_id}_{user_no}`) — an alternate encoding of the same member.
pub type UserId = u64;

/// Timestamp embedded in a registry-allocated Snowflake ID. `None` for the
/// nil ID; used by AgeFilter as the fallback when hydration did not supply
/// `created_at_ms`.
pub fn snowflake_timestamp_ms(id: u64) -> Option<u64> {
    (id != 0).then(|| (id >> 22) + id_service::SNOWFLAKE_EPOCH_MS)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdError {
    Empty,
    InvalidLength { actual: usize },
    InvalidHex,
    Uppercase,
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "object id is empty"),
            Self::InvalidLength { actual } => {
                write!(f, "object id must be 24 lowercase hex chars, got {actual}")
            }
            Self::InvalidHex => write!(f, "object id contains non-hex characters"),
            Self::Uppercase => write!(f, "object id must be lowercase hex"),
        }
    }
}

impl std::error::Error for IdError {}

impl ObjectId {
    pub const NIL: Self = Self([0; 12]);

    pub fn parse(s: &str) -> Result<Self, IdError> {
        if s.is_empty() {
            return Err(IdError::Empty);
        }
        if s.len() != 24 {
            return Err(IdError::InvalidLength { actual: s.len() });
        }
        if s.bytes().any(|b| b.is_ascii_uppercase()) {
            return Err(IdError::Uppercase);
        }
        let mut bytes = [0u8; 12];
        for (i, chunk) in s.as_bytes().chunks_exact(2).enumerate() {
            let hex = std::str::from_utf8(chunk).map_err(|_| IdError::InvalidHex)?;
            bytes[i] = u8::from_str_radix(hex, 16).map_err(|_| IdError::InvalidHex)?;
        }
        Ok(Self(bytes))
    }

    /// `""` → `None`. Anything else must be a valid 24-char lowercase hex id.
    pub fn parse_optional(s: &str) -> Result<Option<Self>, IdError> {
        if s.is_empty() {
            return Ok(None);
        }
        Self::parse(s).map(Some)
    }

    pub fn is_nil(self) -> bool {
        self == Self::NIL
    }

    pub fn as_bytes(&self) -> &[u8; 12] {
        &self.0
    }

    /// First four bytes as a big-endian Unix timestamp. Diagnostic / AgeFilter
    /// fallback only — real post age comes from hydrated `created_at_ms`.
    pub fn timestamp_secs(self) -> u32 {
        u32::from_be_bytes([self.0[0], self.0[1], self.0[2], self.0[3]])
    }

    /// Demo data generator. Layout: 4-byte timestamp || 8-byte sequence, both BE.
    /// Lexicographic order of the 12 bytes matches timestamp order.
    pub fn from_parts(ts_secs: u32, seq: u64) -> Self {
        let mut bytes = [0u8; 12];
        bytes[..4].copy_from_slice(&ts_secs.to_be_bytes());
        bytes[4..].copy_from_slice(&seq.to_be_bytes());
        Self(bytes)
    }

    /// Zero-pad a u64 into the last 8 bytes. Used by tests and the padded
    /// compatibility resolver (`crate::id::PaddedIdentityResolver`).
    /// Production business IDs must use [`parse`].
    pub fn from_u64_be_padded(n: u64) -> Self {
        let mut bytes = [0u8; 12];
        bytes[4..].copy_from_slice(&n.to_be_bytes());
        Self(bytes)
    }

    /// Inverse of [`from_u64_be_padded`]. `None` when the id is not a padded
    /// integer (real 96-bit ObjectIds cannot round-trip through i64/u64).
    pub fn to_u64_be_padded(self) -> Option<u64> {
        if self.0[..4] != [0; 4] {
            return None;
        }
        Some(u64::from_be_bytes(
            self.0[4..].try_into().expect("last 8 bytes"),
        ))
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({self})")
    }
}

impl FromStr for ObjectId {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for ObjectId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
impl From<u64> for ObjectId {
    fn from(n: u64) -> Self {
        Self::from_u64_be_padded(n)
    }
}

pub fn pid(n: u64) -> PostId {
    n
}

pub fn uid(n: u64) -> UserId {
    n
}

/// Parse a proto identity string at an external boundary.
///
/// Empty → `None`. Illegal strings → `None` and increment `invalid`.
/// `"0"` is not a sentinel and does not parse.
pub fn parse_wire_id(raw: &str, invalid: &mut usize) -> Option<ObjectId> {
    match ObjectId::parse_optional(raw) {
        Ok(value) => value.filter(|id| !id.is_nil()),
        Err(_) => {
            *invalid += 1;
            None
        }
    }
}

pub fn wire_id(id: ObjectId) -> String {
    if id.is_nil() {
        String::new()
    } else {
        id.to_string()
    }
}

pub fn wire_optional_id(id: Option<ObjectId>) -> String {
    id.filter(|id| !id.is_nil())
        .map(|id| id.to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_only_24_lowercase_hex() {
        let id = ObjectId::parse("e305c05a62cd1ef55823cd86").unwrap();
        assert_eq!(id.to_string(), "e305c05a62cd1ef55823cd86");
        assert!(ObjectId::parse("E305C05A62CD1EF55823CD86").is_err());
        assert!(ObjectId::parse("abc").is_err());
        assert!(ObjectId::parse("").is_err());
        assert_eq!(ObjectId::parse_optional("").unwrap(), None);
        assert!(ObjectId::parse_optional("0").is_err());
        assert!(ObjectId::parse("000000000000000000000000")
            .unwrap()
            .is_nil());
    }

    #[test]
    fn from_parts_encodes_timestamp_and_seq() {
        let id = ObjectId::from_parts(1_700_000_000, 7);
        assert_eq!(id.timestamp_secs(), 1_700_000_000);
        assert_eq!(id.to_string(), "6553f1000000000000000007");
        let later = ObjectId::from_parts(1_700_000_001, 0);
        assert!(id < later);
    }

    #[test]
    fn padded_u64_roundtrip_and_rejects_real_object_ids() {
        let id = ObjectId::from_u64_be_padded(42);
        assert_eq!(id.to_string(), "00000000000000000000002a");
        assert_eq!(id.to_u64_be_padded(), Some(42));
        assert_eq!(id.timestamp_secs(), 0);
        let real = ObjectId::parse("e305c05a62cd1ef55823cd86").unwrap();
        assert_eq!(real.to_u64_be_padded(), None);
    }

    #[test]
    fn serde_is_lowercase_hex_string() {
        let id = ObjectId::from_u64_be_padded(1);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"000000000000000000000001\"");
        let parsed: ObjectId = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn snowflake_timestamp_ms_decodes_and_rejects_nil() {
        let now_ms = 1_700_000_000_000_u64;
        let id = (now_ms - id_service::SNOWFLAKE_EPOCH_MS) << 22;
        assert_eq!(snowflake_timestamp_ms(id), Some(now_ms));
        assert_eq!(snowflake_timestamp_ms(0), None);
    }

    #[test]
    fn parse_wire_id_drops_nil_zero_and_illegal() {
        let mut invalid = 0;
        assert_eq!(parse_wire_id("", &mut invalid), None);
        assert_eq!(parse_wire_id("0", &mut invalid), None);
        assert_eq!(parse_wire_id("not-an-id", &mut invalid), None);
        assert_eq!(
            parse_wire_id("000000000000000000000001", &mut invalid),
            Some(ObjectId::from_u64_be_padded(1))
        );
        assert_eq!(invalid, 2);
    }
}
