//! Persistent ObjectId ↔ Snowflake identity mapping.
//!
//! The service deliberately keeps the mapping independent from Home Mixer and
//! xrex. External ObjectIds are resolved at an ingress boundary; internal
//! callers exchange the resulting Snowflake value. Redis is the default
//! durable source of truth and includes a bounded process-local cache; local
//! development can explicitly select the in-process `MemoryMappingStore`.

use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

pub mod grpc;
pub mod http;
pub mod logging;
pub mod metrics;
mod redis_registry;
pub use redis_registry::{
    InsertOutcome, MappingStore, MemoryMappingStore, MemoryStoreCalls, RedisIdRegistry,
    RedisIdRegistryConfig, SequenceFloor, SequenceReservation, MAX_ALLOCATION_ATTEMPTS,
    STORAGE_SCHEMA,
};

pub const SNOWFLAKE_EPOCH_MS: u64 = 1_288_834_974_657;
pub const MAPPING_VERSION: u32 = 2;
pub(crate) const WORKER_BITS: u8 = 10;
pub(crate) const SEQUENCE_BITS: u8 = 12;
pub(crate) const MAX_WORKER_ID: u64 = (1 << WORKER_BITS) - 1;
pub(crate) const MAX_SEQUENCE: u64 = (1 << SEQUENCE_BITS) - 1;
pub(crate) const POST_CACHE_TTL_SECS: u64 = 30 * 24 * 60 * 60;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum EntityKind {
    User,
    Post,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdError {
    InvalidObjectId(String),
    InvalidWorkerId(u64),
    /// Allocation is disabled and the listed ObjectIds have no mapping. The
    /// payload is a human-readable summary (count plus the first few ids).
    AllocationDisabled(String),
    /// A read-only resolve requested ObjectIds that have no existing mapping.
    /// The payload is a human-readable summary (count plus the first few ids).
    UnknownObjectIds(String),
    /// The request carried `trusted_snowflake_id` for this ObjectId but the
    /// service was started without `--allow-trusted-import`.
    TrustedImportDisabled(String),
    BeforeSnowflakeEpoch(u64),
    SecondExhausted(u64),
    UnsupportedSnowflake(u64),
    /// No mapping exists for this Snowflake (reverse lookup miss).
    UnknownSnowflake(u64),
    /// The object is already bound to a Snowflake different from
    /// `snowflake_id` (the trusted value, or the id this write attempted).
    MappingConflict {
        object_id: String,
        entity_kind: EntityKind,
        snowflake_id: SnowflakeId,
    },
    /// `snowflake_id` requested for `object_id` is already bound to another
    /// object; `holder` names that object when the store could read it.
    SnowflakeTaken {
        snowflake_id: SnowflakeId,
        object_id: String,
        entity_kind: EntityKind,
        holder: Option<(EntityKind, String)>,
    },
    EntityKindMismatch {
        snowflake_id: SnowflakeId,
        expected: EntityKind,
        actual: EntityKind,
    },
    Io(String),
    Redis(String),
    MappingVersionMismatch {
        expected: u32,
        actual: u32,
    },
    StorageSchemaMismatch {
        expected: String,
        actual: String,
    },
    /// A Redis value under `key` does not parse as a registry record.
    CorruptRecord {
        key: String,
        reason: String,
    },
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidObjectId(id) => write!(f, "invalid ObjectId: {id}"),
            Self::InvalidWorkerId(id) => write!(f, "worker id {id} exceeds {MAX_WORKER_ID}"),
            Self::AllocationDisabled(summary) => {
                write!(f, "allocation is disabled and no mapping exists for {summary}")
            }
            Self::UnknownObjectIds(summary) => {
                write!(f, "no ObjectId mapping exists for {summary}")
            }
            Self::TrustedImportDisabled(id) => write!(
                f,
                "trusted Snowflake import is disabled on this service; ObjectId {id} carried trusted_snowflake_id"
            ),
            Self::BeforeSnowflakeEpoch(ms) => write!(f, "timestamp {ms} predates Snowflake epoch"),
            Self::SecondExhausted(ms) => {
                write!(f, "Snowflake allocation exhausted for second {ms}")
            }
            Self::UnsupportedSnowflake(id) => write!(f, "unsupported Snowflake id {id}"),
            Self::UnknownSnowflake(id) => write!(f, "unknown Snowflake id {id}"),
            Self::MappingConflict { object_id, entity_kind, snowflake_id } => write!(
                f,
                "mapping conflict for {entity_kind:?} {object_id}: the registry already binds it to a Snowflake other than {snowflake_id}"
            ),
            Self::SnowflakeTaken {
                snowflake_id,
                object_id,
                entity_kind,
                holder,
            } => {
                write!(
                    f,
                    "Snowflake {snowflake_id} requested for {entity_kind:?} {object_id} is already bound to another object"
                )?;
                if let Some((holder_kind, holder_id)) = holder {
                    write!(f, " ({holder_kind:?} {holder_id})")?;
                }
                Ok(())
            }
            Self::EntityKindMismatch {
                snowflake_id,
                expected,
                actual,
            } => write!(
                f,
                "entity kind mismatch for Snowflake {snowflake_id}: expected {expected:?}, found {actual:?}"
            ),
            Self::Io(error) => write!(f, "id registry I/O error: {error}"),
            Self::Redis(error) => write!(f, "id registry Redis error: {error}"),
            Self::MappingVersionMismatch { expected, actual } => write!(
                f,
                "id registry mapping version mismatch: expected {expected}, found {actual}"
            ),
            Self::StorageSchemaMismatch { expected, actual } => write!(
                f,
                "id registry storage schema mismatch: expected {expected}, found {actual}"
            ),
            Self::CorruptRecord { key, reason } => {
                write!(f, "corrupt id registry record at {key}: {reason}")
            }
        }
    }
}

impl std::error::Error for IdError {}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize)]
pub struct SnowflakeId(u64);

impl SnowflakeId {
    pub fn new(value: u64) -> Result<Self, IdError> {
        if value == 0 || value > i64::MAX as u64 {
            return Err(IdError::UnsupportedSnowflake(value));
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }

    pub fn is_nil(self) -> bool {
        self.0 == 0
    }
}

/// Deserializes through `u64` and [`SnowflakeId::new`], so zero and values
/// outside the positive signed-64 range are rejected at the boundary instead
/// of producing an invalid id.
impl<'de> Deserialize<'de> for SnowflakeId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for SnowflakeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Mapping {
    pub object_id: String,
    pub entity_kind: EntityKind,
    pub snowflake_id: SnowflakeId,
    pub mapping_version: u32,
}

pub(crate) fn validate_object_id(value: &str) -> Result<(), IdError> {
    if value.len() != 24
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(IdError::InvalidObjectId(value.to_string()));
    }
    Ok(())
}

pub(crate) fn object_id_timestamp_secs(value: &str) -> Result<u64, IdError> {
    validate_object_id(value)?;
    u32::from_str_radix(&value[..8], 16)
        .map(u64::from)
        .map_err(|_| IdError::InvalidObjectId(value.to_string()))
}

pub(crate) fn object_id_timestamp_ms(value: &str) -> Result<u64, IdError> {
    object_id_timestamp_secs(value).map(|seconds| seconds.saturating_mul(1_000))
}

pub(crate) fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snowflake_id_deserializes_only_positive_signed_values() {
        assert_eq!(
            serde_json::from_str::<SnowflakeId>("42").unwrap(),
            SnowflakeId::new(42).unwrap()
        );
        assert!(serde_json::from_str::<SnowflakeId>("0").is_err());
        assert!(serde_json::from_str::<SnowflakeId>("9223372036854775808").is_err());
        assert!(serde_json::from_str::<SnowflakeId>("-1").is_err());
        assert_eq!(
            serde_json::to_string(&SnowflakeId::new(42).unwrap()).unwrap(),
            "42"
        );
    }

    #[test]
    fn conflict_messages_name_the_right_side_of_the_binding() {
        let conflict = IdError::MappingConflict {
            object_id: "65f1a2b3c4d5e6f708091011".into(),
            entity_kind: EntityKind::User,
            snowflake_id: SnowflakeId::new(7).unwrap(),
        }
        .to_string();
        assert_eq!(
            conflict,
            "mapping conflict for User 65f1a2b3c4d5e6f708091011: the registry already binds it to a Snowflake other than 7"
        );
        assert!(!conflict.contains("trusted"));

        let taken = IdError::SnowflakeTaken {
            snowflake_id: SnowflakeId::new(7).unwrap(),
            object_id: "65f1a2b3c4d5e6f708091011".into(),
            entity_kind: EntityKind::User,
            holder: Some((EntityKind::Post, "66f1a2b3c4d5e6f708091011".into())),
        }
        .to_string();
        assert_eq!(
            taken,
            "Snowflake 7 requested for User 65f1a2b3c4d5e6f708091011 is already bound to another object (Post 66f1a2b3c4d5e6f708091011)"
        );
        let taken_unknown_holder = IdError::SnowflakeTaken {
            snowflake_id: SnowflakeId::new(7).unwrap(),
            object_id: "65f1a2b3c4d5e6f708091011".into(),
            entity_kind: EntityKind::User,
            holder: None,
        }
        .to_string();
        assert!(taken_unknown_holder.ends_with("is already bound to another object"));
    }
}
