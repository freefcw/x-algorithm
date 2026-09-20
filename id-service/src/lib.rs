//! Persistent ObjectId ↔ Snowflake identity mapping.
//!
//! The service deliberately keeps the mapping independent from Home Mixer and
//! xrex. External ObjectIds are resolved at an ingress boundary; internal
//! callers exchange the resulting Snowflake value. The append-only file is the
//! source of truth, while the in-memory maps are only indexes and hot caches.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const SNOWFLAKE_EPOCH_MS: u64 = 1_288_834_974_657;
pub const MAPPING_VERSION: u32 = 1;
const WORKER_BITS: u8 = 10;
const SEQUENCE_BITS: u8 = 12;
const MAX_WORKER_ID: u64 = (1 << WORKER_BITS) - 1;
const MAX_SEQUENCE: u64 = (1 << SEQUENCE_BITS) - 1;
const POST_CACHE_TTL_SECS: u64 = 30 * 24 * 60 * 60;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum EntityKind {
    User,
    Post,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdError {
    InvalidObjectId(String),
    InvalidWorkerId(u64),
    AllocationDisabled(String),
    BeforeSnowflakeEpoch(u64),
    SecondExhausted(u64),
    UnsupportedSnowflake(u64),
    MappingConflict {
        object_id: String,
        entity_kind: EntityKind,
        snowflake_id: SnowflakeId,
    },
    EntityKindMismatch {
        snowflake_id: SnowflakeId,
        expected: EntityKind,
        actual: EntityKind,
    },
    Io(String),
    CorruptRecord {
        line: usize,
        reason: String,
    },
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidObjectId(id) => write!(f, "invalid ObjectId: {id}"),
            Self::InvalidWorkerId(id) => write!(f, "worker id {id} exceeds {MAX_WORKER_ID}"),
            Self::AllocationDisabled(id) => {
                write!(f, "no trusted Snowflake mapping exists for ObjectId {id}; allocation is disabled")
            }
            Self::BeforeSnowflakeEpoch(ms) => write!(f, "timestamp {ms} predates Snowflake epoch"),
            Self::SecondExhausted(ms) => {
                write!(f, "Snowflake allocation exhausted for second {ms}")
            }
            Self::UnsupportedSnowflake(id) => write!(f, "unsupported Snowflake id {id}"),
            Self::MappingConflict { object_id, entity_kind, snowflake_id } => write!(
                f,
                "mapping conflict for {entity_kind:?} {object_id}: trusted Snowflake {snowflake_id} differs from the registry"
            ),
            Self::EntityKindMismatch {
                snowflake_id,
                expected,
                actual,
            } => write!(
                f,
                "entity kind mismatch for Snowflake {snowflake_id}: expected {expected:?}, found {actual:?}"
            ),
            Self::Io(error) => write!(f, "id registry I/O error: {error}"),
            Self::CorruptRecord { line, reason } => {
                write!(f, "corrupt id registry record at line {line}: {reason}")
            }
        }
    }
}

impl std::error::Error for IdError {}

#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize,
)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MappingRecord {
    object_id: String,
    entity_kind: EntityKind,
    snowflake_id: SnowflakeId,
    #[serde(default = "current_mapping_version")]
    mapping_version: u32,
}

fn current_mapping_version() -> u32 {
    MAPPING_VERSION
}

impl MappingRecord {
    fn mapping(&self) -> Mapping {
        Mapping {
            object_id: self.object_id.clone(),
            entity_kind: self.entity_kind,
            snowflake_id: self.snowflake_id,
            mapping_version: self.mapping_version,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct CacheEntry {
    snowflake_id: SnowflakeId,
    expires_at_secs: Option<u64>,
}

/// A single-process registry with durable append-only storage.
///
/// User mappings remain hot for the process lifetime. Post mappings are cached
/// for 30 days from the ObjectId timestamp; older mappings remain durable and
/// are read from the registry file on demand.
pub struct IdRegistry {
    path: PathBuf,
    file: File,
    by_object: HashMap<(EntityKind, String), u64>,
    by_snowflake: HashMap<u64, MappingRecord>,
    object_cache: HashMap<(EntityKind, String), CacheEntry>,
    snowflake_cache: HashMap<u64, CacheEntry>,
    worker_id: u64,
    next_by_second: HashMap<u64, u64>,
    allow_allocation: bool,
}

impl IdRegistry {
    /// Open a registry with allocation enabled for local/test compatibility.
    /// Production services should use [`Self::open_with_options`] and make
    /// allocation an explicit deployment decision.
    pub fn open(path: impl AsRef<Path>, worker_id: u64) -> Result<Self, IdError> {
        Self::open_with_options(path, worker_id, true)
    }

    pub fn open_with_options(
        path: impl AsRef<Path>,
        worker_id: u64,
        allow_allocation: bool,
    ) -> Result<Self, IdError> {
        if worker_id > MAX_WORKER_ID {
            return Err(IdError::InvalidWorkerId(worker_id));
        }
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|e| IdError::Io(e.to_string()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|e| IdError::Io(e.to_string()))?;
        file.try_lock().map_err(|e| IdError::Io(e.to_string()))?;
        let mut registry = Self {
            path,
            file,
            by_object: HashMap::new(),
            by_snowflake: HashMap::new(),
            object_cache: HashMap::new(),
            snowflake_cache: HashMap::new(),
            worker_id,
            next_by_second: HashMap::new(),
            allow_allocation,
        };
        registry.load()?;
        Ok(registry)
    }

    pub fn resolve_one(
        &mut self,
        object_id: &str,
        entity_kind: EntityKind,
    ) -> Result<SnowflakeId, IdError> {
        self.resolve_one_with_trusted(object_id, entity_kind, None)
    }

    /// Resolves an ObjectId, optionally preserving a trusted Snowflake from an
    /// existing checkpoint or retrieval index. Existing mappings are immutable.
    pub fn resolve_one_with_trusted(
        &mut self,
        object_id: &str,
        entity_kind: EntityKind,
        trusted_snowflake_id: Option<SnowflakeId>,
    ) -> Result<SnowflakeId, IdError> {
        validate_object_id(object_id)?;
        let key = (entity_kind, object_id.to_string());
        if let Some(trusted) = trusted_snowflake_id {
            if let Some(existing) = self.by_object.get(&key).copied() {
                if existing != trusted.get() {
                    return Err(IdError::MappingConflict {
                        object_id: object_id.to_string(),
                        entity_kind,
                        snowflake_id: trusted,
                    });
                }
            }
        }
        if let Some(entry) = self.object_cache.get(&key).copied() {
            if entry
                .expires_at_secs
                .is_none_or(|expiry| expiry > now_secs())
            {
                return Ok(entry.snowflake_id);
            }
            self.object_cache.remove(&key);
        }
        if let Some(value) = self.by_object.get(&key).copied() {
            let id = SnowflakeId::new(value)?;
            self.cache(key, id);
            return Ok(id);
        }

        let id = match trusted_snowflake_id {
            Some(id) => {
                if self.by_snowflake.contains_key(&id.get()) {
                    return Err(IdError::MappingConflict {
                        object_id: object_id.to_string(),
                        entity_kind,
                        snowflake_id: id,
                    });
                }
                id
            }
            None => {
                if !self.allow_allocation {
                    return Err(IdError::AllocationDisabled(object_id.to_string()));
                }
                self.allocate(object_id_timestamp_ms(object_id)?)?
            }
        };
        let record = MappingRecord {
            object_id: object_id.to_string(),
            entity_kind,
            snowflake_id: id,
            mapping_version: MAPPING_VERSION,
        };
        self.append(&record)?;
        self.by_object.insert(key.clone(), id.get());
        self.by_snowflake.insert(id.get(), record);
        self.observe_allocated_id(id.get());
        self.cache(key, id);
        self.cache_snowflake(id);
        Ok(id)
    }

    pub fn resolve_batch(
        &mut self,
        ids: &[(String, EntityKind)],
    ) -> Result<Vec<SnowflakeId>, IdError> {
        self.resolve_batch_with_trusted(
            &ids.iter()
                .map(|(id, kind)| (id.clone(), *kind, None))
                .collect::<Vec<_>>(),
        )
    }

    pub fn resolve_batch_with_trusted(
        &mut self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
    ) -> Result<Vec<SnowflakeId>, IdError> {
        // Plan every lookup before mutating the durable indexes. In particular,
        // a conflict in a later item must not leave earlier items committed.
        for (object_id, _, trusted) in ids {
            validate_object_id(object_id)?;
            if trusted.is_none() {
                let timestamp_ms = object_id_timestamp_ms(object_id)?;
                if timestamp_ms < SNOWFLAKE_EPOCH_MS {
                    return Err(IdError::BeforeSnowflakeEpoch(timestamp_ms));
                }
            }
        }

        let original_next_by_second = self.next_by_second.clone();
        let mut planned_by_object: HashMap<(EntityKind, String), SnowflakeId> = HashMap::new();
        let mut planned_by_snowflake: HashMap<u64, (EntityKind, String)> = HashMap::new();
        let mut planned_records = Vec::new();
        let mut result = Vec::with_capacity(ids.len());

        let plan_result = (|| {
            for (object_id, entity_kind, trusted) in ids {
                let key = (*entity_kind, object_id.clone());
                if let Some(existing) = self.by_object.get(&key).copied() {
                    let existing = SnowflakeId::new(existing)?;
                    if let Some(trusted) = trusted {
                        if existing != *trusted {
                            return Err(IdError::MappingConflict {
                                object_id: object_id.clone(),
                                entity_kind: *entity_kind,
                                snowflake_id: *trusted,
                            });
                        }
                    }
                    result.push(existing);
                    continue;
                }

                if let Some(existing) = planned_by_object.get(&key).copied() {
                    if let Some(trusted) = trusted {
                        if existing != *trusted {
                            return Err(IdError::MappingConflict {
                                object_id: object_id.clone(),
                                entity_kind: *entity_kind,
                                snowflake_id: *trusted,
                            });
                        }
                    }
                    result.push(existing);
                    continue;
                }

                let id = match trusted {
                    Some(id) => *id,
                    None => {
                        if !self.allow_allocation {
                            return Err(IdError::AllocationDisabled(object_id.clone()));
                        }
                        self.allocate(object_id_timestamp_ms(object_id)?)?
                    }
                };
                if self.by_snowflake.contains_key(&id.get())
                    || planned_by_snowflake.contains_key(&id.get())
                {
                    return Err(IdError::MappingConflict {
                        object_id: object_id.clone(),
                        entity_kind: *entity_kind,
                        snowflake_id: id,
                    });
                }

                let record = MappingRecord {
                    object_id: object_id.clone(),
                    entity_kind: *entity_kind,
                    snowflake_id: id,
                    mapping_version: MAPPING_VERSION,
                };
                planned_by_object.insert(key.clone(), id);
                planned_by_snowflake.insert(id.get(), key);
                planned_records.push(record);
                result.push(id);
            }
            Ok::<(), IdError>(())
        })();

        if let Err(error) = plan_result {
            self.next_by_second = original_next_by_second;
            return Err(error);
        }

        if let Err(error) = self.append_batch(&planned_records) {
            self.next_by_second = original_next_by_second;
            return Err(error);
        }

        for record in planned_records {
            let key = (record.entity_kind, record.object_id.clone());
            let id = record.snowflake_id;
            self.by_object.insert(key.clone(), id.get());
            self.by_snowflake.insert(id.get(), record);
            self.observe_allocated_id(id.get());
            self.cache(key, id);
            self.cache_snowflake(id);
        }
        Ok(result)
    }

    pub fn reverse_one(
        &mut self,
        snowflake_id: SnowflakeId,
        expected_kind: EntityKind,
    ) -> Result<Mapping, IdError> {
        if let Some(entry) = self.snowflake_cache.get(&snowflake_id.get()).copied() {
            if entry
                .expires_at_secs
                .is_none_or(|expiry| expiry > now_secs())
            {
                let record = self
                    .by_snowflake
                    .get(&snowflake_id.get())
                    .ok_or(IdError::UnsupportedSnowflake(snowflake_id.get()))?;
                return self.checked_mapping(record, snowflake_id, expected_kind);
            }
            self.snowflake_cache.remove(&snowflake_id.get());
        }
        let mapping = {
            let record = self
                .by_snowflake
                .get(&snowflake_id.get())
                .ok_or(IdError::UnsupportedSnowflake(snowflake_id.get()))?;
            self.checked_mapping(record, snowflake_id, expected_kind)?
        };
        self.cache_snowflake(snowflake_id);
        Ok(mapping)
    }

    pub fn reverse_batch(
        &mut self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> Result<Vec<Mapping>, IdError> {
        ids.iter()
            .map(|(id, kind)| self.reverse_one(*id, *kind))
            .collect()
    }

    fn checked_mapping(
        &self,
        record: &MappingRecord,
        snowflake_id: SnowflakeId,
        expected_kind: EntityKind,
    ) -> Result<Mapping, IdError> {
        if record.entity_kind != expected_kind {
            return Err(IdError::EntityKindMismatch {
                snowflake_id,
                expected: expected_kind,
                actual: record.entity_kind,
            });
        }
        Ok(record.mapping())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn cache_sizes(&self) -> (usize, usize) {
        (self.object_cache.len(), self.snowflake_cache.len())
    }

    fn load(&mut self) -> Result<(), IdError> {
        let file = File::open(&self.path).map_err(|e| IdError::Io(e.to_string()))?;
        for (index, line) in BufReader::new(file).lines().enumerate() {
            let line = line.map_err(|e| IdError::Io(e.to_string()))?;
            let record: MappingRecord =
                serde_json::from_str(&line).map_err(|e| IdError::CorruptRecord {
                    line: index + 1,
                    reason: e.to_string(),
                })?;
            if record.mapping_version != MAPPING_VERSION {
                return Err(IdError::CorruptRecord {
                    line: index + 1,
                    reason: format!("unsupported mapping_version {}", record.mapping_version),
                });
            }
            validate_object_id(&record.object_id)?;
            let key = (record.entity_kind, record.object_id.clone());
            let value = record.snowflake_id.get();
            if self.by_object.insert(key, value).is_some()
                || self.by_snowflake.insert(value, record.clone()).is_some()
            {
                return Err(IdError::CorruptRecord {
                    line: index + 1,
                    reason: "duplicate mapping".to_string(),
                });
            }
            self.observe_allocated_id(value);
        }
        Ok(())
    }

    fn observe_allocated_id(&mut self, value: u64) {
        let timestamp_ms = (value >> (WORKER_BITS + SEQUENCE_BITS)) + SNOWFLAKE_EPOCH_MS;
        let worker_id = (value >> SEQUENCE_BITS) & MAX_WORKER_ID;
        if worker_id == self.worker_id {
            let second = timestamp_ms / 1000 * 1000;
            let next = (timestamp_ms - second) * (MAX_SEQUENCE + 1) + (value & MAX_SEQUENCE) + 1;
            let entry = self.next_by_second.entry(second).or_default();
            *entry = (*entry).max(next);
        }
    }

    fn append(&mut self, record: &MappingRecord) -> Result<(), IdError> {
        self.append_batch(std::slice::from_ref(record))
    }

    fn append_batch(&mut self, records: &[MappingRecord]) -> Result<(), IdError> {
        let mut payload = Vec::new();
        for record in records {
            serde_json::to_writer(&mut payload, record).map_err(|e| IdError::Io(e.to_string()))?;
            payload.push(b'\n');
        }
        self.file
            .write_all(&payload)
            .and_then(|_| self.file.sync_data())
            .map_err(|e| IdError::Io(e.to_string()))
    }

    fn allocate(&mut self, timestamp_ms: u64) -> Result<SnowflakeId, IdError> {
        if timestamp_ms < SNOWFLAKE_EPOCH_MS {
            return Err(IdError::BeforeSnowflakeEpoch(timestamp_ms));
        }
        let next = self.next_by_second.entry(timestamp_ms).or_default();
        if *next >= 1000 * (MAX_SEQUENCE + 1) {
            return Err(IdError::SecondExhausted(timestamp_ms));
        }
        let millis = timestamp_ms + *next / (MAX_SEQUENCE + 1);
        let value = ((millis - SNOWFLAKE_EPOCH_MS) << (WORKER_BITS + SEQUENCE_BITS))
            | (self.worker_id << SEQUENCE_BITS)
            | (*next & MAX_SEQUENCE);
        let id = SnowflakeId::new(value)?;
        *next += 1;
        Ok(id)
    }

    fn cache(&mut self, key: (EntityKind, String), id: SnowflakeId) {
        let expires_at_secs = match key.0 {
            EntityKind::User => None,
            EntityKind::Post => object_id_timestamp_secs(&key.1)
                .ok()
                .map(|created| created.saturating_add(POST_CACHE_TTL_SECS)),
        };
        if expires_at_secs.is_some_and(|expiry| expiry <= now_secs()) {
            return;
        }
        self.object_cache.insert(
            key,
            CacheEntry {
                snowflake_id: id,
                expires_at_secs,
            },
        );
    }

    fn cache_snowflake(&mut self, id: SnowflakeId) {
        let Some(record) = self.by_snowflake.get(&id.get()) else {
            return;
        };
        let expires_at_secs = match record.entity_kind {
            EntityKind::User => None,
            EntityKind::Post => object_id_timestamp_secs(&record.object_id)
                .ok()
                .map(|created| created.saturating_add(POST_CACHE_TTL_SECS)),
        };
        if expires_at_secs.is_some_and(|expiry| expiry <= now_secs()) {
            return;
        }
        self.snowflake_cache.insert(
            id.get(),
            CacheEntry {
                snowflake_id: id,
                expires_at_secs,
            },
        );
    }
}

fn validate_object_id(value: &str) -> Result<(), IdError> {
    if value.len() != 24
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(IdError::InvalidObjectId(value.to_string()));
    }
    Ok(())
}

fn object_id_timestamp_secs(value: &str) -> Result<u64, IdError> {
    validate_object_id(value)?;
    u32::from_str_radix(&value[..8], 16)
        .map(u64::from)
        .map_err(|_| IdError::InvalidObjectId(value.to_string()))
}

fn object_id_timestamp_ms(value: &str) -> Result<u64, IdError> {
    object_id_timestamp_secs(value).map(|seconds| seconds.saturating_mul(1_000))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path() -> PathBuf {
        let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "id-registry-{}-{nanos}-{suffix}.jsonl",
            std::process::id()
        ))
    }

    #[test]
    fn refuses_second_writer_until_first_is_dropped() {
        let path = temp_path();
        let first = IdRegistry::open(&path, 1).unwrap();
        assert!(IdRegistry::open(&path, 2).is_err());
        drop(first);
        assert!(IdRegistry::open(&path, 2).is_ok());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn backfill_preserves_original_second_and_sequence_after_restart() {
        let path = temp_path();
        let old_second = 1_700_000_000_u64;
        let recent_second = old_second + 31 * 86400;
        let old = format!("{old_second:08x}0000000000000001");
        let recent = format!("{recent_second:08x}0000000000000002");
        let mut registry = IdRegistry::open(&path, 3).unwrap();
        registry.resolve_one(&recent, EntityKind::Post).unwrap();
        let first = registry.resolve_one(&old, EntityKind::Post).unwrap();
        assert_eq!((first.get() >> 22) + SNOWFLAKE_EPOCH_MS, old_second * 1000);
        drop(registry);
        let mut registry = IdRegistry::open(&path, 3).unwrap();
        let next = format!("{old_second:08x}0000000000000003");
        let second = registry.resolve_one(&next, EntityKind::Post).unwrap();
        assert_ne!(first, second);
        assert_eq!((second.get() >> 22) + SNOWFLAKE_EPOCH_MS, old_second * 1000);
        assert_eq!(
            registry
                .reverse_one(first, EntityKind::Post)
                .unwrap()
                .object_id,
            old
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn invalid_timestamp_in_batch_does_not_write_prefix() {
        let path = temp_path();
        let mut registry = IdRegistry::open(&path, 0).unwrap();
        assert!(registry
            .resolve_batch(&[
                ("65f1a2b3c4d5e6f708091011".into(), EntityKind::Post),
                ("000000010000000000000001".into(), EntityKind::Post),
            ])
            .is_err());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn trusted_conflict_in_batch_does_not_commit_earlier_items() {
        let path = temp_path();
        let mut registry = IdRegistry::open(&path, 0).unwrap();
        let trusted = SnowflakeId::new(123).unwrap();
        registry
            .resolve_one_with_trusted("65f1a2b3c4d5e6f708091011", EntityKind::User, Some(trusted))
            .unwrap();

        let error = registry
            .resolve_batch_with_trusted(&[
                ("65f1a2b3c4d5e6f708091012".into(), EntityKind::Post, None),
                (
                    "65f1a2b3c4d5e6f708091011".into(),
                    EntityKind::User,
                    Some(SnowflakeId::new(124).unwrap()),
                ),
            ])
            .unwrap_err();
        assert!(matches!(error, IdError::MappingConflict { .. }));
        let size_after_failed_batch = std::fs::metadata(&path).unwrap().len();
        assert!(registry
            .reverse_one(SnowflakeId::new(123).unwrap(), EntityKind::User)
            .is_ok());
        assert!(matches!(
            registry.reverse_one(SnowflakeId::new(124).unwrap(), EntityKind::Post),
            Err(IdError::UnsupportedSnowflake(124))
        ));
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            size_after_failed_batch
        );
        drop(registry);
        let reopened = IdRegistry::open(&path, 0).unwrap();
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn resolves_single_id_and_round_trips_after_reopen() {
        let path = temp_path();
        let user = "65f1a2b3c4d5e6f708091011";
        let id = {
            let mut registry = IdRegistry::open(&path, 7).unwrap();
            let id = registry.resolve_one(user, EntityKind::User).unwrap();
            assert_eq!(
                registry
                    .reverse_one(id, EntityKind::User)
                    .unwrap()
                    .object_id,
                user
            );
            id
        };
        let mut reopened = IdRegistry::open(&path, 7).unwrap();
        assert_eq!(reopened.resolve_one(user, EntityKind::User).unwrap(), id);
        assert_eq!(
            reopened
                .reverse_one(id, EntityKind::User)
                .unwrap()
                .object_id,
            user
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn batch_resolution_is_stable_and_keeps_entity_namespaces_distinct() {
        let path = temp_path();
        let mut registry = IdRegistry::open(&path, 1).unwrap();
        let ids = registry
            .resolve_batch(&[
                ("65f1a2b3c4d5e6f708091011".to_string(), EntityKind::User),
                ("65f1a2b3c4d5e6f708091011".to_string(), EntityKind::Post),
            ])
            .unwrap();
        assert_ne!(ids[0], ids[1]);
        let mappings = registry
            .reverse_batch(&[(ids[0], EntityKind::User), (ids[1], EntityKind::Post)])
            .unwrap();
        assert_eq!(
            mappings
                .iter()
                .map(|m| m.object_id.as_str())
                .collect::<Vec<_>>(),
            vec!["65f1a2b3c4d5e6f708091011", "65f1a2b3c4d5e6f708091011",]
        );
        assert!(mappings
            .iter()
            .all(|m| m.mapping_version == MAPPING_VERSION));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn caches_all_users_and_only_posts_from_the_last_thirty_days() {
        let path = temp_path();
        let now = now_secs();
        let recent = format!("{:08x}0000000000000001", now - 24 * 60 * 60);
        let old = format!("{:08x}0000000000000002", now - 31 * 24 * 60 * 60);
        let mut registry = IdRegistry::open(&path, 2).unwrap();
        registry
            .resolve_one("65f1a2b3c4d5e6f708091011", EntityKind::User)
            .unwrap();
        registry.resolve_one(&recent, EntityKind::Post).unwrap();
        registry.resolve_one(&old, EntityKind::Post).unwrap();
        assert_eq!(registry.cache_sizes(), (2, 2));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trusted_snowflake_is_preserved_and_conflicts_are_rejected() {
        let path = temp_path();
        let trusted = SnowflakeId::new(123).unwrap();
        let object_id = "65f1a2b3c4d5e6f708091011";
        let mut registry = IdRegistry::open(&path, 0).unwrap();
        assert_eq!(
            registry
                .resolve_one_with_trusted(object_id, EntityKind::User, Some(trusted))
                .unwrap(),
            trusted
        );
        assert_eq!(
            registry
                .resolve_one_with_trusted(object_id, EntityKind::User, Some(trusted))
                .unwrap(),
            trusted
        );
        assert!(matches!(
            registry.resolve_one_with_trusted(
                "65f1a2b3c4d5e6f708091012",
                EntityKind::Post,
                Some(trusted),
            ),
            Err(IdError::MappingConflict { .. })
        ));
        drop(registry);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn disabled_allocation_requires_a_trusted_snowflake() {
        let path = temp_path();
        let mut registry = IdRegistry::open_with_options(&path, 0, false).unwrap();
        assert!(matches!(
            registry.resolve_one("65f1a2b3c4d5e6f708091011", EntityKind::Post),
            Err(IdError::AllocationDisabled(_))
        ));
        assert!(matches!(
            registry.resolve_batch(&[("65f1a2b3c4d5e6f708091012".into(), EntityKind::Post,)]),
            Err(IdError::AllocationDisabled(_))
        ));
        let trusted = SnowflakeId::new(42).unwrap();
        assert_eq!(
            registry
                .resolve_one_with_trusted(
                    "65f1a2b3c4d5e6f708091011",
                    EntityKind::Post,
                    Some(trusted),
                )
                .unwrap(),
            trusted
        );
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_invalid_object_ids_and_out_of_range_workers() {
        let path = temp_path();
        assert!(matches!(
            IdRegistry::open(&path, MAX_WORKER_ID + 1),
            Err(IdError::InvalidWorkerId(_))
        ));
        let mut registry = IdRegistry::open(&path, 0).unwrap();
        assert!(matches!(
            registry.resolve_one("not-an-object-id", EntityKind::Post),
            Err(IdError::InvalidObjectId(_))
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn reverse_rejects_the_wrong_entity_kind() {
        let path = temp_path();
        let mut registry = IdRegistry::open(&path, 0).unwrap();
        let id = registry
            .resolve_one("65f1a2b3c4d5e6f708091011", EntityKind::Post)
            .unwrap();
        assert!(matches!(
            registry.reverse_one(id, EntityKind::User),
            Err(IdError::EntityKindMismatch {
                expected: EntityKind::User,
                actual: EntityKind::Post,
                ..
            })
        ));
        assert!(matches!(
            registry.reverse_batch(&[(id, EntityKind::User)]),
            Err(IdError::EntityKindMismatch { .. })
        ));
        assert_eq!(
            registry
                .reverse_one(id, EntityKind::Post)
                .unwrap()
                .object_id,
            "65f1a2b3c4d5e6f708091011"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn allocated_ids_decode_to_the_object_id_second_with_the_standard_epoch() {
        let path = temp_path();
        let mut registry = IdRegistry::open(&path, 5).unwrap();
        let timestamp_ms = 1_700_000_000_000_u64;
        let object_id = format!("{:08x}0000000000000001", timestamp_ms / 1000);
        let id = registry.resolve_one(&object_id, EntityKind::Post).unwrap();
        let decoded_ms = (id.get() >> (WORKER_BITS + SEQUENCE_BITS)) + SNOWFLAKE_EPOCH_MS;
        assert!(decoded_ms >= timestamp_ms && decoded_ms <= timestamp_ms + 999);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn legacy_records_default_to_v1_and_unknown_versions_fail_startup() {
        let path = temp_path();
        std::fs::write(
            &path,
            "{\"object_id\":\"65f1a2b3c4d5e6f708091011\",\"entity_kind\":\"Post\",\"snowflake_id\":42}\n",
        )
        .unwrap();
        let mut registry = IdRegistry::open(&path, 0).unwrap();
        let mapping = registry
            .reverse_one(SnowflakeId::new(42).unwrap(), EntityKind::Post)
            .unwrap();
        assert_eq!(mapping.mapping_version, MAPPING_VERSION);
        assert_eq!(mapping.object_id, "65f1a2b3c4d5e6f708091011");
        drop(registry);

        let path = temp_path();
        std::fs::write(
            &path,
            "{\"object_id\":\"65f1a2b3c4d5e6f708091011\",\"entity_kind\":\"Post\",\"snowflake_id\":42,\"mapping_version\":2}\n",
        )
        .unwrap();
        assert!(matches!(
            IdRegistry::open(&path, 0),
            Err(IdError::CorruptRecord { .. })
        ));
        let _ = std::fs::remove_file(path);
    }
}
