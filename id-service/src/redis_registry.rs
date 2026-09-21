//! Identity registry: storage port, application service and Redis adapter.
//!
//! * [`MappingStore`] is the persistence port. Every operation is batch-first
//!   so the service can plan a whole request before touching the store.
//! * [`RedisIdRegistry`] holds the identity rules (caller-provided ids,
//!   Snowflake allocation, conflict handling) and knows nothing about Redis keys.
//! * `RedisMappingStore` maps the port onto sharded Redis keys with a bounded
//!   local cache; [`MemoryMappingStore`] is the in-process development backend
//!   and test fixture.

use async_trait::async_trait;
use futures::future::try_join_all;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::cluster::ClusterClientBuilder;
use redis::{ErrorKind, FromRedisValue};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::Duration;

use crate::metrics::metrics;
use crate::{
    object_id_timestamp_secs, EntityKind, IdError, Mapping, SnowflakeId, MAPPING_VERSION,
    MAX_SEQUENCE, MAX_WORKER_ID, POST_CACHE_TTL_SECS, SEQUENCE_BITS, SNOWFLAKE_EPOCH_MS,
    WORKER_BITS,
};

const SEQUENCE_TTL_SECS: i64 = 2 * 24 * 60 * 60;
const MAPPING_SHARD_COUNT: u16 = 256;
/// Sequence values available per `(worker, second)`: 1000 milliseconds times
/// 4096 sequence numbers.
const SEQUENCES_PER_SECOND: u64 = 1_000 * (MAX_SEQUENCE + 1);

/// Identifier of the key layout, persisted next to the mapping version so a
/// prefix written by another layout is rejected instead of silently read.
pub const STORAGE_SCHEMA: &str = "sharded-256";

/// Upper bound on write attempts for one allocated mapping. Each attempt
/// draws a fresh sequence value after the previous Snowflake turned out to be
/// bound already (registered by another caller, or an orphan reverse entry).
pub const MAX_ALLOCATION_ATTEMPTS: usize = 16;

/// `SET NX` with idempotent replays: 1 created, 0 identical value already
/// present, -1 a different value is present.
static RESERVE_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r#"
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
if current == ARGV[1] then
  return 0
end
return -1
"#,
    )
});

/// Delete the key only while it still holds the value this writer stored.
static DELETE_IF_VALUE_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
end
return 1
"#,
    )
});

/// Reserve `ARGV[1]` consecutive sequence values and refresh the TTL.
static SEQUENCE_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r#"
local value = redis.call('INCRBY', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return value
"#,
    )
});

/// Raise a sequence counter to at least `ARGV[1]`; never lowers it.
static SEQUENCE_FLOOR_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r#"
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local floor = tonumber(ARGV[1])
if current < floor then
  redis.call('SET', KEYS[1], ARGV[1])
  redis.call('EXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
"#,
    )
});

fn all_scripts() -> [&'static redis::Script; 4] {
    [
        &RESERVE_SCRIPT,
        &DELETE_IF_VALUE_SCRIPT,
        &SEQUENCE_SCRIPT,
        &SEQUENCE_FLOOR_SCRIPT,
    ]
}

type ObjectKey = (EntityKind, String);

/// Result of one conditional mapping write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertOutcome {
    /// Both index entries were created by this write.
    Inserted,
    /// The identical mapping already existed (replay or a concurrent writer
    /// of the same mapping); nothing changed.
    AlreadyPresent,
    /// The object is already bound to a different Snowflake.
    ObjectConflict,
    /// The Snowflake is already bound to a different object; nothing changed.
    SnowflakeConflict,
}

/// Reservation of `count` consecutive values from the `(worker, second)`
/// counter that `object_timestamp_ms` falls into.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SequenceReservation {
    pub worker_id: u64,
    pub object_timestamp_ms: u64,
    pub count: u64,
}

/// Lower bound for a `(worker, second)` counter, derived from an imported
/// Snowflake so later allocations skip the values it already occupies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SequenceFloor {
    pub worker_id: u64,
    pub second: u64,
    pub min_next: u64,
}

/// Persistence port for the identity registry application service.
///
/// Batch methods are the primitives; the single-item methods are provided
/// conveniences that fixtures may override to count per-item traffic.
#[async_trait]
pub trait MappingStore: Send + Sync {
    /// Returns exactly one row per input id, in input order (`None` when the
    /// mapping is absent); callers rely on the positional alignment.
    async fn find_by_object_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> Result<Vec<Option<Mapping>>, IdError>;

    /// Reverse lookups only report entries whose forward entry points back
    /// at the same Snowflake; an orphan reverse entry counts as absent.
    async fn find_by_snowflake_batch(
        &self,
        ids: &[SnowflakeId],
    ) -> Result<Vec<Option<Mapping>>, IdError>;

    /// Two-phase conditional write (reverse entry first, then forward entry)
    /// for every mapping, returning one outcome per input position.
    async fn insert_if_absent_batch(
        &self,
        mappings: &[Mapping],
    ) -> Result<Vec<InsertOutcome>, IdError>;

    /// Returns the counter value after each reservation; the reserved values
    /// are `[value - count + 1, value]`.
    async fn next_sequence_batch(
        &self,
        reservations: &[SequenceReservation],
    ) -> Result<Vec<u64>, IdError>;

    async fn observe_sequence_batch(&self, floors: &[SequenceFloor]) -> Result<(), IdError>;

    async fn check_ready(&self) -> Result<(), IdError>;

    fn cache_sizes(&self) -> (usize, usize);

    async fn find_by_object(
        &self,
        entity_kind: EntityKind,
        object_id: &str,
    ) -> Result<Option<Mapping>, IdError> {
        let mut found = self
            .find_by_object_batch(&[(object_id.to_string(), entity_kind)])
            .await?;
        Ok(found.pop().flatten())
    }

    async fn find_by_snowflake(
        &self,
        snowflake_id: SnowflakeId,
    ) -> Result<Option<Mapping>, IdError> {
        let mut found = self.find_by_snowflake_batch(&[snowflake_id]).await?;
        Ok(found.pop().flatten())
    }

    async fn insert_if_absent(&self, mapping: &Mapping) -> Result<InsertOutcome, IdError> {
        let mut outcomes = self
            .insert_if_absent_batch(std::slice::from_ref(mapping))
            .await?;
        outcomes
            .pop()
            .ok_or_else(|| IdError::Redis("mapping write returned no outcome".to_string()))
    }

    async fn next_sequence(
        &self,
        worker_id: u64,
        object_timestamp_ms: u64,
    ) -> Result<u64, IdError> {
        let mut values = self
            .next_sequence_batch(&[SequenceReservation {
                worker_id,
                object_timestamp_ms,
                count: 1,
            }])
            .await?;
        values
            .pop()
            .ok_or_else(|| IdError::Redis("sequence reservation returned no value".to_string()))
    }

    async fn observe_sequence(
        &self,
        worker_id: u64,
        second: u64,
        min_next: u64,
    ) -> Result<(), IdError> {
        self.observe_sequence_batch(&[SequenceFloor {
            worker_id,
            second,
            min_next,
        }])
        .await
    }
}

#[derive(Clone, Debug)]
pub struct RedisIdRegistryConfig {
    /// Whether to use Redis as the durable primary store. When disabled, the
    /// registry uses an in-process [`MemoryMappingStore`] only. This is useful
    /// for local development and does not provide cross-process persistence.
    pub redis_enabled: bool,
    pub single_url: Option<String>,
    pub cluster_urls: Option<Vec<String>>,
    pub key_prefix: String,
    pub cache_capacity: usize,
    pub worker_id: u64,
    pub allow_allocation: bool,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

/// Application service containing identity rules but no Redis-specific logic.
pub struct RedisIdRegistry {
    store: Arc<dyn MappingStore>,
    worker_id: u64,
    allow_allocation: bool,
}

/// One distinct `(entity_kind, object_id)` of a request after
/// de-duplication, with the provided Snowflake merged from its duplicates.
struct RequestItem {
    object_id: String,
    entity_kind: EntityKind,
    provided: Option<SnowflakeId>,
}

impl RequestItem {
    fn key(&self) -> ObjectKey {
        (self.entity_kind, self.object_id.clone())
    }
}

/// A mapping about to be written and the request item it satisfies.
struct PendingWrite<'a> {
    item: &'a RequestItem,
    snowflake_id: SnowflakeId,
}

impl PendingWrite<'_> {
    fn is_provided(&self) -> bool {
        self.item.provided.is_some()
    }

    fn mapping(&self) -> Mapping {
        Mapping {
            object_id: self.item.object_id.clone(),
            entity_kind: self.item.entity_kind,
            snowflake_id: self.snowflake_id,
            mapping_version: MAPPING_VERSION,
        }
    }
}

impl RedisIdRegistry {
    pub async fn connect(config: RedisIdRegistryConfig) -> Result<Self, IdError> {
        if config.worker_id > MAX_WORKER_ID {
            return Err(IdError::InvalidWorkerId(config.worker_id));
        }
        if config.cache_capacity == 0 {
            return Err(IdError::Redis(
                "cache capacity must be positive".to_string(),
            ));
        }
        let store: Arc<dyn MappingStore> = if config.redis_enabled {
            Arc::new(
                RedisMappingStore::connect(
                    config.single_url.as_deref(),
                    config.cluster_urls.as_deref(),
                    config.key_prefix,
                    config.cache_capacity,
                    config.connect_timeout,
                    config.request_timeout,
                )
                .await?,
            )
        } else {
            Arc::new(MemoryMappingStore::new())
        };
        Self::with_store(store, config.worker_id, config.allow_allocation)
    }

    pub fn with_store(
        store: Arc<dyn MappingStore>,
        worker_id: u64,
        allow_allocation: bool,
    ) -> Result<Self, IdError> {
        if worker_id > MAX_WORKER_ID {
            return Err(IdError::InvalidWorkerId(worker_id));
        }
        Ok(Self {
            store,
            worker_id,
            allow_allocation,
        })
    }

    /// Single-item allocation shares the batch path so both behave the same
    /// on conflicts, orphans and provided ids.
    pub async fn allocate_one(
        &self,
        object_id: &str,
        entity_kind: EntityKind,
        provided_snowflake_id: Option<SnowflakeId>,
    ) -> Result<SnowflakeId, IdError> {
        let mut resolved = self
            .allocate_batch(&[(
                object_id.to_string(),
                entity_kind,
                provided_snowflake_id,
            )])
            .await?;
        resolved
            .pop()
            .ok_or_else(|| IdError::Redis("resolution returned no result".to_string()))
    }

    /// Read-only resolution. Unlike `resolve_batch`, this method never writes
    /// or allocates; it is used by downstream readers and all egress paths.
    pub async fn resolve_existing_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> Result<Vec<SnowflakeId>, IdError> {
        for (object_id, _) in ids {
            crate::validate_object_id(object_id)?;
        }
        let mappings = self.store.find_by_object_batch(ids).await?;
        if mappings.len() != ids.len() {
            return Err(IdError::CorruptRecord {
                key: "mapping store batch".to_string(),
                reason: format!(
                    "find_by_object_batch returned {} rows for {} inputs",
                    mappings.len(),
                    ids.len()
                ),
            });
        }
        let unknown = ids
            .iter()
            .zip(&mappings)
            .filter(|(_, mapping)| mapping.is_none())
            .map(|((object_id, kind), _)| (*kind, object_id.as_str()))
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(IdError::UnknownObjectIds(unknown_ids_summary(
                unknown.into_iter(),
            )));
        }
        let mut result = Vec::with_capacity(ids.len());
        for ((_, kind), mapping) in ids.iter().zip(mappings) {
            let mapping = mapping.expect("unknown ids rejected above");
            if mapping.entity_kind != *kind {
                return Err(IdError::EntityKindMismatch {
                    snowflake_id: mapping.snowflake_id,
                    expected: *kind,
                    actual: mapping.entity_kind,
                });
            }
            result.push(mapping.snowflake_id);
        }
        Ok(result)
    }

    /// Read one existing mapping without making transport handlers interpret
    /// the cardinality of a batch result themselves.
    pub async fn resolve_existing_one(
        &self,
        object_id: &str,
        entity_kind: EntityKind,
    ) -> Result<SnowflakeId, IdError> {
        let mut resolved = self
            .resolve_existing_batch(&[(object_id.to_string(), entity_kind)])
            .await?;
        resolved.pop().ok_or_else(|| IdError::CorruptRecord {
            key: "mapping store batch".to_string(),
            reason: "single existing mapping lookup returned no result".to_string(),
        })
    }

    /// Allocate a batch: provided ids are registered as-is, missing ids are
    /// drawn from the sequence.
    ///
    /// The batch is planned before the first write: existing mappings that
    /// contradict a provided id, provided ids already bound elsewhere,
    /// in-batch contradictions, and disabled allocation all reject the whole
    /// batch with nothing written. Only a concurrent writer racing between
    /// the reads and the writes can leave a partial batch; those items
    /// converge on the winning mapping through the outcome handling in
    /// `commit`.
    pub async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
    ) -> Result<Vec<SnowflakeId>, IdError> {
        self.resolve_batch_inner(ids)
            .await
            .map_err(observe_rejection)
    }

    async fn resolve_batch_inner(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
    ) -> Result<Vec<SnowflakeId>, IdError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        for (object_id, _, _) in ids {
            crate::validate_object_id(object_id)?;
        }

        let items = dedupe_requests(ids)?;
        let lookup = items
            .iter()
            .map(|item| (item.object_id.clone(), item.entity_kind))
            .collect::<Vec<_>>();
        let existing = self.store.find_by_object_batch(&lookup).await?;

        let mut resolved: HashMap<ObjectKey, SnowflakeId> = HashMap::with_capacity(items.len());
        let mut missing: Vec<&RequestItem> = Vec::new();
        for (item, current) in items.iter().zip(existing) {
            match current {
                Some(mapping) => {
                    resolved.insert(item.key(), accept_existing(&mapping, item.provided)?);
                }
                None => missing.push(item),
            }
        }
        if !missing.is_empty() {
            self.resolve_missing(&missing, &mut resolved).await?;
        }

        ids.iter()
            .map(|(object_id, entity_kind, _)| {
                resolved
                    .get(&(*entity_kind, object_id.clone()))
                    .copied()
                    .ok_or_else(|| {
                        IdError::Redis(format!("{entity_kind:?} {object_id} was not resolved"))
                    })
            })
            .collect()
    }

    async fn resolve_missing<'a>(
        &self,
        missing: &[&'a RequestItem],
        resolved: &mut HashMap<ObjectKey, SnowflakeId>,
    ) -> Result<(), IdError> {
        let (provided, unassigned): (Vec<&'a RequestItem>, Vec<&'a RequestItem>) = missing
            .iter()
            .copied()
            .partition(|item| item.provided.is_some());
        if !unassigned.is_empty() {
            if !self.allow_allocation {
                return Err(allocation_disabled(&unassigned));
            }
            for item in &unassigned {
                allocation_timestamp_ms(&item.object_id)?;
            }
        }
        self.check_provided_available(&provided).await?;

        let mut pending = provided
            .iter()
            .filter_map(|&item| {
                item.provided
                    .map(|snowflake_id| PendingWrite { item, snowflake_id })
            })
            .collect::<Vec<_>>();
        pending.extend(self.allocate(&unassigned).await?);
        self.commit(pending, resolved).await
    }

    /// A provided Snowflake may only be registered while it is unbound or
    /// already bound to the same object (orphan repair).
    async fn check_provided_available(&self, provided: &[&RequestItem]) -> Result<(), IdError> {
        if provided.is_empty() {
            return Ok(());
        }
        let ids = provided
            .iter()
            .filter_map(|item| item.provided)
            .collect::<Vec<_>>();
        let holders = self.store.find_by_snowflake_batch(&ids).await?;
        for (item, holder) in provided.iter().zip(holders) {
            if let Some(holder) = holder {
                if holder.object_id != item.object_id || holder.entity_kind != item.entity_kind {
                    return Err(snowflake_taken(item, holder.snowflake_id, Some(holder)));
                }
            }
        }
        Ok(())
    }

    /// Write the pending mappings and settle every outcome. Allocated ids
    /// that turn out to be bound already are re-drawn up to
    /// [`MAX_ALLOCATION_ATTEMPTS`] times.
    async fn commit<'a>(
        &self,
        mut pending: Vec<PendingWrite<'a>>,
        resolved: &mut HashMap<ObjectKey, SnowflakeId>,
    ) -> Result<(), IdError> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let mappings = pending
                .iter()
                .map(PendingWrite::mapping)
                .collect::<Vec<_>>();
            let outcomes = self.store.insert_if_absent_batch(&mappings).await?;
            if outcomes.len() != pending.len() {
                return Err(IdError::Redis(
                    "mapping write returned a mismatched outcome count".to_string(),
                ));
            }

            let mut lost_object: Vec<&PendingWrite<'a>> = Vec::new();
            let mut collided: Vec<&'a RequestItem> = Vec::new();
            let mut last_collision: Option<&PendingWrite<'a>> = None;
            let mut floors: Vec<SequenceFloor> = Vec::new();
            for (write, outcome) in pending.iter().zip(outcomes) {
                match outcome {
                    InsertOutcome::Inserted => {
                        resolved.insert(write.item.key(), write.snowflake_id);
                        self.record_insert(write, &mut floors);
                    }
                    InsertOutcome::AlreadyPresent => {
                        resolved.insert(write.item.key(), write.snowflake_id);
                    }
                    InsertOutcome::ObjectConflict => lost_object.push(write),
                    InsertOutcome::SnowflakeConflict if write.is_provided() => {
                        return Err(self.snowflake_taken_now(write).await);
                    }
                    InsertOutcome::SnowflakeConflict => {
                        collided.push(write.item);
                        last_collision = Some(write);
                    }
                }
            }
            if !floors.is_empty() {
                self.store
                    .observe_sequence_batch(&merge_floors(floors))
                    .await?;
            }
            self.adopt_concurrent_winners(&lost_object, resolved)
                .await?;

            let Some(last_collision) = last_collision else {
                return Ok(());
            };
            if attempts >= MAX_ALLOCATION_ATTEMPTS {
                return Err(self.snowflake_taken_now(last_collision).await);
            }
            log::warn!(
                "allocated Snowflake {} for {:?} {} is already bound; redrawing ({} of {MAX_ALLOCATION_ATTEMPTS} attempts used)",
                last_collision.snowflake_id,
                last_collision.item.entity_kind,
                last_collision.item.object_id,
                attempts
            );

            // Another writer may have bound the object in the meantime;
            // adopt its id instead of drawing a new sequence value.
            let lookup = collided
                .iter()
                .map(|item| (item.object_id.clone(), item.entity_kind))
                .collect::<Vec<_>>();
            let current = self.store.find_by_object_batch(&lookup).await?;
            let mut still_missing: Vec<&'a RequestItem> = Vec::new();
            for (item, current) in collided.into_iter().zip(current) {
                match current {
                    Some(mapping) => {
                        resolved.insert(item.key(), mapping.snowflake_id);
                    }
                    None => still_missing.push(item),
                }
            }
            if still_missing.is_empty() {
                return Ok(());
            }
            pending = self.allocate(&still_missing).await?;
        }
    }

    /// An object conflict means a concurrent writer bound the object between
    /// our read and our write; re-read and accept its mapping (or reject a
    /// provided id that contradicts it).
    async fn adopt_concurrent_winners(
        &self,
        lost: &[&PendingWrite<'_>],
        resolved: &mut HashMap<ObjectKey, SnowflakeId>,
    ) -> Result<(), IdError> {
        if lost.is_empty() {
            return Ok(());
        }
        let lookup = lost
            .iter()
            .map(|write| (write.item.object_id.clone(), write.item.entity_kind))
            .collect::<Vec<_>>();
        let current = self.store.find_by_object_batch(&lookup).await?;
        for (write, current) in lost.iter().zip(current) {
            match current {
                Some(mapping) => {
                    resolved.insert(
                        write.item.key(),
                        accept_existing(&mapping, write.item.provided)?,
                    );
                }
                None => {
                    return Err(IdError::MappingConflict {
                        object_id: write.item.object_id.clone(),
                        entity_kind: write.item.entity_kind,
                        snowflake_id: write.snowflake_id,
                    })
                }
            }
        }
        Ok(())
    }

    fn record_insert(&self, write: &PendingWrite<'_>, floors: &mut Vec<SequenceFloor>) {
        if write.is_provided() {
            metrics().record_provided_import();
            log::info!(
                "provided import: {:?} {} -> {}",
                write.item.entity_kind,
                write.item.object_id,
                write.snowflake_id
            );
            if let Some(floor) = sequence_floor(write.snowflake_id, self.worker_id) {
                floors.push(floor);
            }
        } else {
            metrics().record_allocation();
        }
    }

    async fn snowflake_taken_now(&self, write: &PendingWrite<'_>) -> IdError {
        let holder = self
            .store
            .find_by_snowflake(write.snowflake_id)
            .await
            .ok()
            .flatten();
        snowflake_taken(write.item, write.snowflake_id, holder)
    }

    /// Reserve sequence values per `(worker, second)` and turn them into
    /// Snowflakes for the given items.
    async fn allocate<'a>(
        &self,
        items: &[&'a RequestItem],
    ) -> Result<Vec<PendingWrite<'a>>, IdError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let mut groups: BTreeMap<u64, Vec<&'a RequestItem>> = BTreeMap::new();
        for &item in items {
            groups
                .entry(allocation_timestamp_ms(&item.object_id)?)
                .or_default()
                .push(item);
        }
        let reservations = groups
            .iter()
            .map(|(timestamp_ms, group)| SequenceReservation {
                worker_id: self.worker_id,
                object_timestamp_ms: *timestamp_ms,
                count: group.len() as u64,
            })
            .collect::<Vec<_>>();
        let uppers = self.store.next_sequence_batch(&reservations).await?;
        if uppers.len() != reservations.len() {
            return Err(IdError::Redis(
                "sequence reservation returned a mismatched count".to_string(),
            ));
        }

        let mut writes = Vec::with_capacity(items.len());
        for ((timestamp_ms, group), upper) in groups.into_iter().zip(uppers) {
            let count = group.len() as u64;
            if upper < count || upper > SEQUENCES_PER_SECOND {
                return Err(IdError::SecondExhausted(timestamp_ms));
            }
            let first = upper - count + 1;
            for (offset, item) in group.into_iter().enumerate() {
                writes.push(PendingWrite {
                    item,
                    snowflake_id: self.snowflake_for(timestamp_ms, first + offset as u64)?,
                });
            }
        }
        Ok(writes)
    }

    /// Sequence value `sequence` (1-based) within the second of
    /// `object_timestamp_ms`: the first 4096 values share the first
    /// millisecond, the next 4096 the second one, and so on.
    fn snowflake_for(
        &self,
        object_timestamp_ms: u64,
        sequence: u64,
    ) -> Result<SnowflakeId, IdError> {
        let offset = sequence - 1;
        let millis = object_timestamp_ms + offset / (MAX_SEQUENCE + 1);
        SnowflakeId::new(
            ((millis - SNOWFLAKE_EPOCH_MS) << (WORKER_BITS + SEQUENCE_BITS))
                | (self.worker_id << SEQUENCE_BITS)
                | (offset & MAX_SEQUENCE),
        )
    }

    pub async fn reverse_one(
        &self,
        snowflake_id: SnowflakeId,
        expected_kind: EntityKind,
    ) -> Result<Mapping, IdError> {
        let mut mappings = self.reverse_batch(&[(snowflake_id, expected_kind)]).await?;
        mappings
            .pop()
            .ok_or(IdError::UnknownSnowflake(snowflake_id.get()))
    }

    pub async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> Result<Vec<Mapping>, IdError> {
        let snowflakes = ids.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let mappings = self.store.find_by_snowflake_batch(&snowflakes).await?;
        ids.iter()
            .zip(mappings)
            .map(|((snowflake_id, expected_kind), mapping)| {
                let mapping = mapping.ok_or(IdError::UnknownSnowflake(snowflake_id.get()))?;
                check_kind(mapping, *snowflake_id, *expected_kind)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(observe_rejection)
    }

    pub async fn check_ready(&self) -> Result<(), IdError> {
        self.store.check_ready().await
    }

    pub fn cache_sizes(&self) -> (usize, usize) {
        self.store.cache_sizes()
    }
}

/// Collapse duplicates of the same `(entity_kind, object_id)` and reject
/// in-batch contradictions: one object with two provided ids, or one
/// provided id for two objects.
fn dedupe_requests(
    ids: &[(String, EntityKind, Option<SnowflakeId>)],
) -> Result<Vec<RequestItem>, IdError> {
    let mut items: Vec<RequestItem> = Vec::with_capacity(ids.len());
    let mut index_by_key: HashMap<ObjectKey, usize> = HashMap::with_capacity(ids.len());
    let mut index_by_provided: HashMap<u64, usize> = HashMap::new();
    for (object_id, entity_kind, provided) in ids {
        let key = (*entity_kind, object_id.clone());
        let index = match index_by_key.get(&key) {
            Some(&index) => {
                if let Some(provided) = provided {
                    let current = items[index].provided;
                    match current {
                        None => items[index].provided = Some(*provided),
                        Some(existing) if existing != *provided => {
                            return Err(IdError::MappingConflict {
                                object_id: object_id.clone(),
                                entity_kind: *entity_kind,
                                snowflake_id: *provided,
                            })
                        }
                        Some(_) => {}
                    }
                }
                index
            }
            None => {
                items.push(RequestItem {
                    object_id: object_id.clone(),
                    entity_kind: *entity_kind,
                    provided: *provided,
                });
                index_by_key.insert(key, items.len() - 1);
                items.len() - 1
            }
        };
        if let Some(provided) = provided {
            match index_by_provided.get(&provided.get()) {
                Some(&other) if other != index => {
                    return Err(IdError::SnowflakeTaken {
                        snowflake_id: *provided,
                        object_id: object_id.clone(),
                        entity_kind: *entity_kind,
                        holder: Some((items[other].entity_kind, items[other].object_id.clone())),
                    })
                }
                _ => {
                    index_by_provided.insert(provided.get(), index);
                }
            }
        }
    }
    Ok(items)
}

/// An existing mapping is accepted unless the caller provided a different
/// Snowflake. The lookup that produced `existing` was keyed by the
/// request's own `(entity_kind, object_id)`, so those never differ here.
fn accept_existing(
    existing: &Mapping,
    provided: Option<SnowflakeId>,
) -> Result<SnowflakeId, IdError> {
    if let Some(provided) = provided {
        if provided != existing.snowflake_id {
            return Err(IdError::MappingConflict {
                object_id: existing.object_id.clone(),
                entity_kind: existing.entity_kind,
                snowflake_id: provided,
            });
        }
    }
    Ok(existing.snowflake_id)
}

fn check_kind(
    mapping: Mapping,
    snowflake_id: SnowflakeId,
    expected_kind: EntityKind,
) -> Result<Mapping, IdError> {
    if mapping.entity_kind != expected_kind {
        return Err(IdError::EntityKindMismatch {
            snowflake_id,
            expected: expected_kind,
            actual: mapping.entity_kind,
        });
    }
    Ok(mapping)
}

fn snowflake_taken(
    item: &RequestItem,
    snowflake_id: SnowflakeId,
    holder: Option<Mapping>,
) -> IdError {
    IdError::SnowflakeTaken {
        snowflake_id,
        object_id: item.object_id.clone(),
        entity_kind: item.entity_kind,
        holder: holder.map(|mapping| (mapping.entity_kind, mapping.object_id)),
    }
}

/// Summary of every unknown id when allocation is disabled: the count and
/// the first five ids, so a caller can find the missing imports.
fn allocation_disabled(items: &[&RequestItem]) -> IdError {
    IdError::AllocationDisabled(unknown_ids_summary(
        items
            .iter()
            .map(|item| (item.entity_kind, item.object_id.as_str())),
    ))
}

fn unknown_ids_summary<'a>(items: impl Iterator<Item = (EntityKind, &'a str)>) -> String {
    const PREVIEW: usize = 5;
    let items = items.collect::<Vec<_>>();
    let preview = items
        .iter()
        .take(PREVIEW)
        .map(|(kind, object_id)| format!("{kind:?} {object_id}"))
        .collect::<Vec<_>>()
        .join(", ");
    let summary = if items.len() == 1 {
        preview
    } else if items.len() <= PREVIEW {
        format!("{} ids: {preview}", items.len())
    } else {
        format!("{} ids, first {PREVIEW}: {preview}", items.len())
    };
    summary
}

fn allocation_timestamp_ms(object_id: &str) -> Result<u64, IdError> {
    let timestamp_ms = crate::object_id_timestamp_ms(object_id)?;
    if timestamp_ms < SNOWFLAKE_EPOCH_MS {
        return Err(IdError::BeforeSnowflakeEpoch(timestamp_ms));
    }
    Ok(timestamp_ms)
}

/// The `(worker, second)` counter position occupied by an imported
/// Snowflake, when it carries this service's worker id. The second is the
/// Snowflake's own millisecond timestamp divided by 1000, the same unit
/// `next_sequence` derives from an ObjectId's creation second.
fn sequence_floor(snowflake_id: SnowflakeId, worker_id: u64) -> Option<SequenceFloor> {
    let raw = snowflake_id.get();
    if (raw >> SEQUENCE_BITS) & MAX_WORKER_ID != worker_id {
        return None;
    }
    let millis = (raw >> (WORKER_BITS + SEQUENCE_BITS)) + SNOWFLAKE_EPOCH_MS;
    let second = millis / 1_000;
    let offset = (millis - second * 1_000) * (MAX_SEQUENCE + 1) + (raw & MAX_SEQUENCE);
    Some(SequenceFloor {
        worker_id,
        second,
        min_next: offset + 1,
    })
}

/// One floor per `(worker, second)`: the highest requested value wins.
fn merge_floors(floors: Vec<SequenceFloor>) -> Vec<SequenceFloor> {
    let mut merged: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    for floor in floors {
        let entry = merged.entry((floor.worker_id, floor.second)).or_default();
        *entry = (*entry).max(floor.min_next);
    }
    merged
        .into_iter()
        .map(|((worker_id, second), min_next)| SequenceFloor {
            worker_id,
            second,
            min_next,
        })
        .collect()
}

/// Conflicts are expected outcomes, but each one is worth a warning and a
/// counter so an import that keeps colliding is visible.
fn observe_rejection(error: IdError) -> IdError {
    let kind = match &error {
        IdError::MappingConflict { .. } => "mapping",
        IdError::SnowflakeTaken { .. } => "snowflake_taken",
        IdError::EntityKindMismatch { .. } => "entity_kind",
        _ => return error,
    };
    metrics().record_conflict(kind);
    log::warn!("{error}");
    error
}

fn lock_ignoring_poison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone)]
enum RedisConnection {
    Single(Box<ConnectionManager>),
    Cluster(Box<redis::cluster_async::ClusterConnection>),
}

/// A failed Redis round trip, kept as the raw error until the caller has
/// decided whether it can recover (NOSCRIPT after a script flush).
enum RedisFailure {
    Timeout,
    Error(redis::RedisError),
}

impl RedisFailure {
    fn is_missing_script(&self) -> bool {
        matches!(self, Self::Error(error) if error.kind() == ErrorKind::NoScriptError)
    }

    fn into_id_error(self, op: &str) -> IdError {
        metrics().record_redis_error();
        let message = match self {
            Self::Timeout => format!("{op} timed out"),
            Self::Error(error) => format!("{op} failed: {error}"),
        };
        log::error!("redis {message}");
        IdError::Redis(message)
    }
}

impl RedisConnection {
    async fn connect(
        single_url: Option<&str>,
        cluster_urls: Option<&[String]>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, IdError> {
        let connection = match (single_url, cluster_urls) {
            (Some(_), Some(_)) => {
                return Err(IdError::Redis(
                    "configure either Redis URL or cluster URLs, not both".to_string(),
                ))
            }
            (None, None) => return Err(IdError::Redis("no Redis endpoint configured".to_string())),
            (Some(url), None) => {
                let client = redis::Client::open(url)
                    .map_err(|error| IdError::Redis(format!("invalid Redis URL: {error}")))?;
                let config = ConnectionManagerConfig::new()
                    .set_number_of_retries(0)
                    .set_connection_timeout(connect_timeout)
                    .set_response_timeout(request_timeout);
                let connection = tokio::time::timeout(
                    connect_timeout,
                    ConnectionManager::new_with_config(client, config),
                )
                .await
                .map_err(|_| IdError::Redis("Redis connection timed out".to_string()))?
                .map_err(|error| IdError::Redis(format!("Redis connection failed: {error}")))?;
                Self::Single(Box::new(connection))
            }
            (None, Some(urls)) => {
                let client = ClusterClientBuilder::new(urls.to_vec())
                    .connection_timeout(connect_timeout)
                    .response_timeout(request_timeout)
                    .build()
                    .map_err(|error| {
                        IdError::Redis(format!("invalid Redis cluster URLs: {error}"))
                    })?;
                let connection =
                    tokio::time::timeout(connect_timeout, client.get_async_connection())
                        .await
                        .map_err(|_| {
                            IdError::Redis("Redis cluster connection timed out".to_string())
                        })?
                        .map_err(|error| {
                            IdError::Redis(format!("Redis cluster connection failed: {error}"))
                        })?;
                Self::Cluster(Box::new(connection))
            }
        };
        let pong: String = connection
            .run_command(&redis::cmd("PING"), request_timeout)
            .await
            .map_err(|failure| failure.into_id_error("PING"))?;
        if pong != "PONG" {
            return Err(IdError::Redis(format!(
                "Redis health check returned {pong}"
            )));
        }
        Ok(connection)
    }

    fn is_cluster(&self) -> bool {
        matches!(self, Self::Cluster(_))
    }

    async fn run_command<T: FromRedisValue>(
        &self,
        command: &redis::Cmd,
        timeout: Duration,
    ) -> Result<T, RedisFailure> {
        let mut connection = self.clone();
        match tokio::time::timeout(timeout, command.query_async(&mut connection)).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(RedisFailure::Error(error)),
            Err(_) => Err(RedisFailure::Timeout),
        }
    }

    async fn run_pipeline<T: FromRedisValue>(
        &self,
        pipeline: &redis::Pipeline,
        timeout: Duration,
    ) -> Result<T, RedisFailure> {
        let mut connection = self.clone();
        match tokio::time::timeout(timeout, pipeline.query_async(&mut connection)).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(RedisFailure::Error(error)),
            Err(_) => Err(RedisFailure::Timeout),
        }
    }
}

impl redis::aio::ConnectionLike for RedisConnection {
    fn req_packed_command<'a>(
        &'a mut self,
        command: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        match self {
            Self::Single(connection) => connection.req_packed_command(command),
            Self::Cluster(connection) => connection.req_packed_command(command),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        match self {
            Self::Single(connection) => connection.req_packed_commands(pipeline, offset, count),
            Self::Cluster(connection) => connection.req_packed_commands(pipeline, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Single(connection) => connection.get_db(),
            Self::Cluster(connection) => connection.get_db(),
        }
    }
}

/// A command together with the cluster slot of its (single) key, so a batch
/// can be split into pipelines that a cluster will accept.
struct KeyedCommand {
    slot: u16,
    command: redis::Cmd,
}

fn keyed(key: &str, command: redis::Cmd) -> KeyedCommand {
    KeyedCommand {
        slot: redis::cluster_routing::get_slot(key.as_bytes()),
        command,
    }
}

fn get_command(key: &str) -> KeyedCommand {
    let mut command = redis::cmd("GET");
    command.arg(key);
    keyed(key, command)
}

/// `EVALSHA` for a preloaded script over exactly one key.
fn script_command(script: &redis::Script, key: &str, args: &[&str]) -> KeyedCommand {
    let mut command = redis::cmd("EVALSHA");
    command.arg(script.get_hash()).arg(1).arg(key);
    for arg in args {
        command.arg(*arg);
    }
    keyed(key, command)
}

struct RedisMappingStore {
    connection: RedisConnection,
    key_prefix: String,
    request_timeout: Duration,
    object_cache: Mutex<BoundedCache<ObjectKey, SnowflakeId>>,
    snowflake_cache: Mutex<BoundedCache<u64, Mapping>>,
}

impl RedisMappingStore {
    async fn connect(
        single_url: Option<&str>,
        cluster_urls: Option<&[String]>,
        prefix: impl Into<String>,
        cache_capacity: usize,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, IdError> {
        if cache_capacity == 0 {
            return Err(IdError::Redis(
                "cache capacity must be positive".to_string(),
            ));
        }
        let prefix = prefix.into();
        if prefix.trim().is_empty() {
            return Err(IdError::Redis(
                "Redis key prefix must not be empty".to_string(),
            ));
        }
        if prefix.contains('{') || prefix.contains('}') {
            return Err(IdError::Redis(
                "Redis key prefix must not contain hash-tag braces".to_string(),
            ));
        }
        let connection =
            RedisConnection::connect(single_url, cluster_urls, connect_timeout, request_timeout)
                .await?;
        let store = Self {
            connection,
            key_prefix: prefix,
            request_timeout,
            object_cache: Mutex::new(BoundedCache::new(cache_capacity, "object")),
            snowflake_cache: Mutex::new(BoundedCache::new(cache_capacity, "snowflake")),
        };
        store.load_scripts().await?;
        store.ensure_metadata().await?;
        Ok(store)
    }

    /// `SCRIPT LOAD` every script so pipelines can use `EVALSHA` from the
    /// first request. In cluster mode redis-rs routes `SCRIPT LOAD` to all
    /// nodes.
    async fn load_scripts(&self) -> Result<(), IdError> {
        for script in all_scripts() {
            let mut connection = self.connection.clone();
            let invocation = script.prepare_invoke();
            let load = invocation.load_async(&mut connection);
            match tokio::time::timeout(self.request_timeout, load).await {
                Ok(Ok(_hash)) => {}
                Ok(Err(error)) => {
                    return Err(RedisFailure::Error(error).into_id_error("SCRIPT LOAD"))
                }
                Err(_) => return Err(RedisFailure::Timeout.into_id_error("SCRIPT LOAD")),
            }
        }
        Ok(())
    }

    /// Run one pipeline; a `NOSCRIPT` reply (scripts flushed or a fresh
    /// cluster node) reloads the scripts and retries once.
    async fn run_pipeline<T: FromRedisValue>(
        &self,
        op: &'static str,
        pipeline: &redis::Pipeline,
    ) -> Result<T, IdError> {
        match self
            .connection
            .run_pipeline(pipeline, self.request_timeout)
            .await
        {
            Ok(value) => Ok(value),
            Err(failure) if failure.is_missing_script() => {
                log::warn!(
                    "redis {op}: scripts are not loaded (NOSCRIPT); reloading and retrying once"
                );
                self.load_scripts().await?;
                self.connection
                    .run_pipeline(pipeline, self.request_timeout)
                    .await
                    .map_err(|failure| failure.into_id_error(op))
            }
            Err(failure) => Err(failure.into_id_error(op)),
        }
    }

    /// Execute single-key commands and return their replies in input order.
    ///
    /// A single endpoint takes everything in one pipeline. A cluster routes
    /// a pipeline by its first key only, so commands are grouped by slot,
    /// one pipeline per group, and the groups run concurrently.
    async fn run_keyed<T>(
        &self,
        op: &'static str,
        commands: Vec<KeyedCommand>,
    ) -> Result<Vec<T>, IdError>
    where
        T: FromRedisValue + Send,
    {
        if commands.is_empty() {
            return Ok(Vec::new());
        }
        let expected = commands.len();
        let values: Vec<T> = if self.connection.is_cluster() {
            let mut groups: HashMap<u16, Vec<usize>> = HashMap::new();
            for (index, command) in commands.iter().enumerate() {
                groups.entry(command.slot).or_default().push(index);
            }
            let runs = groups.into_values().map(|indices| {
                let mut pipeline = redis::pipe();
                for &index in &indices {
                    pipeline.add_command(commands[index].command.clone());
                }
                async move {
                    let values: Vec<T> = self.run_pipeline(op, &pipeline).await?;
                    Ok::<_, IdError>((indices, values))
                }
            });
            let mut ordered: Vec<Option<T>> =
                std::iter::repeat_with(|| None).take(expected).collect();
            for (indices, values) in try_join_all(runs).await? {
                if values.len() != indices.len() {
                    return Err(short_reply(op));
                }
                for (index, value) in indices.into_iter().zip(values) {
                    ordered[index] = Some(value);
                }
            }
            ordered
                .into_iter()
                .map(|value| value.ok_or_else(|| short_reply(op)))
                .collect::<Result<Vec<T>, IdError>>()?
        } else {
            let mut pipeline = redis::pipe();
            for command in commands {
                pipeline.add_command(command.command);
            }
            self.run_pipeline(op, &pipeline).await?
        };
        if values.len() != expected {
            return Err(short_reply(op));
        }
        Ok(values)
    }

    fn metadata_key(&self, name: &str) -> String {
        format!("{}:metadata:{name}", self.key_prefix)
    }

    /// Startup only: create the metadata on first use, then verify it.
    async fn ensure_metadata(&self) -> Result<(), IdError> {
        let version = self
            .ensure_metadata_value("mapping_version", &MAPPING_VERSION.to_string())
            .await?;
        check_mapping_version(version.as_deref())?;
        let schema = self
            .ensure_metadata_value("storage_schema", STORAGE_SCHEMA)
            .await?;
        check_storage_schema(schema.as_deref())?;
        Ok(())
    }

    /// `SETNX` + `GET` in one transaction: whoever starts first writes the
    /// value, everyone reads back what is actually stored.
    async fn ensure_metadata_value(
        &self,
        name: &str,
        expected: &str,
    ) -> Result<Option<String>, IdError> {
        let key = self.metadata_key(name);
        let mut pipeline = redis::pipe();
        pipeline
            .atomic()
            .cmd("SETNX")
            .arg(&key)
            .arg(expected)
            .cmd("GET")
            .arg(&key);
        let (_created, value): (bool, Option<String>) =
            self.run_pipeline("metadata SETNX", &pipeline).await?;
        Ok(value)
    }

    fn object_key(&self, entity_kind: EntityKind, object_id: &str) -> String {
        let shard = mapping_shard(&format!("{}:{}", entity_kind_name(entity_kind), object_id));
        format!(
            "{}:object:{{{shard:03}}}:{}:{}",
            self.key_prefix,
            entity_kind_name(entity_kind),
            object_id
        )
    }

    fn snowflake_key(&self, snowflake_id: SnowflakeId) -> String {
        let shard = mapping_shard(&snowflake_id.get().to_string());
        format!(
            "{}:snowflake:{{{shard:03}}}:{}",
            self.key_prefix,
            snowflake_id.get()
        )
    }

    fn sequence_key(&self, worker_id: u64, second: u64) -> String {
        format!("{}:sequence:{worker_id}:{second}", self.key_prefix)
    }

    fn cache_mapping(&self, mapping: &Mapping) {
        let expires = match mapping.entity_kind {
            EntityKind::User => None,
            EntityKind::Post => object_id_timestamp_secs(&mapping.object_id)
                .ok()
                .map(|created| created.saturating_add(POST_CACHE_TTL_SECS)),
        };
        if expires.is_some_and(|expiry| expiry <= crate::now_secs()) {
            return;
        }
        lock_ignoring_poison(&self.object_cache).insert(
            (mapping.entity_kind, mapping.object_id.clone()),
            mapping.snowflake_id,
            expires,
        );
        lock_ignoring_poison(&self.snowflake_cache).insert(
            mapping.snowflake_id.get(),
            mapping.clone(),
            expires,
        );
    }
}

#[async_trait]
impl MappingStore for RedisMappingStore {
    async fn find_by_object_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> Result<Vec<Option<Mapping>>, IdError> {
        let mut result = vec![None; ids.len()];
        let mut pending = Vec::new();
        let mut commands = Vec::new();
        {
            let mut cache = lock_ignoring_poison(&self.object_cache);
            for (index, (object_id, entity_kind)) in ids.iter().enumerate() {
                match cache.get(&(*entity_kind, object_id.clone())) {
                    Some(snowflake_id) => {
                        result[index] = Some(mapping_of(object_id, *entity_kind, snowflake_id));
                    }
                    None => {
                        pending.push(index);
                        commands.push(get_command(&self.object_key(*entity_kind, object_id)));
                    }
                }
            }
        }
        let values: Vec<Option<String>> = self.run_keyed("GET object", commands).await?;
        for (index, raw) in pending.into_iter().zip(values) {
            if let Some(raw) = raw {
                let (object_id, entity_kind) = &ids[index];
                let snowflake_id =
                    parse_snowflake(&raw, || self.object_key(*entity_kind, object_id))?;
                let mapping = mapping_of(object_id, *entity_kind, snowflake_id);
                self.cache_mapping(&mapping);
                result[index] = Some(mapping);
            }
        }
        Ok(result)
    }

    async fn find_by_snowflake_batch(
        &self,
        ids: &[SnowflakeId],
    ) -> Result<Vec<Option<Mapping>>, IdError> {
        let mut result = vec![None; ids.len()];
        let mut pending = Vec::new();
        let mut commands = Vec::new();
        {
            let mut cache = lock_ignoring_poison(&self.snowflake_cache);
            for (index, snowflake_id) in ids.iter().enumerate() {
                match cache.get(&snowflake_id.get()) {
                    Some(mapping) => result[index] = Some(mapping),
                    None => {
                        pending.push(index);
                        commands.push(get_command(&self.snowflake_key(*snowflake_id)));
                    }
                }
            }
        }
        let reverse_values: Vec<Option<String>> = self.run_keyed("GET snowflake", commands).await?;

        // Fail closed: a reverse entry only counts when the forward entry
        // points back at the same Snowflake. A two-phase write that died
        // between its phases leaves a reverse-only orphan; reporting it
        // would break resolve(reverse(x)) == x.
        let mut candidates = Vec::new();
        let mut forward_commands = Vec::new();
        for (index, raw) in pending.into_iter().zip(reverse_values) {
            if let Some(raw) = raw {
                let snowflake_id = ids[index];
                let mapping =
                    parse_reverse_value(&raw, snowflake_id, || self.snowflake_key(snowflake_id))?;
                forward_commands.push(get_command(
                    &self.object_key(mapping.entity_kind, &mapping.object_id),
                ));
                candidates.push((index, mapping));
            }
        }
        let forward_values: Vec<Option<String>> =
            self.run_keyed("GET object", forward_commands).await?;
        for ((index, mapping), forward) in candidates.into_iter().zip(forward_values) {
            let confirmed = forward
                .as_deref()
                .and_then(|raw| raw.parse::<u64>().ok())
                .is_some_and(|value| value == mapping.snowflake_id.get());
            if confirmed {
                self.cache_mapping(&mapping);
                result[index] = Some(mapping);
            } else {
                metrics().record_orphan_reverse_mapping();
                log::warn!(
                    "orphan reverse mapping: snowflake {} -> {:?} {} has no matching forward entry (forward={:?}); treated as absent",
                    mapping.snowflake_id,
                    mapping.entity_kind,
                    mapping.object_id,
                    forward
                );
            }
        }
        Ok(result)
    }

    async fn insert_if_absent_batch(
        &self,
        mappings: &[Mapping],
    ) -> Result<Vec<InsertOutcome>, IdError> {
        if mappings.is_empty() {
            return Ok(Vec::new());
        }
        let reverse_keys = mappings
            .iter()
            .map(|mapping| self.snowflake_key(mapping.snowflake_id))
            .collect::<Vec<_>>();
        let reverse_values = mappings.iter().map(reverse_value).collect::<Vec<_>>();
        let object_keys = mappings
            .iter()
            .map(|mapping| self.object_key(mapping.entity_kind, &mapping.object_id))
            .collect::<Vec<_>>();
        let object_values = mappings
            .iter()
            .map(|mapping| mapping.snowflake_id.get().to_string())
            .collect::<Vec<_>>();

        // Round 1: reserve every reverse entry.
        let reverse_status: Vec<i64> = self
            .run_keyed(
                "reserve reverse mapping",
                (0..mappings.len())
                    .map(|index| {
                        script_command(
                            &RESERVE_SCRIPT,
                            &reverse_keys[index],
                            &[reverse_values[index].as_str()],
                        )
                    })
                    .collect(),
            )
            .await?;

        // Round 2: reserve the forward entry where the reverse entry is ours
        // (just created, or already holding the identical value).
        let mut outcomes = vec![InsertOutcome::SnowflakeConflict; mappings.len()];
        let mut forward_indices = Vec::new();
        let mut forward_commands = Vec::new();
        for (index, status) in reverse_status.iter().enumerate() {
            match status {
                1 | 0 => {
                    forward_indices.push(index);
                    forward_commands.push(script_command(
                        &RESERVE_SCRIPT,
                        &object_keys[index],
                        &[object_values[index].as_str()],
                    ));
                }
                -1 => {}
                other => return Err(unexpected_status("reverse", *other)),
            }
        }
        let forward_status: Vec<i64> = self
            .run_keyed("reserve object mapping", forward_commands)
            .await?;

        // Round 3: drop the reverse entries this call created for objects
        // that turned out to be bound elsewhere.
        let mut rollback = Vec::new();
        for (index, status) in forward_indices.into_iter().zip(forward_status) {
            match status {
                1 => {
                    outcomes[index] = InsertOutcome::Inserted;
                    if reverse_status[index] == 0 {
                        log::info!(
                            "repaired orphan reverse mapping: snowflake {} -> {:?} {}",
                            mappings[index].snowflake_id,
                            mappings[index].entity_kind,
                            mappings[index].object_id
                        );
                    }
                    self.cache_mapping(&mappings[index]);
                }
                0 => {
                    outcomes[index] = InsertOutcome::AlreadyPresent;
                    self.cache_mapping(&mappings[index]);
                }
                -1 => {
                    outcomes[index] = InsertOutcome::ObjectConflict;
                    if reverse_status[index] == 1 {
                        rollback.push(script_command(
                            &DELETE_IF_VALUE_SCRIPT,
                            &reverse_keys[index],
                            &[reverse_values[index].as_str()],
                        ));
                    }
                }
                other => return Err(unexpected_status("object", other)),
            }
        }
        let _: Vec<i64> = self.run_keyed("delete reverse mapping", rollback).await?;
        Ok(outcomes)
    }

    async fn next_sequence_batch(
        &self,
        reservations: &[SequenceReservation],
    ) -> Result<Vec<u64>, IdError> {
        let ttl = SEQUENCE_TTL_SECS.to_string();
        let commands = reservations
            .iter()
            .map(|reservation| {
                script_command(
                    &SEQUENCE_SCRIPT,
                    &self.sequence_key(
                        reservation.worker_id,
                        reservation.object_timestamp_ms / 1_000,
                    ),
                    &[reservation.count.to_string().as_str(), ttl.as_str()],
                )
            })
            .collect();
        let values: Vec<i64> = self.run_keyed("INCRBY sequence", commands).await?;
        values
            .into_iter()
            .map(|value| {
                u64::try_from(value).map_err(|_| {
                    IdError::Redis("Redis allocation returned a negative sequence".to_string())
                })
            })
            .collect()
    }

    async fn observe_sequence_batch(&self, floors: &[SequenceFloor]) -> Result<(), IdError> {
        let ttl = SEQUENCE_TTL_SECS.to_string();
        let commands = floors
            .iter()
            .map(|floor| {
                script_command(
                    &SEQUENCE_FLOOR_SCRIPT,
                    &self.sequence_key(floor.worker_id, floor.second),
                    &[floor.min_next.to_string().as_str(), ttl.as_str()],
                )
            })
            .collect();
        let _: Vec<i64> = self.run_keyed("raise sequence floor", commands).await?;
        Ok(())
    }

    /// Read-only: readiness must not recreate metadata that went missing.
    async fn check_ready(&self) -> Result<(), IdError> {
        let values: Vec<Option<String>> = self
            .run_keyed(
                "GET metadata",
                vec![
                    get_command(&self.metadata_key("mapping_version")),
                    get_command(&self.metadata_key("storage_schema")),
                ],
            )
            .await?;
        check_mapping_version(values[0].as_deref())?;
        check_storage_schema(values[1].as_deref())?;
        Ok(())
    }

    fn cache_sizes(&self) -> (usize, usize) {
        (
            lock_ignoring_poison(&self.object_cache).len(),
            lock_ignoring_poison(&self.snowflake_cache).len(),
        )
    }
}

fn check_mapping_version(value: Option<&str>) -> Result<(), IdError> {
    let raw =
        value.ok_or_else(|| IdError::Redis("mapping version metadata is missing".to_string()))?;
    let actual = raw
        .parse::<u32>()
        .map_err(|_| IdError::Redis(format!("mapping version metadata is invalid: {raw:?}")))?;
    if actual != MAPPING_VERSION {
        return Err(IdError::MappingVersionMismatch {
            expected: MAPPING_VERSION,
            actual,
        });
    }
    Ok(())
}

fn check_storage_schema(value: Option<&str>) -> Result<(), IdError> {
    let actual =
        value.ok_or_else(|| IdError::Redis("storage schema metadata is missing".to_string()))?;
    if actual != STORAGE_SCHEMA {
        return Err(IdError::StorageSchemaMismatch {
            expected: STORAGE_SCHEMA.to_string(),
            actual: actual.to_string(),
        });
    }
    Ok(())
}

fn short_reply(op: &str) -> IdError {
    IdError::Redis(format!("{op} returned fewer replies than commands"))
}

fn unexpected_status(phase: &str, status: i64) -> IdError {
    IdError::Redis(format!("unexpected {phase} mapping result: {status}"))
}

#[derive(Clone)]
struct CacheEntry<V> {
    value: V,
    expires_at_secs: Option<u64>,
    generation: u64,
}

struct BoundedCache<K, V>
where
    K: Eq + std::hash::Hash + Clone,
    V: Clone,
{
    values: HashMap<K, CacheEntry<V>>,
    order: VecDeque<(K, u64)>,
    next_generation: u64,
    capacity: usize,
    /// Metrics label identifying the lookup direction (`object`/`snowflake`).
    direction: &'static str,
}

impl<K, V> BoundedCache<K, V>
where
    K: Eq + std::hash::Hash + Clone,
    V: Clone,
{
    fn new(capacity: usize, direction: &'static str) -> Self {
        Self {
            values: HashMap::new(),
            order: VecDeque::new(),
            next_generation: 0,
            capacity,
            direction,
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        let Some(entry) = self.values.get(key).cloned() else {
            metrics().record_cache_lookup(self.direction, "miss");
            return None;
        };
        if entry
            .expires_at_secs
            .is_some_and(|expiry| expiry <= crate::now_secs())
        {
            self.values.remove(key);
            metrics().record_cache_lookup(self.direction, "expired");
            return None;
        }
        metrics().record_cache_lookup(self.direction, "hit");
        self.next_generation = self.next_generation.wrapping_add(1);
        let generation = self.next_generation;
        if let Some(current) = self.values.get_mut(key) {
            current.generation = generation;
        }
        self.order.push_back((key.clone(), generation));
        self.evict();
        Some(entry.value)
    }

    fn insert(&mut self, key: K, value: V, expires_at_secs: Option<u64>) {
        self.next_generation = self.next_generation.wrapping_add(1);
        let generation = self.next_generation;
        self.values.insert(
            key.clone(),
            CacheEntry {
                value,
                expires_at_secs,
                generation,
            },
        );
        self.order.push_back((key, generation));
        self.evict();
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn evict(&mut self) {
        while self.values.len() > self.capacity {
            let Some((key, generation)) = self.order.pop_front() else {
                break;
            };
            if self
                .values
                .get(&key)
                .is_some_and(|entry| entry.generation == generation)
            {
                self.values.remove(&key);
                metrics().record_cache_eviction(self.direction);
            }
        }
        if self.order.len() > self.capacity.saturating_mul(2).saturating_add(1) {
            self.order.retain(|(key, generation)| {
                self.values
                    .get(key)
                    .is_some_and(|entry| entry.generation == *generation)
            });
        }
    }
}

fn entity_kind_name(kind: EntityKind) -> &'static str {
    match kind {
        EntityKind::User => "user",
        EntityKind::Post => "post",
    }
}

fn mapping_of(object_id: &str, entity_kind: EntityKind, snowflake_id: SnowflakeId) -> Mapping {
    Mapping {
        object_id: object_id.to_string(),
        entity_kind,
        snowflake_id,
        mapping_version: MAPPING_VERSION,
    }
}

/// CRC16/XMODEM of the value modulo the shard count; the shard becomes the
/// key's hash tag. Cluster slot routing is left to redis-rs (`get_slot`).
fn mapping_shard(value: &str) -> u16 {
    let mut crc = 0u16;
    for byte in value.bytes() {
        crc ^= u16::from(byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc % MAPPING_SHARD_COUNT
}

fn reverse_value(mapping: &Mapping) -> String {
    format!(
        "{}:{}",
        entity_kind_name(mapping.entity_kind),
        mapping.object_id
    )
}

fn parse_snowflake(value: &str, key: impl FnOnce() -> String) -> Result<SnowflakeId, IdError> {
    value
        .parse::<u64>()
        .ok()
        .and_then(|raw| SnowflakeId::new(raw).ok())
        .ok_or_else(|| IdError::CorruptRecord {
            key: key(),
            reason: format!("{value:?} is not a SnowflakeId"),
        })
}

fn parse_reverse_value(
    value: &str,
    snowflake_id: SnowflakeId,
    key: impl FnOnce() -> String,
) -> Result<Mapping, IdError> {
    let corrupt = |reason: String| IdError::CorruptRecord { key: key(), reason };
    let Some((kind, object_id)) = value.split_once(':') else {
        return Err(corrupt(format!("{value:?} is not a reverse mapping")));
    };
    let entity_kind = match kind {
        "user" => EntityKind::User,
        "post" => EntityKind::Post,
        other => return Err(corrupt(format!("unknown entity kind {other:?}"))),
    };
    if crate::validate_object_id(object_id).is_err() {
        return Err(corrupt(format!("{object_id:?} is not an ObjectId")));
    }
    Ok(mapping_of(object_id, entity_kind, snowflake_id))
}

/// Store calls served so far, so tests can assert batching behavior.
#[derive(Debug, Default)]
pub struct MemoryStoreCalls {
    pub find_by_object: AtomicU64,
    pub find_by_snowflake: AtomicU64,
    pub find_by_object_batch: AtomicU64,
    pub find_by_snowflake_batch: AtomicU64,
    pub insert_if_absent: AtomicU64,
    pub insert_if_absent_batch: AtomicU64,
    pub next_sequence_batch: AtomicU64,
    pub observe_sequence_batch: AtomicU64,
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// In-memory `MappingStore` for unit tests and local development; inject it
/// via `RedisIdRegistry::with_store` or select it through
/// `RedisIdRegistryConfig::redis_enabled = false` when no Redis instance
/// should be involved.
/// It mirrors the Redis adapter's semantics: two independent indexes written
/// reverse-first, reverse lookups validated against the forward entry, and
/// `(worker, second)` sequence counters.
pub struct MemoryMappingStore {
    by_object: Mutex<HashMap<ObjectKey, Mapping>>,
    by_snowflake: Mutex<HashMap<u64, Mapping>>,
    sequences: Mutex<HashMap<(u64, u64), u64>>,
    pub calls: MemoryStoreCalls,
}

impl MemoryMappingStore {
    pub fn new() -> Self {
        Self {
            by_object: Mutex::new(HashMap::new()),
            by_snowflake: Mutex::new(HashMap::new()),
            sequences: Mutex::new(HashMap::new()),
            calls: MemoryStoreCalls::default(),
        }
    }

    /// The forward entry as stored.
    pub fn by_object(&self, kind: EntityKind, object_id: &str) -> Option<Mapping> {
        lock_ignoring_poison(&self.by_object)
            .get(&(kind, object_id.to_string()))
            .cloned()
    }

    /// The reverse entry as stored, without the forward validation that
    /// `find_by_snowflake` applies.
    pub fn by_snowflake(&self, id: SnowflakeId) -> Option<Mapping> {
        lock_ignoring_poison(&self.by_snowflake)
            .get(&id.get())
            .cloned()
    }

    /// Write only the reverse entry, reproducing a two-phase write that
    /// stopped between its phases (an orphan).
    pub fn insert_reverse_only(&self, mapping: &Mapping) {
        lock_ignoring_poison(&self.by_snowflake)
            .insert(mapping.snowflake_id.get(), mapping.clone());
    }

    /// Current value of the `(worker, second)` counter, 0 when absent.
    pub fn sequence(&self, worker_id: u64, second: u64) -> u64 {
        lock_ignoring_poison(&self.sequences)
            .get(&(worker_id, second))
            .copied()
            .unwrap_or(0)
    }

    /// Drop every counter, reproducing Redis TTL expiry.
    pub fn reset_sequences(&self) {
        lock_ignoring_poison(&self.sequences).clear();
    }
}

impl Default for MemoryMappingStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MappingStore for MemoryMappingStore {
    async fn find_by_object_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> Result<Vec<Option<Mapping>>, IdError> {
        bump(&self.calls.find_by_object_batch);
        Ok(ids
            .iter()
            .map(|(object_id, kind)| self.by_object(*kind, object_id))
            .collect())
    }

    async fn find_by_snowflake_batch(
        &self,
        ids: &[SnowflakeId],
    ) -> Result<Vec<Option<Mapping>>, IdError> {
        bump(&self.calls.find_by_snowflake_batch);
        Ok(ids
            .iter()
            .map(|id| {
                let reverse = self.by_snowflake(*id)?;
                let forward = self.by_object(reverse.entity_kind, &reverse.object_id)?;
                (forward.snowflake_id == *id).then_some(reverse)
            })
            .collect())
    }

    async fn insert_if_absent_batch(
        &self,
        mappings: &[Mapping],
    ) -> Result<Vec<InsertOutcome>, IdError> {
        bump(&self.calls.insert_if_absent_batch);
        let mut by_snowflake = lock_ignoring_poison(&self.by_snowflake);
        let mut by_object = lock_ignoring_poison(&self.by_object);
        let mut outcomes = Vec::with_capacity(mappings.len());
        for mapping in mappings {
            let created_reverse = match by_snowflake.get(&mapping.snowflake_id.get()) {
                None => {
                    by_snowflake.insert(mapping.snowflake_id.get(), mapping.clone());
                    true
                }
                Some(existing)
                    if existing.object_id == mapping.object_id
                        && existing.entity_kind == mapping.entity_kind =>
                {
                    false
                }
                Some(_) => {
                    outcomes.push(InsertOutcome::SnowflakeConflict);
                    continue;
                }
            };
            let key = (mapping.entity_kind, mapping.object_id.clone());
            match by_object.get(&key) {
                None => {
                    by_object.insert(key, mapping.clone());
                    outcomes.push(InsertOutcome::Inserted);
                }
                Some(existing) if existing.snowflake_id == mapping.snowflake_id => {
                    outcomes.push(InsertOutcome::AlreadyPresent);
                }
                Some(_) => {
                    if created_reverse {
                        by_snowflake.remove(&mapping.snowflake_id.get());
                    }
                    outcomes.push(InsertOutcome::ObjectConflict);
                }
            }
        }
        Ok(outcomes)
    }

    async fn next_sequence_batch(
        &self,
        reservations: &[SequenceReservation],
    ) -> Result<Vec<u64>, IdError> {
        bump(&self.calls.next_sequence_batch);
        let mut sequences = lock_ignoring_poison(&self.sequences);
        Ok(reservations
            .iter()
            .map(|reservation| {
                let counter = sequences
                    .entry((
                        reservation.worker_id,
                        reservation.object_timestamp_ms / 1_000,
                    ))
                    .or_default();
                *counter += reservation.count;
                *counter
            })
            .collect())
    }

    async fn observe_sequence_batch(&self, floors: &[SequenceFloor]) -> Result<(), IdError> {
        bump(&self.calls.observe_sequence_batch);
        let mut sequences = lock_ignoring_poison(&self.sequences);
        for floor in floors {
            let counter = sequences
                .entry((floor.worker_id, floor.second))
                .or_default();
            *counter = (*counter).max(floor.min_next);
        }
        Ok(())
    }

    async fn check_ready(&self) -> Result<(), IdError> {
        Ok(())
    }

    fn cache_sizes(&self) -> (usize, usize) {
        (
            lock_ignoring_poison(&self.by_object).len(),
            lock_ignoring_poison(&self.by_snowflake).len(),
        )
    }

    async fn find_by_object(
        &self,
        entity_kind: EntityKind,
        object_id: &str,
    ) -> Result<Option<Mapping>, IdError> {
        bump(&self.calls.find_by_object);
        Ok(self.by_object(entity_kind, object_id))
    }

    async fn find_by_snowflake(
        &self,
        snowflake_id: SnowflakeId,
    ) -> Result<Option<Mapping>, IdError> {
        bump(&self.calls.find_by_snowflake);
        let mut found = self.find_by_snowflake_batch(&[snowflake_id]).await?;
        Ok(found.pop().flatten())
    }

    async fn insert_if_absent(&self, mapping: &Mapping) -> Result<InsertOutcome, IdError> {
        bump(&self.calls.insert_if_absent);
        let mut outcomes = self
            .insert_if_absent_batch(std::slice::from_ref(mapping))
            .await?;
        outcomes
            .pop()
            .ok_or_else(|| IdError::Redis("mapping write returned no outcome".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const USER: &str = "65f1a2b3c4d5e6f708091011";
    const POST: &str = "66f1a2b3c4d5e6f708091011";
    const OTHER_POST: &str = "66f1a2b3c4d5e6f708091013";

    use super::MemoryMappingStore as MemoryStore;

    struct ShortObjectBatchStore;

    #[async_trait::async_trait]
    impl MappingStore for ShortObjectBatchStore {
        async fn find_by_object_batch(
            &self,
            _ids: &[(String, EntityKind)],
        ) -> Result<Vec<Option<Mapping>>, IdError> {
            Ok(Vec::new())
        }

        async fn find_by_snowflake_batch(
            &self,
            ids: &[SnowflakeId],
        ) -> Result<Vec<Option<Mapping>>, IdError> {
            Ok(vec![None; ids.len()])
        }

        async fn insert_if_absent_batch(
            &self,
            mappings: &[Mapping],
        ) -> Result<Vec<InsertOutcome>, IdError> {
            Ok(vec![InsertOutcome::Inserted; mappings.len()])
        }

        async fn next_sequence_batch(
            &self,
            reservations: &[SequenceReservation],
        ) -> Result<Vec<u64>, IdError> {
            Ok(vec![1; reservations.len()])
        }

        async fn observe_sequence_batch(&self, _floors: &[SequenceFloor]) -> Result<(), IdError> {
            Ok(())
        }

        async fn check_ready(&self) -> Result<(), IdError> {
            Ok(())
        }

        fn cache_sizes(&self) -> (usize, usize) {
            (0, 0)
        }
    }

    fn registry(store: &Arc<MemoryStore>, allow_allocation: bool) -> RedisIdRegistry {
        RedisIdRegistry::with_store(store.clone(), 0, allow_allocation).unwrap()
    }

    fn snowflake(value: u64) -> SnowflakeId {
        SnowflakeId::new(value).unwrap()
    }

    /// Snowflake `(second, worker, sequence)` with the sequence in the
    /// second's first millisecond, exactly what the allocator produces for
    /// the first 4096 ids of a second.
    fn snowflake_at(second: u64, worker_id: u64, sequence: u64) -> SnowflakeId {
        snowflake(
            ((second * 1_000 - SNOWFLAKE_EPOCH_MS) << (WORKER_BITS + SEQUENCE_BITS))
                | (worker_id << SEQUENCE_BITS)
                | sequence,
        )
    }

    fn object_id_at(second: u64, suffix: u32) -> String {
        format!("{second:08x}{suffix:016x}")
    }

    #[tokio::test]
    async fn connect_without_redis_uses_process_local_memory_store() {
        let registry = RedisIdRegistry::connect(RedisIdRegistryConfig {
            redis_enabled: false,
            single_url: None,
            cluster_urls: None,
            key_prefix: "id-registry:memory-test".to_string(),
            cache_capacity: 4,
            worker_id: 0,
            allow_allocation: true,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
        })
        .await
        .unwrap();

        let id = registry.allocate_one(POST, EntityKind::Post, None).await.unwrap();
        let mapping = registry.reverse_one(id, EntityKind::Post).await.unwrap();
        assert_eq!(mapping.object_id, POST);
        assert_eq!(registry.cache_sizes(), (1, 1));
        registry.check_ready().await.unwrap();
    }

    #[tokio::test]
    async fn connect_with_redis_enabled_requires_a_redis_endpoint() {
        let result = RedisIdRegistry::connect(RedisIdRegistryConfig {
            redis_enabled: true,
            single_url: None,
            cluster_urls: None,
            key_prefix: "id-registry:redis-test".to_string(),
            cache_capacity: 4,
            worker_id: 0,
            allow_allocation: true,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
        })
        .await;
        let Err(error) = result else {
            panic!("Redis mode must reject a missing endpoint");
        };
        assert!(matches!(
            error,
            IdError::Redis(message) if message == "no Redis endpoint configured"
        ));
    }

    fn loads(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    #[tokio::test]
    async fn rejects_a_snowflake_reused_across_entity_kinds() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, false);
        let provided = snowflake(42);
        registry
            .allocate_one(USER, EntityKind::User, Some(provided))
            .await
            .unwrap();
        let error = registry
            .allocate_one(POST, EntityKind::Post, Some(provided))
            .await
            .unwrap_err();
        assert_eq!(
            error,
            IdError::SnowflakeTaken {
                snowflake_id: provided,
                object_id: POST.to_string(),
                entity_kind: EntityKind::Post,
                holder: Some((EntityKind::User, USER.to_string())),
            }
        );
    }

    #[tokio::test]
    async fn batch_resolve_and_reverse_preserve_order() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, false);
        let input = vec![
            (USER.to_string(), EntityKind::User, Some(snowflake(101))),
            (POST.to_string(), EntityKind::Post, Some(snowflake(202))),
        ];
        let ids = registry.allocate_batch(&input).await.unwrap();
        assert_eq!(ids, vec![snowflake(101), snowflake(202)]);
        let mappings = registry
            .reverse_batch(&[(ids[1], EntityKind::Post), (ids[0], EntityKind::User)])
            .await
            .unwrap();
        assert_eq!(mappings[0].object_id, POST);
        assert_eq!(mappings[1].object_id, USER);
        assert_eq!(loads(&store.calls.find_by_object), 0);
    }

    #[tokio::test]
    async fn provided_conflict_in_batch_does_not_commit_earlier_items() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, true);
        let provided = snowflake(123);
        registry
            .allocate_one(USER, EntityKind::User, Some(provided))
            .await
            .unwrap();

        let error = registry
            .allocate_batch(&[
                (POST.to_string(), EntityKind::Post, None),
                (USER.to_string(), EntityKind::User, Some(snowflake(124))),
            ])
            .await
            .unwrap_err();
        assert!(matches!(error, IdError::MappingConflict { .. }));
        assert!(store.by_object(EntityKind::Post, POST).is_none());
        assert_eq!(
            store
                .by_object(EntityKind::User, USER)
                .unwrap()
                .snowflake_id,
            provided
        );
        // The conflict is found while examining existing mappings, before
        // any sequence value is drawn for the allocated item.
        assert_eq!(loads(&store.calls.next_sequence_batch), 0);
    }

    #[tokio::test]
    async fn batch_rejects_provided_snowflake_bound_to_another_object_before_writing() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, false);
        registry
            .allocate_one(USER, EntityKind::User, Some(snowflake(42)))
            .await
            .unwrap();

        // The post is missing, so only the reverse pre-check can catch that
        // its provided snowflake is already bound to the user.
        let error = registry
            .allocate_batch(&[
                (POST.to_string(), EntityKind::Post, Some(snowflake(42))),
                (
                    OTHER_POST.to_string(),
                    EntityKind::Post,
                    Some(snowflake(43)),
                ),
            ])
            .await
            .unwrap_err();
        assert!(matches!(error, IdError::SnowflakeTaken { .. }), "{error}");
        assert!(store.by_object(EntityKind::Post, POST).is_none());
        assert!(store.by_object(EntityKind::Post, OTHER_POST).is_none());
        assert!(store.by_snowflake(snowflake(43)).is_none());
        assert_eq!(loads(&store.calls.insert_if_absent_batch), 1);
    }

    #[tokio::test]
    async fn batch_rejects_conflicting_items_within_the_same_batch() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, false);

        // Same object twice with different provided ids.
        let error = registry
            .allocate_batch(&[
                (USER.to_string(), EntityKind::User, Some(snowflake(7))),
                (USER.to_string(), EntityKind::User, Some(snowflake(8))),
            ])
            .await
            .unwrap_err();
        assert!(matches!(error, IdError::MappingConflict { .. }));

        // Different objects sharing one provided snowflake.
        let error = registry
            .allocate_batch(&[
                (USER.to_string(), EntityKind::User, Some(snowflake(9))),
                (POST.to_string(), EntityKind::Post, Some(snowflake(9))),
            ])
            .await
            .unwrap_err();
        assert_eq!(
            error,
            IdError::SnowflakeTaken {
                snowflake_id: snowflake(9),
                object_id: POST.to_string(),
                entity_kind: EntityKind::Post,
                holder: Some((EntityKind::User, USER.to_string())),
            }
        );

        assert!(store.by_object(EntityKind::User, USER).is_none());
        assert!(store.by_object(EntityKind::Post, POST).is_none());
        assert!(store.by_snowflake(snowflake(7)).is_none());
        assert!(store.by_snowflake(snowflake(9)).is_none());
        // In-batch contradictions are found before any store call.
        assert_eq!(loads(&store.calls.find_by_object_batch), 0);
    }

    #[tokio::test]
    async fn batch_resolves_in_batch_duplicates_to_a_single_mapping() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, true);
        let provided = snowflake(31);
        let ids = registry
            .allocate_batch(&[
                (USER.to_string(), EntityKind::User, Some(provided)),
                (USER.to_string(), EntityKind::User, Some(provided)),
                // A duplicate without the provided id merges into the same item.
                (USER.to_string(), EntityKind::User, None),
            ])
            .await
            .unwrap();
        assert_eq!(ids, vec![provided, provided, provided]);
        assert_eq!(
            store
                .by_object(EntityKind::User, USER)
                .unwrap()
                .snowflake_id,
            provided
        );

        // Non-provided duplicates share one allocation instead of burning a
        // second sequence number for the same object.
        let allocated = registry
            .allocate_batch(&[
                (POST.to_string(), EntityKind::Post, None),
                (POST.to_string(), EntityKind::Post, None),
            ])
            .await
            .unwrap();
        assert_eq!(allocated[0], allocated[1]);
        assert_eq!(
            store
                .by_object(EntityKind::Post, POST)
                .unwrap()
                .snowflake_id,
            allocated[0]
        );
        let second = crate::object_id_timestamp_secs(POST).unwrap();
        assert_eq!(store.sequence(0, second), 1);
    }

    #[tokio::test]
    async fn allocation_skips_sequence_values_held_by_provided_imports() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, true);
        let second = crate::object_id_timestamp_secs(USER).unwrap();

        // A provided import carrying our worker id occupies (second, seq 0).
        registry
            .allocate_one(USER, EntityKind::User, Some(snowflake_at(second, 0, 0)))
            .await
            .unwrap();
        assert_eq!(store.sequence(0, second), 1);
        assert_eq!(loads(&store.calls.observe_sequence_batch), 1);

        // The next allocation in the same second gets seq 1 on the first try.
        let post = object_id_at(second, 1);
        let allocated = registry.allocate_one(&post, EntityKind::Post, None).await.unwrap();
        assert_eq!(allocated, snowflake_at(second, 0, 1));
        assert_eq!(loads(&store.calls.insert_if_absent_batch), 2);

        // After the counter expired (TTL), the allocator collides with both
        // existing ids and redraws until it finds a free value.
        store.reset_sequences();
        let another = object_id_at(second, 2);
        let allocated = registry
            .allocate_one(&another, EntityKind::Post, None)
            .await
            .unwrap();
        assert_eq!(allocated, snowflake_at(second, 0, 2));
        assert_eq!(
            registry
                .reverse_one(allocated, EntityKind::Post)
                .await
                .unwrap()
                .object_id,
            another
        );
    }

    #[tokio::test]
    async fn provided_import_from_another_worker_leaves_our_sequence_alone() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, true);
        let second = crate::object_id_timestamp_secs(USER).unwrap();
        registry
            .allocate_one(USER, EntityKind::User, Some(snowflake_at(second, 7, 0)))
            .await
            .unwrap();
        assert_eq!(store.sequence(0, second), 0);
        assert_eq!(store.sequence(7, second), 0);
        assert_eq!(loads(&store.calls.observe_sequence_batch), 0);
    }

    #[tokio::test]
    async fn allocation_gives_up_after_max_attempts() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, true);
        let second = crate::object_id_timestamp_secs(USER).unwrap();
        for sequence in 0..MAX_ALLOCATION_ATTEMPTS as u64 {
            registry
                .allocate_one(
                    &object_id_at(second, 100 + sequence as u32),
                    EntityKind::User,
                    Some(snowflake_at(second, 0, sequence)),
                )
                .await
                .unwrap();
        }
        store.reset_sequences();
        let writes_before = loads(&store.calls.insert_if_absent_batch);

        let error = registry
            .allocate_one(&object_id_at(second, 1), EntityKind::Post, None)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                IdError::SnowflakeTaken { snowflake_id, holder: Some(_), .. }
                    if snowflake_id == snowflake_at(second, 0, MAX_ALLOCATION_ATTEMPTS as u64 - 1)
            ),
            "{error}"
        );
        assert_eq!(
            loads(&store.calls.insert_if_absent_batch) - writes_before,
            MAX_ALLOCATION_ATTEMPTS as u64
        );
        assert!(store
            .by_object(EntityKind::Post, &object_id_at(second, 1))
            .is_none());
    }

    #[tokio::test]
    async fn batch_of_provided_misses_touches_the_store_once_per_operation() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, true);
        let input = (0..50u32)
            .map(|index| {
                (
                    object_id_at(0x65f1_a2b3, index),
                    EntityKind::User,
                    Some(snowflake(1_000 + u64::from(index))),
                )
            })
            .collect::<Vec<_>>();
        let ids = registry.allocate_batch(&input).await.unwrap();
        assert_eq!(ids.len(), 50);
        assert_eq!(loads(&store.calls.find_by_object_batch), 1);
        assert_eq!(loads(&store.calls.find_by_snowflake_batch), 1);
        assert_eq!(loads(&store.calls.insert_if_absent_batch), 1);
        assert_eq!(loads(&store.calls.find_by_object), 0);
        assert_eq!(loads(&store.calls.find_by_snowflake), 0);
        assert_eq!(loads(&store.calls.insert_if_absent), 0);
        assert_eq!(loads(&store.calls.next_sequence_batch), 0);

        // Allocation for many objects across several seconds is one
        // sequence call and one write as well.
        let input = (0..40u32)
            .map(|index| {
                (
                    object_id_at(0x66f1_a2b3 + u64::from(index % 4), index),
                    EntityKind::Post,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let ids = registry.allocate_batch(&input).await.unwrap();
        assert_eq!(
            ids.iter().collect::<std::collections::HashSet<_>>().len(),
            40
        );
        assert_eq!(loads(&store.calls.next_sequence_batch), 1);
        assert_eq!(loads(&store.calls.insert_if_absent_batch), 2);
        assert_eq!(loads(&store.calls.find_by_object_batch), 2);
        for second_offset in 0..4u64 {
            assert_eq!(store.sequence(0, 0x66f1_a2b3 + second_offset), 10);
        }
    }

    #[tokio::test]
    async fn disabled_allocation_reports_every_unknown_id_at_once() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, false);
        let unknown = (0..7u32)
            .map(|index| (object_id_at(0x65f1_a2b3, index), EntityKind::Post, None))
            .collect::<Vec<_>>();
        let error = registry.allocate_batch(&unknown).await.unwrap_err();
        let IdError::AllocationDisabled(summary) = &error else {
            panic!("unexpected error {error}");
        };
        assert!(summary.starts_with("7 ids, first 5: "), "{summary}");
        for (object_id, _, _) in &unknown[..5] {
            assert!(summary.contains(object_id), "{summary}");
        }
        assert!(!summary.contains(&unknown[5].0), "{summary}");
        assert_eq!(loads(&store.calls.insert_if_absent_batch), 0);

        let error = registry
            .allocate_one(POST, EntityKind::Post, None)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("allocation is disabled and no mapping exists for Post {POST}")
        );
    }

    #[tokio::test]
    async fn read_only_resolution_reports_unknown_mappings_without_allocation_language() {
        for allow_allocation in [false, true] {
            let store = Arc::new(MemoryStore::new());
            let registry = registry(&store, allow_allocation);
            let error = registry
                .resolve_existing_batch(&[(POST.to_string(), EntityKind::Post)])
                .await
                .unwrap_err();
            assert_eq!(error, IdError::UnknownObjectIds(format!("Post {POST}")));
            assert_eq!(
                error.to_string(),
                format!("no ObjectId mapping exists for Post {POST}")
            );
        }
    }

    #[tokio::test]
    async fn read_only_resolution_rejects_misaligned_store_results() {
        let registry =
            RedisIdRegistry::with_store(Arc::new(ShortObjectBatchStore), 0, true).unwrap();
        let error = registry
            .resolve_existing_one(POST, EntityKind::Post)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            IdError::CorruptRecord { ref key, ref reason }
                if key == "mapping store batch"
                    && reason == "find_by_object_batch returned 0 rows for 1 inputs"
        ));
    }

    #[tokio::test]
    async fn orphan_reverse_entries_are_invisible_and_repaired_by_the_same_import() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, true);
        let orphan = snowflake(555_000_111);
        store.insert_reverse_only(&mapping_of(POST, EntityKind::Post, orphan));

        // Fail closed on the read side: the orphan is not a mapping.
        assert_eq!(
            registry
                .reverse_one(orphan, EntityKind::Post)
                .await
                .unwrap_err(),
            IdError::UnknownSnowflake(orphan.get())
        );
        assert!(registry
            .reverse_batch(&[(orphan, EntityKind::Post)])
            .await
            .is_err());

        // A provided import of the same object completes the mapping through
        // the single path...
        assert_eq!(
            registry
                .allocate_one(POST, EntityKind::Post, Some(orphan))
                .await
                .unwrap(),
            orphan
        );
        assert_eq!(
            store
                .by_object(EntityKind::Post, POST)
                .unwrap()
                .snowflake_id,
            orphan
        );
        assert_eq!(
            registry
                .reverse_one(orphan, EntityKind::Post)
                .await
                .unwrap()
                .object_id,
            POST
        );

        // ...and through the batch path.
        let batch_orphan = snowflake(555_000_112);
        store.insert_reverse_only(&mapping_of(OTHER_POST, EntityKind::Post, batch_orphan));
        let ids = registry
            .allocate_batch(&[
                (OTHER_POST.to_string(), EntityKind::Post, Some(batch_orphan)),
                (USER.to_string(), EntityKind::User, Some(snowflake(9))),
            ])
            .await
            .unwrap();
        assert_eq!(ids, vec![batch_orphan, snowflake(9)]);
        assert_eq!(
            store
                .by_object(EntityKind::Post, OTHER_POST)
                .unwrap()
                .snowflake_id,
            batch_orphan
        );

        // An orphan of a different object still blocks a provided import...
        let foreign = snowflake(555_000_113);
        store.insert_reverse_only(&mapping_of(USER, EntityKind::User, foreign));
        let error = registry
            .allocate_one(
                &object_id_at(0x65f1_a2b3, 77),
                EntityKind::User,
                Some(foreign),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, IdError::SnowflakeTaken { snowflake_id, holder: None, .. } if snowflake_id == foreign),
            "{error}"
        );
    }

    #[tokio::test]
    async fn allocation_colliding_with_an_orphan_reverse_entry_redraws() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, true);
        let second = crate::object_id_timestamp_secs(POST).unwrap();
        store.insert_reverse_only(&mapping_of(
            USER,
            EntityKind::User,
            snowflake_at(second, 0, 0),
        ));
        let allocated = registry.allocate_one(POST, EntityKind::Post, None).await.unwrap();
        assert_eq!(allocated, snowflake_at(second, 0, 1));
        assert_eq!(loads(&store.calls.insert_if_absent_batch), 2);
    }

    #[tokio::test]
    async fn reverse_lookup_of_an_unknown_snowflake_is_unknown() {
        let store = Arc::new(MemoryStore::new());
        let registry = registry(&store, false);
        assert_eq!(
            registry
                .reverse_one(snowflake(404), EntityKind::User)
                .await
                .unwrap_err(),
            IdError::UnknownSnowflake(404)
        );
        registry
            .allocate_one(USER, EntityKind::User, Some(snowflake(1)))
            .await
            .unwrap();
        assert_eq!(
            registry
                .reverse_one(snowflake(1), EntityKind::Post)
                .await
                .unwrap_err(),
            IdError::EntityKindMismatch {
                snowflake_id: snowflake(1),
                expected: EntityKind::Post,
                actual: EntityKind::User,
            }
        );
    }

    #[test]
    fn sequence_floor_decodes_second_and_offset() {
        let second = 1_700_000_000;
        assert_eq!(
            sequence_floor(snowflake_at(second, 0, 0), 0),
            Some(SequenceFloor {
                worker_id: 0,
                second,
                min_next: 1
            })
        );
        assert_eq!(
            sequence_floor(snowflake_at(second, 0, 5), 0),
            Some(SequenceFloor {
                worker_id: 0,
                second,
                min_next: 6
            })
        );
        // Third millisecond of the second, sequence 4: offset 2 * 4096 + 4.
        let in_third_millisecond = snowflake(
            ((second * 1_000 + 2 - SNOWFLAKE_EPOCH_MS) << (WORKER_BITS + SEQUENCE_BITS)) | 4,
        );
        assert_eq!(
            sequence_floor(in_third_millisecond, 0),
            Some(SequenceFloor {
                worker_id: 0,
                second,
                min_next: 2 * 4096 + 5
            })
        );
        assert_eq!(sequence_floor(snowflake_at(second, 3, 0), 0), None);
        assert_eq!(
            merge_floors(vec![
                SequenceFloor {
                    worker_id: 0,
                    second,
                    min_next: 3
                },
                SequenceFloor {
                    worker_id: 0,
                    second,
                    min_next: 9
                },
                SequenceFloor {
                    worker_id: 0,
                    second: second + 1,
                    min_next: 1
                },
            ]),
            vec![
                SequenceFloor {
                    worker_id: 0,
                    second,
                    min_next: 9
                },
                SequenceFloor {
                    worker_id: 0,
                    second: second + 1,
                    min_next: 1
                },
            ]
        );
    }

    #[test]
    fn hash_tagged_keys_share_the_slot_of_their_shard() {
        let store_prefix = "id-registry:v2";
        let key = format!("{store_prefix}:object:{{037}}:user:{USER}");
        assert_eq!(
            redis::cluster_routing::get_slot(key.as_bytes()),
            redis::cluster_routing::get_slot(b"037")
        );
        assert_eq!(mapping_shard("user:abc"), mapping_shard("user:abc"));
        assert!(mapping_shard(&format!("user:{USER}")) < MAPPING_SHARD_COUNT);
    }

    #[test]
    fn bounded_cache_evicts_old_entries() {
        let mut cache = BoundedCache::new(2, "test");
        cache.insert("a", 1, None);
        cache.insert("b", 2, None);
        assert_eq!(cache.get(&"a"), Some(1));
        cache.insert("c", 3, None);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"a"), Some(1));
        assert_eq!(cache.get(&"c"), Some(3));
    }

    #[test]
    fn corrupt_redis_values_are_reported_with_their_key() {
        let error = parse_snowflake("abc", || "k".to_string()).unwrap_err();
        assert!(matches!(error, IdError::CorruptRecord { ref key, .. } if key == "k"));
        let error = parse_reverse_value("thing:oid", snowflake(1), || "k".to_string()).unwrap_err();
        assert!(matches!(error, IdError::CorruptRecord { ref key, .. } if key == "k"));
        let error = parse_reverse_value("user:bad", snowflake(1), || "k".to_string()).unwrap_err();
        assert!(matches!(error, IdError::CorruptRecord { .. }));
        assert_eq!(
            parse_reverse_value(&format!("post:{POST}"), snowflake(1), || unreachable!())
                .unwrap()
                .entity_kind,
            EntityKind::Post
        );
    }

    async fn raw_connection(url: &str) -> redis::aio::MultiplexedConnection {
        redis::Client::open(url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap()
    }

    async fn delete_prefix(url: &str, prefix: &str) {
        let mut connection = raw_connection(url).await;
        let mut cursor: u64 = 0;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("{prefix}*"))
                .arg("COUNT")
                .arg(1_000)
                .query_async(&mut connection)
                .await
                .unwrap();
            if !keys.is_empty() {
                let _: i64 = redis::cmd("DEL")
                    .arg(&keys)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
    }

    fn redis_config(url: &str, prefix: &str) -> RedisIdRegistryConfig {
        RedisIdRegistryConfig {
            redis_enabled: true,
            single_url: Some(url.to_string()),
            cluster_urls: None,
            key_prefix: prefix.to_string(),
            cache_capacity: 4,
            worker_id: 0,
            allow_allocation: true,
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(1),
        }
    }

    fn test_prefix(label: &str) -> String {
        format!(
            "id-registry-test-{label}-{}-{}",
            std::process::id(),
            crate::now_secs()
        )
    }

    #[tokio::test]
    #[ignore = "requires ID_REGISTRY_TEST_REDIS_URL"]
    async fn redis_round_trip_and_version_metadata() {
        let url = std::env::var("ID_REGISTRY_TEST_REDIS_URL").unwrap();
        let prefix = test_prefix("roundtrip");
        let registry = RedisIdRegistry::connect(redis_config(&url, &prefix))
            .await
            .unwrap();
        let id = snowflake(9001);
        registry
            .allocate_one(USER, EntityKind::User, Some(id))
            .await
            .unwrap();
        assert_eq!(
            registry.allocate_one(USER, EntityKind::User, None).await.unwrap(),
            id
        );
        assert_eq!(
            registry
                .reverse_one(id, EntityKind::User)
                .await
                .unwrap()
                .object_id,
            USER
        );
        assert_eq!(registry.cache_sizes(), (1, 1));
        registry.check_ready().await.unwrap();

        // Scripts flushed by an operator: the next write reloads them.
        let mut raw = raw_connection(&url).await;
        let _: String = redis::cmd("SCRIPT")
            .arg("FLUSH")
            .query_async(&mut raw)
            .await
            .unwrap();
        registry
            .allocate_one(POST, EntityKind::Post, Some(snowflake(9002)))
            .await
            .unwrap();

        // Readiness is read-only: missing metadata is reported, not rebuilt.
        let version_key = format!("{prefix}:metadata:mapping_version");
        let schema_key = format!("{prefix}:metadata:storage_schema");
        let schema: String = redis::cmd("GET")
            .arg(&schema_key)
            .query_async(&mut raw)
            .await
            .unwrap();
        assert_eq!(schema, STORAGE_SCHEMA);
        let _: i64 = redis::cmd("DEL")
            .arg(&version_key)
            .arg(&schema_key)
            .query_async(&mut raw)
            .await
            .unwrap();
        let error = registry.check_ready().await.unwrap_err();
        assert_eq!(
            error,
            IdError::Redis("mapping version metadata is missing".to_string())
        );
        let remaining: i64 = redis::cmd("EXISTS")
            .arg(&version_key)
            .arg(&schema_key)
            .query_async(&mut raw)
            .await
            .unwrap();
        assert_eq!(remaining, 0, "readiness must not recreate metadata");

        // A wrong schema marker is rejected by readiness and by startup.
        let _: () = redis::cmd("SET")
            .arg(&version_key)
            .arg(MAPPING_VERSION)
            .query_async(&mut raw)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg(&schema_key)
            .arg("single-hash")
            .query_async(&mut raw)
            .await
            .unwrap();
        assert!(matches!(
            registry.check_ready().await.unwrap_err(),
            IdError::StorageSchemaMismatch { .. }
        ));
        let Err(error) = RedisIdRegistry::connect(redis_config(&url, &prefix)).await else {
            panic!("startup must reject a foreign storage schema");
        };
        assert!(matches!(error, IdError::StorageSchemaMismatch { .. }));

        delete_prefix(&url, &prefix).await;
    }

    #[tokio::test]
    #[ignore = "requires ID_REGISTRY_TEST_REDIS_URL"]
    async fn redis_orphan_reverse_entry_is_unknown_until_repaired() {
        let url = std::env::var("ID_REGISTRY_TEST_REDIS_URL").unwrap();
        let prefix = test_prefix("orphan");
        let registry = RedisIdRegistry::connect(redis_config(&url, &prefix))
            .await
            .unwrap();
        let orphan = snowflake(555_000_111);
        let mut raw = raw_connection(&url).await;
        let reverse_key = format!(
            "{prefix}:snowflake:{{{:03}}}:{}",
            mapping_shard(&orphan.get().to_string()),
            orphan.get()
        );
        let _: () = redis::cmd("SET")
            .arg(&reverse_key)
            .arg(format!("post:{POST}"))
            .query_async(&mut raw)
            .await
            .unwrap();

        assert_eq!(
            registry
                .reverse_one(orphan, EntityKind::Post)
                .await
                .unwrap_err(),
            IdError::UnknownSnowflake(orphan.get())
        );
        assert_eq!(registry.cache_sizes(), (0, 0));

        // Importing the same object completes the mapping (single and batch).
        assert_eq!(
            registry
                .allocate_batch(&[(POST.to_string(), EntityKind::Post, Some(orphan))])
                .await
                .unwrap(),
            vec![orphan]
        );
        assert_eq!(
            registry
                .reverse_one(orphan, EntityKind::Post)
                .await
                .unwrap()
                .object_id,
            POST
        );

        // An allocation whose fresh id collides with an imported one redraws.
        let second = crate::object_id_timestamp_secs(USER).unwrap();
        registry
            .allocate_one(USER, EntityKind::User, Some(snowflake_at(second, 0, 0)))
            .await
            .unwrap();
        let allocated = registry
            .allocate_one(&object_id_at(second, 1), EntityKind::Post, None)
            .await
            .unwrap();
        assert_eq!(allocated, snowflake_at(second, 0, 1));
        let counter: i64 = redis::cmd("GET")
            .arg(format!("{prefix}:sequence:0:{second}"))
            .query_async(&mut raw)
            .await
            .unwrap();
        assert_eq!(counter, 2);

        delete_prefix(&url, &prefix).await;
    }
}
