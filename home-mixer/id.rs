//! Home Mixer identity boundary.
//!
//! Public RPCs carry ObjectId strings. This module is the only Home Mixer
//! boundary that resolves them before the internal Snowflake migration.

pub use id_service::{EntityKind, IdError, SnowflakeId};
use serde::Deserialize;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tonic::transport::{Channel, Endpoint};
use x_algorithm_proto::id_registry as registry_pb;

const ID_REGISTRY_REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

/// Internal identity used by the recommendation path after ingress
/// normalization.
pub type InternalId = SnowflakeId;

#[derive(Clone)]
pub struct RegistryClient {
    transport: std::sync::Arc<dyn RegistryTransport>,
    calls: crate::metrics::ClientCallRecorder,
}

pub type SharedRegistryClient = std::sync::Arc<RegistryClient>;

/// Read-only ObjectId ↔ Snowflake boundary.
#[tonic::async_trait]
pub trait IdentityReader: Send + Sync {
    async fn resolve_batch(&self, ids: &[(String, EntityKind)])
        -> anyhow::Result<Vec<SnowflakeId>>;
    /// Resolve a batch while preserving missing mappings as `None` in input
    /// order. Implementations backed by the Registry should use its partial
    /// batch RPC; the default keeps compatibility with test/legacy resolvers.
    async fn resolve_batch_partial(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        match self.resolve_batch(ids).await {
            Ok(values) => {
                anyhow::ensure!(
                    values.len() == ids.len(),
                    "identity partial result count mismatch"
                );
                Ok(values.into_iter().map(Some).collect())
            }
            Err(error) if is_mapping_miss(&error) => {
                let mut rows = Vec::with_capacity(ids.len());
                for id in ids {
                    match self.resolve_batch(std::slice::from_ref(id)).await {
                        Ok(mut values) => rows.push(values.pop()),
                        Err(error) if is_mapping_miss(&error) => rows.push(None),
                        Err(error) => return Err(error),
                    }
                }
                Ok(rows)
            }
            Err(error) => Err(error),
        }
    }
    async fn reverse_batch(&self, ids: &[(SnowflakeId, EntityKind)])
        -> anyhow::Result<Vec<String>>;

    async fn resolve_batch_with_timeout(
        &self,
        ids: &[(String, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        tokio::time::timeout(timeout, self.resolve_batch(ids))
            .await
            .map_err(|_| anyhow::anyhow!("identity resolve deadline exceeded"))?
    }

    async fn resolve_batch_partial_with_timeout(
        &self,
        ids: &[(String, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
        tokio::time::timeout(timeout, self.resolve_batch_partial(ids))
            .await
            .map_err(|_| anyhow::anyhow!("identity partial resolve deadline exceeded"))?
    }

    async fn reverse_batch_with_timeout(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<String>> {
        tokio::time::timeout(timeout, self.reverse_batch(ids))
            .await
            .map_err(|_| anyhow::anyhow!("identity reverse deadline exceeded"))?
    }
}

/// Write-side identity capability. Only ingress adapters receive this trait.
#[tonic::async_trait]
pub trait IdentityAllocator: Send + Sync {
    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>>;

    async fn allocate_batch_with_timeout(
        &self,
        ids: &[(String, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        tokio::time::timeout(timeout, self.allocate_batch(ids))
            .await
            .map_err(|_| anyhow::anyhow!("identity allocation deadline exceeded"))?
    }
}

/// Combined capability used only by ingress normalizers.
pub trait IdentityIngress: IdentityReader + IdentityAllocator {}
impl<T: IdentityReader + IdentityAllocator> IdentityIngress for T {}

pub type SharedIdentityReader = std::sync::Arc<dyn IdentityReader>;
// An `Arc<dyn IdentityIngress>` narrows to `Arc<dyn IdentityReader>` via
// trait upcasting, but only at explicit coercion sites (struct fields, let
// bindings, return positions) — routing it through generic functions like
// `Arc::clone` fails to compile. When one owner needs both views, erase the
// concrete `Arc<RegistryClient>` into each trait object separately (see
// `HomeMixerServer::build_with_metrics`).
pub type SharedIdentityIngress = std::sync::Arc<dyn IdentityIngress>;

type ForwardIdentityKey = (EntityKind, String);
type ReverseIdentityKey = (SnowflakeId, EntityKind);

#[derive(Clone, Copy)]
enum ForwardCacheEntry {
    Found(SnowflakeId),
    Missing,
}
type IdentityKeyFlight = std::sync::Arc<AsyncMutex<()>>;

#[derive(Default)]
struct ForwardMissPlan {
    missing: Vec<(String, EntityKind)>,
    positions: Vec<(usize, usize)>,
}

#[derive(Default)]
struct ReverseMissPlan {
    missing: Vec<ReverseIdentityKey>,
    positions: Vec<(usize, usize)>,
}

/// Request-scoped identity memoization boundary.
///
/// A recommendation request crosses several external contracts (mrpyq,
/// Redis state, the response and exposure event). They may all ask for the
/// same mapping. This context deduplicates those asks for one request and
/// seeds both directions whenever an ingress allocation succeeds. It does not
/// replace the Registry's durable cache or share state across requests.
pub struct IdentityContext {
    inner: SharedIdentityReader,
    shared: std::sync::Arc<IdentityContextShared>,
    stats: std::sync::Arc<IdentityContextStatsAtomic>,
    deadline: Option<Instant>,
}

/// Mutable request-local state shared by the main request and any explicitly
/// detached side-effect view of that request. The deadline stays on
/// [`IdentityContext`] because a side effect has its own bounded lifecycle,
/// while both paths must still see the same cache and identity bill.
struct IdentityContextShared {
    forward: Mutex<HashMap<ForwardIdentityKey, ForwardCacheEntry>>,
    reverse: Mutex<HashMap<ReverseIdentityKey, String>>,
    /// Per-key single-flight locks. A request may resolve unrelated identity
    /// keys concurrently, while overlapping calls for the same key still
    /// collapse to one Registry operation. Keys are acquired in a stable
    /// order to avoid deadlocks for overlapping batches.
    forward_flight: AsyncMutex<HashMap<ForwardIdentityKey, IdentityKeyFlight>>,
    reverse_flight: AsyncMutex<HashMap<ReverseIdentityKey, IdentityKeyFlight>>,
}

/// Counts the Registry operations attributable to one Home Mixer request.
///
/// These are *backend* calls after request-local deduplication, not calls to
/// the context API.  Keeping the counters on the request context makes it
/// possible to compare an end-to-end request's identity bill without adding a
/// high-cardinality request-id label to the process-wide Prometheus metrics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IdentityContextStats {
    pub allocate_batches: u64,
    pub allocate_ids: u64,
    pub resolve_batches: u64,
    pub resolve_ids: u64,
    pub reverse_batches: u64,
    pub reverse_ids: u64,
}

#[derive(Debug, Default)]
struct IdentityContextStatsAtomic {
    allocate_batches: AtomicU64,
    allocate_ids: AtomicU64,
    resolve_batches: AtomicU64,
    resolve_ids: AtomicU64,
    reverse_batches: AtomicU64,
    reverse_ids: AtomicU64,
}

impl IdentityContextStatsAtomic {
    fn snapshot(&self) -> IdentityContextStats {
        IdentityContextStats {
            allocate_batches: self.allocate_batches.load(Ordering::Relaxed),
            allocate_ids: self.allocate_ids.load(Ordering::Relaxed),
            resolve_batches: self.resolve_batches.load(Ordering::Relaxed),
            resolve_ids: self.resolve_ids.load(Ordering::Relaxed),
            reverse_batches: self.reverse_batches.load(Ordering::Relaxed),
            reverse_ids: self.reverse_ids.load(Ordering::Relaxed),
        }
    }
}

impl std::fmt::Debug for IdentityContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IdentityContext")
            .field(
                "forward_entries",
                &self
                    .shared
                    .forward
                    .lock()
                    .expect("identity context forward cache")
                    .len(),
            )
            .field(
                "reverse_entries",
                &self
                    .shared
                    .reverse
                    .lock()
                    .expect("identity context reverse cache")
                    .len(),
            )
            .finish_non_exhaustive()
    }
}

impl IdentityContext {
    pub fn new(inner: SharedIdentityReader) -> Self {
        Self::new_with_deadline(inner, None)
    }

    pub fn new_with_deadline(inner: SharedIdentityReader, deadline: Option<Instant>) -> Self {
        Self {
            inner,
            shared: std::sync::Arc::new(IdentityContextShared {
                forward: Mutex::new(HashMap::new()),
                reverse: Mutex::new(HashMap::new()),
                forward_flight: AsyncMutex::new(HashMap::new()),
                reverse_flight: AsyncMutex::new(HashMap::new()),
            }),
            stats: std::sync::Arc::new(IdentityContextStatsAtomic::default()),
            deadline,
        }
    }

    /// Create a view for a detached side effect. It shares request-local
    /// mappings and in-flight single-flight state with the main request, but
    /// starts an independent bill and absolute budget for that side-effect
    /// lifecycle. The main RPC deadline is never extended.
    pub(crate) fn for_side_effect(&self, budget: Duration) -> Self {
        Self {
            inner: std::sync::Arc::clone(&self.inner),
            shared: std::sync::Arc::clone(&self.shared),
            stats: std::sync::Arc::new(IdentityContextStatsAtomic::default()),
            deadline: Some(Instant::now() + budget),
        }
    }

    async fn resolve_inner(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        match self.deadline {
            Some(deadline) => {
                let timeout = remaining(deadline)?;
                bounded(
                    self.inner.resolve_batch_with_timeout(ids, timeout),
                    timeout,
                    "identity resolve",
                )
                .await
            }
            None => self.inner.resolve_batch(ids).await,
        }
    }

    async fn resolve_partial_inner(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
        match self.deadline {
            Some(deadline) => {
                let timeout = remaining(deadline)?;
                bounded(
                    self.inner.resolve_batch_partial_with_timeout(ids, timeout),
                    timeout,
                    "identity partial resolve",
                )
                .await
            }
            None => self.inner.resolve_batch_partial(ids).await,
        }
    }

    async fn reverse_inner(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> anyhow::Result<Vec<String>> {
        match self.deadline {
            Some(deadline) => {
                let timeout = remaining(deadline)?;
                bounded(
                    self.inner.reverse_batch_with_timeout(ids, timeout),
                    timeout,
                    "identity reverse",
                )
                .await
            }
            None => self.inner.reverse_batch(ids).await,
        }
    }

    /// Return the request-local Registry bill after any completed operations.
    pub fn stats(&self) -> IdentityContextStats {
        self.stats.snapshot()
    }

    async fn lock_forward_keys(&self, keys: &[ForwardIdentityKey]) -> Vec<OwnedMutexGuard<()>> {
        let mut keys = keys.to_vec();
        keys.sort_by(|left, right| {
            entity_kind_rank(left.0)
                .cmp(&entity_kind_rank(right.0))
                .then_with(|| left.1.cmp(&right.1))
        });
        keys.dedup();
        let locks = {
            let mut flight = self.shared.forward_flight.lock().await;
            keys.into_iter()
                .map(|key| {
                    flight
                        .entry(key)
                        .or_insert_with(|| std::sync::Arc::new(AsyncMutex::new(())))
                        .clone()
                })
                .collect::<Vec<_>>()
        };
        let mut guards = Vec::with_capacity(locks.len());
        for lock in locks {
            guards.push(lock.lock_owned().await);
        }
        guards
    }

    async fn lock_reverse_keys(&self, keys: &[ReverseIdentityKey]) -> Vec<OwnedMutexGuard<()>> {
        let mut keys = keys.to_vec();
        keys.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| entity_kind_rank(left.1).cmp(&entity_kind_rank(right.1)))
        });
        keys.dedup();
        let locks = {
            let mut flight = self.shared.reverse_flight.lock().await;
            keys.into_iter()
                .map(|key| {
                    flight
                        .entry(key)
                        .or_insert_with(|| std::sync::Arc::new(AsyncMutex::new(())))
                        .clone()
                })
                .collect::<Vec<_>>()
        };
        let mut guards = Vec::with_capacity(locks.len());
        for lock in locks {
            guards.push(lock.lock_owned().await);
        }
        guards
    }

    fn read_forward_cache(
        &self,
        ids: &[(String, EntityKind)],
        retry_missing: bool,
    ) -> (Vec<Option<SnowflakeId>>, ForwardMissPlan) {
        let mut result = vec![None; ids.len()];
        let mut plan = ForwardMissPlan::default();
        let mut missing_index = HashMap::new();
        let cache = self
            .shared
            .forward
            .lock()
            .expect("identity context forward cache");
        for (index, (object_id, kind)) in ids.iter().enumerate() {
            match cache.get(&(*kind, object_id.clone())) {
                Some(ForwardCacheEntry::Found(value)) => result[index] = Some(*value),
                Some(ForwardCacheEntry::Missing) if !retry_missing => {}
                Some(ForwardCacheEntry::Missing) | None => {
                    let key = (*kind, object_id.clone());
                    let unique_index = match missing_index.get(&key) {
                        Some(unique_index) => *unique_index,
                        None => {
                            let unique_index = plan.missing.len();
                            missing_index.insert(key, unique_index);
                            plan.missing.push((object_id.clone(), *kind));
                            unique_index
                        }
                    };
                    plan.positions.push((index, unique_index));
                }
            }
        }
        (result, plan)
    }

    async fn read_and_lock_forward(
        &self,
        ids: &[(String, EntityKind)],
        retry_missing: bool,
    ) -> (
        Vec<Option<SnowflakeId>>,
        Vec<OwnedMutexGuard<()>>,
        ForwardMissPlan,
    ) {
        let (result, initial) = self.read_forward_cache(ids, retry_missing);
        if initial.missing.is_empty() {
            return (result, Vec::new(), initial);
        }
        let keys = initial
            .missing
            .iter()
            .map(|(object_id, kind)| (*kind, object_id.clone()))
            .collect::<Vec<_>>();
        let guards = self.lock_forward_keys(&keys).await;
        let (result, plan) = self.read_forward_cache(ids, retry_missing);
        (result, guards, plan)
    }

    fn remember_forward(
        &self,
        requests: &[(String, EntityKind)],
        resolved: &[SnowflakeId],
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            requests.len() == resolved.len(),
            "identity context result count mismatch"
        );
        let mut forward = self
            .shared
            .forward
            .lock()
            .expect("identity context forward cache");
        let mut reverse = self
            .shared
            .reverse
            .lock()
            .expect("identity context reverse cache");
        for ((object_id, kind), snowflake) in requests.iter().zip(resolved) {
            forward.insert(
                (*kind, object_id.clone()),
                ForwardCacheEntry::Found(*snowflake),
            );
            reverse.insert((*snowflake, *kind), object_id.clone());
        }
        Ok(())
    }

    fn remember_reverse(
        &self,
        requests: &[(SnowflakeId, EntityKind)],
        object_ids: &[String],
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            requests.len() == object_ids.len(),
            "identity context result count mismatch"
        );
        let mut forward = self
            .shared
            .forward
            .lock()
            .expect("identity context forward cache");
        let mut reverse = self
            .shared
            .reverse
            .lock()
            .expect("identity context reverse cache");
        for ((snowflake, kind), object_id) in requests.iter().zip(object_ids) {
            reverse.insert((*snowflake, *kind), object_id.clone());
            forward.insert(
                (*kind, object_id.clone()),
                ForwardCacheEntry::Found(*snowflake),
            );
        }
        Ok(())
    }

    fn remember_forward_partial(
        &self,
        requests: &[(String, EntityKind)],
        resolved: &[Option<SnowflakeId>],
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            requests.len() == resolved.len(),
            "identity context result count mismatch"
        );
        let mut forward = self
            .shared
            .forward
            .lock()
            .expect("identity context forward cache");
        let mut reverse = self
            .shared
            .reverse
            .lock()
            .expect("identity context reverse cache");
        for ((object_id, kind), value) in requests.iter().zip(resolved) {
            match value {
                Some(snowflake) => {
                    forward.insert(
                        (*kind, object_id.clone()),
                        ForwardCacheEntry::Found(*snowflake),
                    );
                    reverse.insert((*snowflake, *kind), object_id.clone());
                }
                None => {
                    forward.insert((*kind, object_id.clone()), ForwardCacheEntry::Missing);
                }
            }
        }
        Ok(())
    }

    async fn resolve_cached(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let (mut result, _flight, plan) = self.read_and_lock_forward(ids, true).await;
        if !plan.missing.is_empty() {
            let missing = &plan.missing;
            let missing_positions = &plan.positions;
            self.stats.resolve_batches.fetch_add(1, Ordering::Relaxed);
            self.stats
                .resolve_ids
                .fetch_add(missing.len() as u64, Ordering::Relaxed);
            let resolved = self.resolve_inner(missing).await?;
            self.remember_forward(missing, &resolved)?;
            for &(position, unique_index) in missing_positions {
                result[position] = Some(resolved[unique_index]);
            }
        }
        result
            .into_iter()
            .map(|value| value.ok_or_else(|| anyhow::anyhow!("identity context missing result")))
            .collect()
    }

    async fn resolve_partial_cached(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let (mut result, _flight, plan) = self.read_and_lock_forward(ids, false).await;
        if !plan.missing.is_empty() {
            self.stats.resolve_batches.fetch_add(1, Ordering::Relaxed);
            self.stats
                .resolve_ids
                .fetch_add(plan.missing.len() as u64, Ordering::Relaxed);
            let resolved = self.resolve_partial_inner(&plan.missing).await?;
            anyhow::ensure!(
                resolved.len() == plan.missing.len(),
                "identity context partial result count mismatch"
            );
            self.remember_forward_partial(&plan.missing, &resolved)?;
            for &(position, unique_index) in &plan.positions {
                result[position] = resolved[unique_index];
            }
        }
        Ok(result)
    }

    fn read_reverse_cache(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> (Vec<Option<String>>, ReverseMissPlan) {
        let mut result = vec![None; ids.len()];
        let mut plan = ReverseMissPlan::default();
        let mut missing_index = HashMap::new();
        let cache = self
            .shared
            .reverse
            .lock()
            .expect("identity context reverse cache");
        for (index, key) in ids.iter().enumerate() {
            match cache.get(key) {
                Some(value) => result[index] = Some(value.clone()),
                None => {
                    let unique_index = match missing_index.get(key) {
                        Some(unique_index) => *unique_index,
                        None => {
                            let unique_index = plan.missing.len();
                            missing_index.insert(*key, unique_index);
                            plan.missing.push(*key);
                            unique_index
                        }
                    };
                    plan.positions.push((index, unique_index));
                }
            }
        }
        (result, plan)
    }

    async fn read_and_lock_reverse(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> (
        Vec<Option<String>>,
        Vec<OwnedMutexGuard<()>>,
        ReverseMissPlan,
    ) {
        let (result, initial) = self.read_reverse_cache(ids);
        if initial.missing.is_empty() {
            return (result, Vec::new(), initial);
        }
        let guards = self.lock_reverse_keys(&initial.missing).await;
        let (result, plan) = self.read_reverse_cache(ids);
        (result, guards, plan)
    }

    async fn allocate_cached_with(
        &self,
        allocator: &dyn IdentityAllocator,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let (mut result, _flight, plan) = self.read_and_lock_forward(ids, true).await;
        if !plan.missing.is_empty() {
            self.stats.allocate_batches.fetch_add(1, Ordering::Relaxed);
            self.stats
                .allocate_ids
                .fetch_add(plan.missing.len() as u64, Ordering::Relaxed);
            let resolved = match self.deadline {
                Some(deadline) => {
                    let timeout = remaining(deadline)?;
                    bounded(
                        allocator.allocate_batch_with_timeout(&plan.missing, timeout),
                        timeout,
                        "identity allocation",
                    )
                    .await?
                }
                None => allocator.allocate_batch(&plan.missing).await?,
            };
            self.remember_forward(&plan.missing, &resolved)?;
            for &(position, unique_index) in &plan.positions {
                result[position] = Some(resolved[unique_index]);
            }
        }
        result
            .into_iter()
            .map(|value| value.ok_or_else(|| anyhow::anyhow!("identity context missing result")))
            .collect()
    }
}

#[tonic::async_trait]
impl IdentityReader for IdentityContext {
    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        self.resolve_cached(ids).await
    }

    async fn resolve_batch_partial(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
        self.resolve_partial_cached(ids).await
    }

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> anyhow::Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let (mut result, _flight, plan) = self.read_and_lock_reverse(ids).await;
        if !plan.missing.is_empty() {
            self.stats.reverse_batches.fetch_add(1, Ordering::Relaxed);
            self.stats
                .reverse_ids
                .fetch_add(plan.missing.len() as u64, Ordering::Relaxed);
            let object_ids = self.reverse_inner(&plan.missing).await?;
            self.remember_reverse(&plan.missing, &object_ids)?;
            for &(position, unique_index) in &plan.positions {
                result[position] = Some(object_ids[unique_index].clone());
            }
        }
        result
            .into_iter()
            .map(|value| value.ok_or_else(|| anyhow::anyhow!("identity context missing result")))
            .collect()
    }
}

impl Default for IdentityContext {
    fn default() -> Self {
        Self::new(std::sync::Arc::new(PaddedIdentityResolver::new()))
    }
}

/// Explicit write-capable view of a request identity context. Read-only
/// adapters receive `IdentityContext`; only registration boundaries receive
/// this type.
pub struct IdentityRegistrationContext {
    reader: std::sync::Arc<IdentityContext>,
    allocator: SharedIdentityIngress,
}

impl IdentityRegistrationContext {
    pub fn new(reader: std::sync::Arc<IdentityContext>, allocator: SharedIdentityIngress) -> Self {
        Self { reader, allocator }
    }

    pub fn reader(&self) -> std::sync::Arc<IdentityContext> {
        std::sync::Arc::clone(&self.reader)
    }
}

impl std::fmt::Debug for IdentityRegistrationContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IdentityRegistrationContext")
            .field("reader", &self.reader)
            .finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl IdentityReader for IdentityRegistrationContext {
    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        self.reader.resolve_batch(ids).await
    }

    async fn resolve_batch_partial(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
        self.reader.resolve_batch_partial(ids).await
    }

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> anyhow::Result<Vec<String>> {
        self.reader.reverse_batch(ids).await
    }
}

#[tonic::async_trait]
impl IdentityAllocator for IdentityRegistrationContext {
    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        self.reader
            .allocate_cached_with(self.allocator.as_ref(), ids)
            .await
    }
}

#[tonic::async_trait]
impl IdentityReader for RegistryClient {
    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        RegistryClient::resolve_batch(self, ids).await
    }

    async fn resolve_batch_partial(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
        RegistryClient::resolve_batch_partial(self, ids).await
    }

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> anyhow::Result<Vec<String>> {
        RegistryClient::reverse_batch(self, ids).await
    }

    async fn resolve_batch_with_timeout(
        &self,
        ids: &[(String, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        RegistryClient::resolve_batch_with_timeout(self, ids, timeout).await
    }

    async fn resolve_batch_partial_with_timeout(
        &self,
        ids: &[(String, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
        RegistryClient::resolve_batch_partial_with_timeout(self, ids, timeout).await
    }

    async fn reverse_batch_with_timeout(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<String>> {
        RegistryClient::reverse_batch_with_timeout(self, ids, timeout).await
    }
}

#[tonic::async_trait]
impl IdentityAllocator for RegistryClient {
    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        let request = ids
            .iter()
            .map(|(id, kind)| (id.clone(), *kind, None))
            .collect::<Vec<_>>();
        RegistryClient::allocate_batch(self, &request).await
    }

    async fn allocate_batch_with_timeout(
        &self,
        ids: &[(String, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        let request = ids
            .iter()
            .map(|(id, kind)| (id.clone(), *kind, None))
            .collect::<Vec<_>>();
        RegistryClient::allocate_batch_with_timeout(self, &request, timeout).await
    }
}

/// Deterministic resolver for tests and compatibility constructors that run
/// without external services. It only accepts the zero-padded ObjectId form
/// `00000000 + 16 hex` produced by `ObjectId::from_u64_be_padded`; real
/// ObjectIds are rejected instead of being silently remapped.
#[derive(Default)]
pub struct PaddedIdentityResolver;

impl PaddedIdentityResolver {
    pub fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl IdentityReader for PaddedIdentityResolver {
    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        ids.iter()
            .map(|(object_id, _)| {
                let parsed = crate::models::ObjectId::parse(object_id)
                    .map_err(|error| anyhow::anyhow!("padded resolver: {error}"))?;
                let value = parsed
                    .to_u64_be_padded()
                    .filter(|value| *value != 0 && *value <= i64::MAX as u64)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "padded resolver cannot map ObjectId {object_id}; use the ID Registry"
                        )
                    })?;
                SnowflakeId::new(value).map_err(Into::into)
            })
            .collect()
    }

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> anyhow::Result<Vec<String>> {
        ids.iter()
            .map(|(id, _)| {
                anyhow::ensure!(
                    !id.is_nil() && id.get() <= i64::MAX as u64,
                    "padded resolver cannot reverse Snowflake {id}"
                );
                Ok(crate::models::ObjectId::from_u64_be_padded(id.get()).to_string())
            })
            .collect()
    }
}

#[tonic::async_trait]
impl IdentityAllocator for PaddedIdentityResolver {
    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        IdentityReader::resolve_batch(self, ids).await
    }
}

#[derive(Deserialize)]
struct ResolvedId {
    object_id: String,
    entity_kind: EntityKind,
    snowflake_id: u64,
    mapping_version: u32,
}

#[derive(Deserialize)]
struct ResolvedPartialId {
    object_id: String,
    entity_kind: EntityKind,
    snowflake_id: Option<u64>,
    mapping_version: u32,
}

#[derive(Deserialize)]
struct ReversedId {
    snowflake_id: u64,
    object_id: String,
    entity_kind: EntityKind,
    mapping_version: u32,
}

#[derive(Clone)]
struct HttpRegistryTransport {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    allocation_token: Option<String>,
}

#[derive(Clone)]
struct GrpcRegistryTransport {
    endpoint: Endpoint,
    channel: std::sync::Arc<OnceCell<Channel>>,
    allocation_token: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}

#[tonic::async_trait]
trait RegistryTransport: Send + Sync {
    fn resolve_method(&self) -> &'static str;
    fn resolve_partial_method(&self) -> &'static str;
    fn allocate_method(&self) -> &'static str;
    fn reverse_method(&self) -> &'static str;

    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>>;

    async fn resolve_batch_partial(
        &self,
        ids: &[(String, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedPartialId>>;

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ReversedId>>;

    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>>;
}

fn http_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .connect_timeout(ID_REGISTRY_REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()?)
}

fn entity_kind_rank(kind: EntityKind) -> u8 {
    match kind {
        EntityKind::User => 0,
        EntityKind::Post => 1,
    }
}

async fn bounded<T>(
    future: impl Future<Output = anyhow::Result<T>>,
    timeout: Duration,
    operation: &str,
) -> anyhow::Result<T> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| anyhow::anyhow!("{operation} deadline exceeded"))?
}

fn remaining(deadline: Instant) -> anyhow::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| anyhow::anyhow!("ID Registry request deadline exceeded"))
}

impl HttpRegistryTransport {
    fn new(endpoint: &str) -> anyhow::Result<Self> {
        let endpoint = reqwest::Url::parse(endpoint)?;
        anyhow::ensure!(
            matches!(endpoint.scheme(), "http" | "https"),
            "ID_REGISTRY_URL must use HTTP or HTTPS"
        );
        Ok(Self {
            client: http_client()?,
            endpoint,
            allocation_token: std::env::var("HOME_MIXER_ID_REGISTRY_ALLOCATION_TOKEN")
                .ok()
                .filter(|token| !token.is_empty()),
        })
    }
}

#[tonic::async_trait]
impl RegistryTransport for HttpRegistryTransport {
    fn resolve_method(&self) -> &'static str {
        "resolve_http"
    }

    fn resolve_partial_method(&self) -> &'static str {
        "resolve_partial_http"
    }

    fn allocate_method(&self) -> &'static str {
        "allocate_http"
    }

    fn reverse_method(&self) -> &'static str {
        "reverse_http"
    }

    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>> {
        let payload = ids
            .iter()
            .map(|(id, kind, provided)| {
                let mut value = serde_json::json!({
                    "object_id": id,
                    "entity_kind": kind,
                });
                if let Some(provided) = provided {
                    value["snowflake_id"] = serde_json::json!(provided.get());
                }
                value
            })
            .collect::<Vec<_>>();
        Ok(self
            .client
            .post(self.endpoint.join("/v1/resolve:batch")?)
            .json(&serde_json::json!({"ids": payload}))
            .timeout(timeout)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn resolve_batch_partial(
        &self,
        ids: &[(String, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedPartialId>> {
        let payload = ids
            .iter()
            .map(|(id, kind)| serde_json::json!({"object_id": id, "entity_kind": kind}))
            .collect::<Vec<_>>();
        Ok(self
            .client
            .post(self.endpoint.join("/v1/resolve_partial:batch")?)
            .json(&serde_json::json!({"ids": payload}))
            .timeout(timeout)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ReversedId>> {
        let payload = serde_json::json!({
            "ids": ids
                .iter()
                .map(|(id, kind)| {
                    serde_json::json!({"snowflake_id": id.get(), "entity_kind": kind})
                })
                .collect::<Vec<_>>()
        });
        Ok(self
            .client
            .post(self.endpoint.join("/v1/reverse:batch")?)
            .json(&payload)
            .timeout(timeout)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>> {
        let payload = ids
            .iter()
            .map(|(id, kind, provided)| {
                let mut value = serde_json::json!({"object_id": id, "entity_kind": kind});
                if let Some(provided) = provided {
                    value["snowflake_id"] = serde_json::json!(provided.get());
                }
                value
            })
            .collect::<Vec<_>>();
        let mut request = self
            .client
            .post(self.endpoint.join("/v1/allocate:batch")?)
            .json(&serde_json::json!({"ids": payload}));
        if let Some(token) = &self.allocation_token {
            request = request.header("x-id-registry-allocation-token", token);
        }
        Ok(request
            .timeout(timeout)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

impl GrpcRegistryTransport {
    fn new(endpoint: &str) -> anyhow::Result<Self> {
        Ok(Self {
            endpoint: Endpoint::from_shared(endpoint.to_string())?
                .connect_timeout(ID_REGISTRY_REQUEST_TIMEOUT)
                .timeout(ID_REGISTRY_REQUEST_TIMEOUT),
            channel: std::sync::Arc::new(OnceCell::new()),
            allocation_token: std::env::var("HOME_MIXER_ID_REGISTRY_ALLOCATION_TOKEN")
                .ok()
                .filter(|token| !token.is_empty())
                .map(|token| token.parse())
                .transpose()
                .map_err(|error| {
                    anyhow::anyhow!("invalid ID Registry allocation token: {error}")
                })?,
        })
    }

    async fn channel(&self) -> Channel {
        self.channel
            .get_or_init(|| async { self.endpoint.connect_lazy() })
            .await
            .clone()
    }
}

#[tonic::async_trait]
impl RegistryTransport for GrpcRegistryTransport {
    fn resolve_method(&self) -> &'static str {
        "resolve_grpc"
    }

    fn resolve_partial_method(&self) -> &'static str {
        "resolve_partial_grpc"
    }

    fn allocate_method(&self) -> &'static str {
        "allocate_grpc"
    }

    fn reverse_method(&self) -> &'static str {
        "reverse_grpc"
    }

    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>> {
        let request = registry_pb::ResolveBatchRequest {
            ids: ids
                .iter()
                .map(|(object_id, kind, provided)| registry_pb::ResolveRequest {
                    object_id: object_id.clone(),
                    entity_kind: proto_kind(*kind),
                    snowflake_id: provided.map(SnowflakeId::get),
                })
                .collect(),
        };
        let mut request = tonic::Request::new(request);
        request.set_timeout(timeout);
        let response =
            registry_pb::identity_registry_service_client::IdentityRegistryServiceClient::new(
                self.channel().await,
            )
            .resolve_batch(request)
            .await?
            .into_inner();
        response
            .rows
            .into_iter()
            .map(|row| {
                Ok(ResolvedId {
                    object_id: row.object_id,
                    entity_kind: entity_kind(row.entity_kind)?,
                    snowflake_id: row.snowflake_id,
                    mapping_version: row.mapping_version,
                })
            })
            .collect()
    }

    async fn resolve_batch_partial(
        &self,
        ids: &[(String, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedPartialId>> {
        let request = registry_pb::ResolveBatchRequest {
            ids: ids
                .iter()
                .map(|(object_id, kind)| registry_pb::ResolveRequest {
                    object_id: object_id.clone(),
                    entity_kind: proto_kind(*kind),
                    snowflake_id: None,
                })
                .collect(),
        };
        let mut request = tonic::Request::new(request);
        request.set_timeout(timeout);
        let response =
            registry_pb::identity_registry_service_client::IdentityRegistryServiceClient::new(
                self.channel().await,
            )
            .resolve_batch_partial(request)
            .await?
            .into_inner();
        response
            .rows
            .into_iter()
            .map(|row| {
                Ok(ResolvedPartialId {
                    object_id: row.object_id,
                    entity_kind: entity_kind(row.entity_kind)?,
                    snowflake_id: row.snowflake_id,
                    mapping_version: row.mapping_version,
                })
            })
            .collect()
    }

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ReversedId>> {
        let request = registry_pb::ReverseBatchRequest {
            ids: ids
                .iter()
                .map(|(id, kind)| registry_pb::ReverseRequest {
                    snowflake_id: id.get(),
                    entity_kind: proto_kind(*kind),
                })
                .collect(),
        };
        let mut request = tonic::Request::new(request);
        request.set_timeout(timeout);
        let response =
            registry_pb::identity_registry_service_client::IdentityRegistryServiceClient::new(
                self.channel().await,
            )
            .reverse_batch(request)
            .await?
            .into_inner();
        response
            .rows
            .into_iter()
            .map(|row| {
                Ok(ReversedId {
                    snowflake_id: row.snowflake_id,
                    object_id: row.object_id,
                    entity_kind: entity_kind(row.entity_kind)?,
                    mapping_version: row.mapping_version,
                })
            })
            .collect()
    }

    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>> {
        let request = registry_pb::ResolveBatchRequest {
            ids: ids
                .iter()
                .map(|(object_id, kind, provided)| registry_pb::ResolveRequest {
                    object_id: object_id.clone(),
                    entity_kind: proto_kind(*kind),
                    snowflake_id: provided.map(SnowflakeId::get),
                })
                .collect(),
        };
        let mut request = tonic::Request::new(request);
        request.set_timeout(timeout);
        if let Some(token) = &self.allocation_token {
            request
                .metadata_mut()
                .insert("x-id-registry-allocation-token", token.clone());
        }
        let response =
            registry_pb::identity_registry_service_client::IdentityRegistryServiceClient::new(
                self.channel().await,
            )
            .allocate_batch(request)
            .await?
            .into_inner();
        response
            .rows
            .into_iter()
            .map(|row| {
                Ok(ResolvedId {
                    object_id: row.object_id,
                    entity_kind: entity_kind(row.entity_kind)?,
                    snowflake_id: row.snowflake_id,
                    mapping_version: row.mapping_version,
                })
            })
            .collect()
    }
}

fn proto_kind(kind: EntityKind) -> i32 {
    match kind {
        EntityKind::User => registry_pb::EntityKind::User as i32,
        EntityKind::Post => registry_pb::EntityKind::Post as i32,
    }
}

fn entity_kind(value: i32) -> anyhow::Result<EntityKind> {
    match registry_pb::EntityKind::try_from(value).unwrap_or(registry_pb::EntityKind::Unspecified) {
        registry_pb::EntityKind::User => Ok(EntityKind::User),
        registry_pb::EntityKind::Post => Ok(EntityKind::Post),
        registry_pb::EntityKind::Unspecified => Err(anyhow::anyhow!(
            "ID Registry returned an unspecified entity kind"
        )),
    }
}

fn validate_resolve_rows(
    rows: Vec<ResolvedId>,
    ids: &[(String, EntityKind, Option<SnowflakeId>)],
) -> anyhow::Result<Vec<SnowflakeId>> {
    anyhow::ensure!(
        rows.len() == ids.len(),
        "ID Registry response count mismatch"
    );
    rows.into_iter()
        .zip(ids)
        .map(|(row, (id, kind, provided))| {
            anyhow::ensure!(
                row.object_id == *id && row.entity_kind == *kind,
                "ID Registry response identity mismatch"
            );
            anyhow::ensure!(
                row.mapping_version == id_service::MAPPING_VERSION,
                "ID Registry returned unsupported mapping_version {}",
                row.mapping_version
            );
            let resolved = SnowflakeId::new(row.snowflake_id)?;
            if let Some(provided) = provided {
                anyhow::ensure!(
                    resolved == *provided,
                    "ID Registry returned a mismatched provided Snowflake"
                );
            }
            Ok(resolved)
        })
        .collect()
}

fn validate_partial_resolve_rows(
    rows: Vec<ResolvedPartialId>,
    ids: &[(String, EntityKind)],
) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
    anyhow::ensure!(
        rows.len() == ids.len(),
        "ID Registry response count mismatch"
    );
    rows.into_iter()
        .zip(ids)
        .map(|(row, (id, kind))| {
            anyhow::ensure!(
                row.object_id == *id && row.entity_kind == *kind,
                "ID Registry response identity mismatch"
            );
            anyhow::ensure!(
                row.mapping_version == id_service::MAPPING_VERSION,
                "ID Registry returned unsupported mapping_version {}",
                row.mapping_version
            );
            row.snowflake_id
                .map(SnowflakeId::new)
                .transpose()
                .map_err(Into::into)
        })
        .collect()
}

fn validate_reverse_rows(
    rows: Vec<ReversedId>,
    ids: &[(SnowflakeId, EntityKind)],
) -> anyhow::Result<Vec<String>> {
    anyhow::ensure!(
        rows.len() == ids.len(),
        "ID Registry response count mismatch"
    );
    rows.into_iter()
        .zip(ids)
        .map(|(row, (id, kind))| {
            anyhow::ensure!(
                row.snowflake_id == id.get() && row.entity_kind == *kind,
                "ID Registry response identity mismatch"
            );
            anyhow::ensure!(
                row.mapping_version == id_service::MAPPING_VERSION,
                "ID Registry returned unsupported mapping_version {}",
                row.mapping_version
            );
            let object_id = crate::models::ObjectId::parse(&row.object_id)
                .map_err(|error| anyhow::anyhow!("ID Registry returned {error}"))?;
            anyhow::ensure!(!object_id.is_nil(), "ID Registry returned nil ObjectId");
            Ok(row.object_id)
        })
        .collect()
}

impl RegistryClient {
    /// Construct the legacy HTTP-only client. Production Home Mixer code uses
    /// [`Self::new_with_grpc`] so an RPC failure is returned directly.
    pub fn new(endpoint: &str) -> anyhow::Result<Self> {
        Ok(Self {
            transport: std::sync::Arc::new(HttpRegistryTransport::new(endpoint)?),
            calls: crate::metrics::ClientCallRecorder::default(),
        })
    }

    /// Build the production client. Home Mixer uses gRPC as its only Registry
    /// transport; the HTTP constructor above is reserved for legacy callers
    /// and compatibility tests.
    pub fn new_with_grpc(grpc_endpoint: &str) -> anyhow::Result<Self> {
        Ok(Self {
            transport: std::sync::Arc::new(GrpcRegistryTransport::new(grpc_endpoint)?),
            calls: crate::metrics::ClientCallRecorder::default(),
        })
    }

    /// Attach process metrics without changing the transport behavior.
    pub fn with_calls(mut self, calls: crate::metrics::ClientCallRecorder) -> Self {
        self.calls = calls;
        self
    }

    /// Read-only resolution: unknown ObjectIds come back as an error and
    /// nothing is written. Ingress adapters that may create mappings use
    /// [`Self::allocate_batch`] instead.
    pub async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        self.resolve_batch_with_timeout(ids, ID_REGISTRY_REQUEST_TIMEOUT)
            .await
    }

    pub async fn resolve_batch_with_timeout(
        &self,
        ids: &[(String, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let request = ids
            .iter()
            .map(|(id, kind)| (id.clone(), *kind, None))
            .collect::<Vec<_>>();
        let deadline = Instant::now() + timeout;
        let started = Instant::now();
        let method = self.transport.resolve_method();
        let rows = match self
            .transport
            .resolve_batch(&request, remaining(deadline)?)
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                self.calls.record(
                    "id_registry",
                    method,
                    registry_error_label(&error, "mapping_miss"),
                    started,
                );
                return Err(error);
            }
        };
        let result = validate_resolve_rows(rows, &request);
        self.calls.record(
            "id_registry",
            method,
            if result.is_ok() { "ok" } else { "rejected" },
            started,
        );
        result
    }

    pub async fn resolve_batch_partial(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
        self.resolve_batch_partial_with_timeout(ids, ID_REGISTRY_REQUEST_TIMEOUT)
            .await
    }

    pub async fn resolve_batch_partial_with_timeout(
        &self,
        ids: &[(String, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let request = ids.to_vec();
        let deadline = Instant::now() + timeout;
        let started = Instant::now();
        let method = self.transport.resolve_partial_method();
        let rows = match self
            .transport
            .resolve_batch_partial(&request, remaining(deadline)?)
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                self.calls.record(
                    "id_registry",
                    method,
                    registry_error_label(&error, "mapping_miss"),
                    started,
                );
                return Err(error);
            }
        };
        let result = validate_partial_resolve_rows(rows, ids);
        self.calls.record(
            "id_registry",
            method,
            if result.is_ok() { "ok" } else { "rejected" },
            started,
        );
        result
    }

    /// Allocate identities for an ingress adapter. Items carrying a
    /// caller-provided Snowflake are registered with that exact value;
    /// items without one receive a freshly allocated id. The Registry
    /// server still enforces whether allocation is enabled.
    pub async fn allocate_batch(
        &self,
        request: &[(String, EntityKind, Option<SnowflakeId>)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        self.allocate_batch_with_timeout(request, ID_REGISTRY_REQUEST_TIMEOUT)
            .await
    }

    pub async fn allocate_batch_with_timeout(
        &self,
        request: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        if request.is_empty() {
            return Ok(Vec::new());
        }
        let deadline = Instant::now() + timeout;
        let started = Instant::now();
        let method = self.transport.allocate_method();
        let rows = match self
            .transport
            .allocate_batch(request, remaining(deadline)?)
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                self.calls.record(
                    "id_registry",
                    method,
                    registry_error_label(&error, "mapping_miss"),
                    started,
                );
                return Err(error);
            }
        };
        let result = validate_resolve_rows(rows, request);
        if result.is_ok() {
            self.calls.record("id_registry", method, "ok", started);
        } else {
            self.calls
                .record("id_registry", method, "rejected", started);
        }
        result
    }

    pub async fn resolve_one(
        &self,
        object_id: &str,
        entity_kind: EntityKind,
    ) -> anyhow::Result<SnowflakeId> {
        self.resolve_batch(&[(object_id.to_string(), entity_kind)])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("ID Registry returned no result"))
    }

    pub async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> anyhow::Result<Vec<String>> {
        self.reverse_batch_with_timeout(ids, ID_REGISTRY_REQUEST_TIMEOUT)
            .await
    }

    pub async fn reverse_batch_with_timeout(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let deadline = Instant::now() + timeout;
        let started = std::time::Instant::now();
        let method = self.transport.reverse_method();
        let rows = match self
            .transport
            .reverse_batch(ids, remaining(deadline)?)
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                self.calls.record(
                    "id_registry",
                    method,
                    registry_error_label(&error, "reverse_miss"),
                    started,
                );
                return Err(error);
            }
        };
        let result = validate_reverse_rows(rows, ids);
        self.calls.record(
            "id_registry",
            method,
            if result.is_ok() { "ok" } else { "rejected" },
            started,
        );
        result
    }
}

fn registry_error_label(error: &anyhow::Error, not_found_label: &'static str) -> &'static str {
    // Only transport-level "not found" (gRPC NotFound / HTTP 404) counts as a
    // mapping miss; everything else — including client-side row validation
    // failures like an unsupported mapping_version — is a genuine error.
    let is_not_found = error.chain().any(|cause| {
        cause
            .downcast_ref::<tonic::Status>()
            .is_some_and(|status| status.code() == tonic::Code::NotFound)
            || cause
                .downcast_ref::<reqwest::Error>()
                .and_then(|error| error.status())
                .is_some_and(|status| status == reqwest::StatusCode::NOT_FOUND)
    });
    if is_not_found {
        not_found_label
    } else {
        "error"
    }
}

/// Whether an identity read failed because the requested mapping does not
/// exist. Transport failures must not be treated as stale client hints.
pub(crate) fn is_mapping_miss(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<tonic::Status>()
            .is_some_and(|status| status.code() == tonic::Code::NotFound)
            || cause
                .downcast_ref::<reqwest::Error>()
                .and_then(|error| error.status())
                .is_some_and(|status| status == reqwest::StatusCode::NOT_FOUND)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ObjectId;
    use axum::{routing::post, Json, Router};
    use id_service::{
        grpc::{GrpcIdRegistryService, IdentityRegistryServiceServer},
        MemoryMappingStore, RedisIdRegistry,
    };
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), server)
    }

    async fn serve_grpc(
        registry: Arc<RedisIdRegistry>,
        max_batch_size: usize,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpListenerStream::new(listener);
        let service = GrpcIdRegistryService::new(registry, max_batch_size);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(IdentityRegistryServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        (format!("http://{addr}"), server)
    }

    fn unused_endpoint() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{addr}")
    }

    async fn serve_blackhole() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut connections = Vec::new();
            loop {
                let (connection, _) = listener.accept().await.unwrap();
                // Keep the connection open without speaking HTTP/2. The client
                // must terminate the RPC using its configured request deadline.
                connections.push(connection);
            }
        });
        (format!("http://{addr}"), server)
    }

    #[tokio::test]
    async fn production_client_uses_the_real_grpc_server() {
        let store = Arc::new(MemoryMappingStore::new());
        let registry = Arc::new(RedisIdRegistry::with_store(store, 0, true).unwrap());
        let (grpc_endpoint, server) = serve_grpc(registry, 10).await;
        let client = RegistryClient::new_with_grpc(&grpc_endpoint).unwrap();
        let ids = [("65f1a2b3c4d5e6f708091011".to_string(), EntityKind::User)];

        let resolved = client
            .allocate_batch(&[(
                ids[0].0.clone(),
                ids[0].1,
                Some(SnowflakeId::new(4242).unwrap()),
            )])
            .await
            .unwrap();
        assert_eq!(resolved, vec![SnowflakeId::new(4242).unwrap()]);
        assert_eq!(
            client
                .reverse_batch(&[(resolved[0], EntityKind::User)])
                .await
                .unwrap(),
            vec![ids[0].0.clone()]
        );
        let partial = client
            .resolve_batch_partial(&[
                (ids[0].0.clone(), EntityKind::User),
                ("65f1a2b3c4d5e6f708091012".into(), EntityKind::User),
            ])
            .await
            .unwrap();
        assert_eq!(partial, vec![Some(SnowflakeId::new(4242).unwrap()), None]);
        server.abort();
    }

    #[tokio::test]
    async fn production_client_returns_grpc_error_without_http_fallback() {
        let metrics = crate::metrics::Metrics::new();
        let client = RegistryClient::new_with_grpc(&unused_endpoint())
            .unwrap()
            .with_calls(metrics.client_calls());

        let error = client
            .resolve_batch(&[("65f1a2b3c4d5e6f708091011".into(), EntityKind::User)])
            .await
            .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<tonic::Status>()
                .expect("transport errors preserve gRPC status")
                .code(),
            tonic::Code::Unavailable
        );
        let metrics_text = metrics.encode().unwrap();
        assert!(metrics_text.contains(
            "home_mixer_client_calls_total{client=\"id_registry\",method=\"resolve_grpc\",result=\"error\"} 1"
        ));
    }

    #[tokio::test]
    async fn empty_allocation_batch_does_not_call_registry() {
        let client = RegistryClient::new(&unused_endpoint()).unwrap();
        assert!(client.allocate_batch(&[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn production_client_respects_one_total_deadline_when_grpc_times_out() {
        let (grpc_endpoint, grpc_server) = serve_blackhole().await;
        let client = RegistryClient::new_with_grpc(&grpc_endpoint).unwrap();
        let started = std::time::Instant::now();

        let error = client
            .resolve_batch(&[("65f1a2b3c4d5e6f708091011".into(), EntityKind::User)])
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<tonic::Status>().is_some());
        assert!(started.elapsed() >= ID_REGISTRY_REQUEST_TIMEOUT);
        assert!(started.elapsed() < Duration::from_secs(1));
        grpc_server.abort();
    }

    #[tokio::test]
    async fn business_errors_from_grpc_are_returned_directly() {
        let store = Arc::new(MemoryMappingStore::new());
        let registry = Arc::new(RedisIdRegistry::with_store(store, 0, false).unwrap());
        let (grpc_endpoint, grpc_server) = serve_grpc(registry, 10).await;
        let client = RegistryClient::new_with_grpc(&grpc_endpoint).unwrap();

        let error = client
            .resolve_batch(&[("65f1a2b3c4d5e6f708091011".into(), EntityKind::User)])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no ObjectId mapping exists"));
        grpc_server.abort();
    }

    #[tokio::test]
    async fn registry_client_preserves_batch_order_and_rejects_mismatched_identity() {
        let app = Router::new().route("/v1/resolve:batch", post(|Json(body): Json<Value>| async move {
            let ids = body["ids"].as_array().unwrap();
            assert_eq!(ids.len(), 2);
            Json(json!([
                {"object_id": ids[0]["object_id"], "entity_kind": ids[0]["entity_kind"], "snowflake_id": 9007199254740993_u64, "mapping_version": 2},
                {"object_id": ids[1]["object_id"], "entity_kind": ids[1]["entity_kind"], "snowflake_id": 71, "mapping_version": 2}
            ]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        let ids = vec![
            ("65f1a2b3c4d5e6f708091011".into(), EntityKind::User),
            ("65f1a2b3c4d5e6f708091012".into(), EntityKind::Post),
        ];
        let resolved = client.resolve_batch(&ids).await.unwrap();
        assert_eq!(
            resolved.iter().map(|id| id.get()).collect::<Vec<_>>(),
            vec![9007199254740993, 71]
        );
        server.abort();

        let app = Router::new().route("/v1/resolve:batch", post(|| async {
            Json(json!([{"object_id": "65f1a2b3c4d5e6f708091012", "entity_kind": "User", "snowflake_id": 71, "mapping_version": 2}]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        assert!(client.resolve_batch(&ids[..1]).await.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn registry_client_partial_resolve_preserves_missing_rows() {
        let app = Router::new().route(
            "/v1/resolve_partial:batch",
            post(|Json(body): Json<Value>| async move {
                let ids = body["ids"].as_array().unwrap();
                Json(json!([
                    {
                        "object_id": ids[0]["object_id"],
                        "entity_kind": ids[0]["entity_kind"],
                        "snowflake_id": 71,
                        "mapping_version": 2
                    },
                    {
                        "object_id": ids[1]["object_id"],
                        "entity_kind": ids[1]["entity_kind"],
                        "snowflake_id": null,
                        "mapping_version": 2
                    }
                ]))
            }),
        );
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        let ids = vec![
            ("65f1a2b3c4d5e6f708091011".into(), EntityKind::User),
            ("65f1a2b3c4d5e6f708091012".into(), EntityKind::Post),
        ];
        assert_eq!(
            client.resolve_batch_partial(&ids).await.unwrap(),
            vec![Some(SnowflakeId::new(71).unwrap()), None]
        );
        server.abort();
    }

    #[tokio::test]
    async fn resolve_batch_rejects_an_unknown_mapping_version() {
        let app = Router::new().route("/v1/resolve:batch", post(|Json(body): Json<Value>| async move {
            let ids = body["ids"].as_array().unwrap();
            Json(json!([
                {"object_id": ids[0]["object_id"], "entity_kind": ids[0]["entity_kind"], "snowflake_id": 71, "mapping_version": 3}
            ]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        let error = client
            .resolve_batch(&[("65f1a2b3c4d5e6f708091011".into(), EntityKind::User)])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("mapping_version"));
        server.abort();
    }

    #[tokio::test]
    async fn allocation_contract_rejection_is_not_recorded_as_ok() {
        let app = Router::new().route(
            "/v1/allocate:batch",
            post(|Json(body): Json<Value>| async move {
                let ids = body["ids"].as_array().unwrap();
                Json(json!([{
                    "object_id": ids[0]["object_id"],
                    "entity_kind": ids[0]["entity_kind"],
                    "snowflake_id": 71,
                    "mapping_version": 3
                }]))
            }),
        );
        let (endpoint, server) = serve(app).await;
        let metrics = crate::metrics::Metrics::new();
        let client = RegistryClient::new(&endpoint)
            .unwrap()
            .with_calls(metrics.client_calls());

        let error = client
            .allocate_batch(&[("65f1a2b3c4d5e6f708091011".into(), EntityKind::User, None)])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("mapping_version"));

        let text = metrics.encode().unwrap();
        assert!(text.contains(
            "home_mixer_client_calls_total{client=\"id_registry\",method=\"allocate_http\",result=\"rejected\"} 1"
        ));
        assert!(!text.contains(
            "home_mixer_client_calls_total{client=\"id_registry\",method=\"allocate_http\",result=\"ok\"} 1"
        ));
        server.abort();
    }

    #[tokio::test]
    async fn reverse_batch_sends_kind_and_validates_identity_version_and_object_id() {
        let app = Router::new().route("/v1/reverse:batch", post(|Json(body): Json<Value>| async move {
            let ids = body["ids"].as_array().unwrap();
            assert_eq!(ids.len(), 2);
            assert_eq!(ids[0]["entity_kind"], "Post");
            assert_eq!(ids[1]["entity_kind"], "Post");
            Json(json!([
                {"snowflake_id": ids[0]["snowflake_id"], "object_id": "65f1a2b3c4d5e6f708091011", "entity_kind": "Post", "mapping_version": 2},
                {"snowflake_id": ids[1]["snowflake_id"], "object_id": "65f1a2b3c4d5e6f708091012", "entity_kind": "Post", "mapping_version": 2}
            ]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        let ids = vec![
            (SnowflakeId::new(42).unwrap(), EntityKind::Post),
            (SnowflakeId::new(43).unwrap(), EntityKind::Post),
        ];
        assert_eq!(
            client.reverse_batch(&ids).await.unwrap(),
            vec!["65f1a2b3c4d5e6f708091011", "65f1a2b3c4d5e6f708091012"]
        );
        server.abort();
    }

    #[tokio::test]
    async fn reverse_batch_rejects_wrong_kind_version_and_order() {
        let ids = vec![(SnowflakeId::new(42).unwrap(), EntityKind::Post)];

        // Wrong entity kind echoed back.
        let app = Router::new().route("/v1/reverse:batch", post(|| async {
            Json(json!([{"snowflake_id": 42, "object_id": "65f1a2b3c4d5e6f708091011", "entity_kind": "User", "mapping_version": 2}]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        assert!(client.reverse_batch(&ids).await.is_err());
        server.abort();

        // Unknown mapping version.
        let app = Router::new().route("/v1/reverse:batch", post(|| async {
            Json(json!([{"snowflake_id": 42, "object_id": "65f1a2b3c4d5e6f708091011", "entity_kind": "Post", "mapping_version": 99}]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        assert!(client.reverse_batch(&ids).await.is_err());
        server.abort();

        // Rows returned out of order.
        let app = Router::new().route("/v1/reverse:batch", post(|| async {
            Json(json!([
                {"snowflake_id": 43, "object_id": "65f1a2b3c4d5e6f708091012", "entity_kind": "Post", "mapping_version": 2},
                {"snowflake_id": 42, "object_id": "65f1a2b3c4d5e6f708091011", "entity_kind": "Post", "mapping_version": 2}
            ]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        let two = vec![
            (SnowflakeId::new(42).unwrap(), EntityKind::Post),
            (SnowflakeId::new(43).unwrap(), EntityKind::Post),
        ];
        assert!(client.reverse_batch(&two).await.is_err());
        server.abort();

        // Invalid ObjectId payload.
        let app = Router::new().route("/v1/reverse:batch", post(|| async {
            Json(json!([{"snowflake_id": 42, "object_id": "not-an-object-id", "entity_kind": "Post", "mapping_version": 2}]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        assert!(client.reverse_batch(&ids).await.is_err());
        server.abort();
    }

    #[derive(Default)]
    struct CountingIdentity {
        allocate_calls: std::sync::atomic::AtomicUsize,
        resolve_calls: std::sync::atomic::AtomicUsize,
        reverse_calls: std::sync::atomic::AtomicUsize,
    }

    #[tonic::async_trait]
    impl IdentityReader for CountingIdentity {
        async fn resolve_batch(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            self.resolve_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ids
                .iter()
                .enumerate()
                .map(|(index, _)| SnowflakeId::new(index as u64 + 1).unwrap())
                .collect())
        }

        async fn reverse_batch(
            &self,
            ids: &[(SnowflakeId, EntityKind)],
        ) -> anyhow::Result<Vec<String>> {
            self.reverse_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ids
                .iter()
                .map(|(id, _)| format!("65f1a2b3c4d5e6f70809{:04x}", id.get()))
                .collect())
        }
    }

    #[tonic::async_trait]
    impl IdentityAllocator for CountingIdentity {
        async fn allocate_batch(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            self.allocate_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ids
                .iter()
                .enumerate()
                .map(|(index, _)| SnowflakeId::new(index as u64 + 100).unwrap())
                .collect())
        }
    }

    #[derive(Default)]
    struct PartialMissIdentity {
        resolve_partial_calls: std::sync::atomic::AtomicUsize,
    }

    #[tonic::async_trait]
    impl IdentityReader for PartialMissIdentity {
        async fn resolve_batch(
            &self,
            _ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            Err(anyhow::anyhow!("mapping missing"))
        }

        async fn resolve_batch_partial(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
            self.resolve_partial_calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![None; ids.len()])
        }

        async fn reverse_batch(
            &self,
            ids: &[(SnowflakeId, EntityKind)],
        ) -> anyhow::Result<Vec<String>> {
            Ok(ids.iter().map(|_| "missing".to_string()).collect())
        }
    }

    #[derive(Default)]
    struct DeadlineIdentity {
        observed: std::sync::Mutex<Vec<Duration>>,
    }

    #[derive(Default)]
    struct ParallelIdentity {
        active: std::sync::atomic::AtomicUsize,
        max_active: std::sync::atomic::AtomicUsize,
        resolve_calls: std::sync::atomic::AtomicUsize,
    }

    struct IgnoresTimeoutIdentity;

    #[tonic::async_trait]
    impl IdentityReader for IgnoresTimeoutIdentity {
        async fn resolve_batch(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(ids.iter().map(|_| SnowflakeId::new(1).unwrap()).collect())
        }

        async fn reverse_batch(
            &self,
            ids: &[(SnowflakeId, EntityKind)],
        ) -> anyhow::Result<Vec<String>> {
            Ok(ids
                .iter()
                .map(|(id, _)| ObjectId::from_u64_be_padded(id.get()).to_string())
                .collect())
        }

        async fn resolve_batch_with_timeout(
            &self,
            ids: &[(String, EntityKind)],
            _timeout: Duration,
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            // Simulates a legacy adapter that overrides the timeout-aware
            // method but accidentally ignores the supplied timeout.
            self.resolve_batch(ids).await
        }
    }

    #[tonic::async_trait]
    impl IdentityReader for ParallelIdentity {
        async fn resolve_batch(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            self.resolve_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let active = self
                .active
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            self.max_active
                .fetch_max(active, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.active
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ids
                .iter()
                .enumerate()
                .map(|(index, _)| SnowflakeId::new(index as u64 + 1).unwrap())
                .collect())
        }

        async fn reverse_batch(
            &self,
            ids: &[(SnowflakeId, EntityKind)],
        ) -> anyhow::Result<Vec<String>> {
            Ok(ids
                .iter()
                .map(|(id, _)| format!("65f1a2b3c4d5e6f70809{:04x}", id.get()))
                .collect())
        }
    }

    #[tonic::async_trait]
    impl IdentityReader for DeadlineIdentity {
        async fn resolve_batch(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            Ok(ids.iter().map(|_| SnowflakeId::new(1).unwrap()).collect())
        }

        async fn reverse_batch(
            &self,
            ids: &[(SnowflakeId, EntityKind)],
        ) -> anyhow::Result<Vec<String>> {
            Ok(ids
                .iter()
                .map(|(id, _)| format!("65f1a2b3c4d5e6f70809{:04x}", id.get()))
                .collect())
        }

        async fn resolve_batch_with_timeout(
            &self,
            ids: &[(String, EntityKind)],
            timeout: Duration,
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            self.observed
                .lock()
                .expect("deadline observations")
                .push(timeout);
            self.resolve_batch(ids).await
        }
    }

    #[tonic::async_trait]
    impl IdentityAllocator for DeadlineIdentity {
        async fn allocate_batch(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            self.resolve_batch(ids).await
        }
    }

    #[tokio::test]
    async fn request_identity_context_reuses_forward_and_reverse_mappings() {
        let underlying = Arc::new(CountingIdentity::default());
        let context = Arc::new(IdentityContext::new(underlying.clone()));
        let registration = IdentityRegistrationContext::new(
            Arc::clone(&context),
            underlying.clone() as SharedIdentityIngress,
        );
        let object_id = "65f1a2b3c4d5e6f708091011".to_string();
        let request = vec![
            (object_id.clone(), EntityKind::User),
            (object_id.clone(), EntityKind::User),
        ];

        let first = registration.allocate_batch(&request).await.unwrap();
        let second = registration.allocate_batch(&request).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(
            underlying
                .allocate_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );

        let reverse_id = SnowflakeId::new(999).unwrap();
        let reverse_request = vec![
            (reverse_id, EntityKind::Post),
            (reverse_id, EntityKind::Post),
        ];
        assert_eq!(
            context.reverse_batch(&reverse_request).await.unwrap(),
            context.reverse_batch(&reverse_request).await.unwrap()
        );
        assert_eq!(
            underlying
                .reverse_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );

        assert_eq!(
            registration.reader().stats(),
            IdentityContextStats {
                allocate_batches: 1,
                allocate_ids: 1,
                resolve_batches: 0,
                resolve_ids: 0,
                reverse_batches: 1,
                reverse_ids: 1,
            }
        );

        // Allocation seeds the reverse cache, so the already-known user does
        // not cause another Registry round trip when an egress path asks for it.
        assert_eq!(
            context
                .reverse_batch(&[(first[0], EntityKind::User)])
                .await
                .unwrap(),
            vec![object_id]
        );
        assert_eq!(
            underlying
                .reverse_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn request_identity_context_counts_resolve_only_on_cache_misses() {
        let underlying = Arc::new(CountingIdentity::default());
        let context = IdentityContext::new(underlying);
        let ids = vec![
            ("65f1a2b3c4d5e6f708091011".to_string(), EntityKind::Post),
            ("65f1a2b3c4d5e6f708091011".to_string(), EntityKind::Post),
        ];

        context.resolve_batch(&ids).await.unwrap();
        context.resolve_batch(&ids).await.unwrap();

        assert_eq!(
            context.stats(),
            IdentityContextStats {
                allocate_batches: 0,
                allocate_ids: 0,
                resolve_batches: 1,
                resolve_ids: 1,
                reverse_batches: 0,
                reverse_ids: 0,
            }
        );
    }

    #[tokio::test]
    async fn request_identity_context_caches_partial_misses() {
        let underlying = Arc::new(PartialMissIdentity::default());
        let context = IdentityContext::new(underlying.clone());
        let ids = vec![("unknown".to_string(), EntityKind::Post)];

        let first = context.resolve_batch_partial(&ids).await.unwrap();
        let second = context.resolve_batch_partial(&ids).await.unwrap();

        assert_eq!(first, vec![None]);
        assert_eq!(second, first);
        assert_eq!(underlying.resolve_partial_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn request_identity_context_single_flight_is_per_key() {
        let underlying = Arc::new(ParallelIdentity::default());
        let context = Arc::new(IdentityContext::new(
            underlying.clone() as SharedIdentityReader
        ));

        let first = Arc::clone(&context);
        let second = Arc::clone(&context);
        let (first, second) = tokio::join!(
            async move {
                first
                    .resolve_batch(&[("first".to_string(), EntityKind::Post)])
                    .await
            },
            async move {
                second
                    .resolve_batch(&[("second".to_string(), EntityKind::Post)])
                    .await
            },
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(
            underlying
                .max_active
                .load(std::sync::atomic::Ordering::SeqCst),
            2,
            "unrelated keys should not wait on one direction-wide lock"
        );

        let same = Arc::new(IdentityContext::new(
            underlying.clone() as SharedIdentityReader
        ));
        let left = Arc::clone(&same);
        let right = Arc::clone(&same);
        let (left, right) = tokio::join!(
            async move {
                left.resolve_batch(&[("same".to_string(), EntityKind::Post)])
                    .await
            },
            async move {
                right
                    .resolve_batch(&[("same".to_string(), EntityKind::Post)])
                    .await
            },
        );
        left.unwrap();
        right.unwrap();
        assert_eq!(
            underlying
                .resolve_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            3,
            "the same key should still issue only one backend call"
        );
    }

    #[tokio::test]
    async fn request_identity_context_forwards_remaining_deadline() {
        let underlying = Arc::new(DeadlineIdentity::default());
        let context = IdentityContext::new_with_deadline(
            underlying.clone() as SharedIdentityReader,
            Some(Instant::now() + Duration::from_millis(50)),
        );
        context
            .resolve_batch(&[("65f1a2b3c4d5e6f708091011".to_string(), EntityKind::User)])
            .await
            .unwrap();

        let observed = underlying.observed.lock().expect("deadline observations");
        assert_eq!(observed.len(), 1);
        assert!(observed[0] <= Duration::from_millis(50));
        assert!(observed[0] > Duration::ZERO);
    }

    #[tokio::test]
    async fn default_identity_timeout_wrapper_bounds_slow_custom_readers() {
        let underlying = Arc::new(ParallelIdentity::default());
        let context = IdentityContext::new_with_deadline(
            underlying as SharedIdentityReader,
            Some(Instant::now() + Duration::from_millis(2)),
        );
        let error = context
            .resolve_batch(&[("slow".to_string(), EntityKind::Post)])
            .await
            .expect_err("the default timeout adapter must bound custom readers");
        assert!(error.to_string().contains("deadline"));
    }

    #[tokio::test]
    async fn identity_context_hard_bounds_timeout_overrides_that_ignore_budget() {
        let context = IdentityContext::new_with_deadline(
            Arc::new(IgnoresTimeoutIdentity) as SharedIdentityReader,
            Some(Instant::now() + Duration::from_millis(2)),
        );
        let started = Instant::now();
        let error = context
            .resolve_batch(&[("slow".to_string(), EntityKind::Post)])
            .await
            .expect_err("IdentityContext must enforce its own deadline");
        assert!(error.to_string().contains("deadline"));
        assert!(started.elapsed() < Duration::from_millis(80));
    }

    #[tokio::test]
    async fn side_effect_identity_view_restarts_budget_but_shares_cache_with_independent_bill() {
        let underlying = Arc::new(CountingIdentity::default());
        let context = IdentityContext::new_with_deadline(
            underlying.clone() as SharedIdentityReader,
            Some(Instant::now() - Duration::from_millis(1)),
        );
        let request = [("65f1a2b3c4d5e6f708091011".to_string(), EntityKind::Post)];

        assert!(context.resolve_batch(&request).await.is_err());
        let side_effect = context.for_side_effect(Duration::from_millis(20));
        assert!(side_effect.resolve_batch(&request).await.is_ok());

        // The detached view shares the request cache, so the expired main
        // view can still consume a mapping that was already resolved before
        // the response path ended.
        assert!(context.resolve_batch(&request).await.is_ok());
        assert_eq!(
            underlying
                .resolve_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(context.stats().resolve_batches, 1);
        assert_eq!(side_effect.stats().resolve_batches, 1);
    }
}
