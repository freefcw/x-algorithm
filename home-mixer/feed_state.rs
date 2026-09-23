//! Served-history and request-timestamp state contracts (U1).
//!
//! This domain module provides the store contract and the bounded in-memory
//! implementation used by tests and demo mode. Production adapters live under
//! `clients`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FeedStateSnapshot {
    pub served_post_ids: Vec<crate::models::PostId>,
    pub request_timestamps_ms: Vec<i64>,
}

#[tonic::async_trait]
pub trait FeedStateStore: Send + Sync {
    async fn load(&self, user_id: crate::models::UserId) -> Result<FeedStateSnapshot, String>;
    async fn record(
        &self,
        user_id: crate::models::UserId,
        served_post_ids: Vec<crate::models::PostId>,
        request_timestamp_ms: i64,
    ) -> Result<(), String>;

    /// Request-aware read hook. Stateless implementations can keep the
    /// legacy behavior; external-key adapters use the context to avoid a
    /// second Registry lookup in the same request.
    async fn load_with_identity(
        &self,
        user_id: crate::models::UserId,
        _identity: Arc<crate::id::IdentityContext>,
    ) -> Result<FeedStateSnapshot, String> {
        self.load(user_id).await
    }

    /// Request-aware write hook with the same compatibility default.
    async fn record_with_identity(
        &self,
        user_id: crate::models::UserId,
        served_post_ids: Vec<crate::models::PostId>,
        request_timestamp_ms: i64,
        _identity: Arc<crate::id::IdentityContext>,
    ) -> Result<(), String> {
        self.record(user_id, served_post_ids, request_timestamp_ms)
            .await
    }
}

#[derive(Default)]
struct UserFeedState {
    served_post_ids: VecDeque<crate::models::PostId>,
    request_timestamps_ms: VecDeque<i64>,
}

#[derive(Default)]
struct FeedStateCache {
    users: HashMap<crate::models::UserId, UserFeedState>,
    least_to_most_recent: VecDeque<crate::models::UserId>,
}

pub struct InMemoryFeedStateStore {
    cache: Mutex<FeedStateCache>,
    max_served_ids: usize,
    max_request_timestamps: usize,
    max_users: usize,
}

impl InMemoryFeedStateStore {
    pub fn new(max_served_ids: usize, max_request_timestamps: usize) -> Self {
        Self::with_max_users(
            max_served_ids,
            max_request_timestamps,
            crate::params::LOCAL_STATE_USER_LIMIT,
        )
    }

    pub fn with_max_users(
        max_served_ids: usize,
        max_request_timestamps: usize,
        max_users: usize,
    ) -> Self {
        Self {
            cache: Mutex::new(FeedStateCache::default()),
            max_served_ids,
            max_request_timestamps,
            max_users,
        }
    }
}

#[tonic::async_trait]
impl FeedStateStore for InMemoryFeedStateStore {
    async fn load(&self, user_id: crate::models::UserId) -> Result<FeedStateSnapshot, String> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "feed state lock poisoned".to_string())?;
        let snapshot = cache.users.get(&user_id).map(|state| FeedStateSnapshot {
            served_post_ids: state.served_post_ids.iter().copied().collect(),
            request_timestamps_ms: state.request_timestamps_ms.iter().copied().collect(),
        });
        if snapshot.is_some() {
            touch_user(&mut cache.least_to_most_recent, user_id);
        }
        Ok(snapshot.unwrap_or_default())
    }

    async fn record(
        &self,
        user_id: crate::models::UserId,
        served_post_ids: Vec<crate::models::PostId>,
        request_timestamp_ms: i64,
    ) -> Result<(), String> {
        if self.max_users == 0 {
            return Ok(());
        }
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "feed state lock poisoned".to_string())?;
        if !cache.users.contains_key(&user_id) && cache.users.len() >= self.max_users {
            evict_oldest_user(&mut cache);
        }

        let state = cache.users.entry(user_id).or_default();
        for post_id in served_post_ids {
            state
                .served_post_ids
                .retain(|existing| *existing != post_id);
            state.served_post_ids.push_back(post_id);
        }
        truncate_front(&mut state.served_post_ids, self.max_served_ids);
        state.request_timestamps_ms.push_back(request_timestamp_ms);
        truncate_front(
            &mut state.request_timestamps_ms,
            self.max_request_timestamps,
        );
        touch_user(&mut cache.least_to_most_recent, user_id);
        Ok(())
    }
}

fn touch_user(users: &mut VecDeque<crate::models::UserId>, user_id: crate::models::UserId) {
    users.retain(|existing| *existing != user_id);
    users.push_back(user_id);
}

fn evict_oldest_user(cache: &mut FeedStateCache) {
    while let Some(user_id) = cache.least_to_most_recent.pop_front() {
        if cache.users.remove(&user_id).is_some() {
            return;
        }
    }
}

fn truncate_front<T>(values: &mut VecDeque<T>, limit: usize) {
    while values.len() > limit {
        values.pop_front();
    }
}
