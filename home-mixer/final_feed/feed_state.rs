use crate::candidate_pipeline::query::ScoredPostsQuery;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FeedStateSnapshot {
    pub served_post_ids: Vec<i64>,
    pub request_timestamps_ms: Vec<i64>,
}

pub trait FeedStateStore: Send + Sync {
    fn load(&self, user_id: i64) -> Result<FeedStateSnapshot, String>;
    fn record(
        &self,
        user_id: i64,
        served_post_ids: Vec<i64>,
        request_timestamp_ms: i64,
    ) -> Result<(), String>;
}

#[derive(Default)]
struct UserFeedState {
    served_post_ids: VecDeque<i64>,
    request_timestamps_ms: VecDeque<i64>,
}

#[derive(Default)]
struct FeedStateCache {
    users: HashMap<i64, UserFeedState>,
    least_to_most_recent: VecDeque<i64>,
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

impl FeedStateStore for InMemoryFeedStateStore {
    fn load(&self, user_id: i64) -> Result<FeedStateSnapshot, String> {
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

    fn record(
        &self,
        user_id: i64,
        served_post_ids: Vec<i64>,
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

fn touch_user(users: &mut VecDeque<i64>, user_id: i64) {
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

pub struct LocalFeedStateQueryHydrator {
    store: Arc<dyn FeedStateStore>,
}

impl LocalFeedStateQueryHydrator {
    pub fn new(store: Arc<dyn FeedStateStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for LocalFeedStateQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        let snapshot = self.store.load(query.user_id)?;
        let mut hydrated = query.clone();
        append_unique(&mut hydrated.served_ids, snapshot.served_post_ids);
        append_unique(
            &mut hydrated.past_request_timestamps_ms,
            snapshot.request_timestamps_ms,
        );
        Ok(hydrated)
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.served_ids = hydrated.served_ids;
        query.past_request_timestamps_ms = hydrated.past_request_timestamps_ms;
    }
}

fn append_unique<T: PartialEq>(target: &mut Vec<T>, values: Vec<T>) {
    for value in values {
        if !target.contains(&value) {
            target.push(value);
        }
    }
}
