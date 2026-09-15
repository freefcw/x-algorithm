//! Served exposure persistence port.
//!
//! This is intentionally separate from request/feed state. A production
//! adapter must make the exposure event durable and idempotent.

use crate::feed_state::FeedStateStore;
use crate::models::ids::{PostId, UserId};
use std::sync::Arc;
use tonic::async_trait;

#[async_trait]
pub trait ServedPersistence: Send + Sync {
    async fn persist(
        &self,
        viewer_id: UserId,
        served_post_ids: &[PostId],
        request_time_ms: i64,
    ) -> Result<(), String>;
}

/// Persistence adapter backed by a [`FeedStateStore`]. The same type works
/// with the in-memory store in tests and the Redis store at runtime.
pub struct FeedStateServedPersistence {
    store: Arc<dyn FeedStateStore>,
}

impl FeedStateServedPersistence {
    pub fn new(store: Arc<dyn FeedStateStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ServedPersistence for FeedStateServedPersistence {
    async fn persist(
        &self,
        viewer_id: UserId,
        served_post_ids: &[PostId],
        request_time_ms: i64,
    ) -> Result<(), String> {
        self.store
            .record(viewer_id, served_post_ids.to_vec(), request_time_ms)
            .await
    }
}

/// Backward-compatible name for callers that explicitly want the test-only
/// in-memory wiring.
pub type InMemoryServedPersistence = FeedStateServedPersistence;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed_state::InMemoryFeedStateStore;
    use crate::models::{pid, uid};

    #[tokio::test]
    async fn persists_served_ids_through_the_domain_port() {
        let adapter = InMemoryServedPersistence::new(Arc::new(InMemoryFeedStateStore::new(10, 2)));
        adapter
            .persist(uid(7), &[pid(1), pid(2)], 100)
            .await
            .unwrap();
    }
}
