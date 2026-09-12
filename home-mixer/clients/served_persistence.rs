//! Served exposure persistence port.
//!
//! This is intentionally separate from request/feed state. A production
//! adapter must make the exposure event durable and idempotent. The in-memory
//! implementation keeps the fail-closed request gate for Demo, Degraded, and
//! tests until that adapter is injected.

use crate::feed_state::FeedStateStore;
use crate::models::ids::{PostId, UserId};
use std::sync::Arc;

pub trait ServedPersistence: Send + Sync {
    fn persist(
        &self,
        viewer_id: UserId,
        served_post_ids: &[PostId],
        request_time_ms: i64,
    ) -> Result<(), String>;
}

/// Local-only adapter. It is never selected implicitly for production.
pub struct InMemoryServedPersistence {
    store: Arc<dyn FeedStateStore>,
}

impl InMemoryServedPersistence {
    pub fn new(store: Arc<dyn FeedStateStore>) -> Self {
        Self { store }
    }
}

impl ServedPersistence for InMemoryServedPersistence {
    fn persist(
        &self,
        viewer_id: UserId,
        served_post_ids: &[PostId],
        request_time_ms: i64,
    ) -> Result<(), String> {
        self.store
            .record(viewer_id, served_post_ids.to_vec(), request_time_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed_state::InMemoryFeedStateStore;
    use crate::models::{pid, uid};

    #[test]
    fn persists_served_ids_through_the_domain_port() {
        let adapter = InMemoryServedPersistence::new(Arc::new(InMemoryFeedStateStore::new(10, 2)));
        adapter.persist(uid(7), &[pid(1), pid(2)], 100).unwrap();
    }
}
