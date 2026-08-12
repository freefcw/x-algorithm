use crate::feed_state::FeedStateStore;
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

pub struct ServedHistoryQueryHydrator {
    store: Arc<dyn FeedStateStore>,
}

impl ServedHistoryQueryHydrator {
    pub fn from_store(store: Arc<dyn FeedStateStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for ServedHistoryQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        let mut served_ids = query.served_ids.clone();
        append_unique(
            &mut served_ids,
            self.store.load(query.user_id)?.served_post_ids,
        );
        Ok(ScoredPostsQuery {
            served_ids,
            ..Default::default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.served_ids = hydrated.served_ids;
    }
}

fn append_unique<T: PartialEq>(target: &mut Vec<T>, values: Vec<T>) {
    for value in values {
        if !target.contains(&value) {
            target.push(value);
        }
    }
}
