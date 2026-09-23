use crate::feed_state::FeedStateStore;
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

pub struct PastRequestTimestampsQueryHydrator {
    store: Arc<dyn FeedStateStore>,
}

impl PastRequestTimestampsQueryHydrator {
    pub fn from_store(store: Arc<dyn FeedStateStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for PastRequestTimestampsQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        let mut past_request_timestamps_ms = query.past_request_timestamps_ms.clone();
        append_unique(
            &mut past_request_timestamps_ms,
            query
                .load_feed_state(self.store.as_ref())
                .await?
                .request_timestamps_ms
                .clone(),
        );
        Ok(ScoredPostsQuery {
            past_request_timestamps_ms,
            ..ScoredPostsQuery::test_default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
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
