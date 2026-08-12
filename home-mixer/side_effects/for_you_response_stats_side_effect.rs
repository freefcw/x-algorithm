use crate::feed_stats::{FeedResponseStats, FeedStatsSink};
use crate::models::feed_item::FeedItem;
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::side_effect::{SideEffect, SideEffectInput};

/// Upstream-shaped response stats boundary over the local sink port.
pub struct ForYouResponseStatsSideEffect {
    sink: Arc<dyn FeedStatsSink>,
}

impl ForYouResponseStatsSideEffect {
    pub fn new(sink: Arc<dyn FeedStatsSink>) -> Self {
        Self { sink }
    }
}

#[async_trait]
impl SideEffect<ScoredPostsQuery, FeedItem> for ForYouResponseStatsSideEffect {
    async fn side_effect(
        &self,
        input: Arc<SideEffectInput<ScoredPostsQuery, FeedItem>>,
    ) -> Result<(), String> {
        self.sink.record(FeedResponseStats::from_items(
            input.query.request_id.clone(),
            &input.selected_candidates,
        ))
    }
}
