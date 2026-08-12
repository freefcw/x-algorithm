use crate::final_feed::{FeedItem, FeedResponseStatsSideEffect, FeedStatsSink};
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::side_effect::{SideEffect, SideEffectInput};

/// Upstream-shaped response stats boundary over the local sink port.
pub struct ForYouResponseStatsSideEffect {
    inner: FeedResponseStatsSideEffect,
}

impl ForYouResponseStatsSideEffect {
    pub fn new(sink: Arc<dyn FeedStatsSink>) -> Self {
        Self {
            inner: FeedResponseStatsSideEffect::new(sink),
        }
    }
}

#[async_trait]
impl SideEffect<ScoredPostsQuery, FeedItem> for ForYouResponseStatsSideEffect {
    async fn side_effect(
        &self,
        input: Arc<SideEffectInput<ScoredPostsQuery, FeedItem>>,
    ) -> Result<(), String> {
        self.inner.side_effect(input).await
    }
}
