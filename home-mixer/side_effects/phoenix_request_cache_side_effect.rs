use crate::clients::strato_client::StratoClient;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::side_effects::cache_request_info_side_effect::CacheRequestInfoSideEffect;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::side_effect::{SideEffect, SideEffectInput};

/// Upstream-shaped boundary over the local request-cache sink adapter.
pub struct PhoenixRequestCacheSideEffect {
    inner: CacheRequestInfoSideEffect,
}

impl PhoenixRequestCacheSideEffect {
    pub fn new(strato_client: Arc<dyn StratoClient + Send + Sync>, enabled: bool) -> Self {
        Self {
            inner: CacheRequestInfoSideEffect::new(strato_client, enabled),
        }
    }
}

#[async_trait]
impl SideEffect<ScoredPostsQuery, PostCandidate> for PhoenixRequestCacheSideEffect {
    fn enable(&self, query: Arc<ScoredPostsQuery>) -> bool {
        self.inner.enable(query)
    }

    async fn side_effect(
        &self,
        input: Arc<SideEffectInput<ScoredPostsQuery, PostCandidate>>,
    ) -> Result<(), String> {
        self.inner.side_effect(input).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::strato_client::DemoStratoClient;

    #[test]
    fn facade_keeps_explicit_enablement_policy() {
        let client: Arc<dyn StratoClient + Send + Sync> = Arc::new(DemoStratoClient);
        let query = Arc::new(ScoredPostsQuery::default());

        assert!(
            !PhoenixRequestCacheSideEffect::new(Arc::clone(&client), false)
                .enable(Arc::clone(&query))
        );
        assert!(PhoenixRequestCacheSideEffect::new(client, true).enable(query));
    }
}
