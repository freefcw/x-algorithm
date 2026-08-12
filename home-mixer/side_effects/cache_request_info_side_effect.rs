use crate::clients::strato_client::{decode, StratoClient, StratoResult, StratoValue};
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::params;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use xai_candidate_pipeline::side_effect::{SideEffect, SideEffectInput};

pub struct CacheRequestInfoSideEffect {
    strato_client: Arc<dyn StratoClient + Send + Sync>,
    enabled: bool,
    write_timeout: Duration,
}

impl CacheRequestInfoSideEffect {
    pub fn new(strato_client: Arc<dyn StratoClient + Send + Sync>, enabled: bool) -> Self {
        Self {
            strato_client,
            enabled,
            write_timeout: Duration::from_millis(params::STRATO_WRITE_TIMEOUT_MS),
        }
    }

    #[cfg(test)]
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }
}

#[async_trait]
impl SideEffect<ScoredPostsQuery, PostCandidate> for CacheRequestInfoSideEffect {
    fn enable(&self, query: Arc<ScoredPostsQuery>) -> bool {
        self.enabled && !query.in_network_only
    }

    async fn side_effect(
        &self,
        input: Arc<SideEffectInput<ScoredPostsQuery, PostCandidate>>,
    ) -> Result<(), String> {
        let user_id = input.query.user_id;

        let post_ids: Vec<u64> = input
            .selected_candidates
            .iter()
            .map(|c| c.tweet_id)
            .collect();
        let client = &self.strato_client;
        let res = tokio::time::timeout(
            self.write_timeout,
            client.store_request_info(user_id, post_ids),
        )
        .await
        .map_err(|_| {
            format!(
                "Strato request-info write timed out after {}ms",
                self.write_timeout.as_millis()
            )
        })?
        .map_err(|e| e.to_string())?;
        let decoded: StratoResult<StratoValue<()>> = decode(&res);
        match decoded {
            StratoResult::Ok(_) => Ok(()),
            StratoResult::Err(_) => Err("error received from strato".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::strato_client::DemoStratoClient;

    struct SlowStratoClient;

    #[tonic::async_trait]
    impl StratoClient for SlowStratoClient {
        async fn get_user_features(&self, _user_id: u64) -> Result<Vec<u8>, anyhow::Error> {
            Ok(Vec::new())
        }

        async fn store_request_info(
            &self,
            _user_id: u64,
            _post_ids: Vec<u64>,
        ) -> Result<Vec<u8>, anyhow::Error> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn slow_request_cache_write_is_bounded() {
        let side_effect = CacheRequestInfoSideEffect::new(Arc::new(SlowStratoClient), true)
            .with_write_timeout(Duration::from_millis(1));
        let input = Arc::new(SideEffectInput {
            query: Arc::new(ScoredPostsQuery {
                user_id: 42,
                ..Default::default()
            }),
            selected_candidates: vec![PostCandidate {
                tweet_id: 100,
                ..Default::default()
            }],
            non_selected_candidates: Vec::new(),
        });

        let error = side_effect
            .side_effect(input)
            .await
            .expect_err("slow request-cache write must time out");

        assert!(error.contains("timed out"));
    }

    #[test]
    fn request_cache_requires_explicit_enablement() {
        let client: Arc<dyn StratoClient + Send + Sync> = Arc::new(DemoStratoClient);
        let query = Arc::new(ScoredPostsQuery::default());

        assert!(!CacheRequestInfoSideEffect::new(Arc::clone(&client), false)
            .enable(Arc::clone(&query,)));
        assert!(CacheRequestInfoSideEffect::new(Arc::clone(&client), true).enable(query));
    }

    #[test]
    fn request_cache_stays_disabled_for_in_network_only_requests() {
        let client: Arc<dyn StratoClient + Send + Sync> = Arc::new(DemoStratoClient);
        let query = Arc::new(ScoredPostsQuery {
            in_network_only: true,
            ..Default::default()
        });

        assert!(!CacheRequestInfoSideEffect::new(client, true).enable(query));
    }
}
