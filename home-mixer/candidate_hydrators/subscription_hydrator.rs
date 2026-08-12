use crate::clients::tweet_entity_service_client::TESClient;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::params;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct SubscriptionHydrator {
    pub tes_client: Arc<dyn TESClient + Send + Sync>,
    request_timeout: Duration,
}

impl SubscriptionHydrator {
    pub async fn new(tes_client: Arc<dyn TESClient + Send + Sync>) -> Self {
        Self {
            tes_client,
            request_timeout: Duration::from_millis(params::TES_REQUEST_TIMEOUT_MS),
        }
    }

    #[cfg(test)]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for SubscriptionHydrator {
    async fn hydrate(
        &self,
        _query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let client = &self.tes_client;

        let tweet_ids = candidates.iter().map(|c| c.tweet_id).collect::<Vec<_>>();

        let post_features = match tokio::time::timeout(
            self.request_timeout,
            client.get_subscription_author_ids(tweet_ids.clone()),
        )
        .await
        {
            Ok(Ok(post_features)) => post_features,
            Ok(Err(error)) => return vec![Err(error.to_string()); candidates.len()],
            Err(_) => {
                return vec![
                    Err(format!(
                        "TES subscription request timed out after {}ms",
                        self.request_timeout.as_millis()
                    ));
                    candidates.len()
                ];
            }
        };

        let mut hydrated_candidates = Vec::with_capacity(candidates.len());
        for tweet_id in tweet_ids {
            let post_features = post_features.get(&tweet_id);
            let subscription_author_id = post_features.and_then(|x| *x);
            let hydrated = PostCandidate {
                subscription_author_id,
                ..Default::default()
            };
            hydrated_candidates.push(Ok(hydrated));
        }

        hydrated_candidates
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.subscription_author_id = hydrated.subscription_author_id;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::candidate_features::{MediaEntities, PureCoreData};
    use std::collections::HashMap;

    struct SlowTesClient;

    #[tonic::async_trait]
    impl TESClient for SlowTesClient {
        async fn get_tweet_core_datas(
            &self,
            _tweet_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<PureCoreData>>, anyhow::Error> {
            Ok(HashMap::new())
        }

        async fn get_tweet_media_entities(
            &self,
            _tweet_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<MediaEntities>>, anyhow::Error> {
            Ok(HashMap::new())
        }

        async fn get_subscription_author_ids(
            &self,
            _tweet_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<u64>>, anyhow::Error> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(HashMap::new())
        }
    }

    #[tokio::test]
    async fn slow_subscription_metadata_is_bounded() {
        let hydrator = SubscriptionHydrator::new(Arc::new(SlowTesClient))
            .await
            .with_request_timeout(Duration::from_millis(1));
        let candidates = [PostCandidate {
            tweet_id: 100,
            ..Default::default()
        }];

        let result = hydrator
            .hydrate(&ScoredPostsQuery::default(), &candidates)
            .await;

        assert!(result[0]
            .as_ref()
            .expect_err("slow subscription lookup must time out")
            .contains("timed out"));
    }
}
