use crate::clients::strato_client::{decode, StratoClient, StratoResult};
use crate::models::query::ScoredPostsQuery;
use crate::models::user_features::UserFeatures;
use crate::params;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

pub struct UserFeaturesQueryHydrator {
    pub strato_client: Arc<dyn StratoClient + Send + Sync>,
    fetch_timeout: Duration,
    feature_cache: Mutex<FeatureCache>,
}

type CachedFeatures = OnceCell<Result<UserFeatures, String>>;
type FeatureCache = HashMap<String, Weak<CachedFeatures>>;

impl UserFeaturesQueryHydrator {
    pub fn new(strato_client: Arc<dyn StratoClient + Send + Sync>) -> Self {
        Self {
            strato_client,
            fetch_timeout: Duration::from_millis(params::USER_FEATURES_FETCH_TIMEOUT_MS),
            feature_cache: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    pub fn with_fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = timeout;
        self
    }

    pub async fn hydrate_features(&self, query: &ScoredPostsQuery) -> Result<UserFeatures, String> {
        let cache_key = format!(
            "{}:{}:{}",
            query.request_id, query.user_id, query.prediction_id
        );
        let cell = {
            let mut cache = self.feature_cache.lock().await;
            match cache.get(&cache_key).and_then(Weak::upgrade) {
                Some(cell) => cell,
                None => {
                    let cell = Arc::new(OnceCell::new());
                    cache.insert(cache_key.clone(), Arc::downgrade(&cell));
                    cell
                }
            }
        };

        tokio::task::yield_now().await;
        let result = cell
            .get_or_init(|| async {
                let result = tokio::time::timeout(
                    self.fetch_timeout,
                    self.strato_client.get_user_features(query.user_id),
                )
                .await
                .map_err(|_| {
                    format!(
                        "User features fetch timed out after {}ms",
                        self.fetch_timeout.as_millis()
                    )
                })?
                .map_err(|error| error.to_string())?;
                match decode(&result) {
                    StratoResult::Ok(value) => Ok(value.v.unwrap_or_default()),
                    StratoResult::Err(_) => Err("Error received from strato".to_string()),
                }
            })
            .await
            .clone();

        let mut cache = self.feature_cache.lock().await;
        if cache
            .get(&cache_key)
            .and_then(Weak::upgrade)
            .is_some_and(|cached| Arc::ptr_eq(&cached, &cell))
        {
            cache.remove(&cache_key);
        }

        result
    }
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for UserFeaturesQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        Ok(ScoredPostsQuery {
            user_features: self.hydrate_features(query).await?,
            ..Default::default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.user_features = hydrated.user_features;
    }

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::strato_client::DemoStratoClient;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingStratoClient {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl StratoClient for CountingStratoClient {
        async fn get_user_features(&self, user_id: u64) -> Result<Vec<u8>, anyhow::Error> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            DemoStratoClient.get_user_features(user_id).await
        }

        async fn store_request_info(
            &self,
            _user_id: u64,
            _post_ids: Vec<u64>,
        ) -> Result<Vec<u8>, anyhow::Error> {
            unreachable!("feature hydration does not write request state")
        }
    }

    struct SlowStratoClient;

    #[async_trait]
    impl StratoClient for SlowStratoClient {
        async fn get_user_features(&self, _user_id: u64) -> Result<Vec<u8>, anyhow::Error> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(Vec::new())
        }

        async fn store_request_info(
            &self,
            _user_id: u64,
            _post_ids: Vec<u64>,
        ) -> Result<Vec<u8>, anyhow::Error> {
            unreachable!("feature hydration does not write request state")
        }
    }

    #[tokio::test]
    async fn slow_feature_fetch_is_bounded() {
        let provider = UserFeaturesQueryHydrator::new(Arc::new(SlowStratoClient))
            .with_fetch_timeout(Duration::from_millis(1));
        let query = ScoredPostsQuery {
            user_id: 42,
            request_id: "slow-features".to_string(),
            prediction_id: 7,
            ..Default::default()
        };

        let error = provider
            .hydrate_features(&query)
            .await
            .expect_err("slow feature fetch must time out");

        assert!(error.contains("timed out"));
    }

    #[tokio::test]
    async fn concurrent_field_owners_share_one_feature_read() {
        let client = Arc::new(CountingStratoClient {
            calls: AtomicUsize::new(0),
        });
        let provider = UserFeaturesQueryHydrator::new(client.clone());
        let query = ScoredPostsQuery {
            user_id: 42,
            request_id: "request-1".to_string(),
            prediction_id: 7,
            ..Default::default()
        };

        let (blocked, followed) = tokio::join!(
            provider.hydrate_features(&query),
            provider.hydrate_features(&query)
        );

        assert!(blocked.is_ok());
        assert!(followed.is_ok());
        assert_eq!(client.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn different_users_never_share_feature_results() {
        let client = Arc::new(CountingStratoClient {
            calls: AtomicUsize::new(0),
        });
        let provider = UserFeaturesQueryHydrator::new(client.clone());
        let first = ScoredPostsQuery {
            user_id: 42,
            request_id: "same-request-label".to_string(),
            prediction_id: 7,
            ..Default::default()
        };
        let second = ScoredPostsQuery {
            user_id: 43,
            request_id: first.request_id.clone(),
            prediction_id: first.prediction_id,
            ..Default::default()
        };

        let (first_result, second_result) = tokio::join!(
            provider.hydrate_features(&first),
            provider.hydrate_features(&second)
        );

        assert!(first_result.is_ok());
        assert!(second_result.is_ok());
        assert_eq!(client.calls.load(Ordering::Relaxed), 2);
    }
}
