use crate::clients::tweet_entity_service_client::TESClient;
use crate::models::candidate::PostCandidate;
use crate::models::candidate_features::MediaEntities;
use crate::models::ids::PostId;
use crate::models::query::ScoredPostsQuery;
use crate::params;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};

pub struct TesHydrationProvider {
    tes_client: Arc<dyn TESClient + Send + Sync>,
    request_timeout: Duration,
    core_cache: Mutex<HashMap<String, Weak<CachedCoreBatch>>>,
    media_cache: Mutex<HashMap<String, Weak<CachedMediaBatch>>>,
}

type CoreBatch = Vec<Result<PostCandidate, String>>;
type CachedCoreBatch = OnceCell<Arc<CoreBatch>>;
type MediaBatch = HashMap<PostId, Option<MediaEntities>>;
type CachedMediaBatch = OnceCell<Result<Arc<MediaBatch>, String>>;

impl TesHydrationProvider {
    pub fn new(tes_client: Arc<dyn TESClient + Send + Sync>) -> Self {
        Self {
            tes_client,
            request_timeout: Duration::from_millis(params::TES_REQUEST_TIMEOUT_MS),
            core_cache: Mutex::new(HashMap::new()),
            media_cache: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub async fn core_candidates(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Arc<CoreBatch> {
        let tweet_ids = candidates
            .iter()
            .map(|candidate| candidate.tweet_id)
            .collect::<Vec<_>>();
        let cache_key = batch_key(query, &tweet_ids);
        let cell = {
            let mut cache = self.core_cache.lock().await;
            match cache.get(&cache_key).and_then(Weak::upgrade) {
                Some(cell) => cell,
                None => {
                    let cell = Arc::new(OnceCell::new());
                    cache.insert(cache_key.clone(), Arc::downgrade(&cell));
                    cell
                }
            }
        };

        // Candidate hydrators are polled as one concurrent stage. Yield once so
        // every field owner joins this request-scoped batch before fast adapters finish.
        tokio::task::yield_now().await;
        let result = cell
            .get_or_init(|| async { Arc::new(self.load_core_candidates(tweet_ids).await) })
            .await
            .clone();

        let mut cache = self.core_cache.lock().await;
        if cache
            .get(&cache_key)
            .and_then(Weak::upgrade)
            .is_some_and(|cached| Arc::ptr_eq(&cached, &cell))
        {
            cache.remove(&cache_key);
        }

        result
    }

    pub async fn media_by_post(
        &self,
        query: &ScoredPostsQuery,
        post_ids: Vec<PostId>,
    ) -> Result<Arc<MediaBatch>, String> {
        let mut lookup_ids = post_ids;
        lookup_ids.sort_unstable();
        lookup_ids.dedup();
        if lookup_ids.is_empty() {
            return Ok(Arc::new(HashMap::new()));
        }

        let cache_key = batch_key(query, &lookup_ids);
        let cell = {
            let mut cache = self.media_cache.lock().await;
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
                tokio::time::timeout(
                    self.request_timeout,
                    self.tes_client.get_tweet_media_entities(lookup_ids),
                )
                .await
                .map_err(|_| {
                    format!(
                        "TES media request timed out after {}ms",
                        self.request_timeout.as_millis()
                    )
                })?
                .map(Arc::new)
                .map_err(|error| error.to_string())
            })
            .await
            .clone();

        let mut cache = self.media_cache.lock().await;
        if cache
            .get(&cache_key)
            .and_then(Weak::upgrade)
            .is_some_and(|cached| Arc::ptr_eq(&cached, &cell))
        {
            cache.remove(&cache_key);
        }

        result
    }

    async fn load_core_candidates(&self, tweet_ids: Vec<PostId>) -> CoreBatch {
        let core_by_tweet = match tokio::time::timeout(
            self.request_timeout,
            self.tes_client.get_tweet_core_datas(tweet_ids.clone()),
        )
        .await
        {
            Ok(Ok(core_by_tweet)) => core_by_tweet,
            Ok(Err(error)) => return vec![Err(error.to_string()); tweet_ids.len()],
            Err(_) => {
                return vec![
                    Err(format!(
                        "TES core request timed out after {}ms",
                        self.request_timeout.as_millis()
                    ));
                    tweet_ids.len()
                ];
            }
        };

        let quoted_ids = core_by_tweet
            .values()
            .filter_map(|core| core.as_ref()?.quoted_tweet_id)
            .collect::<HashSet<_>>();
        let quoted_core_by_tweet = if quoted_ids.is_empty() {
            HashMap::new()
        } else {
            match tokio::time::timeout(
                self.request_timeout,
                self.tes_client
                    .get_tweet_core_datas(quoted_ids.into_iter().collect()),
            )
            .await
            {
                Ok(Ok(quoted_core_by_tweet)) => quoted_core_by_tweet,
                Ok(Err(error)) => return vec![Err(error.to_string()); tweet_ids.len()],
                Err(_) => {
                    return vec![
                        Err(format!(
                            "TES quoted core request timed out after {}ms",
                            self.request_timeout.as_millis()
                        ));
                        tweet_ids.len()
                    ];
                }
            }
        };

        tweet_ids
            .into_iter()
            .map(|tweet_id| {
                let core = core_by_tweet.get(&tweet_id).and_then(Option::as_ref);
                let quoted_text = core
                    .and_then(|value| value.quoted_tweet_id)
                    .and_then(|id| quoted_core_by_tweet.get(&id))
                    .and_then(Option::as_ref)
                    .map(|quoted| quoted.text.clone())
                    .unwrap_or_default();

                Ok(PostCandidate {
                    author_id: core.map(|value| value.author_id).unwrap_or_default(),
                    tweet_text: core.map(|value| value.text.clone()).unwrap_or_default(),
                    quoted_tweet_text: quoted_text,
                    retweeted_user_id: core.and_then(|value| value.source_user_id),
                    retweeted_tweet_id: core.and_then(|value| value.source_tweet_id),
                    quoted_tweet_id: core.and_then(|value| value.quoted_tweet_id),
                    quoted_user_id: core.and_then(|value| value.quoted_user_id),
                    in_reply_to_tweet_id: core.and_then(|value| value.in_reply_to_tweet_id),
                    created_at_ms: core.and_then(|value| value.created_at_ms),
                    recommendation_eligible: core.and_then(|value| value.recommendation_eligible),
                    language_code: core.and_then(|value| value.language_code.clone()),
                    favorite_count: core.and_then(|value| value.favorite_count),
                    view_count: core.and_then(|value| value.view_count),
                    reply_count: core.and_then(|value| value.reply_count),
                    repost_count: core.and_then(|value| value.repost_count),
                    quote_count: core.and_then(|value| value.quote_count),
                    filtered_topic_ids: core
                        .map(|value| value.filtered_topic_ids.clone())
                        .unwrap_or_default(),
                    unfiltered_topic_ids: core
                        .map(|value| value.unfiltered_topic_ids.clone())
                        .unwrap_or_default(),
                    ..Default::default()
                })
            })
            .collect()
    }
}

fn batch_key(query: &ScoredPostsQuery, ids: &[PostId]) -> String {
    format!(
        "{}:{}:{}:{ids:?}",
        query.request_id, query.user_id, query.prediction_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::candidate_features::PureCoreData;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingTesClient {
        core_calls: AtomicUsize,
        media_calls: AtomicUsize,
    }

    #[tonic::async_trait]
    impl TESClient for CountingTesClient {
        async fn get_tweet_core_datas(
            &self,
            tweet_ids: Vec<crate::models::PostId>,
        ) -> Result<HashMap<crate::models::PostId, Option<PureCoreData>>, anyhow::Error> {
            self.core_calls.fetch_add(1, Ordering::Relaxed);
            Ok(tweet_ids
                .into_iter()
                .map(|id| {
                    (
                        id,
                        Some(PureCoreData {
                            author_id: id,
                            text: format!("post {id}"),
                            ..Default::default()
                        }),
                    )
                })
                .collect())
        }

        async fn get_tweet_media_entities(
            &self,
            tweet_ids: Vec<crate::models::PostId>,
        ) -> Result<HashMap<crate::models::PostId, Option<MediaEntities>>, anyhow::Error> {
            self.media_calls.fetch_add(1, Ordering::Relaxed);
            Ok(tweet_ids.into_iter().map(|id| (id, Some(vec![]))).collect())
        }

        async fn get_subscription_author_ids(
            &self,
            _tweet_ids: Vec<crate::models::PostId>,
        ) -> Result<HashMap<crate::models::PostId, Option<crate::models::UserId>>, anyhow::Error>
        {
            Ok(HashMap::new())
        }
    }

    struct SlowTesClient;

    #[tonic::async_trait]
    impl TESClient for SlowTesClient {
        async fn get_tweet_core_datas(
            &self,
            _tweet_ids: Vec<crate::models::PostId>,
        ) -> Result<HashMap<crate::models::PostId, Option<PureCoreData>>, anyhow::Error> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(HashMap::new())
        }

        async fn get_tweet_media_entities(
            &self,
            _tweet_ids: Vec<crate::models::PostId>,
        ) -> Result<HashMap<crate::models::PostId, Option<MediaEntities>>, anyhow::Error> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(HashMap::new())
        }

        async fn get_subscription_author_ids(
            &self,
            _tweet_ids: Vec<crate::models::PostId>,
        ) -> Result<HashMap<crate::models::PostId, Option<crate::models::UserId>>, anyhow::Error>
        {
            Ok(HashMap::new())
        }
    }

    #[tokio::test]
    async fn slow_tes_batches_are_bounded() {
        let provider = TesHydrationProvider::new(Arc::new(SlowTesClient))
            .with_request_timeout(Duration::from_millis(1));
        let query = ScoredPostsQuery {
            request_id: "slow-tes".to_string(),
            prediction_id: 7,
            ..Default::default()
        };
        let candidates = [PostCandidate {
            tweet_id: 100,
            ..Default::default()
        }];

        let result = provider.core_candidates(&query, &candidates).await;

        assert!(result[0]
            .as_ref()
            .expect_err("slow TES core must time out")
            .contains("timed out"));
        let media_error = provider
            .media_by_post(&query, vec![crate::models::pid(100)])
            .await
            .expect_err("slow TES media must time out");
        assert!(media_error.contains("timed out"));
    }

    #[tokio::test]
    async fn concurrent_field_owners_share_core_and_media_batches() {
        let client = Arc::new(CountingTesClient {
            core_calls: AtomicUsize::new(0),
            media_calls: AtomicUsize::new(0),
        });
        let provider = TesHydrationProvider::new(client.clone());
        let query = ScoredPostsQuery {
            request_id: "request-1".to_string(),
            prediction_id: 7,
            ..Default::default()
        };
        let candidates = [PostCandidate {
            tweet_id: 100,
            ..Default::default()
        }];

        let (core, quote) = tokio::join!(
            provider.core_candidates(&query, &candidates),
            provider.core_candidates(&query, &candidates)
        );
        let (video, has_media) = tokio::join!(
            provider.media_by_post(&query, vec![crate::models::pid(100)]),
            provider.media_by_post(&query, vec![crate::models::pid(100)])
        );

        assert!(core[0].is_ok());
        assert!(quote[0].is_ok());
        assert!(video.is_ok());
        assert!(has_media.is_ok());
        assert_eq!(client.core_calls.load(Ordering::Relaxed), 1);
        assert_eq!(client.media_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn different_requests_never_share_tes_batches() {
        let client = Arc::new(CountingTesClient {
            core_calls: AtomicUsize::new(0),
            media_calls: AtomicUsize::new(0),
        });
        let provider = TesHydrationProvider::new(client.clone());
        let first = ScoredPostsQuery {
            user_id: 42,
            request_id: "request-1".to_string(),
            prediction_id: 7,
            ..Default::default()
        };
        let second = ScoredPostsQuery {
            user_id: 42,
            request_id: "request-2".to_string(),
            prediction_id: 8,
            ..Default::default()
        };
        let candidates = [PostCandidate {
            tweet_id: 100,
            ..Default::default()
        }];

        let (first_result, second_result) = tokio::join!(
            provider.core_candidates(&first, &candidates),
            provider.core_candidates(&second, &candidates)
        );

        assert!(first_result[0].is_ok());
        assert!(second_result[0].is_ok());
        assert_eq!(client.core_calls.load(Ordering::Relaxed), 2);
    }
}
