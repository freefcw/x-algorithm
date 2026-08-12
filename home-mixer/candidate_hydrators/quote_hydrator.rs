use crate::candidate_hydrators::tes_hydration_provider::TesHydrationProvider;
use crate::models::candidate::PostCandidate;
use crate::models::candidate_features::{MediaEntities, MediaInfo};
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct QuoteHydrator {
    provider: Arc<TesHydrationProvider>,
}

impl QuoteHydrator {
    pub fn new(provider: Arc<TesHydrationProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for QuoteHydrator {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.has_cached_posts
    }

    async fn hydrate(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let core = self.provider.core_candidates(query, candidates).await;
        let quoted_ids = core
            .iter()
            .filter_map(|candidate| candidate.as_ref().ok()?.quoted_tweet_id)
            .collect::<Vec<_>>();
        let media_by_post = match self.provider.media_by_post(query, quoted_ids).await {
            Ok(media_by_post) => media_by_post,
            Err(error) => {
                log::warn!(
                    "request_id={} QuoteHydrator quoted media unavailable: {}",
                    query.request_id,
                    error
                );
                Arc::new(Default::default())
            }
        };

        core.iter()
            .map(|candidate| {
                let candidate = candidate.as_ref().map_err(Clone::clone)?;
                let quoted_video_duration_ms = candidate
                    .quoted_tweet_id
                    .and_then(|id| media_by_post.get(&id))
                    .and_then(Option::as_ref)
                    .and_then(video_duration);
                Ok(PostCandidate {
                    quoted_tweet_id: candidate.quoted_tweet_id,
                    quoted_user_id: candidate.quoted_user_id,
                    quoted_tweet_text: candidate.quoted_tweet_text.clone(),
                    quoted_video_duration_ms,
                    ..Default::default()
                })
            })
            .collect()
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.quoted_tweet_id = hydrated.quoted_tweet_id;
        candidate.quoted_user_id = hydrated.quoted_user_id;
        candidate.quoted_tweet_text = hydrated.quoted_tweet_text;
        candidate.quoted_video_duration_ms = hydrated.quoted_video_duration_ms;
    }
}

fn video_duration(media_entities: &MediaEntities) -> Option<i32> {
    media_entities.iter().find_map(|entity| {
        entity
            .media_info
            .as_ref()
            .map(|media_info| match media_info {
                MediaInfo::VideoInfo(video_info) => video_info.duration_millis,
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::tweet_entity_service_client::TESClient;
    use crate::models::candidate_features::PureCoreData;
    use std::collections::HashMap;

    struct MediaFailingTesClient;

    #[async_trait]
    impl TESClient for MediaFailingTesClient {
        async fn get_tweet_core_datas(
            &self,
            tweet_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<PureCoreData>>, anyhow::Error> {
            Ok(tweet_ids
                .into_iter()
                .map(|id| {
                    let data = if id == 1 {
                        PureCoreData {
                            author_id: 10,
                            text: "main".to_string(),
                            quoted_tweet_id: Some(2),
                            quoted_user_id: Some(20),
                            ..Default::default()
                        }
                    } else {
                        PureCoreData {
                            author_id: 20,
                            text: "quote".to_string(),
                            ..Default::default()
                        }
                    };
                    (id, Some(data))
                })
                .collect())
        }

        async fn get_tweet_media_entities(
            &self,
            _tweet_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<MediaEntities>>, anyhow::Error> {
            Err(anyhow::anyhow!("media unavailable"))
        }

        async fn get_subscription_author_ids(
            &self,
            _tweet_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<u64>>, anyhow::Error> {
            Ok(HashMap::new())
        }
    }

    #[tokio::test]
    async fn media_failure_keeps_quote_metadata() {
        let provider = Arc::new(TesHydrationProvider::new(Arc::new(MediaFailingTesClient)));
        let hydrator = QuoteHydrator::new(provider);
        let query = ScoredPostsQuery {
            request_id: "request-1".to_string(),
            ..Default::default()
        };
        let candidates = [PostCandidate {
            tweet_id: 1,
            ..Default::default()
        }];

        let hydrated = hydrator.hydrate(&query, &candidates).await;
        let hydrated = hydrated[0].as_ref().expect("quote hydration");
        assert_eq!(hydrated.quoted_tweet_id, Some(2));
        assert_eq!(hydrated.quoted_user_id, Some(20));
        assert_eq!(hydrated.quoted_tweet_text, "quote");
        assert_eq!(hydrated.quoted_video_duration_ms, None);
    }
}
