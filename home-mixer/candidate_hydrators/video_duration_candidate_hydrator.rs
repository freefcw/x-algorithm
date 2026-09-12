use crate::candidate_hydrators::tes_hydration_provider::TesHydrationProvider;
use crate::models::candidate::PostCandidate;
use crate::models::candidate_features::{MediaEntities, MediaInfo};
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct VideoDurationCandidateHydrator {
    provider: Arc<TesHydrationProvider>,
}

impl VideoDurationCandidateHydrator {
    pub fn new(provider: Arc<TesHydrationProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for VideoDurationCandidateHydrator {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.has_cached_posts
    }

    async fn hydrate(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let lookup_ids = candidates
            .iter()
            .map(|candidate| candidate.retweeted_tweet_id.unwrap_or(candidate.tweet_id))
            .collect::<Vec<_>>();
        let media_by_post = match self.provider.media_by_post(query, lookup_ids).await {
            Ok(media_by_post) => media_by_post,
            Err(error) => return vec![Err(error); candidates.len()],
        };

        candidates
            .iter()
            .map(|candidate| {
                let post_id = candidate.retweeted_tweet_id.unwrap_or(candidate.tweet_id);
                let media = media_by_post.get(&post_id).and_then(Option::as_ref);
                Ok(PostCandidate {
                    video_duration_ms: video_duration(media),
                    ..Default::default()
                })
            })
            .collect()
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.video_duration_ms = hydrated.video_duration_ms;
    }
}

fn video_duration(media_entities: Option<&MediaEntities>) -> Option<i32> {
    media_entities.and_then(|entities| {
        entities.iter().find_map(|entity| {
            entity
                .media_info
                .as_ref()
                .map(|media_info| match media_info {
                    MediaInfo::VideoInfo(video_info) => video_info.duration_millis,
                })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::tweet_entity_service_client::TESClient;
    use crate::models::candidate_features::{MediaEntity, PureCoreData, VideoInfo};
    use std::collections::HashMap;

    struct FakeTESClient;

    #[async_trait]
    impl TESClient for FakeTESClient {
        async fn get_tweet_core_datas(
            &self,
            _tweet_ids: Vec<crate::models::PostId>,
        ) -> Result<HashMap<crate::models::PostId, Option<PureCoreData>>, anyhow::Error> {
            Ok(HashMap::new())
        }

        async fn get_tweet_media_entities(
            &self,
            tweet_ids: Vec<crate::models::PostId>,
        ) -> Result<HashMap<crate::models::PostId, Option<MediaEntities>>, anyhow::Error> {
            Ok(tweet_ids
                .into_iter()
                .map(|id| {
                    (
                        id,
                        Some(vec![MediaEntity {
                            media_info: Some(MediaInfo::VideoInfo(VideoInfo {
                                duration_millis: 10_000,
                            })),
                        }]),
                    )
                })
                .collect())
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
    async fn owns_original_post_video_duration() {
        let provider = Arc::new(TesHydrationProvider::new(Arc::new(FakeTESClient)));
        let hydrator = VideoDurationCandidateHydrator::new(provider);
        let query = ScoredPostsQuery {
            request_id: "request-1".to_string(),
            ..Default::default()
        };
        let candidate = PostCandidate {
            tweet_id: 100.into(),
            retweeted_tweet_id: Some(200.into()),
            quoted_tweet_id: Some(300.into()),
            ..Default::default()
        };

        let hydrated = hydrator.hydrate(&query, &[candidate]).await;
        let hydrated = hydrated[0].as_ref().expect("media hydration");
        assert_eq!(hydrated.video_duration_ms, Some(10_000));
        assert_eq!(hydrated.quoted_video_duration_ms, None);
        assert_eq!(hydrated.has_media, None);
    }
}
