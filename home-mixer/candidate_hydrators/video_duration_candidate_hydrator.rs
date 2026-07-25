use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::candidate_features::{MediaEntities, MediaInfo};
use crate::candidate_pipeline::query::ScoredPostsQuery;
use crate::clients::tweet_entity_service_client::TESClient;
use std::collections::HashSet;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct VideoDurationCandidateHydrator {
    pub tes_client: Arc<dyn TESClient + Send + Sync>,
}

impl VideoDurationCandidateHydrator {
    pub async fn new(tes_client: Arc<dyn TESClient + Send + Sync>) -> Self {
        Self { tes_client }
    }
}

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for VideoDurationCandidateHydrator {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.has_cached_posts
    }

    async fn hydrate(
        &self,
        _query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Result<Vec<PostCandidate>, String> {
        let lookup_ids = candidates
            .iter()
            .flat_map(|candidate| {
                [
                    Some(
                        candidate
                            .retweeted_tweet_id
                            .unwrap_or(candidate.tweet_id as u64) as i64,
                    ),
                    candidate.quoted_tweet_id.map(|id| id as i64),
                ]
                .into_iter()
                .flatten()
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let media_by_post = self
            .tes_client
            .get_tweet_media_entities(lookup_ids)
            .await
            .map_err(|error| error.to_string())?;

        Ok(candidates
            .iter()
            .map(|candidate| {
                let main_id = candidate
                    .retweeted_tweet_id
                    .unwrap_or(candidate.tweet_id as u64) as i64;
                let main_media = media_by_post.get(&main_id).and_then(Option::as_ref);
                let quoted_media = candidate
                    .quoted_tweet_id
                    .and_then(|id| media_by_post.get(&(id as i64)))
                    .and_then(Option::as_ref);

                PostCandidate {
                    video_duration_ms: video_duration(main_media),
                    quoted_video_duration_ms: video_duration(quoted_media),
                    has_media: Some(main_media.is_some_and(|entities| !entities.is_empty())),
                    ..Default::default()
                }
            })
            .collect())
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.video_duration_ms = hydrated.video_duration_ms;
        candidate.quoted_video_duration_ms = hydrated.quoted_video_duration_ms;
        candidate.has_media = hydrated.has_media;
    }
}

fn video_duration(media_entities: Option<&MediaEntities>) -> Option<i32> {
    media_entities.and_then(|entities| {
        entities.iter().find_map(|entity| match &entity.media_info {
            Some(MediaInfo::VideoInfo(video_info)) => Some(video_info.duration_millis),
            None => None,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate_pipeline::candidate_features::{MediaEntity, PureCoreData, VideoInfo};
    use std::collections::HashMap;

    struct FakeTESClient;

    #[async_trait]
    impl TESClient for FakeTESClient {
        async fn get_tweet_core_datas(
            &self,
            _tweet_ids: Vec<i64>,
        ) -> Result<HashMap<i64, Option<PureCoreData>>, anyhow::Error> {
            Ok(HashMap::new())
        }

        async fn get_tweet_media_entities(
            &self,
            tweet_ids: Vec<i64>,
        ) -> Result<HashMap<i64, Option<MediaEntities>>, anyhow::Error> {
            Ok(tweet_ids
                .into_iter()
                .map(|id| {
                    let duration_millis = if id == 200 { 10_000 } else { 20_000 };
                    (
                        id,
                        Some(vec![MediaEntity {
                            media_info: Some(MediaInfo::VideoInfo(VideoInfo { duration_millis })),
                        }]),
                    )
                })
                .collect())
        }

        async fn get_subscription_author_ids(
            &self,
            _tweet_ids: Vec<i64>,
        ) -> Result<HashMap<i64, Option<u64>>, anyhow::Error> {
            Ok(HashMap::new())
        }
    }

    #[test]
    fn hydrates_reposted_and_quoted_video_durations() {
        let hydrator = VideoDurationCandidateHydrator {
            tes_client: Arc::new(FakeTESClient),
        };
        let candidate = PostCandidate {
            tweet_id: 100,
            retweeted_tweet_id: Some(200),
            quoted_tweet_id: Some(300),
            ..Default::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");

        let hydrated = runtime
            .block_on(hydrator.hydrate(&ScoredPostsQuery::default(), &[candidate]))
            .expect("media hydration");

        assert_eq!(hydrated[0].video_duration_ms, Some(10_000));
        assert_eq!(hydrated[0].quoted_video_duration_ms, Some(20_000));
        assert_eq!(hydrated[0].has_media, Some(true));
    }
}
