use crate::candidate_hydrators::tes_hydration_provider::TesHydrationProvider;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct HasMediaHydrator {
    provider: Arc<TesHydrationProvider>,
}

impl HasMediaHydrator {
    pub fn new(provider: Arc<TesHydrationProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for HasMediaHydrator {
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
                let has_media = media_by_post
                    .get(&post_id)
                    .and_then(Option::as_ref)
                    .is_some_and(|entities| !entities.is_empty());
                Ok(PostCandidate {
                    has_media: Some(has_media),
                    ..Default::default()
                })
            })
            .collect()
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.has_media = hydrated.has_media;
    }
}
