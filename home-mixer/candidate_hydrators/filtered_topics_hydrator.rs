use crate::candidate_hydrators::tes_hydration_provider::TesHydrationProvider;
use crate::models::candidate::PostCandidate;
use crate::models::query::{ScoredPostsQuery, TopicRecallMode};
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct FilteredTopicsHydrator {
    provider: Arc<TesHydrationProvider>,
}

impl FilteredTopicsHydrator {
    pub fn new(provider: Arc<TesHydrationProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for FilteredTopicsHydrator {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.has_cached_posts
            && (query.topic_recall_mode() != TopicRecallMode::None
                || !query.excluded_topic_ids.is_empty())
    }

    async fn hydrate(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        self.provider
            .core_candidates(query, candidates)
            .await
            .iter()
            .map(|candidate| {
                let candidate = candidate.as_ref().map_err(Clone::clone)?;
                Ok(PostCandidate {
                    filtered_topic_ids: candidate.filtered_topic_ids.clone(),
                    unfiltered_topic_ids: candidate.unfiltered_topic_ids.clone(),
                    ..Default::default()
                })
            })
            .collect()
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.filtered_topic_ids = hydrated.filtered_topic_ids;
        candidate.unfiltered_topic_ids = hydrated.unfiltered_topic_ids;
    }
}
