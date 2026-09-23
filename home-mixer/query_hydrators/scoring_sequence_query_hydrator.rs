use crate::models::query::ScoredPostsQuery;
use crate::query_hydrators::user_action_seq_query_hydrator::UserActionSeqQueryHydrator;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

pub struct ScoringSequenceQueryHydrator {
    provider: Arc<UserActionSeqQueryHydrator>,
}

impl ScoringSequenceQueryHydrator {
    pub fn new(provider: Arc<UserActionSeqQueryHydrator>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for ScoringSequenceQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        let sequence = self.provider.hydrate_sequence(query).await?;
        Ok(ScoredPostsQuery {
            user_action_sequence: Some(sequence.clone()),
            scoring_sequence: Some(sequence),
            ..ScoredPostsQuery::test_default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.user_action_sequence = hydrated.user_action_sequence;
        query.scoring_sequence = hydrated.scoring_sequence;
    }
}
