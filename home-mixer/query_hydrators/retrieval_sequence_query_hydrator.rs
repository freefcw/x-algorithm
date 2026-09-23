use crate::models::query::ScoredPostsQuery;
use crate::query_hydrators::user_action_seq_query_hydrator::UserActionSeqQueryHydrator;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

pub struct RetrievalSequenceQueryHydrator {
    provider: Arc<UserActionSeqQueryHydrator>,
}

impl RetrievalSequenceQueryHydrator {
    pub fn new(provider: Arc<UserActionSeqQueryHydrator>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for RetrievalSequenceQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        Ok(ScoredPostsQuery {
            retrieval_sequence: Some(self.provider.hydrate_sequence(query).await?),
            ..ScoredPostsQuery::test_default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.retrieval_sequence = hydrated.retrieval_sequence;
    }
}
