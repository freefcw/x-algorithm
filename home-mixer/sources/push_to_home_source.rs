use crate::final_feed::FeedItem;
use crate::models::query::ScoredPostsQuery;
use tonic::async_trait;
use xai_candidate_pipeline::source::Source;

pub struct PushToHomeSource;

#[async_trait]
impl Source<ScoredPostsQuery, FeedItem> for PushToHomeSource {
    fn enable(&self, _query: &ScoredPostsQuery) -> bool {
        false
    }

    async fn source(&self, _query: &ScoredPostsQuery) -> Result<Vec<FeedItem>, String> {
        Ok(Vec::new())
    }
}
