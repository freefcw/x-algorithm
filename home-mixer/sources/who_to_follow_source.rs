use crate::models::feed_item::FeedItem;
use crate::models::query::ScoredPostsQuery;
use tonic::async_trait;
use xai_candidate_pipeline::source::Source;

pub struct WhoToFollowSource;

#[async_trait]
impl Source<ScoredPostsQuery, FeedItem> for WhoToFollowSource {
    fn enable(&self, _query: &ScoredPostsQuery) -> bool {
        false
    }

    async fn source(&self, _query: &ScoredPostsQuery) -> Result<Vec<FeedItem>, String> {
        Ok(Vec::new())
    }
}
