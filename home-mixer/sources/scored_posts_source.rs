use crate::models::feed_item::FeedItem;
use crate::models::query::ScoredPostsQuery;
use crate::scored_posts_server::{ScoredPostsOutput, ScoredPostsServer};
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::source::Source;

#[async_trait]
pub trait ScoredPostsProvider: Send + Sync {
    async fn score_posts(&self, query: ScoredPostsQuery) -> Result<ScoredPostsOutput, String>;
}

#[async_trait]
impl ScoredPostsProvider for ScoredPostsServer {
    async fn score_posts(&self, query: ScoredPostsQuery) -> Result<ScoredPostsOutput, String> {
        Ok(self.score(query).await)
    }
}

pub struct ScoredPostsSource {
    provider: Arc<dyn ScoredPostsProvider>,
}

impl ScoredPostsSource {
    pub fn new(provider: Arc<dyn ScoredPostsProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl Source<ScoredPostsQuery, FeedItem> for ScoredPostsSource {
    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<FeedItem>, String> {
        let output = self.provider.score_posts(query.clone()).await?;
        Ok(output.posts.into_iter().map(FeedItem::post).collect())
    }
}
