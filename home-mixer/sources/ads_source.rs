use crate::final_feed::{AdvertisementSource, FeedItem};
use crate::models::query::ScoredPostsQuery;
use tonic::async_trait;
use xai_candidate_pipeline::source::Source;

/// Explicitly disabled upstream Ads source entry.
pub struct AdsSource {
    inner: AdvertisementSource,
}

impl AdsSource {
    pub fn disabled() -> Self {
        Self {
            inner: AdvertisementSource::disabled(),
        }
    }
}

#[async_trait]
impl Source<ScoredPostsQuery, FeedItem> for AdsSource {
    fn enable(&self, _query: &ScoredPostsQuery) -> bool {
        false
    }

    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<FeedItem>, String> {
        self.inner.source(query).await
    }
}
