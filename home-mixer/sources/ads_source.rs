use crate::models::feed_item::{Advertisement, FeedItem, FeedItemContent};
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::source::Source;

/// Port for a future real advertisement service (U3: no public ads contract).
#[async_trait]
pub trait AdvertisementProvider: Send + Sync {
    async fn fetch(&self, query: &ScoredPostsQuery) -> Result<Vec<Advertisement>, String>;
}

pub struct DisabledAdvertisementProvider;

#[async_trait]
impl AdvertisementProvider for DisabledAdvertisementProvider {
    async fn fetch(&self, _query: &ScoredPostsQuery) -> Result<Vec<Advertisement>, String> {
        Ok(Vec::new())
    }
}

pub struct AdvertisementSource {
    provider: Arc<dyn AdvertisementProvider>,
}

impl AdvertisementSource {
    pub fn new(provider: Arc<dyn AdvertisementProvider>) -> Self {
        Self { provider }
    }

    pub fn disabled() -> Self {
        Self::new(Arc::new(DisabledAdvertisementProvider))
    }
}

#[async_trait]
impl Source<ScoredPostsQuery, FeedItem> for AdvertisementSource {
    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<FeedItem>, String> {
        Ok(self
            .provider
            .fetch(query)
            .await?
            .into_iter()
            .map(|advertisement| FeedItem {
                position: 0,
                content: FeedItemContent::Advertisement(advertisement),
            })
            .collect())
    }
}

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
