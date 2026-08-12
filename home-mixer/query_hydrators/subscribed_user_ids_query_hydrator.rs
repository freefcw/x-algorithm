use crate::models::query::ScoredPostsQuery;
use crate::models::user_features::UserFeatures;
use crate::query_hydrators::user_features_query_hydrator::UserFeaturesQueryHydrator;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

pub struct SubscribedUserIdsQueryHydrator {
    provider: Arc<UserFeaturesQueryHydrator>,
}

impl SubscribedUserIdsQueryHydrator {
    pub fn new(provider: Arc<UserFeaturesQueryHydrator>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for SubscribedUserIdsQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        Ok(ScoredPostsQuery {
            user_features: UserFeatures {
                subscribed_user_ids: self
                    .provider
                    .hydrate_features(query)
                    .await?
                    .subscribed_user_ids,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.user_features.subscribed_user_ids = hydrated.user_features.subscribed_user_ids;
    }
}
