use crate::models::query::ScoredPostsQuery;
use crate::models::user_features::UserFeatures;
use crate::query_hydrators::user_features_query_hydrator::UserFeaturesQueryHydrator;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

/// Additive local owner for safety fields absent from the upstream hydrator list.
pub struct UserSafetyFeaturesQueryHydrator {
    provider: Arc<UserFeaturesQueryHydrator>,
}

impl UserSafetyFeaturesQueryHydrator {
    pub fn new(provider: Arc<UserFeaturesQueryHydrator>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for UserSafetyFeaturesQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        let features = self.provider.hydrate_features(query).await?;
        Ok(ScoredPostsQuery {
            user_features: UserFeatures {
                muted_keywords: features.muted_keywords,
                blocked_by_user_ids: features.blocked_by_user_ids,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.user_features.muted_keywords = hydrated.user_features.muted_keywords;
        query.user_features.blocked_by_user_ids = hydrated.user_features.blocked_by_user_ids;
    }
}
