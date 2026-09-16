use crate::models::query::ScoredPostsQuery;
use crate::models::user_features::UserFeatures;
use crate::query_hydrators::user_features_query_hydrator::UserFeaturesQueryHydrator;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

pub struct BlockedUserIdsQueryHydrator {
    provider: Arc<UserFeaturesQueryHydrator>,
}

impl BlockedUserIdsQueryHydrator {
    pub fn new(provider: Arc<UserFeaturesQueryHydrator>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for BlockedUserIdsQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        Ok(ScoredPostsQuery {
            user_features: UserFeatures {
                blocked_user_ids: self
                    .provider
                    .hydrate_features(query)
                    .await?
                    .blocked_user_ids,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.user_features.blocked_user_ids = hydrated.user_features.blocked_user_ids;
        query.viewer_relations_hydrated = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::strato_client::DemoStratoClient;
    use crate::models::uid;

    #[tokio::test]
    async fn a_successful_read_marks_viewer_relations_ready() {
        let hydrator = BlockedUserIdsQueryHydrator::new(Arc::new(UserFeaturesQueryHydrator::new(
            Arc::new(DemoStratoClient),
        )));
        let query = ScoredPostsQuery {
            user_id: uid(42),
            request_id: "relations-ready".to_string(),
            ..Default::default()
        };

        let hydrated = hydrator.hydrate(&query).await.expect("demo relations");
        let mut updated = query;
        hydrator.update(&mut updated, hydrated);

        assert!(updated.viewer_relations_hydrated);
    }
}
