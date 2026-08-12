//! 上游同构的已曝光帖子 Query Hydrator（QH-05）。
//!
//! 默认不装配：本地当前由请求方在公开 proto 中携带 `impressed_post_ids`，
//! `QueryBuilder` 负责映射。接入服务端曝光存储后，本组件覆盖请求值成为
//! 唯一数据源——启用属于装配层的数据源优先级决策，需要显式验收。

use crate::clients::impressed_posts_client::ImpressedPostsClient;
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

pub struct ImpressedPostsQueryHydrator {
    pub client: Arc<dyn ImpressedPostsClient>,
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for ImpressedPostsQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        let impressed_post_ids = self.client.get(query.user_id).await?;

        Ok(ScoredPostsQuery {
            impressed_post_ids,
            ..Default::default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.impressed_post_ids = hydrated.impressed_post_ids;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeImpressedPosts {
        ids: Vec<u64>,
    }

    #[async_trait]
    impl ImpressedPostsClient for FakeImpressedPosts {
        async fn get(&self, _user_id: u64) -> Result<Vec<u64>, String> {
            Ok(self.ids.clone())
        }
    }

    #[tokio::test]
    async fn store_values_replace_request_values() {
        let hydrator = ImpressedPostsQueryHydrator {
            client: Arc::new(FakeImpressedPosts { ids: vec![5, 6] }),
        };
        let mut query = ScoredPostsQuery {
            user_id: 42,
            impressed_post_ids: vec![1],
            ..Default::default()
        };

        let hydrated = hydrator.hydrate(&query).await.expect("hydrate");
        hydrator.update(&mut query, hydrated);

        assert_eq!(query.impressed_post_ids, vec![5, 6]);
    }
}
