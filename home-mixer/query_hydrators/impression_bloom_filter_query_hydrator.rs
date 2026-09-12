//! 上游同构的曝光 Bloom Filter Query Hydrator（QH-06）。
//!
//! 默认不装配：本地当前由请求方在公开 proto 中携带 `bloom_filter_entries`。
//! 接入服务端曝光存储后，本组件覆盖请求值成为唯一数据源——启用属于装配
//! 层的数据源优先级决策，需要显式验收。

use crate::clients::impression_bloom_filter_client::ImpressionBloomFilterClient;
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

pub struct ImpressionBloomFilterQueryHydrator {
    pub client: Arc<dyn ImpressionBloomFilterClient>,
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for ImpressionBloomFilterQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        let bloom_filter_entries = self.client.get(query.user_id).await?;

        Ok(ScoredPostsQuery {
            bloom_filter_entries,
            ..Default::default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.bloom_filter_entries = hydrated.bloom_filter_entries;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use x_algorithm_proto::home_mixer::ImpressionBloomFilterEntry;

    struct FakeBloomFilterClient {
        entries: Vec<ImpressionBloomFilterEntry>,
    }

    #[async_trait]
    impl ImpressionBloomFilterClient for FakeBloomFilterClient {
        async fn get(
            &self,
            _user_id: crate::models::UserId,
        ) -> Result<Vec<ImpressionBloomFilterEntry>, String> {
            Ok(self.entries.clone())
        }
    }

    #[tokio::test]
    async fn store_entries_replace_request_entries() {
        let store_entry = ImpressionBloomFilterEntry {
            data: vec![1, 2, 3],
            num_bits: 128,
            num_hash_functions: 2,
        };
        let hydrator = ImpressionBloomFilterQueryHydrator {
            client: Arc::new(FakeBloomFilterClient {
                entries: vec![store_entry.clone()],
            }),
        };
        let mut query = ScoredPostsQuery {
            user_id: 42.into(),
            ..Default::default()
        };

        let hydrated = hydrator.hydrate(&query).await.expect("hydrate");
        hydrator.update(&mut query, hydrated);

        assert_eq!(query.bloom_filter_entries, vec![store_entry]);
    }
}
