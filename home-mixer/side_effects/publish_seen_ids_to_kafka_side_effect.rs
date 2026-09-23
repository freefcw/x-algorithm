//! 上游同构的已见帖子发布 SideEffect（SE-07）。
//!
//! 上游把 thrift 曝光列表发到 Kafka（topic、集群、序列化都在内部合同
//! 里）。本地只保留领域级端口 `SeenIdsPublisher`（U1）：Adapter 负责
//! topic、schema、序列化、重试与幂等。默认不装配；上游的 `is_prod` 与
//! feature switch 启用条件对应到装配层的显式注入决策。

use crate::models::feed_item::FeedItem;
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::side_effect::{SideEffect, SideEffectInput};

#[async_trait]
pub trait SeenIdsPublisher: Send + Sync {
    async fn publish_seen_ids(
        &self,
        user_id: crate::models::UserId,
        request_time_ms: i64,
        seen_ids: &[crate::models::PostId],
    ) -> Result<(), String>;
}

pub struct PublishSeenIdsToKafkaSideEffect {
    publisher: Arc<dyn SeenIdsPublisher>,
}

impl PublishSeenIdsToKafkaSideEffect {
    pub fn new(publisher: Arc<dyn SeenIdsPublisher>) -> Self {
        Self { publisher }
    }
}

#[async_trait]
impl SideEffect<ScoredPostsQuery, FeedItem> for PublishSeenIdsToKafkaSideEffect {
    fn enable(&self, query: Arc<ScoredPostsQuery>) -> bool {
        !query.seen_ids.is_empty()
    }

    async fn side_effect(
        &self,
        input: Arc<SideEffectInput<ScoredPostsQuery, FeedItem>>,
    ) -> Result<(), String> {
        let query = &input.query;
        if query.seen_ids.is_empty() {
            return Ok(());
        }

        self.publisher
            .publish_seen_ids(query.user_id, query.request_time_ms, &query.seen_ids)
            .await
            .map_err(|error| format!("Seen-IDs publish failed: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingPublisher {
        published: Mutex<Vec<(crate::models::UserId, i64, Vec<crate::models::PostId>)>>,
    }

    #[async_trait]
    impl SeenIdsPublisher for RecordingPublisher {
        async fn publish_seen_ids(
            &self,
            user_id: crate::models::UserId,
            request_time_ms: i64,
            seen_ids: &[crate::models::PostId],
        ) -> Result<(), String> {
            self.published.lock().expect("publish lock").push((
                user_id,
                request_time_ms,
                seen_ids.to_vec(),
            ));
            Ok(())
        }
    }

    #[tokio::test]
    async fn publishes_seen_ids_and_skips_empty_requests() {
        let publisher = Arc::new(RecordingPublisher::default());
        let side_effect = PublishSeenIdsToKafkaSideEffect::new(
            Arc::clone(&publisher) as Arc<dyn SeenIdsPublisher>
        );

        let empty_query = Arc::new(ScoredPostsQuery::test_default());
        assert!(!side_effect.enable(Arc::clone(&empty_query)));

        let query = ScoredPostsQuery {
            user_id: 42,
            request_time_ms: 1_700_000_000_000,
            seen_ids: vec![1, 2],
            ..ScoredPostsQuery::test_default()
        };
        assert!(side_effect.enable(Arc::new(query.clone())));

        let input = Arc::new(SideEffectInput {
            query: Arc::new(query),
            selected_candidates: Vec::new(),
            non_selected_candidates: Vec::new(),
        });
        side_effect.side_effect(input).await.expect("side effect");

        let published = publisher.published.lock().expect("publish lock");
        assert_eq!(
            published.as_slice(),
            &[(
                crate::models::uid(42),
                1_700_000_000_000,
                vec![crate::models::pid(1), crate::models::pid(2)]
            )]
        );
    }
}
