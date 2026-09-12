//! 上游同构的最终下发候选发布 SideEffect（SE-11）。
//!
//! 上游把 timeline_logging thrift 记录发到 Kafka，用于训练样本与审计。
//! 本地只保留领域级端口 `ServedCandidatesSink`（U1）：Adapter 负责事件
//! schema、序列化、topic、重试与幂等，未来接入时把领域 Query/FeedItem
//! 映射为自己的记录格式。默认不装配；上游的 `is_prod` 与 feature switch
//! 启用条件对应到装配层的显式注入决策，影子流量门槛保留为请求级条件。

use crate::models::feed_item::FeedItem;
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::side_effect::{SideEffect, SideEffectInput};

#[async_trait]
pub trait ServedCandidatesSink: Send + Sync {
    async fn publish(&self, query: &ScoredPostsQuery, items: &[FeedItem]) -> Result<(), String>;
}

pub struct ServedCandidatesKafkaSideEffect {
    sink: Arc<dyn ServedCandidatesSink>,
}

impl ServedCandidatesKafkaSideEffect {
    pub fn new(sink: Arc<dyn ServedCandidatesSink>) -> Self {
        Self { sink }
    }
}

#[async_trait]
impl SideEffect<ScoredPostsQuery, FeedItem> for ServedCandidatesKafkaSideEffect {
    fn enable(&self, query: Arc<ScoredPostsQuery>) -> bool {
        query.is_shadow_traffic
    }

    async fn side_effect(
        &self,
        input: Arc<SideEffectInput<ScoredPostsQuery, FeedItem>>,
    ) -> Result<(), String> {
        if input.selected_candidates.is_empty() {
            return Ok(());
        }

        self.sink
            .publish(&input.query, &input.selected_candidates)
            .await
            .map_err(|error| format!("Served-candidates publish failed: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use x_algorithm_proto::home_mixer::ScoredPost;

    #[derive(Default)]
    struct RecordingSink {
        published: Mutex<Vec<(String, Vec<crate::models::PostId>)>>,
    }

    #[async_trait]
    impl ServedCandidatesSink for RecordingSink {
        async fn publish(
            &self,
            query: &ScoredPostsQuery,
            items: &[FeedItem],
        ) -> Result<(), String> {
            self.published.lock().expect("publish lock").push((
                query.request_id.clone(),
                items.iter().filter_map(FeedItem::post_id).collect(),
            ));
            Ok(())
        }
    }

    #[tokio::test]
    async fn only_shadow_traffic_enables_and_items_are_published() {
        let sink = Arc::new(RecordingSink::default());
        let side_effect = ServedCandidatesKafkaSideEffect::new(
            Arc::clone(&sink) as Arc<dyn ServedCandidatesSink>
        );

        assert!(!side_effect.enable(Arc::new(ScoredPostsQuery::default())));

        let query = ScoredPostsQuery {
            request_id: "req-1".to_string(),
            is_shadow_traffic: true,
            ..Default::default()
        };
        assert!(side_effect.enable(Arc::new(query.clone())));

        let input = Arc::new(SideEffectInput {
            query: Arc::new(query),
            selected_candidates: vec![FeedItem::post(ScoredPost {
                tweet_id: crate::models::pid(9).to_string(),
                ..Default::default()
            })],
            non_selected_candidates: Vec::new(),
        });
        side_effect.side_effect(input).await.expect("side effect");

        let published = sink.published.lock().expect("publish lock");
        assert_eq!(
            published.as_slice(),
            &[("req-1".to_string(), vec![crate::models::pid(9)])]
        );
    }
}
