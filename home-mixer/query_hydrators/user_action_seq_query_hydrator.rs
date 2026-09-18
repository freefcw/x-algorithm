use crate::clients::uas_fetcher::UserActionSequenceOps;
use crate::models::query::ScoredPostsQuery;
use crate::params as p;
use crate::recsys_compat::aggregation::{DefaultAggregator, UserActionAggregator};
use crate::recsys_compat::filters::{
    AggregatedActionFilter, DenseAggregatedActionFilter, KeepOriginalUserActionFilter,
    UserActionFilter,
};
use crate::uas_compat::convert::thrift_to_proto_aggregated_user_action;
use crate::uas_compat::{
    AggregatedUserAction as ThriftAggregatedUserAction,
    UserActionSequence as ThriftUserActionSequence,
    UserActionSequenceMeta as ThriftUserActionSequenceMeta,
};
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, OnceCell};
use tonic::async_trait;
use x_algorithm_proto::recsys::{
    user_action_sequence_data_container::Data as ProtoDataContainer, AggregatedUserActionList,
    Mask, MaskType, UserActionSequence, UserActionSequenceDataContainer, UserActionSequenceMeta,
};
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

/// Hydrate a sequence that captures the user's recent actions
pub struct UserActionSeqQueryHydrator {
    pub uas_fetcher: Arc<dyn UserActionSequenceOps>,
    global_filter: Arc<dyn UserActionFilter>,
    aggregator: Arc<dyn UserActionAggregator>,
    post_filters: Vec<Arc<dyn AggregatedActionFilter>>,
    fetch_timeout: Duration,
    sequence_cache: Mutex<SequenceCache>,
}

type CachedSequence = OnceCell<Result<UserActionSequence, String>>;
type SequenceCache = HashMap<String, Weak<CachedSequence>>;

impl UserActionSeqQueryHydrator {
    pub fn new(uas_fetcher: Arc<dyn UserActionSequenceOps>) -> Self {
        Self {
            uas_fetcher,
            global_filter: Arc::new(KeepOriginalUserActionFilter::new()),
            aggregator: Arc::new(DefaultAggregator),
            post_filters: vec![Arc::new(DenseAggregatedActionFilter::new())],
            fetch_timeout: Duration::from_millis(p::UAS_FETCH_TIMEOUT_MS),
            sequence_cache: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    pub fn with_fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = timeout;
        self
    }

    pub async fn hydrate_sequence(
        &self,
        query: &ScoredPostsQuery,
    ) -> Result<UserActionSequence, String> {
        let cache_key = format!(
            "{}:{}:{}",
            query.request_id, query.user_id, query.prediction_id
        );
        let cell = {
            let mut cache = self.sequence_cache.lock().await;
            match cache.get(&cache_key).and_then(Weak::upgrade) {
                Some(cell) => cell,
                None => {
                    let cell = Arc::new(OnceCell::new());
                    cache.insert(cache_key.clone(), Arc::downgrade(&cell));
                    cell
                }
            }
        };

        // Query hydrators run concurrently. Yield once so both upstream sequence
        // owners can join the same request-scoped cell before a fast demo adapter completes.
        tokio::task::yield_now().await;
        let result = cell
            .get_or_init(|| async {
                let uas_thrift = tokio::time::timeout(
                    self.fetch_timeout,
                    self.uas_fetcher.get_by_user_id(query.user_id),
                )
                .await
                .map_err(|_| {
                    format!(
                        "User action sequence fetch timed out after {}ms",
                        self.fetch_timeout.as_millis()
                    )
                })?
                .map_err(|e| format!("Failed to fetch user action sequence: {}", e))?;
                self.aggregate_user_action_sequence(query, uas_thrift)
            })
            .await
            .clone();

        let mut cache = self.sequence_cache.lock().await;
        if cache
            .get(&cache_key)
            .and_then(Weak::upgrade)
            .is_some_and(|cached| Arc::ptr_eq(&cached, &cell))
        {
            cache.remove(&cache_key);
        }

        result
    }
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for UserActionSeqQueryHydrator {
    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        let aggregated_uas_proto = self.hydrate_sequence(query).await?;

        Ok(ScoredPostsQuery {
            user_action_sequence: Some(aggregated_uas_proto.clone()),
            retrieval_sequence: Some(aggregated_uas_proto.clone()),
            scoring_sequence: Some(aggregated_uas_proto),
            ..Default::default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.user_action_sequence = hydrated.user_action_sequence;
        query.retrieval_sequence = hydrated.retrieval_sequence;
        query.scoring_sequence = hydrated.scoring_sequence;
    }

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

impl UserActionSeqQueryHydrator {
    fn aggregate_user_action_sequence(
        &self,
        query: &ScoredPostsQuery,
        uas_thrift: ThriftUserActionSequence,
    ) -> Result<UserActionSequence, String> {
        if query.request_time_ms <= 0 {
            return Err(format!(
                "invalid request_time_ms {}; cannot anchor UAS aggregation window for user {}",
                query.request_time_ms, query.user_id
            ));
        }

        // Extract user_actions from thrift sequence
        let thrift_user_actions = uas_thrift.user_actions.clone().unwrap_or_default();
        if thrift_user_actions.is_empty() {
            return Err(format!("No user actions found for user {}", query.user_id));
        }

        // Pre-aggregation filter
        let filtered_actions = self.global_filter.run(thrift_user_actions);
        if filtered_actions.is_empty() {
            return Err(format!(
                "No user actions remaining after filtering for user {}",
                query.user_id
            ));
        }

        // Aggregate
        let mut aggregated_actions = self.aggregator.run(
            &filtered_actions,
            p::UAS_WINDOW_TIME_MS,
            query.request_time_ms,
        );

        // Post-aggregation filters
        for filter in &self.post_filters {
            aggregated_actions = filter.run(aggregated_actions);
        }

        // Truncate to max sequence length (keep last N items)
        if aggregated_actions.len() > p::UAS_MAX_SEQUENCE_LENGTH {
            let drain_count = aggregated_actions.len() - p::UAS_MAX_SEQUENCE_LENGTH;
            aggregated_actions.drain(0..drain_count);
        }

        // Convert to proto format
        let original_metadata = uas_thrift.metadata.clone().unwrap_or_default();
        convert_to_proto_sequence(
            query.user_id,
            original_metadata,
            aggregated_actions,
            self.aggregator.name(),
        )
    }
}

fn convert_to_proto_sequence(
    user_id: crate::models::UserId,
    original_metadata: ThriftUserActionSequenceMeta,
    aggregated_actions: Vec<ThriftAggregatedUserAction>,
    aggregator_name: &str,
) -> Result<UserActionSequence, String> {
    if aggregated_actions.is_empty() {
        return Err("Cannot create sequence from empty aggregated actions".to_string());
    }

    let first_sequence_time = aggregated_actions
        .first()
        .and_then(|action| action.impressed_time_ms)
        .and_then(|time| u64::try_from(time).ok())
        .unwrap_or(0);
    let last_sequence_time = aggregated_actions
        .last()
        .and_then(|action| action.impressed_time_ms)
        .and_then(|time| u64::try_from(time).ok())
        .unwrap_or(0);

    // Preserve lastModifiedEpochMs and lastKafkaPublishEpochMs from original metadata
    let last_modified_epoch_ms = original_metadata
        .last_modified_epoch_ms
        .and_then(|time| u64::try_from(time).ok())
        .unwrap_or(0);
    let previous_kafka_publish_epoch_ms = original_metadata
        .last_kafka_publish_epoch_ms
        .and_then(|time| u64::try_from(time).ok())
        .unwrap_or(0);

    let proto_metadata = UserActionSequenceMeta {
        length: u64::try_from(aggregated_actions.len()).unwrap_or(u64::MAX),
        first_sequence_time,
        last_sequence_time,
        last_modified_epoch_ms,
        previous_kafka_publish_epoch_ms,
    };

    // Convert thrift aggregated actions to proto
    let mut proto_agg_actions = Vec::with_capacity(aggregated_actions.len());
    for action in aggregated_actions {
        proto_agg_actions.push(
            thrift_to_proto_aggregated_user_action(action)
                .map_err(|e| format!("Failed to convert aggregated action: {}", e))?,
        );
    }

    let aggregation_time_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);

    let agg_list = AggregatedUserActionList {
        aggregated_user_actions: proto_agg_actions,
        aggregation_provider: aggregator_name.to_string(),
        aggregation_time_ms,
    };

    let mask = Mask {
        mask_type: MaskType::NewEvent as i32,
        mask: vec![false; agg_list.aggregated_user_actions.len()],
    };

    // Build the final UserActionSequence
    Ok(UserActionSequence {
        user_id: user_id.to_string(),
        metadata: Some(proto_metadata),
        user_actions_data: Some(UserActionSequenceDataContainer {
            data: Some(ProtoDataContainer::OrderedAggregatedUserActionsList(
                agg_list,
            )),
        }),
        masks: vec![mask],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    struct SlowFetcher;

    #[async_trait]
    impl UserActionSequenceOps for SlowFetcher {
        async fn get_by_user_id(
            &self,
            _user_id: crate::models::UserId,
        ) -> Result<ThriftUserActionSequence, anyhow::Error> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(ThriftUserActionSequence {
                metadata: None,
                user_actions: Some(Vec::new()),
            })
        }
    }

    struct StubFetcher {
        actions: Vec<crate::uas_compat::UserAction>,
    }

    #[async_trait]
    impl UserActionSequenceOps for StubFetcher {
        async fn get_by_user_id(
            &self,
            _user_id: crate::models::UserId,
        ) -> Result<ThriftUserActionSequence, anyhow::Error> {
            Ok(ThriftUserActionSequence {
                metadata: None,
                user_actions: Some(self.actions.clone()),
            })
        }
    }

    fn raw_action(
        tweet_seq: u64,
        action_time_ms: i64,
        action_type: i32,
    ) -> crate::uas_compat::UserAction {
        crate::uas_compat::UserAction {
            tweet_id: Some(crate::models::pid(tweet_seq)),
            author_id: Some(crate::models::uid(100 + tweet_seq)),
            action_time_ms: Some(action_time_ms),
            action_type: Some(action_type),
            product_surface: Some(0),
        }
    }

    fn proto_aggregated_actions(
        sequence: &UserActionSequence,
    ) -> &[x_algorithm_proto::recsys::AggregatedUserAction] {
        match sequence
            .user_actions_data
            .as_ref()
            .and_then(|container| container.data.as_ref())
        {
            Some(ProtoDataContainer::OrderedAggregatedUserActionsList(list)) => {
                &list.aggregated_user_actions
            }
            _ => &[],
        }
    }

    #[tokio::test]
    async fn slow_uas_fetch_is_bounded() {
        let provider = UserActionSeqQueryHydrator::new(Arc::new(SlowFetcher))
            .with_fetch_timeout(Duration::from_millis(1));
        let query = ScoredPostsQuery {
            user_id: 42.into(),
            request_id: "slow-uas".to_string(),
            prediction_id: 7,
            ..Default::default()
        };

        let error = provider
            .hydrate_sequence(&query)
            .await
            .expect_err("slow UAS must time out");

        assert!(error.contains("timed out"));
    }

    #[tokio::test]
    async fn aggregation_anchors_window_at_request_time() {
        let reference_time_ms = 700_000_000_i64;
        let cutoff = reference_time_ms - p::UAS_WINDOW_TIME_MS as i64;
        let provider = UserActionSeqQueryHydrator::new(Arc::new(StubFetcher {
            actions: vec![
                raw_action(1, cutoff - 1, 1),
                raw_action(2, cutoff, 1),
                raw_action(3, reference_time_ms, 2),
                raw_action(4, reference_time_ms + 1, 1),
                raw_action(2, cutoff + 10, 3),
            ],
        }));
        let query = ScoredPostsQuery {
            user_id: crate::models::uid(7),
            request_id: "window-anchor".to_string(),
            prediction_id: 1,
            request_time_ms: reference_time_ms,
            ..Default::default()
        };

        let sequence = provider.hydrate_sequence(&query).await.expect("sequence");
        let actions = proto_aggregated_actions(&sequence);

        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].tweet_id, crate::models::pid(2).to_string());
        assert_eq!(actions[0].impressed_time_ms, cutoff as u64);
        assert!(actions[0].action_mask[1]);
        assert!(actions[0].action_mask[3]);
        assert_eq!(actions[1].tweet_id, crate::models::pid(3).to_string());
        assert_eq!(actions[1].impressed_time_ms, reference_time_ms as u64);

        let metadata = sequence.metadata.expect("metadata");
        assert_eq!(metadata.length, 2);
        assert_eq!(metadata.first_sequence_time, cutoff as u64);
        assert_eq!(metadata.last_sequence_time, reference_time_ms as u64);
    }

    #[tokio::test]
    async fn sequence_is_truncated_to_most_recent_max_length() {
        let reference_time_ms = 700_000_000_i64;
        let total = p::UAS_MAX_SEQUENCE_LENGTH + 5;
        let actions = (0..total)
            .map(|i| raw_action(i as u64 + 1, reference_time_ms - i as i64, 1))
            .collect();
        let provider = UserActionSeqQueryHydrator::new(Arc::new(StubFetcher { actions }));
        let query = ScoredPostsQuery {
            user_id: crate::models::uid(7),
            request_id: "truncate".to_string(),
            prediction_id: 1,
            request_time_ms: reference_time_ms,
            ..Default::default()
        };

        let sequence = provider.hydrate_sequence(&query).await.expect("sequence");
        let actions = proto_aggregated_actions(&sequence);

        assert_eq!(actions.len(), p::UAS_MAX_SEQUENCE_LENGTH);
        assert_eq!(
            actions.last().expect("last action").impressed_time_ms,
            reference_time_ms as u64
        );
        assert_eq!(
            actions.first().expect("first action").impressed_time_ms,
            (reference_time_ms - (p::UAS_MAX_SEQUENCE_LENGTH - 1) as i64) as u64
        );
    }

    #[tokio::test]
    async fn invalid_request_time_fails_aggregation() {
        let provider = UserActionSeqQueryHydrator::new(Arc::new(StubFetcher {
            actions: vec![raw_action(1, 10, 1)],
        }));
        let query = ScoredPostsQuery {
            user_id: crate::models::uid(7),
            request_id: "bad-time".to_string(),
            prediction_id: 1,
            request_time_ms: 0,
            ..Default::default()
        };

        let error = provider
            .hydrate_sequence(&query)
            .await
            .expect_err("nonpositive request time must fail");

        assert!(error.contains("request_time_ms"));
    }
}
