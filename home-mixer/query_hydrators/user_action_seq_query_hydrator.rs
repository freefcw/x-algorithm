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
                self.aggregate_user_action_sequence(query.user_id, uas_thrift)
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
        user_id: u64,
        uas_thrift: ThriftUserActionSequence,
    ) -> Result<UserActionSequence, String> {
        // Extract user_actions from thrift sequence
        let thrift_user_actions = uas_thrift.user_actions.clone().unwrap_or_default();
        if thrift_user_actions.is_empty() {
            return Err(format!("No user actions found for user {}", user_id));
        }

        // Pre-aggregation filter
        let filtered_actions = self.global_filter.run(thrift_user_actions);
        if filtered_actions.is_empty() {
            return Err(format!(
                "No user actions remaining after filtering for user {}",
                user_id
            ));
        }

        // Aggregate
        let mut aggregated_actions =
            self.aggregator
                .run(&filtered_actions, p::UAS_WINDOW_TIME_MS, 0);

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
            user_id,
            original_metadata,
            aggregated_actions,
            self.aggregator.name(),
        )
    }
}

fn convert_to_proto_sequence(
    user_id: u64,
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
        // TEMP(U4-P1): 十进制 u64 桥接，P1 迁移到 PostId 后删除
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
    use crate::clients::uas_fetcher::DemoUserActionSequenceFetcher;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingFetcher {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl UserActionSequenceOps for CountingFetcher {
        async fn get_by_user_id(
            &self,
            user_id: u64,
        ) -> Result<ThriftUserActionSequence, anyhow::Error> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            DemoUserActionSequenceFetcher.get_by_user_id(user_id).await
        }
    }

    struct SlowFetcher;

    #[async_trait]
    impl UserActionSequenceOps for SlowFetcher {
        async fn get_by_user_id(
            &self,
            user_id: u64,
        ) -> Result<ThriftUserActionSequence, anyhow::Error> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            DemoUserActionSequenceFetcher.get_by_user_id(user_id).await
        }
    }

    #[tokio::test]
    async fn slow_uas_fetch_is_bounded() {
        let provider = UserActionSeqQueryHydrator::new(Arc::new(SlowFetcher))
            .with_fetch_timeout(Duration::from_millis(1));
        let query = ScoredPostsQuery {
            user_id: 42,
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
    async fn concurrent_sequence_owners_share_one_request_fetch() {
        let fetcher = Arc::new(CountingFetcher {
            calls: AtomicUsize::new(0),
        });
        let provider = UserActionSeqQueryHydrator::new(fetcher.clone());
        let query = ScoredPostsQuery {
            user_id: 42,
            request_id: "request-1".to_string(),
            prediction_id: 7,
            ..Default::default()
        };

        let (scoring, retrieval) = tokio::join!(
            provider.hydrate_sequence(&query),
            provider.hydrate_sequence(&query)
        );

        assert!(scoring.is_ok());
        assert!(retrieval.is_ok());
        assert_eq!(fetcher.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn different_users_never_share_sequence_results() {
        let fetcher = Arc::new(CountingFetcher {
            calls: AtomicUsize::new(0),
        });
        let provider = UserActionSeqQueryHydrator::new(fetcher.clone());
        let first = ScoredPostsQuery {
            user_id: 42,
            request_id: "same-request-label".to_string(),
            prediction_id: 7,
            ..Default::default()
        };
        let second = ScoredPostsQuery {
            user_id: 43,
            request_id: first.request_id.clone(),
            prediction_id: first.prediction_id,
            ..Default::default()
        };

        let (first_result, second_result) = tokio::join!(
            provider.hydrate_sequence(&first),
            provider.hydrate_sequence(&second)
        );

        // TEMP(U4-P1): 十进制 u64 桥接，P1 迁移到 PostId 后删除
        assert_eq!(first_result.expect("first sequence").user_id, "42");
        assert_eq!(second_result.expect("second sequence").user_id, "43");
        assert_eq!(fetcher.calls.load(Ordering::Relaxed), 2);
    }
}
