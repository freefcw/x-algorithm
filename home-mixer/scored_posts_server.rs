use crate::candidate_pipeline::phoenix_candidate_pipeline::PhoenixCandidatePipeline;
use crate::clients::served_persistence::{InMemoryServedPersistence, ServedPersistence};
use crate::debug_access::{DebugAccessError, DebugAccessPolicy};
use crate::feed_state::{FeedStateStore, InMemoryFeedStateStore};
use crate::models::candidate::CandidateHelpers;
use crate::models::ids::{wire_id, wire_optional_id};
use crate::models::query::ScoredPostsQuery;
use crate::query_builder::QueryBuilder;
use crate::visibility::models::VisibilityDecision;
use log::info;
use std::sync::Arc;
use std::time::Instant;
use x_algorithm_proto::home_mixer as pb;
use x_algorithm_proto::home_mixer::ScoredPost;
use xai_candidate_pipeline::candidate_pipeline::CandidatePipeline;

pub struct ScoredPostsOutput {
    pub posts: Vec<ScoredPost>,
    pub selected_ids: Vec<crate::models::PostId>,
    pub request_id: String,
}

pub struct ScoredPostsServer {
    pipeline: Arc<PhoenixCandidatePipeline>,
    query_builder: QueryBuilder,
    debug_access: DebugAccessPolicy,
    feed_state: Arc<dyn FeedStateStore>,
    served_persist: Arc<dyn ServedPersistence>,
}

impl ScoredPostsServer {
    pub fn new(query_builder: QueryBuilder, pipeline: PhoenixCandidatePipeline) -> Self {
        let state_store: Arc<dyn FeedStateStore> = Arc::new(InMemoryFeedStateStore::new(
            crate::params::LOCAL_SERVED_HISTORY_LIMIT,
            crate::params::LOCAL_REQUEST_TIMESTAMP_LIMIT,
        ));
        Self::with_state(query_builder, pipeline, state_store)
    }

    pub fn with_state(
        query_builder: QueryBuilder,
        mut pipeline: PhoenixCandidatePipeline,
        state_store: Arc<dyn FeedStateStore>,
    ) -> Self {
        pipeline.install_feed_state_store(Arc::clone(&state_store));
        let pipeline = Arc::new(pipeline);
        for stage in pipeline.components() {
            info!(
                "Scored Posts components - stage={:?} components=[{}]",
                stage.stage,
                stage.components.join(", ")
            );
        }
        Self {
            pipeline,
            query_builder,
            debug_access: DebugAccessPolicy::default(),
            served_persist: Arc::new(InMemoryServedPersistence::new(Arc::clone(&state_store))),
            feed_state: state_store,
        }
    }

    pub fn with_pipeline(pipeline: PhoenixCandidatePipeline) -> Self {
        Self::new(QueryBuilder::default(), pipeline)
    }

    pub fn with_served_persist(mut self, persist: Arc<dyn ServedPersistence>) -> Self {
        self.served_persist = persist;
        self
    }

    pub fn with_debug_access(mut self, debug_access: DebugAccessPolicy) -> Self {
        self.debug_access = debug_access;
        self
    }

    pub(crate) fn authorize_debug(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<(), DebugAccessError> {
        self.debug_access.authorize(metadata)
    }

    pub(crate) fn query_builder(&self) -> QueryBuilder {
        self.query_builder.clone()
    }

    pub(crate) fn feed_state_store(&self) -> Arc<dyn FeedStateStore> {
        Arc::clone(&self.feed_state)
    }

    pub(crate) fn served_persist(&self) -> Arc<dyn ServedPersistence> {
        Arc::clone(&self.served_persist)
    }

    pub(crate) fn persist_selected(
        &self,
        user_id: crate::models::UserId,
        ids: &[crate::models::PostId],
        request_time_ms: i64,
    ) -> Result<(), String> {
        self.served_persist.persist(user_id, ids, request_time_ms)
    }

    pub async fn score(&self, query: ScoredPostsQuery) -> ScoredPostsOutput {
        self.score_with_debug(query).await.0
    }

    pub(crate) async fn score_with_debug(
        &self,
        query: ScoredPostsQuery,
    ) -> (ScoredPostsOutput, pb::PipelineDebugInfo) {
        let start = Instant::now();
        let pipeline_result = self.pipeline.execute(query).await;
        let request_id = pipeline_result.query.request_id.clone();
        let debug = pipeline_debug_info(
            request_id.clone(),
            &pipeline_result.retrieved_candidates,
            &pipeline_result.filtered_candidates,
            &pipeline_result.selected_candidates,
        );
        let selected_ids = pipeline_result
            .selected_candidates
            .iter()
            .map(|candidate| candidate.tweet_id)
            .collect::<Vec<_>>();
        let posts = pipeline_result
            .selected_candidates
            .into_iter()
            .map(candidate_to_scored_post)
            .collect::<Vec<_>>();

        info!(
            "Scored Posts response - request_id {} - {} posts ({} ms)",
            request_id,
            posts.len(),
            start.elapsed().as_millis()
        );
        (
            ScoredPostsOutput {
                posts,
                selected_ids,
                request_id,
            },
            debug,
        )
    }
}

fn pipeline_debug_info(
    request_id: String,
    retrieved: &[crate::models::candidate::PostCandidate],
    filtered: &[crate::models::candidate::PostCandidate],
    selected: &[crate::models::candidate::PostCandidate],
) -> pb::PipelineDebugInfo {
    pb::PipelineDebugInfo {
        request_id,
        retrieved: Some(stage_debug(retrieved)),
        filtered: Some(stage_debug(filtered)),
        selected: Some(stage_debug(selected)),
    }
}

fn stage_debug(candidates: &[crate::models::candidate::PostCandidate]) -> pb::PipelineStageDebug {
    pb::PipelineStageDebug {
        count: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
        tweet_ids: candidates
            .iter()
            .map(|candidate| candidate.tweet_id.to_string())
            .collect(),
    }
}

fn candidate_to_scored_post(candidate: crate::models::candidate::PostCandidate) -> ScoredPost {
    let screen_names = candidate
        .get_screen_names()
        .into_iter()
        .map(|(id, name)| (id.to_string(), name))
        .collect();
    ScoredPost {
        tweet_id: wire_id(candidate.tweet_id),
        author_id: wire_id(candidate.author_id),
        retweeted_tweet_id: wire_optional_id(candidate.retweeted_tweet_id),
        retweeted_user_id: wire_optional_id(candidate.retweeted_user_id),
        in_reply_to_tweet_id: wire_optional_id(candidate.in_reply_to_tweet_id),
        score: candidate.score.unwrap_or(0.0) as f32,
        in_network: candidate.in_network.unwrap_or(false),
        served_type: candidate
            .served_type
            .map(|value| value as i32)
            .unwrap_or_default(),
        last_scored_timestamp_ms: candidate.last_scored_at_ms.unwrap_or(0),
        prediction_request_id: candidate.prediction_request_id.unwrap_or(0),
        ancestors: candidate
            .ancestors
            .into_iter()
            .map(|id| id.to_string())
            .collect(),
        screen_names,
        visibility_reason: match candidate.visibility_decision {
            VisibilityDecision::Restricted(reason) => {
                let (reason_code, description) = reason.into_proto();
                Some(pb::VisibilityFilteredReason {
                    reason_code,
                    description,
                })
            }
            VisibilityDecision::Unchecked
            | VisibilityDecision::Allowed
            | VisibilityDecision::Unavailable(_) => None,
        },
        brand_safety_verdict: candidate
            .brand_safety_verdict
            .map(pb::BrandSafetyVerdict::from)
            .unwrap_or(pb::BrandSafetyVerdict::Unspecified) as i32,
        tweet_text: candidate.tweet_text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::brand_safety::BrandSafetyVerdict;
    use crate::models::candidate::PostCandidate;

    #[test]
    fn pipeline_debug_preserves_stage_counts_and_ids() {
        let retrieved = vec![PostCandidate {
            tweet_id: crate::models::pid(1),
            ..Default::default()
        }];
        let filtered = vec![PostCandidate {
            tweet_id: crate::models::pid(2),
            ..Default::default()
        }];
        let selected = vec![
            PostCandidate {
                tweet_id: crate::models::pid(3),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: crate::models::pid(4),
                ..Default::default()
            },
        ];

        let debug = pipeline_debug_info("request-1".to_string(), &retrieved, &filtered, &selected);

        assert_eq!(debug.request_id, "request-1");
        assert_eq!(
            debug.retrieved.expect("retrieved").tweet_ids,
            vec!["000000000000000000000001"]
        );
        assert_eq!(
            debug.filtered.expect("filtered").tweet_ids,
            vec!["000000000000000000000002"]
        );
        let selected = debug.selected.expect("selected");
        assert_eq!(selected.count, 2);
        assert_eq!(
            selected.tweet_ids,
            vec!["000000000000000000000003", "000000000000000000000004"]
        );
    }

    #[test]
    fn candidate_brand_safety_verdict_reaches_scored_post() {
        let cases = [
            (
                BrandSafetyVerdict::Unspecified,
                pb::BrandSafetyVerdict::Unspecified,
            ),
            (
                BrandSafetyVerdict::Safe,
                pb::BrandSafetyVerdict::SafeForAdjacency,
            ),
            (BrandSafetyVerdict::LowRisk, pb::BrandSafetyVerdict::LowRisk),
            (
                BrandSafetyVerdict::MediumRisk,
                pb::BrandSafetyVerdict::AvoidAdjacency,
            ),
        ];

        for (domain_verdict, wire_verdict) in cases {
            let scored_post = candidate_to_scored_post(PostCandidate {
                brand_safety_verdict: Some(domain_verdict),
                ..Default::default()
            });

            assert_eq!(scored_post.brand_safety_verdict, wire_verdict as i32);
        }
    }
}
