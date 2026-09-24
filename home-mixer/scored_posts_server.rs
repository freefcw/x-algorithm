use crate::candidate_pipeline::phoenix_candidate_pipeline::PhoenixCandidatePipeline;
use crate::clients::served_persistence::{FeedStateServedPersistence, ServedPersistence};
use crate::debug_access::{DebugAccessError, DebugAccessPolicy};
use crate::feed_state::{FeedStateStore, InMemoryFeedStateStore};
use crate::id::{EntityKind, IdentityReader, SnowflakeId};
use crate::models::candidate::{CandidateHelpers, PostCandidate};
use crate::models::query::ScoredPostsQuery;
use crate::query_builder::QueryBuilder;
use crate::rpc_policy::RpcPolicy;
use crate::visibility::models::VisibilityDecision;
use log::info;
use std::collections::{HashMap, HashSet};
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
    rpc_policy: RpcPolicy,
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
            rpc_policy: RpcPolicy::default(),
            served_persist: Arc::new(FeedStateServedPersistence::new(Arc::clone(&state_store))),
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

    /// Request budget and metrics sink for the RPC entry points. The For You
    /// server built on top of this one inherits the same policy.
    pub fn with_rpc_policy(mut self, rpc_policy: RpcPolicy) -> Self {
        self.rpc_policy = rpc_policy;
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

    pub(crate) fn rpc_policy(&self) -> &RpcPolicy {
        &self.rpc_policy
    }

    pub(crate) fn feed_state_store(&self) -> Arc<dyn FeedStateStore> {
        Arc::clone(&self.feed_state)
    }

    pub(crate) fn served_persist(&self) -> Arc<dyn ServedPersistence> {
        Arc::clone(&self.served_persist)
    }

    /// Tracker of the pipeline's asynchronous side-effect tasks.
    pub fn background_tasks(&self) -> tokio_util::task::TaskTracker {
        self.pipeline.background_tasks()
    }

    /// Wait for outstanding side effects and flush their sinks; see
    /// `PhoenixCandidatePipeline::drain_side_effects`.
    pub async fn drain_side_effects(&self, timeout: std::time::Duration) -> bool {
        self.pipeline.drain_side_effects(timeout).await
    }

    pub(crate) async fn persist_selected(
        &self,
        user_id: crate::models::UserId,
        ids: &[crate::models::PostId],
        request_time_ms: i64,
        identity: Arc<crate::id::IdentityContext>,
    ) -> Result<(), String> {
        self.served_persist
            .persist(user_id, ids, request_time_ms, identity)
            .await
    }

    pub async fn score(&self, query: ScoredPostsQuery) -> Result<ScoredPostsOutput, String> {
        self.score_in_request_with_debug(query.start_request(), false)
            .await
            .map(|(output, _)| output)
    }

    pub(crate) async fn score_with_debug(
        &self,
        query: ScoredPostsQuery,
    ) -> Result<(ScoredPostsOutput, pb::PipelineDebugInfo), String> {
        self.score_in_request_with_debug(query.start_request(), true)
            .await
    }

    /// Score as part of an enclosing For You request, retaining its FeedState
    /// snapshot instead of starting a second request context.
    pub(crate) async fn score_in_request(
        &self,
        query: ScoredPostsQuery,
    ) -> Result<ScoredPostsOutput, String> {
        self.score_in_request_with_debug(query, false)
            .await
            .map(|(output, _)| output)
    }

    async fn score_in_request_with_debug(
        &self,
        query: ScoredPostsQuery,
        include_debug: bool,
    ) -> Result<(ScoredPostsOutput, pb::PipelineDebugInfo), String> {
        let start = Instant::now();
        let pipeline_result = self.pipeline.execute(query).await;
        let request_id = pipeline_result.query.request_id.clone();
        let external = self.external_ids(&pipeline_result, include_debug).await?;
        let debug = if include_debug {
            pipeline_debug_info(
                request_id.clone(),
                &pipeline_result.retrieved_candidates,
                &pipeline_result.filtered_candidates,
                &pipeline_result.selected_candidates,
                &external,
            )?
        } else {
            pb::PipelineDebugInfo::default()
        };
        let selected_ids = pipeline_result
            .selected_candidates
            .iter()
            .map(|candidate| candidate.tweet_id)
            .collect::<Vec<_>>();
        let posts = pipeline_result
            .selected_candidates
            .into_iter()
            .map(|candidate| candidate_to_scored_post(candidate, &external))
            .collect::<Result<Vec<_>, _>>()?;

        info!(
            "Scored Posts response - request_id {} - {} posts ({} ms)",
            request_id,
            posts.len(),
            start.elapsed().as_millis()
        );
        Ok((
            ScoredPostsOutput {
                posts,
                selected_ids,
                request_id,
            },
            debug,
        ))
    }

    /// Batch reverse every numeric ID the response exposes. Posts and users
    /// reverse in two kind-scoped calls; zero IDs are the internal NIL sentinel
    /// and skip the registry, mapping back to the empty wire form.
    async fn external_ids(
        &self,
        result: &xai_candidate_pipeline::candidate_pipeline::PipelineResult<
            ScoredPostsQuery,
            PostCandidate,
        >,
        include_debug: bool,
    ) -> Result<ExternalIds, String> {
        let mut post_ids = HashSet::new();
        let mut user_ids = HashSet::new();
        let candidates = if include_debug {
            result
                .retrieved_candidates
                .iter()
                .chain(&result.filtered_candidates)
                .chain(&result.selected_candidates)
                .collect::<Vec<_>>()
        } else {
            result.selected_candidates.iter().collect::<Vec<_>>()
        };
        for candidate in candidates {
            collect_post_ids(candidate, &mut post_ids);
            collect_user_ids(candidate, &mut user_ids);
        }
        Ok(ExternalIds {
            posts: reverse_kind(
                &*result.query.identity_context(),
                EntityKind::Post,
                post_ids,
            )
            .await?,
            users: reverse_kind(
                &*result.query.identity_context(),
                EntityKind::User,
                user_ids,
            )
            .await?,
        })
    }
}

fn collect_post_ids(candidate: &PostCandidate, out: &mut HashSet<u64>) {
    for id in [candidate.tweet_id]
        .into_iter()
        .chain(candidate.retweeted_tweet_id)
        .chain(candidate.in_reply_to_tweet_id)
        .chain(candidate.ancestors.iter().copied())
    {
        if id != 0 {
            out.insert(id);
        }
    }
}

fn collect_user_ids(candidate: &PostCandidate, out: &mut HashSet<u64>) {
    for id in [candidate.author_id]
        .into_iter()
        .chain(candidate.retweeted_user_id)
        .chain(candidate.get_screen_names().into_keys())
    {
        if id != 0 {
            out.insert(id);
        }
    }
}

/// One batch of registry reversals at a single entity kind, keyed by the
/// internal numeric ID for response assembly.
async fn reverse_kind(
    identity: &dyn IdentityReader,
    kind: EntityKind,
    ids: HashSet<u64>,
) -> Result<HashMap<u64, String>, String> {
    let ids = ids.into_iter().collect::<Vec<_>>();
    let pairs = ids
        .iter()
        .map(|id| {
            SnowflakeId::new(*id)
                .map(|snowflake| (snowflake, kind))
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let reversed = identity
        .reverse_batch(&pairs)
        .await
        .map_err(|error| error.to_string())?;
    Ok(ids.into_iter().zip(reversed).collect())
}

/// Registry-backed external wire IDs for one pipeline result.
struct ExternalIds {
    posts: HashMap<u64, String>,
    users: HashMap<u64, String>,
}

impl ExternalIds {
    fn post(&self, id: u64) -> Result<String, String> {
        if id == 0 {
            return Ok(String::new());
        }
        self.posts
            .get(&id)
            .cloned()
            .ok_or_else(|| format!("ID Registry returned no post mapping for internal ID {id}"))
    }

    fn user(&self, id: u64) -> Result<String, String> {
        if id == 0 {
            return Ok(String::new());
        }
        self.users
            .get(&id)
            .cloned()
            .ok_or_else(|| format!("ID Registry returned no user mapping for internal ID {id}"))
    }
}

fn pipeline_debug_info(
    request_id: String,
    retrieved: &[crate::models::candidate::PostCandidate],
    filtered: &[crate::models::candidate::PostCandidate],
    selected: &[crate::models::candidate::PostCandidate],
    external: &ExternalIds,
) -> Result<pb::PipelineDebugInfo, String> {
    Ok(pb::PipelineDebugInfo {
        request_id,
        retrieved: Some(stage_debug(retrieved, external)?),
        filtered: Some(stage_debug(filtered, external)?),
        selected: Some(stage_debug(selected, external)?),
    })
}

fn stage_debug(
    candidates: &[crate::models::candidate::PostCandidate],
    external: &ExternalIds,
) -> Result<pb::PipelineStageDebug, String> {
    Ok(pb::PipelineStageDebug {
        count: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
        tweet_ids: candidates
            .iter()
            .map(|candidate| external.post(candidate.tweet_id))
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn candidate_to_scored_post(
    candidate: crate::models::candidate::PostCandidate,
    external: &ExternalIds,
) -> Result<ScoredPost, String> {
    let screen_names = candidate
        .get_screen_names()
        .into_iter()
        .map(|(id, name)| external.user(id).map(|id| (id, name)))
        .collect::<Result<HashMap<_, _>, _>>()?;
    Ok(ScoredPost {
        tweet_id: external.post(candidate.tweet_id)?,
        author_id: external.user(candidate.author_id)?,
        retweeted_tweet_id: external.post(candidate.retweeted_tweet_id.unwrap_or(0))?,
        retweeted_user_id: external.user(candidate.retweeted_user_id.unwrap_or(0))?,
        in_reply_to_tweet_id: external.post(candidate.in_reply_to_tweet_id.unwrap_or(0))?,
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
            .map(|id| external.post(id))
            .collect::<Result<Vec<_>, _>>()?,
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::brand_safety::BrandSafetyVerdict;
    use crate::models::candidate::PostCandidate;
    use crate::models::ids::ObjectId;

    fn external() -> ExternalIds {
        ExternalIds {
            posts: (1..=4)
                .map(|n| (n, ObjectId::from_u64_be_padded(n).to_string()))
                .collect(),
            users: HashMap::new(),
        }
    }

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

        let debug = pipeline_debug_info(
            "request-1".to_string(),
            &retrieved,
            &filtered,
            &selected,
            &external(),
        )
        .expect("debug info");

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
            let scored_post = candidate_to_scored_post(
                PostCandidate {
                    brand_safety_verdict: Some(domain_verdict),
                    ..Default::default()
                },
                &external(),
            )
            .expect("scored post");

            assert_eq!(scored_post.brand_safety_verdict, wire_verdict as i32);
        }
    }
}
