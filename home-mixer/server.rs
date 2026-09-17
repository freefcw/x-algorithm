use crate::clients::redis_feed_state_store::RedisFeedStateStore;
#[cfg(test)]
use crate::feature_policy::HomeMixerFeatures;
use crate::feed_state::{FeedStateStore, InMemoryFeedStateStore};
use crate::for_you_server::ForYouFeedServer;
use crate::metrics::Metrics;
#[cfg(test)]
use crate::models::query::ScoredPostsQuery;
use crate::rpc_policy::{within_budget, RpcPolicy};
use crate::runtime_config::FeedStateConfig;
use crate::scored_posts_server::ScoredPostsServer;
use log::info;
use std::sync::Arc;
use tonic::codec::CompressionEncoding;
use tonic::service::RoutesBuilder;
use tonic::{Request, Response, Status};
use x_algorithm_proto::home_mixer as pb;
use x_algorithm_proto::home_mixer::ScoredPostsResponse;

use crate::debug_access::DebugAccessError;
pub use crate::debug_access::DebugAccessPolicy;
pub use crate::query_builder::{QueryBuilder, RequestContext};
pub use crate::runtime_config::{HomeMixerConfig, HomeMixerMode};

/// gRPC method names as they appear in the `rpc` metric label.
const RPC_GET_SCORED_POSTS: &str = "GetScoredPosts";
const RPC_DEBUG_SCORED_POSTS: &str = "DebugScoredPosts";
const RPC_GET_FOR_YOU_FEED: &str = "GetForYouFeed";
const RPC_GET_FOR_YOU_FEED_V2: &str = "GetForYouFeedV2";

pub struct HomeMixerServer {
    scored_posts_server: Arc<ScoredPostsServer>,
    for_you_feed_server: Arc<ForYouFeedServer>,
}

impl HomeMixerServer {
    pub async fn build(config: HomeMixerConfig) -> anyhow::Result<Self> {
        Self::build_with_metrics(config, Arc::new(Metrics::new())).await
    }

    /// Build against a registry the caller already exposes, so the admin HTTP
    /// server can start answering probes before the pipeline is assembled.
    pub async fn build_with_metrics(
        config: HomeMixerConfig,
        metrics: Arc<Metrics>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        if config.mode == HomeMixerMode::Degraded {
            log::warn!(
                "HOME_MIXER_MODE=degraded: caller identity, Gizmoduck, Phoenix metadata, and served persist contracts are still incomplete"
            );
        }
        let query_builder = QueryBuilder::new(config.features);
        // Stage summaries, side-effect outcomes and exposure-event publishes
        // land in the same registry the admin port exposes.
        let pipeline = crate::candidate_pipeline::phoenix_candidate_pipeline::PhoenixCandidatePipeline::
            assemble_with_uas_and_metrics(
                config.mode,
                config.features,
                config.uas,
                Arc::clone(&metrics),
            )
            .await?;
        let debug_access = DebugAccessPolicy::new(config.features.debug_rpc, config.debug_token);
        let state_store: Arc<dyn FeedStateStore> = match config.feed_state {
            FeedStateConfig::InMemory => Arc::new(InMemoryFeedStateStore::new(
                crate::params::LOCAL_SERVED_HISTORY_LIMIT,
                crate::params::LOCAL_REQUEST_TIMESTAMP_LIMIT,
            )),
            FeedStateConfig::Redis(redis) => Arc::new(
                RedisFeedStateStore::new(redis)
                    .await
                    .map(|store| store.with_calls(metrics.client_calls()))
                    .map_err(anyhow::Error::msg)?,
            ),
        };
        let scored_posts_server = Arc::new(
            ScoredPostsServer::with_state(query_builder, pipeline, state_store)
                .with_debug_access(debug_access)
                .with_rpc_policy(RpcPolicy::new(config.request_timeout, metrics)),
        );
        Ok(Self::with_scored_posts_server(scored_posts_server))
    }

    pub async fn new() -> anyhow::Result<Self> {
        Self::build(HomeMixerConfig::from_env()?).await
    }

    pub fn with_scored_posts_server(scored_posts_server: Arc<ScoredPostsServer>) -> Self {
        let query_builder = scored_posts_server.query_builder();
        let for_you_feed_server = Arc::new(ForYouFeedServer::new(
            query_builder,
            Arc::clone(&scored_posts_server),
        ));
        HomeMixerServer {
            scored_posts_server,
            for_you_feed_server,
        }
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(self.scored_posts_server.rpc_policy().metrics())
    }

    /// Shutdown step after the listeners have drained: wait up to `timeout`
    /// for side effects (exposure log, diversity stats) still running behind
    /// already-returned responses, then give every side effect of both
    /// pipelines its shutdown hook (the exposure sink flushes its producer
    /// there). Both pipelines spawn on one tracker, so one wait covers both.
    pub async fn drain_side_effects(&self, timeout: std::time::Duration) -> bool {
        let started = std::time::Instant::now();
        let drained = self.scored_posts_server.drain_side_effects(timeout).await;
        self.for_you_feed_server
            .shutdown_side_effects(timeout.saturating_sub(started.elapsed()))
            .await;
        drained
    }

    pub fn register(self: Arc<Self>, routes: &mut RoutesBuilder) {
        routes.add_service(
            pb::scored_posts_service_server::ScoredPostsServiceServer::from_arc(Arc::clone(
                &self.scored_posts_server,
            ))
            .max_decoding_message_size(crate::params::MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(crate::params::MAX_GRPC_MESSAGE_SIZE)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd),
        );
        routes.add_service(
            pb::for_you_feed_service_server::ForYouFeedServiceServer::from_arc(Arc::clone(
                &self.for_you_feed_server,
            ))
            .max_decoding_message_size(crate::params::MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(crate::params::MAX_GRPC_MESSAGE_SIZE)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd),
        );
    }
}

#[tonic::async_trait]
impl pb::for_you_feed_service_server::ForYouFeedService for ForYouFeedServer {
    async fn get_for_you_feed(
        &self,
        request: Request<pb::ScoredPostsQuery>,
    ) -> Result<Response<pb::ForYouFeedResponse>, Status> {
        let (metadata, _, proto_query) = request.into_parts();
        let budget = self.rpc_policy().budget(&metadata);
        self.rpc_policy()
            .observe(
                RPC_GET_FOR_YOU_FEED,
                run_for_you_rpc(self, proto_query, budget),
            )
            .await
    }

    async fn get_for_you_feed_v2(
        &self,
        request: Request<pb::ForYouFeedQuery>,
    ) -> Result<Response<pb::ForYouFeedResponse>, Status> {
        let (metadata, _, wrapper) = request.into_parts();
        let budget = self.rpc_policy().budget(&metadata);
        self.rpc_policy()
            .observe(RPC_GET_FOR_YOU_FEED_V2, async {
                let query = for_you_query_from_wrapper(wrapper)
                    .ok_or_else(|| Status::invalid_argument("ForYouFeedQuery.query is required"))?;
                run_for_you_rpc(self, query, budget).await
            })
            .await
    }
}

fn for_you_query_from_wrapper(wrapper: pb::ForYouFeedQuery) -> Option<pb::ScoredPostsQuery> {
    wrapper.query
}

fn persist_unavailable(error: String) -> Status {
    Status::unavailable(format!("served persist failed: {error}"))
}

async fn run_for_you_rpc(
    server: &ForYouFeedServer,
    proto_query: pb::ScoredPostsQuery,
    budget: std::time::Duration,
) -> Result<Response<pb::ForYouFeedResponse>, Status> {
    let context = server.query_builder().build(proto_query).await?;
    let query = context.query;
    let request_id = query.request_id.clone();
    info!("For You request - request_id {request_id}");
    let output = within_budget(budget, &request_id, async {
        let output = server.get_for_you_feed(query).await;
        match output.persist_error {
            Some(error) => Err(persist_unavailable(error)),
            None => Ok(output),
        }
    })
    .await?;
    Ok(Response::new(pb::ForYouFeedResponse {
        items: output
            .items
            .into_iter()
            .map(|item| item.into_proto())
            .collect(),
        request_id: output.request_id,
    }))
}

#[tonic::async_trait]
impl pb::scored_posts_service_server::ScoredPostsService for ScoredPostsServer {
    async fn get_scored_posts(
        &self,
        request: Request<pb::ScoredPostsQuery>,
    ) -> Result<Response<ScoredPostsResponse>, Status> {
        let (metadata, _, proto_query) = request.into_parts();
        let budget = self.rpc_policy().budget(&metadata);
        self.rpc_policy()
            .observe(RPC_GET_SCORED_POSTS, async {
                let context = self.query_builder().build(proto_query).await?;
                let query = context.query;
                let request_id = query.request_id.clone();
                info!("Scored Posts request - request_id {request_id}");
                let user_id = query.user_id;
                let request_time_ms = query.request_time_ms;
                let output = within_budget(budget, &request_id, async {
                    let output = self.score(query).await;
                    self.persist_selected(user_id, &output.selected_ids, request_time_ms)
                        .await
                        .map_err(persist_unavailable)?;
                    Ok(output)
                })
                .await?;
                Ok(Response::new(ScoredPostsResponse {
                    scored_posts: output.posts,
                }))
            })
            .await
    }

    async fn debug_scored_posts(
        &self,
        request: Request<pb::ScoredPostsQuery>,
    ) -> Result<Response<pb::DebugScoredPostsResponse>, Status> {
        let (metadata, _, proto_query) = request.into_parts();
        let budget = self.rpc_policy().budget(&metadata);
        self.rpc_policy()
            .observe(RPC_DEBUG_SCORED_POSTS, async {
                self.authorize_debug(&metadata)
                    .map_err(DebugAccessError::into_status)?;
                let context = self.query_builder().build(proto_query).await?;
                let query = context.query;
                let request_id = query.request_id.clone();
                info!("Debug Scored Posts request - request_id {request_id}");
                let user_id = query.user_id;
                let request_time_ms = query.request_time_ms;
                let (output, debug) = within_budget(budget, &request_id, async {
                    let (output, debug) = self.score_with_debug(query).await;
                    self.persist_selected(user_id, &output.selected_ids, request_time_ms)
                        .await
                        .map_err(persist_unavailable)?;
                    Ok((output, debug))
                })
                .await?;
                Ok(Response::new(pb::DebugScoredPostsResponse {
                    response: Some(ScoredPostsResponse {
                        scored_posts: output.posts,
                    }),
                    debug: Some(debug),
                }))
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::query::TopicRecallMode;

    #[tokio::test]
    async fn business_assembly_rejects_local_state_before_external_clients() {
        let error = match HomeMixerServer::build(HomeMixerConfig::default()).await {
            Err(error) => error,
            Ok(_) => panic!("business deployment must not silently use local history"),
        };
        assert!(
            error.to_string().contains("HOME_MIXER_REDIS_URL"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn demo_assembly_uses_explicit_local_state() {
        let server = HomeMixerServer::build(HomeMixerConfig {
            mode: HomeMixerMode::Demo,
            feed_state: FeedStateConfig::InMemory,
            ..Default::default()
        })
        .await
        .expect("self-contained demo");
        let state = server.scored_posts_server.feed_state_store();
        state
            .record(crate::models::uid(7), vec![crate::models::pid(9)], 123)
            .await
            .unwrap();
        assert_eq!(
            state
                .load(crate::models::uid(7))
                .await
                .unwrap()
                .served_post_ids,
            vec![crate::models::pid(9)]
        );
    }

    #[test]
    fn for_you_wrapper_requires_and_preserves_inner_query() {
        assert!(for_you_query_from_wrapper(pb::ForYouFeedQuery { query: None }).is_none());

        let inner = pb::ScoredPostsQuery {
            viewer_id: "00000000000000000000002a".to_string(),
            ..Default::default()
        };
        let extracted = for_you_query_from_wrapper(pb::ForYouFeedQuery { query: Some(inner) })
            .expect("wrapped query");
        assert_eq!(extracted.viewer_id, crate::models::uid(42).to_string());
    }

    #[tokio::test]
    async fn degraded_assembly_without_mrpyq_fails_to_start() {
        assert!(
            !std::env::var("MRPYQ_RECOMMENDATION_DATA_ADDR")
                .is_ok_and(|addr| !addr.trim().is_empty()),
            "unset MRPYQ_RECOMMENDATION_DATA_ADDR to run this test"
        );
        let result = HomeMixerServer::build(HomeMixerConfig {
            mode: HomeMixerMode::Degraded,
            features: HomeMixerFeatures::default(),
            debug_token: None,
            feed_state: FeedStateConfig::Redis(
                crate::clients::redis_feed_state_store::RedisFeedStateConfig::new(
                    "redis://localhost:6379/",
                ),
            ),
            uas: crate::runtime_config::UasConfig::Disabled,
            ..Default::default()
        })
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("real traffic cannot start without mrpyq"),
        };
        assert!(
            error.to_string().contains("MRPYQ_RECOMMENDATION_DATA_ADDR"),
            "{error}"
        );
    }

    #[test]
    fn application_servers_own_their_rpc_interfaces() {
        fn assert_scored<T: pb::scored_posts_service_server::ScoredPostsService>() {}
        fn assert_for_you<T: pb::for_you_feed_service_server::ForYouFeedService>() {}

        assert_scored::<ScoredPostsServer>();
        assert_for_you::<ForYouFeedServer>();
    }

    async fn build_query(
        proto_query: pb::ScoredPostsQuery,
        features: HomeMixerFeatures,
    ) -> ScoredPostsQuery {
        QueryBuilder::new(features)
            .build(proto_query)
            .await
            .expect("valid query")
            .query
    }

    #[tokio::test]
    async fn maps_p3_request_context_and_cached_candidates() {
        let query = build_query(
            pb::ScoredPostsQuery {
                viewer_id: "00000000000000000000002a".to_string(),
                client_app_id: 7,
                country_code: "US".to_string(),
                language_code: "en".to_string(),
                seen_ids: vec![crate::models::pid(1).to_string(), "-1".to_string()],
                served_ids: vec![crate::models::pid(2).to_string(), "-2".to_string()],
                in_network_only: true,
                is_bottom_request: true,
                bloom_filter_entries: vec![pb::ImpressionBloomFilterEntry {
                    data: vec![1],
                    num_bits: 8,
                    num_hash_functions: 2,
                }],
                topic_ids: vec![10],
                excluded_topic_ids: vec![99],
                new_user_topic_ids: vec![20],
                exclude_videos: true,
                enable_phoenix_moe: true,
                impressed_post_ids: vec![crate::models::pid(7).to_string(), "-7".to_string()],
                past_request_timestamps_ms: vec![1_700_000_000_000],
                cached_posts: vec![pb::CachedPost {
                    tweet_id: crate::models::pid(100).to_string(),
                    author_id: crate::models::uid(200).to_string(),
                    tweet_text: "cached text".to_string(),
                    quoted_tweet_id: crate::models::pid(300).to_string(),
                    filtered_topic_ids: vec![10],
                    video_duration_ms: 5_000,
                    language_code: "en".to_string(),
                    ..Default::default()
                }],
                is_preview: true,
                is_shadow_traffic: true,
                is_polling: true,
                ip_address: "203.0.113.1".to_string(),
                user_agent: "test-client".to_string(),
            },
            HomeMixerFeatures {
                phoenix_moe: true,
                unsigned_cached_posts: true,
                ..Default::default()
            },
        )
        .await;

        assert_eq!(query.user_id, crate::models::uid(42));
        assert_eq!(query.client_app_id, 7);
        assert_eq!(query.country_code, "US");
        assert_eq!(query.language_code, "en");
        assert_eq!(query.seen_ids, vec![crate::models::pid(1)]);
        assert_eq!(query.served_ids, vec![crate::models::pid(2)]);
        assert!(query.in_network_only && query.is_bottom_request);
        assert_eq!(query.bloom_filter_entries.len(), 1);
        assert!(query
            .request_id
            .ends_with(&format!("-{}", crate::models::uid(42))));
        assert!(query.prediction_id > 0);
        assert!(query.request_time_ms > 0);
        assert_eq!(query.topic_ids, vec![10]);
        assert_eq!(query.excluded_topic_ids, vec![99]);
        assert_eq!(query.new_user_topic_ids, vec![20]);
        assert_eq!(query.topic_recall_mode(), TopicRecallMode::Strict);
        assert!(query.exclude_videos);
        assert!(query.enable_phoenix_moe);
        assert_eq!(query.impressed_post_ids, vec![crate::models::pid(7)]);
        assert!(query.has_cached_posts);
        assert_eq!(query.cached_posts[0].tweet_id, crate::models::pid(100));
        assert_eq!(query.cached_posts[0].tweet_text, "cached text");
        assert_eq!(
            query.cached_posts[0].quoted_tweet_id,
            Some(crate::models::pid(300))
        );
        assert_eq!(query.cached_posts[0].filtered_topic_ids, vec![10]);
        assert_eq!(query.cached_posts[0].video_duration_ms, Some(5_000));
        assert_eq!(query.cached_posts[0].language_code.as_deref(), Some("en"));
        assert_eq!(
            query.cached_posts[0].served_type,
            Some(pb::ServedType::ForYouCachedPost)
        );
        assert!(query.is_preview && query.is_shadow_traffic && query.is_polling);
        assert_eq!(query.ip_address, "203.0.113.1");
        assert_eq!(query.user_agent, "test-client");
    }

    #[tokio::test]
    async fn maps_new_user_topics_to_cold_start_mode() {
        let query = build_query(
            pb::ScoredPostsQuery {
                viewer_id: "00000000000000000000002a".to_string(),
                new_user_topic_ids: vec![20],
                ..Default::default()
            },
            HomeMixerFeatures::default(),
        )
        .await;

        assert_eq!(query.new_user_topic_ids, vec![20]);
        assert_eq!(query.topic_recall_mode(), TopicRecallMode::ColdStart);
    }

    #[tokio::test]
    async fn request_defaults_to_full_network() {
        let query = build_query(
            pb::ScoredPostsQuery {
                viewer_id: "00000000000000000000002a".to_string(),
                ..Default::default()
            },
            HomeMixerFeatures::default(),
        )
        .await;

        assert!(!query.in_network_only);
    }

    #[tokio::test]
    async fn unsigned_cached_posts_are_rejected_by_default() {
        let error = QueryBuilder::default()
            .build(pb::ScoredPostsQuery {
                viewer_id: "00000000000000000000002a".to_string(),
                cached_posts: vec![pb::CachedPost {
                    tweet_id: crate::models::pid(100).to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .err()
            .expect("untrusted cached posts must be rejected");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn request_cannot_enable_moe_when_global_switch_is_off() {
        let query = build_query(
            pb::ScoredPostsQuery {
                viewer_id: "00000000000000000000002a".to_string(),
                enable_phoenix_moe: true,
                ..Default::default()
            },
            HomeMixerFeatures::default(),
        )
        .await;

        assert!(!query.enable_phoenix_moe);
    }

    #[tokio::test]
    async fn query_builder_rejects_non_positive_viewer_id() {
        for viewer_id in ["".to_string(), "0".to_string(), "not-an-id".to_string()] {
            let error = QueryBuilder::default()
                .build(pb::ScoredPostsQuery {
                    viewer_id,
                    ..Default::default()
                })
                .await
                .err()
                .expect("non-positive viewer_id must fail");

            assert_eq!(error.code(), tonic::Code::InvalidArgument);
        }
    }
}
