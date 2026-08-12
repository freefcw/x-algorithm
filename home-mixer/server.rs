use crate::clients::gizmoduck_client::{
    DemoGizmoduckClient, DisabledGizmoduckClient, GizmoduckClient,
};
#[cfg(test)]
use crate::clients::gizmoduck_client::{ViewerData, ViewerEligibility};
#[cfg(test)]
use crate::feature_policy::HomeMixerFeatures;
use crate::for_you_server::ForYouFeedServer;
#[cfg(test)]
use crate::models::query::ScoredPostsQuery;
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

pub struct HomeMixerServer {
    scored_posts_server: Arc<ScoredPostsServer>,
    for_you_feed_server: Arc<ForYouFeedServer>,
}

impl HomeMixerServer {
    pub async fn build(config: HomeMixerConfig) -> anyhow::Result<Self> {
        config.validate()?;
        if config.mode == HomeMixerMode::Degraded {
            log::warn!(
                "HOME_MIXER_MODE=degraded: mandatory production caller identity, Viewer, UAS, Strato, TES, Gizmoduck, VF, Phoenix, and Thunder contracts are unavailable"
            );
        }
        let viewer_client: Arc<dyn GizmoduckClient + Send + Sync> = match config.mode {
            HomeMixerMode::Demo => Arc::new(DemoGizmoduckClient),
            HomeMixerMode::Degraded | HomeMixerMode::ProductionReady => {
                Arc::new(DisabledGizmoduckClient)
            }
        };
        let query_builder = QueryBuilder::new(config.features, viewer_client);
        let pipeline = Arc::new(
            crate::candidate_pipeline::phoenix_candidate_pipeline::PhoenixCandidatePipeline::
                assemble_for_mode(config.mode, config.features)
                .await,
        );
        let debug_access = DebugAccessPolicy::new(config.features.debug_rpc, config.debug_token);
        let scored_posts_server = Arc::new(
            ScoredPostsServer::new(query_builder, pipeline).with_debug_access(debug_access),
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
        run_for_you_rpc(self, request.into_inner()).await
    }

    async fn get_for_you_feed_v2(
        &self,
        request: Request<pb::ForYouFeedQuery>,
    ) -> Result<Response<pb::ForYouFeedResponse>, Status> {
        let query = for_you_query_from_wrapper(request.into_inner())
            .ok_or_else(|| Status::invalid_argument("ForYouFeedQuery.query is required"))?;
        run_for_you_rpc(self, query).await
    }
}

fn for_you_query_from_wrapper(wrapper: pb::ForYouFeedQuery) -> Option<pb::ScoredPostsQuery> {
    wrapper.query
}

async fn run_for_you_rpc(
    server: &ForYouFeedServer,
    proto_query: pb::ScoredPostsQuery,
) -> Result<Response<pb::ForYouFeedResponse>, Status> {
    let context = server.query_builder().build(proto_query).await?;
    let query = context.query;
    info!("For You request - request_id {}", query.request_id);
    let output = server.get_for_you_feed(query).await;
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
        let context = self.query_builder().build(request.into_inner()).await?;
        let query = context.query;
        info!("Scored Posts request - request_id {}", query.request_id);
        let output = self.score(query).await;
        Ok(Response::new(ScoredPostsResponse {
            scored_posts: output.posts,
        }))
    }

    async fn debug_scored_posts(
        &self,
        request: Request<pb::ScoredPostsQuery>,
    ) -> Result<Response<pb::DebugScoredPostsResponse>, Status> {
        self.authorize_debug(request.metadata())
            .map_err(DebugAccessError::into_status)?;
        let context = self.query_builder().build(request.into_inner()).await?;
        let query = context.query;
        info!(
            "Debug Scored Posts request - request_id {}",
            query.request_id
        );
        let (output, debug) = self.score_with_debug(query).await;
        Ok(Response::new(pb::DebugScoredPostsResponse {
            response: Some(ScoredPostsResponse {
                scored_posts: output.posts,
            }),
            debug: Some(debug),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate_hydrators::vf_candidate_hydrator::VFCandidateHydrator;
    use crate::filters::ancillary_vf_filter::AncillaryVFFilter;
    use crate::models::candidate_features::GizmoduckUserResult;
    use crate::models::query::TopicRecallMode;
    use crate::visibility::models::FilteredReason;
    use crate::visibility::vf_client::{
        SafetyLevel, TwitterContextViewer, VisibilityFilteringClient,
    };
    use std::collections::HashMap;
    use std::time::Duration;
    use xai_candidate_pipeline::filter::Filter;
    use xai_candidate_pipeline::hydrator::Hydrator;

    #[test]
    fn for_you_wrapper_requires_and_preserves_inner_query() {
        assert!(for_you_query_from_wrapper(pb::ForYouFeedQuery { query: None }).is_none());

        let inner = pb::ScoredPostsQuery {
            viewer_id: 42,
            ..Default::default()
        };
        let extracted = for_you_query_from_wrapper(pb::ForYouFeedQuery { query: Some(inner) })
            .expect("wrapped query");
        assert_eq!(extracted.viewer_id, 42);
    }

    #[test]
    fn application_servers_own_their_rpc_interfaces() {
        fn assert_scored<T: pb::scored_posts_service_server::ScoredPostsService>() {}
        fn assert_for_you<T: pb::for_you_feed_service_server::ForYouFeedService>() {}

        assert_scored::<ScoredPostsServer>();
        assert_for_you::<ForYouFeedServer>();
    }

    enum ViewerResponse {
        Data(ViewerData),
        Error,
        Slow(ViewerData),
    }

    struct TestGizmoduckClient {
        response: ViewerResponse,
    }

    #[tonic::async_trait]
    impl GizmoduckClient for TestGizmoduckClient {
        async fn get_viewer_data(&self, _viewer_id: u64) -> Result<ViewerData, anyhow::Error> {
            match &self.response {
                ViewerResponse::Data(data) => Ok(data.clone()),
                ViewerResponse::Error => anyhow::bail!("viewer service unavailable"),
                ViewerResponse::Slow(data) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok(data.clone())
                }
            }
        }

        async fn get_users(
            &self,
            _user_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<GizmoduckUserResult>>, anyhow::Error> {
            Ok(HashMap::new())
        }
    }

    async fn build_query(
        proto_query: pb::ScoredPostsQuery,
        features: HomeMixerFeatures,
    ) -> ScoredPostsQuery {
        QueryBuilder::new(features, Arc::new(DisabledGizmoduckClient))
            .build(proto_query)
            .await
            .expect("valid query")
            .query
    }

    struct QuoteRejectingVisibilityClient;

    #[tonic::async_trait]
    impl VisibilityFilteringClient for QuoteRejectingVisibilityClient {
        async fn get_result(
            &self,
            tweet_ids: Vec<u64>,
            _safety_level: SafetyLevel,
            _for_user_id: u64,
            _context: Option<TwitterContextViewer>,
        ) -> Result<HashMap<u64, Option<FilteredReason>>, anyhow::Error> {
            Ok(tweet_ids
                .into_iter()
                .map(|id| {
                    let reason = (id == 300)
                        .then(|| FilteredReason::GenericFiltered("unsafe quote".to_string()));
                    (id, reason)
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn maps_p3_request_context_and_cached_candidates() {
        let query = build_query(
            pb::ScoredPostsQuery {
                viewer_id: 42,
                client_app_id: 7,
                country_code: "US".to_string(),
                language_code: "en".to_string(),
                seen_ids: vec![1, -1],
                served_ids: vec![2, -2],
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
                impressed_post_ids: vec![7, -7],
                past_request_timestamps_ms: vec![1_700_000_000_000],
                cached_posts: vec![pb::CachedPost {
                    tweet_id: 100,
                    author_id: 200,
                    tweet_text: "cached text".to_string(),
                    quoted_tweet_id: 300,
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

        assert_eq!(query.user_id, 42);
        assert_eq!(query.client_app_id, 7);
        assert_eq!(query.country_code, "US");
        assert_eq!(query.language_code, "en");
        assert_eq!(query.seen_ids, vec![1]);
        assert_eq!(query.served_ids, vec![2]);
        assert!(query.in_network_only && query.is_bottom_request);
        assert_eq!(query.bloom_filter_entries.len(), 1);
        assert!(query.request_id.ends_with("-42"));
        assert!(query.prediction_id > 0);
        assert!(query.request_time_ms > 0);
        assert_eq!(query.topic_ids, vec![10]);
        assert_eq!(query.excluded_topic_ids, vec![99]);
        assert_eq!(query.new_user_topic_ids, vec![20]);
        assert_eq!(query.topic_recall_mode(), TopicRecallMode::Strict);
        assert!(query.exclude_videos);
        assert!(query.enable_phoenix_moe);
        assert_eq!(query.impressed_post_ids, vec![7]);
        assert!(query.has_cached_posts);
        assert_eq!(query.cached_posts[0].tweet_id, 100);
        assert_eq!(query.cached_posts[0].tweet_text, "cached text");
        assert_eq!(query.cached_posts[0].quoted_tweet_id, Some(300));
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
                viewer_id: 42,
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
    async fn removes_cached_candidate_when_quoted_post_is_not_visible() {
        let query = build_query(
            pb::ScoredPostsQuery {
                viewer_id: 42,
                cached_posts: vec![pb::CachedPost {
                    tweet_id: 100,
                    author_id: 200,
                    quoted_tweet_id: 300,
                    ..Default::default()
                }],
                ..Default::default()
            },
            HomeMixerFeatures {
                unsigned_cached_posts: true,
                ..Default::default()
            },
        )
        .await;
        let hydrator = VFCandidateHydrator {
            vf_client: Arc::new(QuoteRejectingVisibilityClient),
        };
        let hydrated = hydrator.hydrate(&query, &query.cached_posts).await;
        let hydrated = hydrated[0].as_ref().expect("visibility hydration");
        let mut candidate = query.cached_posts[0].clone();
        hydrator.update(&mut candidate, hydrated.clone());

        let result = AncillaryVFFilter.filter(&query, vec![candidate]);

        assert!(result.kept.is_empty());
        assert_eq!(result.removed.len(), 1);
    }

    #[tokio::test]
    async fn viewer_policy_can_force_in_network_only() {
        let client = Arc::new(TestGizmoduckClient {
            response: ViewerResponse::Data(ViewerData {
                for_you_eligibility: ViewerEligibility::Denied,
            }),
        });
        let query = QueryBuilder::new(HomeMixerFeatures::default(), client)
            .build(pb::ScoredPostsQuery {
                viewer_id: 42,
                ..Default::default()
            })
            .await
            .expect("valid query")
            .query;

        assert!(query.in_network_only);
    }

    #[tokio::test]
    async fn unavailable_viewer_policy_restricts_to_in_network() {
        let client = Arc::new(TestGizmoduckClient {
            response: ViewerResponse::Error,
        });
        let query = QueryBuilder::new(HomeMixerFeatures::default(), client)
            .build(pb::ScoredPostsQuery {
                viewer_id: 42,
                ..Default::default()
            })
            .await
            .expect("viewer failure must not fail the request")
            .query;

        assert!(query.in_network_only);
    }

    #[tokio::test]
    async fn viewer_policy_timeout_restricts_to_in_network() {
        let client = Arc::new(TestGizmoduckClient {
            response: ViewerResponse::Slow(ViewerData {
                for_you_eligibility: ViewerEligibility::Allowed,
            }),
        });
        let query = QueryBuilder::new(HomeMixerFeatures::default(), client)
            .with_viewer_data_timeout(Duration::from_millis(1))
            .build(pb::ScoredPostsQuery {
                viewer_id: 42,
                ..Default::default()
            })
            .await
            .expect("viewer timeout must not fail the request")
            .query;

        assert!(query.in_network_only);
    }

    #[tokio::test]
    async fn explicit_viewer_permission_allows_out_of_network() {
        let client = Arc::new(TestGizmoduckClient {
            response: ViewerResponse::Data(ViewerData {
                for_you_eligibility: ViewerEligibility::Allowed,
            }),
        });
        let query = QueryBuilder::new(HomeMixerFeatures::default(), client)
            .build(pb::ScoredPostsQuery {
                viewer_id: 42,
                ..Default::default()
            })
            .await
            .expect("known viewer permission")
            .query;

        assert!(!query.in_network_only);
    }

    #[tokio::test]
    async fn unsigned_cached_posts_are_rejected_by_default() {
        let error = QueryBuilder::default()
            .build(pb::ScoredPostsQuery {
                viewer_id: 42,
                cached_posts: vec![pb::CachedPost {
                    tweet_id: 100,
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
                viewer_id: 42,
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
        for viewer_id in [0, -1] {
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
