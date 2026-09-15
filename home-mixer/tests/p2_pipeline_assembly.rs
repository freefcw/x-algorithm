use home_mixer::clients::served_persistence::ServedPersistence;
use home_mixer::feed_state::{FeedStateSnapshot, FeedStateStore, InMemoryFeedStateStore};
use home_mixer::feed_stats::InMemoryFeedStats;
use home_mixer::for_you_server::ForYouFeedServer;
use home_mixer::models::candidate::PostCandidate;
use home_mixer::models::{pid, uid};
use home_mixer::query_builder::QueryBuilder;
use home_mixer::scored_posts_server::{ScoredPostsOutput, ScoredPostsServer};
use home_mixer::selectors::blender_selector::BlenderConfig;
use home_mixer::sources::scored_posts_source::ScoredPostsProvider;
use home_mixer::{HomeMixerFeatures, PhoenixCandidatePipeline};
use std::sync::Arc;
use tonic::Request;
use x_algorithm_proto::home_mixer as pb;
use x_algorithm_proto::home_mixer::for_you_feed_service_server::ForYouFeedService;
use x_algorithm_proto::home_mixer::scored_posts_service_server::ScoredPostsService;
use x_algorithm_proto::home_mixer::ScoredPost;
use xai_candidate_pipeline::candidate_pipeline::{CandidatePipeline, PipelineStage};

fn names(
    components: &[xai_candidate_pipeline::candidate_pipeline::PipelineComponents],
    stage: PipelineStage,
) -> Vec<String> {
    components
        .iter()
        .find(|entry| entry.stage == stage)
        .map(|entry| entry.components.clone())
        .unwrap_or_default()
}

#[tokio::test]
async fn degraded_assembly_without_mrpyq_fails_to_start() {
    assert!(
        !std::env::var("MRPYQ_RECOMMENDATION_DATA_ADDR").is_ok_and(|addr| !addr.trim().is_empty()),
        "unset MRPYQ_RECOMMENDATION_DATA_ADDR to run this test"
    );
    let result = PhoenixCandidatePipeline::assemble_for_mode(
        home_mixer::runtime_config::HomeMixerMode::Degraded,
        HomeMixerFeatures::default(),
    )
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

#[tokio::test]
async fn demo_assembly_contains_p2_fallback_components_and_u5_is_absent() {
    let pipeline = PhoenixCandidatePipeline::assemble_for_mode(
        home_mixer::runtime_config::HomeMixerMode::Demo,
        HomeMixerFeatures::default(),
    )
    .await
    .expect("demo assembly");
    let components = pipeline.components();

    let sources = names(&components, PipelineStage::Source);
    assert!(
        sources.contains(&"FallbackSource".to_string()),
        "{sources:?}"
    );
    #[cfg(feature = "legacy-int-ids")]
    assert!(
        sources.contains(&"ThunderSource".to_string()),
        "demo assembly should keep integer Thunder for padded IDs: {sources:?}"
    );
    assert!(
        names(&components, PipelineStage::Filter).contains(&"FirstStageEligibleFilter".to_string())
    );
    assert!(names(&components, PipelineStage::Scorer).contains(&"RuleFallbackScorer".to_string()));

    for stage in [
        PipelineStage::QueryHydrator,
        PipelineStage::Source,
        PipelineStage::Hydrator,
        PipelineStage::Filter,
        PipelineStage::PostSelectionFilter,
    ] {
        let names = names(&components, stage);
        for removed in [
            "QuoteHydrator",
            "SubscriptionHydrator",
            "SubscribedUserIdsQueryHydrator",
            "RetweetDeduplicationFilter",
            "IneligibleSubscriptionFilter",
            "AncillaryVFFilter",
        ] {
            assert!(
                !names.iter().any(|name| name == removed),
                "U5 component {removed} unexpectedly assembled in {stage:?}"
            );
        }
    }
}

#[tokio::test]
async fn served_state_is_read_by_the_same_pipeline_that_persists_it() {
    let mut pipeline = PhoenixCandidatePipeline::assemble_for_mode(
        home_mixer::runtime_config::HomeMixerMode::Demo,
        HomeMixerFeatures::default(),
    )
    .await
    .expect("demo assembly");
    let store: Arc<dyn FeedStateStore> = Arc::new(InMemoryFeedStateStore::new(10, 10));
    store.record(uid(7), vec![pid(9)], 123).expect("seed state");
    pipeline.install_feed_state_store(store);
    let components = pipeline.components();
    let query_hydrators = names(&components, PipelineStage::QueryHydrator);
    assert_eq!(query_hydrators[0], "ServedHistoryQueryHydrator");
    assert_eq!(query_hydrators[1], "PastRequestTimestampsQueryHydrator");
}

struct FailingServedPersistence;

impl ServedPersistence for FailingServedPersistence {
    fn persist(
        &self,
        _viewer_id: home_mixer::models::UserId,
        _served_post_ids: &[home_mixer::models::PostId],
        _request_time_ms: i64,
    ) -> Result<(), String> {
        Err("durable served store unavailable".to_string())
    }
}

#[tokio::test]
async fn scored_posts_rpc_persist_failure_returns_unavailable() {
    let server =
        ScoredPostsServer::new(unsigned_cached_posts_query_builder(), demo_pipeline().await)
            .with_served_persist(Arc::new(FailingServedPersistence));

    let error = ScoredPostsService::get_scored_posts(
        &server,
        Request::new(pb::ScoredPostsQuery {
            viewer_id: uid(7).to_string(),
            cached_posts: vec![proto_cached_post(pid(9))],
            ..Default::default()
        }),
    )
    .await
    .expect_err("served persist failure must not 2xx");

    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(
        error.message().contains("served persist failed"),
        "{}",
        error.message()
    );
}

struct StaticPostsProvider;

#[tonic::async_trait]
impl ScoredPostsProvider for StaticPostsProvider {
    async fn score_posts(
        &self,
        query: home_mixer::models::query::ScoredPostsQuery,
    ) -> Result<ScoredPostsOutput, String> {
        Ok(ScoredPostsOutput {
            posts: vec![ScoredPost {
                tweet_id: pid(9).to_string(),
                score: 1.0,
                ..Default::default()
            }],
            selected_ids: vec![pid(9)],
            request_id: query.request_id,
        })
    }
}

struct FailingFeedStateStore;

impl FeedStateStore for FailingFeedStateStore {
    fn load(&self, _user_id: home_mixer::models::UserId) -> Result<FeedStateSnapshot, String> {
        Ok(FeedStateSnapshot::default())
    }

    fn record(
        &self,
        _user_id: home_mixer::models::UserId,
        _served_post_ids: Vec<home_mixer::models::PostId>,
        _request_timestamp_ms: i64,
    ) -> Result<(), String> {
        Err("durable served store unavailable".to_string())
    }
}

#[tokio::test]
async fn for_you_served_persist_failure_returns_unavailable() {
    let server = ForYouFeedServer::with_local_state(
        Arc::new(StaticPostsProvider),
        BlenderConfig::default(),
        Vec::new(),
        Arc::new(FailingFeedStateStore),
        Arc::new(InMemoryFeedStats::default()),
    );

    let error = ForYouFeedService::get_for_you_feed(
        &server,
        Request::new(pb::ScoredPostsQuery {
            viewer_id: uid(7).to_string(),
            ..Default::default()
        }),
    )
    .await
    .expect_err("served persist failure must not 2xx");

    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(
        error.message().contains("served persist failed"),
        "{}",
        error.message()
    );
}

fn cached_candidate(tweet_id: home_mixer::models::PostId) -> PostCandidate {
    PostCandidate {
        tweet_id,
        author_id: uid(8),
        tweet_text: "cached".to_string(),
        score: Some(1.0),
        created_at_ms: Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_millis() as u64,
        ),
        in_network: Some(true),
        ..Default::default()
    }
}

async fn demo_pipeline() -> PhoenixCandidatePipeline {
    PhoenixCandidatePipeline::assemble_for_mode(
        home_mixer::runtime_config::HomeMixerMode::Demo,
        HomeMixerFeatures::default(),
    )
    .await
    .expect("demo assembly")
}

fn unsigned_cached_posts_query_builder() -> QueryBuilder {
    QueryBuilder::new(HomeMixerFeatures {
        unsigned_cached_posts: true,
        ..Default::default()
    })
}

#[tokio::test]
async fn score_does_not_persist_served_ids() {
    let store: Arc<dyn FeedStateStore> = Arc::new(InMemoryFeedStateStore::new(10, 10));
    let server = ScoredPostsServer::with_state(
        QueryBuilder::default(),
        demo_pipeline().await,
        Arc::clone(&store),
    );

    let output = server
        .score(home_mixer::models::query::ScoredPostsQuery {
            user_id: uid(7),
            has_cached_posts: true,
            cached_posts: vec![cached_candidate(pid(9))],
            request_time_ms: x_algorithm_proto::demo::now_ms(),
            ..Default::default()
        })
        .await;

    assert_eq!(output.posts.len(), 1);
    assert_eq!(
        output.selected_ids,
        vec![pid(9)],
        "attribution ids come straight from the selected candidates, no proto re-parse"
    );
    assert!(
        store
            .load(uid(7))
            .expect("feed state")
            .served_post_ids
            .is_empty(),
        "score() is a nested source path and must not persist exposures"
    );
}

fn recent_post_id(seq: u64) -> home_mixer::models::PostId {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as u32;
    home_mixer::models::ObjectId::from_parts(ts, seq)
}

fn proto_cached_post(tweet_id: home_mixer::models::PostId) -> pb::CachedPost {
    pb::CachedPost {
        tweet_id: tweet_id.to_string(),
        author_id: uid(8).to_string(),
        tweet_text: "cached".to_string(),
        in_network: true,
        score: 1.0,
        ..Default::default()
    }
}

#[tokio::test]
async fn scored_posts_rpc_persists_before_2xx() {
    let store: Arc<dyn FeedStateStore> = Arc::new(InMemoryFeedStateStore::new(10, 10));
    let tweet_id = recent_post_id(9);
    let server = ScoredPostsServer::with_state(
        unsigned_cached_posts_query_builder(),
        demo_pipeline().await,
        Arc::clone(&store),
    );

    ScoredPostsService::get_scored_posts(
        &server,
        Request::new(pb::ScoredPostsQuery {
            viewer_id: uid(7).to_string(),
            cached_posts: vec![proto_cached_post(tweet_id)],
            ..Default::default()
        }),
    )
    .await
    .expect("served persist success is 2xx");

    assert_eq!(
        store.load(uid(7)).expect("feed state").served_post_ids,
        vec![tweet_id]
    );
}

#[tokio::test]
async fn for_you_persist_is_visible_to_the_next_scored_posts_request() {
    let store: Arc<dyn FeedStateStore> = Arc::new(InMemoryFeedStateStore::new(10, 10));
    let tweet_id = recent_post_id(9);
    let query_builder = unsigned_cached_posts_query_builder();
    let scored = Arc::new(ScoredPostsServer::with_state(
        query_builder.clone(),
        demo_pipeline().await,
        Arc::clone(&store),
    ));
    let for_you = ForYouFeedServer::new(query_builder, Arc::clone(&scored));

    ForYouFeedService::get_for_you_feed(
        &for_you,
        Request::new(pb::ScoredPostsQuery {
            viewer_id: uid(7).to_string(),
            cached_posts: vec![proto_cached_post(tweet_id)],
            ..Default::default()
        }),
    )
    .await
    .expect("for you persist success is 2xx");

    assert_eq!(
        store.load(uid(7)).expect("feed state").served_post_ids,
        vec![tweet_id],
        "ForYou must persist into the same store ScoredPosts hydrators read"
    );

    let output = scored
        .score(home_mixer::models::query::ScoredPostsQuery {
            user_id: uid(7),
            is_bottom_request: true,
            has_cached_posts: true,
            cached_posts: vec![
                cached_candidate(tweet_id),
                cached_candidate(recent_post_id(10)),
            ],
            request_time_ms: x_algorithm_proto::demo::now_ms(),
            ..Default::default()
        })
        .await;
    let remaining: Vec<_> = output
        .posts
        .iter()
        .map(|post| post.tweet_id.clone())
        .collect();
    assert_eq!(remaining.len(), 1);
    assert_ne!(remaining[0], tweet_id.to_string());
}
