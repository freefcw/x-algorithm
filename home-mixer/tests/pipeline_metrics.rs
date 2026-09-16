//! The process metrics registry observes both candidate pipelines: one request
//! through the For You entry point must produce stage series for the outer
//! `ForYouCandidatePipeline` and the nested `PhoenixCandidatePipeline`, plus a
//! settled-side-effect series once the asynchronous side effects finish.

use home_mixer::feed_state::InMemoryFeedStateStore;
use home_mixer::for_you_server::ForYouFeedServer;
use home_mixer::metrics::Metrics;
use home_mixer::models::candidate::PostCandidate;
use home_mixer::models::query::ScoredPostsQuery;
use home_mixer::models::{uid, PostId};
use home_mixer::query_builder::QueryBuilder;
use home_mixer::rpc_policy::RpcPolicy;
use home_mixer::runtime_config::HomeMixerMode;
use home_mixer::scored_posts_server::ScoredPostsServer;
use home_mixer::{HomeMixerFeatures, PhoenixCandidatePipeline};
use std::sync::Arc;
use std::time::Duration;
use xai_candidate_pipeline::observer::PipelineObserver;

fn recent_post_id(sequence: u64) -> PostId {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as u32;
    home_mixer::models::ObjectId::from_parts(timestamp, sequence)
}

fn cached_candidate(tweet_id: PostId) -> PostCandidate {
    PostCandidate {
        tweet_id,
        author_id: uid(8),
        tweet_text: "cached".to_string(),
        score: Some(1.0),
        created_at_ms: Some(x_algorithm_proto::demo::now_ms() as u64),
        in_network: Some(true),
        ..Default::default()
    }
}

fn query() -> ScoredPostsQuery {
    ScoredPostsQuery {
        user_id: uid(7),
        is_bottom_request: true,
        has_cached_posts: true,
        cached_posts: vec![
            cached_candidate(recent_post_id(1)),
            cached_candidate(recent_post_id(2)),
        ],
        request_id: "pipeline-metrics-request".to_string(),
        request_time_ms: x_algorithm_proto::demo::now_ms(),
        ..Default::default()
    }
}

/// Side effects settle on spawned tasks after the response; give the scheduler
/// a bounded number of turns to run them.
async fn wait_for(metrics: &Metrics, needle: &str) -> String {
    for _ in 0..200 {
        let text = metrics.encode().expect("text exposition");
        if text.contains(needle) {
            return text;
        }
        tokio::task::yield_now().await;
    }
    metrics.encode().expect("text exposition")
}

#[tokio::test]
async fn one_for_you_request_is_observed_by_both_pipelines_and_their_side_effects() {
    let metrics = Arc::new(Metrics::new());
    let pipeline = PhoenixCandidatePipeline::assemble_for_mode(
        HomeMixerMode::Demo,
        HomeMixerFeatures::default(),
    )
    .await
    .expect("demo pipeline")
    .with_observer(Arc::clone(&metrics) as Arc<dyn PipelineObserver>);
    let scored = Arc::new(
        ScoredPostsServer::with_state(
            QueryBuilder::default(),
            pipeline,
            Arc::new(InMemoryFeedStateStore::new(10, 2)),
        )
        .with_rpc_policy(RpcPolicy::new(Duration::from_secs(5), Arc::clone(&metrics))),
    );
    let for_you = ForYouFeedServer::new(QueryBuilder::default(), scored);

    let output = for_you.get_for_you_feed(query()).await;
    assert!(output.persist_error.is_none());

    let text = metrics.encode().expect("text exposition");
    for expected in [
        "home_mixer_pipeline_duration_seconds_count{pipeline=\"PhoenixCandidatePipeline\"} 1",
        "home_mixer_pipeline_duration_seconds_count{pipeline=\"ForYouCandidatePipeline\"} 1",
        "home_mixer_pipeline_result_size_count{pipeline=\"PhoenixCandidatePipeline\"} 1",
        // Two cached posts came in through CachedPostsSource.
        "home_mixer_source_candidates_total{pipeline=\"PhoenixCandidatePipeline\",source=\"CachedPostsSource\"} 2",
        "home_mixer_stage_duration_seconds_count{pipeline=\"PhoenixCandidatePipeline\",stage=\"sources\"} 1",
        "home_mixer_stage_duration_seconds_count{pipeline=\"PhoenixCandidatePipeline\",stage=\"filters\"} 1",
        "home_mixer_stage_duration_seconds_count{pipeline=\"PhoenixCandidatePipeline\",stage=\"scorers\"} 1",
        "home_mixer_stage_duration_seconds_count{pipeline=\"ForYouCandidatePipeline\",stage=\"sources\"} 1",
    ] {
        assert!(text.contains(expected), "missing {expected}\n{text}");
    }
    // Two cached posts against a 35-post target: the run is underfilled.
    assert!(
        text.contains(
            "home_mixer_pipeline_underfilled_total{pipeline=\"PhoenixCandidatePipeline\"} 1"
        ),
        "{text}"
    );

    let text = wait_for(
        &metrics,
        "home_mixer_side_effect_runs_total{component=\"ResponseDiversityStatsSideEffect\",pipeline=\"PhoenixCandidatePipeline\",result=\"ok\"} 1",
    )
    .await;
    assert!(
        text.contains("home_mixer_side_effect_runs_total{component=\"ResponseDiversityStatsSideEffect\",pipeline=\"PhoenixCandidatePipeline\",result=\"ok\"} 1"),
        "{text}"
    );
    assert!(
        text.contains("home_mixer_side_effect_duration_seconds_count{component=\"ResponseDiversityStatsSideEffect\",pipeline=\"PhoenixCandidatePipeline\"} 1"),
        "{text}"
    );
}
