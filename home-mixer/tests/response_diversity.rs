use home_mixer::models::candidate::PostCandidate;
use home_mixer::models::ids::ObjectId;
use home_mixer::models::query::ScoredPostsQuery;
use home_mixer::models::{pid, uid};
use home_mixer::runtime_config::HomeMixerMode;
use home_mixer::side_effects::response_diversity_stats_side_effect::{
    CandidateDiversityStats, CandidateDiversityStatsSink, ResponseDiversityStatsSideEffect,
    SamplingDecision,
};
use home_mixer::util::composition::Composition;
use home_mixer::{HomeMixerFeatures, PhoenixCandidatePipeline};
use std::sync::{Arc, Mutex};
use xai_candidate_pipeline::candidate_pipeline::CandidatePipeline;
use xai_candidate_pipeline::side_effect::{SideEffect, SideEffectInput};

#[test]
fn composition_handles_empty_single_uniform_and_skewed_counts() {
    assert_eq!(Composition::from_counts([]), Composition::default());

    let single = Composition::from_counts([1]);
    assert_eq!(single.size, 1);
    assert_eq!(single.unique, 1);
    assert_eq!(single.max_count, 1);
    assert_eq!(single.hhi, 1.0);
    assert_eq!(single.entropy_norm, 1.0);

    let uniform = Composition::from_counts([2, 2]);
    assert_eq!(uniform.size, 4);
    assert_eq!(uniform.unique, 2);
    assert_eq!(uniform.max_count, 2);
    assert!((uniform.hhi - 0.5).abs() < 1e-9);
    assert!((uniform.entropy_norm - 0.5).abs() < 1e-9);

    let skewed = Composition::from_counts([3, 1]);
    assert_eq!(skewed.max_count, 3);
    assert!((skewed.max_share() - 0.75).abs() < 1e-9);
    assert!(skewed.hhi > uniform.hhi);
    assert!(skewed.entropy_norm < 1.0);
}

#[derive(Default)]
struct AlwaysSample;

impl SamplingDecision for AlwaysSample {
    fn should_sample(&self) -> bool {
        true
    }
}

#[derive(Default)]
struct RecordingSink {
    records: Mutex<Vec<CandidateDiversityStats>>,
}

impl CandidateDiversityStatsSink for RecordingSink {
    fn record(&self, stats: CandidateDiversityStats) -> Result<(), String> {
        self.records.lock().unwrap().push(stats);
        Ok(())
    }
}

#[derive(Clone)]
struct FailingSink;

impl CandidateDiversityStatsSink for FailingSink {
    fn record(&self, _stats: CandidateDiversityStats) -> Result<(), String> {
        Err("sink unavailable".to_string())
    }
}

fn candidate(id: ObjectId, author: ObjectId, source: i32, score: Option<f64>) -> PostCandidate {
    PostCandidate {
        tweet_id: id,
        author_id: author,
        served_type: x_algorithm_proto::home_mixer::ServedType::try_from(source).ok(),
        in_network: Some(source == 1),
        score,
        weighted_score: None,
        ..Default::default()
    }
}

fn input(candidates: Vec<PostCandidate>) -> Arc<SideEffectInput<ScoredPostsQuery, PostCandidate>> {
    Arc::new(SideEffectInput {
        query: Arc::new(ScoredPostsQuery {
            request_id: "diversity-request".to_string(),
            ..Default::default()
        }),
        selected_candidates: candidates,
        non_selected_candidates: Vec::new(),
    })
}

#[tokio::test]
async fn records_stable_final_and_top10_composition_without_mutating_input() {
    let sink = Arc::new(RecordingSink::default());
    let effect =
        ResponseDiversityStatsSideEffect::with_sampler(sink.clone(), Arc::new(AlwaysSample));
    let real_a = ObjectId::parse("e305c05a62cd1ef55823cd86").unwrap();
    let real_b = ObjectId::parse("a105c05a62cd1ef55823cd86").unwrap();
    let candidates = vec![
        candidate(pid(1), real_a, 1, Some(1.0)),
        candidate(pid(2), real_b, 2, Some(3.0)),
        candidate(pid(3), real_a, 1, Some(2.0)),
    ];
    let input = input(candidates);
    let original_ids: Vec<_> = input
        .selected_candidates
        .iter()
        .map(|c| c.tweet_id)
        .collect();
    effect.side_effect(Arc::clone(&input)).await.unwrap();

    let records = sink.records.lock().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].stage, "final");
    assert_eq!(records[0].size, 3);
    assert_eq!(records[0].authors.unique, 2);
    assert_eq!(records[0].sources.unique, 2);
    assert!((records[0].in_network_share - 2.0 / 3.0).abs() < 1e-9);
    assert_eq!(records[1].stage, "top10");
    assert_eq!(records[1].size, 3);
    assert_eq!(
        input
            .selected_candidates
            .iter()
            .map(|c| c.tweet_id)
            .collect::<Vec<_>>(),
        original_ids
    );
}

#[tokio::test]
async fn top10_uses_stable_score_order_and_ignores_weighted_score() {
    let sink = Arc::new(RecordingSink::default());
    let effect =
        ResponseDiversityStatsSideEffect::with_sampler(sink.clone(), Arc::new(AlwaysSample));
    let mut candidates: Vec<_> = (0..12)
        .map(|index| {
            let mut candidate = candidate(
                pid(index + 1),
                uid(index + 1),
                if index < 6 { 1 } else { 2 },
                Some(if index < 2 { 100.0 } else { index as f64 }),
            );
            candidate.weighted_score = None;
            candidate
        })
        .collect();
    candidates[0].author_id = uid(900);
    candidates[1].author_id = uid(900);

    effect.side_effect(input(candidates)).await.unwrap();

    let records = sink.records.lock().unwrap();
    assert_eq!(records[0].stage, "final");
    assert_eq!(records[0].size, 12);
    assert_eq!(records[1].stage, "top10");
    assert_eq!(records[1].size, 10);
    assert_eq!(records[1].authors.max_count, 2);
    assert_eq!(records[1].authors.unique, 9);
    assert!((records[1].in_network_share - 0.4).abs() < 1e-9);
}

#[tokio::test]
async fn top10_keeps_input_order_at_ties_and_places_invalid_scores_last() {
    let sink = Arc::new(RecordingSink::default());
    let effect =
        ResponseDiversityStatsSideEffect::with_sampler(sink.clone(), Arc::new(AlwaysSample));
    // The first invalid score must not displace a scored candidate. The two
    // tied authors straddle rank 10, so stability changes observable diversity.
    let mut candidates = vec![candidate(pid(90), uid(90), 2, Some(f64::NAN))];
    candidates.extend((1..=9).map(|id| candidate(pid(id), uid(1), 1, Some(2.0))));
    candidates.push(candidate(pid(10), uid(1), 1, Some(1.0)));
    candidates.push(candidate(pid(11), uid(2), 2, Some(1.0)));
    candidates.push(candidate(pid(12), uid(3), 2, None));
    candidates.push(candidate(pid(13), uid(4), 2, Some(f64::INFINITY)));
    effect.side_effect(input(candidates)).await.unwrap();
    let records = sink.records.lock().unwrap();
    assert_eq!(records[0].size, 14);
    assert_eq!(records[1].authors.unique, 1);
    assert_eq!(records[1].in_network_share, 1.0);
}

#[tokio::test]
async fn empty_or_unsampled_slates_are_not_recorded_and_sink_errors_propagate() {
    let sink = Arc::new(RecordingSink::default());
    let effect =
        ResponseDiversityStatsSideEffect::with_sampler(sink.clone(), Arc::new(AlwaysSample));
    effect.side_effect(input(Vec::new())).await.unwrap();
    assert!(sink.records.lock().unwrap().is_empty());

    struct NeverSample;
    impl SamplingDecision for NeverSample {
        fn should_sample(&self) -> bool {
            false
        }
    }
    let unsampled =
        ResponseDiversityStatsSideEffect::with_sampler(sink.clone(), Arc::new(NeverSample));
    unsampled
        .side_effect(input(vec![candidate(pid(1), uid(1), 1, Some(1.0))]))
        .await
        .unwrap();
    assert!(sink.records.lock().unwrap().is_empty());

    let failing = ResponseDiversityStatsSideEffect::with_sampler(
        Arc::new(FailingSink),
        Arc::new(AlwaysSample),
    );
    let error = failing
        .side_effect(input(vec![candidate(pid(1), uid(1), 1, Some(1.0))]))
        .await
        .expect_err("sink failure should be returned to pipeline isolation");
    assert!(error.contains("sink unavailable"));
}

#[tokio::test]
async fn source_composition_keeps_unknown_sources_as_an_explicit_bucket() {
    let sink = Arc::new(RecordingSink::default());
    let effect =
        ResponseDiversityStatsSideEffect::with_sampler(sink.clone(), Arc::new(AlwaysSample));
    let mut unknown = candidate(pid(1), uid(1), 1, Some(2.0));
    unknown.served_type = None;
    effect.side_effect(input(vec![unknown])).await.unwrap();

    let records = sink.records.lock().unwrap();
    assert_eq!(records[0].sources.size, 1);
    assert_eq!(records[0].sources.unique, 1);
    assert_eq!(records[0].sources.max_count, 1);
}

#[tokio::test]
async fn pipeline_assembles_response_diversity_side_effect() {
    let pipeline = PhoenixCandidatePipeline::assemble_for_mode(
        HomeMixerMode::Demo,
        HomeMixerFeatures::default(),
    )
    .await
    .unwrap();
    let components = pipeline.components();
    let side_effects = components
        .iter()
        .find(|entry| {
            entry.stage == xai_candidate_pipeline::candidate_pipeline::PipelineStage::SideEffect
        })
        .unwrap();
    assert!(side_effects
        .components
        .iter()
        .any(|name| name == "ResponseDiversityStatsSideEffect"));
}

struct UnusedPhoenix;

#[tonic::async_trait]
impl home_mixer::clients::phoenix_prediction_client::PhoenixPredictionClient for UnusedPhoenix {
    async fn predict(
        &self,
        _user_id: home_mixer::models::ids::UserId,
        _sequence: x_algorithm_proto::recsys::UserActionSequence,
        _candidates: Vec<x_algorithm_proto::recsys::TweetInfo>,
    ) -> Result<x_algorithm_proto::recsys::PredictNextActionsResponse, anyhow::Error> {
        panic!("the fixture has no history and must use rule fallback")
    }
}

#[tonic::async_trait]
impl home_mixer::clients::phoenix_retrieval_client::PhoenixRetrievalClient for UnusedPhoenix {
    async fn retrieve(
        &self,
        _user_id: home_mixer::models::ids::UserId,
        _sequence: x_algorithm_proto::recsys::UserActionSequence,
        _max_results: u32,
    ) -> Result<x_algorithm_proto::recsys::RetrieveResponse, anyhow::Error> {
        panic!("the fixture requests in-network content only")
    }
}

fn local_dependencies() -> home_mixer::PhoenixDependencies {
    home_mixer::PhoenixDependencies {
        uas_fetcher: Arc::new(home_mixer::clients::uas_fetcher::DisabledUserActionSequenceFetcher),
        phoenix_client: Arc::new(UnusedPhoenix),
        phoenix_retrieval_client: Arc::new(UnusedPhoenix),
        in_network_client: None,
        strato_client: Arc::new(home_mixer::clients::strato_client::DemoStratoClient),
        tes_client: Arc::new(home_mixer::clients::tweet_entity_service_client::DemoTESClient),
        gizmoduck_client: Arc::new(home_mixer::clients::gizmoduck_client::DemoGizmoduckClient),
        vf_client: Arc::new(home_mixer::visibility::vf_client::DemoVisibilityFilteringClient),
        topic_clients: None,
        moe_retrieval_client: None,
        fallback_client: None,
        features: HomeMixerFeatures::default(),
    }
}

#[tokio::test]
async fn injected_sink_failure_does_not_change_the_pipeline_response() {
    struct NotifyingSink {
        observed: tokio::sync::Notify,
        records: Mutex<Vec<CandidateDiversityStats>>,
        fail: bool,
    }
    impl CandidateDiversityStatsSink for NotifyingSink {
        fn record(&self, stats: CandidateDiversityStats) -> Result<(), String> {
            self.records.lock().unwrap().push(stats);
            self.observed.notify_one();
            if self.fail {
                Err("test sink unavailable".into())
            } else {
                Ok(())
            }
        }
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut responses = Vec::new();
    for fail in [false, true] {
        let sink = Arc::new(NotifyingSink {
            observed: tokio::sync::Notify::new(),
            records: Mutex::default(),
            fail,
        });
        let pipeline = PhoenixCandidatePipeline::build_with_clients_and_diversity_stats(
            local_dependencies(),
            ResponseDiversityStatsSideEffect::with_sampler(sink.clone(), Arc::new(AlwaysSample)),
        )
        .await;
        assert_eq!(
            pipeline
                .side_effects()
                .iter()
                .filter(|effect| { effect.name() == "ResponseDiversityStatsSideEffect" })
                .count(),
            1
        );

        let result = pipeline
            .execute(ScoredPostsQuery {
                user_id: uid(900),
                in_network_only: true,
                request_id: "sink-isolation".into(),
                request_time_ms: now_ms as i64,
                has_cached_posts: true,
                cached_posts: vec![PostCandidate {
                    tweet_id: pid(42),
                    author_id: uid(43),
                    tweet_text: "cached fixture".into(),
                    created_at_ms: Some(now_ms.saturating_sub(1_000)),
                    in_network: Some(true),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await;
        assert_eq!(result.selected_candidates.len(), 1);
        assert_eq!(result.selected_candidates[0].weighted_score, None);
        assert!(result.selected_candidates[0].degraded_reason.is_some());
        tokio::time::timeout(std::time::Duration::from_secs(1), sink.observed.notified())
            .await
            .expect("pipeline dispatched the injected sink");
        let records = sink.records.lock().unwrap();
        assert_eq!(records.len(), if fail { 1 } else { 2 });
        assert!(records
            .iter()
            .all(|record| record.request_id == "sink-isolation"));
        responses.push(
            result
                .selected_candidates
                .iter()
                .map(|candidate| (candidate.tweet_id, candidate.score))
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(responses[0], responses[1]);
}
