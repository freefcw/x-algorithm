//! SE-11 end to end: an assembled `ServedCandidatesSink` receives one event per
//! request whose records mirror the final served list, and a failing sink never
//! changes the response.

use home_mixer::models::candidate::PostCandidate;
use home_mixer::models::query::ScoredPostsQuery;
use home_mixer::models::{pid, uid};
use home_mixer::side_effects::served_candidates_kafka_side_effect::{
    ServedCandidatesEvent, ServedCandidatesSink, SERVED_CANDIDATES_EVENT_SCHEMA_VERSION,
};
use home_mixer::{HomeMixerFeatures, PhoenixCandidatePipeline, PhoenixDependencies};
use std::sync::{Arc, Mutex};
use tonic::async_trait;
use xai_candidate_pipeline::candidate_pipeline::CandidatePipeline;

struct UnusedPhoenix;

#[async_trait]
impl home_mixer::clients::phoenix_prediction_client::PhoenixPredictionClient for UnusedPhoenix {
    async fn predict(
        &self,
        _user_id: home_mixer::models::ids::UserId,
        _sequence: x_algorithm_proto::recsys::UserActionSequence,
        _candidates: Vec<x_algorithm_proto::recsys::TweetInfo>,
    ) -> Result<x_algorithm_proto::recsys::PredictNextActionsResponse, anyhow::Error> {
        anyhow::bail!("the fixture has no behavior sequence, so Phoenix is never called")
    }
}

#[async_trait]
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

struct NotifyingSink {
    observed: tokio::sync::Notify,
    events: Mutex<Vec<ServedCandidatesEvent>>,
    fail: bool,
}

#[async_trait]
impl ServedCandidatesSink for NotifyingSink {
    async fn publish(&self, event: &ServedCandidatesEvent) -> Result<(), String> {
        self.events.lock().unwrap().push(event.clone());
        self.observed.notify_one();
        if self.fail {
            Err("test sink unavailable".into())
        } else {
            Ok(())
        }
    }
}

fn dependencies(sink: Arc<dyn ServedCandidatesSink>) -> PhoenixDependencies {
    PhoenixDependencies {
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
        served_candidates_sink: Some(sink),
        features: HomeMixerFeatures::default(),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn cached_query(now_ms: u64, request_id: &str) -> ScoredPostsQuery {
    ScoredPostsQuery {
        user_id: uid(900),
        in_network_only: true,
        request_id: request_id.into(),
        prediction_id: 4242,
        request_time_ms: now_ms as i64,
        client_app_id: 9,
        has_cached_posts: true,
        cached_posts: vec![
            PostCandidate {
                tweet_id: pid(42),
                author_id: uid(43),
                tweet_text: "cached fixture".into(),
                created_at_ms: Some(now_ms.saturating_sub(1_000)),
                in_network: Some(true),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: pid(44),
                author_id: uid(45),
                tweet_text: "second cached fixture".into(),
                created_at_ms: Some(now_ms.saturating_sub(2_000)),
                in_network: Some(true),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

#[tokio::test]
async fn served_event_mirrors_the_final_response_and_sink_failure_is_isolated() {
    let now_ms = now_ms();
    let mut responses = Vec::new();
    for fail in [false, true] {
        let sink = Arc::new(NotifyingSink {
            observed: tokio::sync::Notify::new(),
            events: Mutex::default(),
            fail,
        });
        let pipeline = PhoenixCandidatePipeline::build_with_clients(dependencies(
            Arc::clone(&sink) as Arc<dyn ServedCandidatesSink>,
        ))
        .await;
        assert_eq!(
            pipeline
                .side_effects()
                .iter()
                .filter(|effect| effect.name() == "ServedCandidatesKafkaSideEffect")
                .count(),
            1
        );

        let result = pipeline.execute(cached_query(now_ms, "served-event")).await;
        assert_eq!(result.selected_candidates.len(), 2);

        tokio::time::timeout(std::time::Duration::from_secs(1), sink.observed.notified())
            .await
            .expect("pipeline dispatched the served-candidates sink");
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1, "one event per request");
        let event = &events[0];
        assert_eq!(event.schema_version, SERVED_CANDIDATES_EVENT_SCHEMA_VERSION);
        assert_eq!(event.request_id, "served-event");
        assert_eq!(event.prediction_request_id, 4242);
        assert_eq!(event.viewer_id, uid(900).to_string());
        assert_eq!(event.request_time_ms, now_ms as i64);
        assert!(event.in_network_only);
        assert_eq!(event.client_app_id, 9);

        // Records follow the served order and carry the same identity and
        // scores the response exposes.
        let served: Vec<_> = result
            .selected_candidates
            .iter()
            .map(|candidate| (candidate.tweet_id.to_string(), candidate.score))
            .collect();
        let logged: Vec<_> = event
            .candidates
            .iter()
            .map(|record| (record.post_id.clone(), record.score))
            .collect();
        assert_eq!(logged, served);
        assert_eq!(
            event
                .candidates
                .iter()
                .map(|record| record.position)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(event
            .candidates
            .iter()
            .all(|record| record.in_network == Some(true) && record.degraded_reason.is_some()));

        responses.push(served);
    }
    assert_eq!(
        responses[0], responses[1],
        "a failing sink never alters the feed"
    );
}
