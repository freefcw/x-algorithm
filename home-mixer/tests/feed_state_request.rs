use home_mixer::feed_state::{FeedStateSnapshot, FeedStateStore};
use home_mixer::for_you_server::ForYouFeedServer;
use home_mixer::models::candidate::PostCandidate;
use home_mixer::models::query::ScoredPostsQuery;
use home_mixer::models::{pid, uid, PostId, UserId};
use home_mixer::query_builder::QueryBuilder;
use home_mixer::query_hydrators::past_request_timestamps_query_hydrator::PastRequestTimestampsQueryHydrator;
use home_mixer::query_hydrators::served_history_query_hydrator::ServedHistoryQueryHydrator;
use home_mixer::runtime_config::HomeMixerMode;
use home_mixer::scored_posts_server::ScoredPostsServer;
use home_mixer::{HomeMixerFeatures, PhoenixCandidatePipeline};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

struct CountingStore {
    loads: AtomicUsize,
    snapshot: Mutex<Result<FeedStateSnapshot, String>>,
}

impl CountingStore {
    fn new(snapshot: Result<FeedStateSnapshot, String>) -> Self {
        Self {
            loads: AtomicUsize::new(0),
            snapshot: Mutex::new(snapshot),
        }
    }

    fn load_count(&self) -> usize {
        self.loads.load(Ordering::SeqCst)
    }

    fn replace(&self, snapshot: Result<FeedStateSnapshot, String>) {
        *self.snapshot.lock().expect("snapshot lock") = snapshot;
    }
}

#[async_trait]
impl FeedStateStore for CountingStore {
    async fn load(&self, _user_id: UserId) -> Result<FeedStateSnapshot, String> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.snapshot.lock().expect("snapshot lock").clone()
    }

    async fn record(
        &self,
        _user_id: UserId,
        _served_post_ids: Vec<PostId>,
        _request_timestamp_ms: i64,
    ) -> Result<(), String> {
        Ok(())
    }
}

async fn demo_pipeline() -> PhoenixCandidatePipeline {
    PhoenixCandidatePipeline::assemble_for_mode(HomeMixerMode::Demo, HomeMixerFeatures::default())
        .await
        .expect("demo pipeline")
}

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

fn query(first: PostId, second: PostId) -> ScoredPostsQuery {
    ScoredPostsQuery {
        user_id: uid(7),
        is_bottom_request: true,
        has_cached_posts: true,
        cached_posts: vec![cached_candidate(first), cached_candidate(second)],
        request_id: "feed-state-request".to_string(),
        request_time_ms: x_algorithm_proto::demo::now_ms(),
        ..Default::default()
    }
}

fn selected_ids(output: &home_mixer::scored_posts_server::ScoredPostsOutput) -> Vec<PostId> {
    output.selected_ids.clone()
}

#[tokio::test]
async fn for_you_and_nested_scored_posts_load_one_snapshot() {
    let first = recent_post_id(1);
    let second = recent_post_id(2);
    let store = Arc::new(CountingStore::new(Ok(FeedStateSnapshot {
        served_post_ids: vec![first],
        request_timestamps_ms: vec![111],
    })));
    let state_store: Arc<dyn FeedStateStore> = store.clone();
    let scored = Arc::new(ScoredPostsServer::with_state(
        QueryBuilder::default(),
        demo_pipeline().await,
        state_store,
    ));
    let for_you = ForYouFeedServer::new(QueryBuilder::default(), scored);

    let output = for_you.get_for_you_feed(query(first, second)).await;

    assert_eq!(store.load_count(), 1);
    assert_eq!(
        output
            .items
            .iter()
            .filter_map(|item| item.served_post_id())
            .collect::<Vec<_>>(),
        vec![second]
    );
}

#[tokio::test]
async fn cloned_query_starts_a_fresh_snapshot_for_each_scored_posts_request() {
    let first = recent_post_id(11);
    let second = recent_post_id(12);
    let store = Arc::new(CountingStore::new(Ok(FeedStateSnapshot {
        served_post_ids: vec![first],
        request_timestamps_ms: vec![111],
    })));
    let state_store: Arc<dyn FeedStateStore> = store.clone();
    let server =
        ScoredPostsServer::with_state(QueryBuilder::default(), demo_pipeline().await, state_store);
    let request = query(first, second);

    let first_output = server.score(request.clone()).await;
    store.replace(Ok(FeedStateSnapshot {
        served_post_ids: vec![second],
        request_timestamps_ms: vec![222],
    }));
    let second_output = server.score(request).await;

    assert_eq!(store.load_count(), 2);
    assert_eq!(selected_ids(&first_output), vec![second]);
    assert_eq!(selected_ids(&second_output), vec![first]);
}

#[tokio::test]
async fn cloned_query_starts_a_fresh_snapshot_for_each_for_you_request() {
    let first = recent_post_id(13);
    let second = recent_post_id(14);
    let store = Arc::new(CountingStore::new(Ok(FeedStateSnapshot {
        served_post_ids: vec![first],
        request_timestamps_ms: vec![111],
    })));
    let state_store: Arc<dyn FeedStateStore> = store.clone();
    let scored = Arc::new(ScoredPostsServer::with_state(
        QueryBuilder::default(),
        demo_pipeline().await,
        state_store,
    ));
    let server = ForYouFeedServer::new(QueryBuilder::default(), scored);
    let request = query(first, second);

    let first_output = server.get_for_you_feed(request.clone()).await;
    store.replace(Ok(FeedStateSnapshot {
        served_post_ids: vec![second],
        request_timestamps_ms: vec![222],
    }));
    let second_output = server.get_for_you_feed(request).await;

    assert_eq!(store.load_count(), 2);
    assert_eq!(
        first_output
            .items
            .iter()
            .filter_map(|item| item.served_post_id())
            .collect::<Vec<_>>(),
        vec![second]
    );
    assert_eq!(
        second_output
            .items
            .iter()
            .filter_map(|item| item.served_post_id())
            .collect::<Vec<_>>(),
        vec![first]
    );
}

#[tokio::test]
async fn empty_and_failed_snapshots_are_each_loaded_once_across_for_you_and_scored_posts() {
    for snapshot in [
        Ok(FeedStateSnapshot::default()),
        Err("unavailable".to_string()),
    ] {
        let first = recent_post_id(21);
        let second = recent_post_id(22);
        let store = Arc::new(CountingStore::new(snapshot));
        let state_store: Arc<dyn FeedStateStore> = store.clone();
        let scored = Arc::new(ScoredPostsServer::with_state(
            QueryBuilder::default(),
            demo_pipeline().await,
            state_store,
        ));
        let server = ForYouFeedServer::new(QueryBuilder::default(), scored);

        let output = server.get_for_you_feed(query(first, second)).await;

        assert_eq!(store.load_count(), 1);
        assert_eq!(
            output
                .items
                .iter()
                .filter_map(|item| item.served_post_id())
                .collect::<Vec<_>>(),
            vec![first, second]
        );
    }
}

#[tokio::test]
async fn served_ids_and_timestamps_come_from_the_same_snapshot() {
    let store = Arc::new(CountingStore::new(Ok(FeedStateSnapshot {
        served_post_ids: vec![pid(31)],
        request_timestamps_ms: vec![310],
    })));
    let state_store: Arc<dyn FeedStateStore> = store.clone();
    let served = ServedHistoryQueryHydrator::from_store(Arc::clone(&state_store));
    let timestamps = PastRequestTimestampsQueryHydrator::from_store(state_store);
    let query = ScoredPostsQuery {
        user_id: uid(7),
        served_ids: vec![pid(30)],
        past_request_timestamps_ms: vec![300],
        ..Default::default()
    };

    let served_result = served.hydrate(&query).await.expect("served history");
    store.replace(Ok(FeedStateSnapshot {
        served_post_ids: vec![pid(41)],
        request_timestamps_ms: vec![410],
    }));
    let timestamp_result = timestamps.hydrate(&query).await.expect("timestamps");

    assert_eq!(store.load_count(), 1);
    assert_eq!(served_result.served_ids, vec![pid(30), pid(31)]);
    assert_eq!(timestamp_result.past_request_timestamps_ms, vec![300, 310]);
}
