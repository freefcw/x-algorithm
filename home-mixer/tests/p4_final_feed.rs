use home_mixer::final_feed::{
    AdsBlenderStrategy, AdvertisementSource, BlenderConfig, BlenderSelector, FeedItem,
    FeedItemKind, FeedResponseStats, FeedStateStore, FeedStatsSink, ForYouFeedServer,
    InMemoryFeedStateStore, InMemoryFeedStats, ScoredPostsProvider, ScoredPostsQuery,
};
use home_mixer::scored_posts_server::ScoredPostsOutput;
use std::sync::{Arc, Mutex};
use tonic::async_trait;
use x_algorithm_proto::home_mixer::{feed_item, BrandSafetyVerdict, ScoredPost};
use xai_candidate_pipeline::source::Source;

fn post(id: u64, score: f32) -> FeedItem {
    FeedItem::post(ScoredPost {
        tweet_id: id,
        score,
        ..Default::default()
    })
}

fn ad_safe_post(id: u64, score: f32) -> FeedItem {
    FeedItem::post(ScoredPost {
        tweet_id: id,
        score,
        brand_safety_verdict: BrandSafetyVerdict::SafeForAdjacency as i32,
        ..Default::default()
    })
}

#[test]
fn natural_posts_keep_p3_order_and_receive_final_positions() {
    let result = BlenderSelector::new(BlenderConfig::default()).blend(vec![
        post(30, 0.9),
        post(20, 0.8),
        post(10, 0.7),
    ]);

    let post_ids = result
        .selected
        .iter()
        .filter_map(FeedItem::post_id)
        .collect::<Vec<_>>();
    let positions = result
        .selected
        .iter()
        .map(|item| item.position)
        .collect::<Vec<_>>();

    assert_eq!(post_ids, vec![30, 20, 10]);
    assert_eq!(positions, vec![0, 1, 2]);
    assert!(result.non_selected.is_empty());
}

#[test]
fn modules_are_inserted_without_rescoring_or_reordering_posts() {
    let selector = BlenderSelector::new(BlenderConfig {
        prompt_position: 1,
        who_to_follow_position: 3,
        max_items: 10,
        ..Default::default()
    });
    let result = selector.blend(vec![
        post(40, 0.9),
        post(30, 0.8),
        post(20, 0.7),
        post(10, 0.6),
        FeedItem::prompt("onboarding"),
        FeedItem::who_to_follow("people-you-may-know", vec![101, 102]),
        FeedItem::push_to_home(
            "notification-1",
            ScoredPost {
                tweet_id: 99,
                score: 0.1,
                ..Default::default()
            },
        ),
    ]);

    let kinds = result
        .selected
        .iter()
        .map(FeedItem::kind)
        .collect::<Vec<_>>();
    let post_ids = result
        .selected
        .iter()
        .filter_map(FeedItem::post_id)
        .collect::<Vec<_>>();

    assert_eq!(
        kinds,
        vec![
            FeedItemKind::PushToHome,
            FeedItemKind::Post,
            FeedItemKind::Prompt,
            FeedItemKind::Post,
            FeedItemKind::WhoToFollow,
            FeedItemKind::Post,
            FeedItemKind::Post,
        ]
    );
    assert_eq!(post_ids, vec![40, 30, 20, 10]);
    assert_eq!(
        result
            .selected
            .iter()
            .map(|item| item.position)
            .collect::<Vec<_>>(),
        (0..7).collect::<Vec<_>>()
    );
}

#[test]
fn only_one_push_to_home_item_is_selected() {
    let result = BlenderSelector::new(BlenderConfig::default()).blend(vec![
        post(1, 1.0),
        FeedItem::push_to_home("first", ScoredPost::default()),
        FeedItem::push_to_home("second", ScoredPost::default()),
    ]);

    assert_eq!(result.selected[0].kind(), FeedItemKind::PushToHome);
    assert_eq!(
        result
            .selected
            .iter()
            .filter(|item| item.kind() == FeedItemKind::PushToHome)
            .count(),
        1
    );
    assert_eq!(result.non_selected.len(), 1);
    assert_eq!(result.non_selected[0].kind(), FeedItemKind::PushToHome);
}

struct FakeScoredPostsProvider {
    seen_requests: Mutex<Vec<String>>,
}

#[async_trait]
impl ScoredPostsProvider for FakeScoredPostsProvider {
    async fn score_posts(&self, query: ScoredPostsQuery) -> Result<ScoredPostsOutput, String> {
        self.seen_requests
            .lock()
            .expect("request log")
            .push(query.request_id.clone());
        Ok(ScoredPostsOutput {
            posts: vec![
                ScoredPost {
                    tweet_id: 30,
                    score: 0.9,
                    ..Default::default()
                },
                ScoredPost {
                    tweet_id: 20,
                    score: 0.8,
                    ..Default::default()
                },
            ],
            request_id: query.request_id,
        })
    }
}

#[tokio::test]
async fn disabled_advertisement_source_never_emits_candidates() {
    let source = AdvertisementSource::disabled();

    let candidates = source
        .get_candidates(&ScoredPostsQuery::default())
        .await
        .expect("disabled source");

    assert!(candidates.is_empty());
}

#[test]
fn advertisements_are_rejected_while_blending_is_disabled() {
    let result = BlenderSelector::new(BlenderConfig::default()).blend(vec![
        ad_safe_post(1, 1.0),
        ad_safe_post(2, 0.9),
        ad_safe_post(3, 0.8),
        ad_safe_post(4, 0.7),
        ad_safe_post(5, 0.6),
        FeedItem::advertisement("ad-1", 2),
    ]);

    assert!(result
        .selected
        .iter()
        .all(|item| item.kind() != FeedItemKind::Advertisement));
    assert_eq!(result.non_selected.len(), 1);
}

#[test]
fn safe_gap_blender_places_ads_only_between_explicitly_safe_posts() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::SafeGap,
        min_posts_for_ads: 5,
        min_organic_gap: 2,
        ..Default::default()
    });
    let result = selector.blend(vec![
        ad_safe_post(5, 1.0),
        ad_safe_post(4, 0.9),
        ad_safe_post(3, 0.8),
        ad_safe_post(2, 0.7),
        ad_safe_post(1, 0.6),
        FeedItem::advertisement("ad-1", 2),
    ]);

    assert_eq!(
        result
            .selected
            .iter()
            .map(FeedItem::kind)
            .collect::<Vec<_>>(),
        vec![
            FeedItemKind::Post,
            FeedItemKind::Post,
            FeedItemKind::Advertisement,
            FeedItemKind::Post,
            FeedItemKind::Post,
            FeedItemKind::Post,
        ]
    );
    assert_eq!(
        result
            .selected
            .iter()
            .filter_map(FeedItem::post_id)
            .collect::<Vec<_>>(),
        vec![5, 4, 3, 2, 1]
    );
}

#[test]
fn missing_brand_safety_verdict_prevents_ad_insertion() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::SafeGap,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let result = selector.blend(vec![
        post(5, 1.0),
        post(4, 0.9),
        post(3, 0.8),
        post(2, 0.7),
        post(1, 0.6),
        FeedItem::advertisement("ad-1", 2),
    ]);

    assert!(result
        .selected
        .iter()
        .all(|item| item.kind() != FeedItemKind::Advertisement));
    assert_eq!(result.non_selected.len(), 1);
}

#[test]
fn partition_organic_uses_safe_gaps_without_reordering_posts() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::PartitionOrganic,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let unsafe_post = ScoredPost {
        tweet_id: 9,
        score: 1.0,
        brand_safety_verdict: BrandSafetyVerdict::AvoidAdjacency as i32,
        ..Default::default()
    };
    let result = selector.blend(vec![
        FeedItem::post(unsafe_post),
        ad_safe_post(8, 0.9),
        ad_safe_post(7, 0.8),
        ad_safe_post(6, 0.7),
        ad_safe_post(5, 0.6),
        FeedItem::advertisement("ad-1", 2),
    ]);

    assert_eq!(result.selected[0].post_id(), Some(9));
    assert_eq!(result.selected[1].post_id(), Some(8));
    assert_eq!(result.selected[2].kind(), FeedItemKind::Advertisement);
    assert_eq!(
        result
            .selected
            .iter()
            .filter_map(FeedItem::post_id)
            .collect::<Vec<_>>(),
        vec![9, 8, 7, 6, 5]
    );
}

#[test]
fn domain_items_map_to_distinct_transport_variants() {
    let items = vec![
        post(7, 0.7),
        FeedItem::advertisement("ad-1", 2),
        FeedItem::who_to_follow("wtf-1", vec![10, 20]),
        FeedItem::prompt("prompt-1"),
        FeedItem::push_to_home("push-1", ScoredPost::default()),
    ]
    .into_iter()
    .map(FeedItem::into_proto)
    .collect::<Vec<_>>();

    assert!(matches!(items[0].item, Some(feed_item::Item::Post(_))));
    assert!(matches!(
        items[1].item,
        Some(feed_item::Item::Advertisement(_))
    ));
    assert!(matches!(
        items[2].item,
        Some(feed_item::Item::WhoToFollow(_))
    ));
    assert!(matches!(items[3].item, Some(feed_item::Item::Prompt(_))));
    assert!(matches!(
        items[4].item,
        Some(feed_item::Item::PushToHome(_))
    ));
}

struct StaticPromptSource;

#[async_trait]
impl Source<ScoredPostsQuery, FeedItem> for StaticPromptSource {
    async fn get_candidates(&self, _query: &ScoredPostsQuery) -> Result<Vec<FeedItem>, String> {
        Ok(vec![FeedItem::prompt("local-prompt")])
    }
}

#[tokio::test]
async fn supplemental_sources_are_injected_without_changing_the_scored_posts_port() {
    let provider = Arc::new(FakeScoredPostsProvider {
        seen_requests: Mutex::new(Vec::new()),
    });
    let server = ForYouFeedServer::with_provider_and_sources(
        provider,
        BlenderConfig {
            prompt_position: 1,
            ..Default::default()
        },
        vec![Box::new(StaticPromptSource)],
    );

    let output = server
        .get_for_you_feed(ScoredPostsQuery {
            request_id: "request-with-prompt".to_string(),
            ..Default::default()
        })
        .await;

    assert_eq!(
        output.items.iter().map(FeedItem::kind).collect::<Vec<_>>(),
        vec![FeedItemKind::Post, FeedItemKind::Prompt, FeedItemKind::Post]
    );
}

struct ServedAwareScoredPostsProvider {
    seen_served_ids: Mutex<Vec<Vec<i64>>>,
}

#[async_trait]
impl ScoredPostsProvider for ServedAwareScoredPostsProvider {
    async fn score_posts(&self, query: ScoredPostsQuery) -> Result<ScoredPostsOutput, String> {
        self.seen_served_ids
            .lock()
            .expect("served request log")
            .push(query.served_ids.clone());
        let posts = [30_u64, 20_u64]
            .into_iter()
            .filter(|id| !query.served_ids.contains(&(*id as i64)))
            .map(|tweet_id| ScoredPost {
                tweet_id,
                score: tweet_id as f32,
                ..Default::default()
            })
            .collect();
        Ok(ScoredPostsOutput {
            posts,
            request_id: query.request_id,
        })
    }
}

#[test]
fn local_state_truncates_oldest_ids_and_timestamps() {
    let state = InMemoryFeedStateStore::new(2, 1);
    state.record(42, vec![1, 2], 100).expect("first update");
    state.record(42, vec![3], 200).expect("second update");

    let snapshot = state.load(42).expect("state snapshot");

    assert_eq!(snapshot.served_post_ids, vec![2, 3]);
    assert_eq!(snapshot.request_timestamps_ms, vec![200]);
}

#[test]
fn local_state_evicts_the_least_recently_updated_user() {
    let state = InMemoryFeedStateStore::with_max_users(2, 1, 2);
    state.record(1, vec![10], 100).expect("user one");
    state.record(2, vec![20], 200).expect("user two");
    state.record(3, vec![30], 300).expect("user three");

    assert_eq!(state.load(1).expect("evicted user"), Default::default());
    assert_eq!(
        state.load(2).expect("second user").served_post_ids,
        vec![20]
    );
    assert_eq!(state.load(3).expect("third user").served_post_ids, vec![30]);
}

struct FailingFeedStats;

impl FeedStatsSink for FailingFeedStats {
    fn record(&self, _stats: FeedResponseStats) -> Result<(), String> {
        Err("stats unavailable".to_string())
    }
}

#[tokio::test]
async fn stats_failure_does_not_change_the_feed_response() {
    let provider = Arc::new(FakeScoredPostsProvider {
        seen_requests: Mutex::new(Vec::new()),
    });
    let state = Arc::new(InMemoryFeedStateStore::new(20, 5));
    let server = ForYouFeedServer::with_local_state(
        provider,
        BlenderConfig::default(),
        Vec::new(),
        state,
        Arc::new(FailingFeedStats),
    );

    let output = server
        .get_for_you_feed(ScoredPostsQuery {
            request_id: "stats-failure".to_string(),
            ..Default::default()
        })
        .await;

    assert_eq!(
        output
            .items
            .iter()
            .filter_map(FeedItem::post_id)
            .collect::<Vec<_>>(),
        vec![30, 20]
    );
}

#[tokio::test]
async fn local_state_hydrates_the_next_request_and_records_feed_stats() {
    let provider = Arc::new(ServedAwareScoredPostsProvider {
        seen_served_ids: Mutex::new(Vec::new()),
    });
    let state = Arc::new(InMemoryFeedStateStore::new(20, 5));
    let stats = Arc::new(InMemoryFeedStats::default());
    let server = ForYouFeedServer::with_local_state(
        provider.clone(),
        BlenderConfig::default(),
        Vec::new(),
        state.clone(),
        stats.clone(),
    );

    let first = server
        .get_for_you_feed(ScoredPostsQuery {
            user_id: 42,
            request_id: "state-1".to_string(),
            ..Default::default()
        })
        .await;
    stats.wait_for_records(1).await;
    let second = server
        .get_for_you_feed(ScoredPostsQuery {
            user_id: 42,
            request_id: "state-2".to_string(),
            ..Default::default()
        })
        .await;

    assert_eq!(
        first
            .items
            .iter()
            .filter_map(FeedItem::post_id)
            .collect::<Vec<_>>(),
        vec![30, 20]
    );
    assert!(second.items.is_empty());
    assert_eq!(
        provider
            .seen_served_ids
            .lock()
            .expect("served request log")
            .as_slice(),
        [Vec::<i64>::new(), vec![30, 20]]
    );
    let records = stats.records();
    assert_eq!(records[0].request_id, "state-1");
    assert_eq!(records[0].total_items, 2);
    assert_eq!(records[0].count(FeedItemKind::Post), 2);
}

#[tokio::test]
async fn final_feed_server_bridges_scored_posts_without_changing_order() {
    let provider = Arc::new(FakeScoredPostsProvider {
        seen_requests: Mutex::new(Vec::new()),
    });
    let server = ForYouFeedServer::with_provider(provider.clone(), BlenderConfig::default());
    let query = ScoredPostsQuery {
        request_id: "request-42".to_string(),
        ..Default::default()
    };

    let output = server.get_for_you_feed(query).await;

    assert_eq!(output.request_id, "request-42");
    assert_eq!(
        output
            .items
            .iter()
            .filter_map(FeedItem::post_id)
            .collect::<Vec<_>>(),
        vec![30, 20]
    );
    assert_eq!(
        provider
            .seen_requests
            .lock()
            .expect("request log")
            .as_slice(),
        ["request-42"]
    );
}
