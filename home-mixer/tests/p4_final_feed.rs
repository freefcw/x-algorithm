use home_mixer::feed_state::{FeedStateStore, InMemoryFeedStateStore};
use home_mixer::feed_stats::{FeedResponseStats, FeedStatsSink, InMemoryFeedStats};
use home_mixer::for_you_server::ForYouFeedServer;
use home_mixer::models::feed_item::{Advertisement, FeedItem, FeedItemContent, FeedItemKind};
use home_mixer::models::query::ScoredPostsQuery;
use home_mixer::models::{pid, uid};
use home_mixer::scored_posts_server::ScoredPostsOutput;
use home_mixer::selectors::blender_selector::{AdsBlenderStrategy, BlenderConfig, BlenderSelector};
use home_mixer::sources::ads_source::AdvertisementSource;
use home_mixer::sources::scored_posts_source::ScoredPostsProvider;
use std::sync::{Arc, Mutex};
use tonic::async_trait;
use x_algorithm_proto::home_mixer::{
    feed_item, BrandSafetyRiskLevel, BrandSafetyVerdict, ScoredPost,
};
use xai_candidate_pipeline::source::Source;

fn post(id: u64, score: f32) -> FeedItem {
    FeedItem::post(
        ScoredPost {
            tweet_id: pid(id).to_string(),
            score,
            ..Default::default()
        },
        pid(id),
    )
}

fn ad_safe_post(id: u64, score: f32) -> FeedItem {
    FeedItem::post(
        ScoredPost {
            tweet_id: pid(id).to_string(),
            score,
            brand_safety_verdict: BrandSafetyVerdict::SafeForAdjacency as i32,
            ..Default::default()
        },
        pid(id),
    )
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

    assert_eq!(post_ids, vec![pid(30), pid(20), pid(10)]);
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
        FeedItem::who_to_follow("people-you-may-know", vec![uid(101), uid(102)]),
        FeedItem::push_to_home(
            "notification-1",
            ScoredPost {
                tweet_id: pid(99).to_string(),
                score: 0.1,
                ..Default::default()
            },
            pid(99),
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
    assert_eq!(post_ids, vec![pid(40), pid(30), pid(20), pid(10)]);
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
        FeedItem::push_to_home(
            "first",
            ScoredPost::default(),
            home_mixer::models::PostId::NIL,
        ),
        FeedItem::push_to_home(
            "second",
            ScoredPost::default(),
            home_mixer::models::PostId::NIL,
        ),
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
                    tweet_id: pid(30).to_string(),
                    score: 0.9,
                    ..Default::default()
                },
                ScoredPost {
                    tweet_id: pid(20).to_string(),
                    score: 0.8,
                    ..Default::default()
                },
            ],
            selected_ids: vec![pid(30), pid(20)],
            request_id: query.request_id,
        })
    }
}

#[tokio::test]
async fn disabled_advertisement_source_never_emits_candidates() {
    let source = AdvertisementSource::disabled();

    let candidates = source
        .source(&ScoredPostsQuery::default())
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
        vec![pid(5), pid(4), pid(3), pid(2), pid(1)]
    );
}

#[test]
fn safe_gap_blender_allows_low_risk_posts() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::SafeGap,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let result = selector.blend(vec![
        ad_low_risk_post(5, 0.5),
        ad_low_risk_post(4, 0.4),
        ad_low_risk_post(3, 0.3),
        ad_low_risk_post(2, 0.2),
        ad_low_risk_post(1, 0.1),
        FeedItem::advertisement("ad-low-risk-gap", 2),
    ]);

    assert!(result
        .selected
        .iter()
        .any(|item| item.kind() == FeedItemKind::Advertisement));
    assert!(result.non_selected.is_empty());
}

#[test]
fn safe_gap_blender_uses_upstream_requested_and_min_spacing() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::SafeGap,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let mut candidates = (1..=11)
        .rev()
        .map(|id| ad_safe_post(id, id as f32))
        .collect::<Vec<_>>();
    candidates.extend([
        FeedItem::advertisement("ad-1", 2),
        FeedItem::advertisement("ad-2", 5),
        FeedItem::advertisement("ad-3", 9),
    ]);

    let result = selector.blend(candidates);
    let ad_positions = result
        .selected
        .iter()
        .filter(|item| item.kind() == FeedItemKind::Advertisement)
        .map(|item| item.position)
        .collect::<Vec<_>>();

    assert_eq!(ad_positions, vec![2, 6, 10]);
    assert!(result.non_selected.is_empty());
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
fn partition_organic_matches_upstream_grouping() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::PartitionOrganic,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let unsafe_post = ScoredPost {
        tweet_id: pid(9).to_string(),
        score: 1.0,
        brand_safety_verdict: BrandSafetyVerdict::AvoidAdjacency as i32,
        ..Default::default()
    };
    let result = selector.blend(vec![
        FeedItem::post(unsafe_post, pid(9)),
        ad_safe_post(8, 0.9),
        ad_safe_post(7, 0.8),
        ad_safe_post(6, 0.7),
        ad_safe_post(5, 0.6),
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
            FeedItemKind::Advertisement,
            FeedItemKind::Post,
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
        vec![pid(8), pid(7), pid(9), pid(6), pid(5)]
    );
    assert!(result.non_selected.is_empty());
}

/// 构造带安全属性的广告 FeedItem
fn ad_with_safety(
    ad_id: &str,
    position: usize,
    risk: BrandSafetyRiskLevel,
    avoid_handles: Vec<home_mixer::models::UserId>,
    avoid_keywords: Vec<&str>,
) -> FeedItem {
    FeedItem {
        position: 0,
        content: FeedItemContent::Advertisement(Advertisement {
            ad_id: ad_id.to_string(),
            requested_position: position,
            brand_safety_risk: risk,
            avoid_handles,
            avoid_keywords: avoid_keywords.into_iter().map(String::from).collect(),
        }),
        post_id: None,
    }
}

/// 带作者 ID 的安全帖
fn ad_safe_post_with_author(id: u64, author_id: u64, text: &str, score: f32) -> FeedItem {
    FeedItem::post(
        ScoredPost {
            tweet_id: pid(id).to_string(),
            author_id: uid(author_id).to_string(),
            score,
            tweet_text: text.to_string(),
            brand_safety_verdict: BrandSafetyVerdict::SafeForAdjacency as i32,
            ..Default::default()
        },
        pid(id),
    )
}

fn ad_low_risk_post(id: u64, score: f32) -> FeedItem {
    FeedItem::post(
        ScoredPost {
            tweet_id: pid(id).to_string(),
            score,
            brand_safety_verdict: BrandSafetyVerdict::LowRisk as i32,
            ..Default::default()
        },
        pid(id),
    )
}

#[test]
fn partition_organic_drops_bsr_low_and_ias_ads_next_to_low_risk_post() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::PartitionOrganic,
        min_posts_for_ads: 5,
        ..Default::default()
    });

    for risk in [BrandSafetyRiskLevel::BsrLow, BrandSafetyRiskLevel::BsrIas] {
        let result = selector.blend(vec![
            ad_low_risk_post(5, 0.5),
            ad_safe_post(4, 0.4),
            ad_safe_post(3, 0.3),
            ad_safe_post(2, 0.2),
            ad_safe_post(1, 0.1),
            ad_with_safety("ad-low", 2, risk, vec![], vec![]),
        ]);

        assert!(result
            .selected
            .iter()
            .all(|item| item.kind() != FeedItemKind::Advertisement));
        assert_eq!(result.non_selected.len(), 1);
        assert_eq!(result.non_selected[0].kind(), FeedItemKind::Advertisement);
    }
}

#[test]
fn partition_organic_limits_low_risk_rule_to_bsr_low_and_ias() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::PartitionOrganic,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let result = selector.blend(vec![
        ad_low_risk_post(5, 0.5),
        ad_safe_post(4, 0.4),
        ad_safe_post(3, 0.3),
        ad_safe_post(2, 0.2),
        ad_safe_post(1, 0.1),
        ad_with_safety("ad-high", 2, BrandSafetyRiskLevel::BsrHigh, vec![], vec![]),
    ]);

    assert!(result
        .selected
        .iter()
        .any(|item| item.kind() == FeedItemKind::Advertisement));
    assert!(result.non_selected.is_empty());
}

#[test]
fn partition_organic_drops_ad_when_adjacent_author_in_avoid_handles() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::PartitionOrganic,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let posts = vec![
        ad_safe_post_with_author(5, 100, "post 5", 0.5),
        ad_safe_post_with_author(4, 200, "post 4", 0.4),
        ad_safe_post_with_author(3, 300, "post 3", 0.3),
        ad_safe_post_with_author(2, 400, "post 2", 0.2),
        ad_safe_post_with_author(1, 500, "post 1", 0.1),
    ];
    let ads = vec![ad_with_safety(
        "ad-handle",
        2,
        BrandSafetyRiskLevel::Unspecified,
        vec![uid(100), uid(200), uid(300), uid(400), uid(500)], // 所有帖子作者都在规避列表
        vec![],
    )];

    let result = selector.blend(posts.into_iter().chain(ads).collect::<Vec<_>>());

    // 所有帖子作者都在规避列表，广告无法放置
    assert!(result
        .non_selected
        .iter()
        .any(|item| item.kind() == FeedItemKind::Advertisement));
    assert!(!result
        .selected
        .iter()
        .any(|item| item.kind() == FeedItemKind::Advertisement));
}

#[test]
fn partition_organic_rejects_ad_when_assigned_group_matches_keyword() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::PartitionOrganic,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let posts = vec![
        ad_safe_post_with_author(5, 100, "normal content", 0.5),
        ad_safe_post_with_author(4, 200, "contains spoiler text", 0.4),
        ad_safe_post_with_author(3, 300, "normal content", 0.3),
        ad_safe_post_with_author(2, 400, "normal content", 0.2),
        ad_safe_post_with_author(1, 500, "normal content", 0.1),
    ];
    let advertisement = ad_with_safety(
        "ad-keyword",
        2,
        BrandSafetyRiskLevel::Unspecified,
        vec![],
        vec!["spoiler"],
    );

    let result = selector.blend(posts.into_iter().chain([advertisement]).collect());

    assert!(result
        .selected
        .iter()
        .all(|item| item.kind() != FeedItemKind::Advertisement));
    assert_eq!(result.non_selected.len(), 1);
    assert_eq!(result.non_selected[0].kind(), FeedItemKind::Advertisement);
}

#[test]
fn partition_organic_rejects_ad_when_every_gap_matches_avoid_keyword() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::PartitionOrganic,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let posts = vec![
        ad_safe_post_with_author(5, 100, "spoiler 5", 0.5),
        ad_safe_post_with_author(4, 200, "spoiler 4", 0.4),
        ad_safe_post_with_author(3, 300, "spoiler 3", 0.3),
        ad_safe_post_with_author(2, 400, "spoiler 2", 0.2),
        ad_safe_post_with_author(1, 500, "spoiler 1", 0.1),
    ];
    let advertisement = ad_with_safety(
        "ad-keyword",
        2,
        BrandSafetyRiskLevel::Unspecified,
        vec![],
        vec!["spoiler"],
    );

    let result = selector.blend(posts.into_iter().chain([advertisement]).collect());

    assert!(result
        .selected
        .iter()
        .all(|item| item.kind() != FeedItemKind::Advertisement));
    assert_eq!(result.non_selected.len(), 1);
    assert_eq!(result.non_selected[0].kind(), FeedItemKind::Advertisement);
}

#[test]
fn partition_organic_places_one_ad_with_two_safe_posts() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::PartitionOrganic,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let result = selector.blend(vec![
        post(5, 0.5),
        ad_safe_post(4, 0.4),
        ad_safe_post(3, 0.3),
        post(2, 0.2),
        post(1, 0.1),
        FeedItem::advertisement("ad-safe-pair", 2),
    ]);

    let ad_position = result
        .selected
        .iter()
        .position(|item| item.kind() == FeedItemKind::Advertisement);

    assert_eq!(ad_position, Some(1));
    assert!(!result
        .non_selected
        .iter()
        .any(|item| item.kind() == FeedItemKind::Advertisement));
}

#[test]
fn partition_organic_limits_ad_count_using_upstream_requested_spacing() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::PartitionOrganic,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let result = selector.blend(vec![
        ad_safe_post(6, 0.6),
        ad_safe_post(5, 0.5),
        ad_safe_post(4, 0.4),
        ad_safe_post(3, 0.3),
        ad_safe_post(2, 0.2),
        ad_safe_post(1, 0.1),
        FeedItem::advertisement("ad-1", 1),
        FeedItem::advertisement("ad-2", 2),
        FeedItem::advertisement("ad-3", 3),
    ]);

    assert_eq!(
        result
            .selected
            .iter()
            .filter(|item| item.kind() == FeedItemKind::Advertisement)
            .count(),
        1
    );
    assert_eq!(
        result
            .non_selected
            .iter()
            .filter(|item| item.kind() == FeedItemKind::Advertisement)
            .count(),
        2
    );
}

#[test]
fn partition_organic_reuses_group_after_rejected_ad() {
    let selector = BlenderSelector::new(BlenderConfig {
        ads_strategy: AdsBlenderStrategy::PartitionOrganic,
        min_posts_for_ads: 5,
        ..Default::default()
    });
    let result = selector.blend(vec![
        ad_low_risk_post(7, 0.7),
        ad_safe_post(6, 0.6),
        ad_safe_post(5, 0.5),
        ad_safe_post(4, 0.4),
        ad_safe_post(3, 0.3),
        ad_safe_post(2, 0.2),
        ad_safe_post(1, 0.1),
        ad_with_safety("ad-low", 0, BrandSafetyRiskLevel::BsrLow, vec![], vec![]),
        FeedItem::advertisement("ad-1", 1),
        FeedItem::advertisement("ad-2", 4),
    ]);

    let selected_ad_ids = result
        .selected
        .iter()
        .filter_map(|item| match &item.content {
            FeedItemContent::Advertisement(advertisement) => Some(advertisement.ad_id.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let rejected_ad_ids = result
        .non_selected
        .iter()
        .filter_map(|item| match &item.content {
            FeedItemContent::Advertisement(advertisement) => Some(advertisement.ad_id.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(selected_ad_ids, vec!["ad-1", "ad-2"]);
    assert_eq!(rejected_ad_ids, vec!["ad-low"]);
    assert_eq!(
        result
            .selected
            .iter()
            .filter(|item| item.kind() == FeedItemKind::Advertisement)
            .map(|item| item.position)
            .collect::<Vec<_>>(),
        vec![1, 5]
    );
}

#[test]
fn domain_items_map_to_distinct_transport_variants() {
    let items = vec![
        post(7, 0.7),
        FeedItem::advertisement("ad-1", 2),
        FeedItem::who_to_follow("wtf-1", vec![uid(10), uid(20)]),
        FeedItem::prompt("prompt-1"),
        FeedItem::push_to_home(
            "push-1",
            ScoredPost::default(),
            home_mixer::models::PostId::NIL,
        ),
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
    async fn source(&self, _query: &ScoredPostsQuery) -> Result<Vec<FeedItem>, String> {
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
    seen_served_ids: Mutex<Vec<Vec<home_mixer::models::PostId>>>,
}

#[async_trait]
impl ScoredPostsProvider for ServedAwareScoredPostsProvider {
    async fn score_posts(&self, query: ScoredPostsQuery) -> Result<ScoredPostsOutput, String> {
        self.seen_served_ids
            .lock()
            .expect("served request log")
            .push(query.served_ids.clone());
        let selected_ids = [pid(30), pid(20)]
            .into_iter()
            .filter(|id| !query.served_ids.contains(id))
            .collect::<Vec<_>>();
        let posts = selected_ids
            .iter()
            .map(|tweet_id| ScoredPost {
                tweet_id: tweet_id.to_string(),
                score: tweet_id.to_u64_be_padded().unwrap_or(0) as f32,
                ..Default::default()
            })
            .collect();
        Ok(ScoredPostsOutput {
            posts,
            selected_ids,
            request_id: query.request_id,
        })
    }
}

#[test]
fn local_state_truncates_oldest_ids_and_timestamps() {
    let state = InMemoryFeedStateStore::new(2, 1);
    state
        .record(uid(42), vec![pid(1), pid(2)], 100)
        .expect("first update");
    state
        .record(uid(42), vec![pid(3)], 200)
        .expect("second update");

    let snapshot = state.load(uid(42)).expect("state snapshot");

    assert_eq!(snapshot.served_post_ids, vec![pid(2), pid(3)]);
    assert_eq!(snapshot.request_timestamps_ms, vec![200]);
}

#[test]
fn local_state_evicts_the_least_recently_updated_user() {
    let state = InMemoryFeedStateStore::with_max_users(2, 1, 2);
    state.record(uid(1), vec![pid(10)], 100).expect("user one");
    state.record(uid(2), vec![pid(20)], 200).expect("user two");
    state
        .record(uid(3), vec![pid(30)], 300)
        .expect("user three");

    assert_eq!(
        state.load(uid(1)).expect("evicted user"),
        Default::default()
    );
    assert_eq!(
        state.load(uid(2)).expect("second user").served_post_ids,
        vec![pid(20)]
    );
    assert_eq!(
        state.load(uid(3)).expect("third user").served_post_ids,
        vec![pid(30)]
    );
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
        vec![pid(30), pid(20)]
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
            user_id: uid(42),
            request_id: "state-1".to_string(),
            ..Default::default()
        })
        .await;
    stats.wait_for_records(1).await;
    let second = server
        .get_for_you_feed(ScoredPostsQuery {
            user_id: uid(42),
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
        vec![pid(30), pid(20)]
    );
    assert!(second.items.is_empty());
    assert_eq!(
        provider
            .seen_served_ids
            .lock()
            .expect("served request log")
            .as_slice(),
        [
            Vec::<home_mixer::models::PostId>::new(),
            vec![pid(30), pid(20)]
        ]
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
        vec![pid(30), pid(20)]
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
