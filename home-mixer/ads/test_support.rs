//! 广告混排单测的共用夹具，供 `util`、`safe_gap_blender`、`partition_organic_blender`
//! 三个测试模块使用。用例来源为上游 `home-mixer/ads/tests/`。
//!
//! 与上游夹具的两处必要差异：
//! - 上游 `make_post` 用默认 verdict 表示"可邻接"，本地 `is_safe_for_adjacency`
//!   对未补全的 verdict 按不可邻接处理，因此 `safe_post` 显式写 `SafeForAdjacency`。
//! - 本地 proto 没有 `BSR_NORMAL`，`normal_ad` 用 `Unspecified`——两者在混排里
//!   都表示"不附加 LowRisk 邻接规则"。

use crate::models::feed_item::{Advertisement, FeedItem, FeedItemContent};
use x_algorithm_proto::home_mixer::{BrandSafetyRiskLevel, BrandSafetyVerdict, ScoredPost};

pub(crate) fn post(tweet_id: u64, verdict: BrandSafetyVerdict) -> FeedItem {
    FeedItem::post(
        ScoredPost {
            tweet_id: crate::models::pid(tweet_id).to_string(),
            score: 1.0 - tweet_id as f32 * 0.01,
            brand_safety_verdict: verdict as i32,
            ..Default::default()
        },
        crate::models::pid(tweet_id),
    )
}

pub(crate) fn safe_post(tweet_id: u64) -> FeedItem {
    post(tweet_id, BrandSafetyVerdict::SafeForAdjacency)
}

pub(crate) fn low_risk_post(tweet_id: u64) -> FeedItem {
    post(tweet_id, BrandSafetyVerdict::LowRisk)
}

pub(crate) fn avoid_post(tweet_id: u64) -> FeedItem {
    post(tweet_id, BrandSafetyVerdict::AvoidAdjacency)
}

pub(crate) fn post_with_text(tweet_id: u64, text: &str) -> FeedItem {
    FeedItem::post(
        ScoredPost {
            tweet_id: crate::models::pid(tweet_id).to_string(),
            score: 1.0 - tweet_id as f32 * 0.01,
            brand_safety_verdict: BrandSafetyVerdict::SafeForAdjacency as i32,
            tweet_text: text.to_string(),
            ..Default::default()
        },
        crate::models::pid(tweet_id),
    )
}

pub(crate) fn advertisement(id: u32, brand_safety_risk: BrandSafetyRiskLevel) -> Advertisement {
    Advertisement {
        ad_id: format!("ad-{id}"),
        requested_position: 0,
        brand_safety_risk,
        avoid_handles: Vec::new(),
        avoid_keywords: Vec::new(),
    }
}

pub(crate) fn ad_item(advertisement: Advertisement) -> FeedItem {
    FeedItem {
        position: 0,
        content: FeedItemContent::Advertisement(advertisement),
        post_id: None,
    }
}

pub(crate) fn normal_ad(id: u32) -> FeedItem {
    ad_item(advertisement(id, BrandSafetyRiskLevel::Unspecified))
}

pub(crate) fn normal_ad_at(id: u32, requested_position: usize) -> FeedItem {
    ad_item(Advertisement {
        requested_position,
        ..advertisement(id, BrandSafetyRiskLevel::Unspecified)
    })
}

pub(crate) fn sensitive_ad(id: u32) -> FeedItem {
    ad_item(advertisement(id, BrandSafetyRiskLevel::BsrLow))
}

pub(crate) fn bsr_high_ad(id: u32) -> FeedItem {
    ad_item(advertisement(id, BrandSafetyRiskLevel::BsrHigh))
}

pub(crate) fn keyword_ad(id: u32, keywords: &[&str]) -> Advertisement {
    Advertisement {
        avoid_keywords: keywords.iter().map(|k| k.to_string()).collect(),
        ..advertisement(id, BrandSafetyRiskLevel::Unspecified)
    }
}

pub(crate) fn handle_ad(id: u32, handles: &[u64]) -> Advertisement {
    Advertisement {
        avoid_handles: handles.iter().copied().map(crate::models::uid).collect(),
        ..advertisement(id, BrandSafetyRiskLevel::Unspecified)
    }
}

pub(crate) fn ad_count(items: &[FeedItem]) -> usize {
    items
        .iter()
        .filter(|item| matches!(item.content, FeedItemContent::Advertisement(_)))
        .count()
}

/// 广告在结果序列里的下标。本地混排器不写 `position` 字段（由 `BlenderSelector`
/// 统一编号），所以用下标表达上游用例里的 `ad_positions`。
pub(crate) fn ad_indices(items: &[FeedItem]) -> Vec<usize> {
    items
        .iter()
        .enumerate()
        .filter(|(_, item)| matches!(item.content, FeedItemContent::Advertisement(_)))
        .map(|(index, _)| index)
        .collect()
}

pub(crate) fn ad_risk_levels(items: &[FeedItem]) -> Vec<BrandSafetyRiskLevel> {
    items
        .iter()
        .filter_map(|item| match &item.content {
            FeedItemContent::Advertisement(advertisement) => Some(advertisement.brand_safety_risk),
            _ => None,
        })
        .collect()
}

type NeighbourVerdicts = (Option<BrandSafetyVerdict>, Option<BrandSafetyVerdict>);

pub(crate) fn ad_neighbour_verdicts(items: &[FeedItem]) -> Vec<NeighbourVerdicts> {
    let verdict_at = |index: usize| match items.get(index).map(|item| &item.content) {
        Some(FeedItemContent::Post(post)) => {
            BrandSafetyVerdict::try_from(post.brand_safety_verdict).ok()
        }
        _ => None,
    };

    ad_indices(items)
        .into_iter()
        .map(|index| {
            let above = index.checked_sub(1).and_then(verdict_at);
            (above, verdict_at(index + 1))
        })
        .collect()
}

pub(crate) fn labels(items: &[FeedItem]) -> Vec<String> {
    items
        .iter()
        .map(|item| match &item.content {
            FeedItemContent::Post(post) => match crate::models::ObjectId::parse(&post.tweet_id)
                .ok()
                .and_then(|id| id.to_u64_be_padded())
            {
                Some(n) => format!("post-{n}"),
                None => format!("post-{}", post.tweet_id),
            },
            FeedItemContent::Advertisement(advertisement) => advertisement.ad_id.clone(),
            other => format!("{other:?}"),
        })
        .collect()
}
