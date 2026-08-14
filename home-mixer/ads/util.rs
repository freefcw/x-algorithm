use crate::models::feed_item::{Advertisement, FeedItem, FeedItemContent};
use crate::post_text::TweetTokenizer;
use std::sync::LazyLock;
use x_algorithm_proto::home_mixer::{BrandSafetyRiskLevel, BrandSafetyVerdict};

static TWEET_TOKENIZER: LazyLock<TweetTokenizer> = LazyLock::new(TweetTokenizer::new);

pub(crate) const MIN_REQUESTED_GAP: usize = 3;
pub(crate) const DEFAULT_SPACING: AdSpacing = AdSpacing {
    requested: 3,
    min: 2,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdSpacing {
    pub(crate) requested: usize,
    pub(crate) min: usize,
}

pub(crate) fn compute_spacing(advertisements: &[FeedItem]) -> AdSpacing {
    if advertisements.len() < 2 {
        return DEFAULT_SPACING;
    }

    let mut positions = advertisements
        .iter()
        .take(4)
        .map(requested_ad_position)
        .collect::<Vec<_>>();
    positions.sort_unstable();

    let min_diff = positions
        .windows(2)
        .map(|window| window[1].abs_diff(window[0]))
        .filter(|difference| *difference > 0)
        .min();

    match min_diff {
        Some(requested) if requested >= MIN_REQUESTED_GAP => AdSpacing {
            requested,
            min: requested.div_ceil(2),
        },
        _ => DEFAULT_SPACING,
    }
}

pub(crate) fn find_safe_gaps(posts: &[FeedItem]) -> Vec<usize> {
    (1..posts.len())
        .filter(|gap| is_safe_for_adjacency(&posts[gap - 1]))
        .filter(|gap| is_safe_for_adjacency(&posts[*gap]))
        .collect()
}

pub(crate) fn requested_ad_position(item: &FeedItem) -> usize {
    match &item.content {
        FeedItemContent::Advertisement(advertisement) => advertisement.requested_position,
        _ => 0,
    }
}

pub(crate) fn interleave_at_gaps(
    posts: Vec<FeedItem>,
    advertisements: Vec<FeedItem>,
    placements: &[usize],
) -> Vec<FeedItem> {
    let mut advertisements = advertisements.into_iter();
    let mut placement_index = 0;
    let mut blended = Vec::with_capacity(posts.len() + placements.len());

    for (post_index, post) in posts.into_iter().enumerate() {
        if placements.get(placement_index) == Some(&post_index) {
            if let Some(advertisement) = advertisements.next() {
                blended.push(advertisement);
            }
            placement_index += 1;
        }
        blended.push(post);
    }

    blended
}

pub(crate) fn is_safe_for_adjacency(item: &FeedItem) -> bool {
    matches!(
        brand_safety_verdict(item),
        Some(BrandSafetyVerdict::SafeForAdjacency | BrandSafetyVerdict::LowRisk)
    )
}

pub(crate) fn should_drop_bsr_low(
    advertisement: &Advertisement,
    above: Option<&FeedItem>,
    below: Option<&FeedItem>,
) -> bool {
    if !matches!(
        advertisement.brand_safety_risk,
        BrandSafetyRiskLevel::BsrLow | BrandSafetyRiskLevel::BsrIas
    ) {
        return false;
    }

    above.is_some_and(is_low_risk_post) || below.is_some_and(is_low_risk_post)
}

pub(crate) fn should_drop_handle(
    advertisement: &Advertisement,
    above: Option<&FeedItem>,
    below: Option<&FeedItem>,
) -> bool {
    if advertisement.avoid_handles.is_empty() {
        return false;
    }

    // 0 表示"无此关系"，不能当作真实账号去匹配规避名单。
    let is_avoided = |user_id: u64| {
        user_id != 0
            && i64::try_from(user_id)
                .ok()
                .is_some_and(|id| advertisement.avoid_handles.contains(&id))
    };

    // 转推会把被规避账号的内容带到广告旁边，只看 author_id 会漏掉这条路径。
    let has_avoided_author = |item: &FeedItem| {
        let FeedItemContent::Post(post) = &item.content else {
            return false;
        };
        is_avoided(post.author_id) || is_avoided(post.retweeted_user_id)
    };

    above.is_some_and(has_avoided_author) || below.is_some_and(has_avoided_author)
}

pub(crate) fn should_drop_keyword(
    advertisement: &Advertisement,
    above: Option<&FeedItem>,
    below: Option<&FeedItem>,
) -> bool {
    let keyword_sequences = advertisement
        .avoid_keywords
        .iter()
        .map(|keyword| TWEET_TOKENIZER.tokenize(keyword))
        .filter(|sequence| !sequence.tokens.is_empty())
        .collect::<Vec<_>>();
    if keyword_sequences.is_empty() {
        return false;
    }

    let text_matches = |item: &FeedItem| {
        let FeedItemContent::Post(post) = &item.content else {
            return false;
        };
        let tweet_sequence = TWEET_TOKENIZER.tokenize(&post.tweet_text);
        keyword_sequences
            .iter()
            .any(|keyword| tweet_sequence.contains_keyword_sequence(keyword))
    };

    above.is_some_and(text_matches) || below.is_some_and(text_matches)
}

fn brand_safety_verdict(item: &FeedItem) -> Option<BrandSafetyVerdict> {
    let FeedItemContent::Post(post) = &item.content else {
        return None;
    };
    BrandSafetyVerdict::try_from(post.brand_safety_verdict).ok()
}

fn is_low_risk_post(item: &FeedItem) -> bool {
    brand_safety_verdict(item) == Some(BrandSafetyVerdict::LowRisk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use x_algorithm_proto::home_mixer::ScoredPost;

    #[test]
    fn unsigned_author_beyond_signed_contract_does_not_match_negative_handle() {
        let advertisement = Advertisement {
            ad_id: "ad-1".to_string(),
            requested_position: 1,
            brand_safety_risk: BrandSafetyRiskLevel::BsrLow,
            avoid_handles: vec![-1],
            avoid_keywords: Vec::new(),
        };
        let post = FeedItem {
            position: 0,
            content: FeedItemContent::Post(ScoredPost {
                author_id: u64::MAX,
                ..Default::default()
            }),
        };

        assert!(!should_drop_handle(&advertisement, Some(&post), None));
    }

    fn ad_avoiding(handle: i64) -> Advertisement {
        Advertisement {
            ad_id: "ad-1".to_string(),
            requested_position: 1,
            brand_safety_risk: BrandSafetyRiskLevel::BsrLow,
            avoid_handles: vec![handle],
            avoid_keywords: Vec::new(),
        }
    }

    fn post_item(post: ScoredPost) -> FeedItem {
        FeedItem {
            position: 0,
            content: FeedItemContent::Post(post),
        }
    }

    #[test]
    fn retweet_of_an_avoided_handle_drops_the_ad() {
        let item = post_item(ScoredPost {
            author_id: 500,
            retweeted_user_id: 42,
            ..Default::default()
        });

        assert!(should_drop_handle(&ad_avoiding(42), Some(&item), None));
    }

    #[test]
    fn absent_retweet_relationship_does_not_match_a_zero_handle() {
        let item = post_item(ScoredPost {
            author_id: 500,
            retweeted_user_id: 0,
            ..Default::default()
        });

        assert!(!should_drop_handle(&ad_avoiding(0), Some(&item), None));
    }
}
