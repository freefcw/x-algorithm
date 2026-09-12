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

    // 空串表示"无此关系"，不能当作真实账号去匹配规避名单。
    let is_avoided = |user_id: &str| {
        !user_id.is_empty()
            && advertisement
                .avoid_handles
                .iter()
                .any(|id| id.to_string() == user_id)
    };

    // 转推会把被规避账号的内容带到广告旁边，只看 author_id 会漏掉这条路径。
    let has_avoided_author = |item: &FeedItem| {
        let FeedItemContent::Post(post) = &item.content else {
            return false;
        };
        is_avoided(&post.author_id) || is_avoided(&post.retweeted_user_id)
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
    use super::super::test_support::*;
    use super::*;
    use x_algorithm_proto::home_mixer::ScoredPost;

    #[test]
    fn unsigned_author_beyond_signed_contract_does_not_match_negative_handle() {
        let advertisement = Advertisement {
            ad_id: "ad-1".to_string(),
            requested_position: 1,
            brand_safety_risk: BrandSafetyRiskLevel::BsrLow,
            avoid_handles: vec![crate::models::uid(1)],
            avoid_keywords: Vec::new(),
        };
        let post = FeedItem {
            position: 0,
            content: FeedItemContent::Post(ScoredPost {
                author_id: crate::models::uid(u64::MAX).to_string(),
                ..Default::default()
            }),
        };

        assert!(!should_drop_handle(&advertisement, Some(&post), None));
    }

    fn ad_avoiding(handle: u64) -> Advertisement {
        Advertisement {
            ad_id: "ad-1".to_string(),
            requested_position: 1,
            brand_safety_risk: BrandSafetyRiskLevel::BsrLow,
            avoid_handles: vec![crate::models::uid(handle)],
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
            author_id: crate::models::uid(500).to_string(),
            retweeted_user_id: crate::models::uid(42).to_string(),
            ..Default::default()
        });

        assert!(should_drop_handle(&ad_avoiding(42), Some(&item), None));
    }

    #[test]
    fn absent_retweet_relationship_does_not_match_a_zero_handle() {
        let item = post_item(ScoredPost {
            author_id: crate::models::uid(500).to_string(),
            retweeted_user_id: String::new(),
            ..Default::default()
        });

        assert!(!should_drop_handle(&ad_avoiding(0), Some(&item), None));
    }

    #[test]
    fn safe_and_low_risk_verdicts_allow_adjacency() {
        assert!(is_safe_for_adjacency(&safe_post(1)));
        assert!(is_safe_for_adjacency(&low_risk_post(1)));
    }

    #[test]
    fn avoid_verdict_blocks_adjacency() {
        assert!(!is_safe_for_adjacency(&avoid_post(1)));
    }

    #[test]
    fn unspecified_verdict_blocks_adjacency() {
        // 上游把默认 verdict 当作可邻接，本地按 proto 注释「未补全时按不可邻接处理」收紧。
        assert!(!is_safe_for_adjacency(&post(
            1,
            BrandSafetyVerdict::Unspecified
        )));
    }

    #[test]
    fn find_safe_gaps_all_safe() {
        let posts = [safe_post(1), safe_post(2), safe_post(3)];
        assert_eq!(find_safe_gaps(&posts), vec![1, 2]);
    }

    #[test]
    fn find_safe_gaps_avoid_blocks_adjacent_gaps() {
        let posts = [safe_post(1), avoid_post(2), safe_post(3), safe_post(4)];
        assert_eq!(find_safe_gaps(&posts), vec![3]);
    }

    #[test]
    fn find_safe_gaps_all_avoid() {
        let posts = [avoid_post(1), avoid_post(2), avoid_post(3)];
        assert_eq!(find_safe_gaps(&posts), Vec::<usize>::new());
    }

    #[test]
    fn find_safe_gaps_empty_posts() {
        assert_eq!(find_safe_gaps(&[]), Vec::<usize>::new());
    }

    #[test]
    fn compute_spacing_standard() {
        let advertisements = [
            normal_ad_at(1, 1),
            normal_ad_at(2, 4),
            normal_ad_at(3, 7),
            normal_ad_at(4, 10),
            normal_ad_at(5, 13),
        ];
        assert_eq!(
            compute_spacing(&advertisements),
            AdSpacing {
                requested: 3,
                min: 2
            }
        );
    }

    #[test]
    fn compute_spacing_wide() {
        let advertisements = [
            normal_ad_at(1, 1),
            normal_ad_at(2, 8),
            normal_ad_at(3, 15),
            normal_ad_at(4, 22),
            normal_ad_at(5, 29),
        ];
        assert_eq!(
            compute_spacing(&advertisements),
            AdSpacing {
                requested: 7,
                min: 4
            }
        );
    }

    #[test]
    fn compute_spacing_single_ad_uses_default() {
        assert_eq!(compute_spacing(&[normal_ad_at(1, 5)]), DEFAULT_SPACING);
    }

    #[test]
    fn compute_spacing_empty_uses_default() {
        assert_eq!(compute_spacing(&[]), DEFAULT_SPACING);
    }

    #[test]
    fn compute_spacing_same_positions_uses_default() {
        let advertisements = [normal_ad(1), normal_ad(2), normal_ad(3)];
        assert_eq!(compute_spacing(&advertisements), DEFAULT_SPACING);
    }

    #[test]
    fn compute_spacing_small_gap_falls_back_to_default() {
        let advertisements = [normal_ad_at(1, 1), normal_ad_at(2, 4), normal_ad_at(3, 5)];
        assert_eq!(compute_spacing(&advertisements), DEFAULT_SPACING);
    }

    #[test]
    fn compute_spacing_gap_of_two_falls_back_to_default() {
        let advertisements = [normal_ad_at(1, 1), normal_ad_at(2, 3), normal_ad_at(3, 5)];
        assert_eq!(compute_spacing(&advertisements), DEFAULT_SPACING);
    }

    #[test]
    fn compute_spacing_consecutive_positions_fall_back_to_default() {
        let advertisements = [normal_ad_at(1, 1), normal_ad_at(2, 2), normal_ad_at(3, 3)];
        assert_eq!(compute_spacing(&advertisements), DEFAULT_SPACING);
    }

    #[test]
    fn compute_spacing_gap_of_three_is_accepted() {
        let advertisements = [normal_ad_at(1, 1), normal_ad_at(2, 4), normal_ad_at(3, 7)];
        assert_eq!(
            compute_spacing(&advertisements),
            AdSpacing {
                requested: 3,
                min: 2
            }
        );
    }

    #[test]
    fn compute_spacing_unsorted_positions() {
        let advertisements = [normal_ad_at(1, 15), normal_ad_at(2, 1), normal_ad_at(3, 8)];
        assert_eq!(
            compute_spacing(&advertisements),
            AdSpacing {
                requested: 7,
                min: 4
            }
        );
    }

    fn keyword_drops_above(keywords: &[&str], text: &str) -> bool {
        should_drop_keyword(
            &keyword_ad(100, keywords),
            Some(&post_with_text(1, text)),
            None,
        )
    }

    #[test]
    fn keyword_exact_word_match() {
        assert!(keyword_drops_above(
            &["acme"],
            "Acme Corp releases new product today"
        ));
    }

    #[test]
    fn keyword_no_sub_word_match() {
        assert!(!keyword_drops_above(
            &["ho"],
            "How do we feel about who does this?"
        ));
    }

    #[test]
    fn keyword_no_sub_word_match_pot() {
        assert!(!keyword_drops_above(
            &["pot"],
            "This shows potential for growth"
        ));
    }

    #[test]
    fn keyword_no_sub_word_match_ass() {
        assert!(!keyword_drops_above(
            &["ass"],
            "The Passiflora plant is a beautiful class of flowers"
        ));
    }

    #[test]
    fn keyword_no_sub_word_match_meth() {
        assert!(!keyword_drops_above(
            &["meth"],
            "Something is occurring metaphysically beyond our realm"
        ));
    }

    #[test]
    fn keyword_no_sub_word_match_cop() {
        assert!(!keyword_drops_above(
            &["cop"],
            "You can't copy the link for videos anymore"
        ));
    }

    #[test]
    fn keyword_empty_string_never_matches() {
        assert!(!keyword_drops_above(
            &[""],
            "Any content here should not be matched"
        ));
    }

    #[test]
    fn keyword_all_empty_keywords_never_match() {
        assert!(!keyword_drops_above(
            &["", "", ""],
            "Some random tweet text"
        ));
    }

    #[test]
    fn keyword_empty_tweet_text_never_matches() {
        assert!(!keyword_drops_above(&["acme"], ""));
    }

    #[test]
    fn keyword_case_insensitive() {
        assert!(keyword_drops_above(
            &["ACME"],
            "acme corp launches new service"
        ));
    }

    #[test]
    fn keyword_with_punctuation() {
        assert!(keyword_drops_above(
            &["beer"],
            "He wasn't asking for money to buy beer."
        ));
    }

    #[test]
    fn keyword_multi_word_phrase_match() {
        assert!(keyword_drops_above(
            &["san francisco"],
            "I visited San Francisco last week"
        ));
    }

    #[test]
    fn keyword_multi_word_phrase_no_match_non_consecutive() {
        assert!(!keyword_drops_above(
            &["san francisco"],
            "San Diego and Francisco are different places"
        ));
    }

    #[test]
    fn keyword_multi_word_phrase_match_names() {
        assert!(keyword_drops_above(
            &["jane doe"],
            "Jane Doe has officially announced something"
        ));
    }

    #[test]
    fn keyword_multi_word_no_partial() {
        assert!(!keyword_drops_above(
            &["jane doe"],
            "Jane tweeted something interesting today"
        ));
    }

    #[test]
    fn keyword_with_hashtag() {
        assert!(keyword_drops_above(
            &["acme"],
            "#acme 2024 product launch event"
        ));
    }

    #[test]
    fn keyword_checks_both_neighbours() {
        let above = post_with_text(1, "Just a normal day at the park");
        let below = post_with_text(2, "Buy bitcoin now while it's cheap!");

        assert!(should_drop_keyword(
            &keyword_ad(100, &["bitcoin"]),
            Some(&above),
            Some(&below)
        ));
    }

    #[test]
    fn keyword_no_match_returns_false() {
        assert!(!keyword_drops_above(
            &["bitcoin"],
            "Beautiful sunset over the mountains"
        ));
    }

    #[test]
    fn keyword_short_word_18_no_match_in_2018() {
        assert!(!keyword_drops_above(
            &["18"],
            "In 2018 the world changed significantly"
        ));
    }

    #[test]
    fn keyword_accent_normalization() {
        assert!(keyword_drops_above(
            &["cafe"],
            "I visited a lovely café downtown"
        ));
    }

    #[test]
    fn keyword_multi_word_two_word_match() {
        assert!(keyword_drops_above(
            &["ice cream"],
            "We stopped for ice cream on the way home"
        ));
    }

    #[test]
    fn keyword_multi_word_three_word_match() {
        assert!(keyword_drops_above(
            &["new york city"],
            "Visiting New York City this weekend for the first time"
        ));
    }

    #[test]
    fn keyword_multi_word_broken_by_intervening_word() {
        assert!(!keyword_drops_above(
            &["ice cream"],
            "The ice cold cream soda was refreshing"
        ));
    }
}
