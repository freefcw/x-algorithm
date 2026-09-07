use super::util::{
    compute_spacing, is_safe_for_adjacency, should_drop_bsr_low, should_drop_handle,
    should_drop_keyword,
};
use super::{AdBlendResult, AdsBlender};
use crate::models::feed_item::{FeedItem, FeedItemContent};
use std::cmp::Ordering;

pub(crate) struct PartitionOrganicAdsBlender;

impl AdsBlender for PartitionOrganicAdsBlender {
    fn blend(
        &self,
        posts: Vec<FeedItem>,
        advertisements: Vec<FeedItem>,
        min_posts: usize,
    ) -> AdBlendResult {
        let post_count = posts.len();
        if advertisements.is_empty() || post_count < min_posts {
            return AdBlendResult {
                selected: posts,
                rejected: advertisements,
            };
        }

        let spacing = compute_spacing(&advertisements);
        let safe_count = posts
            .iter()
            .filter(|post| is_safe_for_adjacency(post))
            .count();
        let max_from_safe_posts = safe_count / 2;
        let expected_from_spacing = post_count
            .saturating_sub(1)
            .checked_div(spacing.requested)
            .unwrap_or(0);
        let actual_ads = advertisements
            .len()
            .min(expected_from_spacing)
            .min(max_from_safe_posts);

        if actual_ads == 0 {
            return AdBlendResult {
                selected: posts,
                rejected: advertisements,
            };
        }

        let (safe_posts, unsafe_posts): (Vec<_>, Vec<_>) =
            posts.into_iter().partition(is_safe_for_adjacency);
        let safe_post_count = safe_posts.len();
        let group_size = safe_post_count / actual_ads;
        let mut safe_posts = safe_posts.into_iter().map(Some).collect::<Vec<_>>();
        let mut triples = Vec::new();
        let mut rejected = Vec::new();
        let mut group_index = 0;

        for advertisement in advertisements {
            if group_index >= actual_ads {
                rejected.push(advertisement);
                continue;
            }

            let group_start = group_index * group_size;
            let above = safe_posts[group_start].as_ref();
            let below = safe_posts[group_start + 1].as_ref();
            let FeedItemContent::Advertisement(advertisement_data) = &advertisement.content else {
                rejected.push(advertisement);
                continue;
            };

            if should_drop_bsr_low(advertisement_data, above, below)
                || should_drop_handle(advertisement_data, above, below)
                || should_drop_keyword(advertisement_data, above, below)
            {
                rejected.push(advertisement);
                continue;
            }

            let above = safe_posts[group_start]
                .take()
                .expect("partition group always has an upper post");
            let below = safe_posts[group_start + 1]
                .take()
                .expect("partition group always has a lower post");
            triples.push((advertisement, above, below));
            group_index += 1;
        }

        if triples.is_empty() {
            let mut selected = safe_posts.into_iter().flatten().collect::<Vec<_>>();
            selected.extend(unsafe_posts);
            sort_posts_by_score(&mut selected);
            return AdBlendResult { selected, rejected };
        }

        let placed_ads = triples.len();
        let mut filler = Vec::with_capacity(
            safe_post_count
                .saturating_sub(2 * placed_ads)
                .saturating_add(unsafe_posts.len()),
        );
        filler.extend(safe_posts.into_iter().flatten());
        filler.extend(unsafe_posts);
        sort_posts_by_score(&mut filler);

        let filler_per_gap = filler.len() / placed_ads;
        let remainder = filler.len() % placed_ads;
        let mut filler = filler.into_iter();
        let mut selected = Vec::with_capacity(post_count + placed_ads);

        for (index, (advertisement, above, below)) in triples.into_iter().enumerate() {
            selected.push(above);
            selected.push(advertisement);
            selected.push(below);

            let filler_count =
                filler_per_gap + usize::from(index >= placed_ads.saturating_sub(remainder));
            for _ in 0..filler_count {
                if let Some(post) = filler.next() {
                    selected.push(post);
                }
            }
        }

        AdBlendResult { selected, rejected }
    }
}

fn sort_posts_by_score(posts: &mut [FeedItem]) {
    posts.sort_by(|left, right| {
        post_score(right)
            .partial_cmp(&post_score(left))
            .unwrap_or(Ordering::Equal)
    });
}

fn post_score(item: &FeedItem) -> f32 {
    match &item.content {
        FeedItemContent::Post(post) => post.score,
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use x_algorithm_proto::home_mixer::{BrandSafetyRiskLevel, BrandSafetyVerdict};

    fn blend(posts: Vec<FeedItem>, advertisements: Vec<FeedItem>) -> AdBlendResult {
        PartitionOrganicAdsBlender.blend(posts, advertisements, 5)
    }

    fn post_count(items: &[FeedItem]) -> usize {
        items
            .iter()
            .filter(|item| matches!(item.content, FeedItemContent::Post(_)))
            .count()
    }

    fn assert_no_avoid_neighbours(items: &[FeedItem]) {
        for (above, below) in ad_neighbour_verdicts(items) {
            assert_ne!(above, Some(BrandSafetyVerdict::AvoidAdjacency));
            assert_ne!(below, Some(BrandSafetyVerdict::AvoidAdjacency));
        }
    }

    fn assert_sensitive_ads_avoid_low_risk(items: &[FeedItem]) {
        let neighbours = ad_neighbour_verdicts(items);
        for (index, risk) in ad_risk_levels(items).into_iter().enumerate() {
            if risk != BrandSafetyRiskLevel::BsrLow {
                continue;
            }
            let (above, below) = neighbours[index];
            assert_ne!(above, Some(BrandSafetyVerdict::LowRisk));
            assert_ne!(below, Some(BrandSafetyVerdict::LowRisk));
        }
    }

    #[test]
    fn no_advertisements_returns_posts_unchanged() {
        let posts = (1..=3).map(safe_post).collect::<Vec<_>>();
        let result = blend(posts, Vec::new());

        assert_eq!(result.selected.len(), 3);
        assert_eq!(ad_count(&result.selected), 0);
    }

    #[test]
    fn no_posts_rejects_the_advertisement() {
        let result = blend(Vec::new(), vec![normal_ad(100)]);

        assert!(result.selected.is_empty());
        assert_eq!(result.rejected.len(), 1);
    }

    #[test]
    fn too_few_posts_skips_advertisements() {
        let posts = (1..=4).map(safe_post).collect::<Vec<_>>();
        let result = blend(posts, vec![normal_ad(100)]);

        assert_eq!(ad_count(&result.selected), 0);
        assert_eq!(result.rejected.len(), 1);
    }

    #[test]
    fn basic_blending_all_safe() {
        let posts = (1..=10).map(safe_post).collect::<Vec<_>>();
        let result = blend(posts, vec![normal_ad(100), normal_ad(200)]);

        assert!(ad_count(&result.selected) > 0);
        assert_no_avoid_neighbours(&result.selected);
    }

    #[test]
    fn sensitive_advertisement_not_adjacent_to_low_risk_post() {
        let mut posts = (1..=3).map(safe_post).collect::<Vec<_>>();
        posts.push(low_risk_post(4));
        posts.extend((5..=8).map(safe_post));
        let result = blend(posts, vec![sensitive_ad(100)]);

        assert_eq!(ad_count(&result.selected), 1);
        assert_sensitive_ads_avoid_low_risk(&result.selected);
    }

    #[test]
    fn normal_advertisement_can_be_adjacent_to_low_risk_post() {
        let mut posts = vec![low_risk_post(1), low_risk_post(2)];
        posts.extend((3..=6).map(safe_post));
        let result = blend(posts, vec![normal_ad(100)]);

        assert_eq!(ad_count(&result.selected), 1);
    }

    #[test]
    fn sensitive_advertisement_dropped_when_group_has_low_risk() {
        let mut posts = vec![low_risk_post(1)];
        posts.extend((2..=6).map(safe_post));
        let result = blend(posts, vec![sensitive_ad(100)]);

        assert_eq!(ad_count(&result.selected), 0);
        assert_eq!(labels(&result.rejected), ["ad-100"]);
    }

    #[test]
    fn sensitive_advertisement_dropped_when_no_safe_swap() {
        let posts = (1..=6).map(low_risk_post).collect::<Vec<_>>();
        let result = blend(posts, vec![sensitive_ad(100)]);

        assert_eq!(ad_count(&result.selected), 0);
        assert_eq!(result.selected.len(), 6);
    }

    #[test]
    fn mixed_risk_advertisements() {
        let mut posts = vec![safe_post(1), low_risk_post(2)];
        posts.extend((3..=10).map(safe_post));
        let result = blend(posts, vec![sensitive_ad(100), normal_ad(200)]);

        assert!(ad_count(&result.selected) >= 1);
        assert_no_avoid_neighbours(&result.selected);
        assert_sensitive_ads_avoid_low_risk(&result.selected);
    }

    #[test]
    fn all_low_risk_posts_drop_sensitive_and_keep_normal() {
        let posts = (1..=8).map(low_risk_post).collect::<Vec<_>>();
        let result = blend(posts, vec![sensitive_ad(100), normal_ad(200)]);

        assert_eq!(labels(&result.rejected), ["ad-100"]);
        assert!(labels(&result.selected).contains(&"ad-200".to_string()));
    }

    #[test]
    fn no_avoid_posts_places_both_advertisements() {
        let posts = vec![
            safe_post(1),
            safe_post(2),
            low_risk_post(3),
            safe_post(4),
            safe_post(5),
            low_risk_post(6),
            safe_post(7),
            safe_post(8),
        ];
        let result = blend(posts, vec![sensitive_ad(100), normal_ad(200)]);

        assert_eq!(ad_count(&result.selected), 2);
        assert_sensitive_ads_avoid_low_risk(&result.selected);
    }

    #[test]
    fn advertisement_never_first_or_last() {
        let posts = (1..=10).map(safe_post).collect::<Vec<_>>();
        let result = blend(posts, vec![sensitive_ad(100), normal_ad(200)]);

        assert!(matches!(
            result.selected.first().unwrap().content,
            FeedItemContent::Post(_)
        ));
        assert!(matches!(
            result.selected.last().unwrap().content,
            FeedItemContent::Post(_)
        ));
    }

    #[test]
    fn single_sensitive_advertisement_dropped_when_both_neighbours_low_risk() {
        let posts = (1..=6).map(low_risk_post).collect::<Vec<_>>();
        let result = blend(posts, vec![sensitive_ad(100)]);

        assert_eq!(ad_count(&result.selected), 0);
        assert_eq!(result.selected.len(), 6);
    }

    #[test]
    fn drop_sensitive_keep_normal_with_all_low_risk_posts() {
        let posts = (1..=10).map(low_risk_post).collect::<Vec<_>>();
        let advertisements = vec![sensitive_ad(100), normal_ad(200), sensitive_ad(300)];
        let result = blend(posts, advertisements);

        assert_eq!(
            labels(&result.selected)
                .iter()
                .filter(|l| l.starts_with("ad-"))
                .count(),
            1
        );
        assert_eq!(labels(&result.rejected), ["ad-100", "ad-300"]);
    }

    #[test]
    fn sensitive_advertisement_dropped_when_only_one_safe_post_available() {
        let mut posts = vec![safe_post(1)];
        posts.extend((2..=6).map(low_risk_post));
        let result = blend(posts, vec![sensitive_ad(100)]);

        assert_sensitive_ads_avoid_low_risk(&result.selected);
    }

    #[test]
    fn multiple_sensitive_advertisements_compete_for_safe_posts() {
        let mut posts = (1..=4).map(safe_post).collect::<Vec<_>>();
        posts.extend((5..=12).map(low_risk_post));
        let advertisements = vec![sensitive_ad(100), sensitive_ad(200), sensitive_ad(300)];
        let result = blend(posts, advertisements);

        let placed = ad_risk_levels(&result.selected)
            .into_iter()
            .filter(|risk| *risk == BrandSafetyRiskLevel::BsrLow)
            .count();
        assert!(placed <= 2, "at most 2 sensitive ads fit 4 safe posts");
        assert_sensitive_ads_avoid_low_risk(&result.selected);
    }

    #[test]
    fn sensitive_dropped_while_normal_still_placed() {
        let mut posts = vec![
            low_risk_post(1),
            safe_post(2),
            safe_post(3),
            safe_post(4),
            low_risk_post(5),
        ];
        posts.extend((6..=10).map(safe_post));
        let result = blend(posts, vec![sensitive_ad(100), normal_ad(200)]);

        assert_eq!(ad_count(&result.selected), 1);
        assert_eq!(labels(&result.rejected), ["ad-100"]);
    }

    #[test]
    fn all_advertisements_dropped_returns_posts_only() {
        let posts = (1..=8).map(low_risk_post).collect::<Vec<_>>();
        let advertisements = vec![sensitive_ad(100), sensitive_ad(200), sensitive_ad(300)];
        let result = blend(posts, advertisements);

        assert_eq!(ad_count(&result.selected), 0);
        assert_eq!(post_count(&result.selected), 8);
        assert_eq!(result.rejected.len(), 3);
    }

    #[test]
    fn dropped_advertisement_does_not_lose_posts() {
        let mut posts = vec![low_risk_post(1)];
        posts.extend((2..=10).map(safe_post));
        let result = blend(posts, vec![sensitive_ad(100), normal_ad(200)]);

        assert_eq!(post_count(&result.selected), 10);
    }

    #[test]
    fn mixed_verdicts_sensitive_advertisement_swap_chain() {
        let posts = vec![
            safe_post(1),
            low_risk_post(2),
            avoid_post(3),
            safe_post(4),
            safe_post(5),
            low_risk_post(6),
            safe_post(7),
            avoid_post(8),
            safe_post(9),
            safe_post(10),
            low_risk_post(11),
            safe_post(12),
            safe_post(13),
            avoid_post(14),
            safe_post(15),
        ];
        let advertisements = vec![
            sensitive_ad(100),
            normal_ad(200),
            sensitive_ad(300),
            normal_ad(400),
        ];
        let result = blend(posts, advertisements);

        assert_eq!(
            labels(&result.selected),
            [
                "post-1", "ad-200", "post-2", "post-3", "post-4", "post-7", "post-8", "post-9",
                "post-5", "ad-400", "post-6", "post-10", "post-11", "post-12", "post-13",
                "post-14", "post-15",
            ]
        );
        assert_eq!(labels(&result.rejected), ["ad-100", "ad-300"]);
    }

    #[test]
    fn dropped_advertisements_do_not_waste_a_group_slot() {
        let posts = vec![
            safe_post(1),
            low_risk_post(2),
            safe_post(3),
            low_risk_post(4),
            safe_post(5),
            low_risk_post(6),
            safe_post(7),
            low_risk_post(8),
            safe_post(9),
            low_risk_post(10),
        ];
        let advertisements = vec![
            sensitive_ad(100),
            sensitive_ad(200),
            sensitive_ad(300),
            sensitive_ad(400),
            normal_ad(500),
            normal_ad(600),
        ];
        let result = blend(posts, advertisements);

        assert_eq!(
            labels(&result.selected),
            [
                "post-1", "ad-500", "post-2", "post-3", "post-6", "post-7", "post-4", "ad-600",
                "post-5", "post-8", "post-9", "post-10",
            ]
        );
    }

    #[test]
    fn keyword_and_handle_drops_do_not_waste_a_group_slot() {
        let mut first = post_with_text(1, "this post contains badword in the text");
        if let FeedItemContent::Post(post) = &mut first.content {
            post.author_id = 9999;
        }
        let posts = vec![
            first,
            post_with_text(2, "just a normal safe post"),
            post_with_text(3, "another safe post here"),
            post_with_text(4, "nothing controversial"),
            post_with_text(5, "great weather today"),
            post_with_text(6, "loving this new recipe"),
            post_with_text(7, "happy birthday friend"),
            post_with_text(8, "what a beautiful sunset"),
            post_with_text(9, "just finished a workout"),
            post_with_text(10, "anyone watching the game"),
        ];
        let advertisements = vec![
            ad_item(keyword_ad(100, &["badword"])),
            ad_item(keyword_ad(200, &["badword"])),
            ad_item(keyword_ad(300, &["badword"])),
            ad_item(handle_ad(400, &[9999])),
            normal_ad(500),
            normal_ad(600),
        ];
        let result = blend(posts, advertisements);

        assert_eq!(
            labels(&result.selected),
            [
                "post-1", "ad-500", "post-2", "post-3", "post-6", "post-7", "post-4", "ad-600",
                "post-5", "post-8", "post-9", "post-10",
            ]
        );
    }

    #[test]
    fn large_timeline_stress() {
        let mut posts = (1..=20).map(safe_post).collect::<Vec<_>>();
        posts.extend((21..=25).map(low_risk_post));
        posts.extend((26..=35).map(avoid_post));

        let mut advertisements = (0..4).map(|i| sensitive_ad(100 + i)).collect::<Vec<_>>();
        advertisements.extend((0..4).map(|i| normal_ad(200 + i)));

        let result = blend(posts, advertisements);

        assert_eq!(post_count(&result.selected), 35);
        assert!(matches!(
            result.selected.first().unwrap().content,
            FeedItemContent::Post(_)
        ));
        assert!(matches!(
            result.selected.last().unwrap().content,
            FeedItemContent::Post(_)
        ));
        assert_no_avoid_neighbours(&result.selected);
        assert_sensitive_ads_avoid_low_risk(&result.selected);
    }

    #[test]
    fn high_risk_advertisement_sits_next_to_safe_not_avoid() {
        let mut posts = (1..=6).map(avoid_post).collect::<Vec<_>>();
        posts.extend((7..=12).map(safe_post));
        let result = blend(posts, vec![bsr_high_ad(100)]);

        assert_eq!(ad_count(&result.selected), 1);
        assert_no_avoid_neighbours(&result.selected);
        assert_eq!(
            ad_risk_levels(&result.selected),
            vec![BrandSafetyRiskLevel::BsrHigh]
        );
    }

    #[test]
    fn high_risk_advertisement_dropped_when_only_avoid_posts() {
        let posts = (1..=8).map(avoid_post).collect::<Vec<_>>();
        let result = blend(posts, vec![bsr_high_ad(100)]);

        assert_eq!(ad_count(&result.selected), 0);
        assert_eq!(result.rejected.len(), 1);
    }
}
