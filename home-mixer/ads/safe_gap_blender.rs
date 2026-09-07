use super::util::{
    compute_spacing, find_safe_gaps, interleave_at_gaps, AdSpacing, DEFAULT_SPACING,
};
use super::{AdBlendResult, AdsBlender};
use crate::models::feed_item::FeedItem;

pub(crate) struct SafeGapAdsBlender;

impl AdsBlender for SafeGapAdsBlender {
    fn blend(
        &self,
        posts: Vec<FeedItem>,
        mut advertisements: Vec<FeedItem>,
        min_posts: usize,
    ) -> AdBlendResult {
        if advertisements.is_empty() || posts.len() < min_posts {
            return AdBlendResult {
                selected: posts,
                rejected: advertisements,
            };
        }

        let safe_gaps = find_safe_gaps(&posts);
        let spacing = compute_spacing(&advertisements);
        let first_ideal = super::util::requested_ad_position(&advertisements[0]);
        let placements = assign_ads_to_gaps(&safe_gaps, advertisements.len(), spacing, first_ideal);
        let rejected = advertisements.split_off(placements.len());
        let selected = interleave_at_gaps(posts, advertisements, &placements);

        AdBlendResult { selected, rejected }
    }
}

fn assign_ads_to_gaps(
    safe_gaps: &[usize],
    advertisement_count: usize,
    spacing: AdSpacing,
    first_ideal: usize,
) -> Vec<usize> {
    let mut placements: Vec<usize> = Vec::new();
    let mut search_from = 0;
    let mut previous_ideal = first_ideal;

    for _ in 0..advertisement_count {
        if search_from >= safe_gaps.len() {
            break;
        }

        let (ideal, minimum) = match placements.last() {
            None => (first_ideal, 1),
            Some(&last_actual) => {
                let ideal = previous_ideal.saturating_add(spacing.requested);
                let minimum = previous_ideal
                    .saturating_add(spacing.min)
                    .max(last_actual.saturating_add(DEFAULT_SPACING.min));
                (ideal, minimum)
            }
        };

        let Some((offset, gap)) = find_best_gap(&safe_gaps[search_from..], ideal, minimum) else {
            break;
        };

        placements.push(gap);
        search_from += offset + 1;
        previous_ideal = ideal;
    }

    placements
}

fn find_best_gap(gaps: &[usize], ideal: usize, minimum: usize) -> Option<(usize, usize)> {
    let minimum_offset = gaps.partition_point(|gap| *gap < minimum);
    if minimum_offset >= gaps.len() {
        return None;
    }

    let candidates = &gaps[minimum_offset..];
    let ideal_position = candidates.partition_point(|gap| *gap < ideal);
    let chosen = if ideal_position >= candidates.len() {
        candidates.len() - 1
    } else if ideal_position == 0 {
        0
    } else {
        let below = candidates[ideal_position - 1];
        let above = candidates[ideal_position];
        if ideal - below <= above - ideal {
            ideal_position - 1
        } else {
            ideal_position
        }
    };

    Some((minimum_offset + chosen, candidates[chosen]))
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    const STANDARD: AdSpacing = AdSpacing {
        requested: 3,
        min: 2,
    };

    fn blend(posts: Vec<FeedItem>, advertisements: Vec<FeedItem>) -> AdBlendResult {
        SafeGapAdsBlender.blend(posts, advertisements, 0)
    }

    #[test]
    fn find_best_gap_exact_match() {
        assert_eq!(find_best_gap(&[3, 5, 7, 9], 7, 5), Some((2, 7)));
    }

    #[test]
    fn find_best_gap_prefers_closer_below() {
        assert_eq!(find_best_gap(&[5, 7, 10], 8, 5), Some((1, 7)));
    }

    #[test]
    fn find_best_gap_prefers_closer_above() {
        assert_eq!(find_best_gap(&[5, 10], 8, 5), Some((1, 10)));
    }

    #[test]
    fn find_best_gap_all_below_ideal() {
        assert_eq!(find_best_gap(&[5, 7, 9], 20, 5), Some((2, 9)));
    }

    #[test]
    fn find_best_gap_all_above_ideal() {
        assert_eq!(find_best_gap(&[5, 7, 9], 3, 5), Some((0, 5)));
    }

    #[test]
    fn find_best_gap_none_above_min() {
        assert_eq!(find_best_gap(&[1, 2, 3], 8, 5), None);
    }

    #[test]
    fn find_best_gap_empty() {
        assert_eq!(find_best_gap(&[], 5, 3), None);
    }

    #[test]
    fn assign_targets_ideal_spacing() {
        let gaps = (1..=9).collect::<Vec<_>>();
        assert_eq!(assign_ads_to_gaps(&gaps, 3, STANDARD, 1), vec![1, 4, 7]);
    }

    #[test]
    fn assign_falls_back_to_min_when_ideal_blocked() {
        assert_eq!(
            assign_ads_to_gaps(&[1, 2, 3, 5, 6, 7, 8, 9], 3, STANDARD, 1),
            vec![1, 3, 7]
        );
    }

    #[test]
    fn assign_wider_spacing_targets_ideal() {
        let gaps = (1..=20).collect::<Vec<_>>();
        let wide = AdSpacing {
            requested: 7,
            min: 4,
        };
        assert_eq!(assign_ads_to_gaps(&gaps, 3, wide, 1), vec![1, 8, 15]);
    }

    #[test]
    fn assign_limited_by_advertisement_count() {
        assert_eq!(assign_ads_to_gaps(&[1, 2, 3, 4], 1, STANDARD, 1), vec![1]);
    }

    #[test]
    fn assign_no_safe_gaps() {
        assert_eq!(assign_ads_to_gaps(&[], 3, STANDARD, 1), Vec::<usize>::new());
    }

    #[test]
    fn assign_sparse_gaps_beyond_ideal() {
        assert_eq!(
            assign_ads_to_gaps(&[1, 10, 20], 3, STANDARD, 1),
            vec![1, 10, 20]
        );
    }

    #[test]
    fn no_advertisements_returns_posts_unchanged() {
        let posts = (1..=3).map(safe_post).collect::<Vec<_>>();
        let result = SafeGapAdsBlender.blend(posts, Vec::new(), 5);

        assert_eq!(labels(&result.selected), ["post-1", "post-2", "post-3"]);
        assert!(result.rejected.is_empty());
    }

    #[test]
    fn no_posts_rejects_every_advertisement() {
        let result = SafeGapAdsBlender.blend(Vec::new(), vec![normal_ad(100), normal_ad(200)], 5);

        assert!(result.selected.is_empty());
        assert_eq!(result.rejected.len(), 2);
    }

    #[test]
    fn too_few_posts_skips_advertisements() {
        let posts = (1..=4).map(safe_post).collect::<Vec<_>>();
        let result = SafeGapAdsBlender.blend(posts, vec![normal_ad(100), normal_ad(200)], 5);

        assert_eq!(result.selected.len(), 4);
        assert_eq!(ad_count(&result.selected), 0);
        assert_eq!(result.rejected.len(), 2);
    }

    #[test]
    fn advertisements_placed_at_ideal_spacing() {
        let posts = (1..=7).map(safe_post).collect::<Vec<_>>();
        let result = blend(posts, vec![normal_ad_at(100, 1), normal_ad_at(200, 4)]);

        assert_eq!(result.selected.len(), 9);
        assert_eq!(ad_indices(&result.selected), vec![1, 5]);
        assert!(result.rejected.is_empty());
    }

    #[test]
    fn advertisement_never_at_the_first_slot() {
        let posts = (1..=3).map(safe_post).collect::<Vec<_>>();
        let result = blend(posts, vec![normal_ad(100)]);

        assert_eq!(ad_indices(&result.selected), vec![1]);
    }

    #[test]
    fn advertisement_not_placed_next_to_avoid_post() {
        let posts = vec![safe_post(1), avoid_post(2), safe_post(3), safe_post(4)];
        let result = blend(posts, vec![normal_ad(100)]);

        assert_eq!(ad_indices(&result.selected), vec![3]);
    }

    #[test]
    fn avoid_at_start_pushes_advertisement_down() {
        let posts = vec![avoid_post(1), safe_post(2), safe_post(3)];
        let result = blend(posts, vec![normal_ad(100)]);

        assert_eq!(ad_indices(&result.selected), vec![2]);
    }

    #[test]
    fn avoid_at_end_still_allows_earlier_advertisements() {
        let posts = vec![safe_post(1), safe_post(2), avoid_post(3)];
        let result = blend(posts, vec![normal_ad(100)]);

        assert_eq!(ad_indices(&result.selected), vec![1]);
    }

    #[test]
    fn all_posts_avoid_rejects_all_advertisements() {
        let posts = (1..=3).map(avoid_post).collect::<Vec<_>>();
        let result = blend(posts, vec![normal_ad(100), normal_ad(200)]);

        assert_eq!(result.selected.len(), 3);
        assert_eq!(ad_count(&result.selected), 0);
        assert_eq!(result.rejected.len(), 2);
    }

    #[test]
    fn ideal_spacing_with_many_advertisements() {
        let posts = (1..=10).map(safe_post).collect::<Vec<_>>();
        let advertisements = vec![
            normal_ad_at(100, 1),
            normal_ad_at(200, 4),
            normal_ad_at(300, 7),
        ];
        let result = blend(posts, advertisements);

        assert_eq!(result.selected.len(), 13);
        let indices = ad_indices(&result.selected);
        assert_eq!(indices, vec![1, 5, 9]);
        for window in indices.windows(2) {
            assert_eq!(window[1] - window[0] - 1, 3);
        }
    }

    #[test]
    fn excess_advertisements_are_rejected_from_the_bottom() {
        let posts = (1..=5).map(safe_post).collect::<Vec<_>>();
        let advertisements = vec![
            normal_ad_at(100, 1),
            normal_ad_at(200, 4),
            normal_ad_at(300, 7),
            normal_ad_at(400, 10),
        ];
        let result = blend(posts, advertisements);

        assert_eq!(ad_count(&result.selected), 2);
        assert_eq!(labels(&result.rejected), ["ad-300", "ad-400"]);
    }

    #[test]
    fn two_avoid_posts_create_isolated_safe_zones() {
        let posts = vec![
            safe_post(1),
            avoid_post(2),
            safe_post(3),
            safe_post(4),
            safe_post(5),
            avoid_post(6),
            safe_post(7),
            safe_post(8),
        ];
        let result = blend(posts, vec![normal_ad_at(100, 1), normal_ad_at(200, 4)]);

        assert_eq!(ad_indices(&result.selected), vec![3, 8]);
    }

    #[test]
    fn same_requested_positions_use_default_spacing() {
        let posts = (1..=7).map(safe_post).collect::<Vec<_>>();
        let result = blend(posts, vec![normal_ad_at(100, 1), normal_ad_at(200, 1)]);

        assert_eq!(ad_indices(&result.selected), vec![1, 5]);
    }

    #[test]
    fn wide_spacing_targets_ideal_gap() {
        let posts = (1..=20).map(safe_post).collect::<Vec<_>>();
        let advertisements = vec![
            normal_ad_at(100, 1),
            normal_ad_at(200, 8),
            normal_ad_at(300, 15),
        ];
        let result = blend(posts, advertisements);

        let indices = ad_indices(&result.selected);
        assert_eq!(indices, vec![1, 9, 17]);
        for window in indices.windows(2) {
            assert_eq!(window[1] - window[0] - 1, 7);
        }
    }

    #[test]
    fn avoid_expands_then_recovers_ideal() {
        let mut posts = (1..=13).map(safe_post).collect::<Vec<_>>();
        posts[3] = avoid_post(4);
        let advertisements = vec![
            normal_ad_at(100, 1),
            normal_ad_at(200, 4),
            normal_ad_at(300, 7),
        ];
        let result = blend(posts, advertisements);

        assert_eq!(ad_indices(&result.selected), vec![1, 6, 9]);
    }

    #[test]
    fn tight_requested_positions_fall_back_to_default_spacing() {
        let posts = (1..=10).map(safe_post).collect::<Vec<_>>();
        let advertisements = vec![
            normal_ad_at(100, 1),
            normal_ad_at(200, 2),
            normal_ad_at(300, 3),
        ];
        let result = blend(posts, advertisements);

        assert_eq!(ad_indices(&result.selected), vec![1, 5, 9]);
    }

    #[test]
    fn all_zero_requested_positions_fall_back_to_default_spacing() {
        let posts = (1..=10).map(safe_post).collect::<Vec<_>>();
        let advertisements = vec![normal_ad(100), normal_ad(200), normal_ad(300)];
        let result = blend(posts, advertisements);

        assert_eq!(ad_indices(&result.selected), vec![1, 4, 8]);
    }

    #[test]
    fn gap_of_two_falls_back_to_default_spacing_end_to_end() {
        let posts = (1..=10).map(safe_post).collect::<Vec<_>>();
        let advertisements = vec![
            normal_ad_at(100, 1),
            normal_ad_at(200, 3),
            normal_ad_at(300, 5),
        ];
        let result = blend(posts, advertisements);

        assert_eq!(ad_indices(&result.selected), vec![1, 5, 9]);
    }

    #[test]
    fn never_fewer_than_two_posts_between_advertisements() {
        let posts = (1..=30).map(safe_post).collect::<Vec<_>>();
        let advertisements = (0..10)
            .map(|index| normal_ad_at(100 + index, index as usize))
            .collect::<Vec<_>>();
        let result = blend(posts, advertisements);

        for window in ad_indices(&result.selected).windows(2) {
            let between = window[1] - window[0] - 1;
            assert!(between >= 2, "only {between} posts between advertisements");
        }
    }
}
