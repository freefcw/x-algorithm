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
