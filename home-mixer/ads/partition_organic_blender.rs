use super::util::{
    compute_spacing, is_safe_for_adjacency, should_drop_bsr_low, should_drop_handle,
    should_drop_keyword,
};
use super::{AdBlendResult, AdsBlender};
use crate::final_feed::{FeedItem, FeedItemContent};
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
