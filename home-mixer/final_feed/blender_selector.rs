use super::feed_item::{FeedItem, FeedItemContent, FeedItemKind};
use crate::candidate_pipeline::query::ScoredPostsQuery;
use x_algorithm_proto::home_mixer::BrandSafetyVerdict;
use xai_candidate_pipeline::selector::{SelectResult, Selector};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AdsBlenderStrategy {
    #[default]
    Disabled,
    SafeGap,
    PartitionOrganic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlenderConfig {
    pub prompt_position: usize,
    pub who_to_follow_position: usize,
    pub max_items: usize,
    pub ads_strategy: AdsBlenderStrategy,
    pub min_posts_for_ads: usize,
    pub min_organic_gap: usize,
}

impl Default for BlenderConfig {
    fn default() -> Self {
        Self {
            prompt_position: 0,
            who_to_follow_position: 10,
            max_items: crate::params::RESULT_SIZE,
            ads_strategy: AdsBlenderStrategy::Disabled,
            min_posts_for_ads: 5,
            min_organic_gap: 2,
        }
    }
}

pub struct BlenderSelector {
    config: BlenderConfig,
}

impl BlenderSelector {
    pub fn new(config: BlenderConfig) -> Self {
        Self { config }
    }

    pub fn blend(&self, candidates: Vec<FeedItem>) -> SelectResult<FeedItem> {
        let mut posts = Vec::new();
        let mut advertisements = Vec::new();
        let mut prompts = Vec::new();
        let mut who_to_follow = Vec::new();
        let mut push_to_home = Vec::new();

        for candidate in candidates {
            match candidate.content {
                FeedItemContent::Post(_) => posts.push(candidate),
                FeedItemContent::Advertisement(_) => advertisements.push(candidate),
                FeedItemContent::Prompt(_) => prompts.push(candidate),
                FeedItemContent::WhoToFollow(_) => who_to_follow.push(candidate),
                FeedItemContent::PushToHome(_) => push_to_home.push(candidate),
            }
        }

        let (mut selected, mut non_selected) = match self.config.ads_strategy {
            AdsBlenderStrategy::Disabled => (posts, advertisements),
            AdsBlenderStrategy::SafeGap => self.blend_safe_gaps(posts, advertisements),
            AdsBlenderStrategy::PartitionOrganic => {
                self.blend_partition_organic(posts, advertisements)
            }
        };

        for (offset, prompt) in prompts.into_iter().enumerate() {
            let position = (self.config.prompt_position + offset).min(selected.len());
            selected.insert(position, prompt);
        }

        if let Some(module) = who_to_follow.first().cloned() {
            let position = self.config.who_to_follow_position.min(selected.len());
            selected.insert(position, module);
        }
        non_selected.extend(who_to_follow.into_iter().skip(1));

        if let Some(item) = push_to_home.first().cloned() {
            selected.insert(0, item);
        }
        non_selected.extend(push_to_home.into_iter().skip(1));

        let overflow = selected.split_off(self.config.max_items.min(selected.len()));
        non_selected.extend(overflow);
        if selected.last().map(FeedItem::kind) == Some(FeedItemKind::Advertisement) {
            if let Some(advertisement) = selected.pop() {
                non_selected.push(advertisement);
            }
        }
        for (position, item) in selected.iter_mut().enumerate() {
            item.position = position;
        }

        SelectResult {
            selected,
            non_selected,
        }
    }

    fn blend_safe_gaps(
        &self,
        posts: Vec<FeedItem>,
        advertisements: Vec<FeedItem>,
    ) -> (Vec<FeedItem>, Vec<FeedItem>) {
        if posts.len() < self.config.min_posts_for_ads {
            return (posts, advertisements);
        }

        let mut available_gaps = (1..posts.len())
            .filter(|gap| is_ad_safe_post(&posts[gap - 1]) && is_ad_safe_post(&posts[*gap]))
            .collect::<Vec<_>>();
        let mut placements = Vec::new();
        let mut rejected = Vec::new();

        for advertisement in advertisements {
            let requested = requested_ad_position(&advertisement);
            let min_position = placements
                .last()
                .map(|(gap, _)| gap + self.config.min_organic_gap)
                .unwrap_or(1);
            let Some((index, gap)) = closest_gap(&available_gaps, requested, min_position) else {
                rejected.push(advertisement);
                continue;
            };
            available_gaps.remove(index);
            placements.push((gap, advertisement));
        }
        placements.sort_by_key(|(gap, _)| *gap);

        (interleave_at_gaps(posts, placements), rejected)
    }

    fn blend_partition_organic(
        &self,
        posts: Vec<FeedItem>,
        advertisements: Vec<FeedItem>,
    ) -> (Vec<FeedItem>, Vec<FeedItem>) {
        if posts.len() < self.config.min_posts_for_ads {
            return (posts, advertisements);
        }

        let mut available_gaps = (1..posts.len())
            .filter(|gap| is_ad_safe_post(&posts[gap - 1]) && is_ad_safe_post(&posts[*gap]))
            .collect::<Vec<_>>();
        let requested_count = advertisements.len();
        let target_spacing = posts.len() / requested_count.saturating_add(1);
        let mut placements = Vec::new();
        let mut rejected = Vec::new();
        for (index, advertisement) in advertisements.into_iter().enumerate() {
            let target = (index + 1) * target_spacing;
            let minimum = placements
                .last()
                .map(|(gap, _)| gap + self.config.min_organic_gap)
                .unwrap_or(1);
            let Some((gap_index, gap)) = closest_gap(&available_gaps, target, minimum) else {
                rejected.push(advertisement);
                continue;
            };
            available_gaps.remove(gap_index);
            placements.push((gap, advertisement));
        }
        placements.sort_by_key(|(gap, _)| *gap);
        (interleave_at_gaps(posts, placements), rejected)
    }
}

fn interleave_at_gaps(posts: Vec<FeedItem>, placements: Vec<(usize, FeedItem)>) -> Vec<FeedItem> {
    let mut placement_iter = placements.into_iter().peekable();
    let mut blended = Vec::with_capacity(posts.len() + placement_iter.len());
    for (post_index, post) in posts.into_iter().enumerate() {
        while matches!(placement_iter.peek(), Some((gap, _)) if *gap == post_index) {
            if let Some((_, advertisement)) = placement_iter.next() {
                blended.push(advertisement);
            }
        }
        blended.push(post);
    }
    blended
}

fn is_ad_safe_post(item: &FeedItem) -> bool {
    let FeedItemContent::Post(post) = &item.content else {
        return false;
    };
    BrandSafetyVerdict::try_from(post.brand_safety_verdict)
        .is_ok_and(|verdict| verdict == BrandSafetyVerdict::SafeForAdjacency)
}

fn requested_ad_position(item: &FeedItem) -> usize {
    match &item.content {
        FeedItemContent::Advertisement(advertisement) => advertisement.requested_position,
        _ => 0,
    }
}

fn closest_gap(gaps: &[usize], requested: usize, minimum: usize) -> Option<(usize, usize)> {
    gaps.iter()
        .copied()
        .enumerate()
        .filter(|(_, gap)| *gap >= minimum)
        .min_by_key(|(_, gap)| gap.abs_diff(requested))
}

impl Selector<ScoredPostsQuery, FeedItem> for BlenderSelector {
    fn select(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<FeedItem>,
    ) -> SelectResult<FeedItem> {
        self.blend(candidates)
    }

    fn score(&self, _candidate: &FeedItem) -> f64 {
        0.0
    }
}
