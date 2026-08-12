use super::feed_item::{FeedItem, FeedItemContent, FeedItemKind};
use crate::ads::{AdBlendResult, AdsBlender, PartitionOrganicAdsBlender, SafeGapAdsBlender};
use crate::models::query::ScoredPostsQuery;
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
}

impl Default for BlenderConfig {
    fn default() -> Self {
        Self {
            prompt_position: 0,
            who_to_follow_position: 10,
            max_items: crate::params::RESULT_SIZE,
            ads_strategy: AdsBlenderStrategy::Disabled,
            min_posts_for_ads: 5,
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

        let AdBlendResult {
            mut selected,
            rejected: mut non_selected,
        } = match self.config.ads_strategy {
            AdsBlenderStrategy::Disabled => AdBlendResult {
                selected: posts,
                rejected: advertisements,
            },
            AdsBlenderStrategy::SafeGap => {
                SafeGapAdsBlender.blend(posts, advertisements, self.config.min_posts_for_ads)
            }
            AdsBlenderStrategy::PartitionOrganic => PartitionOrganicAdsBlender.blend(
                posts,
                advertisements,
                self.config.min_posts_for_ads,
            ),
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
