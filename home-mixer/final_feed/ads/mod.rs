mod partition_organic_blender;
mod safe_gap_blender;
pub(super) mod util;

pub(super) use partition_organic_blender::PartitionOrganicAdsBlender;
pub(super) use safe_gap_blender::SafeGapAdsBlender;

use super::feed_item::FeedItem;

pub(super) struct AdBlendResult {
    pub(super) selected: Vec<FeedItem>,
    pub(super) rejected: Vec<FeedItem>,
}

pub(super) trait AdsBlender {
    fn blend(
        &self,
        posts: Vec<FeedItem>,
        advertisements: Vec<FeedItem>,
        min_posts: usize,
    ) -> AdBlendResult;
}
