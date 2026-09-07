mod partition_organic_blender;
mod safe_gap_blender;
#[cfg(test)]
mod test_support;
pub(crate) mod util;

pub(crate) use partition_organic_blender::PartitionOrganicAdsBlender;
pub(crate) use safe_gap_blender::SafeGapAdsBlender;

use crate::models::feed_item::FeedItem;

pub(crate) struct AdBlendResult {
    pub(crate) selected: Vec<FeedItem>,
    pub(crate) rejected: Vec<FeedItem>,
}

pub(crate) trait AdsBlender {
    fn blend(
        &self,
        posts: Vec<FeedItem>,
        advertisements: Vec<FeedItem>,
        min_posts: usize,
    ) -> AdBlendResult;
}
