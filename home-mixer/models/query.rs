use crate::feed_state::{FeedStateSnapshot, FeedStateStore};
use crate::models::candidate::PostCandidate;
use crate::models::ids::{PostId, UserId};
use crate::models::user_features::UserFeatures;
use crate::visibility::vf_client::{GetTwitterContextViewer, TwitterContextViewer};
use std::sync::Arc;
use tokio::sync::OnceCell;
use x_algorithm_proto::home_mixer::ImpressionBloomFilterEntry;
use xai_candidate_pipeline::candidate_pipeline::HasRequestId;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TopicRecallMode {
    #[default]
    None,
    Strict,
    ColdStart,
    Blend,
}

#[derive(Clone, Default, Debug)]
pub struct ScoredPostsQuery {
    pub user_id: UserId,
    pub client_app_id: i32,
    pub country_code: String,
    pub language_code: String,
    pub seen_ids: Vec<PostId>,
    pub served_ids: Vec<PostId>,
    pub in_network_only: bool,
    pub is_bottom_request: bool,
    pub bloom_filter_entries: Vec<ImpressionBloomFilterEntry>,
    pub user_action_sequence: Option<x_algorithm_proto::recsys::UserActionSequence>,
    pub retrieval_sequence: Option<x_algorithm_proto::recsys::UserActionSequence>,
    pub scoring_sequence: Option<x_algorithm_proto::recsys::UserActionSequence>,
    pub user_features: UserFeatures,
    /// Set by the Strato-backed relation hydrators after a successful read.
    /// Admission filters fail closed when this is still false, so an
    /// unimplemented or timed-out `GetViewerRelations` cannot look like
    /// "this viewer blocked nobody".
    pub viewer_relations_hydrated: bool,
    pub cached_posts: Vec<PostCandidate>,
    pub has_cached_posts: bool,
    pub topic_ids: Vec<i64>,
    pub excluded_topic_ids: Vec<i64>,
    /// 公开 proto 的新用户冷启动话题；保持上游限定召回语义。
    pub new_user_topic_ids: Vec<i64>,
    /// 仅由显式注入的 Adapter 生成，用于首页补充召回。
    pub supplemental_topic_ids: Vec<i64>,
    pub exclude_videos: bool,
    pub enable_phoenix_moe: bool,
    pub impressed_post_ids: Vec<PostId>,
    pub past_request_timestamps_ms: Vec<i64>,
    pub is_preview: bool,
    pub is_shadow_traffic: bool,
    pub is_top_request: bool,
    pub is_polling: bool,
    pub ip_address: String,
    pub user_agent: String,
    pub request_id: String,
    pub prediction_id: u64,
    pub request_time_ms: i64,
    /// A single FeedState read shared by every pipeline participating in one
    /// business request. Entry points replace this cell for each invocation.
    #[doc(hidden)]
    pub feed_state_snapshot: Arc<OnceCell<Result<FeedStateSnapshot, String>>>,
}

impl ScoredPostsQuery {
    pub(crate) fn start_request(mut self) -> Self {
        self.feed_state_snapshot = Arc::new(OnceCell::new());
        self
    }

    pub(crate) async fn load_feed_state(
        &self,
        store: &dyn FeedStateStore,
    ) -> Result<&FeedStateSnapshot, String> {
        self.feed_state_snapshot
            .get_or_init(|| async { store.load(self.user_id).await })
            .await
            .as_ref()
            .map_err(Clone::clone)
    }

    pub fn topic_recall_mode(&self) -> TopicRecallMode {
        if !self.topic_ids.is_empty() {
            return TopicRecallMode::Strict;
        }
        if !self.new_user_topic_ids.is_empty() {
            return TopicRecallMode::ColdStart;
        }
        if !self.supplemental_topic_ids.is_empty() {
            return TopicRecallMode::Blend;
        }
        TopicRecallMode::None
    }

    pub fn selected_topic_ids(&self) -> &[i64] {
        match self.topic_recall_mode() {
            TopicRecallMode::Strict => &self.topic_ids,
            TopicRecallMode::ColdStart => &self.new_user_topic_ids,
            TopicRecallMode::Blend => &self.supplemental_topic_ids,
            TopicRecallMode::None => &[],
        }
    }
}

impl GetTwitterContextViewer for ScoredPostsQuery {
    fn get_viewer(&self) -> Option<TwitterContextViewer> {
        Some(TwitterContextViewer {
            user_id: self.user_id,
            client_application_id: i64::from(self.client_app_id),
            request_country_code: self.country_code.clone(),
            request_language_code: self.language_code.clone(),
        })
    }
}

impl HasRequestId for ScoredPostsQuery {
    fn request_id(&self) -> &str {
        &self.request_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requested_topics_select_strict_recall_over_other_topic_origins() {
        let query = ScoredPostsQuery {
            topic_ids: vec![10],
            new_user_topic_ids: vec![20],
            supplemental_topic_ids: vec![30],
            ..Default::default()
        };

        assert_eq!(query.topic_recall_mode(), TopicRecallMode::Strict);
        assert_eq!(query.selected_topic_ids(), &[10]);
    }

    #[test]
    fn new_user_topics_select_cold_start_recall() {
        let query = ScoredPostsQuery {
            new_user_topic_ids: vec![20],
            ..Default::default()
        };

        assert_eq!(query.topic_recall_mode(), TopicRecallMode::ColdStart);
        assert_eq!(query.selected_topic_ids(), &[20]);
    }

    #[test]
    fn supplemental_topics_select_blended_recall() {
        let query = ScoredPostsQuery {
            supplemental_topic_ids: vec![30],
            ..Default::default()
        };

        assert_eq!(query.topic_recall_mode(), TopicRecallMode::Blend);
        assert_eq!(query.selected_topic_ids(), &[30]);
    }

    #[test]
    fn empty_topics_disable_topic_recall() {
        let query = ScoredPostsQuery::default();

        assert_eq!(query.topic_recall_mode(), TopicRecallMode::None);
        assert!(query.selected_topic_ids().is_empty());
    }
}
