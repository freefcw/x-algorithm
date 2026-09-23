use crate::feed_state::{FeedStateSnapshot, FeedStateStore};
use crate::id::{IdentityContext, IdentityRegistrationContext, PaddedIdentityResolver};
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

#[derive(Clone, Debug)]
pub(crate) struct QueryIdentity {
    context: Arc<IdentityContext>,
    registration: Arc<IdentityRegistrationContext>,
}

impl QueryIdentity {
    fn new(context: Arc<IdentityContext>, registration: Arc<IdentityRegistrationContext>) -> Self {
        assert!(
            Arc::ptr_eq(&context, &registration.reader()),
            "query identity context and registration must share one request context"
        );
        Self {
            context,
            registration,
        }
    }
}

#[derive(Clone, Debug)]
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
    /// Request-local identity capabilities kept together so read and
    /// registration views cannot drift apart.
    pub(crate) identity: QueryIdentity,
    /// A single FeedState read shared by every pipeline participating in one
    /// business request. Entry points replace this cell for each invocation.
    #[doc(hidden)]
    pub feed_state_snapshot: Arc<OnceCell<Result<FeedStateSnapshot, String>>>,
}

impl ScoredPostsQuery {
    /// Explicit fixture constructor for unit and integration tests.
    pub fn test_default() -> Self {
        let resolver = Arc::new(PaddedIdentityResolver::new());
        let identity_context = Arc::new(IdentityContext::new(resolver.clone() as _));
        let identity_registration = Arc::new(IdentityRegistrationContext::new(
            Arc::clone(&identity_context),
            resolver,
        ));
        Self::new_with_identity(identity_context, identity_registration)
    }

    pub fn new_with_identity(
        identity_context: Arc<IdentityContext>,
        identity_registration: Arc<IdentityRegistrationContext>,
    ) -> Self {
        Self {
            user_id: 0,
            client_app_id: 0,
            country_code: String::new(),
            language_code: String::new(),
            seen_ids: Vec::new(),
            served_ids: Vec::new(),
            in_network_only: false,
            is_bottom_request: false,
            bloom_filter_entries: Vec::new(),
            user_action_sequence: None,
            retrieval_sequence: None,
            scoring_sequence: None,
            user_features: UserFeatures::default(),
            viewer_relations_hydrated: false,
            cached_posts: Vec::new(),
            has_cached_posts: false,
            topic_ids: Vec::new(),
            excluded_topic_ids: Vec::new(),
            new_user_topic_ids: Vec::new(),
            supplemental_topic_ids: Vec::new(),
            exclude_videos: false,
            enable_phoenix_moe: false,
            impressed_post_ids: Vec::new(),
            past_request_timestamps_ms: Vec::new(),
            is_preview: false,
            is_shadow_traffic: false,
            is_top_request: false,
            is_polling: false,
            ip_address: String::new(),
            user_agent: String::new(),
            request_id: String::new(),
            prediction_id: 0,
            request_time_ms: 0,
            identity: QueryIdentity::new(identity_context, identity_registration),
            feed_state_snapshot: Arc::new(OnceCell::new()),
        }
    }
}

impl ScoredPostsQuery {
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = request_id.into();
        self
    }

    pub fn with_user_id(mut self, user_id: UserId) -> Self {
        self.user_id = user_id;
        self
    }

    pub fn with_prediction_id(mut self, prediction_id: u64) -> Self {
        self.prediction_id = prediction_id;
        self
    }

    pub fn with_request_time_ms(mut self, request_time_ms: i64) -> Self {
        self.request_time_ms = request_time_ms;
        self
    }

    pub(crate) fn start_request(mut self) -> Self {
        self.feed_state_snapshot = Arc::new(OnceCell::new());
        self
    }

    /// Attach the request-scoped identity capabilities as one invariant unit.
    ///
    /// Production queries should enter through this method so the read cache
    /// and registration capability cannot silently point at different
    /// `IdentityContext` instances. Tests should start from `test_default`.
    pub(crate) fn with_request_identity(
        mut self,
        identity_context: Arc<IdentityContext>,
        identity_registration: Arc<IdentityRegistrationContext>,
    ) -> Self {
        assert!(
            Arc::ptr_eq(&identity_context, &identity_registration.reader()),
            "identity reader and registration capability must share one request context"
        );
        self.identity = QueryIdentity::new(identity_context, identity_registration);
        self
    }

    pub(crate) async fn load_feed_state(
        &self,
        store: &dyn FeedStateStore,
    ) -> Result<&FeedStateSnapshot, String> {
        self.feed_state_snapshot
            .get_or_init(|| async {
                store
                    .load_with_identity(self.user_id, Arc::clone(&self.identity.context))
                    .await
            })
            .await
            .as_ref()
            .map_err(Clone::clone)
    }

    pub(crate) fn identity_context(&self) -> Arc<IdentityContext> {
        Arc::clone(&self.identity.context)
    }

    pub(crate) fn registration_context(&self) -> Arc<IdentityRegistrationContext> {
        Arc::clone(&self.identity.registration)
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
            ..ScoredPostsQuery::test_default()
        };

        assert_eq!(query.topic_recall_mode(), TopicRecallMode::Strict);
        assert_eq!(query.selected_topic_ids(), &[10]);
    }

    #[test]
    fn new_user_topics_select_cold_start_recall() {
        let query = ScoredPostsQuery {
            new_user_topic_ids: vec![20],
            ..ScoredPostsQuery::test_default()
        };

        assert_eq!(query.topic_recall_mode(), TopicRecallMode::ColdStart);
        assert_eq!(query.selected_topic_ids(), &[20]);
    }

    #[test]
    fn supplemental_topics_select_blended_recall() {
        let query = ScoredPostsQuery {
            supplemental_topic_ids: vec![30],
            ..ScoredPostsQuery::test_default()
        };

        assert_eq!(query.topic_recall_mode(), TopicRecallMode::Blend);
        assert_eq!(query.selected_topic_ids(), &[30]);
    }

    #[test]
    fn empty_topics_disable_topic_recall() {
        let query = ScoredPostsQuery::test_default();

        assert_eq!(query.topic_recall_mode(), TopicRecallMode::None);
        assert!(query.selected_topic_ids().is_empty());
    }

    #[test]
    fn request_identity_constructor_keeps_reader_and_registration_on_same_context() {
        let query = ScoredPostsQuery::test_default();

        assert!(Arc::ptr_eq(
            &query.identity.context,
            &query.identity.registration.reader()
        ));

        let resolver = Arc::new(PaddedIdentityResolver::new());
        let request_identity = Arc::new(IdentityContext::new(resolver.clone() as _));
        let registration = Arc::new(IdentityRegistrationContext::new(
            Arc::clone(&request_identity),
            resolver,
        ));
        let query = query.with_request_identity(request_identity, registration);
        assert!(Arc::ptr_eq(
            &query.identity.context,
            &query.identity.registration.reader()
        ));
    }
}
