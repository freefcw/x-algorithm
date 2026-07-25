use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query_features::UserFeatures;
use crate::util::request_util::generate_request_id;
use crate::visibility::vf_client::{GetTwitterContextViewer, TwitterContextViewer};
use x_algorithm_proto::home_mixer::ImpressionBloomFilterEntry;
use xai_candidate_pipeline::candidate_pipeline::HasRequestId;

#[derive(Clone, Default, Debug)]
pub struct ScoredPostsQuery {
    pub user_id: i64,
    pub client_app_id: i32,
    pub country_code: String,
    pub language_code: String,
    pub seen_ids: Vec<i64>,
    pub served_ids: Vec<i64>,
    pub in_network_only: bool,
    pub is_bottom_request: bool,
    pub bloom_filter_entries: Vec<ImpressionBloomFilterEntry>,
    pub user_action_sequence: Option<x_algorithm_proto::recsys::UserActionSequence>,
    pub retrieval_sequence: Option<x_algorithm_proto::recsys::UserActionSequence>,
    pub scoring_sequence: Option<x_algorithm_proto::recsys::UserActionSequence>,
    pub user_features: UserFeatures,
    pub cached_posts: Vec<PostCandidate>,
    pub has_cached_posts: bool,
    pub topic_ids: Vec<i64>,
    pub excluded_topic_ids: Vec<i64>,
    pub new_user_topic_ids: Vec<i64>,
    pub exclude_videos: bool,
    pub enable_phoenix_moe: bool,
    pub impressed_post_ids: Vec<i64>,
    pub past_request_timestamps_ms: Vec<i64>,
    pub is_preview: bool,
    pub is_shadow_traffic: bool,
    pub is_top_request: bool,
    pub is_polling: bool,
    pub ip_address: String,
    pub user_agent: String,
    pub request_id: String,
}

impl ScoredPostsQuery {
    pub fn new(
        user_id: i64,
        client_app_id: i32,
        country_code: String,
        language_code: String,
        seen_ids: Vec<i64>,
        served_ids: Vec<i64>,
        in_network_only: bool,
        is_bottom_request: bool,
        bloom_filter_entries: Vec<ImpressionBloomFilterEntry>,
    ) -> Self {
        let request_id = format!("{}-{}", generate_request_id(), user_id);
        Self {
            user_id,
            client_app_id,
            country_code,
            language_code,
            seen_ids,
            served_ids,
            in_network_only,
            is_bottom_request,
            bloom_filter_entries,
            user_action_sequence: None,
            retrieval_sequence: None,
            scoring_sequence: None,
            user_features: UserFeatures::default(),
            cached_posts: Vec::new(),
            has_cached_posts: false,
            topic_ids: Vec::new(),
            excluded_topic_ids: Vec::new(),
            new_user_topic_ids: Vec::new(),
            exclude_videos: false,
            enable_phoenix_moe: false,
            impressed_post_ids: Vec::new(),
            past_request_timestamps_ms: Vec::new(),
            is_preview: false,
            is_shadow_traffic: false,
            is_top_request: !is_bottom_request,
            is_polling: false,
            ip_address: String::new(),
            user_agent: String::new(),
            request_id,
        }
    }
}

impl GetTwitterContextViewer for ScoredPostsQuery {
    fn get_viewer(&self) -> Option<TwitterContextViewer> {
        Some(TwitterContextViewer {
            user_id: self.user_id,
            client_application_id: self.client_app_id as i64,
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
