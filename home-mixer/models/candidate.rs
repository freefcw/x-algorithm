use crate::visibility::models as vf;
use std::collections::HashMap;
use x_algorithm_proto::home_mixer as pb;

pub use crate::models::brand_safety::BrandSafetyVerdict;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SafetyLabelInfo {
    pub label: String,
    pub description: Option<String>,
    pub source: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct PostCandidate {
    pub tweet_id: u64,
    pub author_id: u64,
    pub tweet_text: String,
    pub quoted_tweet_text: String,
    pub in_reply_to_tweet_id: Option<u64>,
    pub retweeted_tweet_id: Option<u64>,
    pub retweeted_user_id: Option<u64>,
    pub quoted_tweet_id: Option<u64>,
    pub quoted_user_id: Option<u64>,
    pub phoenix_scores: PhoenixScores,
    pub prediction_request_id: Option<u64>,
    pub last_scored_at_ms: Option<u64>,
    pub weighted_score: Option<f64>,
    pub score: Option<f64>,
    pub served_type: Option<pb::ServedType>,
    pub in_network: Option<bool>,
    pub ancestors: Vec<u64>,
    pub video_duration_ms: Option<i32>,
    pub quoted_video_duration_ms: Option<i32>,
    pub author_followers_count: Option<i32>,
    pub author_screen_name: Option<String>,
    pub retweeted_screen_name: Option<String>,
    pub visibility_decision: vf::VisibilityDecision,
    pub drop_ancillary_posts: Option<bool>,
    pub subscription_author_id: Option<u64>,
    pub retrieval_topic_ids: Vec<i64>,
    pub filtered_topic_ids: Vec<i64>,
    pub unfiltered_topic_ids: Vec<i64>,
    pub following_replied_user_ids: Vec<u64>,
    pub has_media: Option<bool>,
    pub language_code: Option<String>,
    pub favorite_count: Option<i64>,
    pub reply_count: Option<i64>,
    pub repost_count: Option<i64>,
    pub quote_count: Option<i64>,
    pub mutual_follow_jaccard: Option<f64>,
    pub brand_safety_verdict: Option<BrandSafetyVerdict>,
    pub safety_labels: Vec<SafetyLabelInfo>,
}

#[derive(Clone, Debug, Default)]
pub struct PhoenixScores {
    pub favorite_score: Option<f64>,
    pub reply_score: Option<f64>,
    pub retweet_score: Option<f64>,
    pub photo_expand_score: Option<f64>,
    pub click_score: Option<f64>,
    pub profile_click_score: Option<f64>,
    pub vqv_score: Option<f64>,
    pub share_score: Option<f64>,
    pub share_via_dm_score: Option<f64>,
    pub share_via_copy_link_score: Option<f64>,
    pub dwell_score: Option<f64>,
    pub quote_score: Option<f64>,
    pub quoted_click_score: Option<f64>,
    pub quoted_vqv_score: Option<f64>,
    pub follow_author_score: Option<f64>,
    pub not_interested_score: Option<f64>,
    pub block_author_score: Option<f64>,
    pub mute_author_score: Option<f64>,
    pub report_score: Option<f64>,
    pub not_dwelled_score: Option<f64>,
    // Continuous actions
    pub dwell_time: Option<f64>,
    pub click_dwell_time: Option<f64>,
}

pub trait CandidateHelpers {
    fn get_screen_names(&self) -> HashMap<u64, String>;
}

impl CandidateHelpers for PostCandidate {
    fn get_screen_names(&self) -> HashMap<u64, String> {
        let mut screen_names = HashMap::<u64, String>::new();
        if let Some(author_screen_name) = self.author_screen_name.clone() {
            screen_names.insert(self.author_id, author_screen_name);
        }
        if let (Some(retweeted_screen_name), Some(retweeted_user_id)) =
            (self.retweeted_screen_name.clone(), self.retweeted_user_id)
        {
            screen_names.insert(retweeted_user_id, retweeted_screen_name);
        }
        screen_names
    }
}
