#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateSource {
    Network,
    Fallback,
}

impl CandidateSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Network => "NETWORK",
            Self::Fallback => "FALLBACK",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CandidateReference {
    pub feed_id: String,
    pub source: CandidateSource,
    pub source_score: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CandidatePage {
    pub candidates: Vec<CandidateReference>,
    pub next_page_token: String,
    pub source_ready: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecommendationContent {
    pub feed_id: String,
    pub creator_account_id: String,
    pub creator_member_id: String,
    pub created_at_ms: i64,
    pub like_count: i32,
    pub comment_count: i32,
    pub gift_value: i32,
    pub recommendation_eligible: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BusinessCandidate {
    pub reference: CandidateReference,
    pub content: RecommendationContent,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BusinessFeedQuery {
    pub viewer_account_id: String,
    pub page_size: usize,
    pub network_page_token: String,
    pub fallback_page_token: String,
    pub seen_feed_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BusinessFeedItem {
    pub feed_id: String,
    pub creator_account_id: String,
    pub creator_member_id: String,
    pub score: f64,
    pub source: CandidateSource,
    pub reason: String,
    pub position: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BusinessFeedOutput {
    pub request_id: String,
    pub items: Vec<BusinessFeedItem>,
    pub next_network_page_token: String,
    pub next_fallback_page_token: String,
}
