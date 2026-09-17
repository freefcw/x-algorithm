use crate::metrics::ClientCallRecorder;
use crate::params::MRPYQ_RECOMMENDATION_DATA_TIMEOUT_MS;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::async_trait;
use tonic::transport::Channel;
use x_algorithm_proto::recommendation_data as pb;
use x_algorithm_proto::recommendation_data::recommendation_data_service_client::RecommendationDataServiceClient;

const ADDRESS_ENV: &str = "MRPYQ_RECOMMENDATION_DATA_ADDR";
const TIMEOUT_ENV: &str = "MRPYQ_RECOMMENDATION_DATA_TIMEOUT_MS";

/// Matches `recommendation_data.proto`: page size and batch hydrate cap.
pub const MAX_FEED_IDS: usize = 200;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CandidateSource {
    Network,
    Fallback,
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IneligibleReason {
    #[default]
    Unspecified,
    Deleted,
    BusinessNotPublic,
    TextAuditNotPublic,
    VideoAuditNotPublic,
}

impl IneligibleReason {
    fn from_proto(value: i32) -> Self {
        match pb::RecommendationIneligibleReason::try_from(value) {
            Ok(pb::RecommendationIneligibleReason::Deleted) => Self::Deleted,
            Ok(pb::RecommendationIneligibleReason::BusinessNotPublic) => Self::BusinessNotPublic,
            Ok(pb::RecommendationIneligibleReason::TextAuditNotPublic) => Self::TextAuditNotPublic,
            Ok(pb::RecommendationIneligibleReason::VideoAuditNotPublic) => {
                Self::VideoAuditNotPublic
            }
            _ => Self::Unspecified,
        }
    }

    pub fn reason_code(self) -> i32 {
        match self {
            Self::Unspecified => 0,
            Self::Deleted => 1,
            Self::BusinessNotPublic => 2,
            Self::TextAuditNotPublic => 3,
            Self::VideoAuditNotPublic => 4,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Deleted => "deleted",
            Self::BusinessNotPublic => "business_not_public",
            Self::TextAuditNotPublic => "text_audit_not_public",
            Self::VideoAuditNotPublic => "video_audit_not_public",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecommendationContent {
    pub feed_id: String,
    pub creator_account_id: String,
    pub creator_member_id: String,
    pub creator_user_id: String,
    pub creator_user_no: i32,
    pub created_at_ms: i64,
    pub text: String,
    pub tag_ids: Vec<String>,
    pub room_id: String,
    pub section_ids: Vec<String>,
    pub like_count: i32,
    pub comment_count: i32,
    pub gift_value: i32,
    pub has_image: bool,
    pub has_video: bool,
    pub video_duration_ms: i32,
    pub recommendation_eligible: bool,
    pub ineligible_reason: IneligibleReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MrpyqRecommendationDataConfig {
    pub address: Option<String>,
    pub timeout: Duration,
}

impl MrpyqRecommendationDataConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> anyhow::Result<Self> {
        let address = lookup(ADDRESS_ENV)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let timeout_ms = match lookup(TIMEOUT_ENV) {
            Some(value) => {
                let parsed = value
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| anyhow::anyhow!("{TIMEOUT_ENV} must be a positive integer"))?;
                if parsed == 0 {
                    anyhow::bail!("{TIMEOUT_ENV} must be a positive integer");
                }
                parsed
            }
            None => MRPYQ_RECOMMENDATION_DATA_TIMEOUT_MS,
        };
        Ok(Self {
            address,
            timeout: Duration::from_millis(timeout_ms),
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MrpyqClientError {
    NotConfigured,
    Timeout,
    Unavailable(String),
    InvalidResponse(String),
}

impl fmt::Display for MrpyqClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotConfigured => write!(formatter, "{ADDRESS_ENV} is not configured"),
            Self::Timeout => write!(formatter, "mrpyq recommendation data request timed out"),
            Self::Unavailable(message) => {
                write!(
                    formatter,
                    "mrpyq recommendation data unavailable: {message}"
                )
            }
            Self::InvalidResponse(message) => {
                write!(
                    formatter,
                    "mrpyq recommendation data returned invalid response: {message}"
                )
            }
        }
    }
}

impl std::error::Error for MrpyqClientError {}

#[async_trait]
pub trait MrpyqRecommendationDataClient: Send + Sync {
    async fn list_candidates(
        &self,
        account_id: String,
        source: CandidateSource,
        page_size: usize,
        page_token: String,
    ) -> Result<CandidatePage, MrpyqClientError>;

    async fn batch_get_contents(
        &self,
        feed_ids: Vec<String>,
    ) -> Result<Vec<RecommendationContent>, MrpyqClientError>;
}

pub fn client_from_config(
    config: MrpyqRecommendationDataConfig,
) -> anyhow::Result<Arc<dyn MrpyqRecommendationDataClient>> {
    client_from_config_with_calls(config, ClientCallRecorder::default())
}

pub fn client_from_config_with_calls(
    config: MrpyqRecommendationDataConfig,
    calls: ClientCallRecorder,
) -> anyhow::Result<Arc<dyn MrpyqRecommendationDataClient>> {
    match config.address {
        Some(address) => Ok(Arc::new(
            GrpcMrpyqRecommendationDataClient::from_addr(address, config.timeout)?
                .with_calls(calls),
        )),
        None => Ok(Arc::new(DisabledMrpyqRecommendationDataClient)),
    }
}

#[derive(Clone, Debug, Default)]
pub struct DisabledMrpyqRecommendationDataClient;

#[async_trait]
impl MrpyqRecommendationDataClient for DisabledMrpyqRecommendationDataClient {
    async fn list_candidates(
        &self,
        _account_id: String,
        _source: CandidateSource,
        _page_size: usize,
        _page_token: String,
    ) -> Result<CandidatePage, MrpyqClientError> {
        Err(MrpyqClientError::NotConfigured)
    }

    async fn batch_get_contents(
        &self,
        _feed_ids: Vec<String>,
    ) -> Result<Vec<RecommendationContent>, MrpyqClientError> {
        Err(MrpyqClientError::NotConfigured)
    }
}

#[derive(Clone)]
pub struct GrpcMrpyqRecommendationDataClient {
    channel: Channel,
    timeout: Duration,
    calls: ClientCallRecorder,
}

impl GrpcMrpyqRecommendationDataClient {
    pub fn from_addr(address: String, timeout: Duration) -> anyhow::Result<Self> {
        let channel = Channel::from_shared(address)?.connect_lazy();
        Ok(Self {
            channel,
            timeout,
            calls: ClientCallRecorder::default(),
        })
    }

    /// Attach the process call metrics; the default records nothing.
    pub fn with_calls(mut self, calls: ClientCallRecorder) -> Self {
        self.calls = calls;
        self
    }

    fn map_status(status: tonic::Status) -> MrpyqClientError {
        let message = format!("{}: {}", status.code(), status.message());
        match status.code() {
            tonic::Code::DeadlineExceeded => MrpyqClientError::Timeout,
            tonic::Code::InvalidArgument
            | tonic::Code::FailedPrecondition
            | tonic::Code::OutOfRange
            | tonic::Code::DataLoss => MrpyqClientError::InvalidResponse(message),
            _ => MrpyqClientError::Unavailable(message),
        }
    }

    async fn list_candidates_rpc(
        &self,
        account_id: String,
        source: CandidateSource,
        page_size: usize,
        page_token: String,
    ) -> Result<CandidatePage, MrpyqClientError> {
        let proto_source = match source {
            CandidateSource::Network => pb::RecommendationCandidateSource::Network,
            CandidateSource::Fallback => pb::RecommendationCandidateSource::Fallback,
        };
        let request = pb::ListRecommendationCandidatesReq {
            account_id,
            source: proto_source as i32,
            page_size: i32::try_from(page_size).unwrap_or(i32::MAX),
            page_token,
        };
        let mut client = RecommendationDataServiceClient::new(self.channel.clone());
        let response =
            tokio::time::timeout(self.timeout, client.list_recommendation_candidates(request))
                .await
                .map_err(|_| MrpyqClientError::Timeout)?
                .map_err(Self::map_status)?
                .into_inner();

        let candidates = response
            .candidates
            .into_iter()
            .map(|candidate| {
                let candidate_source =
                    match pb::RecommendationCandidateSource::try_from(candidate.source) {
                        Ok(pb::RecommendationCandidateSource::Network) => CandidateSource::Network,
                        Ok(pb::RecommendationCandidateSource::Fallback) => {
                            CandidateSource::Fallback
                        }
                        _ => {
                            return Err(MrpyqClientError::InvalidResponse(format!(
                                "candidate {} has unspecified source",
                                candidate.feed_id
                            )))
                        }
                    };
                if candidate.feed_id.trim().is_empty() || candidate_source != source {
                    return Err(MrpyqClientError::InvalidResponse(
                        "candidate feed ID is empty or source mismatched".to_string(),
                    ));
                }
                Ok(CandidateReference {
                    feed_id: candidate.feed_id,
                    source: candidate_source,
                    source_score: candidate.source_score,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(CandidatePage {
            candidates,
            next_page_token: response.next_page_token,
            source_ready: response.source_ready,
        })
    }

    async fn batch_get_contents_rpc(
        &self,
        feed_ids: Vec<String>,
    ) -> Result<Vec<RecommendationContent>, MrpyqClientError> {
        let mut client = RecommendationDataServiceClient::new(self.channel.clone());
        let response = tokio::time::timeout(
            self.timeout,
            client.batch_get_recommendation_contents(pb::BatchGetRecommendationContentsReq {
                feed_ids,
            }),
        )
        .await
        .map_err(|_| MrpyqClientError::Timeout)?
        .map_err(Self::map_status)?
        .into_inner();

        let mut unique = HashSet::new();
        response
            .contents
            .into_iter()
            .map(|content| {
                if content.feed_id.trim().is_empty() || !unique.insert(content.feed_id.clone()) {
                    return Err(MrpyqClientError::InvalidResponse(
                        "hydrated content has empty or duplicate feed ID".to_string(),
                    ));
                }
                Ok(recommendation_content_from_proto(content))
            })
            .collect()
    }
}

#[async_trait]
impl MrpyqRecommendationDataClient for GrpcMrpyqRecommendationDataClient {
    async fn list_candidates(
        &self,
        account_id: String,
        source: CandidateSource,
        page_size: usize,
        page_token: String,
    ) -> Result<CandidatePage, MrpyqClientError> {
        let started = Instant::now();
        let result = self
            .list_candidates_rpc(account_id, source, page_size, page_token)
            .await;
        self.calls.record(
            "mrpyq_recommendation_data",
            "ListRecommendationCandidates",
            if result.is_ok() { "ok" } else { "error" },
            started,
        );
        match &result {
            Ok(page) => log::info!(
                "mrpyq rpc ListRecommendationCandidates source={source:?} page_size={page_size} elapsed_ms={} candidates={} ready={}",
                started.elapsed().as_millis(),
                page.candidates.len(),
                page.source_ready,
            ),
            Err(error) => log::warn!(
                "mrpyq rpc ListRecommendationCandidates source={source:?} page_size={page_size} elapsed_ms={} error={error}",
                started.elapsed().as_millis(),
            ),
        }
        result
    }

    async fn batch_get_contents(
        &self,
        feed_ids: Vec<String>,
    ) -> Result<Vec<RecommendationContent>, MrpyqClientError> {
        let started = Instant::now();
        let feed_count = feed_ids.len();
        let result = self.batch_get_contents_rpc(feed_ids).await;
        self.calls.record(
            "mrpyq_recommendation_data",
            "BatchGetRecommendationContents",
            if result.is_ok() { "ok" } else { "error" },
            started,
        );
        match &result {
            Ok(contents) => log::info!(
                "mrpyq rpc BatchGetRecommendationContents feed_ids={feed_count} elapsed_ms={} contents={}",
                started.elapsed().as_millis(),
                contents.len(),
            ),
            Err(error) => log::warn!(
                "mrpyq rpc BatchGetRecommendationContents feed_ids={feed_count} elapsed_ms={} error={error}",
                started.elapsed().as_millis(),
            ),
        }
        result
    }
}

fn recommendation_content_from_proto(content: pb::RecommendationContent) -> RecommendationContent {
    RecommendationContent {
        feed_id: content.feed_id,
        creator_account_id: content.creator_account_id,
        creator_member_id: content.creator_member_id,
        creator_user_id: content.creator_user_id,
        creator_user_no: content.creator_user_no,
        created_at_ms: content.created_at_ms,
        text: content.text,
        tag_ids: content.tag_ids,
        room_id: content.room_id,
        section_ids: content.section_ids,
        like_count: content.like_count,
        comment_count: content.comment_count,
        gift_value: content.gift_value,
        has_image: content.has_image,
        has_video: content.has_video,
        video_duration_ms: content.video_duration_ms,
        recommendation_eligible: content.recommendation_eligible,
        ineligible_reason: IneligibleReason::from_proto(content.ineligible_reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_to_disabled_and_rejects_invalid_timeout() {
        let disabled = MrpyqRecommendationDataConfig::from_lookup(|_| None).expect("defaults");
        assert_eq!(disabled.address, None);
        assert_eq!(
            disabled.timeout,
            Duration::from_millis(MRPYQ_RECOMMENDATION_DATA_TIMEOUT_MS)
        );

        assert!(MrpyqRecommendationDataConfig::from_lookup(|key| {
            (key == TIMEOUT_ENV).then(|| "0".to_string())
        })
        .is_err());
        assert!(MrpyqRecommendationDataConfig::from_lookup(|key| {
            (key == TIMEOUT_ENV).then(|| "invalid".to_string())
        })
        .is_err());
    }

    #[tokio::test]
    async fn disabled_client_returns_explicit_not_configured_error() {
        let error = DisabledMrpyqRecommendationDataClient
            .list_candidates(
                "account".to_string(),
                CandidateSource::Network,
                10,
                String::new(),
            )
            .await
            .expect_err("disabled client must fail");
        assert_eq!(error, MrpyqClientError::NotConfigured);
    }

    #[test]
    fn tonic_status_preserves_timeout_and_invalid_response_classes() {
        assert_eq!(
            GrpcMrpyqRecommendationDataClient::map_status(
                tonic::Status::deadline_exceeded("slow",)
            ),
            MrpyqClientError::Timeout
        );
        assert!(matches!(
            GrpcMrpyqRecommendationDataClient::map_status(tonic::Status::invalid_argument(
                "bad request",
            )),
            MrpyqClientError::InvalidResponse(_)
        ));
        assert!(matches!(
            GrpcMrpyqRecommendationDataClient::map_status(tonic::Status::unavailable("down")),
            MrpyqClientError::Unavailable(_)
        ));
    }

    #[test]
    fn invalid_address_is_rejected_during_assembly() {
        assert!(GrpcMrpyqRecommendationDataClient::from_addr(
            "not a uri".to_string(),
            Duration::from_millis(1)
        )
        .is_err());
    }

    #[test]
    fn content_mapping_preserves_mrpyq_feed_fields() {
        let content = recommendation_content_from_proto(pb::RecommendationContent {
            feed_id: "e305c05a62cd1ef55823cd86".to_string(),
            creator_account_id: "6553f1000000000000000007".to_string(),
            creator_member_id: "member-1".to_string(),
            creator_user_id: "00000000000000000000000a".to_string(),
            creator_user_no: 9,
            created_at_ms: 1_700_000_000_000,
            text: "hello".to_string(),
            tag_ids: vec!["tag-1".to_string()],
            room_id: "room-1".to_string(),
            section_ids: vec!["section-1".to_string()],
            like_count: 7,
            comment_count: 3,
            gift_value: 11,
            has_image: true,
            has_video: true,
            video_duration_ms: 12_000,
            recommendation_eligible: false,
            ineligible_reason: pb::RecommendationIneligibleReason::Deleted as i32,
        });
        assert_eq!(content.creator_account_id, "6553f1000000000000000007");
        assert_eq!(content.text, "hello");
        assert_eq!(content.like_count, 7);
        assert_eq!(content.comment_count, 3);
        assert_eq!(content.gift_value, 11);
        assert!(content.has_image && content.has_video);
        assert_eq!(content.video_duration_ms, 12_000);
        assert!(!content.recommendation_eligible);
        assert_eq!(content.ineligible_reason, IneligibleReason::Deleted);
        assert_eq!(content.ineligible_reason.reason_code(), 1);
    }
}
