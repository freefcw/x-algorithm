use crate::params::MRPYQ_RECOMMENDATION_DATA_TIMEOUT_MS;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use tonic::transport::Channel;
use x_algorithm_proto::recommendation_data as pb;
use x_algorithm_proto::recommendation_data::recommendation_data_service_client::RecommendationDataServiceClient;

const ADDRESS_ENV: &str = "MRPYQ_RECOMMENDATION_DATA_ADDR";
const TIMEOUT_ENV: &str = "MRPYQ_RECOMMENDATION_DATA_TIMEOUT_MS";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    match config.address {
        Some(address) => Ok(Arc::new(GrpcMrpyqRecommendationDataClient::from_addr(
            address,
            config.timeout,
        )?)),
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
}

impl GrpcMrpyqRecommendationDataClient {
    pub fn from_addr(address: String, timeout: Duration) -> anyhow::Result<Self> {
        let channel = Channel::from_shared(address)?.connect_lazy();
        Ok(Self { channel, timeout })
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

    async fn batch_get_contents(
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
                Ok(RecommendationContent {
                    feed_id: content.feed_id,
                    creator_account_id: content.creator_account_id,
                    creator_member_id: content.creator_member_id,
                    created_at_ms: content.created_at_ms,
                    like_count: content.like_count,
                    comment_count: content.comment_count,
                    gift_value: content.gift_value,
                    recommendation_eligible: content.recommendation_eligible,
                })
            })
            .collect()
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
}
