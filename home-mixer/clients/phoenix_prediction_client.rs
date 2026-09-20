// Phoenix 精排预测客户端
//
// 替代原始被阉割的 Phoenix 预测客户端模块。
//
// 原始功能说明：
// PhoenixPredictionClient 是 Home Mixer 与 Phoenix 精排模型服务
// 之间的 gRPC 客户端。它负责：
//   1. 将候选帖子列表和用户行为序列发送给 Phoenix
//   2. Phoenix 运行 Grok Transformer 模型，预估用户对每条帖子
//      执行各种互动行为的概率
//   3. 返回预测结果，供 RankingScorer 计算最终排序分数
//
// 预测输入：
//   - user_id: 当前用户
//   - user_action_sequence: 用户最近的行为序列（特征）
//   - candidates: 待评分的帖子列表
//
// 预测输出：
//   - PredictNextActionsResponse: 每条帖子上各行为的概率分布
//
// 连接方式：
//   - 设置环境变量 PHOENIX_PREDICT_GRPC_ADDR（如 http://localhost:50053）
//     时，走真实 gRPC 调用 PhoenixPredictionService.PredictNextActions；
//   - 未设置时返回显式不可用错误，由 Scorer 隔离并保留规则排序。

use crate::metrics::ClientCallRecorder;
use crate::models::ids::UserId;
use log::{info, warn};
use std::time::{Duration, Instant};
use tonic::async_trait;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;
use x_algorithm_proto::recsys;
use x_algorithm_proto::recsys::phoenix_prediction_service_client::PhoenixPredictionServiceClient;

/// Phoenix 精排预测客户端 trait
///
/// 定义了调用 Phoenix 精排模型的标准接口。
/// Home Mixer 的 PhoenixScorer 通过此 trait 获取模型预测结果。
#[async_trait]
pub trait PhoenixPredictionClient: Send + Sync {
    /// 调用 Phoenix 精排模型进行预测
    ///
    /// # Arguments
    /// * `user_id` - 目标用户 ID
    /// * `sequence` - 用户最近的行为序列（模型的核心输入特征）
    /// * `candidates` - 待评分的候选帖子列表
    ///
    /// # Returns
    /// 预测响应，包含每条帖子上各行为的概率分布
    async fn predict(
        &self,
        user_id: UserId,
        sequence: recsys::UserActionSequence,
        candidates: Vec<recsys::TweetInfo>,
    ) -> Result<recsys::PredictNextActionsResponse, anyhow::Error>;
}

pub async fn predict_with_timeout(
    client: &(dyn PhoenixPredictionClient + Send + Sync),
    user_id: UserId,
    sequence: recsys::UserActionSequence,
    candidates: Vec<recsys::TweetInfo>,
    timeout: Duration,
) -> Result<recsys::PredictNextActionsResponse, anyhow::Error> {
    tokio::time::timeout(timeout, client.predict(user_id, sequence, candidates))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Phoenix prediction timed out after {}ms",
                timeout.as_millis()
            )
        })?
}

/// 生产环境 Phoenix 精排客户端
///
/// 设置 `PHOENIX_PREDICT_GRPC_ADDR` 后调用真实的 Phoenix gRPC 服务；
/// 未设置时返回显式不可用错误，由 Scorer 记录并保留规则 fallback。
pub struct ProdPhoenixPredictionClient {
    channel: Option<Channel>,
    /// 仅 demo 装配可设为 true；生产环境必须使用训练权重。
    pub allow_random: bool,
    calls: ClientCallRecorder,
}

impl ProdPhoenixPredictionClient {
    pub async fn new() -> Result<Self, anyhow::Error> {
        let channel = match std::env::var("PHOENIX_PREDICT_GRPC_ADDR") {
            Ok(addr) => {
                info!("PhoenixPredictionClient: connecting to {}", addr);
                Some(Channel::from_shared(addr)?.connect_lazy())
            }
            Err(_) => {
                warn!(
                    "PhoenixPredictionClient: PHOENIX_PREDICT_GRPC_ADDR not set; prediction is unavailable"
                );
                None
            }
        };
        Ok(Self {
            channel,
            allow_random: false,
            calls: ClientCallRecorder::default(),
        })
    }

    /// Attach the process call metrics; the default records nothing.
    pub fn with_calls(mut self, calls: ClientCallRecorder) -> Self {
        self.calls = calls;
        self
    }
}

#[async_trait]
impl PhoenixPredictionClient for ProdPhoenixPredictionClient {
    async fn predict(
        &self,
        user_id: UserId,
        sequence: recsys::UserActionSequence,
        candidates: Vec<recsys::TweetInfo>,
    ) -> Result<recsys::PredictNextActionsResponse, anyhow::Error> {
        let Some(channel) = &self.channel else {
            anyhow::bail!("PHOENIX_PREDICT_GRPC_ADDR is not configured");
        };

        let mut client = PhoenixPredictionServiceClient::new(channel.clone());
        let requested = candidates.clone();
        let candidate_count = requested.len();
        let request = recsys::PredictNextActionsRequest {
            user_id,
            user_action_sequence: Some(sequence),
            candidates,
        };

        let started = Instant::now();
        let response = match client.predict_next_actions(request).await {
            Ok(response) => response,
            Err(status) => {
                warn!(
                    "phoenix rpc PredictNextActions candidates={candidate_count} elapsed_ms={} code={} error={}",
                    started.elapsed().as_millis(),
                    status.code(),
                    status.message(),
                );
                self.calls
                    .record("phoenix_prediction", "PredictNextActions", "error", started);
                return Err(status.into());
            }
        };
        // 网络耗时与契约校验耗时分开：调用慢和模型答非所问是两种故障。
        let elapsed_ms = started.elapsed().as_millis();

        if let Err(error) = validate_serving_metadata(response.metadata(), self.allow_random) {
            warn!(
                "phoenix rpc PredictNextActions candidates={candidate_count} elapsed_ms={elapsed_ms} rejected={error:#}"
            );
            self.calls.record(
                "phoenix_prediction",
                "PredictNextActions",
                "rejected",
                started,
            );
            return Err(error);
        }
        let inner = response.into_inner();
        if let Err(error) = validate_predict_response(&requested, &inner) {
            warn!(
                "phoenix rpc PredictNextActions candidates={candidate_count} elapsed_ms={elapsed_ms} rejected={error:#}"
            );
            self.calls.record(
                "phoenix_prediction",
                "PredictNextActions",
                "rejected",
                started,
            );
            return Err(error);
        }

        info!(
            "phoenix rpc PredictNextActions candidates={candidate_count} elapsed_ms={elapsed_ms} scored={}",
            inner
                .distribution_sets
                .first()
                .map_or(0, |set| set.candidate_distributions.len()),
        );
        self.calls
            .record("phoenix_prediction", "PredictNextActions", "ok", started);
        Ok(inner)
    }
}

/// Validate the serving metadata emitted as gRPC trailing metadata.
///
/// The model may answer successfully while using the wrong bundle. Treat a
/// missing, changed, or random bundle as an adapter error so the pipeline can
/// use its whole-batch rule fallback.
///
/// Every `ActionName` whose weight in `params::param` is non-zero must be
/// advertised by the gateway's `supported-actions`, otherwise the batch falls
/// back to rules. v1 head set (docs/implementation/phoenix-training-data-decisions.md
/// §1): favorite (1), reply (2), report (18). A unit test pins this list to the
/// weights; add a head back only through the procedure in that document (§1.4).
pub const REQUIRED_SUPPORTED_ACTIONS: &[u32] = &[1, 2, 18];

pub fn validate_serving_metadata(
    metadata: &MetadataMap,
    allow_random: bool,
) -> Result<String, anyhow::Error> {
    let schema = metadata
        .get("feature-schema")
        .ok_or_else(|| anyhow::anyhow!("Phoenix response missing metadata feature-schema"))?
        .to_str()
        .map_err(|_| anyhow::anyhow!("Phoenix metadata feature-schema is not valid ASCII"))?;
    if schema != "phoenix-snowflake-id-actions-v3" {
        anyhow::bail!("Phoenix metadata feature-schema mismatch: got {schema}");
    }

    let identity_map_version = metadata
        .get("identity-map-version")
        .ok_or_else(|| anyhow::anyhow!("Phoenix response missing metadata identity-map-version"))?
        .to_str()
        .map_err(|_| anyhow::anyhow!("Phoenix metadata identity-map-version is not valid ASCII"))?;
    anyhow::ensure!(
        identity_map_version == id_service::MAPPING_VERSION.to_string(),
        "Phoenix metadata identity-map-version mismatch: got {identity_map_version}"
    );

    let model_version = metadata
        .get("model-version")
        .ok_or_else(|| anyhow::anyhow!("Phoenix response missing metadata model-version"))?
        .to_str()
        .map_err(|_| anyhow::anyhow!("Phoenix metadata model-version is not valid ASCII"))?;
    anyhow::ensure!(
        !model_version.trim().is_empty(),
        "Phoenix metadata model-version is empty"
    );
    let random = metadata
        .get("random-weights")
        .ok_or_else(|| anyhow::anyhow!("Phoenix response missing metadata random-weights"))?
        .to_str()
        .map_err(|_| anyhow::anyhow!("Phoenix metadata random-weights is not valid ASCII"))?;
    anyhow::ensure!(
        matches!(random, "true" | "false"),
        "Phoenix metadata random-weights must be true or false"
    );
    anyhow::ensure!(
        allow_random || (random == "false" && model_version != "random"),
        "Phoenix random weights are only allowed in demo"
    );
    if let Ok(expected) = std::env::var("PHOENIX_EXPECTED_MODEL_VERSION") {
        if !expected.trim().is_empty() && model_version != expected {
            anyhow::bail!(
                "Phoenix metadata model-version mismatch: expected {}, got {}",
                expected,
                model_version
            );
        }
    }

    let supported = metadata
        .get("supported-actions")
        .ok_or_else(|| anyhow::anyhow!("Phoenix response missing metadata supported-actions"))?
        .to_str()
        .map_err(|_| anyhow::anyhow!("Phoenix metadata supported-actions is not valid ASCII"))?;
    let mut actions = supported
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<u32>()
                .map_err(|_| anyhow::anyhow!("Phoenix supported-actions contains {value}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    actions.sort_unstable();
    actions.dedup();
    anyhow::ensure!(
        actions.iter().all(|action| (1..=18).contains(action)),
        "Phoenix metadata supported-actions contains an unknown ActionName"
    );
    if !REQUIRED_SUPPORTED_ACTIONS
        .iter()
        .all(|action| actions.binary_search(action).is_ok())
    {
        anyhow::bail!(
            "Phoenix metadata supported-actions omit a non-zero ranking head: got {supported}"
        );
    }
    Ok(model_version.to_owned())
}

/// Adapter-internal validation (mp-slim contract). PhoenixScorer only sees `Result`.
pub fn validate_predict_response(
    requested: &[recsys::TweetInfo],
    response: &recsys::PredictNextActionsResponse,
) -> Result<(), anyhow::Error> {
    anyhow::ensure!(
        response.distribution_sets.len() == 1,
        "Phoenix response must contain exactly one distribution set"
    );
    let Some(set) = response.distribution_sets.first() else {
        anyhow::bail!("Phoenix response missing distribution_sets");
    };
    let mut seen = std::collections::HashSet::new();
    let mut returned = std::collections::HashSet::new();
    for dist in &set.candidate_distributions {
        let Some(candidate) = &dist.candidate else {
            anyhow::bail!("Phoenix response has a distribution without candidate");
        };
        if candidate.tweet_id == 0 {
            anyhow::bail!("Phoenix response has empty tweet_id");
        }
        if !seen.insert(candidate.tweet_id) {
            anyhow::bail!(
                "Phoenix response has duplicate tweet_id {}",
                candidate.tweet_id
            );
        }
        anyhow::ensure!(
            requested
                .iter()
                .any(|tweet| tweet.tweet_id == candidate.tweet_id),
            "Phoenix response contains unknown candidate {}",
            candidate.tweet_id
        );
        let requested_author = requested
            .iter()
            .find(|tweet| tweet.tweet_id == candidate.tweet_id)
            .map(|tweet| tweet.author_id)
            .unwrap_or_default();
        anyhow::ensure!(
            candidate.author_id == requested_author,
            "Phoenix response author mismatch for {}",
            candidate.tweet_id
        );
        anyhow::ensure!(
            dist.top_log_probs.len() == 19,
            "Phoenix response shape mismatch for {}: expected 19 action probabilities, got {}",
            candidate.tweet_id,
            dist.top_log_probs.len()
        );
        anyhow::ensure!(
            dist.continuous_actions_values.len() == 2,
            "Phoenix response shape mismatch for {}: expected 2 continuous values, got {}",
            candidate.tweet_id,
            dist.continuous_actions_values.len()
        );
        returned.insert(candidate.tweet_id);
        if dist.top_log_probs.iter().any(|v| !v.is_finite())
            || dist
                .continuous_actions_values
                .iter()
                .any(|v| !v.is_finite())
        {
            anyhow::bail!(
                "Phoenix response contains NaN/Inf for {}",
                candidate.tweet_id
            );
        }
    }
    anyhow::ensure!(
        returned.len() == requested.len(),
        "Phoenix response has extra or missing candidates"
    );
    for tweet in requested {
        if !returned.contains(&tweet.tweet_id) {
            anyhow::bail!("Phoenix response missing candidate {}", tweet.tweet_id);
        }
    }
    Ok(())
}

/// Slim engine adapter: inner client + fail-closed response validation.
pub struct SlimPhoenixPredictionClient {
    inner: ProdPhoenixPredictionClient,
}

impl SlimPhoenixPredictionClient {
    pub async fn new() -> Result<Self, anyhow::Error> {
        Self::new_with_allow_random(false).await
    }

    pub async fn new_with_allow_random(allow_random: bool) -> Result<Self, anyhow::Error> {
        resolve_prediction_engine()?;
        let mut inner = ProdPhoenixPredictionClient::new().await?;
        inner.allow_random = allow_random;
        Ok(Self { inner })
    }

    /// Attach the process call metrics to the wrapped production client.
    pub fn with_calls(mut self, calls: ClientCallRecorder) -> Self {
        self.inner = self.inner.with_calls(calls);
        self
    }
}

#[async_trait]
impl PhoenixPredictionClient for SlimPhoenixPredictionClient {
    async fn predict(
        &self,
        user_id: UserId,
        sequence: recsys::UserActionSequence,
        candidates: Vec<recsys::TweetInfo>,
    ) -> Result<recsys::PredictNextActionsResponse, anyhow::Error> {
        self.inner.predict(user_id, sequence, candidates).await
    }
}

/// Engine selector. `PHOENIX_ENGINE=xrex` is reserved; only `slim` is implemented.
pub fn selected_prediction_engine() -> &'static str {
    match std::env::var("PHOENIX_ENGINE") {
        Ok(value) if value.eq_ignore_ascii_case("xrex") => "xrex",
        _ => "slim",
    }
}

/// Parse the operator engine setting and fail closed for unsupported engines.
/// Callers should use this during assembly instead of silently constructing slim.
pub fn resolve_prediction_engine() -> Result<&'static str, anyhow::Error> {
    resolve_prediction_engine_value(std::env::var("PHOENIX_ENGINE").ok().as_deref())
}

fn resolve_prediction_engine_value(value: Option<&str>) -> Result<&'static str, anyhow::Error> {
    match value {
        Some(value) if value.eq_ignore_ascii_case("slim") => Ok("slim"),
        Some(value) if value.eq_ignore_ascii_case("xrex") => {
            anyhow::bail!("PHOENIX_ENGINE=xrex is not implemented in this build")
        }
        Some(value) => anyhow::bail!("unsupported PHOENIX_ENGINE={value:?}"),
        None => Ok("slim"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SlowPredictionClient;

    #[async_trait]
    impl PhoenixPredictionClient for SlowPredictionClient {
        async fn predict(
            &self,
            _user_id: UserId,
            _sequence: recsys::UserActionSequence,
            _candidates: Vec<recsys::TweetInfo>,
        ) -> Result<recsys::PredictNextActionsResponse, anyhow::Error> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(Default::default())
        }
    }

    #[tokio::test]
    async fn missing_prediction_endpoint_is_explicitly_unavailable() {
        let error = ProdPhoenixPredictionClient {
            channel: None,
            allow_random: false,
            calls: ClientCallRecorder::default(),
        }
        .predict(crate::models::uid(1), Default::default(), Vec::new())
        .await
        .expect_err("missing endpoint must not report successful prediction");

        assert!(error.to_string().contains("not configured"));
    }

    #[tokio::test]
    async fn prediction_deadline_bounds_slow_adapter() {
        let error = predict_with_timeout(
            &SlowPredictionClient,
            crate::models::uid(1),
            Default::default(),
            Vec::new(),
            Duration::from_millis(1),
        )
        .await
        .expect_err("slow prediction must time out");

        assert!(error.to_string().contains("timed out"));
    }

    #[test]
    fn serving_metadata_requires_non_random_complete_contract() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "feature-schema",
            "phoenix-snowflake-id-actions-v3".parse().unwrap(),
        );
        metadata.insert("model-version", "step-1".parse().unwrap());
        metadata.insert("random-weights", "false".parse().unwrap());
        metadata.insert("identity-map-version", "1".parse().unwrap());
        metadata.insert(
            "supported-actions",
            (1..=18)
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(",")
                .parse()
                .unwrap(),
        );
        assert!(validate_serving_metadata(&metadata, false).is_ok());
        metadata.remove("supported-actions");
        assert!(validate_serving_metadata(&metadata, false).is_err());
    }

    /// `REQUIRED_SUPPORTED_ACTIONS` and the ranking weights are two copies of
    /// the same decision (decisions doc §1.3). If one changes without the
    /// other, either the gateway is rejected for heads that carry no weight
    /// or a weighted head is scored from a model that never trained it.
    #[test]
    fn required_supported_actions_are_exactly_the_non_zero_weight_heads() {
        use crate::params as p;
        use x_algorithm_proto::recsys::ActionName;

        let head_weights: &[(ActionName, f64)] = &[
            (ActionName::ServerTweetFav, p::FAVORITE_WEIGHT),
            (ActionName::ServerTweetReply, p::REPLY_WEIGHT),
            (ActionName::ServerTweetRetweet, p::RETWEET_WEIGHT),
            (ActionName::ServerTweetQuote, p::QUOTE_WEIGHT),
            (ActionName::ClientTweetPhotoExpand, p::PHOTO_EXPAND_WEIGHT),
            (ActionName::ClientTweetClick, p::CLICK_WEIGHT),
            (ActionName::ClientTweetClickProfile, p::PROFILE_CLICK_WEIGHT),
            (ActionName::ClientTweetVideoQualityView, p::VQV_WEIGHT),
            (ActionName::ClientTweetShare, p::SHARE_WEIGHT),
            (
                ActionName::ClientTweetClickSendViaDirectMessage,
                p::SHARE_VIA_DM_WEIGHT,
            ),
            (
                ActionName::ClientTweetShareViaCopyLink,
                p::SHARE_VIA_COPY_LINK_WEIGHT,
            ),
            (ActionName::ClientTweetRecapDwelled, p::DWELL_WEIGHT),
            (ActionName::ClientQuotedTweetClick, p::QUOTED_CLICK_WEIGHT),
            (ActionName::ClientTweetFollowAuthor, p::FOLLOW_AUTHOR_WEIGHT),
            (
                ActionName::ClientTweetNotInterestedIn,
                p::NOT_INTERESTED_WEIGHT,
            ),
            (ActionName::ClientTweetBlockAuthor, p::BLOCK_AUTHOR_WEIGHT),
            (ActionName::ClientTweetMuteAuthor, p::MUTE_AUTHOR_WEIGHT),
            (ActionName::ClientTweetReport, p::REPORT_WEIGHT),
        ];
        assert_eq!(
            head_weights.len(),
            18,
            "one entry per released ActionName 1..=18"
        );

        let mut weighted: Vec<u32> = head_weights
            .iter()
            .filter(|(_, weight)| *weight != 0.0)
            .map(|(action, _)| *action as u32)
            .collect();
        weighted.sort_unstable();
        assert_eq!(weighted, REQUIRED_SUPPORTED_ACTIONS);
    }

    #[test]
    fn v1_gateway_head_set_is_accepted_and_missing_report_is_rejected() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "feature-schema",
            "phoenix-snowflake-id-actions-v3".parse().unwrap(),
        );
        metadata.insert("model-version", "step-000200@0123456789ab".parse().unwrap());
        metadata.insert("random-weights", "false".parse().unwrap());
        metadata.insert("identity-map-version", "1".parse().unwrap());
        // What a bundle trained with `--observed-actions favorite,reply,report` advertises.
        metadata.insert("supported-actions", "1,2,18".parse().unwrap());
        assert!(validate_serving_metadata(&metadata, false).is_ok());

        metadata.insert("supported-actions", "1,2".parse().unwrap());
        let error = validate_serving_metadata(&metadata, false).unwrap_err();
        assert!(error.to_string().contains("omit a non-zero ranking head"));
    }

    #[test]
    fn serving_metadata_allows_random_only_when_explicitly_enabled() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "feature-schema",
            "phoenix-snowflake-id-actions-v3".parse().unwrap(),
        );
        metadata.insert("model-version", "random".parse().unwrap());
        metadata.insert("random-weights", "true".parse().unwrap());
        metadata.insert("identity-map-version", "1".parse().unwrap());
        metadata.insert(
            "supported-actions",
            REQUIRED_SUPPORTED_ACTIONS
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
                .parse()
                .unwrap(),
        );
        assert!(validate_serving_metadata(&metadata, true).is_ok());
        assert!(validate_serving_metadata(&metadata, false).is_err());
    }

    #[test]
    fn unsupported_engine_values_fail_closed() {
        assert_eq!(resolve_prediction_engine_value(None).unwrap(), "slim");
        assert_eq!(
            resolve_prediction_engine_value(Some("SLIM")).unwrap(),
            "slim"
        );
        assert!(resolve_prediction_engine_value(Some("xrex")).is_err());
        assert!(resolve_prediction_engine_value(Some("typo")).is_err());
    }

    #[test]
    fn validate_predict_response_rejects_nan_duplicate_and_missing() {
        let requested = vec![recsys::TweetInfo {
            tweet_id: 1,
            ..Default::default()
        }];
        let dist = |id: u64, prob: f32| recsys::CandidateDistribution {
            candidate: Some(recsys::TweetInfo {
                tweet_id: id,
                ..Default::default()
            }),
            top_log_probs: vec![prob; 19],
            continuous_actions_values: vec![0.0; 2],
        };
        let wrap = |dists| recsys::PredictNextActionsResponse {
            distribution_sets: vec![recsys::DistributionSet {
                candidate_distributions: dists,
            }],
        };

        assert!(validate_predict_response(&requested, &wrap(vec![dist(1, 0.0)])).is_ok());
        assert!(validate_predict_response(&requested, &wrap(vec![dist(1, f32::NAN)])).is_err());
        assert!(
            validate_predict_response(&requested, &wrap(vec![dist(1, 0.0), dist(1, 0.1),]))
                .is_err()
        );
        assert!(validate_predict_response(&requested, &wrap(vec![])).is_err());
    }

    #[test]
    fn validate_predict_response_rejects_unknown_author_and_shape() {
        let requested = vec![recsys::TweetInfo {
            tweet_id: 1,
            author_id: 2,
            ..Default::default()
        }];
        let distribution =
            |tweet_id: u64, author_id: u64, width: usize| recsys::CandidateDistribution {
                candidate: Some(recsys::TweetInfo {
                    tweet_id,
                    author_id,
                    ..Default::default()
                }),
                top_log_probs: vec![0.0; width],
                continuous_actions_values: vec![0.0; 2],
            };
        let response = |distribution| recsys::PredictNextActionsResponse {
            distribution_sets: vec![recsys::DistributionSet {
                candidate_distributions: vec![distribution],
            }],
        };
        assert!(
            validate_predict_response(&requested, &response(distribution(0xff, 2, 19))).is_err()
        );
        assert!(validate_predict_response(&requested, &response(distribution(1, 3, 19))).is_err());
        assert!(validate_predict_response(&requested, &response(distribution(1, 2, 18))).is_err());
    }
}
