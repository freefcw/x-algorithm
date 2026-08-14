//! VM Ranker 客户端端口（RANK-03）。
//!
//! 上游 `scorers/vm_ranker.rs` 通过 `xai_vm_ranker_proto` 调用内部 VM Ranker
//! gRPC 服务，并从 feature switch 读取集群、value model、DPP 等参数。
//! 47c1bcd 开源了服务实现（本仓库 `vm-ranker/`），但未发布其 wire 定义，
//! 因此这里保留领域级端口，并提供 `GrpcVMRankerClient` 适配到本地重建的
//! `vm_ranker.proto`。集群选择与 value model 由装配显式配置而非 feature
//! switch（U1）；对接 X 内部服务需要另写 Adapter。

use crate::models::candidate::PhoenixScores;
use tonic::async_trait;
use x_algorithm_proto::vm_ranker as pb;

/// 对应上游 `RankCandidate`。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VmRankCandidate {
    pub tweet_id: u64,
    pub author_id: u64,
    pub in_network: bool,
    pub is_retweet: bool,
    pub is_reply: bool,
    pub author_followers_count: i32,
    pub vqv_ineligible: bool,
    pub retweeted_tweet_id: Option<u64>,
    pub score: Option<f64>,
    pub phoenix_scores: PhoenixScores,
}

/// 对应上游 `DppParams`（多样性重排参数）。
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DppParams {
    pub theta: f64,
    pub max_selected_rank: u32,
}

/// 对应上游 `RankRequest`。上游从私有 feature switch 读取的 value model、
/// DPP 与新用户阈值改由装配/Adapter 配置显式传入。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VmRankRequest {
    pub viewer_id: u64,
    pub request_timestamp_ms: i64,
    pub viewer_following_count: usize,
    pub value_model_id: Option<String>,
    pub dpp_params: Option<DppParams>,
    pub candidates: Vec<VmRankCandidate>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct VmRankedCandidate {
    pub tweet_id: u64,
    pub score: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct VmRankResponse {
    pub candidates: Vec<VmRankedCandidate>,
}

#[async_trait]
pub trait VMRankerClient: Send + Sync {
    async fn rank(&self, request: VmRankRequest) -> Result<VmRankResponse, String>;
}

/// 真实 gRPC Adapter：调用本仓库 `vm-ranker` 服务（wire 为本地重建的
/// `vm_ranker.proto`，与上游内部服务不保证兼容）。由装配在
/// `HOME_MIXER_ENABLE_VM_RANKER=1` 且提供 `VM_RANKER_GRPC_ADDR` 时注入。
pub struct GrpcVMRankerClient {
    endpoint: String,
    timeout: std::time::Duration,
}

impl GrpcVMRankerClient {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            timeout: std::time::Duration::from_millis(crate::params::VM_RANKER_TIMEOUT_MS),
        }
    }

    fn to_proto(request: VmRankRequest) -> pb::RankRequest {
        pb::RankRequest {
            viewer_id: request.viewer_id,
            value_model_id: request.value_model_id.unwrap_or_default(),
            request_timestamp_ms: u64::try_from(request.request_timestamp_ms).unwrap_or(0),
            viewer_following_count: u32::try_from(request.viewer_following_count).unwrap_or(0),
            dpp_params: request.dpp_params.map(|p| pb::DppParams {
                theta: p.theta,
                max_selected_rank: p.max_selected_rank,
            }),
            new_user_age_threshold_secs: None,
            candidates: request
                .candidates
                .into_iter()
                .map(|c| pb::RankCandidate {
                    tweet_id: c.tweet_id,
                    author_id: c.author_id,
                    in_network: c.in_network,
                    is_retweet: c.is_retweet,
                    is_reply: c.is_reply,
                    author_followers_count: c.author_followers_count,
                    vqv_ineligible: c.vqv_ineligible,
                    retweeted_tweet_id: c.retweeted_tweet_id.unwrap_or(0),
                    score: c.score,
                    phoenix_scores: Some(pb::PhoenixScores {
                        favorite_score: c.phoenix_scores.favorite_score,
                        reply_score: c.phoenix_scores.reply_score,
                        retweet_score: c.phoenix_scores.retweet_score,
                        photo_expand_score: c.phoenix_scores.photo_expand_score,
                        click_score: c.phoenix_scores.click_score,
                        profile_click_score: c.phoenix_scores.profile_click_score,
                        vqv_score: c.phoenix_scores.vqv_score,
                        share_score: c.phoenix_scores.share_score,
                        share_via_dm_score: c.phoenix_scores.share_via_dm_score,
                        share_via_copy_link_score: c.phoenix_scores.share_via_copy_link_score,
                        dwell_score: c.phoenix_scores.dwell_score,
                        quote_score: c.phoenix_scores.quote_score,
                        quoted_click_score: c.phoenix_scores.quoted_click_score,
                        follow_author_score: c.phoenix_scores.follow_author_score,
                        not_interested_score: c.phoenix_scores.not_interested_score,
                        block_author_score: c.phoenix_scores.block_author_score,
                        mute_author_score: c.phoenix_scores.mute_author_score,
                        report_score: c.phoenix_scores.report_score,
                        not_dwelled_score: c.phoenix_scores.not_dwelled_score,
                        dwell_time: c.phoenix_scores.dwell_time,
                        click_dwell_time: c.phoenix_scores.click_dwell_time,
                        video_open_score: c.phoenix_scores.video_open_score,
                        open_link_score: c.phoenix_scores.open_link_score,
                        quoted_vqv_score: c.phoenix_scores.quoted_vqv_score,
                        post_unexplored_score: c.phoenix_scores.post_unexplored_score,
                        active_secs_5m_residual_norm: c.phoenix_scores.active_secs_5m_residual_norm,
                    }),
                    slate_context: None,
                    head_weights: None,
                    weighted_score: None,
                })
                .collect(),
        }
    }
}

#[async_trait]
impl VMRankerClient for GrpcVMRankerClient {
    async fn rank(&self, request: VmRankRequest) -> Result<VmRankResponse, String> {
        let channel = tonic::transport::Endpoint::from_shared(self.endpoint.clone())
            .map_err(|e| format!("invalid VM Ranker endpoint: {e}"))?
            .timeout(self.timeout)
            .connect()
            .await
            .map_err(|e| format!("VM Ranker connect failed: {e}"))?;
        let mut client = pb::vm_ranker_service_client::VmRankerServiceClient::new(channel)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .send_compressed(tonic::codec::CompressionEncoding::Gzip);

        let response = client
            .rank(Self::to_proto(request))
            .await
            .map_err(|e| format!("VM Ranker rank failed: {e}"))?
            .into_inner();

        Ok(VmRankResponse {
            candidates: response
                .candidates
                .into_iter()
                .map(|c| VmRankedCandidate {
                    tweet_id: c.tweet_id,
                    score: c.score,
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 每个预测头取互不相同的值，任何字段错位都会让断言失败。
    fn distinct_phoenix_scores() -> PhoenixScores {
        PhoenixScores {
            favorite_score: Some(1.0),
            reply_score: Some(2.0),
            retweet_score: Some(3.0),
            photo_expand_score: Some(4.0),
            click_score: Some(5.0),
            profile_click_score: Some(6.0),
            vqv_score: Some(7.0),
            share_score: Some(8.0),
            share_via_dm_score: Some(9.0),
            share_via_copy_link_score: Some(10.0),
            dwell_score: Some(11.0),
            quote_score: Some(12.0),
            quoted_click_score: Some(13.0),
            quoted_vqv_score: Some(14.0),
            follow_author_score: Some(15.0),
            not_interested_score: Some(16.0),
            block_author_score: Some(17.0),
            mute_author_score: Some(18.0),
            report_score: Some(19.0),
            not_dwelled_score: Some(20.0),
            video_open_score: Some(21.0),
            open_link_score: Some(22.0),
            post_unexplored_score: Some(23.0),
            dwell_time: Some(24.0),
            click_dwell_time: Some(25.0),
            active_secs_5m_residual_norm: Some(26.0),
        }
    }

    #[test]
    fn phoenix_score_heads_map_to_matching_proto_slots() {
        let request = VmRankRequest {
            candidates: vec![VmRankCandidate {
                phoenix_scores: distinct_phoenix_scores(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let proto = GrpcVMRankerClient::to_proto(request);
        let scores = proto.candidates[0].phoenix_scores.unwrap();

        assert_eq!(scores.favorite_score, Some(1.0));
        assert_eq!(scores.reply_score, Some(2.0));
        assert_eq!(scores.retweet_score, Some(3.0));
        assert_eq!(scores.photo_expand_score, Some(4.0));
        assert_eq!(scores.click_score, Some(5.0));
        assert_eq!(scores.profile_click_score, Some(6.0));
        assert_eq!(scores.vqv_score, Some(7.0));
        assert_eq!(scores.share_score, Some(8.0));
        assert_eq!(scores.share_via_dm_score, Some(9.0));
        assert_eq!(scores.share_via_copy_link_score, Some(10.0));
        assert_eq!(scores.dwell_score, Some(11.0));
        assert_eq!(scores.quote_score, Some(12.0));
        assert_eq!(scores.quoted_click_score, Some(13.0));
        assert_eq!(scores.quoted_vqv_score, Some(14.0));
        assert_eq!(scores.follow_author_score, Some(15.0));
        assert_eq!(scores.not_interested_score, Some(16.0));
        assert_eq!(scores.block_author_score, Some(17.0));
        assert_eq!(scores.mute_author_score, Some(18.0));
        assert_eq!(scores.report_score, Some(19.0));
        assert_eq!(scores.not_dwelled_score, Some(20.0));
        assert_eq!(scores.video_open_score, Some(21.0));
        assert_eq!(scores.open_link_score, Some(22.0));
        assert_eq!(scores.post_unexplored_score, Some(23.0));
        assert_eq!(scores.dwell_time, Some(24.0));
        assert_eq!(scores.click_dwell_time, Some(25.0));
        assert_eq!(scores.active_secs_5m_residual_norm, Some(26.0));
    }

    #[test]
    fn candidate_and_request_fields_map_to_proto() {
        let request = VmRankRequest {
            viewer_id: 42,
            request_timestamp_ms: 1_700_000_000_000,
            viewer_following_count: 7,
            value_model_id: Some("model-a".to_string()),
            dpp_params: Some(DppParams {
                theta: 0.25,
                max_selected_rank: 60,
            }),
            candidates: vec![VmRankCandidate {
                tweet_id: 11,
                author_id: 22,
                in_network: true,
                is_retweet: true,
                is_reply: false,
                author_followers_count: 333,
                vqv_ineligible: true,
                retweeted_tweet_id: Some(44),
                score: Some(0.5),
                phoenix_scores: PhoenixScores::default(),
            }],
        };

        let proto = GrpcVMRankerClient::to_proto(request);

        assert_eq!(proto.viewer_id, 42);
        assert_eq!(proto.request_timestamp_ms, 1_700_000_000_000);
        assert_eq!(proto.viewer_following_count, 7);
        assert_eq!(proto.value_model_id, "model-a");
        let dpp = proto.dpp_params.unwrap();
        assert_eq!(dpp.theta, 0.25);
        assert_eq!(dpp.max_selected_rank, 60);

        let candidate = &proto.candidates[0];
        assert_eq!(candidate.tweet_id, 11);
        assert_eq!(candidate.author_id, 22);
        assert!(candidate.in_network);
        assert!(candidate.is_retweet);
        assert!(!candidate.is_reply);
        assert_eq!(candidate.author_followers_count, 333);
        assert!(candidate.vqv_ineligible);
        assert_eq!(candidate.retweeted_tweet_id, 44);
        assert_eq!(candidate.score, Some(0.5));
    }

    /// 上游以 0 表示"非转推"，缺省的 `value_model_id` 在服务端记为 unknown。
    #[test]
    fn absent_optionals_use_upstream_defaults() {
        let request = VmRankRequest {
            candidates: vec![VmRankCandidate::default()],
            ..Default::default()
        };

        let proto = GrpcVMRankerClient::to_proto(request);

        assert_eq!(proto.value_model_id, "");
        assert!(proto.dpp_params.is_none());
        assert!(proto.new_user_age_threshold_secs.is_none());
        assert_eq!(proto.candidates[0].retweeted_tweet_id, 0);
        assert_eq!(proto.candidates[0].score, None);
    }

    /// 负时间戳来自上游没有的输入，转换必须落到 0 而不是回绕成巨大的正数。
    #[test]
    fn negative_timestamp_saturates_to_zero() {
        let request = VmRankRequest {
            request_timestamp_ms: -1,
            ..Default::default()
        };

        assert_eq!(
            GrpcVMRankerClient::to_proto(request).request_timestamp_ms,
            0
        );
    }
}
