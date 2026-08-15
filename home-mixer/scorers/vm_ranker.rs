//! 上游同构的 VM Ranker 二次排序 Scorer（RANK-03）。
//!
//! 默认关闭：`HOME_MIXER_ENABLE_VM_RANKER=1` 且提供 `VM_RANKER_GRPC_ADDR`
//! 时，由装配注入 `GrpcVMRankerClient`（指向本仓库 `vm-ranker` 服务）。
//! 上游按 feature switch 启用并选择集群；本地把这两项决策交给装配/Adapter
//! （U1）。客户端失败时按候选数量返回错误，由流水线失败隔离处理，不伪造
//! 分数——主链保留 `RankingScorer` 的分数继续出流。

use crate::clients::vm_ranker_client::{VMRankerClient, VmRankCandidate, VmRankRequest};
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::params::MIN_VIDEO_DURATION_MS;
use crate::scorers::author_cold_start::AuthorColdStart;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::scorer::Scorer;

pub struct VMRanker {
    pub client: Arc<dyn VMRankerClient>,
    /// 上游从 feature switch 读取；本地由装配显式配置。
    pub value_model_id: Option<String>,
    pub author_cold_start: AuthorColdStart,
}

#[async_trait]
impl Scorer<ScoredPostsQuery, PostCandidate> for VMRanker {
    async fn score(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let request = build_request(query, candidates, self.value_model_id.clone());

        let response = match self.client.rank(request).await {
            Ok(response) => response,
            Err(error) => {
                let message = format!("VMRanker call failed: {error}");
                return vec![Err(message); candidates.len()];
            }
        };

        let score_map: HashMap<u64, f64> = response
            .candidates
            .iter()
            .map(|scored| (scored.tweet_id, scored.score))
            .collect();

        let base_scores: Vec<Option<f64>> = candidates
            .iter()
            .map(|candidate| {
                score_map
                    .get(&candidate.tweet_id)
                    .copied()
                    .or(candidate.score)
            })
            .collect();
        let scores = if self.author_cold_start.is_enabled() {
            let numeric_scores: Vec<f64> = base_scores
                .iter()
                .map(|score| score.unwrap_or(0.0))
                .collect();
            self.author_cold_start
                .apply(candidates, &numeric_scores)
                .into_iter()
                .map(Some)
                .collect()
        } else {
            base_scores
        };

        candidates
            .iter()
            .zip(scores)
            .map(|(_candidate, score)| {
                Ok(PostCandidate {
                    score,
                    ..Default::default()
                })
            })
            .collect()
    }

    fn update(&self, candidate: &mut PostCandidate, scored: PostCandidate) {
        candidate.score = scored.score;
    }
}

fn build_request(
    query: &ScoredPostsQuery,
    candidates: &[PostCandidate],
    value_model_id: Option<String>,
) -> VmRankRequest {
    let proto_candidates: Vec<VmRankCandidate> = candidates
        .iter()
        .map(|candidate| VmRankCandidate {
            tweet_id: candidate.tweet_id,
            author_id: candidate.author_id,
            in_network: candidate.in_network.unwrap_or(false),
            is_retweet: candidate.retweeted_tweet_id.is_some(),
            is_reply: candidate.in_reply_to_tweet_id.is_some(),
            author_followers_count: candidate.author_followers_count.unwrap_or(0),
            // 与本地 VQV 计权门槛一致：视频时长不足或缺失时不计 VQV。
            vqv_ineligible: candidate
                .video_duration_ms
                .is_none_or(|ms| ms <= MIN_VIDEO_DURATION_MS),
            retweeted_tweet_id: candidate.retweeted_tweet_id,
            score: candidate.score,
            phoenix_scores: candidate.phoenix_scores.clone(),
        })
        .collect();

    VmRankRequest {
        viewer_id: query.user_id,
        request_timestamp_ms: query.request_time_ms,
        viewer_following_count: query.user_features.followed_user_ids.len(),
        value_model_id,
        // 上游 DPP 参数来自 feature switch；本地待装配显式配置后传入。
        dpp_params: None,
        candidates: proto_candidates,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::vm_ranker_client::{VmRankResponse, VmRankedCandidate};

    struct FakeVmRanker {
        response: Result<VmRankResponse, String>,
    }

    #[async_trait]
    impl VMRankerClient for FakeVmRanker {
        async fn rank(&self, _request: VmRankRequest) -> Result<VmRankResponse, String> {
            self.response.clone()
        }
    }

    fn candidates() -> Vec<PostCandidate> {
        vec![
            PostCandidate {
                tweet_id: 1,
                score: Some(0.1),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2,
                score: Some(0.2),
                ..Default::default()
            },
        ]
    }

    #[tokio::test]
    async fn vm_scores_override_and_missing_scores_fall_back() {
        let scorer = VMRanker {
            client: Arc::new(FakeVmRanker {
                response: Ok(VmRankResponse {
                    candidates: vec![VmRankedCandidate {
                        tweet_id: 1,
                        score: 0.9,
                    }],
                }),
            }),
            value_model_id: None,
            author_cold_start: AuthorColdStart::default(),
        };

        let mut candidates = candidates();
        let scored = scorer
            .score(&ScoredPostsQuery::default(), &candidates)
            .await;
        assert_eq!(scored.len(), 2);
        scorer.update_all(&mut candidates, scored);

        assert_eq!(candidates[0].score, Some(0.9));
        assert_eq!(candidates[1].score, Some(0.2));
    }

    #[tokio::test]
    async fn client_failure_preserves_cardinality_and_existing_scores() {
        let scorer = VMRanker {
            client: Arc::new(FakeVmRanker {
                response: Err("vm ranker unavailable".to_string()),
            }),
            value_model_id: None,
            author_cold_start: AuthorColdStart::default(),
        };

        let mut candidates = candidates();
        let scored = scorer
            .score(&ScoredPostsQuery::default(), &candidates)
            .await;
        assert_eq!(scored.len(), 2);
        assert!(scored.iter().all(Result::is_err));
        scorer.update_all(&mut candidates, scored);

        assert_eq!(candidates[0].score, Some(0.1));
        assert_eq!(candidates[1].score, Some(0.2));
    }
}
