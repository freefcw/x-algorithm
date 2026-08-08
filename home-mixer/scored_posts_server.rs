use crate::candidate_pipeline::candidate::CandidateHelpers;
use crate::candidate_pipeline::phoenix_candidate_pipeline::PhoenixCandidatePipeline;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use log::info;
use std::sync::Arc;
use std::time::Instant;
use x_algorithm_proto::home_mixer as pb;
use x_algorithm_proto::home_mixer::ScoredPost;
use xai_candidate_pipeline::candidate_pipeline::CandidatePipeline;

pub struct ScoredPostsOutput {
    pub posts: Vec<ScoredPost>,
    pub request_id: String,
}

pub struct ScoredPostsServer {
    pipeline: Arc<PhoenixCandidatePipeline>,
}

impl ScoredPostsServer {
    pub async fn new() -> Self {
        Self::with_pipeline(PhoenixCandidatePipeline::prod().await)
    }

    pub fn with_pipeline(pipeline: PhoenixCandidatePipeline) -> Self {
        let pipeline = Arc::new(pipeline);
        for stage in pipeline.components() {
            info!(
                "Scored Posts components - stage={:?} components=[{}]",
                stage.stage,
                stage.components.join(", ")
            );
        }
        Self { pipeline }
    }

    pub async fn score(&self, query: ScoredPostsQuery) -> ScoredPostsOutput {
        let start = Instant::now();
        let pipeline_result = self.pipeline.execute(query).await;
        let request_id = pipeline_result.query.request_id.clone();
        let posts = pipeline_result
            .selected_candidates
            .into_iter()
            .map(candidate_to_scored_post)
            .collect::<Vec<_>>();

        info!(
            "Scored Posts response - request_id {} - {} posts ({} ms)",
            request_id,
            posts.len(),
            start.elapsed().as_millis()
        );
        ScoredPostsOutput { posts, request_id }
    }
}

fn candidate_to_scored_post(
    candidate: crate::candidate_pipeline::candidate::PostCandidate,
) -> ScoredPost {
    let screen_names = candidate.get_screen_names();
    ScoredPost {
        tweet_id: candidate.tweet_id as u64,
        author_id: candidate.author_id,
        retweeted_tweet_id: candidate.retweeted_tweet_id.unwrap_or(0),
        retweeted_user_id: candidate.retweeted_user_id.unwrap_or(0),
        in_reply_to_tweet_id: candidate.in_reply_to_tweet_id.unwrap_or(0),
        score: candidate.score.unwrap_or(0.0) as f32,
        in_network: candidate.in_network.unwrap_or(false),
        served_type: candidate
            .served_type
            .map(|value| value as i32)
            .unwrap_or_default(),
        last_scored_timestamp_ms: candidate.last_scored_at_ms.unwrap_or(0),
        prediction_request_id: candidate.prediction_request_id.unwrap_or(0),
        ancestors: candidate.ancestors,
        screen_names,
        visibility_reason: candidate.visibility_reason.map(|reason| {
            let (reason_code, description) = reason.into_proto();
            pb::VisibilityFilteredReason {
                reason_code,
                description,
            }
        }),
        brand_safety_verdict: candidate
            .brand_safety_verdict
            .map(pb::BrandSafetyVerdict::from)
            .unwrap_or(pb::BrandSafetyVerdict::Unspecified) as i32,
        tweet_text: candidate.tweet_text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate_pipeline::candidate::{BrandSafetyVerdict, PostCandidate};

    #[test]
    fn candidate_brand_safety_verdict_reaches_scored_post() {
        let cases = [
            (
                BrandSafetyVerdict::Unspecified,
                pb::BrandSafetyVerdict::Unspecified,
            ),
            (
                BrandSafetyVerdict::Safe,
                pb::BrandSafetyVerdict::SafeForAdjacency,
            ),
            (BrandSafetyVerdict::LowRisk, pb::BrandSafetyVerdict::LowRisk),
            (
                BrandSafetyVerdict::MediumRisk,
                pb::BrandSafetyVerdict::AvoidAdjacency,
            ),
        ];

        for (domain_verdict, wire_verdict) in cases {
            let scored_post = candidate_to_scored_post(PostCandidate {
                brand_safety_verdict: Some(domain_verdict),
                ..Default::default()
            });

            assert_eq!(scored_post.brand_safety_verdict, wire_verdict as i32);
        }
    }
}
