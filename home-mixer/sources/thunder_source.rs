use crate::clients::thunder_client::{ThunderClient, ThunderCluster};
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::params as p;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use x_algorithm_proto::home_mixer as pb;
use x_algorithm_proto::thunder::in_network_posts_service_client::InNetworkPostsServiceClient;
use x_algorithm_proto::thunder::{GetInNetworkPostsRequest, LightPost};
use xai_candidate_pipeline::source::Source;

pub struct ThunderSource {
    pub thunder_client: Arc<ThunderClient>,
}

fn candidate_from_light_post(post: LightPost) -> Option<PostCandidate> {
    let tweet_id = valid_id(post.post_id)?;
    let author_id = valid_id(post.author_id)?;
    let in_reply_to_tweet_id = post.in_reply_to_post_id.and_then(valid_id);
    let conversation_id = post.conversation_id.and_then(valid_id);
    let retweeted_tweet_id = post.source_post_id.and_then(valid_id);
    let retweeted_user_id = post.source_user_id.and_then(valid_id);

    let mut ancestors = Vec::new();
    if let Some(reply_to) = in_reply_to_tweet_id {
        ancestors.push(reply_to);
        if let Some(root) = conversation_id.filter(|&root| root != reply_to) {
            ancestors.push(root);
        }
    }

    Some(PostCandidate {
        tweet_id,
        author_id,
        in_reply_to_tweet_id,
        retweeted_tweet_id,
        retweeted_user_id,
        ancestors,
        ..Default::default()
    })
}

fn valid_id(id: i64) -> Option<u64> {
    u64::try_from(id).ok().filter(|id| *id != 0)
}

#[async_trait]
impl Source<ScoredPostsQuery, PostCandidate> for ThunderSource {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.has_cached_posts
    }

    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        let cluster = ThunderCluster::Amp;
        let channel = self
            .thunder_client
            .get_random_channel(cluster)
            .ok_or_else(|| "ThunderSource: no available channel".to_string())?;

        let mut client = InNetworkPostsServiceClient::new(channel.clone());
        let request = thunder_request(query);

        let response = tokio::time::timeout(
            Duration::from_millis(p::THUNDER_REQUEST_TIMEOUT_MS),
            client.get_in_network_posts(request),
        )
        .await
        .map_err(|_| {
            format!(
                "ThunderSource: timed out after {}ms",
                p::THUNDER_REQUEST_TIMEOUT_MS
            )
        })?
        .map_err(|e| format!("ThunderSource: {}", e))?;

        let served_type = if query.in_network_only {
            pb::ServedType::RankedFollowing
        } else {
            pb::ServedType::ForYouInNetwork
        };
        let candidates: Vec<PostCandidate> = response
            .into_inner()
            .posts
            .into_iter()
            .filter_map(candidate_from_light_post)
            .map(|candidate| PostCandidate {
                served_type: Some(served_type),
                ..candidate
            })
            .collect();

        Ok(candidates)
    }
}

fn thunder_request(query: &ScoredPostsQuery) -> GetInNetworkPostsRequest {
    GetInNetworkPostsRequest {
        user_id: query.user_id,
        following_user_ids: query
            .user_features
            .followed_user_ids
            .iter()
            .filter_map(|&id| valid_id(id))
            .collect(),
        max_results: p::THUNDER_MAX_RESULTS,
        exclude_tweet_ids: query.seen_ids.clone(),
        algorithm: "default".to_string(),
        debug: false,
        is_video_request: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_filters_negative_following_ids_and_forwards_seen_ids() {
        let query = ScoredPostsQuery {
            user_id: 42,
            seen_ids: vec![100, 200],
            user_features: crate::models::user_features::UserFeatures {
                followed_user_ids: vec![-1, 0, 10, 20],
                ..Default::default()
            },
            ..Default::default()
        };

        let request = thunder_request(&query);

        assert_eq!(request.user_id, 42);
        assert_eq!(request.following_user_ids, vec![10, 20]);
        assert_eq!(request.exclude_tweet_ids, vec![100, 200]);
        assert_eq!(request.max_results, p::THUNDER_MAX_RESULTS);
    }

    #[test]
    fn rejects_nonpositive_thunder_post_or_author_ids() {
        assert!(candidate_from_light_post(LightPost {
            post_id: -1,
            author_id: 1,
            ..Default::default()
        })
        .is_none());
        assert!(candidate_from_light_post(LightPost {
            post_id: 1,
            author_id: -1,
            ..Default::default()
        })
        .is_none());
        assert!(candidate_from_light_post(LightPost {
            post_id: 0,
            author_id: 1,
            ..Default::default()
        })
        .is_none());
    }

    #[test]
    fn maps_valid_thunder_ids_and_conversation_ancestors() {
        let candidate = candidate_from_light_post(LightPost {
            post_id: 10,
            author_id: 20,
            in_reply_to_post_id: Some(30),
            conversation_id: Some(40),
            source_post_id: Some(50),
            source_user_id: Some(60),
            ..Default::default()
        })
        .expect("valid light post");

        assert_eq!(candidate.tweet_id, 10);
        assert_eq!(candidate.author_id, 20);
        assert_eq!(candidate.retweeted_tweet_id, Some(50));
        assert_eq!(candidate.retweeted_user_id, Some(60));
        assert_eq!(candidate.ancestors, vec![30, 40]);
    }
}
