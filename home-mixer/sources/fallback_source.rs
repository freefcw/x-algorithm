use crate::clients::in_network_posts_client::{InNetworkPost, InNetworkPostsClient};
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::params as p;
use std::sync::Arc;
use tonic::async_trait;
use x_algorithm_proto::home_mixer as pb;
use xai_candidate_pipeline::source::Source;

pub struct FallbackSource {
    pub client: Arc<dyn InNetworkPostsClient>,
}

fn candidate_from_post(post: InNetworkPost) -> PostCandidate {
    PostCandidate {
        tweet_id: post.tweet_id,
        author_id: post.author_id,
        created_at_ms: post.created_at_ms,
        in_reply_to_tweet_id: post.in_reply_to_tweet_id,
        retweeted_tweet_id: post.retweeted_tweet_id,
        retweeted_user_id: post.retweeted_user_id,
        ancestors: post.ancestors,
        served_type: Some(pb::ServedType::ForYouPhoenixRetrieval),
        in_network: Some(false),
        ..Default::default()
    }
}

#[async_trait]
impl Source<ScoredPostsQuery, PostCandidate> for FallbackSource {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.in_network_only && !query.has_cached_posts
    }

    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        let posts = self
            .client
            .get_fallback_posts(query, p::PHOENIX_MAX_RESULTS.min(200))
            .await?;
        Ok(posts.into_iter().map(candidate_from_post).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::in_network_posts_client::DisabledInNetworkPostsClient;

    #[test]
    fn disabled_for_in_network_only() {
        let source = FallbackSource {
            client: Arc::new(DisabledInNetworkPostsClient),
        };
        assert!(!source.enable(&ScoredPostsQuery {
            in_network_only: true,
            ..Default::default()
        }));
        assert!(source.enable(&ScoredPostsQuery::default()));
    }
}
