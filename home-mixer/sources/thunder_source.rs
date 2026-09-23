use crate::clients::in_network_posts_client::{InNetworkPost, InNetworkPostsClient};
use crate::models::candidate::{PostCandidate, RetrievalSource};
use crate::models::query::ScoredPostsQuery;
use crate::params as p;
use std::sync::Arc;
use tonic::async_trait;
use x_algorithm_proto::home_mixer as pb;
use xai_candidate_pipeline::source::Source;

pub struct ThunderSource {
    pub client: Arc<dyn InNetworkPostsClient>,
}

fn candidate_from_post(post: InNetworkPost, served_type: pb::ServedType) -> PostCandidate {
    PostCandidate {
        tweet_id: post.tweet_id,
        author_id: post.author_id,
        created_at_ms: post.created_at_ms,
        in_reply_to_tweet_id: post.in_reply_to_tweet_id,
        retweeted_tweet_id: post.retweeted_tweet_id,
        retweeted_user_id: post.retweeted_user_id,
        ancestors: post.ancestors,
        served_type: Some(served_type),
        retrieval_sources: vec![RetrievalSource::from_served_type(served_type)],
        in_network: Some(true),
        ..Default::default()
    }
}

#[async_trait]
impl Source<ScoredPostsQuery, PostCandidate> for ThunderSource {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.has_cached_posts
    }

    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        let served_type = if query.in_network_only {
            pb::ServedType::RankedFollowing
        } else {
            pb::ServedType::ForYouInNetwork
        };
        let posts = self
            .client
            .get_in_network_posts(query, p::THUNDER_MAX_RESULTS)
            .await?;
        Ok(posts
            .into_iter()
            .map(|post| candidate_from_post(post, served_type))
            .collect())
    }
}
