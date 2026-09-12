#[cfg(feature = "legacy-int-ids")]
use crate::clients::thunder_client::{ThunderClient, ThunderCluster};
use crate::models::ids::{PostId, UserId};
use crate::models::query::ScoredPostsQuery;
#[cfg(feature = "legacy-int-ids")]
use crate::params as p;
use tonic::async_trait;
#[cfg(feature = "legacy-int-ids")]
use x_algorithm_proto::thunder::in_network_posts_service_client::InNetworkPostsServiceClient;
#[cfg(feature = "legacy-int-ids")]
use x_algorithm_proto::thunder::{GetInNetworkPostsRequest, LightPost};

/// Light in-network / fallback post used by ThunderSource and FallbackSource.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InNetworkPost {
    pub tweet_id: PostId,
    pub author_id: UserId,
    pub created_at_ms: Option<u64>,
    pub in_reply_to_tweet_id: Option<PostId>,
    pub retweeted_tweet_id: Option<PostId>,
    pub retweeted_user_id: Option<UserId>,
    pub ancestors: Vec<PostId>,
}

#[async_trait]
pub trait InNetworkPostsClient: Send + Sync {
    async fn get_in_network_posts(
        &self,
        query: &ScoredPostsQuery,
        max_results: u32,
    ) -> Result<Vec<InNetworkPost>, String>;

    async fn get_fallback_posts(
        &self,
        query: &ScoredPostsQuery,
        max_results: u32,
    ) -> Result<Vec<InNetworkPost>, String> {
        let _ = (query, max_results);
        Ok(Vec::new())
    }
}

/// Adapter for the legacy integer Thunder RPC.
///
/// The business-facing pipeline speaks in `ObjectId`s.  Until P3 changes
/// `in_network.proto` to string IDs, this adapter only round-trips the padded
/// demo/test IDs and drops identities that cannot be represented losslessly.
/// Explicit no-op adapter used when the legacy integer Thunder contract is
/// compiled out. The source remains present but fails closed, so a caller
/// cannot mistake the feature-disabled build for a working network source.
pub struct DisabledInNetworkPostsClient;

#[async_trait]
impl InNetworkPostsClient for DisabledInNetworkPostsClient {
    async fn get_in_network_posts(
        &self,
        _query: &ScoredPostsQuery,
        _max_results: u32,
    ) -> Result<Vec<InNetworkPost>, String> {
        Err("legacy integer Thunder adapter is disabled".to_string())
    }
}

#[cfg(feature = "legacy-int-ids")]
#[async_trait]
impl InNetworkPostsClient for ThunderClient {
    async fn get_in_network_posts(
        &self,
        query: &ScoredPostsQuery,
        max_results: u32,
    ) -> Result<Vec<InNetworkPost>, String> {
        let channel = self
            .get_random_channel(ThunderCluster::Amp)
            .ok_or_else(|| "ThunderClient: no available channel".to_string())?;
        let mut client = InNetworkPostsServiceClient::new(channel);
        let mut request = thunder_request(query);
        request.max_results = request.max_results.min(max_results);
        let response = tokio::time::timeout(
            std::time::Duration::from_millis(p::THUNDER_REQUEST_TIMEOUT_MS),
            client.get_in_network_posts(request),
        )
        .await
        .map_err(|_| {
            format!(
                "ThunderClient: timed out after {}ms",
                p::THUNDER_REQUEST_TIMEOUT_MS
            )
        })?
        .map_err(|error| format!("ThunderClient: {error}"))?
        .into_inner();

        Ok(response
            .posts
            .into_iter()
            .filter_map(in_network_post_from_light_post)
            .collect())
    }
}

#[cfg(feature = "legacy-int-ids")]
fn in_network_post_from_light_post(post: LightPost) -> Option<InNetworkPost> {
    let tweet_id = valid_id(post.post_id)?;
    let author_id = valid_id(post.author_id)?;
    let in_reply_to_tweet_id = post.in_reply_to_post_id.and_then(valid_id);
    let retweeted_tweet_id = post.source_post_id.and_then(valid_id);
    let retweeted_user_id = post.source_user_id.and_then(valid_id);
    let mut ancestors = Vec::new();
    if let Some(reply_to) = in_reply_to_tweet_id {
        ancestors.push(reply_to);
        if let Some(root) = post
            .conversation_id
            .and_then(valid_id)
            .filter(|root| *root != reply_to)
        {
            ancestors.push(root);
        }
    }
    Some(InNetworkPost {
        tweet_id,
        author_id,
        created_at_ms: u64::try_from(post.created_at)
            .ok()
            .map(|seconds| seconds.saturating_mul(1_000)),
        in_reply_to_tweet_id,
        retweeted_tweet_id,
        retweeted_user_id,
        ancestors,
    })
}

#[cfg(feature = "legacy-int-ids")]
fn valid_id(id: i64) -> Option<crate::models::ObjectId> {
    u64::try_from(id)
        .ok()
        .filter(|id| *id != 0)
        .map(crate::models::ObjectId::from_u64_be_padded)
}

#[cfg(feature = "legacy-int-ids")]
fn thunder_u64(id: crate::models::ObjectId) -> Option<u64> {
    id.to_u64_be_padded().filter(|id| *id != 0)
}

#[cfg(feature = "legacy-int-ids")]
fn thunder_request(query: &ScoredPostsQuery) -> GetInNetworkPostsRequest {
    GetInNetworkPostsRequest {
        user_id: thunder_u64(query.user_id).unwrap_or(0),
        following_user_ids: query
            .user_features
            .followed_user_ids
            .iter()
            .copied()
            .filter_map(thunder_u64)
            .collect(),
        max_results: p::THUNDER_MAX_RESULTS,
        exclude_tweet_ids: query
            .seen_ids
            .iter()
            .copied()
            .filter_map(thunder_u64)
            .collect(),
        algorithm: "default".to_string(),
        debug: false,
        is_video_request: false,
    }
}

/// Demo fallback pool: recent `from_parts` posts authored by the demo follow set.
pub struct DemoFallbackPostsClient;

#[async_trait]
impl InNetworkPostsClient for DemoFallbackPostsClient {
    async fn get_in_network_posts(
        &self,
        _query: &ScoredPostsQuery,
        _max_results: u32,
    ) -> Result<Vec<InNetworkPost>, String> {
        Ok(Vec::new())
    }

    async fn get_fallback_posts(
        &self,
        _query: &ScoredPostsQuery,
        max_results: u32,
    ) -> Result<Vec<InNetworkPost>, String> {
        let now_ms = u64::try_from(x_algorithm_proto::demo::now_ms()).unwrap_or(0);
        let authors = x_algorithm_proto::demo::DEMO_AUTHOR_IDS;
        let n = max_results.min(40) as usize;
        Ok((0..n)
            .map(|index| {
                let ts =
                    u32::try_from(now_ms.saturating_sub(index as u64 * 60_000) / 1000).unwrap_or(0);
                let author = authors[index % authors.len()];
                InNetworkPost {
                    tweet_id: crate::models::ObjectId::from_parts(ts, 9_000_000 + index as u64),
                    author_id: crate::models::ObjectId::from_u64_be_padded(
                        u64::try_from(author).unwrap_or(0),
                    ),
                    created_at_ms: Some(now_ms.saturating_sub(index as u64 * 60_000)),
                    ..Default::default()
                }
            })
            .collect())
    }
}

#[cfg(all(test, feature = "legacy-int-ids"))]
mod tests {
    use super::*;
    use crate::models::uid;

    #[test]
    fn legacy_light_post_maps_to_object_ids_and_created_at() {
        let post = in_network_post_from_light_post(LightPost {
            post_id: 10,
            author_id: 20,
            created_at: 1_700_000_000,
            in_reply_to_post_id: Some(30),
            conversation_id: Some(40),
            source_post_id: Some(50),
            source_user_id: Some(60),
            ..Default::default()
        })
        .expect("valid legacy post");

        assert_eq!(post.tweet_id, crate::models::pid(10));
        assert_eq!(post.author_id, crate::models::uid(20));
        assert_eq!(post.created_at_ms, Some(1_700_000_000_000));
        assert_eq!(post.in_reply_to_tweet_id, Some(crate::models::pid(30)));
        assert_eq!(post.retweeted_tweet_id, Some(crate::models::pid(50)));
        assert_eq!(post.retweeted_user_id, Some(crate::models::uid(60)));
        assert_eq!(
            post.ancestors,
            vec![crate::models::pid(30), crate::models::pid(40)]
        );
    }

    #[test]
    fn legacy_light_post_drops_zero_or_negative_identity() {
        assert!(in_network_post_from_light_post(LightPost {
            post_id: 0,
            author_id: 1,
            ..Default::default()
        })
        .is_none());
        assert!(in_network_post_from_light_post(LightPost {
            post_id: 1,
            author_id: -1,
            ..Default::default()
        })
        .is_none());
    }

    #[tokio::test]
    async fn demo_fallback_emits_recent_object_ids() {
        let posts = DemoFallbackPostsClient
            .get_fallback_posts(
                &ScoredPostsQuery {
                    user_id: uid(1),
                    ..Default::default()
                },
                5,
            )
            .await
            .expect("demo fallback");
        assert_eq!(posts.len(), 5);
        assert!(posts.iter().all(|post| !post.tweet_id.is_nil()));
        assert!(posts.iter().all(|post| post.created_at_ms.is_some()));
    }
}
