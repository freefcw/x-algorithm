use crate::clients::thunder_client::{ThunderClient, ThunderCluster};
use crate::models::ids::{PostId, UserId};
use crate::models::query::ScoredPostsQuery;
use crate::params as p;
use tonic::async_trait;
use x_algorithm_proto::thunder::in_network_posts_service_client::InNetworkPostsServiceClient;
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

/// Fail-closed in-network adapter used when neither Thunder nor mrpyq is configured.
pub struct DisabledInNetworkPostsClient;

#[async_trait]
impl InNetworkPostsClient for DisabledInNetworkPostsClient {
    async fn get_in_network_posts(
        &self,
        _query: &ScoredPostsQuery,
        _max_results: u32,
    ) -> Result<Vec<InNetworkPost>, String> {
        Err(
            "in-network source requires THUNDER_GRPC_ADDR or MRPYQ_RECOMMENDATION_DATA_ADDR"
                .to_string(),
        )
    }
}

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
        let mut request = thunder_request(query)?;
        request.max_results = request.max_results.min(max_results);
        let requested = request.max_results;

        let started = std::time::Instant::now();
        let response = match tokio::time::timeout(
            std::time::Duration::from_millis(p::THUNDER_REQUEST_TIMEOUT_MS),
            client.get_in_network_posts(request),
        )
        .await
        {
            Err(_) => {
                let message = format!(
                    "ThunderClient: timed out after {}ms",
                    p::THUNDER_REQUEST_TIMEOUT_MS
                );
                log::warn!(
                    "thunder rpc GetInNetworkPosts max_results={requested} elapsed_ms={} error={message}",
                    started.elapsed().as_millis(),
                );
                return Err(message);
            }
            Ok(Err(status)) => {
                log::warn!(
                    "thunder rpc GetInNetworkPosts max_results={requested} elapsed_ms={} code={} error={}",
                    started.elapsed().as_millis(),
                    status.code(),
                    status.message(),
                );
                return Err(format!("ThunderClient: {status}"));
            }
            Ok(Ok(response)) => response.into_inner(),
        };
        let elapsed_ms = started.elapsed().as_millis();

        let returned = response.posts.len();
        let posts: Vec<_> = response
            .posts
            .into_iter()
            .filter_map(in_network_post_from_light_post)
            .collect();
        // posts 与 usable 的差值是无法无损转成合法 Snowflake 而被丢弃的部分，
        // 否则这段丢弃在链路上不可见。
        log::info!(
            "thunder rpc GetInNetworkPosts max_results={requested} elapsed_ms={elapsed_ms} posts={returned} usable={}",
            posts.len(),
        );
        Ok(posts)
    }
}

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

fn valid_id(id: i64) -> Option<u64> {
    u64::try_from(id).ok().filter(|id| *id != 0)
}

/// Send the query's internal Snowflake IDs to Thunder's integer wire fields.
/// Out-of-range IDs fail closed rather than query user 0.
pub(crate) fn thunder_request(
    query: &ScoredPostsQuery,
) -> Result<GetInNetworkPostsRequest, String> {
    let user_id = checked_id(query.user_id, "user_id")?;
    let following_user_ids =
        checked_ids(&query.user_features.followed_user_ids, "following_user_ids")?;
    let exclude_tweet_ids = checked_ids(&query.seen_ids, "exclude_tweet_ids")?;
    Ok(GetInNetworkPostsRequest {
        user_id,
        following_user_ids,
        max_results: p::THUNDER_MAX_RESULTS,
        exclude_tweet_ids,
        algorithm: "default".to_string(),
        debug: false,
        is_video_request: false,
    })
}

fn checked_id(id: u64, field: &str) -> Result<u64, String> {
    if id == 0 || id > i64::MAX as u64 {
        return Err(format!(
            "Thunder adapter received out-of-range {field} id {id}"
        ));
    }
    Ok(id)
}

fn checked_ids(ids: &[u64], field: &str) -> Result<Vec<u64>, String> {
    ids.iter().map(|id| checked_id(*id, field)).collect()
}

#[cfg(test)]
mod disabled_client_tests {
    use super::*;
    use crate::models::uid;

    #[tokio::test]
    async fn disabled_client_fails_closed_instead_of_querying_user_zero() {
        let error = DisabledInNetworkPostsClient
            .get_in_network_posts(
                &ScoredPostsQuery {
                    user_id: uid(1),
                    ..ScoredPostsQuery::test_default()
                },
                10,
            )
            .await
            .expect_err("disabled adapter must not succeed");
        assert!(error.contains("MRPYQ_RECOMMENDATION_DATA_ADDR"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::uid;

    #[test]
    fn numeric_light_post_maps_to_internal_ids_and_created_at() {
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
        .expect("valid numeric post");

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
    fn numeric_light_post_drops_zero_or_negative_identity() {
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

    fn numeric_query(user_id: crate::models::UserId) -> ScoredPostsQuery {
        ScoredPostsQuery {
            user_id,
            user_features: crate::models::user_features::UserFeatures {
                followed_user_ids: vec![uid(201), uid(202)],
                ..Default::default()
            },
            seen_ids: vec![crate::models::pid(9)],
            ..ScoredPostsQuery::test_default()
        }
    }

    #[test]
    fn thunder_request_passes_internal_ids_through() {
        let request = thunder_request(&numeric_query(uid(1))).expect("internal ids");
        assert_eq!(request.user_id, 1);
        assert_eq!(request.following_user_ids, vec![201, 202]);
        assert_eq!(request.exclude_tweet_ids, vec![9]);
        assert_ne!(request.user_id, 0);
    }

    #[test]
    fn thunder_request_rejects_out_of_range_ids_instead_of_user_zero() {
        let mut query = numeric_query(0);
        let error = thunder_request(&query).expect_err("must not query user 0");
        assert!(error.contains("out-of-range user_id"));
        assert!(!error.contains("user_id 0"));

        query = numeric_query(uid(1));
        query.user_features.followed_user_ids = vec![u64::MAX];
        let follow_error = thunder_request(&query).expect_err("must not drop follows silently");
        assert!(follow_error.contains("following_user_ids"));

        query.user_features.followed_user_ids = vec![uid(201)];
        query.seen_ids = vec![i64::MAX as u64 + 1];
        let seen_error = thunder_request(&query).expect_err("must not drop seen ids silently");
        assert!(seen_error.contains("exclude_tweet_ids"));
    }
}
