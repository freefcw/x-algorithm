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

/// Fail-closed in-network adapter used when the integer Thunder contract is
/// compiled out, or when a non-demo process has no mrpyq backend.
///
/// Real ObjectIds cannot round-trip through Thunder's `uint64` fields. Leaving
/// this source assembled with a working integer client would query user 0.
pub struct DisabledInNetworkPostsClient;

#[async_trait]
impl InNetworkPostsClient for DisabledInNetworkPostsClient {
    async fn get_in_network_posts(
        &self,
        _query: &ScoredPostsQuery,
        _max_results: u32,
    ) -> Result<Vec<InNetworkPost>, String> {
        Err(
            "in-network source requires MRPYQ_RECOMMENDATION_DATA_ADDR; \
             integer Thunder is demo-only and cannot carry real ObjectIds"
                .to_string(),
        )
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
        // posts 与 usable 的差值是无法无损转成 ObjectId 而被丢弃的部分，
        // 否则这段丢弃在链路上不可见。
        log::info!(
            "thunder rpc GetInNetworkPosts max_results={requested} elapsed_ms={elapsed_ms} posts={returned} usable={}",
            posts.len(),
        );
        Ok(posts)
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

/// Convert a padded demo ObjectId into Thunder's integer wire field.
/// Real 96-bit IDs return `None`; callers must fail closed rather than send 0.
#[cfg(feature = "legacy-int-ids")]
fn thunder_request(query: &ScoredPostsQuery) -> Result<GetInNetworkPostsRequest, String> {
    let user_id = thunder_u64(query.user_id).ok_or_else(|| {
        format!(
            "Thunder adapter cannot round-trip user_id {id} through uint64",
            id = query.user_id
        )
    })?;
    let following_user_ids =
        round_trip_ids(&query.user_features.followed_user_ids, "following_user_ids")?;
    let exclude_tweet_ids = round_trip_ids(&query.seen_ids, "exclude_tweet_ids")?;
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

#[cfg(feature = "legacy-int-ids")]
fn round_trip_ids(ids: &[crate::models::ObjectId], field: &str) -> Result<Vec<u64>, String> {
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        match thunder_u64(*id) {
            Some(n) => out.push(n),
            None => {
                return Err(format!(
                    "Thunder adapter cannot round-trip {field} id {id} through uint64"
                ));
            }
        }
    }
    Ok(out)
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
                    ..Default::default()
                },
                10,
            )
            .await
            .expect_err("disabled adapter must not succeed");
        assert!(error.contains("MRPYQ_RECOMMENDATION_DATA_ADDR"));
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

    fn padded_query(user_id: crate::models::UserId) -> ScoredPostsQuery {
        ScoredPostsQuery {
            user_id,
            user_features: crate::models::user_features::UserFeatures {
                followed_user_ids: vec![uid(201), uid(202)],
                ..Default::default()
            },
            seen_ids: vec![crate::models::pid(9)],
            ..Default::default()
        }
    }

    #[test]
    fn thunder_request_round_trips_padded_demo_ids() {
        let request = thunder_request(&padded_query(uid(1))).expect("padded demo ids");
        assert_eq!(request.user_id, 1);
        assert_eq!(request.following_user_ids, vec![201, 202]);
        assert_eq!(request.exclude_tweet_ids, vec![9]);
        assert_ne!(request.user_id, 0);
    }

    #[test]
    fn thunder_request_rejects_real_object_id_instead_of_user_zero() {
        let user_id =
            crate::models::ObjectId::parse("e305c05a62cd1ef55823cd86").expect("real object id");
        let error = thunder_request(&padded_query(user_id)).expect_err("must not query user 0");
        assert!(error.contains("cannot round-trip user_id"));
        assert!(error.contains("e305c05a62cd1ef55823cd86"));
        assert!(!error.contains("user_id 0"));
    }

    #[test]
    fn thunder_request_rejects_unpadded_follow_or_seen_ids() {
        let real =
            crate::models::ObjectId::parse("e305c05a62cd1ef55823cd86").expect("real object id");
        let mut query = padded_query(uid(1));
        query.user_features.followed_user_ids = vec![real];
        let follow_error = thunder_request(&query).expect_err("must not drop follows silently");
        assert!(follow_error.contains("following_user_ids"));

        query.user_features.followed_user_ids = vec![uid(201)];
        query.seen_ids = vec![real];
        let seen_error = thunder_request(&query).expect_err("must not drop seen ids silently");
        assert!(seen_error.contains("exclude_tweet_ids"));
    }
}
