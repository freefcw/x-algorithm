use crate::clients::gizmoduck_client::{
    DisabledGizmoduckClient, GizmoduckClient, ViewerData, ViewerEligibility,
};
use crate::feature_policy::HomeMixerFeatures;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::util::request_util::{current_time_ms, generate_request_id};
use std::sync::Arc;
use std::time::Duration;
use tonic::Status;
use x_algorithm_proto::home_mixer as pb;

const VIEWER_DATA_TIMEOUT_MS: u64 = 200;

/// Builds request-domain objects at the RPC boundary.
///
/// Viewer policy is fail-safe: only an explicit allow enables out-of-network
/// recommendations. Additional viewer fields require public adapters and
/// explicit query ownership before they can be enabled.
#[derive(Clone)]
pub struct QueryBuilder {
    features: HomeMixerFeatures,
    gizmoduck_client: Arc<dyn GizmoduckClient + Send + Sync>,
    viewer_data_timeout: Duration,
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new(
            HomeMixerFeatures::default(),
            Arc::new(DisabledGizmoduckClient),
        )
    }
}

pub struct RequestContext {
    pub query: ScoredPostsQuery,
}

impl QueryBuilder {
    pub fn new(
        features: HomeMixerFeatures,
        gizmoduck_client: Arc<dyn GizmoduckClient + Send + Sync>,
    ) -> Self {
        Self {
            features,
            gizmoduck_client,
            viewer_data_timeout: Duration::from_millis(VIEWER_DATA_TIMEOUT_MS),
        }
    }

    pub fn with_viewer_data_timeout(mut self, timeout: Duration) -> Self {
        self.viewer_data_timeout = timeout;
        self
    }

    pub async fn build(&self, proto_query: pb::ScoredPostsQuery) -> Result<RequestContext, Status> {
        if proto_query.viewer_id <= 0 {
            return Err(Status::invalid_argument("viewer_id must be specified"));
        }
        if !proto_query.cached_posts.is_empty() && !self.features.unsigned_cached_posts {
            return Err(Status::invalid_argument(
                "unsigned cached_posts are disabled; use a server-owned cache contract",
            ));
        }

        let viewer_id = u64::try_from(proto_query.viewer_id)
            .map_err(|_| Status::invalid_argument("viewer_id must be non-negative"))?;
        let viewer_data = self.fetch_viewer_data(viewer_id).await;
        Ok(RequestContext {
            query: query_from_proto(proto_query, self.features, viewer_data),
        })
    }

    async fn fetch_viewer_data(&self, viewer_id: u64) -> ViewerData {
        match tokio::time::timeout(
            self.viewer_data_timeout,
            self.gizmoduck_client.get_viewer_data(viewer_id),
        )
        .await
        {
            Ok(Ok(data)) => {
                if data.for_you_eligibility == ViewerEligibility::Unknown {
                    log::warn!(
                        "viewer eligibility is unknown for user {}; restricting request to in-network recommendations",
                        viewer_id
                    );
                }
                data
            }
            Ok(Err(error)) => {
                log::warn!(
                    "viewer data unavailable for user {}: {}; restricting request to in-network recommendations",
                    viewer_id,
                    error
                );
                ViewerData::default()
            }
            Err(_) => {
                log::warn!(
                    "viewer data timed out for user {} after {} ms; restricting request to in-network recommendations",
                    viewer_id,
                    self.viewer_data_timeout.as_millis()
                );
                ViewerData::default()
            }
        }
    }
}

fn query_from_proto(
    proto_query: pb::ScoredPostsQuery,
    features: HomeMixerFeatures,
    viewer_data: ViewerData,
) -> ScoredPostsQuery {
    let pb::ScoredPostsQuery {
        viewer_id,
        client_app_id,
        country_code,
        language_code,
        seen_ids,
        served_ids,
        in_network_only,
        is_bottom_request,
        bloom_filter_entries,
        topic_ids,
        excluded_topic_ids,
        new_user_topic_ids,
        exclude_videos,
        impressed_post_ids,
        past_request_timestamps_ms,
        cached_posts,
        is_preview,
        is_shadow_traffic,
        is_polling,
        ip_address,
        user_agent,
        enable_phoenix_moe,
    } = proto_query;
    let cached_posts = cached_posts
        .into_iter()
        .filter_map(|cached| {
            let tweet_id = cached.tweet_id;
            (tweet_id != 0).then(|| PostCandidate {
                tweet_id,
                author_id: cached.author_id,
                tweet_text: cached.tweet_text,
                quoted_tweet_text: cached.quoted_tweet_text,
                quoted_tweet_id: (cached.quoted_tweet_id != 0).then_some(cached.quoted_tweet_id),
                retweeted_tweet_id: (cached.retweeted_tweet_id != 0)
                    .then_some(cached.retweeted_tweet_id),
                retweeted_user_id: (cached.retweeted_user_id != 0)
                    .then_some(cached.retweeted_user_id),
                in_reply_to_tweet_id: (cached.in_reply_to_tweet_id != 0)
                    .then_some(cached.in_reply_to_tweet_id),
                served_type: Some(
                    pb::ServedType::try_from(cached.served_type)
                        .ok()
                        .filter(|served_type| *served_type != pb::ServedType::Unspecified)
                        .unwrap_or(pb::ServedType::ForYouCachedPost),
                ),
                filtered_topic_ids: cached.filtered_topic_ids,
                unfiltered_topic_ids: cached.unfiltered_topic_ids,
                video_duration_ms: (cached.video_duration_ms > 0)
                    .then_some(cached.video_duration_ms),
                quoted_video_duration_ms: (cached.quoted_video_duration_ms > 0)
                    .then_some(cached.quoted_video_duration_ms),
                has_media: Some(
                    cached.video_duration_ms > 0 || cached.quoted_video_duration_ms > 0,
                ),
                in_network: Some(cached.in_network),
                score: Some(cached.score),
                language_code: (!cached.language_code.is_empty()).then_some(cached.language_code),
                ..Default::default()
            })
        })
        .collect::<Vec<_>>();
    let has_cached_posts = !cached_posts.is_empty();

    let user_id = u64::try_from(viewer_id).expect("viewer_id was validated by QueryBuilder");
    let seen_ids = seen_ids
        .into_iter()
        .filter_map(|id| u64::try_from(id).ok())
        .collect();
    let served_ids = served_ids
        .into_iter()
        .filter_map(|id| u64::try_from(id).ok())
        .collect();
    let impressed_post_ids = impressed_post_ids
        .into_iter()
        .filter_map(|id| u64::try_from(id).ok())
        .collect();
    let in_network_only = in_network_only || !viewer_data.for_you_eligibility.allows_for_you();
    let enable_phoenix_moe = features.phoenix_moe && enable_phoenix_moe;

    ScoredPostsQuery {
        user_id,
        client_app_id,
        country_code,
        language_code,
        seen_ids,
        served_ids,
        in_network_only,
        is_bottom_request,
        bloom_filter_entries,
        cached_posts,
        has_cached_posts,
        topic_ids,
        excluded_topic_ids,
        new_user_topic_ids,
        exclude_videos,
        enable_phoenix_moe,
        impressed_post_ids,
        past_request_timestamps_ms,
        is_preview,
        is_shadow_traffic,
        is_top_request: !is_bottom_request,
        is_polling,
        ip_address,
        user_agent,
        request_id: format!("{}-{}", generate_request_id(), user_id),
        prediction_id: generate_request_id(),
        request_time_ms: current_time_ms(),
        ..Default::default()
    }
}
