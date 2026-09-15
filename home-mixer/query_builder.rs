use crate::feature_policy::HomeMixerFeatures;
use crate::models::candidate::PostCandidate;
use crate::models::ids::{parse_wire_id, ObjectId};
use crate::models::query::ScoredPostsQuery;
use crate::util::request_util::{current_time_ms, generate_request_id};
use tonic::Status;
use x_algorithm_proto::home_mixer as pb;

/// Builds request-domain objects at the RPC boundary.
///
/// Full-network recommendations are the product default. Only the explicit
/// request option can restrict the query to in-network.
#[derive(Clone)]
pub struct QueryBuilder {
    features: HomeMixerFeatures,
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new(HomeMixerFeatures::default())
    }
}

pub struct RequestContext {
    pub query: ScoredPostsQuery,
}

impl QueryBuilder {
    pub fn new(features: HomeMixerFeatures) -> Self {
        Self { features }
    }

    pub async fn build(&self, proto_query: pb::ScoredPostsQuery) -> Result<RequestContext, Status> {
        if proto_query.viewer_id.is_empty() {
            return Err(Status::invalid_argument("viewer_id must be specified"));
        }
        let viewer_id = ObjectId::parse(&proto_query.viewer_id).map_err(|_| {
            Status::invalid_argument("viewer_id must be 24 lowercase hex characters")
        })?;
        if viewer_id.is_nil() {
            return Err(Status::invalid_argument("viewer_id must be specified"));
        }
        if !proto_query.cached_posts.is_empty() && !self.features.unsigned_cached_posts {
            return Err(Status::invalid_argument(
                "unsigned cached_posts are disabled; use a server-owned cache contract",
            ));
        }

        Ok(RequestContext {
            query: query_from_proto(proto_query, self.features),
        })
    }
}

fn query_from_proto(
    proto_query: pb::ScoredPostsQuery,
    features: HomeMixerFeatures,
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
    let mut invalid_ids = 0usize;
    let cached_posts = cached_posts
        .into_iter()
        .filter_map(|cached| {
            let tweet_id = parse_wire_id(&cached.tweet_id, &mut invalid_ids)?;
            Some(PostCandidate {
                tweet_id,
                author_id: parse_wire_id(&cached.author_id, &mut invalid_ids).unwrap_or_default(),
                tweet_text: cached.tweet_text,
                quoted_tweet_text: cached.quoted_tweet_text,
                quoted_tweet_id: parse_wire_id(&cached.quoted_tweet_id, &mut invalid_ids),
                retweeted_tweet_id: parse_wire_id(&cached.retweeted_tweet_id, &mut invalid_ids),
                retweeted_user_id: parse_wire_id(&cached.retweeted_user_id, &mut invalid_ids),
                in_reply_to_tweet_id: parse_wire_id(&cached.in_reply_to_tweet_id, &mut invalid_ids),
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

    let user_id = ObjectId::parse(&viewer_id).expect("viewer_id was validated by QueryBuilder");
    let seen_ids = seen_ids
        .into_iter()
        .filter_map(|id| parse_wire_id(&id, &mut invalid_ids))
        .collect();
    let served_ids = served_ids
        .into_iter()
        .filter_map(|id| parse_wire_id(&id, &mut invalid_ids))
        .collect();
    let impressed_post_ids = impressed_post_ids
        .into_iter()
        .filter_map(|id| parse_wire_id(&id, &mut invalid_ids))
        .collect();
    if invalid_ids > 0 {
        log::warn!("QueryBuilder: dropped {invalid_ids} illegal identity string(s)");
    }
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
