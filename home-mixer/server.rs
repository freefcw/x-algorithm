use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use crate::final_feed::ForYouFeedServer;
use crate::scored_posts_server::ScoredPostsServer;
use log::info;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use x_algorithm_proto::home_mixer as pb;
use x_algorithm_proto::home_mixer::ScoredPostsResponse;

#[derive(Clone)]
pub struct HomeMixerServer {
    scored_posts_server: Arc<ScoredPostsServer>,
    for_you_feed_server: Arc<ForYouFeedServer>,
}

impl HomeMixerServer {
    pub async fn new() -> Self {
        let scored_posts_server = Arc::new(ScoredPostsServer::new().await);
        let for_you_feed_server = Arc::new(ForYouFeedServer::new(Arc::clone(&scored_posts_server)));
        HomeMixerServer {
            scored_posts_server,
            for_you_feed_server,
        }
    }
}

#[tonic::async_trait]
impl pb::for_you_feed_service_server::ForYouFeedService for HomeMixerServer {
    async fn get_for_you_feed(
        &self,
        request: Request<pb::ScoredPostsQuery>,
    ) -> Result<Response<pb::ForYouFeedResponse>, Status> {
        let query = query_from_proto(request.into_inner())?;
        info!("For You request - request_id {}", query.request_id);
        let output = self.for_you_feed_server.get_for_you_feed(query).await;
        Ok(Response::new(pb::ForYouFeedResponse {
            items: output
                .items
                .into_iter()
                .map(|item| item.into_proto())
                .collect(),
            request_id: output.request_id,
        }))
    }
}

#[tonic::async_trait]
impl pb::scored_posts_service_server::ScoredPostsService for HomeMixerServer {
    async fn get_scored_posts(
        &self,
        request: Request<pb::ScoredPostsQuery>,
    ) -> Result<Response<ScoredPostsResponse>, Status> {
        let query = query_from_proto(request.into_inner())?;
        info!("Scored Posts request - request_id {}", query.request_id);
        let output = self.scored_posts_server.score(query).await;
        Ok(Response::new(ScoredPostsResponse {
            scored_posts: output.posts,
        }))
    }
}

fn query_from_proto(proto_query: pb::ScoredPostsQuery) -> Result<ScoredPostsQuery, Status> {
    if proto_query.viewer_id == 0 {
        return Err(Status::invalid_argument("viewer_id must be specified"));
    }

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
            let tweet_id = i64::try_from(cached.tweet_id).ok()?;
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

    let mut query = ScoredPostsQuery::new(
        viewer_id,
        client_app_id,
        country_code,
        language_code,
        seen_ids,
        served_ids,
        in_network_only,
        is_bottom_request,
        bloom_filter_entries,
    );
    query.topic_ids = topic_ids;
    query.excluded_topic_ids = excluded_topic_ids;
    query.new_user_topic_ids = new_user_topic_ids;
    query.exclude_videos = exclude_videos;
    query.enable_phoenix_moe = enable_phoenix_moe;
    query.impressed_post_ids = impressed_post_ids;
    query.past_request_timestamps_ms = past_request_timestamps_ms;
    query.cached_posts = cached_posts;
    query.has_cached_posts = has_cached_posts;
    query.is_preview = is_preview;
    query.is_shadow_traffic = is_shadow_traffic;
    query.is_polling = is_polling;
    query.ip_address = ip_address;
    query.user_agent = user_agent;
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate_hydrators::vf_candidate_hydrator::VFCandidateHydrator;
    use crate::filters::ancillary_vf_filter::AncillaryVFFilter;
    use crate::visibility::models::FilteredReason;
    use crate::visibility::vf_client::{
        SafetyLevel, TwitterContextViewer, VisibilityFilteringClient,
    };
    use std::collections::HashMap;
    use xai_candidate_pipeline::filter::Filter;
    use xai_candidate_pipeline::hydrator::Hydrator;

    struct QuoteRejectingVisibilityClient;

    #[tonic::async_trait]
    impl VisibilityFilteringClient for QuoteRejectingVisibilityClient {
        async fn get_result(
            &self,
            tweet_ids: Vec<i64>,
            _safety_level: SafetyLevel,
            _for_user_id: i64,
            _context: Option<TwitterContextViewer>,
        ) -> Result<HashMap<i64, Option<FilteredReason>>, anyhow::Error> {
            Ok(tweet_ids
                .into_iter()
                .map(|id| {
                    let reason = (id == 300)
                        .then(|| FilteredReason::GenericFiltered("unsafe quote".to_string()));
                    (id, reason)
                })
                .collect())
        }
    }

    #[test]
    fn maps_p3_request_context_and_cached_candidates() {
        let query = query_from_proto(pb::ScoredPostsQuery {
            viewer_id: 42,
            topic_ids: vec![10],
            excluded_topic_ids: vec![99],
            new_user_topic_ids: vec![20],
            exclude_videos: true,
            enable_phoenix_moe: true,
            impressed_post_ids: vec![7],
            past_request_timestamps_ms: vec![1_700_000_000_000],
            cached_posts: vec![pb::CachedPost {
                tweet_id: 100,
                author_id: 200,
                tweet_text: "cached text".to_string(),
                quoted_tweet_id: 300,
                filtered_topic_ids: vec![10],
                video_duration_ms: 5_000,
                language_code: "en".to_string(),
                ..Default::default()
            }],
            is_preview: true,
            is_shadow_traffic: true,
            is_polling: true,
            ip_address: "203.0.113.1".to_string(),
            user_agent: "test-client".to_string(),
            ..Default::default()
        })
        .expect("valid query");

        assert_eq!(query.topic_ids, vec![10]);
        assert_eq!(query.excluded_topic_ids, vec![99]);
        assert_eq!(query.new_user_topic_ids, vec![20]);
        assert!(query.exclude_videos);
        assert!(query.enable_phoenix_moe);
        assert_eq!(query.impressed_post_ids, vec![7]);
        assert!(query.has_cached_posts);
        assert_eq!(query.cached_posts[0].tweet_id, 100);
        assert_eq!(query.cached_posts[0].tweet_text, "cached text");
        assert_eq!(query.cached_posts[0].quoted_tweet_id, Some(300));
        assert_eq!(query.cached_posts[0].filtered_topic_ids, vec![10]);
        assert_eq!(query.cached_posts[0].video_duration_ms, Some(5_000));
        assert_eq!(query.cached_posts[0].language_code.as_deref(), Some("en"));
        assert_eq!(
            query.cached_posts[0].served_type,
            Some(pb::ServedType::ForYouCachedPost)
        );
        assert!(query.is_preview && query.is_shadow_traffic && query.is_polling);
        assert_eq!(query.ip_address, "203.0.113.1");
        assert_eq!(query.user_agent, "test-client");
    }

    #[test]
    fn removes_cached_candidate_when_quoted_post_is_not_visible() {
        let query = query_from_proto(pb::ScoredPostsQuery {
            viewer_id: 42,
            cached_posts: vec![pb::CachedPost {
                tweet_id: 100,
                author_id: 200,
                quoted_tweet_id: 300,
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect("valid query");
        let hydrator = VFCandidateHydrator {
            vf_client: Arc::new(QuoteRejectingVisibilityClient),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let hydrated = runtime
            .block_on(hydrator.hydrate(&query, &query.cached_posts))
            .expect("visibility hydration");
        let mut candidate = query.cached_posts[0].clone();
        hydrator.update(&mut candidate, hydrated[0].clone());

        let result = AncillaryVFFilter
            .filter(&query, vec![candidate])
            .expect("ancillary filter");

        assert!(result.kept.is_empty());
        assert_eq!(result.removed.len(), 1);
    }

    #[test]
    fn rejects_missing_viewer_id() {
        let error = query_from_proto(pb::ScoredPostsQuery::default()).unwrap_err();

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}
