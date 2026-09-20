use crate::feature_policy::HomeMixerFeatures;
use crate::id::{EntityKind, PaddedIdentityResolver, RegistryClient, SharedIdentityResolver};
use crate::models::ids::{parse_wire_id, ObjectId};
use crate::models::query::ScoredPostsQuery;
use crate::util::request_util::{current_time_ms, generate_request_id};
use std::sync::Arc;
use tonic::Status;
use x_algorithm_proto::home_mixer as pb;

/// Builds request-domain objects at the RPC boundary.
///
/// Full-network recommendations are the product default. Only the explicit
/// request option can restrict the query to in-network.
#[derive(Clone)]
pub struct QueryBuilder {
    features: HomeMixerFeatures,
    identity: SharedIdentityResolver,
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
    /// Test/compatibility constructor: resolves the zero-padded ObjectId form
    /// `ObjectId::from_u64_be_padded(n)` without external services.
    pub fn new(features: HomeMixerFeatures) -> Self {
        Self {
            features,
            identity: Arc::new(PaddedIdentityResolver::new()),
        }
    }

    pub fn with_identity(features: HomeMixerFeatures, identity: SharedIdentityResolver) -> Self {
        Self { features, identity }
    }

    pub fn with_id_registry(features: HomeMixerFeatures, id_registry: Arc<RegistryClient>) -> Self {
        Self::with_identity(features, id_registry)
    }

    /// Shared resolver for the egress boundary (public response, side
    /// effects). The same instance that ingress resolved through must reverse
    /// the internal Snowflake IDs back to ObjectIds.
    pub(crate) fn identity(&self) -> SharedIdentityResolver {
        Arc::clone(&self.identity)
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
        if !proto_query.cached_posts.is_empty() {
            return Err(Status::invalid_argument(
                "unsigned cached_posts are disabled; use a server-owned cache contract",
            ));
        }

        let mut invalid_ids = 0usize;
        let seen_ids = proto_query
            .seen_ids
            .iter()
            .filter_map(|id| parse_wire_id(id, &mut invalid_ids))
            .collect::<Vec<_>>();
        let served_ids = proto_query
            .served_ids
            .iter()
            .filter_map(|id| parse_wire_id(id, &mut invalid_ids))
            .collect::<Vec<_>>();
        let impressed_post_ids = proto_query
            .impressed_post_ids
            .iter()
            .filter_map(|id| parse_wire_id(id, &mut invalid_ids))
            .collect::<Vec<_>>();
        if invalid_ids > 0 {
            log::warn!("QueryBuilder: dropped {invalid_ids} illegal identity string(s)");
        }

        let mut external_ids = vec![(proto_query.viewer_id.clone(), EntityKind::User)];
        external_ids.extend(seen_ids.iter().map(|id| (id.to_string(), EntityKind::Post)));
        external_ids.extend(
            served_ids
                .iter()
                .map(|id| (id.to_string(), EntityKind::Post)),
        );
        external_ids.extend(
            impressed_post_ids
                .iter()
                .map(|id| (id.to_string(), EntityKind::Post)),
        );
        let resolved = self
            .identity
            .resolve_batch(&external_ids)
            .await
            .map_err(|error| Status::unavailable(format!("resolve request IDs: {error}")))?;
        let mut resolved = resolved.into_iter();
        let user_id = resolved.next().expect("registry result count validated");
        let seen_ids = seen_ids
            .iter()
            .map(|_| {
                resolved
                    .next()
                    .expect("registry result count validated")
                    .get()
            })
            .collect();
        let served_ids = served_ids
            .iter()
            .map(|_| {
                resolved
                    .next()
                    .expect("registry result count validated")
                    .get()
            })
            .collect();
        let impressed_post_ids = impressed_post_ids
            .iter()
            .map(|_| {
                resolved
                    .next()
                    .expect("registry result count validated")
                    .get()
            })
            .collect();

        let mut query = query_from_proto(proto_query, self.features);
        query.user_id = user_id.get();
        query.seen_ids = seen_ids;
        query.served_ids = served_ids;
        query.impressed_post_ids = impressed_post_ids;
        Ok(RequestContext { query })
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
        seen_ids: _,
        served_ids: _,
        in_network_only,
        is_bottom_request,
        bloom_filter_entries,
        topic_ids,
        excluded_topic_ids,
        new_user_topic_ids,
        exclude_videos,
        impressed_post_ids: _,
        past_request_timestamps_ms,
        cached_posts: _,
        is_preview,
        is_shadow_traffic,
        is_polling,
        ip_address,
        user_agent,
        enable_phoenix_moe,
    } = proto_query;
    let enable_phoenix_moe = features.phoenix_moe && enable_phoenix_moe;

    ScoredPostsQuery {
        // Replaced by QueryBuilder::build with the resolved Snowflake ID.
        user_id: 0,
        client_app_id,
        country_code,
        language_code,
        seen_ids: Vec::new(),
        served_ids: Vec::new(),
        in_network_only,
        is_bottom_request,
        bloom_filter_entries,
        cached_posts: Vec::new(),
        has_cached_posts: false,
        topic_ids,
        excluded_topic_ids,
        new_user_topic_ids,
        exclude_videos,
        enable_phoenix_moe,
        impressed_post_ids: Vec::new(),
        past_request_timestamps_ms,
        is_preview,
        is_shadow_traffic,
        is_top_request: !is_bottom_request,
        is_polling,
        ip_address,
        user_agent,
        request_id: format!("{}-{}", generate_request_id(), viewer_id),
        prediction_id: generate_request_id(),
        request_time_ms: current_time_ms(),
        ..Default::default()
    }
}
