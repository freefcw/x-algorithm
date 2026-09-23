use crate::feature_policy::HomeMixerFeatures;
use crate::id::{
    EntityKind, IdentityContext, IdentityReader, IdentityRegistrationContext,
    PaddedIdentityResolver, RegistryClient, SharedIdentityIngress, SharedIdentityReader,
};
use crate::models::ids::{parse_wire_id, ObjectId};
use crate::models::query::ScoredPostsQuery;
use crate::util::request_util::{current_time_ms, generate_request_id};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::Status;
use x_algorithm_proto::home_mixer as pb;

/// Builds request-domain objects at the RPC boundary.
///
/// Full-network recommendations are the product default. Only the explicit
/// request option can restrict the query to in-network.
#[derive(Clone)]
pub struct QueryBuilder {
    features: HomeMixerFeatures,
    identity: SharedIdentityIngress,
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

    pub fn with_identity(features: HomeMixerFeatures, identity: SharedIdentityIngress) -> Self {
        Self { features, identity }
    }

    pub fn with_id_registry(features: HomeMixerFeatures, id_registry: Arc<RegistryClient>) -> Self {
        Self::with_identity(features, id_registry)
    }

    pub async fn build(&self, proto_query: pb::ScoredPostsQuery) -> Result<RequestContext, Status> {
        self.build_with_budget(
            proto_query,
            Duration::from_millis(crate::params::REQUEST_TIMEOUT_MS),
        )
        .await
    }

    pub async fn build_with_budget(
        &self,
        proto_query: pb::ScoredPostsQuery,
        budget: Duration,
    ) -> Result<RequestContext, Status> {
        self.build_with_deadline(proto_query, Instant::now() + budget)
            .await
    }

    pub async fn build_with_deadline(
        &self,
        proto_query: pb::ScoredPostsQuery,
        deadline: Instant,
    ) -> Result<RequestContext, Status> {
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
        // Query input is read-only identity data. Unknown history/exclusion
        // hints are ignored so one stale client hint cannot make the whole
        // recommendation request fail. New mappings are registered by the
        // event/recall ingress paths, not by this query boundary.
        let reader: SharedIdentityReader = Arc::clone(&self.identity) as SharedIdentityReader;
        let identity_context = Arc::new(IdentityContext::new_with_deadline(reader, Some(deadline)));
        let registration_context = Arc::new(IdentityRegistrationContext::new(
            Arc::clone(&identity_context),
            Arc::clone(&self.identity),
        ));
        // Query history hints are best-effort, while the viewer mapping is
        // required. Resolve them in one partial batch so stale hints do not
        // cause an all-or-none request followed by a second Registry call.
        let mut resolved = identity_context
            .resolve_batch_partial(&external_ids)
            .await
            .map_err(|error| Status::unavailable(format!("resolve request IDs: {error}")))?
            .into_iter();
        let user_id = resolved
            .next()
            .flatten()
            .ok_or_else(|| Status::not_found("viewer ID mapping missing"))?;
        let seen_ids = (0..seen_ids.len())
            .filter_map(|_| resolved.next().flatten().map(|id| id.get()))
            .collect();
        let served_ids = (0..served_ids.len())
            .filter_map(|_| resolved.next().flatten().map(|id| id.get()))
            .collect();
        let impressed_post_ids = (0..impressed_post_ids.len())
            .filter_map(|_| resolved.next().flatten().map(|id| id.get()))
            .collect();

        let mut query = query_from_proto(proto_query, self.features);
        query.user_id = user_id.get();
        query.seen_ids = seen_ids;
        query.served_ids = served_ids;
        query.impressed_post_ids = impressed_post_ids;
        query = query.with_request_identity(identity_context, registration_context);
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
        ..ScoredPostsQuery::test_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{IdentityAllocator, IdentityReader, SnowflakeId};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct CountingIdentity {
        allocate_calls: AtomicUsize,
        allocated_ids: AtomicUsize,
    }

    #[tonic::async_trait]
    impl IdentityReader for CountingIdentity {
        async fn resolve_batch(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            Ok(ids
                .iter()
                .enumerate()
                .map(|(index, _)| SnowflakeId::new(index as u64 + 1).unwrap())
                .collect())
        }

        async fn reverse_batch(
            &self,
            ids: &[(SnowflakeId, EntityKind)],
        ) -> anyhow::Result<Vec<String>> {
            Ok(ids
                .iter()
                .map(|(id, _)| ObjectId::from_u64_be_padded(id.get()).to_string())
                .collect())
        }
    }

    #[tonic::async_trait]
    impl IdentityAllocator for CountingIdentity {
        async fn allocate_batch(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            self.allocate_calls.fetch_add(1, Ordering::Relaxed);
            self.allocated_ids.fetch_add(ids.len(), Ordering::Relaxed);
            Ok(ids
                .iter()
                .enumerate()
                .map(|(index, _)| SnowflakeId::new(index as u64 + 100).unwrap())
                .collect())
        }
    }

    #[derive(Default)]
    struct MissOnHintIdentity {
        resolve_calls: AtomicUsize,
    }

    #[tonic::async_trait]
    impl IdentityReader for MissOnHintIdentity {
        async fn resolve_batch(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            self.resolve_calls.fetch_add(1, Ordering::Relaxed);
            if ids.iter().any(|(id, _)| id == "000000000000000000000999") {
                return Err(anyhow::Error::new(tonic::Status::not_found(
                    "mapping missing",
                )));
            }
            Ok(ids
                .iter()
                .map(|(_, kind)| {
                    SnowflakeId::new(if *kind == EntityKind::User { 7 } else { 8 }).unwrap()
                })
                .collect())
        }

        async fn reverse_batch(
            &self,
            ids: &[(SnowflakeId, EntityKind)],
        ) -> anyhow::Result<Vec<String>> {
            Ok(ids
                .iter()
                .map(|(id, _)| ObjectId::from_u64_be_padded(id.get()).to_string())
                .collect())
        }

        async fn resolve_batch_partial(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<Option<SnowflakeId>>> {
            self.resolve_calls.fetch_add(1, Ordering::Relaxed);
            Ok(ids
                .iter()
                .map(|(id, kind)| {
                    if id.ends_with("997") || id.ends_with("998") || id.ends_with("999") {
                        None
                    } else {
                        Some(
                            SnowflakeId::new(if *kind == EntityKind::User { 7 } else { 8 })
                                .unwrap(),
                        )
                    }
                })
                .collect())
        }
    }

    #[tonic::async_trait]
    impl IdentityAllocator for MissOnHintIdentity {
        async fn allocate_batch(
            &self,
            _ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            panic!("query builder must not allocate query identities")
        }
    }

    #[tokio::test]
    async fn query_builder_resolves_query_ids_without_allocation() {
        let identity = Arc::new(CountingIdentity::default());
        let query = QueryBuilder::with_identity(
            HomeMixerFeatures::default(),
            Arc::clone(&identity) as SharedIdentityIngress,
        )
        .build(pb::ScoredPostsQuery {
            viewer_id: "00000000000000000000002a".to_string(),
            seen_ids: vec![
                "000000000000000000000001".to_string(),
                "000000000000000000000001".to_string(),
            ],
            served_ids: vec!["000000000000000000000001".to_string()],
            impressed_post_ids: vec!["000000000000000000000001".to_string()],
            ..Default::default()
        })
        .await
        .unwrap()
        .query;

        assert_eq!(identity.allocate_calls.load(Ordering::Relaxed), 0);
        assert_eq!(identity.allocated_ids.load(Ordering::Relaxed), 0);
        assert_eq!(query.identity_context().stats().allocate_batches, 0);
        assert_eq!(query.identity_context().stats().resolve_batches, 1);
        assert_eq!(query.seen_ids, vec![2, 2]);
        assert_eq!(query.served_ids, vec![2]);
        assert_eq!(query.impressed_post_ids, vec![2]);
    }

    #[tokio::test]
    async fn missing_viewer_mapping_returns_not_found() {
        let identity = Arc::new(MissOnHintIdentity::default());
        let ingress: SharedIdentityIngress = identity;
        let error = QueryBuilder::with_identity(HomeMixerFeatures::default(), ingress)
            .build(pb::ScoredPostsQuery {
                viewer_id: "000000000000000000000999".to_string(),
                ..Default::default()
            })
            .await
            .err()
            .expect("the required viewer mapping is missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn unknown_history_hint_is_dropped_but_viewer_is_required() {
        let identity = Arc::new(MissOnHintIdentity::default());
        let ingress: SharedIdentityIngress = identity.clone();
        let query = QueryBuilder::with_identity(HomeMixerFeatures::default(), ingress)
            .build(pb::ScoredPostsQuery {
                viewer_id: "00000000000000000000002a".to_string(),
                seen_ids: vec![
                    "000000000000000000000999".to_string(),
                    "000000000000000000000998".to_string(),
                    "000000000000000000000997".to_string(),
                ],
                ..Default::default()
            })
            .await
            .expect("unknown history hints are non-fatal")
            .query;

        assert_eq!(query.user_id, 7);
        assert!(query.seen_ids.is_empty());
        assert_eq!(
            identity.resolve_calls.load(Ordering::Relaxed),
            1,
            "unknown hints should be resolved in one partial batch"
        );
    }
}
