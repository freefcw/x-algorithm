//! HTTP surface of the identity service.
//!
//! The JSON shapes, route paths and `entity_kind` spelling (`User` / `Post`)
//! are a contract with `home-mixer/id.rs`, `phoenix/services/xrex_adapter.py`
//! and `phoenix/scripts/build_training_inputs.py`; change them only together
//! with those clients.

use crate::metrics::metrics;
use crate::{EntityKind, IdError, RedisIdRegistry, SnowflakeId, MAPPING_VERSION};
use axum::{
    extract::{MatchedPath, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    registry: Arc<RedisIdRegistry>,
    max_batch_size: usize,
    allocation_token: Option<Arc<str>>,
}

#[derive(Deserialize)]
struct ResolveRequest {
    object_id: String,
    entity_kind: EntityKind,
    /// Caller-provided Snowflake ID. Only the allocate routes accept it;
    /// resolve is read-only.
    #[serde(default)]
    snowflake_id: Option<u64>,
}

#[derive(Deserialize)]
struct ResolveBatchRequest {
    ids: Vec<ResolveRequest>,
}

#[derive(Deserialize)]
struct ReverseRequest {
    snowflake_id: u64,
    entity_kind: EntityKind,
}

#[derive(Deserialize)]
struct ReverseBatchRequest {
    ids: Vec<ReverseRequest>,
}

#[derive(Serialize)]
struct ResolveResponse {
    object_id: String,
    entity_kind: EntityKind,
    snowflake_id: u64,
    mapping_version: u32,
}

#[derive(Serialize)]
struct ResolvePartialResponse {
    object_id: String,
    entity_kind: EntityKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    snowflake_id: Option<u64>,
    mapping_version: u32,
}

#[derive(Serialize)]
struct ReverseResponse {
    snowflake_id: u64,
    object_id: String,
    entity_kind: EntityKind,
    mapping_version: u32,
}

/// An error response: plain-text message with the status derived from the
/// [`IdError`] variant.
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, self.1).into_response()
    }
}

impl From<IdError> for ApiError {
    fn from(error: IdError) -> Self {
        Self(status_for(&error), error.to_string())
    }
}

/// HTTP status per error variant. Client mistakes are 4xx, dependency
/// failures 503, and anything the service cannot classify 500.
pub fn status_for(error: &IdError) -> StatusCode {
    match error {
        IdError::InvalidObjectId(_)
        | IdError::UnsupportedSnowflake(_)
        | IdError::BeforeSnowflakeEpoch(_)
        | IdError::InvalidWorkerId(_) => StatusCode::BAD_REQUEST,
        IdError::AllocationDisabled(_)
        | IdError::UnknownObjectIds(_)
        | IdError::UnknownSnowflake(_) => StatusCode::NOT_FOUND,
        IdError::MappingConflict { .. }
        | IdError::SnowflakeTaken { .. }
        | IdError::EntityKindMismatch { .. } => StatusCode::CONFLICT,
        IdError::Redis(_)
        | IdError::Io(_)
        | IdError::MappingVersionMismatch { .. }
        | IdError::StorageSchemaMismatch { .. } => StatusCode::SERVICE_UNAVAILABLE,
        IdError::SecondExhausted(_) | IdError::CorruptRecord { .. } => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Build the service router. `max_batch_size` bounds the `ids` array of the
/// batch routes; larger requests are rejected with 413.
pub fn router(registry: Arc<RedisIdRegistry>, max_batch_size: usize) -> Router {
    router_with_allocation_token(registry, max_batch_size, None)
}

pub fn router_with_allocation_token(
    registry: Arc<RedisIdRegistry>,
    max_batch_size: usize,
    allocation_token: Option<String>,
) -> Router {
    let state = AppState {
        registry,
        max_batch_size,
        allocation_token: allocation_token.map(Arc::<str>::from),
    };
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics_page))
        .route("/v1/resolve", post(resolve))
        .route("/v1/resolve:batch", post(resolve_batch))
        .route("/v1/resolve_partial:batch", post(resolve_batch_partial))
        .route("/v1/allocate", post(allocate))
        .route("/v1/allocate:batch", post(allocate_batch))
        .route("/v1/reverse", post(reverse))
        .route("/v1/reverse:batch", post(reverse_batch))
        .layer(middleware::from_fn(record_request))
        .with_state(state)
}

/// Count every response by matched route and status.
async fn record_request(
    matched_path: Option<MatchedPath>,
    request: Request,
    next: Next,
) -> Response {
    let route = matched_path
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let response = next.run(request).await;
    metrics().record_request(&route, response.status().as_u16());
    response
}

async fn ready(State(state): State<AppState>) -> Result<&'static str, ApiError> {
    state.registry.check_ready().await?;
    Ok("ready")
}

async fn metrics_page(State(state): State<AppState>) -> Result<Response, ApiError> {
    metrics().set_cache_sizes(state.registry.cache_sizes());
    let body = metrics().encode().map_err(|error| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {error}"),
        )
    })?;
    Ok((
        [(
            header::CONTENT_TYPE,
            crate::metrics::Metrics::content_type(),
        )],
        body,
    )
        .into_response())
}

fn authorize_allocation(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(expected) = state.allocation_token.as_deref() else {
        return Ok(());
    };
    let provided = headers
        .get("x-id-registry-allocation-token")
        .and_then(|value| value.to_str().ok());
    if provided == Some(expected) {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "Allocate requires x-id-registry-allocation-token".into(),
        ))
    }
}

fn check_batch_size(len: usize, max: usize) -> Result<(), ApiError> {
    if len > max {
        return Err(ApiError(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("batch of {len} ids exceeds the limit of {max}"),
        ));
    }
    Ok(())
}

fn parse_snowflake(value: u64) -> Result<SnowflakeId, ApiError> {
    SnowflakeId::new(value).map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))
}

async fn resolve(
    State(state): State<AppState>,
    Json(request): Json<ResolveRequest>,
) -> Result<Json<ResolveResponse>, ApiError> {
    if request.snowflake_id.is_some() {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "resolve is read-only; use allocate to register a provided snowflake_id".into(),
        ));
    }
    let snowflake_id = state
        .registry
        .resolve_existing_one(&request.object_id, request.entity_kind)
        .await?;
    Ok(Json(ResolveResponse {
        object_id: request.object_id,
        entity_kind: request.entity_kind,
        snowflake_id: snowflake_id.get(),
        mapping_version: MAPPING_VERSION,
    }))
}

async fn resolve_batch(
    State(state): State<AppState>,
    Json(request): Json<ResolveBatchRequest>,
) -> Result<Json<Vec<ResolveResponse>>, ApiError> {
    check_batch_size(request.ids.len(), state.max_batch_size)?;
    if request.ids.iter().any(|item| item.snowflake_id.is_some()) {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "resolve is read-only; use allocate to register a provided snowflake_id".into(),
        ));
    }
    let ids = request
        .ids
        .iter()
        .map(|item| (item.object_id.clone(), item.entity_kind))
        .collect::<Vec<_>>();
    let snowflakes = state.registry.resolve_existing_batch(&ids).await?;
    Ok(Json(
        request
            .ids
            .into_iter()
            .zip(snowflakes)
            .map(|(item, id)| ResolveResponse {
                object_id: item.object_id,
                entity_kind: item.entity_kind,
                snowflake_id: id.get(),
                mapping_version: MAPPING_VERSION,
            })
            .collect(),
    ))
}

async fn resolve_batch_partial(
    State(state): State<AppState>,
    Json(request): Json<ResolveBatchRequest>,
) -> Result<Json<Vec<ResolvePartialResponse>>, ApiError> {
    check_batch_size(request.ids.len(), state.max_batch_size)?;
    if request.ids.iter().any(|item| item.snowflake_id.is_some()) {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "resolve is read-only; use allocate to register a provided snowflake_id".into(),
        ));
    }
    let ids = request
        .ids
        .iter()
        .map(|item| (item.object_id.clone(), item.entity_kind))
        .collect::<Vec<_>>();
    let snowflakes = state.registry.resolve_existing_batch_partial(&ids).await?;
    Ok(Json(
        request
            .ids
            .into_iter()
            .zip(snowflakes)
            .map(|(item, id)| ResolvePartialResponse {
                object_id: item.object_id,
                entity_kind: item.entity_kind,
                snowflake_id: id.map(SnowflakeId::get),
                mapping_version: MAPPING_VERSION,
            })
            .collect(),
    ))
}

async fn allocate_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ResolveBatchRequest>,
) -> Result<Json<Vec<ResolveResponse>>, ApiError> {
    authorize_allocation(&state, &headers)?;
    check_batch_size(request.ids.len(), state.max_batch_size)?;
    let ids = request
        .ids
        .iter()
        .map(|item| {
            Ok((
                item.object_id.clone(),
                item.entity_kind,
                item.snowflake_id.map(parse_snowflake).transpose()?,
            ))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let snowflakes = state.registry.allocate_batch(&ids).await?;
    Ok(Json(
        request
            .ids
            .into_iter()
            .zip(snowflakes)
            .map(|(item, id)| ResolveResponse {
                object_id: item.object_id,
                entity_kind: item.entity_kind,
                snowflake_id: id.get(),
                mapping_version: MAPPING_VERSION,
            })
            .collect(),
    ))
}

async fn allocate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ResolveRequest>,
) -> Result<Json<ResolveResponse>, ApiError> {
    authorize_allocation(&state, &headers)?;
    let provided = request.snowflake_id.map(parse_snowflake).transpose()?;
    let id = state
        .registry
        .allocate_one(&request.object_id, request.entity_kind, provided)
        .await?;
    Ok(Json(ResolveResponse {
        object_id: request.object_id,
        entity_kind: request.entity_kind,
        snowflake_id: id.get(),
        mapping_version: MAPPING_VERSION,
    }))
}

async fn reverse(
    State(state): State<AppState>,
    Json(request): Json<ReverseRequest>,
) -> Result<Json<ReverseResponse>, ApiError> {
    let snowflake_id = parse_snowflake(request.snowflake_id)?;
    let mapping = state
        .registry
        .reverse_one(snowflake_id, request.entity_kind)
        .await?;
    Ok(Json(ReverseResponse {
        snowflake_id: mapping.snowflake_id.get(),
        object_id: mapping.object_id,
        entity_kind: mapping.entity_kind,
        mapping_version: mapping.mapping_version,
    }))
}

async fn reverse_batch(
    State(state): State<AppState>,
    Json(request): Json<ReverseBatchRequest>,
) -> Result<Json<Vec<ReverseResponse>>, ApiError> {
    check_batch_size(request.ids.len(), state.max_batch_size)?;
    let ids = request
        .ids
        .iter()
        .map(|item| Ok((parse_snowflake(item.snowflake_id)?, item.entity_kind)))
        .collect::<Result<Vec<_>, ApiError>>()?;
    let mappings = state.registry.reverse_batch(&ids).await?;
    Ok(Json(
        mappings
            .into_iter()
            .map(|mapping| ReverseResponse {
                snowflake_id: mapping.snowflake_id.get(),
                object_id: mapping.object_id,
                entity_kind: mapping.entity_kind,
                mapping_version: mapping.mapping_version,
            })
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        InsertOutcome, Mapping, MappingStore, MemoryMappingStore, SequenceFloor,
        SequenceReservation,
    };
    use async_trait::async_trait;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    const USER: &str = "65f1a2b3c4d5e6f708091011";
    const POST: &str = "66f1a2b3c4d5e6f708091011";

    fn app_with(
        store: Arc<dyn MappingStore>,
        allow_allocation: bool,
        max_batch_size: usize,
    ) -> Router {
        let registry = RedisIdRegistry::with_store(store, 0, allow_allocation).unwrap();
        router(Arc::new(registry), max_batch_size)
    }

    fn app() -> (Arc<MemoryMappingStore>, Router) {
        let store = Arc::new(MemoryMappingStore::new());
        (store.clone(), app_with(store, true, 3))
    }

    async fn call(
        app: &Router,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, String) {
        let request = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    async fn post_json(app: &Router, path: &str, body: Value) -> (StatusCode, Value) {
        let (status, text) = call(app, "POST", path, Some(body)).await;
        let value = serde_json::from_str(&text).unwrap_or(Value::String(text));
        (status, value)
    }

    /// A store whose every operation fails like an unreachable Redis.
    struct FailingStore;

    #[async_trait]
    impl MappingStore for FailingStore {
        async fn find_by_object_batch(
            &self,
            _: &[(String, EntityKind)],
        ) -> Result<Vec<Option<Mapping>>, IdError> {
            Err(IdError::Redis("GET object timed out".into()))
        }
        async fn find_by_snowflake_batch(
            &self,
            _: &[SnowflakeId],
        ) -> Result<Vec<Option<Mapping>>, IdError> {
            Err(IdError::Redis("GET snowflake timed out".into()))
        }
        async fn insert_if_absent_batch(
            &self,
            _: &[Mapping],
        ) -> Result<Vec<InsertOutcome>, IdError> {
            Err(IdError::Redis("EVALSHA timed out".into()))
        }
        async fn next_sequence_batch(
            &self,
            _: &[SequenceReservation],
        ) -> Result<Vec<u64>, IdError> {
            Err(IdError::Redis("INCRBY timed out".into()))
        }
        async fn observe_sequence_batch(&self, _: &[SequenceFloor]) -> Result<(), IdError> {
            Err(IdError::Redis("SET timed out".into()))
        }
        async fn check_ready(&self) -> Result<(), IdError> {
            Err(IdError::Redis("mapping version metadata is missing".into()))
        }
        fn cache_sizes(&self) -> (usize, usize) {
            (0, 0)
        }
    }

    #[tokio::test]
    async fn probes_and_metrics_are_served() {
        let (_, app) = app();
        assert_eq!(
            call(&app, "GET", "/healthz", None).await,
            (StatusCode::OK, "ok".into())
        );
        assert_eq!(
            call(&app, "GET", "/readyz", None).await,
            (StatusCode::OK, "ready".into())
        );
        let (status, text) = call(&app, "GET", "/metrics", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            text.contains("id_service_requests_total{route=\"/healthz\",status=\"200\"}"),
            "{text}"
        );
        assert!(
            text.contains("id_service_cache_entries{direction=\"object\"} 0"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn resolve_and_reverse_keep_the_response_contract() {
        let (store, app) = app();
        let (status, body) = post_json(
            &app,
            "/v1/allocate",
            json!({"object_id": USER, "entity_kind": "User", "snowflake_id": 4242}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({"object_id": USER, "entity_kind": "User", "snowflake_id": 4242, "mapping_version": MAPPING_VERSION})
        );
        assert_eq!(
            store
                .by_object(EntityKind::User, USER)
                .unwrap()
                .snowflake_id
                .get(),
            4242
        );

        let (status, body) = post_json(
            &app,
            "/v1/resolve_partial:batch",
            json!({"ids": [
                {"object_id": USER, "entity_kind": "User"},
                {"object_id": "65f1a2b3c4d5e6f708091099", "entity_kind": "User"}
            ]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["snowflake_id"], 4242);
        assert!(rows[1].get("snowflake_id").is_none());

        // Allocation for a new post; the batch keeps input order and echoes each item.
        let (status, body) = post_json(
            &app,
            "/v1/allocate:batch",
            json!({"ids": [
                {"object_id": POST, "entity_kind": "Post"},
                {"object_id": USER, "entity_kind": "User"},
            ]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["object_id"], POST);
        assert_eq!(rows[0]["entity_kind"], "Post");
        assert_eq!(rows[0]["mapping_version"], MAPPING_VERSION);
        assert!(rows[0]["snowflake_id"].as_u64().unwrap() > 0);
        assert_eq!(
            rows[1],
            json!({"object_id": USER, "entity_kind": "User", "snowflake_id": 4242, "mapping_version": MAPPING_VERSION})
        );
        let post_snowflake = rows[0]["snowflake_id"].as_u64().unwrap();

        let (status, body) = post_json(
            &app,
            "/v1/reverse",
            json!({"snowflake_id": 4242, "entity_kind": "User"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({"snowflake_id": 4242, "object_id": USER, "entity_kind": "User", "mapping_version": MAPPING_VERSION})
        );

        let (status, body) = post_json(
            &app,
            "/v1/reverse:batch",
            json!({"ids": [
                {"snowflake_id": post_snowflake, "entity_kind": "Post"},
                {"snowflake_id": 4242, "entity_kind": "User"},
            ]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().unwrap();
        assert_eq!(rows[0]["object_id"], POST);
        assert_eq!(rows[0]["entity_kind"], "Post");
        assert_eq!(rows[1]["object_id"], USER);
        assert_eq!(rows[1]["entity_kind"], "User");
    }

    #[tokio::test]
    async fn status_codes_follow_the_error_variant() {
        let (_, app) = app();
        // 400: invalid ObjectId, invalid Snowflake in body.
        let (status, _) = post_json(
            &app,
            "/v1/allocate",
            json!({"object_id": "not-hex", "entity_kind": "User"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = post_json(
            &app,
            "/v1/allocate",
            json!({"object_id": USER, "entity_kind": "User", "snowflake_id": 0}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = post_json(
            &app,
            "/v1/reverse",
            json!({"snowflake_id": 0, "entity_kind": "User"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = post_json(
            &app,
            "/v1/reverse:batch",
            json!({"ids": [{"snowflake_id": 9223372036854775808u64, "entity_kind": "User"}]}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // 404: unknown Snowflake.
        let (status, body) = post_json(
            &app,
            "/v1/reverse",
            json!({"snowflake_id": 555000111, "entity_kind": "Post"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

        // 409: provided id already bound elsewhere, existing mapping contradicted, kind mismatch.
        post_json(
            &app,
            "/v1/allocate",
            json!({"object_id": USER, "entity_kind": "User", "snowflake_id": 7}),
        )
        .await;
        let (status, body) = post_json(
            &app,
            "/v1/allocate",
            json!({"object_id": POST, "entity_kind": "Post", "snowflake_id": 7}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        let (status, _) = post_json(
            &app,
            "/v1/allocate",
            json!({"object_id": USER, "entity_kind": "User", "snowflake_id": 8}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        let (status, _) = post_json(
            &app,
            "/v1/reverse",
            json!({"snowflake_id": 7, "entity_kind": "Post"}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);

        // 413: batch above the configured limit (3).
        let too_many = (0..4)
            .map(|index| json!({"object_id": format!("65f1a2b3c4d5e6f70809{index:04x}"), "entity_kind": "User"}))
            .collect::<Vec<_>>();
        let (status, _) = post_json(&app, "/v1/resolve:batch", json!({"ids": too_many})).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        let too_many = (0..4)
            .map(|index| json!({"snowflake_id": 100 + index, "entity_kind": "User"}))
            .collect::<Vec<_>>();
        let (status, _) = post_json(&app, "/v1/reverse:batch", json!({"ids": too_many})).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn allocation_gate() {
        // Read-only misses are 404 regardless of the allocation setting and
        // do not claim that allocation is disabled.
        let store = Arc::new(MemoryMappingStore::new());
        let app = app_with(store, false, 100);
        let (status, body) = post_json(
            &app,
            "/v1/resolve:batch",
            json!({"ids": [
                {"object_id": USER, "entity_kind": "User"},
                {"object_id": POST, "entity_kind": "Post"},
            ]}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let text = body.as_str().unwrap();
        assert!(
            text.contains("2 ids") && text.contains(USER) && text.contains(POST),
            "{text}"
        );
        assert!(text.contains("no ObjectId mapping exists"), "{text}");
        assert!(!text.contains("allocation is disabled"), "{text}");

        // The explicit write endpoint reports the disabled allocation gate.
        let (status, body) = post_json(
            &app,
            "/v1/allocate:batch",
            json!({"ids": [
                {"object_id": USER, "entity_kind": "User"},
                {"object_id": POST, "entity_kind": "Post"},
            ]}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let text = body.as_str().unwrap();
        assert!(text.contains("allocation is disabled"), "{text}");

        // Resolve is read-only: a provided snowflake_id is rejected with 403.
        let store = Arc::new(MemoryMappingStore::new());
        let app = app_with(store.clone(), true, 100);
        let (status, body) = post_json(
            &app,
            "/v1/resolve",
            json!({"object_id": USER, "entity_kind": "User", "snowflake_id": 4242}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        let (status, _) = post_json(
            &app,
            "/v1/resolve:batch",
            json!({"ids": [
                {"object_id": POST, "entity_kind": "Post"},
                {"object_id": USER, "entity_kind": "User", "snowflake_id": 4242},
            ]}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(store.by_object(EntityKind::Post, POST).is_none());
        assert!(store.by_object(EntityKind::User, USER).is_none());
    }

    #[tokio::test]
    async fn store_failures_are_503() {
        let app = app_with(Arc::new(FailingStore), true, 100);
        let (status, body) = call(&app, "GET", "/readyz", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            body.contains("mapping version metadata is missing"),
            "{body}"
        );
        let (status, _) = post_json(
            &app,
            "/v1/resolve",
            json!({"object_id": USER, "entity_kind": "User"}),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let (status, _) = post_json(
            &app,
            "/v1/reverse",
            json!({"snowflake_id": 7, "entity_kind": "User"}),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn every_variant_has_a_status() {
        let corrupt = IdError::CorruptRecord {
            key: "k".into(),
            reason: "Redis returned an unknown entity kind".into(),
        };
        assert_eq!(status_for(&corrupt), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            status_for(&IdError::UnknownSnowflake(1)),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_for(&IdError::UnknownObjectIds("User unknown".into())),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_for(&IdError::StorageSchemaMismatch {
                expected: "a".into(),
                actual: "b".into()
            }),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status_for(&IdError::SecondExhausted(1)),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            status_for(&IdError::InvalidWorkerId(5000)),
            StatusCode::BAD_REQUEST
        );
    }
}
