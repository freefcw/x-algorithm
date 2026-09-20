use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use id_service::{EntityKind, IdError, IdRegistry, SnowflakeId, MAPPING_VERSION};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Parser, Debug)]
#[command(about = "ObjectId ↔ Snowflake identity service")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:50070")]
    listen: SocketAddr,
    #[arg(long, default_value = "./data/id-registry.jsonl")]
    registry: String,
    #[arg(long, default_value_t = 0)]
    worker_id: u64,
    /// Allow first-seen ObjectIDs to receive newly allocated Snowflakes.
    /// Keep disabled when the registry must preserve IDs from main, Thunder,
    /// or published Phoenix indexes; import trusted mappings instead.
    #[arg(long, default_value_t = false)]
    allow_allocation: bool,
}

#[derive(Clone)]
struct AppState {
    registry: Arc<Mutex<IdRegistry>>,
}

#[derive(Deserialize)]
struct ResolveRequest {
    object_id: String,
    entity_kind: EntityKind,
    #[serde(default)]
    trusted_snowflake_id: Option<u64>,
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
struct ReverseResponse {
    snowflake_id: u64,
    object_id: String,
    entity_kind: EntityKind,
    mapping_version: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if let Some(parent) = std::path::Path::new(&args.registry).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let registry =
        IdRegistry::open_with_options(&args.registry, args.worker_id, args.allow_allocation)?;
    let state = AppState {
        registry: Arc::new(Mutex::new(registry)),
    };
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/resolve", post(resolve))
        .route("/v1/resolve:batch", post(resolve_batch))
        .route("/v1/reverse", post(reverse))
        .route("/v1/reverse:batch", post(reverse_batch))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn resolve(
    State(state): State<AppState>,
    Json(request): Json<ResolveRequest>,
) -> Result<Json<ResolveResponse>, (StatusCode, String)> {
    let mut registry = state.registry.lock().await;
    let trusted = request
        .trusted_snowflake_id
        .map(SnowflakeId::new)
        .transpose()
        .map_err(internal_error)?;
    let snowflake_id = registry
        .resolve_one_with_trusted(&request.object_id, request.entity_kind, trusted)
        .map_err(registry_error)?;
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
) -> Result<Json<Vec<ResolveResponse>>, (StatusCode, String)> {
    let mut registry = state.registry.lock().await;
    let ids = request
        .ids
        .iter()
        .map(|item| {
            Ok((
                item.object_id.clone(),
                item.entity_kind,
                item.trusted_snowflake_id
                    .map(SnowflakeId::new)
                    .transpose()
                    .map_err(internal_error)?,
            ))
        })
        .collect::<Result<Vec<_>, (StatusCode, String)>>()?;
    let snowflakes = registry
        .resolve_batch_with_trusted(&ids)
        .map_err(registry_error)?;
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

async fn reverse(
    State(state): State<AppState>,
    Json(request): Json<ReverseRequest>,
) -> Result<Json<ReverseResponse>, (StatusCode, String)> {
    let snowflake_id = SnowflakeId::new(request.snowflake_id).map_err(internal_error)?;
    let mut registry = state.registry.lock().await;
    let mapping = registry
        .reverse_one(snowflake_id, request.entity_kind)
        .map_err(registry_error)?;
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
) -> Result<Json<Vec<ReverseResponse>>, (StatusCode, String)> {
    let ids = request
        .ids
        .iter()
        .map(|item| {
            SnowflakeId::new(item.snowflake_id)
                .map(|id| (id, item.entity_kind))
                .map_err(internal_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut registry = state.registry.lock().await;
    let mappings = registry.reverse_batch(&ids).map_err(registry_error)?;
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

fn registry_error(error: IdError) -> (StatusCode, String) {
    if matches!(error, IdError::EntityKindMismatch { .. }) {
        return (StatusCode::CONFLICT, error.to_string());
    }
    internal_error(error)
}

fn internal_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    let message = error.to_string();
    let status = if message.contains("allocation is disabled") {
        StatusCode::NOT_FOUND
    } else if message.contains("unsupported Snowflake")
        || message.contains("invalid ObjectId")
        || message.contains("predates Snowflake")
        || message.contains("exceeds")
    {
        StatusCode::BAD_REQUEST
    } else if message.contains("unknown") {
        StatusCode::NOT_FOUND
    } else if message.contains("conflict") || message.contains("duplicate") {
        StatusCode::CONFLICT
    } else if message.contains("I/O error") || message.contains("lock acquisition") {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    (status, message)
}
