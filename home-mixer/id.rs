//! Home Mixer identity boundary.
//!
//! Public RPCs carry ObjectId strings. This module is the only Home Mixer
//! boundary that resolves them before the internal Snowflake migration.

pub use id_service::{EntityKind, IdError, SnowflakeId};
use serde::Deserialize;
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;
use tonic::transport::{Channel, Endpoint};
use x_algorithm_proto::id_registry as registry_pb;

const ID_REGISTRY_REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

/// Internal identity used by the recommendation path after ingress
/// normalization.
pub type InternalId = SnowflakeId;

#[derive(Clone)]
pub struct RegistryClient {
    grpc: Option<GrpcRegistryTransport>,
    http: Option<HttpRegistryTransport>,
    calls: crate::metrics::ClientCallRecorder,
}

pub type SharedRegistryClient = std::sync::Arc<RegistryClient>;

/// Read-only ObjectId ↔ Snowflake boundary.
#[tonic::async_trait]
pub trait IdentityReader: Send + Sync {
    async fn resolve_batch(&self, ids: &[(String, EntityKind)])
        -> anyhow::Result<Vec<SnowflakeId>>;
    async fn reverse_batch(&self, ids: &[(SnowflakeId, EntityKind)])
        -> anyhow::Result<Vec<String>>;
}

/// Write-side identity capability. Only ingress adapters receive this trait.
#[tonic::async_trait]
pub trait IdentityAllocator: Send + Sync {
    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>>;
}

/// Combined capability used only by ingress normalizers.
pub trait IdentityIngress: IdentityReader + IdentityAllocator {}
impl<T: IdentityReader + IdentityAllocator> IdentityIngress for T {}

pub type SharedIdentityReader = std::sync::Arc<dyn IdentityReader>;
// An `Arc<dyn IdentityIngress>` narrows to `Arc<dyn IdentityReader>` via
// trait upcasting, but only at explicit coercion sites (struct fields, let
// bindings, return positions) — routing it through generic functions like
// `Arc::clone` fails to compile. When one owner needs both views, erase the
// concrete `Arc<RegistryClient>` into each trait object separately (see
// `HomeMixerServer::build_with_metrics`).
pub type SharedIdentityIngress = std::sync::Arc<dyn IdentityIngress>;

#[tonic::async_trait]
impl IdentityReader for RegistryClient {
    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        RegistryClient::resolve_batch(self, ids).await
    }

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> anyhow::Result<Vec<String>> {
        RegistryClient::reverse_batch(self, ids).await
    }
}

#[tonic::async_trait]
impl IdentityAllocator for RegistryClient {
    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        let request = ids
            .iter()
            .map(|(id, kind)| (id.clone(), *kind, None))
            .collect::<Vec<_>>();
        RegistryClient::allocate_batch(self, &request).await
    }
}

/// Deterministic resolver for tests and compatibility constructors that run
/// without external services. It only accepts the zero-padded ObjectId form
/// `00000000 + 16 hex` produced by `ObjectId::from_u64_be_padded`; real
/// ObjectIds are rejected instead of being silently remapped.
#[derive(Default)]
pub struct PaddedIdentityResolver;

impl PaddedIdentityResolver {
    pub fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl IdentityReader for PaddedIdentityResolver {
    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        ids.iter()
            .map(|(object_id, _)| {
                let parsed = crate::models::ObjectId::parse(object_id)
                    .map_err(|error| anyhow::anyhow!("padded resolver: {error}"))?;
                let value = parsed
                    .to_u64_be_padded()
                    .filter(|value| *value != 0 && *value <= i64::MAX as u64)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "padded resolver cannot map ObjectId {object_id}; use the ID Registry"
                        )
                    })?;
                SnowflakeId::new(value).map_err(Into::into)
            })
            .collect()
    }

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> anyhow::Result<Vec<String>> {
        ids.iter()
            .map(|(id, _)| {
                anyhow::ensure!(
                    !id.is_nil() && id.get() <= i64::MAX as u64,
                    "padded resolver cannot reverse Snowflake {id}"
                );
                Ok(crate::models::ObjectId::from_u64_be_padded(id.get()).to_string())
            })
            .collect()
    }
}

#[tonic::async_trait]
impl IdentityAllocator for PaddedIdentityResolver {
    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        IdentityReader::resolve_batch(self, ids).await
    }
}

#[derive(Deserialize)]
struct ResolvedId {
    object_id: String,
    entity_kind: EntityKind,
    snowflake_id: u64,
    mapping_version: u32,
}

#[derive(Deserialize)]
struct ReversedId {
    snowflake_id: u64,
    object_id: String,
    entity_kind: EntityKind,
    mapping_version: u32,
}

#[derive(Clone)]
struct HttpRegistryTransport {
    client: reqwest::Client,
    endpoint: reqwest::Url,
}

#[derive(Clone)]
struct GrpcRegistryTransport {
    endpoint: Endpoint,
    channel: std::sync::Arc<OnceCell<Channel>>,
}

#[tonic::async_trait]
trait RegistryTransport: Send + Sync {
    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>>;

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ReversedId>>;

    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>>;
}

fn http_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .connect_timeout(ID_REGISTRY_REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()?)
}

fn remaining(deadline: Instant) -> anyhow::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| anyhow::anyhow!("ID Registry request deadline exceeded"))
}

impl HttpRegistryTransport {
    fn new(endpoint: &str) -> anyhow::Result<Self> {
        let endpoint = reqwest::Url::parse(endpoint)?;
        anyhow::ensure!(
            matches!(endpoint.scheme(), "http" | "https"),
            "ID_REGISTRY_URL must use HTTP or HTTPS"
        );
        Ok(Self {
            client: http_client()?,
            endpoint,
        })
    }
}

#[tonic::async_trait]
impl RegistryTransport for HttpRegistryTransport {
    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>> {
        let payload = ids
            .iter()
            .map(|(id, kind, provided)| {
                let mut value = serde_json::json!({
                    "object_id": id,
                    "entity_kind": kind,
                });
                if let Some(provided) = provided {
                    value["snowflake_id"] = serde_json::json!(provided.get());
                }
                value
            })
            .collect::<Vec<_>>();
        Ok(self
            .client
            .post(self.endpoint.join("/v1/resolve:batch")?)
            .json(&serde_json::json!({"ids": payload}))
            .timeout(timeout)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ReversedId>> {
        let payload = serde_json::json!({
            "ids": ids
                .iter()
                .map(|(id, kind)| {
                    serde_json::json!({"snowflake_id": id.get(), "entity_kind": kind})
                })
                .collect::<Vec<_>>()
        });
        Ok(self
            .client
            .post(self.endpoint.join("/v1/reverse:batch")?)
            .json(&payload)
            .timeout(timeout)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>> {
        let payload = ids
            .iter()
            .map(|(id, kind, provided)| {
                let mut value = serde_json::json!({"object_id": id, "entity_kind": kind});
                if let Some(provided) = provided {
                    value["snowflake_id"] = serde_json::json!(provided.get());
                }
                value
            })
            .collect::<Vec<_>>();
        Ok(self
            .client
            .post(self.endpoint.join("/v1/allocate:batch")?)
            .json(&serde_json::json!({"ids": payload}))
            .timeout(timeout)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

impl GrpcRegistryTransport {
    fn new(endpoint: &str) -> anyhow::Result<Self> {
        Ok(Self {
            endpoint: Endpoint::from_shared(endpoint.to_string())?
                .connect_timeout(ID_REGISTRY_REQUEST_TIMEOUT)
                .timeout(ID_REGISTRY_REQUEST_TIMEOUT),
            channel: std::sync::Arc::new(OnceCell::new()),
        })
    }

    async fn channel(&self) -> Channel {
        self.channel
            .get_or_init(|| async { self.endpoint.connect_lazy() })
            .await
            .clone()
    }
}

#[tonic::async_trait]
impl RegistryTransport for GrpcRegistryTransport {
    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>> {
        let request = registry_pb::ResolveBatchRequest {
            ids: ids
                .iter()
                .map(|(object_id, kind, provided)| registry_pb::ResolveRequest {
                    object_id: object_id.clone(),
                    entity_kind: proto_kind(*kind),
                    snowflake_id: provided.map(SnowflakeId::get),
                })
                .collect(),
        };
        let mut request = tonic::Request::new(request);
        request.set_timeout(timeout);
        let response =
            registry_pb::identity_registry_service_client::IdentityRegistryServiceClient::new(
                self.channel().await,
            )
            .resolve_batch(request)
            .await?
            .into_inner();
        response
            .rows
            .into_iter()
            .map(|row| {
                Ok(ResolvedId {
                    object_id: row.object_id,
                    entity_kind: entity_kind(row.entity_kind)?,
                    snowflake_id: row.snowflake_id,
                    mapping_version: row.mapping_version,
                })
            })
            .collect()
    }

    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ReversedId>> {
        let request = registry_pb::ReverseBatchRequest {
            ids: ids
                .iter()
                .map(|(id, kind)| registry_pb::ReverseRequest {
                    snowflake_id: id.get(),
                    entity_kind: proto_kind(*kind),
                })
                .collect(),
        };
        let mut request = tonic::Request::new(request);
        request.set_timeout(timeout);
        let response =
            registry_pb::identity_registry_service_client::IdentityRegistryServiceClient::new(
                self.channel().await,
            )
            .reverse_batch(request)
            .await?
            .into_inner();
        response
            .rows
            .into_iter()
            .map(|row| {
                Ok(ReversedId {
                    snowflake_id: row.snowflake_id,
                    object_id: row.object_id,
                    entity_kind: entity_kind(row.entity_kind)?,
                    mapping_version: row.mapping_version,
                })
            })
            .collect()
    }

    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
        timeout: Duration,
    ) -> anyhow::Result<Vec<ResolvedId>> {
        let request = registry_pb::ResolveBatchRequest {
            ids: ids
                .iter()
                .map(|(object_id, kind, provided)| registry_pb::ResolveRequest {
                    object_id: object_id.clone(),
                    entity_kind: proto_kind(*kind),
                    snowflake_id: provided.map(SnowflakeId::get),
                })
                .collect(),
        };
        let mut request = tonic::Request::new(request);
        request.set_timeout(timeout);
        let response =
            registry_pb::identity_registry_service_client::IdentityRegistryServiceClient::new(
                self.channel().await,
            )
            .allocate_batch(request)
            .await?
            .into_inner();
        response
            .rows
            .into_iter()
            .map(|row| {
                Ok(ResolvedId {
                    object_id: row.object_id,
                    entity_kind: entity_kind(row.entity_kind)?,
                    snowflake_id: row.snowflake_id,
                    mapping_version: row.mapping_version,
                })
            })
            .collect()
    }
}

fn proto_kind(kind: EntityKind) -> i32 {
    match kind {
        EntityKind::User => registry_pb::EntityKind::User as i32,
        EntityKind::Post => registry_pb::EntityKind::Post as i32,
    }
}

fn entity_kind(value: i32) -> anyhow::Result<EntityKind> {
    match registry_pb::EntityKind::try_from(value).unwrap_or(registry_pb::EntityKind::Unspecified) {
        registry_pb::EntityKind::User => Ok(EntityKind::User),
        registry_pb::EntityKind::Post => Ok(EntityKind::Post),
        registry_pb::EntityKind::Unspecified => Err(anyhow::anyhow!(
            "ID Registry returned an unspecified entity kind"
        )),
    }
}

fn validate_resolve_rows(
    rows: Vec<ResolvedId>,
    ids: &[(String, EntityKind, Option<SnowflakeId>)],
) -> anyhow::Result<Vec<SnowflakeId>> {
    anyhow::ensure!(
        rows.len() == ids.len(),
        "ID Registry response count mismatch"
    );
    rows.into_iter()
        .zip(ids)
        .map(|(row, (id, kind, provided))| {
            anyhow::ensure!(
                row.object_id == *id && row.entity_kind == *kind,
                "ID Registry response identity mismatch"
            );
            anyhow::ensure!(
                row.mapping_version == id_service::MAPPING_VERSION,
                "ID Registry returned unsupported mapping_version {}",
                row.mapping_version
            );
            let resolved = SnowflakeId::new(row.snowflake_id)?;
            if let Some(provided) = provided {
                anyhow::ensure!(
                    resolved == *provided,
                    "ID Registry returned a mismatched provided Snowflake"
                );
            }
            Ok(resolved)
        })
        .collect()
}

fn validate_reverse_rows(
    rows: Vec<ReversedId>,
    ids: &[(SnowflakeId, EntityKind)],
) -> anyhow::Result<Vec<String>> {
    anyhow::ensure!(
        rows.len() == ids.len(),
        "ID Registry response count mismatch"
    );
    rows.into_iter()
        .zip(ids)
        .map(|(row, (id, kind))| {
            anyhow::ensure!(
                row.snowflake_id == id.get() && row.entity_kind == *kind,
                "ID Registry response identity mismatch"
            );
            anyhow::ensure!(
                row.mapping_version == id_service::MAPPING_VERSION,
                "ID Registry returned unsupported mapping_version {}",
                row.mapping_version
            );
            let object_id = crate::models::ObjectId::parse(&row.object_id)
                .map_err(|error| anyhow::anyhow!("ID Registry returned {error}"))?;
            anyhow::ensure!(!object_id.is_nil(), "ID Registry returned nil ObjectId");
            Ok(row.object_id)
        })
        .collect()
}

impl RegistryClient {
    /// Construct the legacy HTTP-only client. Production Home Mixer code uses
    /// [`Self::new_with_grpc`] so an RPC failure is returned directly.
    pub fn new(endpoint: &str) -> anyhow::Result<Self> {
        Ok(Self {
            grpc: None,
            http: Some(HttpRegistryTransport::new(endpoint)?),
            calls: crate::metrics::ClientCallRecorder::default(),
        })
    }

    /// Build the production client. Home Mixer uses gRPC as its only Registry
    /// transport; the HTTP constructor above is reserved for legacy callers
    /// and compatibility tests.
    pub fn new_with_grpc(grpc_endpoint: &str) -> anyhow::Result<Self> {
        Ok(Self {
            grpc: Some(GrpcRegistryTransport::new(grpc_endpoint)?),
            http: None,
            calls: crate::metrics::ClientCallRecorder::default(),
        })
    }

    /// Attach process metrics without changing the transport behavior.
    pub fn with_calls(mut self, calls: crate::metrics::ClientCallRecorder) -> Self {
        self.calls = calls;
        self
    }

    /// Read-only resolution: unknown ObjectIds come back as an error and
    /// nothing is written. Ingress adapters that may create mappings use
    /// [`Self::allocate_batch`] instead.
    pub async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let request = ids
            .iter()
            .map(|(id, kind)| (id.clone(), *kind, None))
            .collect::<Vec<_>>();
        let deadline = Instant::now() + ID_REGISTRY_REQUEST_TIMEOUT;
        let started = Instant::now();
        let method = if self.grpc.is_some() {
            "resolve_grpc"
        } else {
            "resolve_http"
        };
        let rows = if let Some(grpc) = &self.grpc {
            match grpc.resolve_batch(&request, remaining(deadline)?).await {
                Ok(rows) => rows,
                Err(error) => {
                    self.calls.record(
                        "id_registry",
                        method,
                        registry_error_label(&error, "mapping_miss"),
                        started,
                    );
                    return Err(error);
                }
            }
        } else {
            let http = self.http.as_ref().ok_or_else(|| {
                anyhow::anyhow!("ID Registry HTTP compatibility transport is not configured")
            })?;
            match http.resolve_batch(&request, remaining(deadline)?).await {
                Ok(rows) => rows,
                Err(error) => {
                    self.calls.record(
                        "id_registry",
                        method,
                        registry_error_label(&error, "mapping_miss"),
                        started,
                    );
                    return Err(error);
                }
            }
        };
        let result = validate_resolve_rows(rows, &request);
        self.calls.record(
            "id_registry",
            method,
            if result.is_ok() { "ok" } else { "rejected" },
            started,
        );
        result
    }

    /// Allocate identities for an ingress adapter. Items carrying a
    /// caller-provided Snowflake are registered with that exact value;
    /// items without one receive a freshly allocated id. The Registry
    /// server still enforces whether allocation is enabled.
    pub async fn allocate_batch(
        &self,
        request: &[(String, EntityKind, Option<SnowflakeId>)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        if request.is_empty() {
            return Ok(Vec::new());
        }
        let deadline = Instant::now() + ID_REGISTRY_REQUEST_TIMEOUT;
        let started = Instant::now();
        let method = if self.grpc.is_some() {
            "allocate_grpc"
        } else {
            "allocate_http"
        };
        let rows = if let Some(grpc) = &self.grpc {
            match grpc.allocate_batch(request, remaining(deadline)?).await {
                Ok(rows) => rows,
                Err(error) => {
                    self.calls.record(
                        "id_registry",
                        method,
                        registry_error_label(&error, "mapping_miss"),
                        started,
                    );
                    return Err(error);
                }
            }
        } else {
            let http = self
                .http
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("ID Registry transport is not configured"))?;
            match http.allocate_batch(request, remaining(deadline)?).await {
                Ok(rows) => rows,
                Err(error) => {
                    self.calls.record(
                        "id_registry",
                        method,
                        registry_error_label(&error, "mapping_miss"),
                        started,
                    );
                    return Err(error);
                }
            }
        };
        let result = validate_resolve_rows(rows, request);
        if result.is_ok() {
            self.calls.record("id_registry", method, "ok", started);
        } else {
            self.calls
                .record("id_registry", method, "rejected", started);
        }
        result
    }

    pub async fn resolve_one(
        &self,
        object_id: &str,
        entity_kind: EntityKind,
    ) -> anyhow::Result<SnowflakeId> {
        self.resolve_batch(&[(object_id.to_string(), entity_kind)])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("ID Registry returned no result"))
    }

    pub async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> anyhow::Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let deadline = Instant::now() + ID_REGISTRY_REQUEST_TIMEOUT;
        if let Some(grpc) = &self.grpc {
            let started = std::time::Instant::now();
            match grpc.reverse_batch(ids, remaining(deadline)?).await {
                Ok(rows) => {
                    let result = validate_reverse_rows(rows, ids);
                    self.calls.record(
                        "id_registry",
                        "reverse_grpc",
                        if result.is_ok() { "ok" } else { "rejected" },
                        started,
                    );
                    return result;
                }
                Err(error) => {
                    self.calls.record(
                        "id_registry",
                        "reverse_grpc",
                        registry_error_label(&error, "reverse_miss"),
                        started,
                    );
                    return Err(error);
                }
            }
        }
        let http = self.http.as_ref().ok_or_else(|| {
            anyhow::anyhow!("ID Registry HTTP compatibility transport is not configured")
        })?;
        let started = std::time::Instant::now();
        let result = match http.reverse_batch(ids, remaining(deadline)?).await {
            Ok(rows) => validate_reverse_rows(rows, ids),
            Err(error) => {
                self.calls.record(
                    "id_registry",
                    "reverse_http",
                    registry_error_label(&error, "reverse_miss"),
                    started,
                );
                return Err(error);
            }
        };
        self.calls.record(
            "id_registry",
            "reverse_http",
            if result.is_ok() { "ok" } else { "rejected" },
            started,
        );
        result
    }
}

fn registry_error_label(error: &anyhow::Error, not_found_label: &'static str) -> &'static str {
    // Only transport-level "not found" (gRPC NotFound / HTTP 404) counts as a
    // mapping miss; everything else — including client-side row validation
    // failures like an unsupported mapping_version — is a genuine error.
    let is_not_found = error.chain().any(|cause| {
        cause
            .downcast_ref::<tonic::Status>()
            .is_some_and(|status| status.code() == tonic::Code::NotFound)
            || cause
                .downcast_ref::<reqwest::Error>()
                .and_then(|error| error.status())
                .is_some_and(|status| status == reqwest::StatusCode::NOT_FOUND)
    });
    if is_not_found {
        not_found_label
    } else {
        "error"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Json, Router};
    use id_service::{
        grpc::{GrpcIdRegistryService, IdentityRegistryServiceServer},
        MemoryMappingStore, RedisIdRegistry,
    };
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), server)
    }

    async fn serve_grpc(
        registry: Arc<RedisIdRegistry>,
        max_batch_size: usize,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpListenerStream::new(listener);
        let service = GrpcIdRegistryService::new(registry, max_batch_size);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(IdentityRegistryServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        (format!("http://{addr}"), server)
    }

    fn unused_endpoint() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{addr}")
    }

    async fn serve_blackhole() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut connections = Vec::new();
            loop {
                let (connection, _) = listener.accept().await.unwrap();
                // Keep the connection open without speaking HTTP/2. The client
                // must terminate the RPC using its configured request deadline.
                connections.push(connection);
            }
        });
        (format!("http://{addr}"), server)
    }

    #[tokio::test]
    async fn production_client_uses_the_real_grpc_server() {
        let store = Arc::new(MemoryMappingStore::new());
        let registry = Arc::new(RedisIdRegistry::with_store(store, 0, true).unwrap());
        let (grpc_endpoint, server) = serve_grpc(registry, 10).await;
        let client = RegistryClient::new_with_grpc(&grpc_endpoint).unwrap();
        let ids = [("65f1a2b3c4d5e6f708091011".to_string(), EntityKind::User)];

        let resolved = client
            .allocate_batch(&[(
                ids[0].0.clone(),
                ids[0].1,
                Some(SnowflakeId::new(4242).unwrap()),
            )])
            .await
            .unwrap();
        assert_eq!(resolved, vec![SnowflakeId::new(4242).unwrap()]);
        assert_eq!(
            client
                .reverse_batch(&[(resolved[0], EntityKind::User)])
                .await
                .unwrap(),
            vec![ids[0].0.clone()]
        );
        server.abort();
    }

    #[tokio::test]
    async fn production_client_returns_grpc_error_without_http_fallback() {
        let metrics = crate::metrics::Metrics::new();
        let client = RegistryClient::new_with_grpc(&unused_endpoint())
            .unwrap()
            .with_calls(metrics.client_calls());

        let error = client
            .resolve_batch(&[("65f1a2b3c4d5e6f708091011".into(), EntityKind::User)])
            .await
            .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<tonic::Status>()
                .expect("transport errors preserve gRPC status")
                .code(),
            tonic::Code::Unavailable
        );
        let metrics_text = metrics.encode().unwrap();
        assert!(metrics_text.contains(
            "home_mixer_client_calls_total{client=\"id_registry\",method=\"resolve_grpc\",result=\"error\"} 1"
        ));
    }

    #[tokio::test]
    async fn empty_allocation_batch_does_not_call_registry() {
        let client = RegistryClient::new(&unused_endpoint()).unwrap();
        assert!(client.allocate_batch(&[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn production_client_respects_one_total_deadline_when_grpc_times_out() {
        let (grpc_endpoint, grpc_server) = serve_blackhole().await;
        let client = RegistryClient::new_with_grpc(&grpc_endpoint).unwrap();
        let started = std::time::Instant::now();

        let error = client
            .resolve_batch(&[("65f1a2b3c4d5e6f708091011".into(), EntityKind::User)])
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<tonic::Status>().is_some());
        assert!(started.elapsed() >= ID_REGISTRY_REQUEST_TIMEOUT);
        assert!(started.elapsed() < Duration::from_secs(1));
        grpc_server.abort();
    }

    #[tokio::test]
    async fn business_errors_from_grpc_are_returned_directly() {
        let store = Arc::new(MemoryMappingStore::new());
        let registry = Arc::new(RedisIdRegistry::with_store(store, 0, false).unwrap());
        let (grpc_endpoint, grpc_server) = serve_grpc(registry, 10).await;
        let client = RegistryClient::new_with_grpc(&grpc_endpoint).unwrap();

        let error = client
            .resolve_batch(&[("65f1a2b3c4d5e6f708091011".into(), EntityKind::User)])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no ObjectId mapping exists"));
        grpc_server.abort();
    }

    #[tokio::test]
    async fn registry_client_preserves_batch_order_and_rejects_mismatched_identity() {
        let app = Router::new().route("/v1/resolve:batch", post(|Json(body): Json<Value>| async move {
            let ids = body["ids"].as_array().unwrap();
            assert_eq!(ids.len(), 2);
            Json(json!([
                {"object_id": ids[0]["object_id"], "entity_kind": ids[0]["entity_kind"], "snowflake_id": 9007199254740993_u64, "mapping_version": 2},
                {"object_id": ids[1]["object_id"], "entity_kind": ids[1]["entity_kind"], "snowflake_id": 71, "mapping_version": 2}
            ]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        let ids = vec![
            ("65f1a2b3c4d5e6f708091011".into(), EntityKind::User),
            ("65f1a2b3c4d5e6f708091012".into(), EntityKind::Post),
        ];
        let resolved = client.resolve_batch(&ids).await.unwrap();
        assert_eq!(
            resolved.iter().map(|id| id.get()).collect::<Vec<_>>(),
            vec![9007199254740993, 71]
        );
        server.abort();

        let app = Router::new().route("/v1/resolve:batch", post(|| async {
            Json(json!([{"object_id": "65f1a2b3c4d5e6f708091012", "entity_kind": "User", "snowflake_id": 71, "mapping_version": 2}]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        assert!(client.resolve_batch(&ids[..1]).await.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn resolve_batch_rejects_an_unknown_mapping_version() {
        let app = Router::new().route("/v1/resolve:batch", post(|Json(body): Json<Value>| async move {
            let ids = body["ids"].as_array().unwrap();
            Json(json!([
                {"object_id": ids[0]["object_id"], "entity_kind": ids[0]["entity_kind"], "snowflake_id": 71, "mapping_version": 3}
            ]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        let error = client
            .resolve_batch(&[("65f1a2b3c4d5e6f708091011".into(), EntityKind::User)])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("mapping_version"));
        server.abort();
    }

    #[tokio::test]
    async fn allocation_contract_rejection_is_not_recorded_as_ok() {
        let app = Router::new().route(
            "/v1/allocate:batch",
            post(|Json(body): Json<Value>| async move {
                let ids = body["ids"].as_array().unwrap();
                Json(json!([{
                    "object_id": ids[0]["object_id"],
                    "entity_kind": ids[0]["entity_kind"],
                    "snowflake_id": 71,
                    "mapping_version": 3
                }]))
            }),
        );
        let (endpoint, server) = serve(app).await;
        let metrics = crate::metrics::Metrics::new();
        let client = RegistryClient::new(&endpoint)
            .unwrap()
            .with_calls(metrics.client_calls());

        let error = client
            .allocate_batch(&[("65f1a2b3c4d5e6f708091011".into(), EntityKind::User, None)])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("mapping_version"));

        let text = metrics.encode().unwrap();
        assert!(text.contains(
            "home_mixer_client_calls_total{client=\"id_registry\",method=\"allocate_http\",result=\"rejected\"} 1"
        ));
        assert!(!text.contains(
            "home_mixer_client_calls_total{client=\"id_registry\",method=\"allocate_http\",result=\"ok\"} 1"
        ));
        server.abort();
    }

    #[tokio::test]
    async fn reverse_batch_sends_kind_and_validates_identity_version_and_object_id() {
        let app = Router::new().route("/v1/reverse:batch", post(|Json(body): Json<Value>| async move {
            let ids = body["ids"].as_array().unwrap();
            assert_eq!(ids.len(), 2);
            assert_eq!(ids[0]["entity_kind"], "Post");
            assert_eq!(ids[1]["entity_kind"], "Post");
            Json(json!([
                {"snowflake_id": ids[0]["snowflake_id"], "object_id": "65f1a2b3c4d5e6f708091011", "entity_kind": "Post", "mapping_version": 2},
                {"snowflake_id": ids[1]["snowflake_id"], "object_id": "65f1a2b3c4d5e6f708091012", "entity_kind": "Post", "mapping_version": 2}
            ]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        let ids = vec![
            (SnowflakeId::new(42).unwrap(), EntityKind::Post),
            (SnowflakeId::new(43).unwrap(), EntityKind::Post),
        ];
        assert_eq!(
            client.reverse_batch(&ids).await.unwrap(),
            vec!["65f1a2b3c4d5e6f708091011", "65f1a2b3c4d5e6f708091012"]
        );
        server.abort();
    }

    #[tokio::test]
    async fn reverse_batch_rejects_wrong_kind_version_and_order() {
        let ids = vec![(SnowflakeId::new(42).unwrap(), EntityKind::Post)];

        // Wrong entity kind echoed back.
        let app = Router::new().route("/v1/reverse:batch", post(|| async {
            Json(json!([{"snowflake_id": 42, "object_id": "65f1a2b3c4d5e6f708091011", "entity_kind": "User", "mapping_version": 2}]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        assert!(client.reverse_batch(&ids).await.is_err());
        server.abort();

        // Unknown mapping version.
        let app = Router::new().route("/v1/reverse:batch", post(|| async {
            Json(json!([{"snowflake_id": 42, "object_id": "65f1a2b3c4d5e6f708091011", "entity_kind": "Post", "mapping_version": 99}]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        assert!(client.reverse_batch(&ids).await.is_err());
        server.abort();

        // Rows returned out of order.
        let app = Router::new().route("/v1/reverse:batch", post(|| async {
            Json(json!([
                {"snowflake_id": 43, "object_id": "65f1a2b3c4d5e6f708091012", "entity_kind": "Post", "mapping_version": 2},
                {"snowflake_id": 42, "object_id": "65f1a2b3c4d5e6f708091011", "entity_kind": "Post", "mapping_version": 2}
            ]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        let two = vec![
            (SnowflakeId::new(42).unwrap(), EntityKind::Post),
            (SnowflakeId::new(43).unwrap(), EntityKind::Post),
        ];
        assert!(client.reverse_batch(&two).await.is_err());
        server.abort();

        // Invalid ObjectId payload.
        let app = Router::new().route("/v1/reverse:batch", post(|| async {
            Json(json!([{"snowflake_id": 42, "object_id": "not-an-object-id", "entity_kind": "Post", "mapping_version": 2}]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        assert!(client.reverse_batch(&ids).await.is_err());
        server.abort();
    }
}
