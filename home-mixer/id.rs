//! Home Mixer identity boundary.
//!
//! Public RPCs carry ObjectId strings. This module is the only Home Mixer
//! boundary that resolves them before the internal Snowflake migration.

pub use id_service::{EntityKind, IdError, SnowflakeId};
use serde::Deserialize;
use std::time::Duration;

/// Internal identity used by the recommendation path after ingress
/// normalization.
pub type InternalId = SnowflakeId;

#[derive(Clone)]
pub struct RegistryClient {
    client: reqwest::Client,
    endpoint: reqwest::Url,
}

pub type SharedRegistryClient = std::sync::Arc<RegistryClient>;

/// Injectable ObjectId ↔ Snowflake boundary. Production resolves through the
/// shared ID Registry; tests and compatibility constructors can substitute a
/// deterministic implementation.
#[tonic::async_trait]
pub trait IdentityResolver: Send + Sync {
    async fn resolve_batch(&self, ids: &[(String, EntityKind)])
        -> anyhow::Result<Vec<SnowflakeId>>;

    async fn reverse_batch(&self, ids: &[(SnowflakeId, EntityKind)])
        -> anyhow::Result<Vec<String>>;
}

pub type SharedIdentityResolver = std::sync::Arc<dyn IdentityResolver>;

#[tonic::async_trait]
impl IdentityResolver for RegistryClient {
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
impl IdentityResolver for PaddedIdentityResolver {
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

impl RegistryClient {
    pub fn new(endpoint: &str) -> anyhow::Result<Self> {
        let endpoint = reqwest::Url::parse(endpoint)?;
        anyhow::ensure!(
            matches!(endpoint.scheme(), "http" | "https"),
            "ID_REGISTRY_URL must use HTTP or HTTPS"
        );
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_millis(500))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint,
        })
    }

    pub async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        self.resolve_batch_with_trusted(
            &ids.iter()
                .map(|(id, kind)| (id.clone(), *kind, None))
                .collect::<Vec<_>>(),
        )
        .await
    }

    /// Resolve IDs while optionally preserving an existing native Snowflake.
    /// Migration tooling should use this for IDs already present in main,
    /// Thunder, or a Phoenix checkpoint; online callers must not invent a
    /// second numeric identity for an existing data asset.
    pub async fn resolve_batch_with_trusted(
        &self,
        ids: &[(String, EntityKind, Option<SnowflakeId>)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let payload = ids
            .iter()
            .map(|(id, kind, trusted)| {
                let mut value = serde_json::json!({
                    "object_id": id, "entity_kind": kind,
                });
                if let Some(trusted) = trusted {
                    value["trusted_snowflake_id"] = serde_json::json!(trusted.get());
                }
                value
            })
            .collect::<Vec<_>>();
        let rows: Vec<ResolvedId> = self
            .client
            .post(self.endpoint.join("/v1/resolve:batch")?)
            .json(&serde_json::json!({"ids": payload}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        anyhow::ensure!(
            rows.len() == ids.len(),
            "ID Registry response count mismatch"
        );
        rows.into_iter()
            .zip(ids)
            .map(|(row, (id, kind, trusted))| {
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
                if let Some(trusted) = trusted {
                    anyhow::ensure!(
                        resolved == *trusted,
                        "ID Registry returned a mismatched trusted Snowflake"
                    );
                }
                Ok(resolved)
            })
            .collect()
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
        let payload = serde_json::json!({
            "ids": ids.iter().map(|(id, kind)| {
                serde_json::json!({"snowflake_id": id.get(), "entity_kind": kind})
            }).collect::<Vec<_>>()
        });
        let rows: Vec<ReversedId> = self
            .client
            .post(self.endpoint.join("/v1/reverse:batch")?)
            .json(&payload)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Json, Router};
    use serde_json::{json, Value};

    async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), server)
    }

    #[tokio::test]
    async fn registry_client_preserves_batch_order_and_rejects_mismatched_identity() {
        let app = Router::new().route("/v1/resolve:batch", post(|Json(body): Json<Value>| async move {
            let ids = body["ids"].as_array().unwrap();
            assert_eq!(ids.len(), 2);
            Json(json!([
                {"object_id": ids[0]["object_id"], "entity_kind": ids[0]["entity_kind"], "snowflake_id": 9007199254740993_u64, "mapping_version": 1},
                {"object_id": ids[1]["object_id"], "entity_kind": ids[1]["entity_kind"], "snowflake_id": 71, "mapping_version": 1}
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
            Json(json!([{"object_id": "65f1a2b3c4d5e6f708091012", "entity_kind": "User", "snowflake_id": 71, "mapping_version": 1}]))
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
                {"object_id": ids[0]["object_id"], "entity_kind": ids[0]["entity_kind"], "snowflake_id": 71, "mapping_version": 2}
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
    async fn reverse_batch_sends_kind_and_validates_identity_version_and_object_id() {
        let app = Router::new().route("/v1/reverse:batch", post(|Json(body): Json<Value>| async move {
            let ids = body["ids"].as_array().unwrap();
            assert_eq!(ids.len(), 2);
            assert_eq!(ids[0]["entity_kind"], "Post");
            assert_eq!(ids[1]["entity_kind"], "Post");
            Json(json!([
                {"snowflake_id": ids[0]["snowflake_id"], "object_id": "65f1a2b3c4d5e6f708091011", "entity_kind": "Post", "mapping_version": 1},
                {"snowflake_id": ids[1]["snowflake_id"], "object_id": "65f1a2b3c4d5e6f708091012", "entity_kind": "Post", "mapping_version": 1}
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
            Json(json!([{"snowflake_id": 42, "object_id": "65f1a2b3c4d5e6f708091011", "entity_kind": "User", "mapping_version": 1}]))
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
                {"snowflake_id": 43, "object_id": "65f1a2b3c4d5e6f708091012", "entity_kind": "Post", "mapping_version": 1},
                {"snowflake_id": 42, "object_id": "65f1a2b3c4d5e6f708091011", "entity_kind": "Post", "mapping_version": 1}
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
            Json(json!([{"snowflake_id": 42, "object_id": "not-an-object-id", "entity_kind": "Post", "mapping_version": 1}]))
        }));
        let (endpoint, server) = serve(app).await;
        let client = RegistryClient::new(&endpoint).unwrap();
        assert!(client.reverse_batch(&ids).await.is_err());
        server.abort();
    }
}
