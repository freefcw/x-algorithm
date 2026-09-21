//! Primary gRPC surface of the identity service.
//!
//! HTTP remains available as a compatibility and operational endpoint, while
//! internal callers should use this service to avoid JSON parsing and HTTP
//! request overhead on the hot path.

#![allow(clippy::result_large_err)]

use crate::metrics::metrics;
use crate::{EntityKind, IdError, RedisIdRegistry, SnowflakeId, MAPPING_VERSION};
use pb::identity_registry_service_server::IdentityRegistryService;
use std::sync::Arc;
use tonic::{Code, Request, Response, Status};
use x_algorithm_proto::id_registry as pb;

pub use pb::identity_registry_service_server::IdentityRegistryServiceServer;

#[derive(Clone)]
pub struct GrpcIdRegistryService {
    registry: Arc<RedisIdRegistry>,
    max_batch_size: usize,
}

impl GrpcIdRegistryService {
    pub fn new(registry: Arc<RedisIdRegistry>, max_batch_size: usize) -> Self {
        Self {
            registry,
            max_batch_size,
        }
    }

    fn check_batch_size(&self, len: usize) -> Result<(), Status> {
        if len > self.max_batch_size {
            return Err(Status::resource_exhausted(format!(
                "batch of {len} ids exceeds the limit of {}",
                self.max_batch_size
            )));
        }
        Ok(())
    }
}

fn record_rpc<T>(method: &'static str, result: &Result<Response<T>, Status>) {
    let code = result
        .as_ref()
        .map(|_| Code::Ok)
        .unwrap_or_else(|error| error.code());
    metrics().record_request(method, code as u16);
}

fn entity_kind(value: i32) -> Result<EntityKind, Status> {
    match pb::EntityKind::try_from(value).unwrap_or(pb::EntityKind::Unspecified) {
        pb::EntityKind::User => Ok(EntityKind::User),
        pb::EntityKind::Post => Ok(EntityKind::Post),
        pb::EntityKind::Unspecified => {
            Err(Status::invalid_argument("entity_kind must be USER or POST"))
        }
    }
}

fn proto_entity_kind(value: EntityKind) -> i32 {
    match value {
        EntityKind::User => pb::EntityKind::User as i32,
        EntityKind::Post => pb::EntityKind::Post as i32,
    }
}

fn snowflake(value: u64) -> Result<SnowflakeId, Status> {
    SnowflakeId::new(value).map_err(|error| Status::invalid_argument(error.to_string()))
}

fn status_for(error: IdError) -> Status {
    let code = match error {
        IdError::InvalidObjectId(_)
        | IdError::UnsupportedSnowflake(_)
        | IdError::BeforeSnowflakeEpoch(_)
        | IdError::InvalidWorkerId(_) => Code::InvalidArgument,
        IdError::TrustedImportDisabled(_) => Code::PermissionDenied,
        IdError::AllocationDisabled(_)
        | IdError::UnknownObjectIds(_)
        | IdError::UnknownSnowflake(_) => Code::NotFound,
        IdError::MappingConflict { .. }
        | IdError::SnowflakeTaken { .. }
        | IdError::EntityKindMismatch { .. } => Code::AlreadyExists,
        IdError::Redis(_)
        | IdError::Io(_)
        | IdError::MappingVersionMismatch { .. }
        | IdError::StorageSchemaMismatch { .. } => Code::Unavailable,
        IdError::SecondExhausted(_) | IdError::CorruptRecord { .. } => Code::Internal,
    };
    Status::new(code, error.to_string())
}

fn resolve_response(
    object_id: String,
    entity_kind: EntityKind,
    snowflake_id: SnowflakeId,
) -> pb::ResolveResponse {
    pb::ResolveResponse {
        object_id,
        entity_kind: proto_entity_kind(entity_kind),
        snowflake_id: snowflake_id.get(),
        mapping_version: MAPPING_VERSION,
    }
}

fn reverse_response(mapping: crate::Mapping) -> pb::ReverseResponse {
    pb::ReverseResponse {
        snowflake_id: mapping.snowflake_id.get(),
        object_id: mapping.object_id,
        entity_kind: proto_entity_kind(mapping.entity_kind),
        mapping_version: mapping.mapping_version,
    }
}

#[tonic::async_trait]
impl IdentityRegistryService for GrpcIdRegistryService {
    async fn resolve(
        &self,
        request: Request<pb::ResolveRequest>,
    ) -> Result<Response<pb::ResolveResponse>, Status> {
        let result = async {
            let request = request.into_inner();
            crate::validate_object_id(&request.object_id).map_err(status_for)?;
            let kind = entity_kind(request.entity_kind)?;
            let trusted = request.snowflake_id.map(snowflake).transpose()?;
            if trusted.is_some() {
                return Err(status_for(IdError::TrustedImportDisabled(
                    request.object_id.clone(),
                )));
            }
            let resolved = self
                .registry
                .resolve_existing_one(&request.object_id, kind)
                .await
                .map_err(status_for)?;
            Ok(Response::new(resolve_response(
                request.object_id,
                kind,
                resolved,
            )))
        }
        .await;
        record_rpc("grpc.Resolve", &result);
        result
    }

    async fn resolve_batch(
        &self,
        request: Request<pb::ResolveBatchRequest>,
    ) -> Result<Response<pb::ResolveBatchResponse>, Status> {
        let result = async {
            let request = request.into_inner();
            self.check_batch_size(request.ids.len())?;
            // Match the HTTP contract: trusted imports are rejected with 403
            // before any per-item validation.
            if request
                .ids
                .iter()
                .any(|item| item.snowflake_id.is_some())
            {
                return Err(status_for(IdError::TrustedImportDisabled(
                    "resolve does not accept trusted imports".to_string(),
                )));
            }
            let ids = request
                .ids
                .iter()
                .map(|item| {
                    crate::validate_object_id(&item.object_id).map_err(status_for)?;
                    Ok((item.object_id.clone(), entity_kind(item.entity_kind)?))
                })
                .collect::<Result<Vec<_>, Status>>()?;
            let resolved = self
                .registry
                .resolve_existing_batch(&ids)
                .await
                .map_err(status_for)?;
            let rows = request
                .ids
                .into_iter()
                .zip(ids.into_iter().map(|(_, kind)| kind))
                .zip(resolved)
                .map(|((item, kind), id)| resolve_response(item.object_id, kind, id))
                .collect();
            Ok(Response::new(pb::ResolveBatchResponse { rows }))
        }
        .await;
        record_rpc("grpc.ResolveBatch", &result);
        result
    }

    async fn allocate(
        &self,
        request: Request<pb::ResolveRequest>,
    ) -> Result<Response<pb::ResolveResponse>, Status> {
        let result = async {
            let request = request.into_inner();
            crate::validate_object_id(&request.object_id).map_err(status_for)?;
            let kind = entity_kind(request.entity_kind)?;
            let trusted = request.snowflake_id.map(snowflake).transpose()?;
            let resolved = self
                .registry
                .resolve_one_with_trusted(&request.object_id, kind, trusted)
                .await
                .map_err(status_for)?;
            Ok(Response::new(resolve_response(
                request.object_id,
                kind,
                resolved,
            )))
        }
        .await;
        record_rpc("grpc.Allocate", &result);
        result
    }

    async fn allocate_batch(
        &self,
        request: Request<pb::ResolveBatchRequest>,
    ) -> Result<Response<pb::ResolveBatchResponse>, Status> {
        let result = async {
            let request = request.into_inner();
            self.check_batch_size(request.ids.len())?;
            let ids = request
                .ids
                .iter()
                .map(|item| {
                    crate::validate_object_id(&item.object_id).map_err(status_for)?;
                    Ok((
                        item.object_id.clone(),
                        entity_kind(item.entity_kind)?,
                        item.snowflake_id.map(snowflake).transpose()?,
                    ))
                })
                .collect::<Result<Vec<_>, Status>>()?;
            let resolved = self
                .registry
                .resolve_batch_with_trusted(&ids)
                .await
                .map_err(status_for)?;
            let rows = request
                .ids
                .into_iter()
                .zip(ids.into_iter().map(|(_, kind, _)| kind))
                .zip(resolved)
                .map(|((item, kind), id)| resolve_response(item.object_id, kind, id))
                .collect();
            Ok(Response::new(pb::ResolveBatchResponse { rows }))
        }
        .await;
        record_rpc("grpc.AllocateBatch", &result);
        result
    }

    async fn reverse(
        &self,
        request: Request<pb::ReverseRequest>,
    ) -> Result<Response<pb::ReverseResponse>, Status> {
        let result = async {
            let request = request.into_inner();
            let kind = entity_kind(request.entity_kind)?;
            let mapping = self
                .registry
                .reverse_one(snowflake(request.snowflake_id)?, kind)
                .await
                .map_err(status_for)?;
            Ok(Response::new(reverse_response(mapping)))
        }
        .await;
        record_rpc("grpc.Reverse", &result);
        result
    }

    async fn reverse_batch(
        &self,
        request: Request<pb::ReverseBatchRequest>,
    ) -> Result<Response<pb::ReverseBatchResponse>, Status> {
        let result = async {
            let request = request.into_inner();
            self.check_batch_size(request.ids.len())?;
            let ids = request
                .ids
                .iter()
                .map(|item| {
                    Ok((
                        snowflake(item.snowflake_id)?,
                        entity_kind(item.entity_kind)?,
                    ))
                })
                .collect::<Result<Vec<_>, Status>>()?;
            let mappings = self
                .registry
                .reverse_batch(&ids)
                .await
                .map_err(status_for)?;
            Ok(Response::new(pb::ReverseBatchResponse {
                rows: mappings.into_iter().map(reverse_response).collect(),
            }))
        }
        .await;
        record_rpc("grpc.ReverseBatch", &result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryMappingStore;

    #[tokio::test]
    async fn resolves_batches_through_the_grpc_contract() {
        let store = Arc::new(MemoryMappingStore::new());
        let registry = Arc::new(RedisIdRegistry::with_store(store, 0, true, true).unwrap());
        let service = GrpcIdRegistryService::new(registry, 10);
        let response = service
            .allocate_batch(Request::new(pb::ResolveBatchRequest {
                ids: vec![pb::ResolveRequest {
                    object_id: "65f1a2b3c4d5e6f708091011".into(),
                    entity_kind: pb::EntityKind::User as i32,
                    snowflake_id: Some(4242),
                }],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.rows.len(), 1);
        assert_eq!(response.rows[0].snowflake_id, 4242);
        assert_eq!(response.rows[0].mapping_version, MAPPING_VERSION);
    }

    #[test]
    fn maps_entity_kinds_without_accepting_unspecified() {
        assert_eq!(
            entity_kind(pb::EntityKind::User as i32).unwrap(),
            EntityKind::User
        );
        assert_eq!(
            entity_kind(pb::EntityKind::Post as i32).unwrap(),
            EntityKind::Post
        );
        assert_eq!(entity_kind(0).unwrap_err().code(), Code::InvalidArgument);
    }

    #[test]
    fn maps_domain_errors_to_stable_grpc_codes() {
        assert_eq!(
            status_for(IdError::UnknownSnowflake(42)).code(),
            Code::NotFound
        );
        assert_eq!(
            status_for(IdError::UnknownObjectIds("User unknown".into())).code(),
            Code::NotFound
        );
        assert_eq!(
            status_for(IdError::Redis("timeout".into())).code(),
            Code::Unavailable
        );
        assert_eq!(
            status_for(IdError::TrustedImportDisabled("x".into())).code(),
            Code::PermissionDenied
        );
    }
}
