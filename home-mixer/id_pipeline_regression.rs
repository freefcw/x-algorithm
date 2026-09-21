//! Cross-boundary numeric-ID regression: one real `RedisIdRegistry` import
//! feeds query ingress, the Thunder wire request, the VM Ranker wire request,
//! and egress reversal — proving the same Snowflake values survive end to end.

use crate::clients::in_network_posts_client::thunder_request;
use crate::clients::vm_ranker_client::{GrpcVMRankerClient, VmRankCandidate, VmRankRequest};
use crate::feature_policy::HomeMixerFeatures;
use crate::id::{EntityKind, IdentityAllocator, IdentityReader, SnowflakeId};
use crate::query_builder::QueryBuilder;
use id_service::{MemoryMappingStore, RedisIdRegistry};
use std::sync::Arc;

const VIEWER_OID: &str = "65f1a2b3c4d5e6f708091011";
const POST_OID: &str = "66a1b2c3d4e5f60718293a4b";
const AUTHOR_OID: &str = "67b0c1d2e3f4051627384a5b";
const VIEWER_SNOWFLAKE: u64 = 101;
const POST_SNOWFLAKE: u64 = 202;
const AUTHOR_SNOWFLAKE: u64 = 303;

struct RegistryIdentityResolver(Arc<RedisIdRegistry>);

#[tonic::async_trait]
impl IdentityReader for RegistryIdentityResolver {
    async fn resolve_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        Ok(self.0.resolve_batch(ids).await?)
    }
    async fn reverse_batch(
        &self,
        ids: &[(SnowflakeId, EntityKind)],
    ) -> anyhow::Result<Vec<String>> {
        Ok(self
            .0
            .reverse_batch(ids)
            .await?
            .into_iter()
            .map(|mapping| mapping.object_id)
            .collect())
    }
}

#[tonic::async_trait]
impl IdentityAllocator for RegistryIdentityResolver {
    async fn allocate_batch(
        &self,
        ids: &[(String, EntityKind)],
    ) -> anyhow::Result<Vec<SnowflakeId>> {
        Ok(self.0.resolve_batch(ids).await?)
    }
}

async fn fixture() -> Arc<RegistryIdentityResolver> {
    // The fixture imports trusted mappings, so trusted import is enabled;
    // allocation stays off like an online replica.
    let registry = RedisIdRegistry::with_store(Arc::new(MemoryMappingStore::new()), 0, false, true)
        .expect("build in-memory registry");
    registry
        .resolve_one_with_trusted(
            VIEWER_OID,
            EntityKind::User,
            Some(SnowflakeId::new(VIEWER_SNOWFLAKE).unwrap()),
        )
        .await
        .expect("import viewer mapping");
    registry
        .resolve_one_with_trusted(
            POST_OID,
            EntityKind::Post,
            Some(SnowflakeId::new(POST_SNOWFLAKE).unwrap()),
        )
        .await
        .expect("import post mapping");
    registry
        .resolve_one_with_trusted(
            AUTHOR_OID,
            EntityKind::User,
            Some(SnowflakeId::new(AUTHOR_SNOWFLAKE).unwrap()),
        )
        .await
        .expect("import author mapping");
    Arc::new(RegistryIdentityResolver(Arc::new(registry)))
}

#[tokio::test]
async fn numeric_ids_round_trip_across_thunder_vm_and_egress() {
    let resolver = fixture().await;

    let query = QueryBuilder::with_identity(
        HomeMixerFeatures::default(),
        std::sync::Arc::clone(&resolver) as crate::id::SharedIdentityIngress,
    )
    .build(x_algorithm_proto::home_mixer::ScoredPostsQuery {
        viewer_id: VIEWER_OID.to_string(),
        seen_ids: vec![POST_OID.to_string()],
        ..Default::default()
    })
    .await
    .expect("public query resolves through the registry")
    .query;
    assert_eq!(query.user_id, VIEWER_SNOWFLAKE);
    assert_eq!(query.seen_ids, vec![POST_SNOWFLAKE]);

    let thunder = thunder_request(&query).expect("thunder wire request");
    assert_eq!(thunder.user_id, VIEWER_SNOWFLAKE);
    assert!(thunder.exclude_tweet_ids.contains(&POST_SNOWFLAKE));

    let vm = GrpcVMRankerClient::to_proto(VmRankRequest {
        viewer_id: VIEWER_SNOWFLAKE,
        candidates: vec![VmRankCandidate {
            tweet_id: POST_SNOWFLAKE,
            author_id: AUTHOR_SNOWFLAKE,
            ..Default::default()
        }],
        ..Default::default()
    })
    .expect("vm ranker wire request");
    assert_eq!(vm.viewer_id, VIEWER_SNOWFLAKE);
    assert_eq!(vm.candidates[0].tweet_id, POST_SNOWFLAKE);
    assert_eq!(vm.candidates[0].author_id, AUTHOR_SNOWFLAKE);

    let external = resolver
        .reverse_batch(&[
            (SnowflakeId::new(POST_SNOWFLAKE).unwrap(), EntityKind::Post),
            (
                SnowflakeId::new(AUTHOR_SNOWFLAKE).unwrap(),
                EntityKind::User,
            ),
        ])
        .await
        .expect("egress reverse resolves original ObjectIds");
    assert_eq!(external, vec![POST_OID.to_string(), AUTHOR_OID.to_string()]);
}
