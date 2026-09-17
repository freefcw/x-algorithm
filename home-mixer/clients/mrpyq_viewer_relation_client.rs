//! Client for mrpyq `ViewerRelationService`.
//!
//! Block / mute state is the viewer half of feed admission, which
//! `RecommendationDataService` explicitly does not carry. It is served by the
//! same mrpyq deployment, so it reuses `MRPYQ_RECOMMENDATION_DATA_ADDR` and the
//! same request timeout rather than introducing a second endpoint to configure.

use crate::clients::mrpyq_recommendation_data_client::MrpyqRecommendationDataConfig;
use crate::metrics::ClientCallRecorder;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::async_trait;
use tonic::transport::Channel;
use x_algorithm_proto::recommendation_data as pb;
use x_algorithm_proto::recommendation_data::viewer_relation_service_client::ViewerRelationServiceClient;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ViewerRelations {
    pub blocked_account_ids: Vec<String>,
    pub blocked_by_account_ids: Vec<String>,
    pub muted_account_ids: Vec<String>,
    pub muted_keywords: Vec<String>,
}

#[async_trait]
pub trait MrpyqViewerRelationClient: Send + Sync {
    async fn get_viewer_relations(&self, account_id: String) -> anyhow::Result<ViewerRelations>;
}

pub fn viewer_relation_client_from_config(
    config: &MrpyqRecommendationDataConfig,
) -> anyhow::Result<Arc<dyn MrpyqViewerRelationClient>> {
    viewer_relation_client_from_config_with_calls(config, ClientCallRecorder::default())
}

pub fn viewer_relation_client_from_config_with_calls(
    config: &MrpyqRecommendationDataConfig,
    calls: ClientCallRecorder,
) -> anyhow::Result<Arc<dyn MrpyqViewerRelationClient>> {
    match config.address.as_ref() {
        Some(address) => Ok(Arc::new(
            GrpcMrpyqViewerRelationClient::from_addr(address.clone(), config.timeout)?
                .with_calls(calls),
        )),
        None => Ok(Arc::new(DisabledMrpyqViewerRelationClient)),
    }
}

#[derive(Clone, Debug, Default)]
pub struct DisabledMrpyqViewerRelationClient;

#[async_trait]
impl MrpyqViewerRelationClient for DisabledMrpyqViewerRelationClient {
    async fn get_viewer_relations(&self, _account_id: String) -> anyhow::Result<ViewerRelations> {
        anyhow::bail!("mrpyq viewer relation service is not configured")
    }
}

#[derive(Clone)]
pub struct GrpcMrpyqViewerRelationClient {
    channel: Channel,
    timeout: Duration,
    calls: ClientCallRecorder,
}

impl GrpcMrpyqViewerRelationClient {
    pub fn from_addr(address: String, timeout: Duration) -> anyhow::Result<Self> {
        let channel = Channel::from_shared(address)?.connect_lazy();
        Ok(Self {
            channel,
            timeout,
            calls: ClientCallRecorder::default(),
        })
    }

    /// Attach the process call metrics; the default records nothing.
    pub fn with_calls(mut self, calls: ClientCallRecorder) -> Self {
        self.calls = calls;
        self
    }

    async fn get_viewer_relations_rpc(
        &self,
        account_id: String,
    ) -> anyhow::Result<ViewerRelations> {
        let request = pb::GetViewerRelationsReq { account_id };
        let mut client = ViewerRelationServiceClient::new(self.channel.clone());
        let response = tokio::time::timeout(self.timeout, client.get_viewer_relations(request))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "mrpyq viewer relation request timed out after {}ms",
                    self.timeout.as_millis()
                )
            })??
            .into_inner();

        Ok(ViewerRelations {
            blocked_account_ids: response.blocked_account_ids,
            blocked_by_account_ids: response.blocked_by_account_ids,
            muted_account_ids: response.muted_account_ids,
            muted_keywords: response.muted_keywords,
        })
    }
}

#[async_trait]
impl MrpyqViewerRelationClient for GrpcMrpyqViewerRelationClient {
    async fn get_viewer_relations(&self, account_id: String) -> anyhow::Result<ViewerRelations> {
        let started = Instant::now();
        let result = self.get_viewer_relations_rpc(account_id).await;
        self.calls.record(
            "mrpyq_viewer_relation",
            "GetViewerRelations",
            if result.is_ok() { "ok" } else { "error" },
            started,
        );
        match &result {
            Ok(relations) => log::info!(
                "mrpyq rpc GetViewerRelations elapsed_ms={} blocked={} blocked_by={} muted={} keywords={}",
                started.elapsed().as_millis(),
                relations.blocked_account_ids.len(),
                relations.blocked_by_account_ids.len(),
                relations.muted_account_ids.len(),
                relations.muted_keywords.len(),
            ),
            Err(error) => log::warn!(
                "mrpyq rpc GetViewerRelations elapsed_ms={} error={error:#}",
                started.elapsed().as_millis(),
            ),
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_client_does_not_answer_as_if_the_viewer_blocked_nobody() {
        let error = DisabledMrpyqViewerRelationClient
            .get_viewer_relations("1".to_string())
            .await
            .expect_err("an unconfigured relation source has no verdict to give");
        assert!(error.to_string().contains("not configured"), "{error}");
    }

    #[test]
    fn an_unconfigured_address_selects_the_disabled_client() {
        let config = MrpyqRecommendationDataConfig {
            address: None,
            timeout: Duration::from_millis(1),
        };
        assert!(viewer_relation_client_from_config(&config).is_ok());
    }
}
