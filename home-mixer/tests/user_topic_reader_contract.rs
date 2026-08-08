use home_mixer::clients::topic_retrieval_client::{TopicPost, TopicRetrievalClient};
use home_mixer::clients::user_topic_reader::UserTopicReader;
use home_mixer::scored_posts_server::ScoredPostsServer;
use home_mixer::{HomeMixerServer, PhoenixCandidatePipeline, TopicPersonalizationClients};
use std::sync::Arc;
use tonic::async_trait;

struct ExternalUserTopicReader;

#[async_trait]
impl UserTopicReader for ExternalUserTopicReader {
    async fn get_supplemental_topic_ids(&self, user_id: i64) -> Result<Vec<i64>, anyhow::Error> {
        Ok(vec![user_id])
    }
}

struct ExternalTopicRetrievalClient;

#[async_trait]
impl TopicRetrievalClient for ExternalTopicRetrievalClient {
    async fn retrieve(
        &self,
        _user_id: i64,
        _topic_ids: &[i64],
        _max_results: usize,
    ) -> Result<Vec<TopicPost>, String> {
        Ok(Vec::new())
    }
}

#[test]
fn public_reader_contract_can_be_implemented_by_an_external_adapter() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("test runtime");

    let topic_ids = runtime
        .block_on(ExternalUserTopicReader.get_supplemental_topic_ids(42))
        .expect("user topics");

    assert_eq!(topic_ids, vec![42]);
}

#[test]
fn external_topic_adapters_can_enter_service_assembly() {
    let clients = TopicPersonalizationClients::new(
        Arc::new(ExternalUserTopicReader),
        Arc::new(ExternalTopicRetrievalClient),
    );

    let pipeline = PhoenixCandidatePipeline::prod_with_topic_clients(clients);
    drop(pipeline);

    let _: fn(PhoenixCandidatePipeline) -> ScoredPostsServer = ScoredPostsServer::with_pipeline;
    let _: fn(Arc<ScoredPostsServer>) -> HomeMixerServer =
        HomeMixerServer::with_scored_posts_server;
}
