use crate::candidate_pipeline::for_you_candidate_pipeline::ForYouCandidatePipeline;
use crate::clients::served_persistence::{FeedStateServedPersistence, ServedPersistence};
use crate::feed_state::FeedStateStore;
use crate::feed_stats::{FeedStatsSink, LoggingFeedStats};
use crate::id::IdentityReader;
use crate::models::feed_item::FeedItem;
use crate::models::query::ScoredPostsQuery;
use crate::query_builder::QueryBuilder;
use crate::rpc_policy::RpcPolicy;
use crate::scored_posts_server::ScoredPostsServer;
use crate::selectors::blender_selector::BlenderConfig;
use crate::sources::scored_posts_source::ScoredPostsProvider;
use log::info;
use std::sync::Arc;
use std::time::Instant;
use xai_candidate_pipeline::candidate_pipeline::CandidatePipeline;
use xai_candidate_pipeline::observer::PipelineObserver;
use xai_candidate_pipeline::source::Source;

pub struct ForYouFeedOutput {
    pub items: Vec<FeedItem>,
    pub request_id: String,
    pub persist_error: Option<String>,
    pub(crate) identity_context: Arc<crate::id::IdentityContext>,
}

pub struct ForYouFeedServer {
    query_builder: QueryBuilder,
    rpc_policy: RpcPolicy,
    pipeline: ForYouCandidatePipeline,
    served_persist: Option<Arc<dyn ServedPersistence>>,
}

impl ForYouFeedServer {
    pub fn new(query_builder: QueryBuilder, scored_posts_server: Arc<ScoredPostsServer>) -> Self {
        let provider: Arc<dyn ScoredPostsProvider> = scored_posts_server.clone();
        let mut server = Self::with_local_state_and_query_builder(
            query_builder,
            provider,
            BlenderConfig::default(),
            Vec::new(),
            scored_posts_server.feed_state_store(),
            Arc::new(LoggingFeedStats),
            Some(scored_posts_server.served_persist()),
        );
        // One budget and one metrics registry per process: the For You entry
        // point must not account separately from the inner scorer.
        server.rpc_policy = scored_posts_server.rpc_policy().clone();
        server.pipeline.install_observer(
            Arc::clone(scored_posts_server.rpc_policy().metrics()) as Arc<dyn PipelineObserver>
        );
        // Both pipelines' side effects settle on one tracker, so the inner
        // server's drain covers the For You stats side effect as well.
        server
            .pipeline
            .install_side_effect_tasks(scored_posts_server.background_tasks());
        server
    }

    pub fn with_provider(provider: Arc<dyn ScoredPostsProvider>, config: BlenderConfig) -> Self {
        Self::with_provider_and_sources(provider, config, Vec::new())
    }

    pub fn with_provider_and_sources(
        provider: Arc<dyn ScoredPostsProvider>,
        config: BlenderConfig,
        supplemental_sources: Vec<Box<dyn Source<ScoredPostsQuery, FeedItem>>>,
    ) -> Self {
        Self {
            query_builder: QueryBuilder::default(),
            rpc_policy: RpcPolicy::default(),
            pipeline: ForYouCandidatePipeline::with_sources(provider, config, supplemental_sources),
            served_persist: None,
        }
    }

    pub fn with_local_state(
        provider: Arc<dyn ScoredPostsProvider>,
        config: BlenderConfig,
        supplemental_sources: Vec<Box<dyn Source<ScoredPostsQuery, FeedItem>>>,
        state_store: Arc<dyn FeedStateStore>,
        stats_sink: Arc<dyn FeedStatsSink>,
    ) -> Self {
        Self::with_local_state_and_query_builder(
            QueryBuilder::default(),
            provider,
            config,
            supplemental_sources,
            state_store,
            stats_sink,
            None,
        )
    }

    fn with_local_state_and_query_builder(
        query_builder: QueryBuilder,
        provider: Arc<dyn ScoredPostsProvider>,
        config: BlenderConfig,
        supplemental_sources: Vec<Box<dyn Source<ScoredPostsQuery, FeedItem>>>,
        state_store: Arc<dyn FeedStateStore>,
        stats_sink: Arc<dyn FeedStatsSink>,
        served_persist: Option<Arc<dyn ServedPersistence>>,
    ) -> Self {
        let served_persist = served_persist
            .unwrap_or_else(|| Arc::new(FeedStateServedPersistence::new(Arc::clone(&state_store))));
        Self {
            query_builder,
            rpc_policy: RpcPolicy::default(),
            pipeline: ForYouCandidatePipeline::with_local_state(
                provider,
                config,
                supplemental_sources,
                state_store,
                stats_sink,
            ),
            served_persist: Some(served_persist),
        }
    }

    pub(crate) fn query_builder(&self) -> QueryBuilder {
        self.query_builder.clone()
    }

    pub(crate) fn rpc_policy(&self) -> &RpcPolicy {
        &self.rpc_policy
    }

    pub(crate) async fn shutdown_side_effects(&self, timeout: std::time::Duration) {
        self.pipeline.shutdown_side_effects(timeout).await
    }

    /// Batch reverse the numeric User IDs carried by non-post feed items
    /// (WhoToFollow modules, ad `avoid_handles`) into external ObjectIds.
    /// Post items are already externalized by the scored-posts egress.
    pub(crate) async fn external_user_ids(
        &self,
        items: &[FeedItem],
        identity: Arc<crate::id::IdentityContext>,
    ) -> Result<std::collections::HashMap<crate::models::UserId, String>, String> {
        use crate::id::{EntityKind, SnowflakeId};
        let mut ids = std::collections::HashSet::new();
        for item in items {
            match &item.content {
                crate::models::feed_item::FeedItemContent::WhoToFollow(module) => {
                    ids.extend(module.user_ids.iter().copied().filter(|id| *id != 0));
                }
                crate::models::feed_item::FeedItemContent::Advertisement(ad) => {
                    ids.extend(ad.avoid_handles.iter().copied().filter(|id| *id != 0));
                }
                _ => {}
            }
        }
        let ids = ids.into_iter().collect::<Vec<_>>();
        let pairs = ids
            .iter()
            .map(|id| {
                SnowflakeId::new(*id)
                    .map(|snowflake| (snowflake, EntityKind::User))
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reversed = identity
            .reverse_batch(&pairs)
            .await
            .map_err(|error| error.to_string())?;
        Ok(ids.into_iter().zip(reversed).collect())
    }

    pub async fn get_for_you_feed(&self, query: ScoredPostsQuery) -> ForYouFeedOutput {
        let started = Instant::now();
        let result = self.pipeline.execute(query.start_request()).await;
        let request_id = result.query.request_id.clone();
        let persist_error = if let Some(persist) = &self.served_persist {
            let served_post_ids = result
                .selected_candidates
                .iter()
                .filter_map(FeedItem::served_post_id)
                .collect::<Vec<_>>();
            persist
                .persist(
                    result.query.user_id,
                    &served_post_ids,
                    result.query.request_time_ms,
                    result.query.identity_context(),
                )
                .await
                .err()
        } else {
            None
        };
        info!(
            "For You response - request_id {} - {} items ({} ms)",
            request_id,
            result.selected_candidates.len(),
            started.elapsed().as_millis()
        );
        ForYouFeedOutput {
            items: result.selected_candidates,
            request_id,
            persist_error,
            identity_context: result.query.identity_context(),
        }
    }
}
