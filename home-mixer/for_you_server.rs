use crate::candidate_pipeline::for_you_candidate_pipeline::ForYouCandidatePipeline;
use crate::feed_state::{FeedStateStore, InMemoryFeedStateStore};
use crate::feed_stats::{FeedStatsSink, LoggingFeedStats};
use crate::models::feed_item::FeedItem;
use crate::models::query::ScoredPostsQuery;
use crate::query_builder::QueryBuilder;
use crate::scored_posts_server::ScoredPostsServer;
use crate::selectors::blender_selector::BlenderConfig;
use crate::sources::scored_posts_source::ScoredPostsProvider;
use crate::util::request_util::current_time_ms;
use log::{error, info};
use std::sync::Arc;
use std::time::Instant;
use xai_candidate_pipeline::candidate_pipeline::CandidatePipeline;
use xai_candidate_pipeline::source::Source;

pub struct ForYouFeedOutput {
    pub items: Vec<FeedItem>,
    pub request_id: String,
}

pub struct ForYouFeedServer {
    query_builder: QueryBuilder,
    pipeline: ForYouCandidatePipeline,
    // Local U2 consistency boundary. Production history clients must define
    // equivalent atomicity before this moves to asynchronous SideEffects.
    local_state_store: Option<Arc<dyn FeedStateStore>>,
}

impl ForYouFeedServer {
    pub fn new(query_builder: QueryBuilder, scored_posts_server: Arc<ScoredPostsServer>) -> Self {
        let state_store: Arc<dyn FeedStateStore> = Arc::new(InMemoryFeedStateStore::new(
            crate::params::LOCAL_SERVED_HISTORY_LIMIT,
            crate::params::LOCAL_REQUEST_TIMESTAMP_LIMIT,
        ));
        let stats_sink: Arc<dyn FeedStatsSink> = Arc::new(LoggingFeedStats);
        Self::with_local_state_and_query_builder(
            query_builder,
            scored_posts_server,
            BlenderConfig::default(),
            Vec::new(),
            state_store,
            stats_sink,
        )
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
            pipeline: ForYouCandidatePipeline::with_sources(provider, config, supplemental_sources),
            local_state_store: None,
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
        )
    }

    fn with_local_state_and_query_builder(
        query_builder: QueryBuilder,
        provider: Arc<dyn ScoredPostsProvider>,
        config: BlenderConfig,
        supplemental_sources: Vec<Box<dyn Source<ScoredPostsQuery, FeedItem>>>,
        state_store: Arc<dyn FeedStateStore>,
        stats_sink: Arc<dyn FeedStatsSink>,
    ) -> Self {
        Self {
            query_builder,
            pipeline: ForYouCandidatePipeline::with_local_state(
                provider,
                config,
                supplemental_sources,
                Arc::clone(&state_store),
                stats_sink,
            ),
            local_state_store: Some(state_store),
        }
    }

    pub(crate) fn query_builder(&self) -> QueryBuilder {
        self.query_builder.clone()
    }

    pub async fn get_for_you_feed(&self, query: ScoredPostsQuery) -> ForYouFeedOutput {
        let started = Instant::now();
        let result = self.pipeline.execute(query).await;
        let request_id = result.query.request_id.clone();
        if let Some(state_store) = &self.local_state_store {
            let served_post_ids = result
                .selected_candidates
                .iter()
                .filter_map(FeedItem::served_post_id)
                .collect();
            if let Err(state_error) =
                state_store.record(result.query.user_id, served_post_ids, current_time_ms())
            {
                error!(
                    "For You local state - request_id {} - failed: {}",
                    request_id, state_error
                );
            }
        }
        info!(
            "For You response - request_id {} - {} items ({} ms)",
            request_id,
            result.selected_candidates.len(),
            started.elapsed().as_millis()
        );
        ForYouFeedOutput {
            items: result.selected_candidates,
            request_id,
        }
    }
}
