use crate::final_feed::{FeedItem, FeedStateStore, FeedStatsSink, ScoredPostsProvider};
use crate::models::query::ScoredPostsQuery;
use crate::query_hydrators::past_request_timestamps_query_hydrator::PastRequestTimestampsQueryHydrator;
use crate::query_hydrators::served_history_query_hydrator::ServedHistoryQueryHydrator;
use crate::selectors::blender_selector::{BlenderConfig, BlenderSelector};
use crate::side_effects::for_you_response_stats_side_effect::ForYouResponseStatsSideEffect;
use crate::sources::ads_source::AdsSource;
use crate::sources::prompts_source::PromptsSource;
use crate::sources::push_to_home_source::PushToHomeSource;
use crate::sources::scored_posts_source::ScoredPostsSource;
use crate::sources::who_to_follow_source::WhoToFollowSource;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::candidate_pipeline::CandidatePipeline;
use xai_candidate_pipeline::filter::Filter;
use xai_candidate_pipeline::hydrator::Hydrator;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;
use xai_candidate_pipeline::scorer::Scorer;
use xai_candidate_pipeline::selector::Selector;
use xai_candidate_pipeline::side_effect::SideEffect;
use xai_candidate_pipeline::source::Source;

pub struct ForYouCandidatePipeline {
    query_hydrators: Vec<Box<dyn QueryHydrator<ScoredPostsQuery>>>,
    sources: Vec<Box<dyn Source<ScoredPostsQuery, FeedItem>>>,
    selector: BlenderSelector,
    side_effects: Arc<Vec<Box<dyn SideEffect<ScoredPostsQuery, FeedItem>>>>,
    result_size: usize,
}

impl ForYouCandidatePipeline {
    pub fn with_sources(
        provider: Arc<dyn ScoredPostsProvider>,
        config: BlenderConfig,
        supplemental_sources: Vec<Box<dyn Source<ScoredPostsQuery, FeedItem>>>,
    ) -> Self {
        Self::with_components(
            provider,
            config,
            supplemental_sources,
            Vec::new(),
            Vec::new(),
        )
    }

    pub fn with_local_state(
        provider: Arc<dyn ScoredPostsProvider>,
        config: BlenderConfig,
        supplemental_sources: Vec<Box<dyn Source<ScoredPostsQuery, FeedItem>>>,
        state_store: Arc<dyn FeedStateStore>,
        stats_sink: Arc<dyn FeedStatsSink>,
    ) -> Self {
        let query_hydrators: Vec<Box<dyn QueryHydrator<ScoredPostsQuery>>> = vec![
            Box::new(ServedHistoryQueryHydrator::from_store(Arc::clone(
                &state_store,
            ))),
            Box::new(PastRequestTimestampsQueryHydrator::from_store(state_store)),
        ];
        let side_effects: Vec<Box<dyn SideEffect<ScoredPostsQuery, FeedItem>>> =
            vec![Box::new(ForYouResponseStatsSideEffect::new(stats_sink))];
        Self::with_components(
            provider,
            config,
            supplemental_sources,
            query_hydrators,
            side_effects,
        )
    }

    fn with_components(
        provider: Arc<dyn ScoredPostsProvider>,
        config: BlenderConfig,
        supplemental_sources: Vec<Box<dyn Source<ScoredPostsQuery, FeedItem>>>,
        query_hydrators: Vec<Box<dyn QueryHydrator<ScoredPostsQuery>>>,
        side_effects: Vec<Box<dyn SideEffect<ScoredPostsQuery, FeedItem>>>,
    ) -> Self {
        let mut sources = standard_sources(provider);
        sources.extend(supplemental_sources);
        Self {
            query_hydrators,
            sources,
            selector: BlenderSelector::new(config),
            side_effects: Arc::new(side_effects),
            result_size: config.max_items,
        }
    }
}

fn standard_sources(
    provider: Arc<dyn ScoredPostsProvider>,
) -> Vec<Box<dyn Source<ScoredPostsQuery, FeedItem>>> {
    vec![
        Box::new(ScoredPostsSource::new(provider)),
        Box::new(AdsSource::disabled()),
        Box::new(WhoToFollowSource),
        Box::new(PromptsSource),
        Box::new(PushToHomeSource),
    ]
}

#[async_trait]
impl CandidatePipeline<ScoredPostsQuery, FeedItem> for ForYouCandidatePipeline {
    fn query_hydrators(&self) -> &[Box<dyn QueryHydrator<ScoredPostsQuery>>] {
        &self.query_hydrators
    }

    fn sources(&self) -> &[Box<dyn Source<ScoredPostsQuery, FeedItem>>] {
        &self.sources
    }

    fn hydrators(&self) -> &[Box<dyn Hydrator<ScoredPostsQuery, FeedItem>>] {
        &[]
    }

    fn filters(&self) -> &[Box<dyn Filter<ScoredPostsQuery, FeedItem>>] {
        &[]
    }

    fn scorers(&self) -> &[Box<dyn Scorer<ScoredPostsQuery, FeedItem>>] {
        &[]
    }

    fn selector(&self) -> &dyn Selector<ScoredPostsQuery, FeedItem> {
        &self.selector
    }

    fn post_selection_hydrators(&self) -> &[Box<dyn Hydrator<ScoredPostsQuery, FeedItem>>] {
        &[]
    }

    fn post_selection_filters(&self) -> &[Box<dyn Filter<ScoredPostsQuery, FeedItem>>] {
        &[]
    }

    fn side_effects(&self) -> Arc<Vec<Box<dyn SideEffect<ScoredPostsQuery, FeedItem>>>> {
        Arc::clone(&self.side_effects)
    }

    fn result_size(&self) -> usize {
        self.result_size
    }
}
