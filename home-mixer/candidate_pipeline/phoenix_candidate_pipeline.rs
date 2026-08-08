use crate::candidate_hydrators::core_data_candidate_hydrator::CoreDataCandidateHydrator;
use crate::candidate_hydrators::gizmoduck_hydrator::GizmoduckCandidateHydrator;
use crate::candidate_hydrators::in_network_candidate_hydrator::InNetworkCandidateHydrator;
use crate::candidate_hydrators::subscription_hydrator::SubscriptionHydrator;
use crate::candidate_hydrators::vf_candidate_hydrator::VFCandidateHydrator;
use crate::candidate_hydrators::video_duration_candidate_hydrator::VideoDurationCandidateHydrator;
use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use crate::clients::gizmoduck_client::{GizmoduckClient, ProdGizmoduckClient};
use crate::clients::phoenix_prediction_client::{
    PhoenixPredictionClient, ProdPhoenixPredictionClient,
};
use crate::clients::phoenix_retrieval_client::{
    PhoenixRetrievalClient, ProdPhoenixRetrievalClient,
};
use crate::clients::s2s::{S2S_CHAIN_PATH, S2S_CRT_PATH, S2S_KEY_PATH};
use crate::clients::socialgraph_client::SocialGraphClient;
use crate::clients::strato_client::{DemoStratoClient, ProdStratoClient, StratoClient};
use crate::clients::thunder_client::ThunderClient;
use crate::clients::topic_retrieval_client::{DemoTopicRetrievalClient, TopicRetrievalClient};
use crate::clients::tweet_entity_service_client::{DemoTESClient, ProdTESClient, TESClient};
use crate::clients::uas_fetcher::{
    DemoUserActionSequenceFetcher, UserActionSequenceFetcher, UserActionSequenceOps,
};
use crate::clients::user_topic_reader::{DemoUserTopicReader, UserTopicReader};
use crate::filters::age_filter::AgeFilter;
use crate::filters::ancillary_vf_filter::AncillaryVFFilter;
use crate::filters::author_socialgraph_filter::AuthorSocialgraphFilter;
use crate::filters::core_data_hydration_filter::CoreDataHydrationFilter;
use crate::filters::dedup_conversation_filter::DedupConversationFilter;
use crate::filters::drop_duplicates_filter::DropDuplicatesFilter;
use crate::filters::ineligible_subscription_filter::IneligibleSubscriptionFilter;
use crate::filters::muted_keyword_filter::MutedKeywordFilter;
use crate::filters::previously_seen_posts_backup_filter::PreviouslySeenPostsBackupFilter;
use crate::filters::previously_seen_posts_filter::PreviouslySeenPostsFilter;
use crate::filters::previously_served_posts_filter::PreviouslyServedPostsFilter;
use crate::filters::retweet_deduplication_filter::RetweetDeduplicationFilter;
use crate::filters::self_tweet_filter::SelfTweetFilter;
use crate::filters::topic_ids_filter::TopicIdsFilter;
use crate::filters::vf_filter::VFFilter;
use crate::filters::video_filter::VideoFilter;
use crate::params;
use crate::query_hydrators::user_action_seq_query_hydrator::UserActionSeqQueryHydrator;
use crate::query_hydrators::user_features_query_hydrator::UserFeaturesQueryHydrator;
use crate::query_hydrators::user_topics_query_hydrator::UserTopicsQueryHydrator;
use crate::scorers::author_diversity_scorer::AuthorDiversityScorer;
use crate::scorers::oon_scorer::OONScorer;
use crate::scorers::phoenix_scorer::PhoenixScorer;
use crate::scorers::weighted_scorer::WeightedScorer;
use crate::selectors::TopKScoreSelector;
use crate::side_effects::cache_request_info_side_effect::CacheRequestInfoSideEffect;
use crate::sources::cached_posts_source::CachedPostsSource;
use crate::sources::phoenix_moe_source::PhoenixMoeSource;
use crate::sources::phoenix_source::PhoenixSource;
use crate::sources::phoenix_topics_source::PhoenixTopicsSource;
use crate::sources::thunder_source::ThunderSource;
use crate::visibility::vf_client::{ProdVisibilityFilteringClient, VisibilityFilteringClient};
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use xai_candidate_pipeline::candidate_pipeline::CandidatePipeline;
use xai_candidate_pipeline::filter::Filter;
use xai_candidate_pipeline::hydrator::Hydrator;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;
use xai_candidate_pipeline::scorer::Scorer;
use xai_candidate_pipeline::selector::Selector;
use xai_candidate_pipeline::side_effect::SideEffect;
use xai_candidate_pipeline::source::Source;

pub struct PhoenixCandidatePipeline {
    query_hydrators: Vec<Box<dyn QueryHydrator<ScoredPostsQuery>>>,
    sources: Vec<Box<dyn Source<ScoredPostsQuery, PostCandidate>>>,
    hydrators: Vec<Box<dyn Hydrator<ScoredPostsQuery, PostCandidate>>>,
    filters: Vec<Box<dyn Filter<ScoredPostsQuery, PostCandidate>>>,
    scorers: Vec<Box<dyn Scorer<ScoredPostsQuery, PostCandidate>>>,
    selector: TopKScoreSelector,
    post_selection_hydrators: Vec<Box<dyn Hydrator<ScoredPostsQuery, PostCandidate>>>,
    post_selection_filters: Vec<Box<dyn Filter<ScoredPostsQuery, PostCandidate>>>,
    side_effects: Arc<Vec<Box<dyn SideEffect<ScoredPostsQuery, PostCandidate>>>>,
}

/// 显式启用补充话题能力所需的原子依赖。
/// Reader 负责资格和话题选择，Home Mixer 只执行召回编排。
pub struct TopicPersonalizationClients {
    reader: Arc<dyn UserTopicReader>,
    retriever: Arc<dyn TopicRetrievalClient>,
}

impl TopicPersonalizationClients {
    pub fn new(reader: Arc<dyn UserTopicReader>, retriever: Arc<dyn TopicRetrievalClient>) -> Self {
        Self { reader, retriever }
    }
}

impl PhoenixCandidatePipeline {
    async fn build_with_clients(
        uas_fetcher: Arc<dyn UserActionSequenceOps>,
        phoenix_client: Arc<dyn PhoenixPredictionClient + Send + Sync>,
        phoenix_retrieval_client: Arc<dyn PhoenixRetrievalClient + Send + Sync>,
        thunder_client: Arc<ThunderClient>,
        strato_client: Arc<dyn StratoClient + Send + Sync>,
        tes_client: Arc<dyn TESClient + Send + Sync>,
        gizmoduck_client: Arc<dyn GizmoduckClient + Send + Sync>,
        vf_client: Arc<dyn VisibilityFilteringClient + Send + Sync>,
        topic_clients: Option<TopicPersonalizationClients>,
        moe_retrieval_client: Option<Arc<dyn PhoenixRetrievalClient + Send + Sync>>,
    ) -> PhoenixCandidatePipeline {
        // Query Hydrators
        let mut query_hydrators: Vec<Box<dyn QueryHydrator<ScoredPostsQuery>>> = vec![
            Box::new(UserActionSeqQueryHydrator::new(uas_fetcher)),
            Box::new(UserFeaturesQueryHydrator {
                strato_client: strato_client.clone(),
            }),
        ];
        let topic_retrieval_client = topic_clients.map(|clients| {
            query_hydrators.push(Box::new(UserTopicsQueryHydrator {
                reader: clients.reader,
            }));
            clients.retriever
        });

        // Sources
        let phoenix_source = Box::new(PhoenixSource {
            phoenix_retrieval_client,
        });
        let thunder_source = Box::new(ThunderSource { thunder_client });
        let mut sources: Vec<Box<dyn Source<ScoredPostsQuery, PostCandidate>>> =
            vec![Box::new(CachedPostsSource)];
        if let Some(client) = topic_retrieval_client {
            sources.push(Box::new(PhoenixTopicsSource { client }));
        }
        sources.push(phoenix_source);
        if let Some(phoenix_retrieval_client) = moe_retrieval_client {
            sources.push(Box::new(PhoenixMoeSource {
                phoenix_retrieval_client,
            }));
        }
        sources.push(thunder_source);

        // Hydrators
        let hydrators: Vec<Box<dyn Hydrator<ScoredPostsQuery, PostCandidate>>> = vec![
            Box::new(InNetworkCandidateHydrator),
            Box::new(CoreDataCandidateHydrator::new(tes_client.clone()).await),
            Box::new(VideoDurationCandidateHydrator::new(tes_client.clone()).await),
            Box::new(SubscriptionHydrator::new(tes_client.clone()).await),
            Box::new(GizmoduckCandidateHydrator::new(gizmoduck_client).await),
        ];

        // Filters
        let filters: Vec<Box<dyn Filter<ScoredPostsQuery, PostCandidate>>> = vec![
            Box::new(DropDuplicatesFilter),
            Box::new(CoreDataHydrationFilter),
            Box::new(AgeFilter::new(Duration::from_secs(params::MAX_POST_AGE))),
            Box::new(SelfTweetFilter),
            Box::new(RetweetDeduplicationFilter),
            Box::new(IneligibleSubscriptionFilter),
            Box::new(PreviouslySeenPostsFilter),
            Box::new(PreviouslySeenPostsBackupFilter),
            Box::new(PreviouslyServedPostsFilter),
            Box::new(MutedKeywordFilter::new()),
            Box::new(AuthorSocialgraphFilter),
            Box::new(TopicIdsFilter),
            Box::new(VideoFilter),
        ];

        // Scorers
        let phoenix_scorer = Box::new(PhoenixScorer { phoenix_client });
        let weighted_scorer = Box::new(WeightedScorer);
        let author_diversity_scorer = Box::new(AuthorDiversityScorer::default());
        let oon_scorer = Box::new(OONScorer);
        let scorers: Vec<Box<dyn Scorer<ScoredPostsQuery, PostCandidate>>> = vec![
            phoenix_scorer,
            weighted_scorer,
            author_diversity_scorer,
            oon_scorer,
        ];

        // Selector
        let selector = TopKScoreSelector;

        // Post-selection hydrators
        let post_selection_hydrators: Vec<Box<dyn Hydrator<ScoredPostsQuery, PostCandidate>>> =
            vec![Box::new(VFCandidateHydrator::new(vf_client.clone()).await)];

        // Post-selection filters
        let post_selection_filters: Vec<Box<dyn Filter<ScoredPostsQuery, PostCandidate>>> = vec![
            Box::new(VFFilter),
            Box::new(AncillaryVFFilter),
            Box::new(DedupConversationFilter),
        ];

        // Side Effects
        let side_effects: Arc<Vec<Box<dyn SideEffect<ScoredPostsQuery, PostCandidate>>>> =
            Arc::new(vec![Box::new(CacheRequestInfoSideEffect { strato_client })]);

        PhoenixCandidatePipeline {
            query_hydrators,
            hydrators,
            filters,
            sources,
            scorers,
            selector,
            post_selection_hydrators,
            post_selection_filters,
            side_effects,
        }
    }

    /// 生产装配入口。
    ///
    /// 演示模式（HOME_MIXER_DEMO=1）在这里统一决策：把 UAS / Strato / TES
    /// 三个数据依赖换成 Demo 实现，其余装配完全一致。
    /// 生产实现内部不包含任何演示分支，替换 stub 时无需关心演示逻辑。
    pub async fn prod() -> PhoenixCandidatePipeline {
        let topic_clients = crate::demo::is_demo_mode().then(|| {
            TopicPersonalizationClients::new(
                Arc::new(DemoUserTopicReader),
                Arc::new(DemoTopicRetrievalClient),
            )
        });
        Self::prod_with_optional_topic_clients(topic_clients).await
    }

    /// 构建显式启用补充话题能力的生产 Pipeline。
    pub async fn prod_with_topic_clients(
        topic_clients: TopicPersonalizationClients,
    ) -> PhoenixCandidatePipeline {
        Self::prod_with_optional_topic_clients(Some(topic_clients)).await
    }

    async fn prod_with_optional_topic_clients(
        topic_clients: Option<TopicPersonalizationClients>,
    ) -> PhoenixCandidatePipeline {
        let demo_mode = crate::demo::is_demo_mode();
        if demo_mode {
            log::info!("HOME_MIXER_DEMO=1: injecting demo UAS / Strato / TES clients");
        }

        let uas_fetcher: Arc<dyn UserActionSequenceOps> = if demo_mode {
            Arc::new(DemoUserActionSequenceFetcher)
        } else {
            Arc::new(UserActionSequenceFetcher::new().expect("Failed to create UAS fetcher"))
        };
        let strato_client: Arc<dyn StratoClient + Send + Sync> = if demo_mode {
            Arc::new(DemoStratoClient)
        } else {
            Arc::new(
                ProdStratoClient::new()
                    .await
                    .expect("Failed to create Strato client"),
            )
        };
        let tes_client: Arc<dyn TESClient + Send + Sync> = if demo_mode {
            Arc::new(DemoTESClient)
        } else {
            Arc::new(
                ProdTESClient::new()
                    .await
                    .expect("Failed to create TES client"),
            )
        };

        let _sgs_client = Arc::new(SocialGraphClient::new());
        let phoenix_client = Arc::new(
            ProdPhoenixPredictionClient::new()
                .await
                .expect("Failed to create Phoenix prediction client"),
        );
        let phoenix_retrieval_client = Arc::new(
            ProdPhoenixRetrievalClient::new()
                .await
                .expect("Failed to create Phoenix retrieval client"),
        );
        let thunder_client = Arc::new(ThunderClient::new().await);
        let gizmoduck_client = Arc::new(
            ProdGizmoduckClient::new()
                .await
                .expect("Failed to create Gizmoduck client"),
        );
        let vf_client = Arc::new(
            ProdVisibilityFilteringClient::new(
                S2S_CHAIN_PATH.clone(),
                S2S_CRT_PATH.clone(),
                S2S_KEY_PATH.clone(),
            )
            .await
            .expect("Failed to create VF client"),
        );
        let moe_retrieval_client = std::env::var("PHOENIX_MOE_GRPC_ADDR").ok().map(|addr| {
            Arc::new(
                ProdPhoenixRetrievalClient::from_addr(addr)
                    .expect("Failed to create Phoenix MoE retrieval client"),
            ) as Arc<dyn PhoenixRetrievalClient + Send + Sync>
        });

        PhoenixCandidatePipeline::build_with_clients(
            uas_fetcher,
            phoenix_client,
            phoenix_retrieval_client,
            thunder_client,
            strato_client,
            tes_client,
            gizmoduck_client,
            vf_client,
            topic_clients,
            moe_retrieval_client,
        )
        .await
    }
}

#[async_trait]
impl CandidatePipeline<ScoredPostsQuery, PostCandidate> for PhoenixCandidatePipeline {
    fn query_hydrators(&self) -> &[Box<dyn QueryHydrator<ScoredPostsQuery>>] {
        &self.query_hydrators
    }

    fn sources(&self) -> &[Box<dyn Source<ScoredPostsQuery, PostCandidate>>] {
        &self.sources
    }
    fn hydrators(&self) -> &[Box<dyn Hydrator<ScoredPostsQuery, PostCandidate>>] {
        &self.hydrators
    }

    fn filters(&self) -> &[Box<dyn Filter<ScoredPostsQuery, PostCandidate>>] {
        &self.filters
    }

    fn scorers(&self) -> &[Box<dyn Scorer<ScoredPostsQuery, PostCandidate>>] {
        &self.scorers
    }

    fn selector(&self) -> &dyn Selector<ScoredPostsQuery, PostCandidate> {
        &self.selector
    }

    fn post_selection_hydrators(&self) -> &[Box<dyn Hydrator<ScoredPostsQuery, PostCandidate>>] {
        &self.post_selection_hydrators
    }

    fn post_selection_filters(&self) -> &[Box<dyn Filter<ScoredPostsQuery, PostCandidate>>] {
        &self.post_selection_filters
    }

    fn side_effects(&self) -> Arc<Vec<Box<dyn SideEffect<ScoredPostsQuery, PostCandidate>>>> {
        Arc::clone(&self.side_effects)
    }

    fn result_size(&self) -> usize {
        params::RESULT_SIZE
    }
}
