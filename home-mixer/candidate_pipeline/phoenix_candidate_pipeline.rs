use crate::candidate_hydrators::core_data_candidate_hydrator::CoreDataCandidateHydrator;
use crate::candidate_hydrators::filtered_topics_hydrator::FilteredTopicsHydrator;
use crate::candidate_hydrators::gizmoduck_hydrator::GizmoduckCandidateHydrator;
use crate::candidate_hydrators::has_media_hydrator::HasMediaHydrator;
use crate::candidate_hydrators::in_network_candidate_hydrator::InNetworkCandidateHydrator;
use crate::candidate_hydrators::language_code_hydrator::LanguageCodeHydrator;
use crate::candidate_hydrators::quote_hydrator::QuoteHydrator;
use crate::candidate_hydrators::subscription_hydrator::SubscriptionHydrator;
use crate::candidate_hydrators::tes_hydration_provider::TesHydrationProvider;
use crate::candidate_hydrators::vf_candidate_hydrator::VFCandidateHydrator;
use crate::candidate_hydrators::video_duration_candidate_hydrator::VideoDurationCandidateHydrator;
use crate::clients::gizmoduck_client::{
    DemoGizmoduckClient, DisabledGizmoduckClient, GizmoduckClient,
};
use crate::clients::phoenix_prediction_client::{
    PhoenixPredictionClient, ProdPhoenixPredictionClient,
};
use crate::clients::phoenix_retrieval_client::{
    PhoenixRetrievalClient, ProdPhoenixRetrievalClient,
};
use crate::clients::s2s::{S2S_CHAIN_PATH, S2S_CRT_PATH, S2S_KEY_PATH};
use crate::clients::strato_client::{DemoStratoClient, DisabledStratoClient, StratoClient};
use crate::clients::thunder_client::ThunderClient;
use crate::clients::topic_retrieval_client::{DemoTopicRetrievalClient, TopicRetrievalClient};
use crate::clients::tweet_entity_service_client::{DemoTESClient, DisabledTESClient, TESClient};
use crate::clients::uas_fetcher::{
    DemoUserActionSequenceFetcher, DisabledUserActionSequenceFetcher, UserActionSequenceOps,
};
use crate::clients::user_topic_reader::{DemoUserTopicReader, UserTopicReader};
use crate::clients::vm_ranker_client::GrpcVMRankerClient;
use crate::feature_policy::HomeMixerFeatures;
use crate::filters::age_filter::AgeFilter;
use crate::filters::ancillary_vf_filter::AncillaryVFFilter;
use crate::filters::author_socialgraph_filter::AuthorSocialgraphFilter;
use crate::filters::core_data_hydration_filter::CoreDataHydrationFilter;
use crate::filters::dedup_conversation_filter::DedupConversationFilter;
use crate::filters::drop_duplicates_filter::DropDuplicatesFilter;
use crate::filters::ineligible_subscription_filter::IneligibleSubscriptionFilter;
use crate::filters::muted_keyword_filter::MutedKeywordFilter;
use crate::filters::new_user_topic_ids_filter::NewUserTopicIdsFilter;
use crate::filters::previously_seen_posts_backup_filter::PreviouslySeenPostsBackupFilter;
use crate::filters::previously_seen_posts_filter::PreviouslySeenPostsFilter;
use crate::filters::previously_served_posts_filter::PreviouslyServedPostsFilter;
use crate::filters::retweet_deduplication_filter::RetweetDeduplicationFilter;
use crate::filters::self_tweet_filter::SelfTweetFilter;
use crate::filters::topic_ids_filter::TopicIdsFilter;
use crate::filters::vf_filter::VFFilter;
use crate::filters::video_filter::VideoFilter;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::params;
use crate::query_hydrators::blocked_user_ids_query_hydrator::BlockedUserIdsQueryHydrator;
use crate::query_hydrators::followed_user_ids_query_hydrator::FollowedUserIdsQueryHydrator;
use crate::query_hydrators::muted_user_ids_query_hydrator::MutedUserIdsQueryHydrator;
use crate::query_hydrators::retrieval_sequence_query_hydrator::RetrievalSequenceQueryHydrator;
use crate::query_hydrators::scoring_sequence_query_hydrator::ScoringSequenceQueryHydrator;
use crate::query_hydrators::subscribed_user_ids_query_hydrator::SubscribedUserIdsQueryHydrator;
use crate::query_hydrators::user_action_seq_query_hydrator::UserActionSeqQueryHydrator;
use crate::query_hydrators::user_features_query_hydrator::UserFeaturesQueryHydrator;
use crate::query_hydrators::user_safety_features_query_hydrator::UserSafetyFeaturesQueryHydrator;
use crate::query_hydrators::user_topics_query_hydrator::UserTopicsQueryHydrator;
use crate::runtime_config::HomeMixerMode;
use crate::scorers::author_cold_start::{AuthorColdStart, ColdStartConfig};
use crate::scorers::phoenix_scorer::PhoenixScorer;
use crate::scorers::ranking_scorer::RankingScorer;
use crate::scorers::vm_ranker::VMRanker;
use crate::selectors::TopKScoreSelector;
use crate::side_effects::phoenix_request_cache_side_effect::PhoenixRequestCacheSideEffect;
use crate::sources::cached_posts_source::CachedPostsSource;
use crate::sources::phoenix_moe_source::PhoenixMoeSource;
use crate::sources::phoenix_source::PhoenixSource;
use crate::sources::phoenix_topics_source::PhoenixTopicsSource;
use crate::sources::thunder_source::ThunderSource;
use crate::visibility::vf_client::{
    DemoVisibilityFilteringClient, DisabledVisibilityFilteringClient, VisibilityFilteringClient,
};
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

struct PhoenixDependencies {
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
    features: HomeMixerFeatures,
}

impl PhoenixCandidatePipeline {
    async fn build_with_clients(dependencies: PhoenixDependencies) -> PhoenixCandidatePipeline {
        let PhoenixDependencies {
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
            features,
        } = dependencies;
        // Query Hydrators
        let sequence_provider = Arc::new(UserActionSeqQueryHydrator::new(uas_fetcher));
        let feature_provider = Arc::new(UserFeaturesQueryHydrator::new(strato_client.clone()));
        let mut query_hydrators: Vec<Box<dyn QueryHydrator<ScoredPostsQuery>>> = vec![
            Box::new(ScoringSequenceQueryHydrator::new(Arc::clone(
                &sequence_provider,
            ))),
            Box::new(RetrievalSequenceQueryHydrator::new(sequence_provider)),
            Box::new(BlockedUserIdsQueryHydrator::new(Arc::clone(
                &feature_provider,
            ))),
            Box::new(MutedUserIdsQueryHydrator::new(Arc::clone(
                &feature_provider,
            ))),
            Box::new(FollowedUserIdsQueryHydrator::new(Arc::clone(
                &feature_provider,
            ))),
            Box::new(SubscribedUserIdsQueryHydrator::new(Arc::clone(
                &feature_provider,
            ))),
            Box::new(UserSafetyFeaturesQueryHydrator::new(feature_provider)),
        ];
        let topic_retrieval_client = topic_clients.map(|clients| {
            query_hydrators.push(Box::new(UserTopicsQueryHydrator {
                reader: clients.reader,
            }));
            clients.retriever
        });

        // Sources follow the upstream order. TweetMixer remains U3 and is omitted.
        let mut sources: Vec<Box<dyn Source<ScoredPostsQuery, PostCandidate>>> = vec![
            Box::new(ThunderSource { thunder_client }),
            Box::new(PhoenixSource {
                phoenix_retrieval_client,
            }),
        ];
        if let Some(client) = topic_retrieval_client {
            sources.push(Box::new(PhoenixTopicsSource { client }));
        }
        if let Some(phoenix_retrieval_client) = moe_retrieval_client {
            sources.push(Box::new(PhoenixMoeSource {
                phoenix_retrieval_client,
            }));
        }
        sources.push(Box::new(CachedPostsSource));

        // Hydrators follow upstream ownership while sharing the public TES batches.
        // BlockedBy remains U3 because no production social-graph contract exists.
        let tes_provider = Arc::new(TesHydrationProvider::new(tes_client.clone()));
        let gizmoduck_hydrator =
            GizmoduckCandidateHydrator::new(Arc::clone(&gizmoduck_client)).await;
        let mut hydrators: Vec<Box<dyn Hydrator<ScoredPostsQuery, PostCandidate>>> = vec![
            Box::new(InNetworkCandidateHydrator),
            Box::new(CoreDataCandidateHydrator::new(Arc::clone(&tes_provider))),
            Box::new(QuoteHydrator::new(Arc::clone(&tes_provider))),
            Box::new(VideoDurationCandidateHydrator::new(Arc::clone(
                &tes_provider,
            ))),
            Box::new(HasMediaHydrator::new(Arc::clone(&tes_provider))),
            Box::new(SubscriptionHydrator::new(tes_client).await),
            Box::new(FilteredTopicsHydrator::new(Arc::clone(&tes_provider))),
            Box::new(LanguageCodeHydrator::new(tes_provider)),
        ];
        if features.author_cold_start {
            hydrators.push(Box::new(
                GizmoduckCandidateHydrator::new(gizmoduck_client).await,
            ));
        }

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
            Box::new(VideoFilter),
            Box::new(TopicIdsFilter),
            Box::new(NewUserTopicIdsFilter),
        ];

        // RankingScorer preserves the local weighted/diversity/OON behavior behind
        // the upstream component boundary.
        let author_cold_start = AuthorColdStart::new(ColdStartConfig {
            enabled: features.author_cold_start,
            thompson_sampling: features.author_cold_start && features.cold_start_thompson_sampling,
            ..Default::default()
        });
        let mut scorers: Vec<Box<dyn Scorer<ScoredPostsQuery, PostCandidate>>> = vec![
            Box::new(PhoenixScorer { phoenix_client }),
            Box::new(RankingScorer::new(author_cold_start.clone())),
        ];

        // 可选旁路：VM Ranker 二次重排（上游 scorers 第三位）。开关 + 地址
        // 齐备时装配本仓库 vm-ranker 服务的 gRPC Adapter；缺地址时禁用旁路
        // 并保留主链（与 MoE 相同的降级规则）。
        if features.vm_ranker {
            match std::env::var("VM_RANKER_GRPC_ADDR") {
                Ok(addr) if !addr.trim().is_empty() => {
                    log::info!("VMRanker scorer enabled via {addr}");
                    scorers.push(Box::new(VMRanker {
                        client: Arc::new(GrpcVMRankerClient::new(addr)),
                        value_model_id: std::env::var("VM_RANKER_VALUE_MODEL_ID").ok(),
                        author_cold_start: author_cold_start.clone(),
                    }));
                }
                _ => {
                    log::warn!(
                        "HOME_MIXER_ENABLE_VM_RANKER is set but VM_RANKER_GRPC_ADDR is missing; disabling VM Ranker scorer"
                    );
                }
            }
        }

        // Selector
        let selector = TopKScoreSelector;

        // Post-selection hydrators
        let post_selection_hydrators: Vec<Box<dyn Hydrator<ScoredPostsQuery, PostCandidate>>> = vec![
            Box::new(gizmoduck_hydrator),
            Box::new(VFCandidateHydrator::new(vf_client.clone()).await),
        ];

        // Post-selection filters
        let post_selection_filters: Vec<Box<dyn Filter<ScoredPostsQuery, PostCandidate>>> = vec![
            Box::new(VFFilter),
            Box::new(AncillaryVFFilter),
            Box::new(DedupConversationFilter),
        ];

        // Side Effects
        let side_effects: Arc<Vec<Box<dyn SideEffect<ScoredPostsQuery, PostCandidate>>>> =
            Arc::new(vec![Box::new(PhoenixRequestCacheSideEffect::new(
                strato_client,
                features.request_cache_side_effect,
            ))]);

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

    /// Upstream-compatible assembly facade.
    ///
    /// New application code should pass explicit runtime intent through
    /// `assemble_for_mode`; this wrapper only preserves existing callers.
    pub async fn prod() -> PhoenixCandidatePipeline {
        Self::prod_with_features(HomeMixerFeatures::from_env()).await
    }

    pub async fn prod_with_features(features: HomeMixerFeatures) -> PhoenixCandidatePipeline {
        Self::assemble_for_mode(Self::compatibility_mode(), features).await
    }

    pub async fn assemble_for_mode(
        mode: HomeMixerMode,
        features: HomeMixerFeatures,
    ) -> PhoenixCandidatePipeline {
        let topic_clients = (mode == HomeMixerMode::Demo).then(|| {
            TopicPersonalizationClients::new(
                Arc::new(DemoUserTopicReader),
                Arc::new(DemoTopicRetrievalClient),
            )
        });
        Self::assemble_with_optional_topic_clients(mode, topic_clients, features).await
    }

    /// Builds the pipeline with explicitly supplied topic adapters.
    ///
    /// Supplying these adapters is the manual enablement action: production
    /// callers must verify the reader and retrieval service contracts first.
    pub async fn prod_with_topic_clients(
        topic_clients: TopicPersonalizationClients,
    ) -> PhoenixCandidatePipeline {
        Self::assemble_with_optional_topic_clients(
            Self::compatibility_mode(),
            Some(topic_clients),
            HomeMixerFeatures::from_env(),
        )
        .await
    }

    fn compatibility_mode() -> HomeMixerMode {
        if crate::demo::is_demo_mode() {
            HomeMixerMode::Demo
        } else {
            HomeMixerMode::Degraded
        }
    }

    async fn assemble_with_optional_topic_clients(
        mode: HomeMixerMode,
        topic_clients: Option<TopicPersonalizationClients>,
        features: HomeMixerFeatures,
    ) -> PhoenixCandidatePipeline {
        let demo_mode = mode == HomeMixerMode::Demo;
        if demo_mode {
            log::info!("HOME_MIXER_MODE=demo: injecting demo UAS / Strato / TES clients");
        }

        let uas_fetcher: Arc<dyn UserActionSequenceOps> = if demo_mode {
            Arc::new(DemoUserActionSequenceFetcher)
        } else {
            Arc::new(
                DisabledUserActionSequenceFetcher::new()
                    .expect("Failed to create disabled UAS boundary"),
            )
        };
        let strato_client: Arc<dyn StratoClient + Send + Sync> = if demo_mode {
            Arc::new(DemoStratoClient)
        } else {
            Arc::new(
                DisabledStratoClient::new()
                    .await
                    .expect("Failed to create disabled Strato boundary"),
            )
        };
        let tes_client: Arc<dyn TESClient + Send + Sync> = if demo_mode {
            Arc::new(DemoTESClient)
        } else {
            Arc::new(
                DisabledTESClient::new()
                    .await
                    .expect("Failed to create disabled TES boundary"),
            )
        };

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
        let gizmoduck_client: Arc<dyn GizmoduckClient + Send + Sync> =
            if demo_mode && features.author_cold_start {
                Arc::new(DemoGizmoduckClient)
            } else {
                Arc::new(
                    DisabledGizmoduckClient::new()
                        .await
                        .expect("Failed to create disabled Gizmoduck boundary"),
                )
            };
        let vf_client: Arc<dyn VisibilityFilteringClient + Send + Sync> = if demo_mode {
            Arc::new(DemoVisibilityFilteringClient)
        } else {
            Arc::new(
                DisabledVisibilityFilteringClient::new(
                    S2S_CHAIN_PATH.clone(),
                    S2S_CRT_PATH.clone(),
                    S2S_KEY_PATH.clone(),
                )
                .await
                .expect("Failed to create disabled VF boundary"),
            )
        };
        let moe_retrieval_client = if features.phoenix_moe {
            match std::env::var("PHOENIX_MOE_GRPC_ADDR") {
                Ok(addr) => Some(Arc::new(
                    ProdPhoenixRetrievalClient::from_addr(addr)
                        .expect("Failed to create Phoenix MoE retrieval client"),
                )
                    as Arc<dyn PhoenixRetrievalClient + Send + Sync>),
                Err(_) => {
                    log::warn!(
                        "HOME_MIXER_ENABLE_PHOENIX_MOE is set but PHOENIX_MOE_GRPC_ADDR is missing; disabling Phoenix MoE source"
                    );
                    None
                }
            }
        } else {
            None
        };
        if features.request_cache_side_effect {
            log::warn!(
                "request cache side effect was operator-enabled; a production StratoClient contract must be supplied before relying on persisted data"
            );
        }

        PhoenixCandidatePipeline::build_with_clients(PhoenixDependencies {
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
            features,
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use xai_candidate_pipeline::candidate_pipeline::PipelineStage;

    #[tokio::test]
    async fn cold_start_adds_pre_selection_author_hydration_explicitly() {
        let pipeline = PhoenixCandidatePipeline::assemble_for_mode(
            HomeMixerMode::Demo,
            HomeMixerFeatures {
                author_cold_start: true,
                ..Default::default()
            },
        )
        .await;
        let components = pipeline.components();
        let pre_selection = components
            .iter()
            .find(|entry| entry.stage == PipelineStage::Hydrator)
            .expect("pre-selection hydrators");

        assert!(pre_selection
            .components
            .iter()
            .any(|name| name == "GizmoduckCandidateHydrator"));
    }

    #[tokio::test]
    async fn profile_hydration_runs_only_after_selection() {
        let pipeline = PhoenixCandidatePipeline::assemble_for_mode(
            HomeMixerMode::Demo,
            HomeMixerFeatures::default(),
        )
        .await;
        let components = pipeline.components();
        let pre_selection = components
            .iter()
            .find(|entry| entry.stage == PipelineStage::Hydrator)
            .expect("pre-selection hydrators");
        let post_selection = components
            .iter()
            .find(|entry| entry.stage == PipelineStage::PostSelectionHydrator)
            .expect("post-selection hydrators");

        assert!(!pre_selection
            .components
            .iter()
            .any(|name| name == "GizmoduckCandidateHydrator"));
        assert!(post_selection
            .components
            .iter()
            .any(|name| name == "GizmoduckCandidateHydrator"));
        assert!(post_selection
            .components
            .iter()
            .any(|name| name == "VFCandidateHydrator"));
    }
}
