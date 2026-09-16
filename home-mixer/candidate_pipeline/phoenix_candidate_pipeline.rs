use crate::candidate_hydrators::core_data_candidate_hydrator::CoreDataCandidateHydrator;
use crate::candidate_hydrators::filtered_topics_hydrator::FilteredTopicsHydrator;
use crate::candidate_hydrators::gizmoduck_hydrator::GizmoduckCandidateHydrator;
use crate::candidate_hydrators::has_media_hydrator::HasMediaHydrator;
use crate::candidate_hydrators::in_network_candidate_hydrator::InNetworkCandidateHydrator;
use crate::candidate_hydrators::language_code_hydrator::LanguageCodeHydrator;

use crate::candidate_diversity_stats::LoggingCandidateDiversityStats;
use crate::candidate_hydrators::tes_hydration_provider::TesHydrationProvider;
use crate::candidate_hydrators::vf_candidate_hydrator::VFCandidateHydrator;
use crate::candidate_hydrators::video_duration_candidate_hydrator::VideoDurationCandidateHydrator;
use crate::clients::gizmoduck_client::{
    DemoGizmoduckClient, DisabledGizmoduckClient, GizmoduckClient,
};
use crate::clients::in_network_posts_client::{DemoFallbackPostsClient, InNetworkPostsClient};
use crate::clients::mrpyq_adapters::{pipeline_adapters_from_env, MrpyqPipelineAdapters};
use crate::clients::phoenix_prediction_client::{
    PhoenixPredictionClient, SlimPhoenixPredictionClient,
};
use crate::clients::phoenix_retrieval_client::{
    PhoenixRetrievalClient, ProdPhoenixRetrievalClient,
};

use crate::clients::strato_client::{DemoStratoClient, StratoClient};
#[cfg(feature = "legacy-int-ids")]
use crate::clients::thunder_client::ThunderClient;
use crate::clients::topic_retrieval_client::{DemoTopicRetrievalClient, TopicRetrievalClient};
use crate::clients::tweet_entity_service_client::{DemoTESClient, TESClient};
use crate::clients::uas_fetcher::{
    DemoUserActionSequenceFetcher, DisabledUserActionSequenceFetcher, RedisUserActionSequenceStore,
    UserActionSequenceOps,
};
use crate::clients::user_topic_reader::{DemoUserTopicReader, UserTopicReader};
#[cfg(feature = "legacy-int-ids")]
use crate::clients::vm_ranker_client::GrpcVMRankerClient;
use crate::feature_policy::HomeMixerFeatures;
use crate::feed_state::FeedStateStore;
use crate::filters::age_filter::AgeFilter;
use crate::filters::author_socialgraph_filter::AuthorSocialgraphFilter;
use crate::filters::core_data_hydration_filter::CoreDataHydrationFilter;
use crate::filters::dedup_conversation_filter::DedupConversationFilter;
use crate::filters::drop_duplicates_filter::DropDuplicatesFilter;
use crate::filters::first_stage_eligible_filter::FirstStageEligibleFilter;
use crate::filters::new_user_topic_ids_filter::NewUserTopicIdsFilter;
use crate::filters::previously_seen_posts_backup_filter::PreviouslySeenPostsBackupFilter;
use crate::filters::previously_seen_posts_filter::PreviouslySeenPostsFilter;
use crate::filters::previously_served_posts_filter::PreviouslyServedPostsFilter;
use crate::filters::self_tweet_filter::SelfTweetFilter;
use crate::filters::topic_ids_filter::TopicIdsFilter;
use crate::filters::vf_filter::VFFilter;
use crate::filters::video_filter::VideoFilter;
use crate::filters::viewer_muted_keyword_filter::ViewerMutedKeywordFilter;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::params;
use crate::query_hydrators::blocked_user_ids_query_hydrator::BlockedUserIdsQueryHydrator;
use crate::query_hydrators::followed_user_ids_query_hydrator::FollowedUserIdsQueryHydrator;
use crate::query_hydrators::muted_user_ids_query_hydrator::MutedUserIdsQueryHydrator;
use crate::query_hydrators::past_request_timestamps_query_hydrator::PastRequestTimestampsQueryHydrator;
use crate::query_hydrators::retrieval_sequence_query_hydrator::RetrievalSequenceQueryHydrator;
use crate::query_hydrators::scoring_sequence_query_hydrator::ScoringSequenceQueryHydrator;
use crate::query_hydrators::served_history_query_hydrator::ServedHistoryQueryHydrator;

use crate::query_hydrators::user_action_seq_query_hydrator::UserActionSeqQueryHydrator;
use crate::query_hydrators::user_features_query_hydrator::UserFeaturesQueryHydrator;
use crate::query_hydrators::user_safety_features_query_hydrator::UserSafetyFeaturesQueryHydrator;
use crate::query_hydrators::user_topics_query_hydrator::UserTopicsQueryHydrator;
use crate::runtime_config::{HomeMixerMode, UasConfig};
use crate::scorers::author_cold_start::{AuthorColdStart, AuthorColdStartScorer, ColdStartConfig};
use crate::scorers::phoenix_scorer::PhoenixScorer;
use crate::scorers::ranking_scorer::RankingScorer;
use crate::scorers::rule_fallback_scorer::RuleFallbackScorer;
#[cfg(feature = "legacy-int-ids")]
use crate::scorers::vm_ranker::VMRanker;
use crate::selectors::TopKScoreSelector;
use crate::side_effects::phoenix_request_cache_side_effect::PhoenixRequestCacheSideEffect;
use crate::side_effects::response_diversity_stats_side_effect::ResponseDiversityStatsSideEffect;
use crate::sources::cached_posts_source::CachedPostsSource;
use crate::sources::fallback_source::FallbackSource;
use crate::sources::phoenix_moe_source::PhoenixMoeSource;
use crate::sources::phoenix_source::PhoenixSource;
use crate::sources::phoenix_topics_source::PhoenixTopicsSource;
use crate::sources::thunder_source::ThunderSource;
use crate::visibility::vf_client::{DemoVisibilityFilteringClient, VisibilityFilteringClient};
use anyhow::Context;
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

/// All runtime ports needed to assemble the Phoenix candidate pipeline.
///
/// Production callers implement these ports against their own services and
/// inject them here; the mode-based helpers below remain Demo/Degraded
/// conveniences and never claim that disabled adapters are production-ready.
pub struct PhoenixDependencies {
    pub uas_fetcher: Arc<dyn UserActionSequenceOps>,
    pub phoenix_client: Arc<dyn PhoenixPredictionClient + Send + Sync>,
    pub phoenix_retrieval_client: Arc<dyn PhoenixRetrievalClient + Send + Sync>,
    pub in_network_client: Option<Arc<dyn InNetworkPostsClient>>,
    pub strato_client: Arc<dyn StratoClient + Send + Sync>,
    pub tes_client: Arc<dyn TESClient + Send + Sync>,
    pub gizmoduck_client: Arc<dyn GizmoduckClient + Send + Sync>,
    pub vf_client: Arc<dyn VisibilityFilteringClient + Send + Sync>,
    pub topic_clients: Option<TopicPersonalizationClients>,
    pub moe_retrieval_client: Option<Arc<dyn PhoenixRetrievalClient + Send + Sync>>,
    pub fallback_client: Option<Arc<dyn InNetworkPostsClient>>,
    pub features: HomeMixerFeatures,
}

impl PhoenixCandidatePipeline {
    /// Add request-local served history and request timestamps to the same
    /// pipeline instance that later records the successful response. Keeping
    /// the store at this boundary avoids a read/write split between servers.
    pub fn with_feed_state_store(mut self, store: Arc<dyn FeedStateStore>) -> Self {
        self.install_feed_state_store(store);
        self
    }

    /// Install state hydrators on an already allocated pipeline.
    pub fn install_feed_state_store(&mut self, store: Arc<dyn FeedStateStore>) {
        self.query_hydrators.insert(
            0,
            Box::new(ServedHistoryQueryHydrator::from_store(Arc::clone(&store))),
        );
        self.query_hydrators.insert(
            1,
            Box::new(PastRequestTimestampsQueryHydrator::from_store(store)),
        );
    }

    pub async fn build_with_clients(dependencies: PhoenixDependencies) -> PhoenixCandidatePipeline {
        Self::build_with_clients_and_diversity_stats(
            dependencies,
            ResponseDiversityStatsSideEffect::new(Arc::new(LoggingCandidateDiversityStats)),
        )
        .await
    }

    /// Assemble with an explicitly supplied diversity sink and sampling policy.
    pub async fn build_with_clients_and_diversity_stats(
        dependencies: PhoenixDependencies,
        diversity_stats: ResponseDiversityStatsSideEffect,
    ) -> PhoenixCandidatePipeline {
        let PhoenixDependencies {
            uas_fetcher,
            phoenix_client,
            phoenix_retrieval_client,
            in_network_client,
            strato_client,
            tes_client,
            gizmoduck_client,
            vf_client,
            topic_clients,
            moe_retrieval_client,
            fallback_client,
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
            Box::new(UserSafetyFeaturesQueryHydrator::new(feature_provider)),
        ];
        let topic_retrieval_client = topic_clients.map(|clients| {
            query_hydrators.push(Box::new(UserTopicsQueryHydrator {
                reader: clients.reader,
            }));
            clients.retriever
        });

        // Sources follow the upstream order. TweetMixer remains U3 and is omitted.
        // Integer Thunder cannot carry real ObjectIds, so the in-network source is
        // left unassembled when neither mrpyq nor the demo adapter is present.
        let mut sources: Vec<Box<dyn Source<ScoredPostsQuery, PostCandidate>>> = Vec::new();
        if let Some(client) = in_network_client {
            sources.push(Box::new(ThunderSource { client }));
        }
        sources.push(Box::new(PhoenixSource {
            phoenix_retrieval_client,
        }));
        if let Some(fallback_client) = fallback_client {
            sources.push(Box::new(FallbackSource {
                client: fallback_client,
            }));
        }
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
            Box::new(VideoDurationCandidateHydrator::new(Arc::clone(
                &tes_provider,
            ))),
            Box::new(HasMediaHydrator::new(Arc::clone(&tes_provider))),
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
            Box::new(FirstStageEligibleFilter),
            Box::new(AgeFilter::new(Duration::from_secs(params::MAX_POST_AGE))),
            Box::new(SelfTweetFilter),
            Box::new(PreviouslySeenPostsFilter),
            Box::new(PreviouslySeenPostsBackupFilter),
            Box::new(PreviouslyServedPostsFilter),
            Box::new(ViewerMutedKeywordFilter::new()),
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
            Box::new(RankingScorer),
            Box::new(RuleFallbackScorer),
        ];

        // 可选旁路：VM Ranker 二次重排（上游 scorers 第三位）。开关 + 地址
        // 齐备时装配本仓库 vm-ranker 服务的 gRPC Adapter；缺地址时禁用旁路
        // 并保留主链（与 MoE 相同的降级规则）。
        #[cfg(feature = "legacy-int-ids")]
        if features.vm_ranker {
            match std::env::var("VM_RANKER_GRPC_ADDR") {
                Ok(addr) if !addr.trim().is_empty() => {
                    match GrpcVMRankerClient::new(addr.clone()) {
                        Ok(client) => {
                            log::info!("VMRanker scorer enabled via {addr}");
                            scorers.push(Box::new(VMRanker {
                                client: Arc::new(client),
                                value_model_id: std::env::var("VM_RANKER_VALUE_MODEL_ID").ok(),
                            }));
                        }
                        Err(error) => log::warn!(
                            "VM_RANKER_GRPC_ADDR={addr} is not a usable endpoint ({error}); disabling VM Ranker scorer"
                        ),
                    }
                }
                _ => {
                    log::warn!(
                        "HOME_MIXER_ENABLE_VM_RANKER is set but VM_RANKER_GRPC_ADDR is missing; disabling VM Ranker scorer"
                    );
                }
            }
        }
        #[cfg(not(feature = "legacy-int-ids"))]
        if features.vm_ranker {
            log::warn!(
                "HOME_MIXER_ENABLE_VM_RANKER is set but legacy-int-ids is disabled; VM Ranker adapter is unavailable"
            );
        }
        if features.author_cold_start {
            scorers.push(Box::new(AuthorColdStartScorer::new(author_cold_start)));
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
            Box::new(VFFilter::new(features.vf_failure_policy)),
            Box::new(DedupConversationFilter),
        ];

        // Side Effects
        let side_effects: Arc<Vec<Box<dyn SideEffect<ScoredPostsQuery, PostCandidate>>>> =
            Arc::new(vec![
                Box::new(PhoenixRequestCacheSideEffect::new(
                    strato_client,
                    features.request_cache_side_effect,
                )),
                Box::new(diversity_stats),
            ]);

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
    pub async fn prod() -> anyhow::Result<PhoenixCandidatePipeline> {
        Self::prod_with_features(HomeMixerFeatures::from_env()).await
    }

    pub async fn prod_with_features(
        features: HomeMixerFeatures,
    ) -> anyhow::Result<PhoenixCandidatePipeline> {
        Self::assemble_for_mode(Self::compatibility_mode(), features).await
    }

    /// Assembles for an explicit mode, resolving the UAS adapter from the
    /// environment like the other external addresses. `HomeMixerServer` uses
    /// [`Self::assemble_with_uas`] with the config it already validated.
    pub async fn assemble_for_mode(
        mode: HomeMixerMode,
        features: HomeMixerFeatures,
    ) -> anyhow::Result<PhoenixCandidatePipeline> {
        Self::assemble_with_uas(mode, features, UasConfig::from_env(mode)?).await
    }

    pub async fn assemble_with_uas(
        mode: HomeMixerMode,
        features: HomeMixerFeatures,
        uas: UasConfig,
    ) -> anyhow::Result<PhoenixCandidatePipeline> {
        let topic_clients = (mode == HomeMixerMode::Demo).then(|| {
            TopicPersonalizationClients::new(
                Arc::new(DemoUserTopicReader),
                Arc::new(DemoTopicRetrievalClient),
            )
        });
        Self::assemble_with_optional_topic_clients(mode, topic_clients, features, uas).await
    }

    /// Builds the pipeline with explicitly supplied topic adapters.
    ///
    /// Supplying these adapters is the manual enablement action: production
    /// callers must verify the reader and retrieval service contracts first.
    pub async fn prod_with_topic_clients(
        topic_clients: TopicPersonalizationClients,
    ) -> anyhow::Result<PhoenixCandidatePipeline> {
        let mode = Self::compatibility_mode();
        Self::assemble_with_optional_topic_clients(
            mode,
            Some(topic_clients),
            HomeMixerFeatures::from_env(),
            UasConfig::from_env(mode)?,
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
        uas: UasConfig,
    ) -> anyhow::Result<PhoenixCandidatePipeline> {
        let demo_mode = mode == HomeMixerMode::Demo;
        let features = features_for_mode(mode, features);
        uas.validate(mode)?;
        if demo_mode {
            log::info!("HOME_MIXER_MODE=demo: injecting demo Strato / TES clients");
        }

        let mrpyq_adapters = pipeline_adapters_from_env(demo_mode)
            .context("failed to create mrpyq recommendation data adapters")?;
        if !demo_mode && mrpyq_adapters.is_none() {
            anyhow::bail!(
                "MRPYQ_RECOMMENDATION_DATA_ADDR is required; integer Thunder cannot carry real ObjectIds"
            );
        }
        let mrpyq_adapters = mrpyq_adapters.as_ref();

        let uas_fetcher: Arc<dyn UserActionSequenceOps> = match uas {
            UasConfig::Demo => {
                log::info!("UAS: demo synthetic sequence");
                Arc::new(DemoUserActionSequenceFetcher)
            }
            UasConfig::Disabled => {
                log::warn!(
                    "UAS: no Redis configured (UAS_REDIS_URL / HOME_MIXER_REDIS_URL); Phoenix retrieval and ranking are skipped and every request is ranked by the rule fallback"
                );
                Arc::new(DisabledUserActionSequenceFetcher::new()?)
            }
            UasConfig::Redis(config) => {
                // The projection job and Home Mixer share HOME_MIXER_REDIS_URL
                // by default; UAS_REDIS_URL points at a dedicated Redis when
                // feed state and UAS are operated separately.
                let store = RedisUserActionSequenceStore::new(config)
                    .await
                    .map_err(|error| {
                        // Name both variables: outside demo the UAS target is
                        // usually the shared HOME_MIXER_REDIS_URL, so a Redis
                        // outage surfaces here first and must not read as a
                        // UAS-only misconfiguration.
                        anyhow::anyhow!(
                            "failed to initialize the Redis UAS adapter (UAS_REDIS_URL, or HOME_MIXER_REDIS_URL when unset): {error}"
                        )
                    })?;
                log::info!("UAS: Redis adapter enabled");
                Arc::new(store)
            }
        };
        let strato_client: Arc<dyn StratoClient + Send + Sync> = if demo_mode {
            Arc::new(DemoStratoClient)
        } else {
            Arc::clone(
                &mrpyq_adapters
                    .expect("non-demo assembly requires mrpyq adapters")
                    .strato,
            )
        };
        let tes_client: Arc<dyn TESClient + Send + Sync> = if demo_mode {
            Arc::new(DemoTESClient)
        } else {
            Arc::clone(
                &mrpyq_adapters
                    .expect("non-demo assembly requires mrpyq adapters")
                    .tes,
            )
        };

        let phoenix_client: Arc<dyn PhoenixPredictionClient + Send + Sync> =
            Arc::new(SlimPhoenixPredictionClient::new_with_allow_random(demo_mode).await?);
        let phoenix_retrieval_client =
            Arc::new(ProdPhoenixRetrievalClient::new_with_allow_random(demo_mode).await?);
        let in_network_client = assemble_in_network_client(demo_mode, mrpyq_adapters).await;
        let fallback_client: Option<Arc<dyn InNetworkPostsClient>> = if demo_mode {
            Some(Arc::new(DemoFallbackPostsClient) as Arc<dyn InNetworkPostsClient>)
        } else {
            mrpyq_adapters.map(|adapters| Arc::clone(&adapters.in_network))
        };
        let gizmoduck_client: Arc<dyn GizmoduckClient + Send + Sync> =
            if demo_mode && features.author_cold_start {
                Arc::new(DemoGizmoduckClient)
            } else {
                Arc::new(DisabledGizmoduckClient::new().await?)
            };
        let vf_client: Arc<dyn VisibilityFilteringClient + Send + Sync> = if demo_mode {
            Arc::new(DemoVisibilityFilteringClient)
        } else {
            Arc::clone(
                &mrpyq_adapters
                    .expect("non-demo assembly requires mrpyq adapters")
                    .vf,
            )
        };
        let moe_retrieval_client = if features.phoenix_moe {
            match std::env::var("PHOENIX_MOE_GRPC_ADDR") {
                Ok(addr) => Some(
                    Arc::new(ProdPhoenixRetrievalClient::from_addr_with_allow_random(
                        addr, demo_mode,
                    )?) as Arc<dyn PhoenixRetrievalClient + Send + Sync>,
                ),
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

        Ok(
            PhoenixCandidatePipeline::build_with_clients(PhoenixDependencies {
                uas_fetcher,
                phoenix_client,
                phoenix_retrieval_client,
                in_network_client,
                strato_client,
                tes_client,
                gizmoduck_client,
                vf_client,
                topic_clients,
                moe_retrieval_client,
                fallback_client,
                features,
            })
            .await,
        )
    }
}

/// Real in-network recall is mrpyq. Integer Thunder exists only for explicit
/// demo mode, because it cannot carry real ObjectIds.
async fn assemble_in_network_client(
    demo_mode: bool,
    mrpyq_adapters: Option<&MrpyqPipelineAdapters>,
) -> Option<Arc<dyn InNetworkPostsClient>> {
    #[cfg(not(feature = "legacy-int-ids"))]
    let _ = demo_mode;

    if let Some(adapters) = mrpyq_adapters {
        return Some(Arc::clone(&adapters.in_network));
    }
    #[cfg(feature = "legacy-int-ids")]
    if demo_mode {
        return Some(Arc::new(ThunderClient::new().await));
    }
    None
}

fn features_for_mode(mode: HomeMixerMode, mut features: HomeMixerFeatures) -> HomeMixerFeatures {
    if mode != HomeMixerMode::Demo && features.author_cold_start {
        log::warn!(
            "Author Cold Start requires verified TES and Gizmoduck count adapters; disabling it outside demo mode"
        );
        features.author_cold_start = false;
        features.cold_start_thompson_sampling = false;
    }
    if mode != HomeMixerMode::Demo && features.vm_ranker {
        log::warn!(
            "VM Ranker still uses integer proto and cannot carry real ObjectIds; disabling it outside demo mode"
        );
        features.vm_ranker = false;
    }
    features
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
        .await
        .expect("demo assembly");
        let components = pipeline.components();
        let pre_selection = components
            .iter()
            .find(|entry| entry.stage == PipelineStage::Hydrator)
            .expect("pre-selection hydrators");
        let scorers = components
            .iter()
            .find(|entry| entry.stage == PipelineStage::Scorer)
            .expect("scorers");

        assert!(pre_selection
            .components
            .iter()
            .any(|name| name == "GizmoduckCandidateHydrator"));
        assert_eq!(
            scorers.components,
            vec![
                "PhoenixScorer",
                "RankingScorer",
                "RuleFallbackScorer",
                "AuthorColdStartScorer",
            ]
        );
    }

    #[test]
    fn cold_start_is_disabled_without_verified_non_demo_adapters() {
        let features = features_for_mode(
            HomeMixerMode::Degraded,
            HomeMixerFeatures {
                author_cold_start: true,
                cold_start_thompson_sampling: true,
                ..Default::default()
            },
        );

        assert!(!features.author_cold_start);
        assert!(!features.cold_start_thompson_sampling);
    }

    #[test]
    fn integer_vm_ranker_is_disabled_outside_demo() {
        let features = features_for_mode(
            HomeMixerMode::Degraded,
            HomeMixerFeatures {
                vm_ranker: true,
                ..Default::default()
            },
        );
        assert!(!features.vm_ranker);
    }

    #[tokio::test]
    async fn profile_hydration_runs_only_after_selection() {
        let pipeline = PhoenixCandidatePipeline::assemble_for_mode(
            HomeMixerMode::Demo,
            HomeMixerFeatures::default(),
        )
        .await
        .expect("demo assembly");
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

    #[tokio::test]
    async fn non_demo_without_mrpyq_fails_assembly() {
        let result = PhoenixCandidatePipeline::assemble_for_mode(
            HomeMixerMode::Degraded,
            HomeMixerFeatures::default(),
        )
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("real traffic cannot start without mrpyq"),
        };
        assert!(
            error.to_string().contains("MRPYQ_RECOMMENDATION_DATA_ADDR"),
            "{error}"
        );
    }
}
