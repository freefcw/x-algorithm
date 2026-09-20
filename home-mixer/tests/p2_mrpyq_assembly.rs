//! Assembly-layer coverage for the mrpyq branch.
//!
//! Both cases drive `MRPYQ_RECOMMENDATION_DATA_ADDR`, which is process-global,
//! so they live in their own test binary and in one sequential test rather than
//! racing the other assembly tests.

use home_mixer::clients::mrpyq_adapters::pipeline_adapters_from_env;
use home_mixer::runtime_config::HomeMixerMode;
use home_mixer::{HomeMixerFeatures, PhoenixCandidatePipeline};
use xai_candidate_pipeline::candidate_pipeline::{CandidatePipeline, PipelineStage};

const ADDRESS_ENV: &str = "MRPYQ_RECOMMENDATION_DATA_ADDR";

#[tokio::test]
async fn mrpyq_address_switches_degraded_assembly_onto_the_business_ports() {
    // The gRPC channel connects lazily, so an unreachable address still
    // exercises the whole env -> config -> client -> adapters -> assembly path.
    std::env::set_var(ADDRESS_ENV, "http://127.0.0.1:1");

    let pipeline = PhoenixCandidatePipeline::assemble_for_mode(
        HomeMixerMode::Degraded,
        HomeMixerFeatures::default(),
    )
    .await
    .expect("mrpyq assembly");
    let sources = pipeline
        .components()
        .into_iter()
        .find(|entry| entry.stage == PipelineStage::Source)
        .map(|entry| entry.components)
        .unwrap_or_default();
    // `FallbackSource` is only assembled when the mrpyq adapters are present,
    // and TES / in-network / fallback / VF all branch on that same `Option`.
    assert!(
        sources.contains(&"FallbackSource".to_string()),
        "degraded assembly ignored the configured mrpyq backend: {sources:?}"
    );

    // A configured but unusable backend must fail assembly instead of quietly
    // degrading to the Disabled ports, which would serve an empty feed.
    std::env::set_var(ADDRESS_ENV, "not a uri");
    assert!(pipeline_adapters_from_env(
        false,
        std::sync::Arc::new(home_mixer::id::PaddedIdentityResolver::new())
    )
    .is_err());

    std::env::remove_var(ADDRESS_ENV);
}
