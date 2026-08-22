# Entrypoint migration map

> Status: Home Mixer `HM-E1..E6` and Phoenix `PHX-E1/E2` portable contracts completed; later portable deltas are absorbed through `c65aa17`; remaining work requires explicit production integration contracts
> Upstream anchor: `c65aa179db7bdd61e2c2821eac87f208a105c053` (2026-08-14 semantic anchor，本文审计范围的时代锚点；**当前活锚点已前移**，最新值见 [`upstream-first-maintenance.md`](./upstream-first-maintenance.md) 头部)
> Snapshot reports: [`../update/20260813.md`](../update/20260813.md) for the `47c1bcd` restructuring and [`../update/20260814.md`](../update/20260814.md) for `c65aa17` migration outcomes. Entrypoint shapes below retain the established For You scope; later multi-Feed products remain unassembled U3 capabilities.
> Local branch: `feature/migrate-20260515`
> Maintenance rule: [`upstream-first-maintenance.md`](./upstream-first-maintenance.md)

## 1. Scope and terminology

This document compares the portable upstream entry paths through `c65aa17` with the current local, runnable paths. “Align” means preserving upstream entry names and ownership where portable. It does not mean importing unavailable X service builders, feature-switch systems, storage clients, models, or deployment code.

The audit proceeds from process entry to service assembly, request construction, application server, pipeline assembly, domain models, component implementations, and external adapters. Work should follow that order so later components do not target another temporary boundary.

## 2. End-to-end entry graphs

### 2.1 Upstream Home Mixer

```text
home-mixer/main.rs
  -> XServiceBuilder::run<HomeMixerServer>(HomeMixerConfig)
  -> HomeMixerServer::build(ServiceContext)
       -> QueryBuilder(feature switches, decider, viewer data)
       -> PhoenixCandidatePipeline::prod(...)
       -> ScoredPostsServer(QueryBuilder, PhoenixCandidatePipeline)
       -> ForYouCandidatePipeline::new(ScoredPostsServer, datacenter)
       -> ForYouFeedServer(QueryBuilder, ForYouCandidatePipeline)
  -> HomeMixerServer::register(...)
       -> ScoredPostsService -> ScoredPostsServer
       -> ForYouFeedService -> ForYouFeedServer
```

### 2.2 Current local Home Mixer

```text
home-mixer/main.rs
  -> HomeMixerConfig::from_env()
  -> HomeMixerServer::build(config)
       -> QueryBuilder(viewer policy, feature policy, request identity)
       -> ScoredPostsServer(PhoenixCandidatePipeline)
       -> ForYouFeedServer(ForYouCandidatePipeline)
  -> HomeMixerServer::register(...)
       -> ScoredPostsService -> ScoredPostsServer
       -> ForYouFeedService -> ForYouFeedServer
       -> tonic reflection + compression/message limits
```

The local path is runnable and now matches the upstream service and request boundaries. Remaining translation cost is concentrated in final-feed module paths, pipeline component ownership, and unavailable adapters.

### 2.3 Current local end-to-end Demo

```text
scripts/run_demo.sh
  -> Phoenix gRPC gateway :50053
  -> Thunder demo-seed service :50052
  -> Home Mixer :50051
  -> demo-client
       -> GetScoredPosts or GetForYouFeed
```

This is a downstream verification entry (`U2`), not an upstream replacement. It must remain operational throughout migration.

## 3. Home Mixer work packages

### HM-E1: Restore service composition boundaries

Status: **completed**. `HomeMixerConfig -> HomeMixerServer::build -> register` is active; RPC traits are owned by their application servers; gzip/zstd and existing message limits are registered centrally. ScoredPosts and ForYou end-to-end Demos pass.

| Item | Upstream | Current | Required work |
|---|---|---|---|
| Process shell | `XServiceBuilder` | manual tonic + axum | Keep the public local shell as `U1`; introduce a local `HomeMixerConfig` and a single `HomeMixerServer::build` assembly entry. |
| RPC ownership | traits on `ScoredPostsServer` / `ForYouFeedServer` | both traits on `HomeMixerServer` | Move RPC implementations to the two application servers; let `HomeMixerServer` only own and register them. |
| Registration | `HomeMixerServer::register` | registration in `main.rs` | Add an upstream-shaped registration method; retain local reflection and message limits. |
| Compression | gzip/zstd | not configured | Add only after current tonic/proto clients have compatibility tests. |

Exit criteria: both existing RPCs return the same responses, reflection still works, `demo-client` passes for scored posts and final feed, and process startup has exactly one dependency assembly entry.

### HM-E2: Restore `QueryBuilder` as the request entry

Status: **completed for the current public contract**. `QueryBuilder` is the sole protobuf mapper and request/prediction identity owner, shares one typed feature policy across both RPCs, and uses an upstream-shaped `GizmoduckClient::get_viewer_data` port with a 200 ms timeout. Only explicit viewer Allow enables out-of-network recommendations; errors, timeouts, and unknown policy restrict the request to in-network. Fields absent from the public proto/model (roles, subscription, full device status, trace context, decider params) remain deferred rather than synthesized.

Current `query_builder.rs::query_from_proto` is private to `QueryBuilder` and only maps public fields. `server.rs` retains a compatibility re-export so upstream-comparable imports remain stable. Upstream `QueryBuilder` also owns request IDs, prediction IDs, viewer data, feature switches, decider context, device context, trace context, and request-level policy.

Required work:

1. Introduce a local `QueryBuilder` and `RequestContext` at the upstream `server.rs` boundary; keep implementation in `query_builder.rs` and a facade re-export in `server.rs`.
2. Move all protobuf-to-domain mapping out of the RPC facade into `QueryBuilder`.
3. Keep unavailable feature switches/decider behind the existing local feature-switch interface (`U1`), not in `ScoredPostsQuery` as private types.
4. Add explicit ports for viewer data and request policy; use public/demo adapters.
5. Generate request/prediction IDs in one place and preserve them across both inner and outer services.
6. Add table-driven tests for every protobuf field, defaults, invalid viewer IDs, cached posts, topics, device context, and request IDs.

Exit criteria: ScoredPosts and ForYou use the same builder; no RPC method mutates query fields after construction except protocol-specific cursor handling.

### HM-E3: Re-anchor model and module paths

Status: **completed** for canonical Home Mixer models and domain ID types.

| Upstream path | Current path | Decision |
|---|---|---|
| `models/query.rs` | `models/query.rs` | Complete. Owns unsigned domain IDs, request ID, prediction ID, and request time. |
| `models/candidate.rs` | `models/candidate.rs` | Complete. `PostCandidate` identity fields use `u64`; signed adapters use checked conversion. |
| `models/candidate_features.rs` | `models/candidate_features.rs` | Complete. The old Candidate Pipeline path is a pure compatibility re-export. |
| `for_you_server.rs` | `for_you_server.rs` | Complete in HM-E5; the legacy `final_feed/` module is deleted. Local domain/state/stats infrastructure now lives at `models/feed_item.rs` (U2), `feed_state.rs` (U1), and `feed_stats.rs` (U1). |
| `candidate_pipeline/for_you_candidate_pipeline.rs` | `candidate_pipeline/for_you_candidate_pipeline.rs` | Complete in HM-E5; the legacy `final_feed/` module is deleted. |
| `ads/`, `selectors/blender_selector.rs`, `sources/*` | canonical upstream paths restored | Complete portable boundaries; Ads/WTF/Prompts/PushToHome adapters remain explicitly disabled. |

The `candidate_pipeline/{candidate,candidate_features,query,query_features}.rs` compatibility re-exports are deleted; all call sites import `crate::models::*` directly. Public protobuf IDs remain signed for wire compatibility and use `try_from` at `QueryBuilder`, Thunder, and signed-store boundaries.

Exit evidence: all Home Mixer production call sites use `crate::models::*`; invalid viewer IDs are rejected; negative history IDs are filtered; request and prediction identity are generated once by `QueryBuilder`; Home Mixer tests and both Demo entry points pass.

### HM-E4: Reconcile the inner Phoenix pipeline assembly

Status: **portable assembly complete**. Publicly implementable component boundaries use upstream-comparable names and order; missing services remain `U1`, `U2`, or `U3`, so this is not a production integration claim.

| Stage | Upstream through `c65aa17` | Current | Work |
|---|---:|---:|---|
| Query Hydrators | 15 configured, one impressed-post hydrator constructed but unused | 7 default + optional local topic reader | Scoring/Retrieval and Blocked/Muted/Followed/Subscribed now use upstream names and field ownership over one request-scoped UAS/Strato read. CachedPosts, MutualFollow, demographics, Grok topics/starter packs, bloom filter, IP, and inferred gender remain `U3`; local safety/topic owners are `U1/U2`. |
| Sources | 6 | 3 default + optional Topic/MoE | Available entries follow upstream order: Thunder, omitted TweetMixer, Phoenix, optional Topics, optional MoE, Cached. TweetMixer now has an upstream-shaped Source over the `TweetMixerClient` port (interface-first, unassembled). |
| Pre-selection Hydrators | 10 | 8 default + demo Cold Start author profile | InNetwork, CoreData, Quote, VideoDuration, HasMedia, Subscription, FilteredTopics, and LanguageCode use upstream entry boundaries; a shared TES provider prevents duplicate core/media batches. Gizmoduck normally runs post-selection (`U2`); explicit demo Cold Start adds a pre-selection pass for follower eligibility. CoreData owns engagement counts because the public TES adapter returns them inside core data (`U1`). BlockedBy remains `U3`. |
| Filters | 15 | 14 | Portable order is complete except `Brazil2026ElectionFilter`, which is an explicit product decision not to adopt. `NewUserTopicIdsFilter` owns cold-start topic matching separately from `TopicIdsFilter`. |
| Scorers | 3 | 2 default + optional VM + demo Cold Start final scorer | `RankingScorer` preserves local Weighted/AuthorDiversity/OON behavior. VM Ranker is assembled when its switch and address are present. Demo Cold Start is a final scorer after optional VM so exploration applies exactly once; non-demo remains disabled until TES/Gizmoduck production adapters are verified. |
| Post-selection Hydrators | 6 | 2 | Gizmoduck and VF. Author profile hydration is deferred until after selection (`U2`) so profile reads follow the truncated candidate set; `profile_hydration_runs_only_after_selection` locks that placement. BlockedBy now has an upstream-shaped Hydrator over `SocialGraphClientOps` (interface-first, unassembled); ads safety, tweet metrics, following replies, and mutual-follow remain `U3` until their data contracts exist. |
| SideEffects | 6 | request cache only | Request cache is default off; seen-ids and served-candidates now have domain ports plus upstream-shaped SideEffects (interface-first, unassembled); other sinks remain `U3` pending consumer/schema/retention/idempotency contracts. |

Completed portable work:

1. Query sequence and social-graph field owners use upstream file/type names and share one local adapter read per request.
2. Source, pre-Hydrator, Filter, Scorer, and request-cache SideEffect ordering is mechanically comparable with upstream.
3. Core/media field owners share a request-scoped TES batch provider; quote media failure is neutral and does not erase quote metadata.
4. `RankingScorer` preserves local weighted, author-diversity, and OON transformations behind one upstream boundary.
5. The upstream Source order changes which equal-scored synthetic out-of-network candidates survive Demo truncation: totals and scores stay the same, while standard Phoenix now precedes Topics. This is an intentional upstream execution-semantic change, not a production ranking claim.

This portable completion unblocked the HM-E5 work recorded below: outer ForYou upstream paths and disabled source/SideEffect entry modules are now restored.

Migrate one vertical behavior at a time: query field source -> component -> filter/scorer use -> response/debug evidence. Do not add fields without an owner and an enablement rule.

Exit criteria: component inventory order is mechanically comparable with upstream; every difference is tagged `U1`, `U2`, or `U3`; Home Mixer tests and default Demo remain unchanged.

### HM-E5: Reconcile the outer For You pipeline

Status: **portable assembly complete**. Root ForYou server, canonical candidate pipeline, state Query Hydrators, five Source entry points, Blender path, Ads modules, and response-stats SideEffect now match upstream boundaries. External content and event sinks remain disabled.

Upstream has two query hydrators, five sources, one blender, and eight side effects. The local mapping is:

1. `ServedHistoryQueryHydrator` and `PastRequestTimestampsQueryHydrator` read the bounded in-memory `FeedStateStore` (`U1`) independently.
2. Sources are ordered ScoredPosts, Ads, WhoToFollow, Prompts, PushToHome. The four non-post adapters return `enable=false`; test-only supplemental sources remain additive.
3. Root `ads/` owns SafeGap/PartitionOrganic behavior, including the local fail-closed missing-verdict rule (`U2`).
4. `ForYouResponseStatsSideEffect` wraps the local `FeedStatsSink`. Four Kafka event sinks remain `U3`.
5. Served history and request timestamp update/truncation remain one synchronous server commit (`U2`) so the next request observes the response immediately. Splitting them into asynchronous production SideEffects requires an atomicity, retry, retention, and recovery contract.

Exit evidence: default final feed remains the scored-post order; no external source emits by default; the P4/P5 25-test suite and both public Home Mixer service traits pass.

### HM-E6: Expand the public RPC surface additively

Status: **completed for the current public contract**. Existing RPC method signatures and field numbers are unchanged.

1. `ForYouFeedQuery` and additive `GetForYouFeedV2` provide a ForYou-owned growth boundary; original `GetForYouFeed(ScoredPostsQuery)` remains source/wire compatible.
2. `DebugScoredPosts` returns the normal response plus typed retrieved/filtered/selected counts and tweet IDs from the same pipeline execution. It is default disabled and requires both `HOME_MIXER_ENABLE_DEBUG_RPC=1` and matching `x-home-mixer-debug-token` metadata.
3. Request-supplied unsigned `cached_posts` are default rejected and may be enabled only in explicit Demo mode; a production cache requires a server-owned or signed opaque contract.
4. URT is unmigrated. `47c1bcd:` upstream published `home-mixer/util/urt/` (17 files) covering cursors, controller data, feedback, and post/ad/frame serialization, so the missing-contract reason no longer holds; the remaining question is scope, since URT arrives together with the new Feed product family.
5. gzip/zstd and message limits are already centralized and tested in HM-E1. Trace-header propagation remains deferred because no public header ownership contract exists.

Exit evidence: generated server traits compile; wrapper validation rejects missing inner queries; Debug is default unavailable, wrong-token requests are permission denied, and an authorized live grpcurl call returned 50 posts with 600/4/50 stage counts. ForYou V2 returned 50 items. Existing clients and both original RPCs remain operational.

## 4. Phoenix work packages

### PHX-E1: Keep the upstream offline entry canonical

Status: **completed**, now **legacy**. `phoenix/run_pipeline.py::main` remains the canonical offline entry and keeps upstream argument names: `artifacts_dir`, `sequence_file`, `corpus_file`, `top_k_retrieval`, and `top_k_display`. Local `impression_timestamp` is explicitly additive and makes post-age features reproducible.

`47c1bcd:` upstream replaced this demo pipeline wholesale with a training framework (`xrex/`, `crates/`, `reference/`) and deleted `run_pipeline.py` along with the artifact zip. Locally both tracks coexist: the published-artifact inference chain described here still serves Home Mixer and keeps its test suite (92 tests as of the 2026-08-22 recheck), while the upstream framework landed verbatim beside it. `PHX-E1/E2` therefore describe the **legacy track** (background in `phoenix/docs/legacy-pipeline.md`); the new framework is not an entrypoint replacement until a locally trained artifact is validated against the inference chain.

The CLI now owns only argument/path parsing and presentation. `services/published_artifacts.py` owns config, NPZ params/embeddings, hashing, model config, and Snowflake age features; `services/published_pipeline.py` owns retrieval-to-ranking execution and output mapping.

Fixture evidence: offline JSON and online proto UAS produce identical published history tensors; candidate age tensors match under a fixed impression timestamp; action-index and deterministic corpus snapshot tests pass.

### PHX-E2: Make online serving an adapter over the same core

Status: **completed for code and fixture parity**. The local `scripts/run_grpc_gateway.py` remains a necessary `U1` process adapter for Home Mixer.

1. Published offline and gRPC modes both construct `PublishedPipeline`, which owns the same `PublishedRankerEngine` and `PublishedRetrievalEngine`.
2. Both engines consume preloaded params/config/embeddings from `PublishedArtifact`; runner `initialize(params=...)` avoids a second checkpoint loader.
3. Proto and JSON input conversion terminate at the same transport-neutral `HistoryFeatures` / `CandidatePrediction` values, batch builder, model runners, and prediction mapping.
4. Published corpus topics use a startup-built post index rather than one full corpus scan per displayed candidate.
5. Random/demo, individual local checkpoint, and published artifact modes remain explicit in the gRPC CLI.

Exit evidence: Phoenix tests pass (88 at phase exit; 92 as of the 2026-08-22 recheck); shared retrieval-to-ranking orchestration, length mismatch, offline/proto preprocessing, fixed-time parity, and indexed topic lookup have fixtures. Consolidated files pass Ruff. The 2.9 GB artifact is an LFS pointer in this working tree, so current-turn real-artifact execution remains unavailable; the exact OID `fbc6017d...a83dac` has historical offline and gRPC validation recorded in the capability inventory.

## 5. Thunder boundary

Status: **revalidated**. `e414c17` does not change Thunder relative to the common base, so no Kafka or storage behavior was imported.

`47c1bcd:` upstream open-sourced the Thrift `schema/` tree (tweet, user, media, events). It landed behind the `legacy` feature, which removes the `crate::schema` compile blocker on the v1 Kafka listener — `cargo check -p thunder --all-targets --all-features` now passes. The listener still does not run: `xai_kafka` and `xai_thunder_proto` are unpublished and stay excluded by `cfg(xai_internal_deps)`, so ingest continues to use the local rdkafka path.

The local `U1/U2` boundary now has:

1. one public `InNetworkPostsService` gRPC dependency with aligned default port `50052`;
2. a 500 ms Home Mixer Source timeout and fail-isolated error propagation;
3. `seen_ids` forwarded as Thunder exclusions;
4. checked, nonzero conversion for signed LightPost post/author/relationship IDs;
5. additive `RankedFollowing` served type for in-network-only requests;
6. Demo readiness waiting for `Server ready`, plus fixed author/reply/video distribution tests.

Kafka catch-up, fallback behavior, and production readiness defects remain in the Thunder roadmap because they are not upstream drift from `e414c17`.

## 6. Grox recovery track

Upstream Grox starts as:

```text
grox/main.py::serve
  -> init context
  -> Engine process
       -> PlanMaster -> model/media/ASR tasks
  -> Dispatcher process
       -> stream generators -> task/result queues
  -> gRPC server
  -> coordinated shutdown
```

The current local entry is only:

```text
python -m grox.demo
  -> WorkItem -> neutral Plan DAG -> WorkResult -> JSON
```

This is not ordinary path drift. The upstream entry imports missing service, config, monitor, model, Prompt, media, queue, and storage modules. Work must remain staged:

1. `GRX-E1`: introduce upstream-named lifecycle ports and a single-process Engine shell around the tested neutral Plan.
2. `GRX-E2`: add Source/Sink acknowledgements, retry/idempotency, and queue contracts with in-memory fixtures.
3. `GRX-E3`: add Dispatcher and graceful shutdown tests.
4. `GRX-E4`: add a public gRPC contract only after WorkItem/WorkResult ownership is stable.
5. `GRX-E5`: restore each real classifier/embedder/summarizer plan only with legal model, Prompt, policy, and output fixtures.

P6-A does not satisfy any model-output acceptance criterion. P6-B remains `U3` until those dependencies exist.

## 7. Execution order

| Order | Work package | Why first | Required evidence |
|---:|---|---|---|
| 1 | `HM-E1` service composition | Establishes one stable application entry | Existing RPC tests + both Demo modes |
| 2 | `HM-E2` QueryBuilder | Establishes field and request-context ownership | table-driven mapping tests |
| 3 | `HM-E3` model/module paths | Removes repeated path and ID translation | compile + model conversion tests |
| 4 | `HM-E4` inner pipeline | Enables component-by-component upstream migration | component order + behavior tests |
| 5 | `HM-E5` outer pipeline | Reuses stable Query/model/service boundaries | 25 P4/P5 tests + final-feed Demo |
| 6 | `HM-E6` additive RPCs | Public surface follows stable internals | wire/contract/E2E tests |
| 7 | `PHX-E1/E2` consolidation | Keeps model core shared by offline/online entries | Python tests + parity fixture |
| 8 | `GRX-E1..E5` | Independent recovery with missing dependencies | Grox tests per stage |
| 9 | P3-B/P4-B/P5-B integrations | Requires external owners and environments | contract, auth, timeout, recovery tests |

## 8. Immediate next slice

Home Mixer `HM-E1..E6`, Phoenix `PHX-E1/E2`, and the unchanged Thunder dependency boundary are complete for portable/public contracts. The remaining tracks are contract-gated:

1. Grox `GRX-E1..E5` requires legal model, Prompt, policy, Source, Sink, queue, and lifecycle contracts before real classifier/embedder/summarizer behavior can be restored.
2. P3-B/P4-B/P5-B integrations require service owners, schema, authentication, timeout/error semantics, retention/privacy, test environments, and recovery responsibility.
3. Ads, WhoToFollow, Prompts, PushToHome, event sinks, and request cache remain default off until those conditions close. `47c1bcd:` VMRanker is no longer in this list — it has a real adapter and assembles behind `HOME_MIXER_ENABLE_VM_RANKER`, still default off.
4. Interface-first ports (maintenance rule 7) exist for TweetMixer, blocked-by, impression store reads, and seen-ids/served-candidates publishing: the remaining work for each is an adapter implementing the port plus an explicit assembly decision, not new component code.

Do not convert a disabled adapter, trait, or historical artifact validation into a production-complete claim.
