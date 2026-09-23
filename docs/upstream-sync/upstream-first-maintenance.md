# Upstream-first maintenance policy

> Status: current-code / design rule
> Upstream common base: `aaa167b3de8a674587c53545a43c90eaad360010`
> Absorbed upstream anchor: `1b3fec2` (P3 inventories [`49815da-capability-inventory.md`](./49815da-capability-inventory.md), [`75d93d9-capability-inventory.md`](./75d93d9-capability-inventory.md), [`fee1d0f-capability-inventory.md`](./fee1d0f-capability-inventory.md), [`6bb4594-capability-inventory.md`](./6bb4594-capability-inventory.md), [`2d4a03c-capability-inventory.md`](./2d4a03c-capability-inventory.md), [`20260918-capability-inventory.md`](./20260918-capability-inventory.md), [`20260924-capability-inventory.md`](./20260924-capability-inventory.md); portable/default code absorbed, external contracts remain U3)
> Tracked upstream head: `1b3fec2`
> Upstream head: `1b3fec2`
>
> Previous anchors: `9b0dc319691b76088266d0d2b48faf22d2b8a82a` (2026-09-04, [`../update/20260904.md`](../update/20260904.md), inventory [`9b0dc31-capability-inventory.md`](./9b0dc31-capability-inventory.md); its `experiment_overrides` U3 rationale is superseded — see [`902a06f-capability-inventory.md`](./902a06f-capability-inventory.md) section 4); `85ac72a1bba41f21615e3f0bca56da75970a6633` (2026-09-02, [`../update/20260903.md`](../update/20260903.md), inventory [`85ac72a-capability-inventory.md`](./85ac72a-capability-inventory.md)); `6384ca7d2c8570fbc645c20c3291730739ac00ce` (2026-09-01, [`../update/20260901.md`](../update/20260901.md)); `24c60942c5c5fdad3a6addffb4c6e6d2f228f04f` (2026-08-28, [`../update/20260828.md`](../update/20260828.md)); `45b48ba6baa40e212f6dcbaf8fe9fdc8d9da722e` (2026-08-26, [`../update/20260826.md`](../update/20260826.md)); `28e414f535e4b5a50ca12ee87674e7649e50c7ad` (2026-08-21, [`../update/20260823.md`](../update/20260823.md)); `d0cef2f943084ee0d4310378031c9c2c37d67f12` (2026-08-20, [`../update/20260820.md`](../update/20260820.md)); `aad7179773944e17eb8798bbbf0231d6cd6c1ffc` (2026-08-19, [`../update/20260819.md`](../update/20260819.md)); `11a71f87d6a7fc4c1e8159dad8f3c5ff90a0f7ed` (2026-08-18, no portable deltas, [`../update/20260818.md`](../update/20260818.md)); `b089ce64891f9c50fab73aa00dbe65acb82f198f` (2026-08-17, [`../update/20260817.md`](../update/20260817.md))
>
> Previous anchor: `c65aa179db7bdd61e2c2821eac87f208a105c053` (2026-08-14 semantic migration; portable changes landed and U3 lanes recorded in [`../update/20260814.md`](../update/20260814.md))
>
> The intermediate `a389166` only removed the `*.npz`/`*.zip` LFS rules. Local `.gitattributes` no longer keeps a `*.zip` LFS rule; the leftover Phoenix artifact pointer has been removed.

## Objective

Keep portable contracts, stage order, type names, and module boundaries close to upstream so that a later upstream change can be reviewed as a small semantic delta. The local repository remains independently buildable and must not import unavailable `xai_*`, Strato, Manhattan, model, Prompt, or deployment implementations into core business code.

Upstream-first does not mean copying every file. It means that a portable upstream component should compile against the same public contract, while environment-specific behavior is supplied by an adapter or recorded as deferred.

## Delta classes

Every intentional difference from upstream belongs to one class:

| Class | Meaning | Rule |
|---|---|---|
| `U0` | Portable upstream behavior | Preserve upstream file, type, method names, and stage semantics. |
| `U1` | Dependency substitution | Keep the upstream-facing contract; replace only metrics, config, cache, RPC, or storage implementation. |
| `U2` | Downstream extension | Make the extension additive. Do not rewrite an upstream method signature when a wrapper or adapter is sufficient. Add a regression test. |
| `U3` | Deferred external capability | Keep the capability in the inventory with its owner, missing contract, enablement state, and re-entry condition. Do not add a production-looking stub. |
| `U4` | Identity type substitution | Business identities are 96-bit ObjectIds that upstream `u64` cannot carry. Inside the pipeline every identity is `PostId` / `UserId` (a `Copy` newtype). When porting upstream code apply a mechanical substitution: `u64` in ID positions → `PostId` / `UserId`; proto3 sentinel `0` → `Option` / empty string; `to_be_bytes()` → `as_bytes()`; `wrapping_mul` bucketing → `to_u64_hash()`; Snowflake-derived time → hydrated `created_at_ms`. Keep `u64` unchanged for timestamps, counters, thresholds, request-local IDs, and hash arithmetic. |
| `U5` | Product not applicable | Concepts the product does not have (quote, retweet, subscription, …). Physically delete only the *dedicated* component files and remove them from assembly; keep the corresponding fields in `PostCandidate` / `PhoenixScores` / `UserFeatures` as `None` / empty; keep the related branches in shared filters as no-ops; set the matching action-head weights to 0; record later upstream changes to those dedicated files as "skipped" in the capability inventory. Do not re-create the files or register them in `mod.rs` / `PhoenixCandidatePipeline`. |

Fixed U5 skip paths (physically deleted; later upstream edits are inventory "skipped"):

| Path | Dedicated component |
|---|---|
| `home-mixer/candidate_hydrators/quote_hydrator.rs` | `QuoteHydrator` |
| `home-mixer/candidate_hydrators/subscription_hydrator.rs` | `SubscriptionHydrator` |
| `home-mixer/query_hydrators/subscribed_user_ids_query_hydrator.rs` | `SubscribedUserIdsQueryHydrator` |
| `home-mixer/filters/retweet_deduplication_filter.rs` | `RetweetDeduplicationFilter` |
| `home-mixer/filters/ineligible_subscription_filter.rs` | `IneligibleSubscriptionFilter` |
| `home-mixer/filters/ancillary_vf_filter.rs` | `AncillaryVFFilter` |

A difference without one of these classifications is drift and should be removed or documented before more code is built on it.

`U4` and `U5` belong to the historical trunk-convergence work; the plan document has been removed because it described deleted Demo and gateway paths. Current identity and protocol facts are defined by `home-mixer/models/ids.rs` and `proto/definitions/`; Thunder / VM Ranker integer protos remain behind zero-pad round-trip (`legacy-int-ids`) and are not deployed.

### Measured upstream-sync friction for `U4`

Measured over the 18 snapshots `c65aa17 → 902a06f`: each snapshot changed 0–19 `home-mixer` files and 0–4k lines; added/removed lines containing `u64` total 67 (0–16 per snapshot), about 0.7% of the total churn. The four pending snapshots `49815da → 6bb4594` contain 21 `u64` lines in total, all of the form `Vec<u64>` / `HashSet<u64>` / `HashMap<u64, …>` / `user_id: u64` parameters. This is the recurring cost of `U4`: a small mechanical substitution per snapshot, surfaced by the compiler once the pipeline types are `PostId` / `UserId`.

## Structural adoption ceilings

Two upstream facts cap how much of `home-mixer` can ever be adopted, independent of effort. Apply both when triaging a candidate file list, before estimating any work.

**`xai_candidate_pipeline::component_library` is not published.** 86 upstream `home-mixer` files import it. The open-sourced `candidate-pipeline/` crate contains no such module, and the name appears in no `Cargo.toml` — it is stripped from the open-source release, not merely undocumented. It supplies `component_library::clients::` (Kafka publisher, Phoenix prediction, Phoenix retrieval, Redis, …) and `component_library::utils::` (`is_prod`, `quality_factor`, `duration_since_creation_opt`, `client_utils`, …). Any upstream file importing it is `U3` by construction: the local equivalent must be a `U1` substitution written against a local port, never a port of the upstream file. This is the single largest cap on upstream adoption and the first filter to apply to any candidate list.

**A portable leaf utility is not automatically worth adopting.** `home-mixer/util/` leaf modules look zero-blocker — no private imports, self-contained, already tested — but the capability that consumes them may be blocked or absent, in which case adopting the leaf only adds unreachable code. Verified 2026-09-07:

| Upstream file | Only consumer | Local status |
|---|---|---|
| `util/rescore.rs` | none — `pub mod rescore;` in `util/mod.rs` is its only reference upstream too | dead upstream; do not adopt |
| `util/url.rs` | `util/urt/feedback.rs` | URT is contract-blocked (see §Next comparison order item 4) |
| `util/string_case.rs` | `update_served_history_side_effect.rs`, `util/urt/client_event.rs` | no local side effect; URT blocked |
| `util/tweet_type_metrics.rs` | `client_events_kafka_side_effect.rs` | no local side effect |
| `util/conversation_grouping.rs` | `invalid_conversation_module_filter.rs`, assembled only in `following_candidate_pipeline` | no local Following pipeline |

`conversation_grouping.rs` is not a capability gap: upstream's Phoenix pipeline uses `DedupConversationFilter`, the same filter the local Phoenix pipeline assembles. Only the Following timeline needs conversation-module semantics.

Triage a candidate file by its consumer's assembly path, not by its own import list. Adopting a leaf whose consumer is absent is the `U3` "no production-looking stub" rule violated one level down.

## Candidate Pipeline anchor

The Candidate Pipeline is the compatibility layer for later Home Mixer components. Its portable API was last re-tracked at the `c65aa17` semantic anchor (the most recent change to the crate, `candidate-pipeline: adopt 47c1bcd per-request stage summary and cached-hydrator extensions`); P3 snapshots include trait cleanups that are tracked in the per-commit inventories and are handled with the Home Mixer owner. Diff newer anchors against it with the sync procedure below before assuming the table is stale.

| Surface | Current rule | Delta class |
|---|---|---|
| `QueryHydrator` | `enable -> run -> hydrate -> update` | `U0`; local logging replaces private instrumentation (`U1`). |
| `Source` | Implement `source`; pipeline calls `run` | `U0`; public RPC clients are adapters (`U1`). |
| `Hydrator` / `CachedHydrator` | Return `Vec<Result<C, String>>`; preserve input cardinality and update only successful candidates | `U0`; in-memory cache substitutes the upstream cache backend (`U1`). |
| `Filter` | Implement synchronous `filter -> FilterResult`; pipeline order matches upstream | `U0`; `try_run` is an additive failure-isolation extension (`U2`). |
| `Scorer` | Return `Vec<Result<C, String>>`; preserve input cardinality and update only successful candidates | `U0`. |
| `Selector` | `run` wraps `select`; `SelectResult::len` reports selected count | `U0`. |
| `SideEffect` | Implement `side_effect`; pipeline calls non-blocking `run` wrapper | `U0`; local sinks are dependency substitutions (`U1`). |
| `PipelineQuery` | Uses `HasRequestId`; feature switches stay behind the local interface | Private `Params`/`Decider` replacement (`U1`). |
| Observability | Uses `log` and request IDs instead of `xai_stats_*` and private tracing macros | `U1`; stage/component/error cardinality must remain observable. |

Contract tests cover partial Hydrator/Scorer failure, cardinality mismatch, cache success-only writes, Source failure isolation, synchronous Filter behavior, selected/non-selected preservation, and SideEffect inputs.

## Optional integration gates

Unavailable or bypass dependencies follow a stricter rule than primary-path adapters:

1. The typed switch lives at process/service assembly, defaults to disabled, and is passed into component construction. Business components do not read environment variables.
2. Enabling a switch is an operator decision, not proof that integration is complete. Code and inventory must name the service owner, schema, authentication, timeout, fallback, test environment, retention, and recovery work still requiring manual support.
3. If a required endpoint or adapter is absent, an optional Source or SideEffect is not assembled (or remains disabled), a warning is emitted, and the primary recommendation path continues.
4. External SideEffect failures never block the response. Response-critical local in-memory state may remain synchronous as an explicit `U2` difference.
5. Demo adapters are selected separately and cannot be mistaken for production completion.
6. Do not create unused switches for capabilities with no public adapter. Keep them `U3` until the first executable contract exists.
7. **Interface-first rule (decision 2026-08-13):** a deferred `U3` capability may ship its domain-level port (trait) and the upstream-shaped component ahead of any executable contract, with unit tests against in-memory fakes. The component stays out of every assembly path until a real adapter passes acceptance (auth, timeout, error semantics, schema, retention where applicable). Ports must reuse existing domain types and must not invent wire schemas, fake data, or enable switches. This narrows future integrations to "implement the port + explicit assembly injection".

The current typed policy is `HomeMixerFeatures`; Phoenix MoE, request-cache writeback, VM Ranker, and demo-only Author Cold Start are explicit default-off integrations.

Ports defined ahead of contracts (rule 7): `TweetMixerClient` (SRC-05), `SocialGraphClientOps::check_blocked_by` (CH-09), `ImpressedPostsClient` (QH-05), `ImpressionBloomFilterClient` (QH-06), `SeenIdsPublisher` (SE-07), `ServedCandidatesSink` (SE-11). Each has an upstream-shaped component and fake-backed tests; none is assembled.

`VMRankerClient` (RANK-03) has graduated: `47c1bcd` open-sourced the service, so the port now has a real `GrpcVMRankerClient` adapter against the in-repo `vm-ranker` crate and is assembled when `HOME_MIXER_ENABLE_VM_RANKER=1` and `VM_RANKER_GRPC_ADDR` are both set. It is the worked example of rule 7 paying off: the integration was an adapter plus an assembly decision, with no component rewrite.

## Sync procedure

For each later upstream snapshot, compare upstream changes before comparing final trees:

```bash
# What upstream changed after the currently anchored snapshot.
git diff --find-renames 1b3fec2..<new-upstream> -- <module>

# How the local implementation intentionally differs from the new anchor.
git diff --find-renames <new-upstream> -- <module>
```

Then migrate in this order:

0. Re-read the local-patch registry in [`9b0dc31-capability-inventory.md`](./9b0dc31-capability-inventory.md) §4. A `U2` edit that lives *inside* an upstream function body is only surfaced by a diff when upstream happens to touch that same file; a second copy of the same patch in a file upstream left alone stays invisible. Check the registry by hand, not by waiting for the diff to warn you.
1. Inventory changed upstream contracts and stage order.
2. Mark each changed dependency as public, replaceable, or unavailable.
3. Port `U0` code with upstream names and file boundaries intact.
4. Put `U1` substitutions behind clients, stores, feature switches, metrics, or sink adapters.
5. Implement `U2` behavior as an additive wrapper and add a focused regression test.
6. Record unavailable behavior as `U3` in the capability inventory.
7. Run module tests, `cargo test --workspace`, and the relevant end-to-end Demo before advancing the anchor.

Do not use total changed lines as completion evidence. The acceptance unit is a behavior or contract with a test, an enablement state, and a known dependency boundary.

## Next comparison order

The concrete entry graphs, work packages, and acceptance sequence are maintained in [`entrypoint-migration-map.md`](./entrypoint-migration-map.md).

1. **Completed:** Home Mixer model and module layout now uses canonical upstream `models::{query,candidate,candidate_features}` paths; compatibility re-exports remain for downstream cleanup.
2. **Completed:** `PhoenixCandidatePipeline` portable Query Hydrator, Source, Hydrator, Filter, Scorer, and request-cache SideEffect boundaries now follow upstream names/order; unavailable services remain tagged `U3`.
3. **Completed:** outer ForYou server/pipeline paths, state Query Hydrators, disabled Source entries, Blender/Ads boundaries, and response stats SideEffect now match upstream (`HM-E5`).
4. **Completed:** additive `ForYouFeedQuery`/V2 and typed DebugScoredPosts RPCs preserve existing wire methods. URT/trace remain unmigrated and contract-blocked: `47c1bcd` open-sourced the `home-mixer/util/urt/` marshalling code, but all 17 files import `xai_urt_thrift`, which exists nowhere in the upstream tree and in no `Cargo.toml`. Migrating URT would mean inventing the wire schema locally, which `U3` forbids, so `HM-E6` is not a sizing question. Authoritative disposition: [`entrypoint-migration-map.md`](./entrypoint-migration-map.md) §4.
5. **Completed:** Phoenix offline and published gRPC entries share artifact loading, preprocessing, model runners, and output mapping (`PHX-E1/PHX-E2`).
6. **Completed:** Thunder public gRPC boundary, checked ID conversion, timeout/exclusions, default port, readiness, and Demo fixtures are revalidated; Kafka remains outside this upstream diff.
7. **Contract-gated next:** P3-B/P4-B/P5-B external clients and SideEffects require schema, authentication, timeout, error, retention, and ownership decisions.
8. **Contract-gated next:** Grox model plans remain `U3` until model, Prompt, policy, Source, and Sink contracts can be independently verified.
