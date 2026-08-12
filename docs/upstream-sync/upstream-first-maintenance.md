# Upstream-first maintenance policy

> Status: current-code / design rule
> Upstream common base: `aaa167b3de8a674587c53545a43c90eaad360010`
> Current upstream anchor: `0bfc2795d308f90032544322747caacd535f75ae` (equals `e414c17` plus the published-artifact LFS replacement; both are absorbed, see PHX-11)

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

A difference without one of these classifications is drift and should be removed or documented before more code is built on it.

## Candidate Pipeline anchor

The Candidate Pipeline is the compatibility layer for later Home Mixer components. Its portable API is now re-anchored to `e414c17`:

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

The current typed policy is `HomeMixerFeatures`; Phoenix MoE and request-cache writeback are its first default-off integrations.

## Sync procedure

For each later upstream snapshot, compare upstream changes before comparing final trees:

```bash
# What upstream changed after the currently anchored snapshot.
git diff --find-renames 0bfc2795d308f90032544322747caacd535f75ae..<new-upstream> -- <module>

# How the local implementation intentionally differs from the new anchor.
git diff --find-renames <new-upstream> -- <module>
```

Then migrate in this order:

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
4. **Completed:** additive `ForYouFeedQuery`/V2 and typed DebugScoredPosts RPCs preserve existing wire methods; URT/trace remain deferred pending public contracts (`HM-E6`).
5. **Completed:** Phoenix offline and published gRPC entries share artifact loading, preprocessing, model runners, and output mapping (`PHX-E1/PHX-E2`).
6. **Completed:** Thunder public gRPC boundary, checked ID conversion, timeout/exclusions, default port, readiness, and Demo fixtures are revalidated; Kafka remains outside this upstream diff.
7. **Contract-gated next:** P3-B/P4-B/P5-B external clients and SideEffects require schema, authentication, timeout, error, retention, and ownership decisions.
8. **Contract-gated next:** Grox model plans remain `U3` until model, Prompt, policy, Source, and Sink contracts can be independently verified.
