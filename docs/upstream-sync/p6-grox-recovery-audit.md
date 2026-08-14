# P6 Grox Recovery Audit

> Upstream anchor: `47c1bcd` (2026-08-13). Rewritten for the restructured upstream tree;
> the `0bfc279`-era file map is preserved in §7 for reading older commit messages.

## Verdict

Upstream restructured Grox between `0bfc279` and `47c1bcd`: the flat
`plans/ tasks/ classifiers/ generators/ embedder/ summarizer/ schedules/` layout is gone,
replaced by `config/ core/ flows/ libs/` (165 files). The conclusion that mattered for P6-A
survives the restructure and is now easier to defend:

- The **only dependency-independent capability is still the task DAG**, now at
  `grox/core/plans/plan.py` and `grox/core/tasks/task.py`.
- Everything model-backed moved into `grox/flows/<domain>/`, where it is *more* entangled with
  unpublished infrastructure than before, not less.
- Upstream still ships **no dependency manifest** for Grox — no `pyproject.toml`,
  no requirements file, no lockfile. It cannot be installed as published.

P6-A therefore keeps its scope: restore orchestration against neutral contracts, claim nothing
about classifiers, safety policy, embeddings or summaries. What changes is the **vocabulary**
the local skeleton should converge on (§3) and the **shape of what is still missing** (§5).

Notable: upstream **deleted `summarizer/` outright** in this snapshot with no replacement, so the
"Summarization" row below is no longer a deferred capability — it is not an upstream capability at all.

## 1. Upstream structure at `47c1bcd`

| Directory | Files | Contents |
|---|---:|---|
| `grox/config/` | 1 | `config.py` only — Pydantic `GroxConfig(BaseSettings)` with YAML deep-merge |
| `grox/core/` | 44 | Framework: `engine.py`, `dispatcher.py`, `loading.py`, `registry.py`, `processors.py`, plus `plans/`, `tasks/`, `generators/`, `schedules/`, `data_loaders/`, `lm/`, `services/` |
| `grox/flows/` | 75 | Domain pipelines: `mm_emb/` (18), `ptos/` (21), `reply_spam/` (19), `upa/` (17) |
| `grox/libs/` | 45 | Vendored clients: `kafka_cli`, `grok_sampler`, `embed`, `wily_cli`, `blobstore_http`, `circuit_breaker`, `grpc_cli`, `html_render`, `video_tools`, `kerberos_cli` |

A "flow" is a domain package that registers `Plan` subclasses (`plan_*.py`) and
`StreamTaskGenerator` subclasses (`generators.py`) via a `@register` decorator from
`grox.core.registry`; `load_all()` recursively imports `grox.flows` so the decorators run.
Registration is discovery-based, replacing the old hardcoded `PlanMaster.ALL_PLANS` list.
Registered plan keys: `mm_emb_v5`, `mm_emb_v8_2` (+ `_for_reply` variants), `safety_ptos`
(+ 2 variants), `spam_comment`, `reply_ranking`, `coordinated_spam`, `post_safety`,
`banger_initial_screen`.

## 2. Capability disposition

| Capability | P6-A disposition | P6-B dependency |
|---|---|---|
| Engine / Dispatcher / task DAG | Re-expressed as standalone `Plan`, `Task`, `WorkItem`, `WorkResult` | Production multiprocessing, service lifecycle and durable acknowledgements |
| Eligibility / dependency skip | Migrated and tested | Plan-key configuration (upstream moved from `TaskEligibility` to `Plan.KEY`, see §3) |
| Failure envelope / cycle checks | Migrated and tested; cycle validation still fixes a real upstream gap (§4) | Retry and idempotency policy for external sinks |
| Kafka / message queue ingestion | Port only | Topics, auth, schemas and consumer ownership |
| Strato / Manhattan sinks | Port only | Queries, auth, schemas, retention and idempotency |
| Spam / initial banger | Not restored | Model, prompt, renderer, data and output contract |
| Post safety / PTOS | Not restored | Policy prompts, model, legal approval and annotation service |
| Reply ranking | Not restored | Model, prompt, conversation renderer and score sink |
| Multimodal embeddings (`mm_emb` v5 / v8.2) | Not restored | Embedding model/client, artifact contract and sink |
| Summarization | **Removed upstream** — no longer a deferred item | n/a |
| Media / ASR | Not restored | Media loaders/processors, ASR endpoint and privacy policy |
| Metrics / traces | Standard local result/log boundary only | Production metrics namespace and trace backend |

## 3. Vocabulary drift against the local skeleton

The local P6-A package was modelled on the old `plan.py`/`task.py`. Upstream kept `Plan` and
`Task` but renamed the data carriers and changed how eligibility works:

| Local skeleton | Upstream `47c1bcd` | Note |
|---|---|---|
| `Plan` | `Plan` (`core/plans/plan.py`) | Same role; `TASKS` + `TASK_DEPENDENCIES` unchanged |
| `Task` | `Task` (`core/tasks/task.py`) | Same role |
| `WorkItem` | `TaskPayload` (`core/schedules/types.py`) | Pydantic model carrying `post` / `user` / `plans` |
| `WorkResult` | `TaskResult` | Upstream slimmed it to `success` / `error`; per-task outputs moved to `TaskContext.state(T)` |
| `Source` | `TaskGenerator` / `StreamTaskGenerator` | Plus loaders under `core/data_loaders/` |
| `Sink` | *(no type)* | Upstream writes through sink **tasks** (`task_write_*_sink.py`) |
| eligibility set | `Plan.KEY` matched against `payload.plans` | Replaces the old `REQUIRED_ELIGIBILITY: TaskEligibility` enum |
| `PlanMaster.ALL_PLANS` | `@register` + `load_all()` | Registration is now import-side-effect discovery |

Renaming the local skeleton to `TaskPayload`/`TaskResult` is a mechanical change and would
reduce future review cost, but it is **not scheduled**: the skeleton has no upstream code
depending on it, and renaming buys nothing until a real flow is restored. Recorded here so the
divergence is a decision rather than an oversight.

## 4. Upstream risks the local skeleton still handles

These were identified against the old tree and re-checked against `47c1bcd`:

- **Cycle detection is still absent upstream.** `Plan` validates only that dependencies are a
  subset of `TASKS`; a dependency cycle still awaits futures forever. The local skeleton rejects
  cycles at construction time.
- No eligible plan returns `SKIPPED` locally instead of applying `min`/`max` to an empty result set.
- Tasks receive dependency outputs rather than mutating shared concurrent context. Upstream moved
  the other way, into typed `TaskContext.state(T)` scratch space — a different answer to the same
  problem, and one that only pays off with real flows.
- Retry is not automatic; future sink retries require an idempotency decision.

Upstream additions worth adopting **if** a real flow is ever restored: per-payload
`asyncio.wait_for(..., task_timeout)` in the Engine, and timeout/cancel mapped into a failed
`TaskResult` rather than an escaping exception.

## 5. What is still unavailable

### Missing `grox.*` modules (referenced by upstream code, absent from the tree)

- `grox.config.env` — supplies `grox_env`, `is_prod`, `is_mm_emb_prod`, `is_ptos_prod`;
  imported by `config.py` and every `disable_rules.py`
- `grox/config/yaml/config.{env}.yaml` — required by `GroxConfig`; not a module, but config load
  fails without it
- No `__init__.py` at `grox/`, `grox/flows/`, `grox/config/`, `grox/libs/`, and no entrypoint module

### Private packages (not published anywhere in the repo)

- `monitor` (`monitor.config`, `monitor.logging`, `monitor.metrics`)
- `strato_http` (`strato_http.queries.*`)
- `thrifts` (`thrifts.serdes`, generated ttypes)
- `protos` (`night_owl_search`, `abuse_cluster_anchor`, `sampler`)
- `grox_fetcher_client`
- `xai_sdk`

### Vendored but not installable as published

`grox/libs/*` ships source, but upstream code imports those packages as **top-level** names
(`from kafka_cli import ...`, not `from grox.libs.kafka_cli import ...`). Running upstream Grox
would require putting each `grox/libs/<pkg>` on `sys.path` as its own top-level package. There is
no manifest that does this.

Public packages (`pydantic`, `pydantic_settings`, `tenacity`, `limits`, `aiokafka`, `numpy`,
`httpx`, `openai`, `PIL`, `cv2`, `av`, `playwright`, `jinja2`, `json_repair`, …) are not added
pre-emptively; they enter `grox/pyproject.toml` only when an approved restored slice uses them.

## 6. P6-A implementation and evidence

The standalone package under `grox/` provides:

- `WorkItem`: JSON-compatible identity, eligibilities and attributes.
- `Task`: async, content-neutral execution port.
- `Plan`: dependency validation, eligibility gate, concurrent scheduling, dependency skip
  propagation and exception capture.
- `WorkResult`: stable success/skipped/failed result envelope.
- `Source` and `Sink`: future integration ports; `InMemorySink` for local evidence.
- `grox.demo`: deterministic normalize/measure workflow. Its output is metadata only and does not
  contain classifier, safety or embedding claims.

```bash
uv run --project grox --group dev pytest -q grox/tests
uv run --project grox python -m grox.demo \
  --input grox/examples/input.json \
  --output /tmp/grox-result.json
```

Current result (2026-08-14): 10 tests pass. The restructure does not affect them — the local
package is an isolated reimplementation and imports nothing from upstream Grox.

## 7. `0bfc279`-era file map (historical)

Kept so older commit messages and the capability inventory's 59-file Grox listing remain readable.
The old tree had `grox/plans/plan.py`, `grox/tasks/task.py`, `grox/classifiers/content/*`,
`grox/embedder/*`, `grox/summarizer/*`, `grox/generators/*`, `grox/schedules/*` and
`grox/data_loaders/*`, and referenced these then-missing modules: `grox.config.config`,
`grox.config.env`, `grox.data_loaders.{data_types,mappers.post_mapper,media_description_loader,media_loader,media_processor}`,
`grox.lm.{convo,post,post_v5,thread,user}`, `grox.prompts.template`,
`grox.classifiers.content.classifier_data_collection`, `grox.service`.
Most of those now exist under `grox/core/` — the tree grew from 59 to 165 files — but the two
blockers that actually matter (`grox.config.env` and the private packages in §5) did not change.

## 8. Resume conditions for P6-B

A model-backed capability may be resumed only when all of these are available:

1. Legal and technical access to the exact model or a named replacement.
2. Versioned input, prompt and output contracts.
3. A fixture set with expected results and failure cases.
4. A source and sink owner, authentication method, timeout/retry policy and idempotency key.
5. Privacy, retention and safety review for the data involved.
6. An independent local or staging acceptance command.

Until then, P6-B remains part of the unified Integration Backlog in `p3b-p6-migration-goal.md`.
