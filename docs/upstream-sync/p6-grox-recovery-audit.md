# P6 Grox Recovery Audit

## Verdict

The upstream `main` branch contains 59 Grox Python files, but it does not contain a self-contained content-understanding service. The reusable, dependency-independent capability is the eligibility-gated task DAG in `grox/plans/plan.py` and the task result semantics in `grox/tasks/task.py`.

P6-A therefore restores only that orchestration behavior against neutral JSON contracts. It does not claim to restore classifiers, safety policy, embeddings, summaries, reply ranking, media processing, or production streams.

## Capability disposition

| Capability | P6-A disposition | P6-B dependency |
|---|---|---|
| Engine / Dispatcher / task DAG | Re-expressed as standalone `Plan`, `Task`, `WorkItem`, `WorkResult` | Production multiprocessing, service lifecycle and durable acknowledgements |
| Eligibility / dependency skip | Migrated and tested | Production eligibility configuration |
| Failure envelope / cycle checks | Migrated and tested; cycle validation fixes an upstream deadlock risk | Retry and idempotency policy for external sinks |
| Kafka / message queue ingestion | Port only | Topics, auth, schemas and consumer ownership |
| Strato / Manhattan sinks | Port only | Queries, auth, schemas, retention and idempotency |
| Spam / initial banger | Not restored | Model, prompt, renderer, data and output contract |
| Post safety / PTOS | Not restored | Policy prompts, model, legal approval and annotation service |
| Reply ranking | Not restored | Model, prompt, conversation renderer and score sink |
| V2/V5 embeddings | Not restored | Embedding model/client, artifact contract and sink |
| Summarization | Not restored | Model, prompt and media/text renderer |
| Media / ASR | Not restored | Media loaders/processors, ASR endpoint and privacy policy |
| Metrics / traces | Standard local result/log boundary only | Production metrics namespace and trace backend |

## Missing modules inside the upstream Grox namespace

The following imports are referenced by `main:grox/**` but the modules are absent from the Git tree:

- `grox.config.config`
- `grox.config.env`
- `grox.data_loaders.data_types`
- `grox.data_loaders.mappers.post_mapper`
- `grox.data_loaders.media_description_loader`
- `grox.data_loaders.media_loader`
- `grox.data_loaders.media_processor`
- `grox.lm.convo`
- `grox.lm.post`
- `grox.lm.post_v5`
- `grox.lm.thread`
- `grox.lm.user`
- `grox.prompts.template`
- `grox.classifiers.content.classifier_data_collection`
- `grox.service`

## Private package and runtime dependencies

- Model inference: `grok_sampler`
- Embedding inference: `embed`
- Internal observability: `monitor`
- Messaging: `kafka_cli`
- Internal storage/query: `strato_http.queries.*`
- Generated schemas/serialization: `thrifts.gen.twitter.*`, `thrifts.serdes`
- Runtime resources: model weights/endpoints, prompt templates, policy definitions, Kafka topics/auth, Strato/Manhattan APIs, Twitter Thrift schemas, media services, ASR service, production config and secrets

Public packages such as `pydantic`, `tenacity`, `aiohttp`, `aiokafka`, `numpy` and `json_repair` are not added pre-emptively. They should enter `grox/pyproject.toml` only when an approved restored slice actually uses them.

## P6-A implementation

The standalone package under `grox/` provides:

- `WorkItem`: JSON-compatible identity, eligibilities and attributes.
- `Task`: async, content-neutral execution port.
- `Plan`: dependency validation, eligibility gate, concurrent scheduling, dependency skip propagation and exception capture.
- `WorkResult`: stable success/skipped/failed result envelope.
- `Source` and `Sink`: future integration ports; `InMemorySink` for local evidence.
- `grox.demo`: deterministic normalize/measure workflow. Its output is metadata only and does not contain classifier, safety or embedding claims.

Upstream risks handled explicitly:

- No eligible plan returns `SKIPPED` instead of applying `min/max` to an empty result set.
- Dependency cycles are rejected during plan construction instead of waiting forever.
- Tasks receive dependency outputs rather than mutating shared concurrent context.
- Retry is not automatic; future sink retries require an idempotency decision.

## Evidence

```bash
uv run --project grox --group dev pytest -q grox/tests
uv run --project grox python -m grox.demo \
  --input grox/examples/input.json \
  --output /tmp/grox-result.json
```

Current result: 10 tests pass. The Demo emits a stable JSON `WorkResult` structure and deterministic normalized-text metadata; `started_at` and `finished_at` intentionally reflect each run's wall-clock time.

## Resume conditions for P6-B

A model-backed capability may be resumed only when all of these are available:

1. Legal and technical access to the exact model or a named replacement.
2. Versioned input, prompt and output contracts.
3. A fixture set with expected results and failure cases.
4. A source and sink owner, authentication method, timeout/retry policy and idempotency key.
5. Privacy, retention and safety review for the data involved.
6. An independent local or staging acceptance command.

Until then, P6-B remains part of the unified Integration Backlog in `p3b-p6-migration-goal.md`.
