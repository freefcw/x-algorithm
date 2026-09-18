# Grox Standalone Runtime

This directory recovers the part of the upstream Grox system that can run without private models, prompts, schemas, storage, or messaging services: an eligibility-gated asynchronous task DAG.

## Run tests

```bash
uv run --project grox --group dev pytest -q grox/tests
```

## Stable boundary

- `WorkItem` is the JSON-compatible input envelope.
- `Plan` validates and runs task dependencies.
- `TaskOutcome` represents success, skip, or failure explicitly.
- `WorkResult` is the versionable result envelope.
- `Source` and `Sink` are ports for future ingestion and output adapters.

Production classifiers, model artifacts, prompt templates, Kafka/Strato/Manhattan adapters, generated Thrift schemas, media/ASR services, and the missing public service contract remain in the integration backlog documented in `docs/upstream-sync/p3b-p6-migration-goal.md`.
