# Phoenix production engine

`xrex/`, `crates/` and the local `python/` packages are the production training,
inference and protocol implementation. The old single-host JAX demo stack has
been removed; this directory no longer ships a fake model, fake corpus, or demo
HTTP service.

## Verification

```bash
uv sync --extra engine --dev
uv run pytest
uv run pytest tests/engine
```

Production entrypoints and configuration are defined by `xrex/driver/`,
`xrex/inference/`, and the Rust serving crates. They require real checkpoints,
feature/event inputs, and deployment configuration; missing configuration must
fail rather than start a random model.

## Retained contracts and tools

- `services/model_contract.py`, `services/inference_types.py`, and
  `services/recsys_proto.py`: transport-neutral artifact and protocol contracts.
- `scripts/build_training_inputs.py`: converts served-candidate and UAS JSONL
  events into parquet training inputs without synthetic data.
- `reference/mm_encoder.py`, `sid_assign.py`, `sid_codebook.py`, `sid_io.py`,
  `sid_index_server.py`, `repack_checkpoint.py`, and `train_step.py`: production
  multimodal/SID/checkpoint utilities.
- `tests/engine/` and the remaining root tests: production regression coverage.

## Production work still required

1. Adapt the home-mixer `.npz`/string-ID gateway contract to the xrex serving
   protocol (the parent deployment work owns gateway scaffolding).
2. Wire real feature-store/event consumers and checkpoint/index provisioning.
3. Provide production deployment configuration, readiness, observability, and
   rollback validation for the xrex servers.
