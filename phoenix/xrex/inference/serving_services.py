# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
from xrex.inference import service_registry
from xrex.inference.checkpoint_storage import (
    append_storage_overrides,
    detect_store,
    maybe_load_from_storage,
    resolve_checkpoint_arg,
)


def _select_serving_filters_runner(model_runner_cls: type, args) -> type:
    if args.service_type == "retrieval" and (args.enable_bloom_filter or args.enable_topic_filter):
        from xrex.inference.serving_filters_runner import FilteredRetrievalModelRunner

        return FilteredRetrievalModelRunner
    return model_runner_cls


service_registry.RUNNER_CLASS_HOOKS.append(_select_serving_filters_runner)
service_registry.CHECKPOINT_RESOLVERS.append(resolve_checkpoint_arg)
service_registry.STORAGE_OVERRIDE_HOOKS.append(append_storage_overrides)
service_registry.CHECKPOINT_STORE_DETECTORS.append(detect_store)
service_registry.CHECKPOINT_STORAGE_LOADERS.append(maybe_load_from_storage)

assert _select_serving_filters_runner in service_registry.RUNNER_CLASS_HOOKS, (
    "serving_services: candidate-filter runner hook not registered"
)
assert resolve_checkpoint_arg in service_registry.CHECKPOINT_RESOLVERS, (
    "serving_services: checkpoint-storage resolver not registered"
)
assert append_storage_overrides in service_registry.STORAGE_OVERRIDE_HOOKS, (
    "serving_services: storage override hook not registered"
)
assert detect_store in service_registry.CHECKPOINT_STORE_DETECTORS, (
    "serving_services: checkpoint store detector not registered"
)
assert maybe_load_from_storage in service_registry.CHECKPOINT_STORAGE_LOADERS, (
    "serving_services: checkpoint storage loader not registered"
)
