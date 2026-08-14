# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
import logging
import os

import jax
import jax.numpy as jnp
import numpy as np
import orbax.checkpoint as ocp
from jax.experimental import multihost_utils
from opentelemetry.trace import Tracer

from xrex.utils.tracer import MockTracer

logger = logging.getLogger(__name__)
rank_logger = logging.getLogger("rank")

_CHECKPOINTER = None


def get_checkpointer(timeout_secs=900):
    global _CHECKPOINTER
    if _CHECKPOINTER is None:
        _CHECKPOINTER = ocp.AsyncCheckpointer(
            ocp.PyTreeCheckpointHandler(use_zarr3=True), timeout_secs
        )
        if not hasattr(_CHECKPOINTER, "_post_finalization_callback"):
            raise RuntimeError("Orbax version is too old")
    return _CHECKPOINTER


class NoCompressionArrayHandler(ocp.type_handlers.ArrayHandler):
    def _get_json_tspec_write(self, *args, **kwargs):
        spec = super()._get_json_tspec_write(*args, **kwargs)
        for codec in spec["metadata"]["codecs"]:
            cfg = codec["configuration"]
            cfg["codecs"] = [c for c in cfg["codecs"] if c["name"] != "zstd"]
        return spec


def save_checkpoint(
    state,
    path,
    callback=None,
    tag=None,
    blocking=False,
    timeout_secs=900,
    chunked=True,
    compressed=True,
    chunk_byte_size: int = 1024 * 1024 * 4,
    tracer: Tracer | None = None,
):
    if not tracer:
        tracer = MockTracer()

    if tag is None:
        tag = "orbax-ckpt"
    os.makedirs(path, exist_ok=True)

    checkpointer = get_checkpointer(timeout_secs)

    with tracer.start_as_current_span("wait_for_previous_checkpoint"):
        try:
            checkpointer.wait_until_finished()
        except Exception as e:
            rank_logger.error("Error waiting for previous checkpoint: %s", e)

    dest = os.path.join(path, tag)
    if os.path.exists(dest):
        rank_logger.warning(
            "Checkpoint destination %s already exists; treating as a "
            "previously-committed save and skipping orbax.save (idempotent)",
            dest,
        )
        if callback and jax.process_index() == 0:
            callback()
        if blocking:
            multihost_utils.sync_global_devices("blocking-checkpoint")
        return

    if jax.devices()[0].platform == "cpu":
        mesh = state.step.sharding.mesh
        pspecs = jax.tree.map(lambda a: a.sharding.spec, state)
        state = multihost_utils.global_array_to_host_local_array(state, mesh, pspecs)
        state = jax.tree.map(lambda a: jnp.array(np.array(jax.device_get(a)).copy()), state)
        state = multihost_utils.host_local_array_to_global_array(state, mesh, pspecs)

    original_handler = ocp.type_handlers.get_type_handler(jax.Array)
    if not compressed:
        ocp.type_handlers.register_type_handler(
            jax.Array, NoCompressionArrayHandler(), override=True
        )

    def _callback():
        ocp.type_handlers.register_type_handler(jax.Array, original_handler, override=True)
        rank_logger.info("Finished writing checkpoint to %s", path)
        if not blocking and callback:
            callback()

    checkpointer._post_finalization_callback = _callback

    chunk_byte_size = chunk_byte_size if chunked else None
    with tracer.start_as_current_span(f"save_checkpoint_blocking_{blocking}"):
        save_args = jax.tree.map(
            lambda _: ocp.SaveArgs(chunk_byte_size=chunk_byte_size),
            state,
        )
        checkpointer.save(
            dest,
            args=ocp.args.PyTreeSave(
                state,
                save_args=save_args,
            ),
        )

        rank_logger.info("Started writing checkpoint to %s", path)

        if blocking:
            checkpointer.wait_until_finished()
            if callback and jax.process_index() == 0:
                callback()
            multihost_utils.sync_global_devices("blocking-checkpoint")


def wait_until_finished():
    get_checkpointer().wait_until_finished()
