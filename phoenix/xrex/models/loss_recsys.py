# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
import logging

import jax
import jax.numpy as jnp
import optax
from jax.lax import with_sharding_constraint
from jax.sharding import PartitionSpec as P

logger = logging.getLogger(__name__)
rank_logger = logging.getLogger("rank")


def multihot_loss_compute(
    logits: jax.Array,
    raw_targets: jax.Array,
    padding_mask: jax.Array,
    loss_mask: jax.Array,
    raw_weights: jax.Array | None = None,
    one_hot_targets_sharding=P(None),
):
    logits = logits.astype(jnp.float32)

    one_hot_targets = with_sharding_constraint(raw_targets, one_hot_targets_sharding)

    assert logits.shape == one_hot_targets.shape

    mask_3d = jnp.expand_dims(padding_mask, axis=-1) * loss_mask
    mask = padding_mask.astype(jnp.int32)

    bce_per_element = optax.sigmoid_binary_cross_entropy(
        logits, one_hot_targets.astype(logits.dtype)
    )

    masked_bce = bce_per_element * mask_3d

    if raw_weights is not None:
        masked_bce = masked_bce * jnp.expand_dims(raw_weights, axis=-1)
        weights = mask * raw_weights
    else:
        weights = mask

    cross_entropy_loss = jnp.sum(masked_bce) / (jnp.sum(weights) + 1e-10)

    return (
        cross_entropy_loss,
        mask,
    )


def continuous_loss_compute(
    gt_raw: jax.Array,
    pred_raw: jax.Array,
    valid_mask: jax.Array,
    negative_sample_mask: jax.Array,
    norm_scale: float,
    loss_type: str = "mse",
    mask_negatives: bool = True,
    raw_weights: jax.Array | None = None,
) -> tuple[jax.Array, jax.Array, jax.Array, jax.Array, jax.Array]:
    gt_raw = gt_raw.astype(jnp.float32)
    pred_raw = pred_raw.astype(jnp.float32)

    gt_clamped = jnp.clip(gt_raw, 0.0, norm_scale)
    gt_norm = gt_clamped / norm_scale
    pred_norm = pred_raw

    pred_in_original_units = pred_raw * norm_scale

    if mask_negatives:
        loss_mask = valid_mask & (~negative_sample_mask)
    else:
        loss_mask = valid_mask

    weights = loss_mask if raw_weights is None else loss_mask * raw_weights
    num_loss_samples = jnp.sum(weights)

    if loss_type == "mse":
        errors = (pred_norm - gt_norm) ** 2
    elif loss_type == "mae":
        errors = jnp.abs(pred_norm - gt_norm)
    elif loss_type == "huber":
        delta = 1.0
        abs_diff = jnp.abs(pred_norm - gt_norm)
        errors = jnp.where(abs_diff <= delta, 0.5 * abs_diff**2, delta * (abs_diff - 0.5 * delta))
    else:
        raise ValueError(f"Unknown loss_type: {loss_type}")

    loss = jnp.sum(errors * weights) / jnp.maximum(num_loss_samples, 1.0)

    return loss, gt_clamped, pred_in_original_units, loss_mask, errors


def tweedie_loss_compute(
    gt_raw: jax.Array,
    pred_raw: jax.Array,
    valid_mask: jax.Array,
    negative_sample_mask: jax.Array,
    p: float = 1.5,
    norm_scale: float = 300.0,
    mask_negatives: bool = True,
    raw_weights: jax.Array | None = None,
) -> tuple[jax.Array, jax.Array, jax.Array, jax.Array, jax.Array]:
    gt = jnp.clip(gt_raw.astype(jnp.float32), 0.0, norm_scale)
    pred = jnp.maximum(pred_raw.astype(jnp.float32), 1e-6)

    if abs(p - 1.0) < 1e-8:
        deviance = -gt * jnp.log(pred) + pred
    elif abs(p - 2.0) < 1e-8:
        deviance = gt / pred + jnp.log(pred)
    else:
        log_pred = jnp.log(pred)
        deviance = -gt * jnp.exp((1.0 - p) * log_pred) / (1.0 - p) + jnp.exp(
            (2.0 - p) * log_pred
        ) / (2.0 - p)

    if mask_negatives:
        loss_mask = valid_mask & (~negative_sample_mask)
    else:
        loss_mask = valid_mask

    weights = loss_mask if raw_weights is None else loss_mask * raw_weights
    num_loss_samples = jnp.sum(weights)
    loss = jnp.sum(deviance * weights) / jnp.maximum(num_loss_samples, 1.0)

    return loss, gt, pred, loss_mask, deviance
