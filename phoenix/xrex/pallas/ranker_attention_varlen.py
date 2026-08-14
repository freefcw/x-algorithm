# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
import dataclasses
import functools
from typing import Any

import jax
import jax.numpy as jnp
from jax import lax
from jax.experimental import pallas as pl
from jax.experimental.pallas.triton import CompilerParams

DEFAULT_MASK_VALUE = -0.7 * float(jnp.finfo(jnp.dtype("float32")).max)


@dataclasses.dataclass(frozen=True)
class TuningConfig:
    block_q: int
    block_kv: int
    num_warps: int
    num_stages: int

    block_q_dq: int | None = None
    block_kv_dq: int | None = None
    block_q_dkv: int | None = None
    block_kv_dkv: int | None = None

    bwd_num_warps: int | None = None
    bwd_num_stages: int | None = None

    def __post_init__(self):
        backward_blocks = [self.block_q_dq, self.block_kv_dq, self.block_q_dkv, self.block_kv_dkv]
        block_is_set = [blk is not None for blk in backward_blocks]
        if any(block_is_set) and not all(block_is_set):
            raise ValueError(
                "Backward block sizes (block_q_dq, block_kv_dq, block_q_dkv, "
                "block_kv_dkv) must either all be specified or all be None."
            )


HOPPER_CONFIG = TuningConfig(
    block_q=128,
    block_kv=128,
    num_warps=8,
    num_stages=2,
    block_q_dq=128,
    block_kv_dq=16,
    block_q_dkv=16,
    block_kv_dkv=128,
    bwd_num_warps=8,
    bwd_num_stages=4,
)

BLACKWELL_CONFIG = TuningConfig(
    block_q=128,
    block_kv=64,
    num_warps=8,
    num_stages=2,
    block_q_dq=128,
    block_kv_dq=16,
    block_q_dkv=16,
    block_kv_dkv=64,
    bwd_num_warps=8,
    bwd_num_stages=4,
)


@functools.cache
def get_device_tuning_config() -> TuningConfig:
    from xrex.utils.gpu import GpuArch, gpu_arch

    return BLACKWELL_CONFIG if gpu_arch() in (GpuArch.GB200, GpuArch.GB300) else HOPPER_CONFIG


PADDING_SEGMENT_ID = 0


def tanh(x):
    return 2 * jax.lax.logistic(2 * x) - 1


def _gather_block_segments(
    segment_ids: jnp.ndarray,
    cu_seqlens: jnp.ndarray,
    max_seqlen: int,
    block_size: int,
) -> tuple[jnp.ndarray, jnp.ndarray]:
    seqlens = cu_seqlens[1:] - cu_seqlens[:-1]
    num_blocks = pl.cdiv(max_seqlen, block_size)
    block_offsets = jnp.arange(num_blocks) * block_size
    elem_offsets = jnp.arange(block_size)
    positions = block_offsets[:, None] + elem_offsets[None, :]
    idx = cu_seqlens[:-1, None, None] + positions[None, :, :]
    idx = idx.astype(jnp.int32)
    block_segments = jnp.take(segment_ids, idx, mode="clip")
    valid = positions[None, :, :] < seqlens[:, None, None]
    block_segments = jnp.where(valid, block_segments, PADDING_SEGMENT_ID)
    return block_segments, seqlens


def compute_block_state_metadata(
    segment_ids: jnp.ndarray,
    cu_seqlens: jnp.ndarray,
    max_seqlen: int,
    block_size: int,
) -> jnp.ndarray:
    block_segments, _ = _gather_block_segments(segment_ids, cu_seqlens, max_seqlen, block_size)
    has_history = jnp.any(block_segments > 0, axis=-1)
    has_candidate = jnp.any(block_segments < 0, axis=-1)
    has_padding = jnp.any(block_segments == PADDING_SEGMENT_ID, axis=-1)

    state_codes = (
        (has_padding.astype(jnp.int8) << 2)
        | (has_candidate.astype(jnp.int8) << 1)
        | has_history.astype(jnp.int8)
    )
    return state_codes


def compute_last_history_block_idx(
    segment_ids: jnp.ndarray,
    cu_seqlens: jnp.ndarray,
    max_seqlen: int,
    block_size: int,
) -> jnp.ndarray:
    block_segments, _ = _gather_block_segments(segment_ids, cu_seqlens, max_seqlen, block_size)
    num_blocks = block_segments.shape[1]
    has_history_tokens = jnp.any(block_segments > 0, axis=-1)
    last_history_block_idx = jnp.max(
        jnp.where(has_history_tokens, jnp.arange(num_blocks), -1), axis=-1
    )
    return last_history_block_idx[:, None]


def pack_inputs(
    q: jnp.ndarray,
    k: jnp.ndarray,
    v: jnp.ndarray,
    temp: jnp.ndarray,
    segment_ids: jnp.ndarray,
    fixed_total_len: int | None = None,
) -> tuple[jax.Array, jax.Array, jax.Array, jax.Array, jax.Array, jax.Array, jax.Array, int, int]:
    batch_size, seq_len, num_q_heads, head_dim = q.shape
    num_kv_heads = k.shape[2]
    total_len = batch_size * seq_len

    flat_segment_ids = segment_ids.reshape(total_len)
    valid = flat_segment_ids != PADDING_SEGMENT_ID

    flat_q = q.reshape(total_len, num_q_heads, head_dim)
    flat_k = k.reshape(total_len, num_kv_heads, head_dim)
    flat_v = v.reshape(total_len, num_kv_heads, head_dim)
    flat_temp = temp.reshape(total_len)

    q_packed = flat_q[valid]
    k_packed = flat_k[valid]
    v_packed = flat_v[valid]
    temp_packed = flat_temp[valid]
    segment_ids_packed = flat_segment_ids[valid]

    packed_len = int(q_packed.shape[0])
    if fixed_total_len is not None:
        if fixed_total_len < packed_len:
            raise ValueError(
                f"fixed_total_len={fixed_total_len} is smaller than packed length {packed_len}"
            )
        pad_len = fixed_total_len - packed_len
        if pad_len > 0:
            q_packed = jnp.pad(q_packed, ((0, pad_len), (0, 0), (0, 0)))
            k_packed = jnp.pad(k_packed, ((0, pad_len), (0, 0), (0, 0)))
            v_packed = jnp.pad(v_packed, ((0, pad_len), (0, 0), (0, 0)))
            temp_packed = jnp.pad(temp_packed, ((0, pad_len),), constant_values=0)
            segment_ids_packed = jnp.pad(
                segment_ids_packed, ((0, pad_len),), constant_values=PADDING_SEGMENT_ID
            )

    seqlens = jnp.sum(segment_ids != PADDING_SEGMENT_ID, axis=1).astype(jnp.int32)
    cu_seqlens = jnp.concatenate([jnp.array([0], dtype=seqlens.dtype), jnp.cumsum(seqlens)])
    max_seqlen = int(jnp.max(seqlens))

    return (
        q_packed,
        k_packed,
        v_packed,
        temp_packed,
        segment_ids_packed,
        cu_seqlens,
        cu_seqlens,
        max_seqlen,
        max_seqlen,
    )


def mha_forward_kernel(
    q_ref,
    k_ref,
    v_ref,
    temp_ref,
    segment_ref,
    cu_seqlens_q_ref,
    cu_seqlens_k_ref,
    block_state_metadata_ref,
    last_history_k_block_idx_ref,
    o_ref,
    *residual_refs,
    sm_scale: float,
    cap: float,
    cap_method: str,
    causal: bool,
    window_len: int,
    block_q: int,
    block_d: int,
    block_k: int,
):
    del window_len
    start_q = pl.program_id(0)
    batch_id = pl.program_id(1)

    m_i = jnp.full(block_q, -float("inf"), dtype=jnp.float32)
    l_i = jnp.zeros(block_q, dtype=jnp.float32)
    acc = jnp.zeros((block_q, block_d), dtype=jnp.float32)

    q_start = pl.load(cu_seqlens_q_ref, (batch_id,))
    q_end = pl.load(cu_seqlens_q_ref, (batch_id + 1,))
    k_start = pl.load(cu_seqlens_k_ref, (batch_id,))
    k_end = pl.load(cu_seqlens_k_ref, (batch_id + 1,))
    seqlen_q = q_end - q_start
    seqlen_k = k_end - k_start

    offset_q_rel = start_q * block_q
    offset_q = q_start + offset_q_rel

    span_q = offset_q_rel + jnp.arange(block_q)
    q_mask = span_q < seqlen_q

    q = pl.load(
        q_ref,
        (pl.dslice(offset_q, block_q), pl.dslice(None)),
        mask=q_mask[:, None],
        other=0.0,
    )

    seg_q = pl.load(
        segment_ref,
        (pl.dslice(offset_q, block_q),),
        mask=q_mask,
        other=PADDING_SEGMENT_ID,
    ).astype(jnp.int8)[:, None]
    seq_q_is_not_padding = jnp.logical_and(seg_q != PADDING_SEGMENT_ID, q_mask[:, None])

    def body(start_k, carry):
        acc, m_prev, l_prev = carry

        offset_k_rel = start_k * block_k
        offset_k = k_start + offset_k_rel
        span_k = offset_k_rel + jnp.arange(block_k)
        k_mask = span_k < seqlen_k

        k = pl.load(
            k_ref,
            (pl.dslice(offset_k, block_k), pl.dslice(None)),
            mask=k_mask[:, None],
            other=0.0,
        )
        seg_k = pl.load(
            segment_ref,
            (pl.dslice(offset_k, block_k),),
            mask=k_mask,
            other=PADDING_SEGMENT_ID,
        ).astype(jnp.int8)[None, :]

        qk = pl.dot(q, k, trans_b=True)
        if sm_scale != 1.0:
            qk *= sm_scale

        if cap > 0.0:
            if cap_method == "tanh":
                qk = cap * tanh(qk / cap)
            elif cap_method == "soft_sign":
                qk = qk / (1.0 + jnp.abs(qk) / cap)
            else:
                raise ValueError(f"cap_method must be in [tanh, soft_sign], got {cap_method}")

        mask = jnp.logical_or(seg_k > 0, span_q[:, None] == span_k[None, :])
        mask = jnp.logical_and(seq_q_is_not_padding, mask)
        mask = jnp.logical_and(mask, k_mask[None, :])

        if causal:
            causal_mask = span_q[:, None] >= span_k[None, :]
            mask = jnp.logical_and(mask, causal_mask)

        qk = jnp.where(mask, qk, DEFAULT_MASK_VALUE)
        max_logit = jnp.max(qk, axis=1)
        max_logit = jnp.maximum(max_logit, 1.0)
        m_curr = jnp.maximum(m_prev, max_logit)
        p = jnp.exp(qk - m_curr[:, None])
        alpha = jnp.exp(m_prev - m_curr)
        l_prev = l_prev * alpha + jnp.sum(p, axis=1)

        p = p.astype(q.dtype)
        v = pl.load(
            v_ref,
            (pl.dslice(offset_k, block_k), pl.dslice(None)),
            mask=k_mask[:, None],
            other=0.0,
        )
        acc = acc * alpha[:, None] + pl.dot(p, v)
        return acc, m_curr, l_prev

    q_block_state_metadata = jnp.squeeze(pl.load(block_state_metadata_ref, (start_q,)))

    last_history_k_block_idx = jnp.squeeze(pl.load(last_history_k_block_idx_ref, ()))
    num_k_blocks = lax.div(seqlen_k + block_k - 1, block_k)
    history_upper_bound = jnp.where(
        q_block_state_metadata == 4,
        0,
        last_history_k_block_idx + 1,
    )
    history_upper_bound = jnp.minimum(history_upper_bound, num_k_blocks)

    acc, m_i, l_i = lax.fori_loop(0, history_upper_bound, body, (acc, m_i, l_i))

    candidate_lower_bound = offset_q_rel // block_k
    candidate_lower_bound = jnp.maximum(history_upper_bound, candidate_lower_bound)

    candidate_upper_bound = (offset_q_rel + block_q - 1) // block_k + 1
    candidate_upper_bound = jnp.where(
        (q_block_state_metadata & 2) == 2,
        candidate_upper_bound,
        0,
    )
    candidate_upper_bound = jnp.minimum(candidate_upper_bound, num_k_blocks)

    acc, m_i, l_i = lax.fori_loop(
        candidate_lower_bound, candidate_upper_bound, body, (acc, m_i, l_i)
    )

    denom = l_i + (l_i == 0).astype(l_i.dtype)
    acc /= denom[:, None]

    if residual_refs:
        l_ref, m_ref = residual_refs
        pl.store(l_ref, (pl.ds(offset_q, block_q),), l_i, mask=q_mask)
        pl.store(m_ref, (pl.ds(offset_q, block_q),), m_i, mask=q_mask)
    acc = acc.astype(o_ref.dtype)
    store_mask = jnp.broadcast_to(q_mask[:, None], (block_q, block_d))
    pl.store(
        o_ref,
        (pl.dslice(offset_q, block_q), pl.dslice(None)),
        acc,
        mask=store_mask,
    )


@functools.partial(
    jax.custom_vjp,
    nondiff_argnums=[
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
    ],
)
@functools.partial(
    jax.jit,
    static_argnames=[
        "max_seqlen_q",
        "max_seqlen_k",
        "sm_scale",
        "cap",
        "cap_method",
        "causal",
        "window_len",
        "z_loss_weight",
        "config",
        "backward_pass_impl",
        "grid",
        "interpret",
        "debug",
    ],
)
def mha(
    q,
    k,
    v,
    temp,
    segment_ids,
    cu_seqlens_q,
    cu_seqlens_k,
    max_seqlen_q: int,
    max_seqlen_k: int,
    sm_scale: float = 1.0,
    cap: float = -1.0,
    cap_method: str = "tanh",
    causal: bool = True,
    window_len: int = -1,
    z_loss_weight: float = 0.0,
    config: TuningConfig = HOPPER_CONFIG,
    backward_pass_impl: str = "triton_split",
    grid=None,
    interpret: bool = False,
    debug: bool = False,
):
    block_q_fwd = config.block_q
    block_k_fwd = config.block_kv
    num_warps = config.num_warps
    num_stages = config.num_stages
    total_q, num_q_heads, head_dim = q.shape
    total_k = k.shape[0]
    num_kv_heads = k.shape[1]
    batch_size = cu_seqlens_q.shape[0] - 1

    assert total_q == segment_ids.shape[0], "segment_ids length must match q"
    assert total_k == segment_ids.shape[0], "segment_ids length must match k"
    assert temp.shape[0] == total_q, "temp length must match q"
    assert num_q_heads % num_kv_heads == 0, (
        f"num_q_heads ({num_q_heads}) must be divisible by num_kv_heads ({num_kv_heads})"
    )
    assert v.shape[1] == num_kv_heads, "k/v head count must match"
    assert k.shape[2] == head_dim and v.shape[2] == head_dim, "head dim must match"
    assert v.shape[0] == total_k, "k/v total length must match"
    q_heads_per_kv_head = num_q_heads // num_kv_heads

    block_q_fwd = min(block_q_fwd, max_seqlen_q)
    block_k_fwd = min(block_k_fwd, max_seqlen_k)

    num_q_blocks = pl.cdiv(max_seqlen_q, block_q_fwd)
    grid_ = grid
    if grid_ is None:
        grid_ = (num_q_blocks, batch_size, num_q_heads)

    num_warps_ = num_warps
    if num_warps_ is None:
        num_warps_ = 4 if head_dim <= 64 else 8

    q_block_state_metadata = compute_block_state_metadata(
        segment_ids, cu_seqlens_q, max_seqlen_q, block_q_fwd
    )
    last_history_k_block_idx = compute_last_history_block_idx(
        segment_ids, cu_seqlens_k, max_seqlen_k, block_k_fwd
    )

    kernel = functools.partial(
        mha_forward_kernel,
        sm_scale=sm_scale,
        cap=cap,
        cap_method=cap_method,
        block_q=block_q_fwd,
        block_k=block_k_fwd,
        block_d=head_dim,
        causal=causal,
        window_len=window_len,
    )
    out_shape = jax.ShapeDtypeStruct(shape=q.shape, dtype=q.dtype)
    return pl.pallas_call(
        kernel,
        grid=grid_,
        in_specs=[
            pl.BlockSpec(
                index_map=lambda _, __, k: (0, k, 0),
                block_shape=(total_q, None, head_dim),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0, k // q_heads_per_kv_head, 0),
                block_shape=(total_k, None, head_dim),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0, k // q_heads_per_kv_head, 0),
                block_shape=(total_k, None, head_dim),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0,),
                block_shape=(total_q,),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0,),
                block_shape=(segment_ids.shape[0],),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0,),
                block_shape=(cu_seqlens_q.shape[0],),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0,),
                block_shape=(cu_seqlens_k.shape[0],),
            ),
            pl.BlockSpec(
                index_map=lambda _, j, k: (j, 0),
                block_shape=(None, num_q_blocks),
            ),
            pl.BlockSpec(index_map=lambda _, j, k: (j, 0), block_shape=(None, 1)),
        ],
        out_specs=pl.BlockSpec(
            index_map=lambda _, __, k: (0, k, 0),
            block_shape=(total_q, None, head_dim),
        ),
        compiler_params=CompilerParams(num_warps=num_warps_, num_stages=num_stages),
        out_shape=out_shape,
        debug=debug,
        interpret=interpret,
        name="mha_forward_varlen",
    )(
        q,
        k,
        v,
        temp,
        segment_ids,
        cu_seqlens_q,
        cu_seqlens_k,
        q_block_state_metadata,
        last_history_k_block_idx,
    )


def _mha_forward(
    q,
    k,
    v,
    temp,
    segment_ids,
    cu_seqlens_q,
    cu_seqlens_k,
    max_seqlen_q: int,
    max_seqlen_k: int,
    sm_scale: float,
    cap: float,
    cap_method: str,
    causal: bool,
    window_len: int,
    z_loss_weight: float,
    config: TuningConfig,
    backward_pass_impl: str,
    grid: Any,
    interpret: bool,
    debug: bool,
):
    del backward_pass_impl
    block_q_fwd = config.block_q
    block_k_fwd = config.block_kv
    num_warps = config.num_warps
    num_stages = config.num_stages
    total_q, num_q_heads, head_dim = q.shape
    total_k = k.shape[0]
    num_kv_heads = k.shape[1]
    batch_size = cu_seqlens_q.shape[0] - 1
    q_heads_per_kv_head = num_q_heads // num_kv_heads
    used_q = cu_seqlens_q[-1]
    used_k = cu_seqlens_k[-1]
    valid_q = jnp.arange(total_q, dtype=jnp.int32) < used_q
    valid_k = jnp.arange(total_k, dtype=jnp.int32) < used_k

    q = jnp.where(valid_q[:, None, None], q, 0)
    k = jnp.where(valid_k[:, None, None], k, 0)
    v = jnp.where(valid_k[:, None, None], v, 0)
    temp = jnp.where(valid_q, temp, 0)
    segment_ids = jnp.where(valid_q, segment_ids, 0)

    block_q_fwd = min(block_q_fwd, max_seqlen_q)
    block_k_fwd = min(block_k_fwd, max_seqlen_k)
    num_q_blocks = pl.cdiv(max_seqlen_q, block_q_fwd)
    grid_ = grid
    if grid_ is None:
        grid_ = (num_q_blocks, batch_size, num_q_heads)

    num_warps_ = num_warps
    if num_warps_ is None:
        num_warps_ = 4 if head_dim <= 64 else 8

    q_block_state_metadata = compute_block_state_metadata(
        segment_ids, cu_seqlens_q, max_seqlen_q, block_q_fwd
    )
    last_history_k_block_idx = compute_last_history_block_idx(
        segment_ids, cu_seqlens_k, max_seqlen_k, block_k_fwd
    )

    kernel = functools.partial(
        mha_forward_kernel,
        sm_scale=sm_scale,
        cap=cap,
        cap_method=cap_method,
        causal=causal,
        window_len=window_len,
        block_q=block_q_fwd,
        block_k=block_k_fwd,
        block_d=head_dim,
    )
    out, l, m = pl.pallas_call(
        kernel,
        grid=grid_,
        in_specs=[
            pl.BlockSpec(
                index_map=lambda _, __, k: (0, k, 0),
                block_shape=(total_q, None, head_dim),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0, k // q_heads_per_kv_head, 0),
                block_shape=(total_k, None, head_dim),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0, k // q_heads_per_kv_head, 0),
                block_shape=(total_k, None, head_dim),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0,),
                block_shape=(total_q,),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0,),
                block_shape=(segment_ids.shape[0],),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0,),
                block_shape=(cu_seqlens_q.shape[0],),
            ),
            pl.BlockSpec(
                index_map=lambda _, __, k: (0,),
                block_shape=(cu_seqlens_k.shape[0],),
            ),
            pl.BlockSpec(index_map=lambda _, j, k: (j, 0), block_shape=(None, num_q_blocks)),
            pl.BlockSpec(index_map=lambda _, j, k: (j, 0), block_shape=(None, 1)),
        ],
        out_specs=[
            pl.BlockSpec(
                index_map=lambda _, __, k: (0, k, 0),
                block_shape=(total_q, None, head_dim),
            ),
            pl.BlockSpec(index_map=lambda _, __, k: (0, k), block_shape=(total_q, None)),
            pl.BlockSpec(index_map=lambda _, __, k: (0, k), block_shape=(total_q, None)),
        ],
        compiler_params=CompilerParams(num_warps=num_warps_, num_stages=num_stages),
        out_shape=[
            jax.ShapeDtypeStruct(shape=q.shape, dtype=q.dtype),
            jax.ShapeDtypeStruct(shape=(total_q, num_q_heads), dtype=jnp.float32),
            jax.ShapeDtypeStruct(shape=(total_q, num_q_heads), dtype=jnp.float32),
        ],
        debug=debug,
        interpret=interpret,
        name="mha_forward",
    )(
        q,
        k,
        v,
        temp,
        segment_ids,
        cu_seqlens_q,
        cu_seqlens_k,
        q_block_state_metadata,
        last_history_k_block_idx,
    )
    out = jnp.where(valid_q[:, None, None], out, 0)
    l = jnp.where(valid_q[:, None], l, 0)
    m = jnp.where(valid_q[:, None], m, 0)
    return out, (
        q,
        k,
        v,
        temp,
        segment_ids,
        cu_seqlens_q,
        cu_seqlens_k,
        out,
        l,
        m,
    )


def _preprocess_backward_packed(
    out: jnp.ndarray,
    do: jnp.ndarray,
    l: jnp.ndarray,
) -> tuple[jnp.ndarray, jnp.ndarray]:
    denom = l.astype(jnp.float32)
    safe_denom = jnp.where(denom == 0, 1.0, denom)
    do_scaled = do.astype(jnp.float32) / safe_denom[:, :, None]
    do_scaled = jnp.where(denom[:, :, None] == 0, 0.0, do_scaled)
    delta = jnp.sum(out.astype(jnp.float32) * do_scaled, axis=-1)
    return do_scaled.astype(do.dtype), delta.astype(jnp.float32)


def mha_backward_kernel_dq(
    q_ref,
    k_ref,
    v_ref,
    temp_ref,
    segment_ref,
    cu_seqlens_q_ref,
    cu_seqlens_k_ref,
    q_block_state_metadata_ref,
    last_history_k_block_idx_ref,
    out_ref,
    do_scaled_ref,
    l_ref,
    m_ref,
    delta_ref,
    dq_ref,
    *,
    sm_scale: float,
    cap: float,
    cap_method: str,
    causal: bool,
    window_len: int,
    z_loss_weight: float,
    block_q: int,
    block_d: int,
    block_k: int,
):
    del out_ref, window_len
    batch_id = pl.program_id(0)
    start_q = pl.program_id(2)

    q_start = pl.load(cu_seqlens_q_ref, (batch_id,))
    q_end = pl.load(cu_seqlens_q_ref, (batch_id + 1,))
    k_start = pl.load(cu_seqlens_k_ref, (batch_id,))
    k_end = pl.load(cu_seqlens_k_ref, (batch_id + 1,))
    seqlen_q = q_end - q_start
    seqlen_k = k_end - k_start

    offset_q_rel = start_q * block_q
    offset_q = q_start + offset_q_rel

    span_q = offset_q_rel + jnp.arange(block_q)
    q_mask = span_q < seqlen_q

    q = pl.load(
        q_ref,
        (pl.ds(offset_q, block_q), slice(None)),
        mask=q_mask[:, None],
        other=0.0,
    )
    m = pl.load(m_ref, (pl.ds(offset_q, block_q),), mask=q_mask, other=0.0)
    l = pl.load(l_ref, (pl.ds(offset_q, block_q),), mask=q_mask, other=0.0)
    do = pl.load(
        do_scaled_ref,
        (pl.ds(offset_q, block_q), slice(None)),
        mask=q_mask[:, None],
        other=0.0,
    )
    di = pl.load(delta_ref, (pl.ds(offset_q, block_q),), mask=q_mask, other=0.0)
    dq = jnp.zeros([block_q, block_d], dtype=jnp.float32)

    seg_q = pl.load(
        segment_ref,
        (pl.ds(offset_q, block_q),),
        mask=q_mask,
        other=PADDING_SEGMENT_ID,
    ).astype(jnp.int8)[:, None]
    seq_q_is_not_padding = jnp.logical_and(seg_q != PADDING_SEGMENT_ID, q_mask[:, None])

    def inner_loop(start_k, dq):
        offset_k_rel = start_k * block_k
        offset_k = k_start + offset_k_rel
        span_k = offset_k_rel + jnp.arange(block_k)
        k_mask = span_k < seqlen_k

        k = pl.load(
            k_ref,
            (pl.ds(offset_k, block_k), slice(None)),
            mask=k_mask[:, None],
            other=0.0,
        )
        v = pl.load(
            v_ref,
            (pl.ds(offset_k, block_k), slice(None)),
            mask=k_mask[:, None],
            other=0.0,
        )
        seg_k = pl.load(
            segment_ref,
            (pl.dslice(offset_k, block_k),),
            mask=k_mask,
            other=PADDING_SEGMENT_ID,
        ).astype(jnp.int8)[None, :]
        qk = pl.dot(q, k, trans_b=True)

        if sm_scale != 1.0:
            qk *= sm_scale

        if cap > 0.0:
            if cap_method == "tanh":
                qk_tanh = tanh(qk / cap)
                qk = cap * qk_tanh
            elif cap_method == "soft_sign":
                soft_sign = 1.0 / (1.0 + jnp.abs(qk) / cap)
                qk = qk * soft_sign
            else:
                raise ValueError("cap_method must be in [tanh, soft_sign]")

        mask = jnp.logical_or(seg_k > 0, span_q[:, None] == span_k[None, :])
        mask = jnp.logical_and(seq_q_is_not_padding, mask)
        mask = jnp.logical_and(mask, k_mask[None, :])
        if causal:
            causal_mask = span_q[:, None] >= span_k[None, :]
            mask = jnp.logical_and(mask, causal_mask)

        p = jnp.exp(qk - m[:, None])
        p = jnp.where(mask, p, 0.0)
        dp = pl.dot(do, v, trans_b=True).astype(jnp.float32) - di[:, None]
        ds = p * dp

        if z_loss_weight > 0:
            ds += z_loss_weight * p * ((jnp.log(l + 1e-12) + m) / l)[:, None]

        if cap > 0.0:
            if cap_method == "tanh":
                ds = ds * (1 - qk_tanh**2)
            elif cap_method == "soft_sign":
                ds = ds * soft_sign**2
            else:
                raise ValueError("cap_method must be in [tanh, soft_sign]")

        if sm_scale != 1.0:
            ds = ds * sm_scale

        dq = dq + pl.dot(ds.astype(k.dtype), k).astype(dq.dtype)
        return dq

    q_block_state_metadata = pl.load(q_block_state_metadata_ref, (start_q,))
    last_history_k_block_idx = pl.load(last_history_k_block_idx_ref, ())
    num_k_blocks = lax.div(seqlen_k + block_k - 1, block_k)

    history_upper_bound = jnp.squeeze(last_history_k_block_idx) + 1
    history_upper_bound = jnp.where(
        q_block_state_metadata == 4,
        0,
        history_upper_bound,
    )
    history_upper_bound = jnp.minimum(history_upper_bound, num_k_blocks)
    dq = lax.fori_loop(0, history_upper_bound, inner_loop, dq)

    candidate_lower_bound = lax.div(offset_q_rel, block_k)
    candidate_lower_bound = jnp.maximum(history_upper_bound, candidate_lower_bound)
    candidate_upper_bound = lax.div(offset_q_rel + block_q - 1, block_k) + 1
    candidate_upper_bound = jnp.where(
        (q_block_state_metadata & 2) == 2,
        candidate_upper_bound,
        0,
    )
    candidate_upper_bound = jnp.minimum(candidate_upper_bound, num_k_blocks)
    dq = lax.fori_loop(candidate_lower_bound, candidate_upper_bound, inner_loop, dq)

    dq_store_mask = jnp.broadcast_to(q_mask[:, None], (block_q, block_d))
    pl.store(
        dq_ref,
        (pl.ds(offset_q, block_q), slice(None)),
        dq.astype(q_ref.dtype),
        mask=dq_store_mask,
    )


def mha_backward_kernel_dkv(
    q_ref,
    k_ref,
    v_ref,
    temp_ref,
    segment_ref,
    cu_seqlens_q_ref,
    cu_seqlens_k_ref,
    block_state_metadata_ref,
    out_ref,
    do_scaled_ref,
    l_ref,
    m_ref,
    delta_ref,
    dk_ref,
    dv_ref,
    *,
    sm_scale: float,
    cap: float,
    cap_method: str,
    causal: bool,
    window_len: int,
    z_loss_weight: float,
    block_q: int,
    block_d: int,
    block_k: int,
):
    del out_ref, window_len
    batch_id = pl.program_id(0)
    start_k = pl.program_id(2)

    q_start = pl.load(cu_seqlens_q_ref, (batch_id,))
    q_end = pl.load(cu_seqlens_q_ref, (batch_id + 1,))
    k_start = pl.load(cu_seqlens_k_ref, (batch_id,))
    k_end = pl.load(cu_seqlens_k_ref, (batch_id + 1,))
    seqlen_q = q_end - q_start
    seqlen_k = k_end - k_start

    offset_k_rel = start_k * block_k
    offset_k = k_start + offset_k_rel

    dv = jnp.zeros([block_k, block_d], dtype=jnp.float32)
    dk = jnp.zeros([block_k, block_d], dtype=jnp.float32)
    span_k = offset_k_rel + jnp.arange(block_k)
    k_mask = span_k < seqlen_k

    k = pl.load(
        k_ref,
        (pl.ds(offset_k, block_k), slice(None)),
        mask=k_mask[:, None],
        other=0.0,
    )
    v = pl.load(
        v_ref,
        (pl.ds(offset_k, block_k), slice(None)),
        mask=k_mask[:, None],
        other=0.0,
    )

    seg_k = pl.load(
        segment_ref,
        (pl.ds(offset_k, block_k),),
        mask=k_mask,
        other=PADDING_SEGMENT_ID,
    ).astype(jnp.int8)[None, :]
    seg_k_is_history = seg_k > 0

    def inner_loop(start_q, carry):
        dv, dk = carry
        offset_q_rel = start_q * block_q
        offset_q = q_start + offset_q_rel
        span_q = offset_q_rel + jnp.arange(block_q)
        q_mask = span_q < seqlen_q

        q = pl.load(
            q_ref,
            (pl.ds(offset_q, block_q), slice(None)),
            mask=q_mask[:, None],
            other=0.0,
        )
        qk = pl.dot(q, k, trans_b=True)
        seg_q = pl.load(
            segment_ref,
            (pl.ds(offset_q, block_q),),
            mask=q_mask,
            other=PADDING_SEGMENT_ID,
        ).astype(jnp.int8)[:, None]
        seq_q_is_not_padding = jnp.logical_and(seg_q != PADDING_SEGMENT_ID, q_mask[:, None])

        m = pl.load(m_ref, (pl.ds(offset_q, block_q),), mask=q_mask, other=0.0)
        do = pl.load(
            do_scaled_ref,
            (pl.ds(offset_q, block_q), slice(None)),
            mask=q_mask[:, None],
            other=0.0,
        )
        di = pl.load(delta_ref, (pl.ds(offset_q, block_q),), mask=q_mask, other=0.0)

        if sm_scale != 1.0:
            qk *= sm_scale

        if cap > 0.0:
            if cap_method == "tanh":
                qk_tanh = tanh(qk / cap)
                qk = cap * qk_tanh
            elif cap_method == "soft_sign":
                soft_sign = 1.0 / (1.0 + jnp.abs(qk) / cap)
                qk = qk * soft_sign
            else:
                raise ValueError("cap_method must be in [tanh, soft_sign]")

        mask = jnp.logical_or(seg_k_is_history, span_q[:, None] == span_k[None, :])
        mask = jnp.logical_and(seq_q_is_not_padding, mask)
        mask = jnp.logical_and(mask, k_mask[None, :])
        if causal:
            causal_mask = span_q[:, None] >= span_k[None, :]
            mask = jnp.logical_and(mask, causal_mask)

        p = jnp.exp(qk - m[:, None])
        p = jnp.where(mask, p, 0.0)
        dot_result = pl.dot(p.astype(do.dtype), do, trans_a=True)
        dv = dv + dot_result.astype(dv.dtype)

        dp = pl.dot(do, v, trans_b=True) - di[:, None]

        ds = p * dp
        if z_loss_weight > 0:
            l = pl.load(l_ref, (pl.ds(offset_q, block_q),), mask=q_mask, other=0.0)
            ds += z_loss_weight * p * ((jnp.log(l + 1e-12) + m) / l)[:, None]
        if cap > 0.0:
            if cap_method == "tanh":
                ds = ds * (1 - qk_tanh**2)
            elif cap_method == "soft_sign":
                ds = ds * soft_sign**2
            else:
                raise ValueError("cap_method must be in [tanh, soft_sign]")
        if sm_scale != 1.0:
            ds = ds * sm_scale
        dk = dk + pl.dot(ds.astype(q_ref.dtype), q, trans_a=True).astype(dk.dtype)
        return dv, dk

    k_block_state_metadata = pl.load(block_state_metadata_ref, (start_k,))
    num_q_blocks = lax.div(seqlen_q + block_q - 1, block_q)

    lower_bound = jnp.where(
        (k_block_state_metadata & 1) == 1,
        0,
        offset_k_rel // block_q,
    )

    upper_bound = jnp.where(
        (k_block_state_metadata & 1) == 1,
        num_q_blocks,
        (offset_k_rel + block_k - 1) // block_q + 1,
    )
    upper_bound = jnp.minimum(upper_bound, num_q_blocks)
    dv, dk = lax.fori_loop(lower_bound, upper_bound, inner_loop, (dv, dk))

    kv_store_mask = jnp.broadcast_to(k_mask[:, None], (block_k, block_d))
    pl.store(
        dv_ref,
        (pl.ds(offset_k, block_k), slice(None)),
        dv.astype(dv_ref.dtype),
        mask=kv_store_mask,
    )
    pl.store(
        dk_ref,
        (pl.ds(offset_k, block_k), slice(None)),
        dk.astype(dk_ref.dtype),
        mask=kv_store_mask,
    )


def _mha_backward(
    max_seqlen_q: int,
    max_seqlen_k: int,
    sm_scale: float,
    cap: float,
    cap_method: str,
    causal: bool,
    window_len: int,
    z_loss_weight: float,
    config: TuningConfig,
    backward_pass_impl: str,
    grid: Any,
    interpret: bool,
    debug: bool,
    res,
    do,
):
    del grid
    assert config.block_q_dq is not None
    assert config.block_kv_dq is not None
    assert config.block_q_dkv is not None
    assert config.block_kv_dkv is not None
    assert config.bwd_num_warps is not None
    assert config.bwd_num_stages is not None

    block_q_bwd_dq = config.block_q_dq
    block_k_bwd_dq = config.block_kv_dq
    block_q_bwd_dkv = config.block_q_dkv
    block_k_bwd_dkv = config.block_kv_dkv
    q, k, v, temp, segment_ids, cu_seqlens_q, cu_seqlens_k, out, l, m = res

    total_q, num_q_heads, head_dim = q.shape
    total_k = k.shape[0]
    num_kv_heads = k.shape[1]
    q_heads_per_kv_head = num_q_heads // num_kv_heads
    batch_size = cu_seqlens_q.shape[0] - 1
    used_q = cu_seqlens_q[-1]
    used_k = cu_seqlens_k[-1]
    valid_q = jnp.arange(total_q, dtype=jnp.int32) < used_q
    valid_k = jnp.arange(total_k, dtype=jnp.int32) < used_k

    q = jnp.where(valid_q[:, None, None], q, 0)
    k = jnp.where(valid_k[:, None, None], k, 0)
    v = jnp.where(valid_k[:, None, None], v, 0)
    temp = jnp.where(valid_q, temp, 0)
    segment_ids = jnp.where(valid_q, segment_ids, 0)
    out = jnp.where(valid_q[:, None, None], out, 0)
    l = jnp.where(valid_q[:, None], l, 0)
    m = jnp.where(valid_q[:, None], m, 0)
    do = jnp.where(valid_q[:, None, None], do, 0)

    block_q_bwd_dq = min(block_q_bwd_dq, max_seqlen_q)
    block_k_bwd_dq = min(block_k_bwd_dq, max_seqlen_k)
    block_q_bwd_dkv = min(block_q_bwd_dkv, max_seqlen_q)
    block_k_bwd_dkv = min(block_k_bwd_dkv, max_seqlen_k)

    num_q_blocks_dq = pl.cdiv(max_seqlen_q, block_q_bwd_dq)
    num_k_blocks_dkv = pl.cdiv(max_seqlen_k, block_k_bwd_dkv)

    do_scaled, delta = _preprocess_backward_packed(out, do, l)

    if backward_pass_impl == "triton_split":
        dtemp = jnp.zeros(temp.shape, temp.dtype)
        dsegment = jnp.zeros(segment_ids.shape, q.dtype)

        num_warps = config.bwd_num_warps
        num_stages = config.bwd_num_stages

        grid_q = (batch_size, num_q_heads, num_q_blocks_dq)
        q_block_bwd_dq_state_metadata = compute_block_state_metadata(
            segment_ids, cu_seqlens_q, max_seqlen_q, block_q_bwd_dq
        )
        last_history_k_block_bwd_dq_idx = compute_last_history_block_idx(
            segment_ids, cu_seqlens_k, max_seqlen_k, block_k_bwd_dq
        )

        dq = pl.pallas_call(
            functools.partial(
                mha_backward_kernel_dq,
                block_q=block_q_bwd_dq,
                block_d=head_dim,
                block_k=block_k_bwd_dq,
                sm_scale=sm_scale,
                cap=cap,
                cap_method=cap_method,
                causal=causal,
                window_len=window_len,
                z_loss_weight=z_loss_weight,
            ),
            grid=grid_q,
            out_shape=jax.ShapeDtypeStruct(shape=q.shape, dtype=q.dtype),
            in_specs=[
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k, 0),
                    block_shape=(total_q, None, head_dim),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k // q_heads_per_kv_head, 0),
                    block_shape=(total_k, None, head_dim),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k // q_heads_per_kv_head, 0),
                    block_shape=(total_k, None, head_dim),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0,),
                    block_shape=(total_q,),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0,),
                    block_shape=(segment_ids.shape[0],),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0,),
                    block_shape=(cu_seqlens_q.shape[0],),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0,),
                    block_shape=(cu_seqlens_k.shape[0],),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (j, 0),
                    block_shape=(None, num_q_blocks_dq),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (j, 0),
                    block_shape=(None, 1),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k, 0),
                    block_shape=(total_q, None, head_dim),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k, 0),
                    block_shape=(total_q, None, head_dim),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k),
                    block_shape=(total_q, None),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k),
                    block_shape=(total_q, None),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k),
                    block_shape=(total_q, None),
                ),
            ],
            out_specs=pl.BlockSpec(
                index_map=lambda j, k, _: (0, k, 0),
                block_shape=(total_q, None, head_dim),
            ),
            name="mha_backward_q",
            debug=debug,
            interpret=interpret,
            compiler_params=CompilerParams(num_warps=num_warps, num_stages=num_stages),
        )(
            q,
            k,
            v,
            temp,
            segment_ids,
            cu_seqlens_q,
            cu_seqlens_k,
            q_block_bwd_dq_state_metadata,
            last_history_k_block_bwd_dq_idx,
            out,
            do_scaled,
            l,
            m,
            delta,
        )
        dq = jnp.where(valid_q[:, None, None], dq, 0)

        grid_kv = (batch_size, num_q_heads, num_k_blocks_dkv)
        k_block_bwd_dkv_state_metadata = compute_block_state_metadata(
            segment_ids, cu_seqlens_k, max_seqlen_k, block_k_bwd_dkv
        )
        out_shapes_kv = [
            jax.ShapeDtypeStruct((total_k, num_q_heads, head_dim), k.dtype),
            jax.ShapeDtypeStruct((total_k, num_q_heads, head_dim), v.dtype),
        ]
        dk, dv = pl.pallas_call(
            functools.partial(
                mha_backward_kernel_dkv,
                block_q=block_q_bwd_dkv,
                block_d=head_dim,
                block_k=block_k_bwd_dkv,
                sm_scale=sm_scale,
                cap=cap,
                cap_method=cap_method,
                causal=causal,
                window_len=window_len,
                z_loss_weight=z_loss_weight,
            ),
            grid=grid_kv,
            out_shape=out_shapes_kv,
            in_specs=[
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k, 0),
                    block_shape=(total_q, None, head_dim),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k // q_heads_per_kv_head, 0),
                    block_shape=(total_k, None, head_dim),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k // q_heads_per_kv_head, 0),
                    block_shape=(total_k, None, head_dim),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0,),
                    block_shape=(total_q,),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0,),
                    block_shape=(segment_ids.shape[0],),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0,),
                    block_shape=(cu_seqlens_q.shape[0],),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0,),
                    block_shape=(cu_seqlens_k.shape[0],),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (j, 0),
                    block_shape=(None, num_k_blocks_dkv),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k, 0),
                    block_shape=(total_q, None, head_dim),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k, 0),
                    block_shape=(total_q, None, head_dim),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k),
                    block_shape=(total_q, None),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k),
                    block_shape=(total_q, None),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k),
                    block_shape=(total_q, None),
                ),
            ],
            out_specs=[
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k, 0),
                    block_shape=(total_k, None, head_dim),
                ),
                pl.BlockSpec(
                    index_map=lambda j, k, _: (0, k, 0),
                    block_shape=(total_k, None, head_dim),
                ),
            ],
            name="mha_backward_kv",
            debug=debug,
            interpret=interpret,
            compiler_params=CompilerParams(num_warps=num_warps, num_stages=num_stages),
        )(
            q,
            k,
            v,
            temp,
            segment_ids,
            cu_seqlens_q,
            cu_seqlens_k,
            k_block_bwd_dkv_state_metadata,
            out,
            do_scaled,
            l,
            m,
            delta,
        )
        if q_heads_per_kv_head > 1:
            dk = dk.reshape(total_k, num_kv_heads, q_heads_per_kv_head, head_dim)
            dk = dk.astype(jnp.float32).sum(axis=2).astype(k.dtype)
            dv = dv.reshape(total_k, num_kv_heads, q_heads_per_kv_head, head_dim)
            dv = dv.astype(jnp.float32).sum(axis=2).astype(v.dtype)
        dk = jnp.where(valid_k[:, None, None], dk, 0)
        dv = jnp.where(valid_k[:, None, None], dv, 0)
    else:
        raise ValueError(f"Invalid backward pass implementation: {backward_pass_impl}")

    return dq.astype(q.dtype), dk, dv, dtemp, dsegment, None, None


mha.defvjp(_mha_forward, _mha_backward)
