# 精排模型训练脚本
#
# 用法：
#   uv run train_ranker.py                        # 用模拟数据跑通训练循环
#   uv run train_ranker.py --data-dir ./my_data   # 用真实 Parquet 数据训练
#
# 依赖：
#   optax 已在 pyproject 主依赖中，uv sync 即可

import argparse
import json
import logging
import math
import os
import re
from pathlib import Path
from typing import Any

import _setup_path  # noqa: F401
import haiku as hk
import jax
import jax.numpy as jnp
import numpy as np

from grok import TransformerConfig
from recsys_model import HashConfig, PhoenixModelConfig, RecsysBatch, RecsysEmbeddings
from runners import ACTIONS, create_example_batch
from services.model_contract import ACTION_IDX_TO_ENUM, FEATURE_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("train")

# ── 超参（与推理脚本保持一致）────────────────────────────────────────────────
EMB_SIZE = 128
HISTORY_LEN = 32
NUM_CANDIDATES = 8
NUM_ACTIONS = len(ACTIONS)
TABLE_SIZE = 100_000
SURFACE_VOCAB = 16
NUM_HASHES = 2


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def normalize_dwell(seconds: float, max_seconds: float = 300.0) -> float:
    """对数归一化停留时长到 [0, 1]。"""
    return min(math.log1p(seconds) / math.log1p(max_seconds), 1.0)


def flatten_dict(d: dict, parent_key: str = "", sep: str = "/") -> dict:
    """把嵌套 dict 展平为单层，key 用 sep 拼接。"""
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep))
        else:
            items[new_key] = np.array(v)
    return items


# ── 嵌入表管理 ────────────────────────────────────────────────────────────────

def init_embedding_tables(seed: int = 0):
    """随机初始化三张嵌入表，第 0 行保留为 padding（全零）。

    标准差取 1/sqrt(EMB_SIZE)，与模型内 proj_mat 的 VarianceScaling(fan_out) 初始化匹配；
    N(0,1) 会让 128 维向量模长 ≈ 11，一进模型就饱和。
    """
    rng = np.random.default_rng(seed)
    std = 1.0 / math.sqrt(EMB_SIZE)
    user_emb   = rng.normal(scale=std, size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
    post_emb   = rng.normal(scale=std, size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
    author_emb = rng.normal(scale=std, size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
    user_emb[0] = post_emb[0] = author_emb[0] = 0.0
    return user_emb, post_emb, author_emb


# ── 嵌入表稀疏更新（row-wise Adagrad）───────────────────────────────────────────
#
# 嵌入表不是 haiku 参数：查表在模型外完成，模型只吃向量。因此梯度先落在查出来的
# RecsysEmbeddings 上，再按哈希下标 scatter 回表。每行维护一个 Adagrad 累加器，
# 步长不依赖 batch 均值缩放，也不用给整张表存 Adam 状态。

EMB_ACC_INIT = 0.1


def init_embedding_state(user_emb, post_emb, author_emb) -> dict:
    tables = {"user": user_emb, "post": post_emb, "author": author_emb}
    return {
        name: {
            "table": jnp.asarray(table),
            "acc": jnp.full((table.shape[0],), EMB_ACC_INIT, dtype=jnp.float32),
        }
        for name, table in tables.items()
    }


def lookup_embeddings_jax(emb_state: dict, batch: RecsysBatch) -> RecsysEmbeddings:
    user, post, author = (emb_state[k]["table"] for k in ("user", "post", "author"))
    return RecsysEmbeddings(
        user_embeddings=user[batch.user_hashes],
        history_post_embeddings=post[batch.history_post_hashes],
        candidate_post_embeddings=post[batch.candidate_post_hashes],
        history_author_embeddings=author[batch.history_author_hashes],
        candidate_author_embeddings=author[batch.candidate_author_hashes],
    )


def _sparse_adagrad(entry: dict, indices, grads, lr: float) -> dict:
    """把若干组 (下标, 梯度) 合并后做一次 row-wise Adagrad 更新。第 0 行（padding）保持全零。"""
    idx = jnp.concatenate([jnp.reshape(i, (-1,)) for i in indices])
    grad = jnp.concatenate([jnp.reshape(g, (-1, g.shape[-1])) for g in grads])
    acc = entry["acc"].at[idx].add(jnp.mean(jnp.square(grad), axis=-1))
    step = grad / (jnp.sqrt(acc[idx]) + 1e-8)[:, None]
    table = entry["table"].at[idx].add(-lr * step)
    table = table.at[0].set(0.0)
    return {"table": table, "acc": acc}


def apply_embedding_grads(
    emb_state: dict, batch: RecsysBatch, grads: RecsysEmbeddings, lr: float
) -> dict:
    return {
        "user": _sparse_adagrad(
            emb_state["user"], [batch.user_hashes], [grads.user_embeddings], lr
        ),
        "post": _sparse_adagrad(
            emb_state["post"],
            [batch.history_post_hashes, batch.candidate_post_hashes],
            [grads.history_post_embeddings, grads.candidate_post_embeddings],
            lr,
        ),
        "author": _sparse_adagrad(
            emb_state["author"],
            [batch.history_author_hashes, batch.candidate_author_hashes],
            [grads.history_author_embeddings, grads.candidate_author_embeddings],
            lr,
        ),
    }


def embedding_tables_to_numpy(emb_state: dict):
    return tuple(np.asarray(emb_state[k]["table"]) for k in ("user", "post", "author"))


# ── 观测头掩码 ────────────────────────────────────────────────────────────────

def action_index(name: str) -> int:
    """接受 `favorite` 或 `favorite_score` 两种写法。"""
    for candidate in (name, f"{name}_score"):
        if candidate in ACTIONS:
            return ACTIONS.index(candidate)
    raise ValueError(f"未知行为 {name!r}，可选：{', '.join(ACTIONS)}")


def resolve_head_mask(observed_actions: str | None) -> np.ndarray:
    """返回 [NUM_ACTIONS] 的 0/1 掩码：哪些头参与损失与评估。

    未指定时全部 19 个头参与（含 dwell_time 回归）。业务日志只采集了部分行为时必须显式
    指定，否则大量全零标签会稀释真实信号，线上也不该把这些未观测头当成有效预测。
    """
    mask = np.zeros((NUM_ACTIONS,), dtype=np.float32)
    if not observed_actions:
        mask[:] = 1.0
        return mask
    for name in observed_actions.split(","):
        name = name.strip()
        if name:
            mask[action_index(name)] = 1.0
    if mask.sum() == 0:
        raise ValueError("--observed-actions 不能为空")
    return mask


def observed_action_names(head_mask: np.ndarray) -> list[str]:
    return [ACTIONS[i] for i in range(NUM_ACTIONS) if head_mask[i] > 0]


def supported_action_enums(head_mask: np.ndarray) -> list[int]:
    """观测到的离散头对应的 phoenix_recsys.proto ActionName 枚举值（dwell_time 不是离散头）。"""
    return sorted(ACTION_IDX_TO_ENUM[i] for i in range(len(ACTION_IDX_TO_ENUM)) if head_mask[i] > 0)


def load_embedding_tables(path: str):
    """从 npz 文件加载嵌入表。"""
    with np.load(path, allow_pickle=False) as tables:
        return tables["user_emb_table"], tables["post_emb_table"], tables["author_emb_table"]


def save_embedding_state(path: str, emb_state: dict) -> None:
    """保存 embedding table 及其 Adagrad 累加器，保证 resume 后状态连续。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    np.savez(
        path,
        user_emb_table=np.asarray(emb_state["user"]["table"]),
        post_emb_table=np.asarray(emb_state["post"]["table"]),
        author_emb_table=np.asarray(emb_state["author"]["table"]),
        user_emb_acc=np.asarray(emb_state["user"]["acc"]),
        post_emb_acc=np.asarray(emb_state["post"]["acc"]),
        author_emb_acc=np.asarray(emb_state["author"]["acc"]),
    )
    logger.info(f"嵌入表及优化器状态已保存到 {path}")


def load_embedding_state(path: str) -> dict:
    """加载 embedding table；旧格式没有累加器时从初始值开始。"""
    tables = load_embedding_tables(path)
    state = init_embedding_state(*tables)
    with np.load(path, allow_pickle=False) as raw:
        acc_names = ("user_emb_acc", "post_emb_acc", "author_emb_acc")
        state_names = ("user", "post", "author")
        if all(name in raw.files for name in acc_names):
            for state_name, acc_name in zip(state_names, acc_names):
                state[state_name]["acc"] = jnp.asarray(raw[acc_name])
    return state


def save_optimizer_state(path: str, opt_state) -> None:
    """按当前 PyTree 叶子顺序保存 Optax 状态，不使用不安全的 pickle。"""
    leaves, _ = jax.tree_util.tree_flatten(opt_state)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    arrays: dict[str, Any] = {
        f"leaf_{index}": np.asarray(leaf) for index, leaf in enumerate(leaves)
    }
    np.savez(path, **arrays)


def load_optimizer_state(path: str, template):
    """按 template 的 PyTree 结构恢复 Optax 状态，并校验叶子数量和 shape。"""
    template_leaves, treedef = jax.tree_util.tree_flatten(template)
    with np.load(path, allow_pickle=False) as raw:
        names = sorted(raw.files, key=lambda name: int(name.removeprefix("leaf_")))
        if len(names) != len(template_leaves):
            raise ValueError(f"优化器状态叶子数量不匹配：{len(names)} != {len(template_leaves)}")
        leaves = []
        for name, template_leaf in zip(names, template_leaves):
            value = raw[name]
            if value.shape != template_leaf.shape:
                raise ValueError(f"优化器状态 shape 不匹配：{name}: {value.shape} != {template_leaf.shape}")
            leaves.append(jnp.asarray(value, dtype=template_leaf.dtype))
    return jax.tree_util.tree_unflatten(treedef, leaves)


def save_embedding_tables(path: str, user_emb, post_emb, author_emb):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    np.savez(path, user_emb_table=user_emb, post_emb_table=post_emb, author_emb_table=author_emb)
    logger.info(f"嵌入表已保存到 {path}")


def lookup_embeddings(
    batch: RecsysBatch,
    user_emb: np.ndarray,
    post_emb: np.ndarray,
    author_emb: np.ndarray,
) -> RecsysEmbeddings:
    """根据 batch 中的哈希值查表，返回 RecsysEmbeddings。"""
    u_idx  = np.asarray(batch.user_hashes,              dtype=np.intp)
    hp_idx = np.asarray(batch.history_post_hashes,      dtype=np.intp)
    cp_idx = np.asarray(batch.candidate_post_hashes,    dtype=np.intp)
    ha_idx = np.asarray(batch.history_author_hashes,    dtype=np.intp)
    ca_idx = np.asarray(batch.candidate_author_hashes,  dtype=np.intp)
    return RecsysEmbeddings(
        user_embeddings=user_emb[u_idx],
        history_post_embeddings=post_emb[hp_idx],
        candidate_post_embeddings=post_emb[cp_idx],
        history_author_embeddings=author_emb[ha_idx],
        candidate_author_embeddings=author_emb[ca_idx],
    )


# ── 模型配置 ──────────────────────────────────────────────────────────────────

def make_model_config() -> PhoenixModelConfig:
    return PhoenixModelConfig(
        emb_size=EMB_SIZE,
        num_actions=NUM_ACTIONS,
        history_seq_len=HISTORY_LEN,
        candidate_seq_len=NUM_CANDIDATES,
        hash_config=HashConfig(
            num_user_hashes=NUM_HASHES,
            num_item_hashes=NUM_HASHES,
            num_author_hashes=NUM_HASHES,
        ),
        product_surface_vocab_size=SURFACE_VOCAB,
        model=TransformerConfig(
            emb_size=EMB_SIZE,
            widening_factor=2,
            key_size=64,
            num_q_heads=2,
            num_kv_heads=2,
            num_layers=2,
            attn_output_multiplier=0.125,
        ),
    )


# ── 损失函数 ──────────────────────────────────────────────────────────────────

def candidate_mask(batch: RecsysBatch) -> jax.Array:
    """[B, C] 1.0 = 真实候选，0.0 = padding（第一个哈希为 0）。"""
    return (jnp.asarray(batch.candidate_post_hashes)[:, :, 0] != 0).astype(jnp.float32)


def forward_logits(batch: RecsysBatch, embeddings: RecsysEmbeddings) -> jax.Array:
    model_config = make_model_config()
    model_config.initialize()
    model_config.fprop_dtype = jnp.float32  # 训练时用 float32 保证梯度精度
    model = model_config.make()
    return model(batch, embeddings).logits.astype(jnp.float32)  # [B, C, num_actions]


def loss_fn(
    batch: RecsysBatch,
    embeddings: RecsysEmbeddings,
    labels: jax.Array,
    head_mask: jax.Array,
) -> jax.Array:
    """
    多目标二元交叉熵损失。

    labels: [B, C, num_actions] float32
        - 索引 0~17：0/1 二分类行为
        - 索引 18（dwell_time）：连续值，用 MSE 单独计算
    head_mask: [num_actions]，为 0 的头不参与损失
    padding 候选（哈希为 0）不参与损失。
    """
    logits = forward_logits(batch, embeddings)
    valid = candidate_mask(batch)  # [B, C]
    denom = jnp.maximum(jnp.sum(valid), 1.0)

    # 前 18 个行为：二元交叉熵
    bce_logits = logits[:, :, :18]
    bce_labels = labels[:, :, :18]
    bce = jax.nn.softplus(bce_logits) - bce_labels * bce_logits
    bce_loss = jnp.sum(jnp.sum(bce * head_mask[:18], axis=-1) * valid) / denom

    # 第 19 个（dwell_time）：MSE
    dwell_pred = jax.nn.sigmoid(logits[:, :, 18])
    dwell_label = labels[:, :, 18]
    dwell_loss = jnp.sum((dwell_pred - dwell_label) ** 2 * valid) / denom * head_mask[18]

    return bce_loss + dwell_loss


# ── 数据加载 ──────────────────────────────────────────────────────────────────

def make_simulated_batch(batch_size: int) -> tuple[RecsysBatch, np.ndarray]:
    """
    生成模拟训练 batch（不需要真实数据）。仅在 init/dummy 阶段使用。
    主训练循环应使用向量化的 `make_simulated_batch_fast`。
    """
    batch, _ = create_example_batch(
        batch_size=batch_size,
        emb_size=EMB_SIZE,
        history_len=HISTORY_LEN,
        num_candidates=NUM_CANDIDATES,
        num_actions=NUM_ACTIONS,
        num_user_hashes=NUM_HASHES,
        num_item_hashes=NUM_HASHES,
        num_author_hashes=NUM_HASHES,
        product_surface_vocab_size=SURFACE_VOCAB,
    )
    rng = np.random.default_rng()
    # 模拟稀疏标签：大部分行为不发生（概率 0.1 触发）
    labels = (rng.random(size=(batch_size, NUM_CANDIDATES, NUM_ACTIONS)) < 0.1).astype(np.float32)
    # dwell_time（索引 18）用归一化连续值
    labels[:, :, 18] = rng.random(size=(batch_size, NUM_CANDIDATES)).astype(np.float32)
    return batch, labels


def make_simulated_batch_fast(
    batch_size: int, rng: np.random.Generator
) -> tuple[RecsysBatch, np.ndarray]:
    """向量化版模拟 batch，去除 create_example_batch 里的 Python for 循环。

    主训练循环每步都调用；实测单步构造时间比原版下降一个数量级以上。
    """
    B = batch_size
    user_hashes = rng.integers(1, TABLE_SIZE, size=(B, NUM_HASHES), dtype=np.int32)
    history_post_hashes = rng.integers(
        1, TABLE_SIZE, size=(B, HISTORY_LEN, NUM_HASHES), dtype=np.int32
    )
    history_author_hashes = rng.integers(
        1, TABLE_SIZE, size=(B, HISTORY_LEN, NUM_HASHES), dtype=np.int32
    )

    # 向量化随机截断历史（valid_len 在 [HISTORY_LEN//2, HISTORY_LEN] 内均匀）
    valid_len_post = rng.integers(HISTORY_LEN // 2, HISTORY_LEN + 1, size=B)
    valid_len_author = rng.integers(HISTORY_LEN // 2, HISTORY_LEN + 1, size=B)
    pos = np.arange(HISTORY_LEN)[None, :]
    history_post_hashes = np.where(
        (pos < valid_len_post[:, None])[:, :, None], history_post_hashes, np.int32(0)
    )
    history_author_hashes = np.where(
        (pos < valid_len_author[:, None])[:, :, None], history_author_hashes, np.int32(0)
    )

    history_actions = (rng.random(size=(B, HISTORY_LEN, NUM_ACTIONS)) > 0.7).astype(np.float32)
    history_product_surface = rng.integers(
        0, SURFACE_VOCAB, size=(B, HISTORY_LEN), dtype=np.int32
    )

    candidate_post_hashes = rng.integers(
        1, TABLE_SIZE, size=(B, NUM_CANDIDATES, NUM_HASHES), dtype=np.int32
    )
    candidate_author_hashes = rng.integers(
        1, TABLE_SIZE, size=(B, NUM_CANDIDATES, NUM_HASHES), dtype=np.int32
    )
    candidate_product_surface = rng.integers(
        0, SURFACE_VOCAB, size=(B, NUM_CANDIDATES), dtype=np.int32
    )

    batch = RecsysBatch(
        user_hashes=user_hashes,
        history_post_hashes=history_post_hashes,
        history_author_hashes=history_author_hashes,
        history_actions=history_actions,
        history_product_surface=history_product_surface,
        candidate_post_hashes=candidate_post_hashes,
        candidate_author_hashes=candidate_author_hashes,
        candidate_product_surface=candidate_product_surface,
    )
    labels = (rng.random(size=(B, NUM_CANDIDATES, NUM_ACTIONS)) < 0.1).astype(np.float32)
    labels[:, :, 18] = rng.random(size=(B, NUM_CANDIDATES)).astype(np.float32)
    return batch, labels


def _col_to_numpy(col, tail_shape: tuple, dtype):
    """把一个 pyarrow list 列（可能嵌套两层）一次性展开并 reshape 为紧凑 numpy 数组。

    假设每条样本中 list 的长度都相同（本项目固定为 HISTORY_LEN / NUM_CANDIDATES / NUM_HASHES /
    NUM_ACTIONS），这样可以直接取底层 buffer 并 reshape，避免 Python 逐元素转换。
    """
    a = col.combine_chunks()
    for _ in tail_shape:
        a = a.values
    flat = a.to_numpy(zero_copy_only=False)
    return flat.reshape(-1, *tail_shape).astype(dtype, copy=False)


# 每列的 numpy shape（不含 batch 维）与 dtype，供流式加载复用。
_PARQUET_COLUMN_SPEC = {
    "user_hashes": ((NUM_HASHES,), np.int32),
    "history_post_hashes": ((HISTORY_LEN, NUM_HASHES), np.int32),
    "history_author_hashes": ((HISTORY_LEN, NUM_HASHES), np.int32),
    "history_actions": ((HISTORY_LEN, NUM_ACTIONS), np.float32),
    "history_product_surface": ((HISTORY_LEN,), np.int32),
    "candidate_post_hashes": ((NUM_CANDIDATES, NUM_HASHES), np.int32),
    "candidate_author_hashes": ((NUM_CANDIDATES, NUM_HASHES), np.int32),
    "candidate_product_surface": ((NUM_CANDIDATES,), np.int32),
    "labels": ((NUM_CANDIDATES, NUM_ACTIONS), np.float32),
}


def load_parquet_dataset(
    data_dir: str,
    max_samples: int | None = None,
    max_files: int | None = None,
    row_group_batch_size: int = 8192,
) -> dict:
    """流式加载目录下所有 Parquet，返回紧凑 numpy 字典。

    为什么用 `iter_batches` 而不是 `pq.read_table + concat`：
      - 单个 Parquet 解压后可能达到数 GB，再整个目录 concat 容易导致 macOS
        下被内核静默杀死（Python 进程 exit code 也会是 0），日志表现为“开始加
        载...”之后沉默退出。
      - 按 RowGroup/batch 逐块读取并立即 flatten+cast 为紧凑 int32/float32，
        peak memory 稳定在单个 chunk 的规模。

    参数：
      max_samples: 累计行数达到此值后立即停止，便于快速试跡。
      max_files:   只读前 N 个 Parquet 文件（文件已按名排序）。
      row_group_batch_size: 分块读取时每块的行数。
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("读取 Parquet 需要安装 pyarrow：uv add pyarrow")

    files = sorted(Path(data_dir).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"在 {data_dir} 下没有找到任何 .parquet 文件")
    if max_files is not None:
        files = files[:max_files]
    logger.info(
        f"找到 {len(files)} 个 Parquet 文件"
        + (f"（限制 max_samples={max_samples}）" if max_samples else "")
        + "，开始流式加载..."
    )

    # 每列各自占一个小 chunk 列表，最后 concatenate，避免把 arrow table 整体做留存。
    chunks: dict[str, list[np.ndarray]] = {name: [] for name in _PARQUET_COLUMN_SPEC}
    total_rows = 0
    for f in files:
        if max_samples is not None and total_rows >= max_samples:
            break
        try:
            pf = pq.ParquetFile(str(f))
        except (OSError, pa.ArrowException) as e:
            logger.warning(f"跳过 {f}：{e}")
            continue
        for rb in pf.iter_batches(
            batch_size=row_group_batch_size,
            columns=list(_PARQUET_COLUMN_SPEC.keys()),
        ):
            take = rb.num_rows
            if max_samples is not None:
                remain = max_samples - total_rows
                if remain <= 0:
                    break
                if take > remain:
                    rb = rb.slice(0, remain)
                    take = remain
            tbl = pa.Table.from_batches([rb])
            for name, (tail_shape, dtype) in _PARQUET_COLUMN_SPEC.items():
                chunks[name].append(_col_to_numpy(tbl[name], tail_shape, dtype))
            total_rows += take
            del tbl, rb
        logger.info(f"  已加载 {f.name}，累计 {total_rows} 行")

    if total_rows == 0:
        raise RuntimeError(f"{data_dir} 下所有 Parquet 都无法读取")

    logger.info(f"共加载 {total_rows} 条样本，正在拼接紧凑 numpy...")
    data = {name: np.concatenate(arrs, axis=0) for name, arrs in chunks.items()}
    return data


def shuffle_candidates(
    batch: RecsysBatch, labels: np.ndarray, rng: np.random.Generator
) -> tuple[RecsysBatch, np.ndarray]:
    """每行独立打乱候选槽位顺序。

    预处理产出的样本正例固定在第 0 个槽位，而候选 token 之间虽然互不可见，RoPE 仍让每个槽位
    相对历史的位置不同；不打乱的话模型会直接学“第 0 槽 = 正例”，完全不看候选是谁
    （实测 HR@1 = 1.0 且打乱候选 ID 后分数不变）。线上候选顺序是任意的，训练必须一致。
    """
    b, c = labels.shape[:2]
    perm = rng.permuted(np.broadcast_to(np.arange(c), (b, c)), axis=1)

    def take(x):
        idx = perm.reshape(perm.shape + (1,) * (x.ndim - 2))
        return np.take_along_axis(x, np.broadcast_to(idx, x.shape), axis=1)

    return (
        batch._replace(
            candidate_post_hashes=take(batch.candidate_post_hashes),
            candidate_author_hashes=take(batch.candidate_author_hashes),
            candidate_product_surface=take(batch.candidate_product_surface),
        ),
        take(labels),
    )


def _make_batch_labels(data: dict, sl) -> tuple[RecsysBatch, np.ndarray]:
    """从 numpy 字典按索引切出 (RecsysBatch, labels)。供 in-memory 与 streaming 共用。"""
    batch = RecsysBatch(
        user_hashes=data["user_hashes"][sl],
        history_post_hashes=data["history_post_hashes"][sl],
        history_author_hashes=data["history_author_hashes"][sl],
        history_actions=data["history_actions"][sl],
        history_product_surface=data["history_product_surface"][sl],
        candidate_post_hashes=data["candidate_post_hashes"][sl],
        candidate_author_hashes=data["candidate_author_hashes"][sl],
        candidate_product_surface=data["candidate_product_surface"][sl],
    )
    labels = data["labels"][sl]
    return batch, labels


def iterate_parquet_dataset(
    data: dict,
    batch_size: int,
    shuffle: bool = True,
    seed: int = 0,
):
    """把内存中的数据按 batch_size 无限循环切片产出 (RecsysBatch, labels)。

    每个 epoch 结束会重新 shuffle 索引。
    """
    n = len(data["user_hashes"])
    if n < batch_size:
        raise ValueError(f"样本数 {n} 小于 batch_size {batch_size}")
    rng = np.random.default_rng(seed)
    while True:
        idx = rng.permutation(n) if shuffle else np.arange(n)
        for start in range(0, n - batch_size + 1, batch_size):
            yield _make_batch_labels(data, idx[start:start + batch_size])


def stream_parquet_batches(
    data_dir: str,
    batch_size: int,
    shuffle_buffer_size: int = 16384,
    max_files: int | None = None,
    row_group_batch_size: int = 8192,
    seed: int = 0,
):
    """无限流式产出 (RecsysBatch, labels)，内存 O(shuffle_buffer)，与数据集大小无关。

    策略：
      1. 遍历所有 parquet 文件；iter_batches 分块读取、flatten+cast 为紧凑 numpy。
      2. 将 chunks 累加到 shuffle buffer；累计达到 shuffle_buffer_size 后做一次
         局部全排列 + 按 batch_size 切片输出，近似于 TF shuffle buffer 的效果。
      3. 不满 batch 的残留行转入下一轮 buffer，避免丢数据。
      4. 文件读完后回到开头，无限 epoch。

    参数：
      shuffle_buffer_size: 内存中同时保留的样本数。默认 16384 ≈ 62 MB (每样 ~3.8 KB)。
      row_group_batch_size: 单次从 parquet 读取的行数。
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("读取 Parquet 需要安装 pyarrow：uv add pyarrow")

    files = sorted(Path(data_dir).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"在 {data_dir} 下没有找到任何 .parquet 文件")
    if max_files is not None:
        files = files[:max_files]
    logger.info(
        f"[streaming] 覆盖 {len(files)} 个 Parquet 文件，"
        f"shuffle_buffer={shuffle_buffer_size}, chunk={row_group_batch_size}"
    )
    rng = np.random.default_rng(seed)

    if shuffle_buffer_size < batch_size:
        raise ValueError(f"shuffle_buffer_size {shuffle_buffer_size} 小于 batch_size {batch_size}")

    def _chunk_iter():
        """无限产出每个 chunk 的紧凑 numpy 字典。"""
        epoch = 0
        while True:
            epoch += 1
            for f in files:
                try:
                    pf = pq.ParquetFile(str(f))
                except (OSError, pa.ArrowException) as e:
                    logger.warning(f"跳过 {f}：{e}")
                    continue
                for rb in pf.iter_batches(
                    batch_size=row_group_batch_size,
                    columns=list(_PARQUET_COLUMN_SPEC.keys()),
                ):
                    tbl = pa.Table.from_batches([rb])
                    chunk = {
                        name: _col_to_numpy(tbl[name], tail, dtype)
                        for name, (tail, dtype) in _PARQUET_COLUMN_SPEC.items()
                    }
                    del tbl, rb
                    yield chunk
            logger.info(f"[streaming] 完成第 {epoch} 轮遍历，继续下一轮 epoch")

    chunks: dict[str, list[np.ndarray]] = {name: [] for name in _PARQUET_COLUMN_SPEC}
    in_buffer = 0
    for chunk in _chunk_iter():
        size = len(chunk["user_hashes"])
        for name, value in chunks.items():
            value.append(chunk[name])
        in_buffer += size
        if in_buffer < shuffle_buffer_size:
            continue
        # buffer 满，一次性全排列 + 切 batch
        merged = {name: np.concatenate(arrs, axis=0) for name, arrs in chunks.items()}
        perm = rng.permutation(in_buffer)
        n_full = (in_buffer // batch_size) * batch_size
        for start in range(0, n_full, batch_size):
            yield _make_batch_labels(merged, perm[start:start + batch_size])
        # 不满 batch 的残留行留给下一轮，避免丢数据
        leftover = in_buffer - n_full
        if leftover > 0:
            tail_sl = perm[n_full:]
            chunks = {name: [merged[name][tail_sl]] for name in merged}
            in_buffer = leftover
        else:
            chunks = {name: [] for name in chunks}
            in_buffer = 0


# ── 检查点保存 ────────────────────────────────────────────────────────────────

def save_checkpoint(ckpt_dir: str, params: Any, step: int) -> str:
    bundle_dir = Path(ckpt_dir) / f"step-{step:06d}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    path = bundle_dir / "model_params.npz"
    np.savez(path, **flatten_dict(params))
    logger.info(f"模型参数已保存到 {path}")
    return str(path)


def save_artifacts(
    ckpt_dir: str, params: Any, emb_state: dict, opt_state, step: int, head_mask: np.ndarray
) -> dict[str, str]:
    """模型参数、嵌入表、优化器状态与 metadata 成套按 step 落盘。"""
    params_path = save_checkpoint(ckpt_dir, params, step)
    bundle_dir = Path(params_path).parent
    emb_path = bundle_dir / "embedding_tables.npz"
    save_embedding_state(str(emb_path), emb_state)
    optimizer_path = bundle_dir / "optimizer_state.npz"
    save_optimizer_state(str(optimizer_path), opt_state)
    metadata = {
        "feature_schema": FEATURE_SCHEMA,
        "step": step,
        "model_params": Path(params_path).name,
        "embedding_tables": emb_path.name,
        "embedding_state_format": 2,
        "optimizer_state": optimizer_path.name,
        "table_size": TABLE_SIZE,
        "emb_size": EMB_SIZE,
        "num_hashes": NUM_HASHES,
        "history_len": HISTORY_LEN,
        "observed_actions": observed_action_names(head_mask),
        "supported_action_enums": supported_action_enums(head_mask),
    }
    metadata_path = bundle_dir / "metadata.json"
    temporary_path = bundle_dir / "metadata.json.tmp"
    with open(temporary_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    os.replace(temporary_path, metadata_path)

    # 根目录只保存一个指针，不复制 metadata，避免“最新 metadata”误配历史模型。
    latest_path = Path(ckpt_dir) / "latest.json"
    latest_temporary_path = Path(ckpt_dir) / "latest.json.tmp"
    with open(latest_temporary_path, "w", encoding="utf-8") as f:
        json.dump({"checkpoint_dir": bundle_dir.name, "step": step}, f, indent=2)
    os.replace(latest_temporary_path, latest_path)
    return {
        "params": params_path,
        "embedding_tables": str(emb_path),
        "metadata": str(metadata_path),
        "checkpoint_dir": str(bundle_dir),
    }


def _step_from_checkpoint_path(path: str | None) -> int | None:
    if path is None:
        return None
    match = re.search(r"(?:model_params|metadata|embedding_tables|optimizer_state)_step(\d+)", Path(path).name)
    return None if match is None else int(match.group(1))


def _checkpoint_dir_from_path(ckpt_dir: str, path: str | None) -> Path | None:
    """解析新 bundle 路径，并兼容旧的 model_params_stepN.npz 文件。"""
    if path is not None:
        candidate = Path(path)
        if candidate.is_dir() and (candidate / "metadata.json").exists():
            return candidate
        if candidate.name == "model_params.npz" and (candidate.parent / "metadata.json").exists():
            return candidate.parent
        step = _step_from_checkpoint_path(path)
        if step is not None:
            new_dir = Path(ckpt_dir) / f"step-{step:06d}"
            if (new_dir / "metadata.json").exists():
                return new_dir
        return None
    latest_path = Path(ckpt_dir) / "latest.json"
    if latest_path.exists():
        with open(latest_path, encoding="utf-8") as f:
            latest = json.load(f)
        candidate = Path(ckpt_dir) / latest["checkpoint_dir"]
        if (candidate / "metadata.json").exists():
            return candidate
    return None


def _resume_embedding_path(ckpt_dir: str, params_path: str | None) -> str | None:
    """选择与 resume_params 同 bundle 的 embedding，兼容旧的平铺文件。"""
    bundle_dir = _checkpoint_dir_from_path(ckpt_dir, params_path)
    if bundle_dir is not None and (bundle_dir / "embedding_tables.npz").exists():
        return str(bundle_dir / "embedding_tables.npz")
    step = _step_from_checkpoint_path(params_path)
    candidates = []
    if step is not None:
        candidates.append(Path(ckpt_dir) / f"embedding_tables_step{step}.npz")
    else:
        candidates.extend(
            sorted(
                Path(ckpt_dir).glob("embedding_tables_step*.npz"),
                key=lambda path: _step_from_checkpoint_path(str(path)) or -1,
                reverse=True,
            )
        )
    candidates.append(Path(ckpt_dir) / "embedding_tables.npz")
    return next((str(path) for path in candidates if path.exists()), None)


def _resume_params_path(ckpt_dir: str, params_path: str | None) -> str | None:
    bundle_dir = _checkpoint_dir_from_path(ckpt_dir, params_path)
    if bundle_dir is not None and (bundle_dir / "model_params.npz").exists():
        return str(bundle_dir / "model_params.npz")
    return params_path


def _resume_optimizer_path(ckpt_dir: str, params_path: str | None) -> str | None:
    bundle_dir = _checkpoint_dir_from_path(ckpt_dir, params_path)
    if bundle_dir is not None and (bundle_dir / "optimizer_state.npz").exists():
        return str(bundle_dir / "optimizer_state.npz")
    step = _step_from_checkpoint_path(params_path)
    candidates = []
    if step is not None:
        candidates.append(Path(ckpt_dir) / f"optimizer_state_step{step}.npz")
    else:
        candidates.extend(
            sorted(
                Path(ckpt_dir).glob("optimizer_state_step*.npz"),
                key=lambda path: _step_from_checkpoint_path(str(path)) or -1,
                reverse=True,
            )
        )
    return next((str(path) for path in candidates if path.exists()), None)


def load_checkpoint(path: str) -> dict:
    """还原 haiku 参数树：模块路径本身含 "/"，只按最后一个 "/" 拆出参数名。"""
    raw = np.load(path, allow_pickle=False)
    params: dict[str, dict[str, np.ndarray]] = {}
    for key in raw.files:
        module, name = key.rsplit("/", 1)
        params.setdefault(module, {})[name] = raw[key]
    return params


# ── 离线评估 ──────────────────────────────────────────────────────────────────
#
# 评估集应是训练集之后的日期（时间切分）。每条样本含 1 个正例 + 若干随机负例，因此：
#   - 逐头 AUC / logloss：在所有有效候选上按头计算；
#   - 组内排序 HR@1 / MRR：正例在同组候选里的名次。分数完全相同的候选按“随机打破平局”
#     的期望名次计，避免退化模型（所有候选同分）因为正例固定在第 0 位而拿满分。

_forward_transform = hk.without_apply_rng(hk.transform(forward_logits))


@jax.jit
def _predict_forward(params, emb_state, batch):
    embeddings = lookup_embeddings_jax(emb_state, batch)
    return jax.nn.sigmoid(_forward_transform.apply(params, batch, embeddings))


def binary_auc(scores: np.ndarray, labels: np.ndarray) -> float | None:
    from scipy.stats import rankdata

    positive = labels > 0.5
    n_pos = int(positive.sum())
    n_neg = int(len(labels) - n_pos)
    if n_pos == 0 or n_neg == 0:
        return None
    ranks = rankdata(scores)
    return float((ranks[positive].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def binary_logloss(probs: np.ndarray, labels: np.ndarray, eps: float = 1e-7) -> float:
    p = np.clip(probs, eps, 1 - eps)
    return float(-np.mean(labels * np.log(p) + (1 - labels) * np.log(1 - p)))


def ranking_metrics(scores: np.ndarray, valid: np.ndarray, positive: np.ndarray) -> dict:
    """scores/valid/positive 形状均为 [N, C]。返回 HR@1、MRR 与随机基线 HR@1。"""
    n = scores.shape[0]
    rows = np.arange(n)
    has_pos = positive.any(axis=1)
    n_valid = valid.sum(axis=1)
    keep = has_pos & (n_valid >= 2)
    if not keep.any():
        return {"hr@1": None, "mrr": None, "random_hr@1": None, "groups": 0}

    pos_idx = np.argmax(positive, axis=1)
    pos_score = scores[rows, pos_idx][:, None]
    others = valid & ~positive
    n_greater = np.sum(others & (scores > pos_score), axis=1)
    n_equal = np.sum(others & (scores == pos_score), axis=1)
    expected_rank = 1.0 + n_greater + 0.5 * n_equal
    hit1 = np.where(n_greater == 0, 1.0 / (1.0 + n_equal), 0.0)

    return {
        "hr@1": float(np.mean(hit1[keep])),
        "mrr": float(np.mean(1.0 / expected_rank[keep])),
        "random_hr@1": float(np.mean(1.0 / n_valid[keep])),
        "groups": int(keep.sum()),
    }


def predict_probs(params, emb_state: dict, data: dict, batch_size: int) -> np.ndarray:
    """对紧凑 numpy 数据集分批前向，返回 [N, C, num_actions] 的 sigmoid 概率。"""

    n = len(data["user_hashes"])
    out = []
    for start in range(0, n, batch_size):
        batch, _ = _make_batch_labels(data, slice(start, min(start + batch_size, n)))
        batch = jax.tree_util.tree_map(jnp.asarray, batch)
        out.append(np.asarray(_predict_forward(params, emb_state, batch)))
    return np.concatenate(out, axis=0)


def evaluate(params, emb_state: dict, data: dict, head_mask: np.ndarray, batch_size: int) -> dict:
    probs = predict_probs(params, emb_state, data, batch_size)  # [N, C, A]
    labels = data["labels"]
    valid = data["candidate_post_hashes"][:, :, 0] != 0  # [N, C]
    binary_heads = [i for i in range(18) if head_mask[i] > 0]

    metrics: dict[str, Any] = {"samples": len(labels), "heads": {}}
    for i in binary_heads:
        name = ACTIONS[i]
        p = probs[:, :, i][valid]
        y = labels[:, :, i][valid]
        metrics["heads"][name] = {
            "auc": binary_auc(p, y),
            "logloss": binary_logloss(p, y),
            "positive_rate": float(np.mean(y)),
        }

    if binary_heads:
        score = probs[:, :, binary_heads].sum(axis=-1)
        positive = (labels[:, :, binary_heads].sum(axis=-1) > 0) & valid
        metrics["ranking"] = ranking_metrics(score, valid, positive)
    return metrics


def format_metrics(metrics: dict) -> str:
    parts = []
    for name, m in metrics.get("heads", {}).items():
        auc = "n/a" if m["auc"] is None else f"{m['auc']:.4f}"
        parts.append(f"{name}: auc={auc} logloss={m['logloss']:.4f} pos={m['positive_rate']:.3f}")
    r = metrics.get("ranking")
    if r and r["hr@1"] is not None:
        parts.append(
            f"HR@1={r['hr@1']:.4f} (random {r['random_hr@1']:.4f}) MRR={r['mrr']:.4f} "
            f"groups={r['groups']}"
        )
    return " | ".join(parts) if parts else "no metrics"


def append_metrics(ckpt_dir: str, step: int, metrics: dict) -> None:
    os.makedirs(ckpt_dir, exist_ok=True)
    path = os.path.join(ckpt_dir, "metrics.json")
    history = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            history = json.load(f)
    history.append({"step": step, **metrics})
    with open(path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


# ── 训练主流程 ────────────────────────────────────────────────────────────────

def train(args):
    try:
        import optax
    except ImportError:
        raise ImportError("训练需要 optax，请先在 phoenix 目录执行 uv sync")

    logger.info("=== Phoenix 精排模型训练开始 ===")
    logger.info(f"数据来源：{'模拟数据' if args.data_dir is None else args.data_dir}")
    logger.info(
        f"训练步数：{args.steps}，batch size：{args.batch_size}，lr：{args.lr}，emb-lr：{args.emb_lr}"
    )
    head_mask_np = resolve_head_mask(args.observed_actions)
    head_mask = jnp.asarray(head_mask_np)
    logger.info(f"参与损失的头：{observed_action_names(head_mask_np)}")

    # 1. 初始化或加载嵌入表。resume 时优先选择与 model_params 同 step 的文件。
    resume_params_path = (
        _resume_params_path(args.ckpt_dir, args.resume_params) if args.resume_params else None
    )
    resume_emb_path = _resume_embedding_path(args.ckpt_dir, args.resume_params)
    if args.resume_emb and resume_emb_path is not None:
        emb_state = load_embedding_state(resume_emb_path)
        logger.info(f"已加载嵌入表：{resume_emb_path}")
    else:
        emb_state = init_embedding_state(*init_embedding_tables())
        logger.info("随机初始化嵌入表")

    # 2. 嵌入表 + Adagrad 累加器上设备；训练中作为状态随 step 更新
    # 3. 初始化模型参数（init 阶段用 device 查表，JIT 前一次性即可）
    loss_transform = hk.without_apply_rng(hk.transform(loss_fn))

    dummy_batch, dummy_labels = make_simulated_batch(batch_size=1)
    dummy_embeddings = lookup_embeddings_jax(emb_state, dummy_batch)
    dummy_labels_jnp = jnp.array(dummy_labels)

    rng = jax.random.PRNGKey(42)
    params = loss_transform.init(rng, dummy_batch, dummy_embeddings, dummy_labels_jnp, head_mask)

    if resume_params_path and os.path.exists(resume_params_path):
        params = load_checkpoint(resume_params_path)
        logger.info(f"已加载模型参数：{resume_params_path}")

    # 4. 优化器（稠密参数用 Adam；嵌入表走 apply_embedding_grads 的稀疏 Adagrad）
    optimizer = optax.adam(learning_rate=args.lr)
    opt_state = optimizer.init(params)
    resume_optimizer_path = _resume_optimizer_path(args.ckpt_dir, args.resume_params)
    if resume_optimizer_path is not None:
        opt_state = load_optimizer_state(resume_optimizer_path, opt_state)
        logger.info(f"已加载优化器状态：{resume_optimizer_path}")

    # 5. JIT 编译 train_step：查表、前反向、稠密与稀疏更新全部融合在 device 上。
    @jax.jit
    def train_step(params, emb_state, opt_state, batch, labels):
        def loss(p, embeddings):
            return loss_transform.apply(p, batch, embeddings, labels, head_mask)

        embeddings = lookup_embeddings_jax(emb_state, batch)
        loss_val, (param_grads, emb_grads) = jax.value_and_grad(loss, argnums=(0, 1))(
            params, embeddings
        )
        updates, new_opt_state = optimizer.update(param_grads, opt_state)
        new_params = optax.apply_updates(params, updates)
        new_emb_state = apply_embedding_grads(emb_state, batch, emb_grads, args.emb_lr)
        return new_params, new_emb_state, new_opt_state, loss_val

    eval_data = None
    if args.eval_dir:
        eval_data = load_parquet_dataset(args.eval_dir, max_samples=args.eval_samples)
        logger.info(f"评估集：{args.eval_dir}，{len(eval_data['user_hashes'])} 条样本")

    def run_eval(step: int):
        if eval_data is None:
            return
        metrics = evaluate(params, emb_state, eval_data, head_mask_np, args.eval_batch_size)
        logger.info(f"[eval step {step}] " + format_metrics(metrics))
        append_metrics(args.ckpt_dir, step, metrics)

    # 6. 数据源：根据 --streaming 选择 in-memory 或流式迭代器
    if args.data_dir is not None:
        if args.streaming:
            data_iter = stream_parquet_batches(
                args.data_dir,
                batch_size=args.batch_size,
                shuffle_buffer_size=args.shuffle_buffer,
                max_files=args.max_files,
                seed=0,
            )
        else:
            dataset = load_parquet_dataset(
                args.data_dir,
                max_samples=args.max_samples,
                max_files=args.max_files,
            )
            data_iter = iterate_parquet_dataset(dataset, args.batch_size, shuffle=True, seed=0)
    else:
        sim_rng = np.random.default_rng(0)

        def _sim_gen():
            while True:
                yield make_simulated_batch_fast(args.batch_size, sim_rng)

        data_iter = _sim_gen()

    # 7. 训练循环：loss 在 device 上累加，每 log_every 步才同步一次
    logger.info("开始训练循环（首次 step 因 JIT 编译会较慢）...")
    loss_accum = jnp.zeros((), dtype=jnp.float32)
    accum_count = 0
    step = 0
    shuffle_rng = np.random.default_rng(1)

    while step < args.steps:
        batch, labels = next(data_iter)
        batch, labels = shuffle_candidates(batch, labels, shuffle_rng)
        # tree_map 下异步 device_put，下一步 train_step 会与之流水重叠
        batch_dev = jax.tree_util.tree_map(jnp.asarray, batch)
        labels_dev = jnp.asarray(labels)

        params, emb_state, opt_state, loss_val = train_step(
            params, emb_state, opt_state, batch_dev, labels_dev
        )
        loss_accum = loss_accum + loss_val
        accum_count += 1
        step += 1

        if step % args.log_every == 0:
            avg_loss = float(loss_accum) / max(accum_count, 1)
            logger.info(f"step {step:5d} / {args.steps}  loss={avg_loss:.4f}")
            loss_accum = jnp.zeros((), dtype=jnp.float32)
            accum_count = 0

        if args.eval_every and step % args.eval_every == 0 and step < args.steps:
            run_eval(step)

        if step % args.save_every == 0 and step < args.steps:
            save_artifacts(args.ckpt_dir, params, emb_state, opt_state, step, head_mask_np)

    # 8. 可选 benchmark：训练循环之后追加一段稳态 step 计时（不会写 checkpoint）
    if args.benchmark:
        import time
        warmup = max(0, args.warmup_steps)
        bench = max(1, args.benchmark_steps)
        logger.info(f"[bench] warmup={warmup} 步，计时={bench} 步，batch={args.batch_size}")
        for _ in range(warmup):
            batch, labels = next(data_iter)
            batch_dev = jax.tree_util.tree_map(jnp.asarray, batch)
            labels_dev = jnp.asarray(labels)
            params, emb_state, opt_state, loss_val = train_step(
                params, emb_state, opt_state, batch_dev, labels_dev
            )
        # barrier：确保 warmup 都落盘后再开始计时
        jax.block_until_ready(loss_val)
        t0 = time.perf_counter()
        for _ in range(bench):
            batch, labels = next(data_iter)
            batch_dev = jax.tree_util.tree_map(jnp.asarray, batch)
            labels_dev = jnp.asarray(labels)
            params, emb_state, opt_state, loss_val = train_step(
                params, emb_state, opt_state, batch_dev, labels_dev
            )
        jax.block_until_ready(loss_val)
        dt = time.perf_counter() - t0
        step_ms = dt / bench * 1000
        samples_per_sec = bench * args.batch_size / dt
        logger.info(
            f"[bench] avg_step={step_ms:.2f} ms, samples/sec={samples_per_sec:.1f} "
            f"({bench} steps after {warmup} warmup)"
        )

    # 9. 最终评估与保存
    run_eval(step)
    artifact_paths = save_artifacts(args.ckpt_dir, params, emb_state, opt_state, step, head_mask_np)
    logger.info("=== 训练完成 ===")
    logger.info(f"产物目录：{args.ckpt_dir}")
    logger.info("推理时加载：")
    logger.info(f"  checkpoint：{artifact_paths['checkpoint_dir']}")
    logger.info(f"  嵌入表  ：{artifact_paths['embedding_tables']}")
    logger.info(f"  元数据  ：{artifact_paths['metadata']}")


# ── 入口 ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Phoenix 精排模型训练")
    parser.add_argument(
        "--data-dir", type=str, default=None,
        help="Parquet 训练数据目录（不传则使用模拟数据）",
    )
    parser.add_argument("--ckpt-dir", type=str, default="./checkpoints", help="检查点保存目录")
    parser.add_argument("--steps", type=int, default=200, help="训练总步数")
    parser.add_argument("--batch-size", type=int, default=4, help="每步 batch 大小")
    parser.add_argument("--lr", type=float, default=1e-4, help="Adam 学习率（稠密模型参数）")
    parser.add_argument(
        "--emb-lr", type=float, default=0.05,
        help="嵌入表 row-wise Adagrad 学习率",
    )
    parser.add_argument(
        "--observed-actions", type=str, default=None,
        help="逗号分隔的已采集行为（如 favorite,reply）；只有这些头参与损失/评估并写入 metadata。"
             "不指定则全部 19 个头参与",
    )
    parser.add_argument(
        "--eval-dir", type=str, default=None,
        help="评估集 Parquet 目录（应是训练集之后的日期，做时间切分评估）",
    )
    parser.add_argument("--eval-every", type=int, default=0, help="每隔多少步评估一次（0 = 只在结束时）")
    parser.add_argument("--eval-samples", type=int, default=20000, help="评估集最多加载的样本数")
    parser.add_argument("--eval-batch-size", type=int, default=256, help="评估前向的 batch 大小")
    parser.add_argument("--log-every", type=int, default=20, help="每隔多少步打印一次 loss")
    parser.add_argument("--save-every", type=int, default=100, help="每隔多少步保存一次检查点")
    parser.add_argument("--resume-params", type=str, default=None, help="从此路径加载模型参数继续训练")
    parser.add_argument("--resume-emb", action="store_true", help="从 ckpt-dir 加载嵌入表继续训练")
    parser.add_argument(
        "--max-samples", type=int, default=None,
        help="in-memory 模式下 Parquet 最多加载的样本行数（防 OOM / 快速试跡）",
    )
    parser.add_argument(
        "--max-files", type=int, default=None,
        help="只读 Parquet 目录下排序后前 N 个文件",
    )
    parser.add_argument(
        "--streaming", action="store_true",
        help="流式读取 Parquet，内存与数据集大小解耦（推荐用于全量大数据训练）",
    )
    parser.add_argument(
        "--shuffle-buffer", type=int, default=16384,
        help="streaming 模式下的 shuffle buffer 大小（样本行数，默认 16384 ≈ 62 MB）",
    )
    parser.add_argument(
        "--benchmark", action="store_true",
        help="训练循环后追加一段稳态 step 计时，输出平均 step 耗时与 samples/sec",
    )
    parser.add_argument(
        "--benchmark-steps", type=int, default=50,
        help="benchmark 阶段的计时步数",
    )
    parser.add_argument(
        "--warmup-steps", type=int, default=10,
        help="benchmark 阶段的热身步数（不计入结果）",
    )
    args = parser.parse_args()

    train(args)
