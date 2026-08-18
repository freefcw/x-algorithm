# 精排模型训练脚本
#
# 用法：
#   uv run train_ranker.py                        # 用模拟数据跑通训练循环
#   uv run train_ranker.py --data-dir ./my_data   # 用真实 Parquet 数据训练
#
# 依赖：
#   optax 已在 pyproject 主依赖中，uv sync 即可

import _setup_path  # noqa: F401

import argparse
import logging
import math
import os
from pathlib import Path
from typing import Any

import haiku as hk
import jax
import jax.numpy as jnp
import numpy as np

from grok import TransformerConfig
from recsys_model import HashConfig, PhoenixModelConfig, RecsysBatch, RecsysEmbeddings
from runners import ACTIONS, create_example_batch

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


def unflatten_dict(flat: dict, sep: str = "/") -> dict:
    """把展平的 dict 还原为嵌套结构。"""
    result: dict = {}
    for key, val in flat.items():
        parts = key.split(sep)
        d = result
        for part in parts[:-1]:
            d = d.setdefault(part, {})
        d[parts[-1]] = val
    return result


# ── 嵌入表管理 ────────────────────────────────────────────────────────────────

def init_embedding_tables(seed: int = 0):
    """随机初始化三张嵌入表，第 0 行保留为 padding（全零）。"""
    rng = np.random.default_rng(seed)
    user_emb   = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
    post_emb   = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
    author_emb = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
    user_emb[0] = post_emb[0] = author_emb[0] = 0.0
    return user_emb, post_emb, author_emb


def load_embedding_tables(path: str):
    """从 npz 文件加载嵌入表。"""
    tables = np.load(path)
    return tables["user_emb_table"], tables["post_emb_table"], tables["author_emb_table"]


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

def loss_fn(batch: RecsysBatch, embeddings: RecsysEmbeddings, labels: jax.Array) -> jax.Array:
    """
    多目标二元交叉熵损失。

    labels: [B, C, num_actions] float32
        - 索引 0~17：0/1 二分类行为
        - 索引 18（dwell_time）：连续值，用 MSE 单独计算
    """
    model_config = make_model_config()
    model_config.initialize()
    model_config.fprop_dtype = jnp.float32  # 训练时用 float32 保证梯度精度
    model = model_config.make()
    output = model(batch, embeddings)

    logits = output.logits.astype(jnp.float32)  # [B, C, num_actions]

    # 前 18 个行为：二元交叉熵
    bce_logits = logits[:, :, :18]
    bce_labels = labels[:, :, :18]
    bce_loss = jnp.mean(
        jnp.sum(
            jax.nn.softplus(bce_logits) - bce_labels * bce_logits,
            axis=-1,
        )
    )

    # 第 19 个（dwell_time）：MSE
    dwell_pred = jax.nn.sigmoid(logits[:, :, 18])
    dwell_label = labels[:, :, 18]
    dwell_loss = jnp.mean((dwell_pred - dwell_label) ** 2)

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
        import pyarrow as pa  # noqa: F401
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
        except Exception as e:
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
        import pyarrow as pa  # noqa: F401
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
                except Exception as e:
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
        for name in chunks:
            chunks[name].append(chunk[name])
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

def save_checkpoint(ckpt_dir: str, params: Any, step: int):
    os.makedirs(ckpt_dir, exist_ok=True)
    path = os.path.join(ckpt_dir, f"model_params_step{step}.npz")
    np.savez(path, **flatten_dict(params))
    logger.info(f"模型参数已保存到 {path}")


def load_checkpoint(path: str) -> dict:
    raw = np.load(path, allow_pickle=False)
    return unflatten_dict({k: raw[k] for k in raw.files})


# ── 训练主流程 ────────────────────────────────────────────────────────────────

def train(args):
    try:
        import optax
    except ImportError:
        raise ImportError("训练需要 optax，请先在 phoenix 目录执行 uv sync")

    logger.info("=== Phoenix 精排模型训练开始 ===")
    logger.info(f"数据来源：{'模拟数据' if args.data_dir is None else args.data_dir}")
    logger.info(f"训练步数：{args.steps}，batch size：{args.batch_size}，lr：{args.lr}")

    # 1. 初始化或加载嵌入表
    emb_path = os.path.join(args.ckpt_dir, "embedding_tables.npz")
    if args.resume_emb and os.path.exists(emb_path):
        user_emb, post_emb, author_emb = load_embedding_tables(emb_path)
        logger.info(f"已加载嵌入表：{emb_path}")
    else:
        user_emb, post_emb, author_emb = init_embedding_tables()
        logger.info("随机初始化嵌入表")

    # 2. 嵌入表上设备（一次性，之后 jnp.take 全部在设备上完成）
    user_emb_dev = jax.device_put(jnp.asarray(user_emb))
    post_emb_dev = jax.device_put(jnp.asarray(post_emb))
    author_emb_dev = jax.device_put(jnp.asarray(author_emb))

    # 3. 初始化模型参数（init 阶段仍用 host numpy 查表，JIT 前一次性即可）
    loss_transform = hk.without_apply_rng(hk.transform(loss_fn))

    dummy_batch, dummy_labels = make_simulated_batch(batch_size=1)
    dummy_embeddings = lookup_embeddings(dummy_batch, user_emb, post_emb, author_emb)
    dummy_labels_jnp = jnp.array(dummy_labels)

    rng = jax.random.PRNGKey(42)
    params = loss_transform.init(rng, dummy_batch, dummy_embeddings, dummy_labels_jnp)

    if args.resume_params and os.path.exists(args.resume_params):
        params = load_checkpoint(args.resume_params)
        logger.info(f"已加载模型参数：{args.resume_params}")

    # 4. 优化器
    optimizer = optax.adam(learning_rate=args.lr)
    opt_state = optimizer.init(params)

    # 5. JIT 编译 train_step：查表与前反向一起融合，避免 per-step host↔device 拷贝。
    def jax_lookup(batch: RecsysBatch) -> RecsysEmbeddings:
        return RecsysEmbeddings(
            user_embeddings=user_emb_dev[batch.user_hashes],
            history_post_embeddings=post_emb_dev[batch.history_post_hashes],
            candidate_post_embeddings=post_emb_dev[batch.candidate_post_hashes],
            history_author_embeddings=author_emb_dev[batch.history_author_hashes],
            candidate_author_embeddings=author_emb_dev[batch.candidate_author_hashes],
        )

    @jax.jit
    def train_step(params, opt_state, batch, labels):
        def loss(p):
            embeddings = jax_lookup(batch)
            return loss_transform.apply(p, batch, embeddings, labels)
        loss_val, grads = jax.value_and_grad(loss)(params)
        updates, new_opt_state = optimizer.update(grads, opt_state)
        new_params = optax.apply_updates(params, updates)
        return new_params, new_opt_state, loss_val

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

    while step < args.steps:
        batch, labels = next(data_iter)
        # tree_map 下异步 device_put，下一步 train_step 会与之流水重叠
        batch_dev = jax.tree_util.tree_map(jnp.asarray, batch)
        labels_dev = jnp.asarray(labels)

        params, opt_state, loss_val = train_step(params, opt_state, batch_dev, labels_dev)
        loss_accum = loss_accum + loss_val
        accum_count += 1
        step += 1

        if step % args.log_every == 0:
            avg_loss = float(loss_accum) / max(accum_count, 1)
            logger.info(f"step {step:5d} / {args.steps}  loss={avg_loss:.4f}")
            loss_accum = jnp.zeros((), dtype=jnp.float32)
            accum_count = 0

        if step % args.save_every == 0:
            save_checkpoint(args.ckpt_dir, params, step)

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
            params, opt_state, loss_val = train_step(
                params, opt_state, batch_dev, labels_dev
            )
        # barrier：确保 warmup 都落盘后再开始计时
        jax.block_until_ready(loss_val)
        t0 = time.perf_counter()
        for _ in range(bench):
            batch, labels = next(data_iter)
            batch_dev = jax.tree_util.tree_map(jnp.asarray, batch)
            labels_dev = jnp.asarray(labels)
            params, opt_state, loss_val = train_step(
                params, opt_state, batch_dev, labels_dev
            )
        jax.block_until_ready(loss_val)
        dt = time.perf_counter() - t0
        step_ms = dt / bench * 1000
        samples_per_sec = bench * args.batch_size / dt
        logger.info(
            f"[bench] avg_step={step_ms:.2f} ms, samples/sec={samples_per_sec:.1f} "
            f"({bench} steps after {warmup} warmup)"
        )

    # 9. 最终保存
    save_checkpoint(args.ckpt_dir, params, step)
    save_embedding_tables(emb_path, user_emb, post_emb, author_emb)
    logger.info("=== 训练完成 ===")
    logger.info(f"产物目录：{args.ckpt_dir}")
    logger.info("推理时加载：")
    logger.info(f"  模型参数：{args.ckpt_dir}/model_params_step{step}.npz")
    logger.info(f"  嵌入表  ：{emb_path}")


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
    parser.add_argument("--lr", type=float, default=1e-4, help="Adam 学习率")
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
