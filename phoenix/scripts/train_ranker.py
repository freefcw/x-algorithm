# 精排模型训练脚本
#
# 用法：
#   uv run train_ranker.py                        # 用模拟数据跑通训练循环
#   uv run train_ranker.py --data-dir ./my_data   # 用真实 Parquet 数据训练
#
# 依赖：
#   uv add optax                                  # 运行前先安装优化器库

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
    生成模拟训练 batch（不需要真实数据）。
    返回 (batch, labels)，labels 形状 [B, C, num_actions]。
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


def load_parquet_batch(parquet_path: str, batch_size: int) -> tuple[RecsysBatch, np.ndarray]:
    """
    从 Parquet 文件加载一个 batch。

    Parquet 每行字段（对应 docs/training_data_spec.md §6）：
        user_hashes:              list[int], shape [2]
        history_post_hashes:      list[list[int]], shape [32, 2]
        history_author_hashes:    list[list[int]], shape [32, 2]
        history_actions:          list[list[float]], shape [32, 19]
        history_product_surface:  list[int], shape [32]
        candidate_post_hashes:    list[list[int]], shape [8, 2]
        candidate_author_hashes:  list[list[int]], shape [8, 2]
        candidate_product_surface: list[int], shape [8]
        labels:                   list[list[float]], shape [8, 19]
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("读取 Parquet 需要安装 pyarrow：uv add pyarrow")

    table = pq.read_table(parquet_path)
    df = table.to_pydict()
    n = min(batch_size, len(df["user_hashes"]))

    batch = RecsysBatch(
        user_hashes=np.array(df["user_hashes"][:n], dtype=np.int32),
        history_post_hashes=np.array(df["history_post_hashes"][:n], dtype=np.int32),
        history_author_hashes=np.array(df["history_author_hashes"][:n], dtype=np.int32),
        history_actions=np.array(df["history_actions"][:n], dtype=np.float32),
        history_product_surface=np.array(df["history_product_surface"][:n], dtype=np.int32),
        candidate_post_hashes=np.array(df["candidate_post_hashes"][:n], dtype=np.int32),
        candidate_author_hashes=np.array(df["candidate_author_hashes"][:n], dtype=np.int32),
        candidate_product_surface=np.array(df["candidate_product_surface"][:n], dtype=np.int32),
    )
    labels = np.array(df["labels"][:n], dtype=np.float32)
    return batch, labels


def iter_parquet_dir(data_dir: str, batch_size: int):
    """遍历目录下所有 Parquet 文件，逐 batch 产出 (batch, labels)。"""
    files = sorted(Path(data_dir).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"在 {data_dir} 下没有找到任何 .parquet 文件")
    logger.info(f"找到 {len(files)} 个 Parquet 文件")
    for f in files:
        try:
            yield load_parquet_batch(str(f), batch_size)
        except Exception as e:
            logger.warning(f"跳过 {f}：{e}")


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
        raise ImportError("训练需要安装 optax：uv add optax")

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

    # 2. 初始化模型参数
    loss_transform = hk.without_apply_rng(hk.transform(loss_fn))

    dummy_batch, dummy_labels = make_simulated_batch(batch_size=1)
    dummy_embeddings = lookup_embeddings(dummy_batch, user_emb, post_emb, author_emb)
    dummy_labels_jnp = jnp.array(dummy_labels)

    rng = jax.random.PRNGKey(42)
    params = loss_transform.init(rng, dummy_batch, dummy_embeddings, dummy_labels_jnp)

    if args.resume_params and os.path.exists(args.resume_params):
        params = load_checkpoint(args.resume_params)
        logger.info(f"已加载模型参数：{args.resume_params}")

    # 3. 初始化优化器
    optimizer = optax.adam(learning_rate=args.lr)
    opt_state = optimizer.init(params)

    # 4. JIT 编译 train_step
    @jax.jit
    def train_step(params, opt_state, batch, embeddings, labels):
        loss_val, grads = jax.value_and_grad(
            lambda p: loss_transform.apply(p, batch, embeddings, labels)
        )(params)
        updates, new_opt_state = optimizer.update(grads, opt_state)
        new_params = optax.apply_updates(params, updates)
        return new_params, new_opt_state, loss_val

    # 5. 训练循环
    logger.info("开始训练循环（首次 step 因 JIT 编译会较慢）...")
    step = 0
    loss_accum = 0.0

    while step < args.steps:
        # 数据来源：真实 Parquet 或模拟数据
        if args.data_dir is not None:
            data_iter = iter_parquet_dir(args.data_dir, args.batch_size)
        else:
            data_iter = (make_simulated_batch(args.batch_size) for _ in range(args.steps))

        for batch, labels in data_iter:
            if step >= args.steps:
                break

            embeddings = lookup_embeddings(batch, user_emb, post_emb, author_emb)
            labels_jnp = jnp.array(labels)

            params, opt_state, loss_val = train_step(params, opt_state, batch, embeddings, labels_jnp)
            loss_accum += float(loss_val)
            step += 1

            if step % args.log_every == 0:
                avg_loss = loss_accum / args.log_every
                logger.info(f"step {step:5d} / {args.steps}  loss={avg_loss:.4f}")
                loss_accum = 0.0

            if step % args.save_every == 0:
                save_checkpoint(args.ckpt_dir, params, step)

    # 6. 最终保存
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
    args = parser.parse_args()

    train(args)
