# 召回模型训练脚本
#
# 用法：
#   uv run train_retrieval.py                        # 用模拟数据跑通训练循环
#   uv run train_retrieval.py --data-dir ./my_data   # 用真实 Parquet 数据训练
#
# 依赖：
#   uv add optax                                    # 运行前先安装优化器库
#
# ── 与 train_ranker.py 的关系 ─────────────────────────────────────────────────
# 本脚本从 train_ranker.py 拷贝演进而来，主要差异：
#   1. 模型：PhoenixRetrievalModel（双塔）而非 PhoenixModel（单塔精排）
#   2. 损失：in-batch sampled softmax（对比学习）而非 19 维多任务 BCE
#   3. 数据：复用同一份 data_preprocessor.py 产出的 parquet，但只用候选位 0（正样本），
#          其余位（负样本、padding）一律忽略——in-batch negatives 会代替它们。
#   4. 产物：训好的参数可直接喂给 run_retrieval.py，在线先召回 top-k 再交给精排。
# ──────────────────────────────────────────────────────────────────────────────

import _setup_path  # noqa: F401

import argparse
import logging
import os
from pathlib import Path
from typing import Any

import haiku as hk
import jax
import jax.numpy as jnp
import numpy as np

from grok import TransformerConfig
from recsys_model import HashConfig, RecsysBatch, RecsysEmbeddings
from recsys_retrieval_model import PhoenixRetrievalModelConfig
from runners import ACTIONS, create_example_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("train_retrieval")

# ── 超参（与 run_retrieval.py / train_ranker.py 的关键维度保持一致）──────────
EMB_SIZE = 128
HISTORY_LEN = 32
NUM_CANDIDATES = 8           # parquet 里每行的候选位数（1 正 + 7 负 + padding），本脚本只取索引 0
NUM_ACTIONS = len(ACTIONS)
TABLE_SIZE = 100_000
SURFACE_VOCAB = 16
NUM_HASHES = 2

# 对比学习温度系数：logits = (user · item) / TEMPERATURE
# 双塔检索常用经验值 0.05~0.1；越小对 top-1 约束越强，但梯度更易饱和。
DEFAULT_TEMPERATURE = 0.05


# ── 工具函数 ──────────────────────────────────────────────────────────────────

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
    """
    根据 batch 中的哈希值查表，返回 RecsysEmbeddings。

    与精排一致：所有候选位（含索引 0 的正样本）都先查好嵌入，loss 里再抽取正样本位。
    """
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

def make_model_config() -> PhoenixRetrievalModelConfig:
    """
    构造召回模型配置。与 run_retrieval.py 中的配置保持一致，
    区别仅在训练时会把 fprop_dtype 切到 float32（见 loss_fn）。
    """
    return PhoenixRetrievalModelConfig(
        emb_size=EMB_SIZE,
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


# ── 损失函数（对比学习 / in-batch negatives）────────────────────────────────

def _take_positive_candidate(
    batch: RecsysBatch,
    embeddings: RecsysEmbeddings,
) -> tuple[RecsysBatch, RecsysEmbeddings]:
    """
    把 batch 里候选位的第 0 个（正样本）切出来，作为一个「只含 1 个候选」的子 batch。

    data_preprocessor 生成的样本里，候选位 0 总是真实的正样本（用户实际交互过的帖子），
    索引 1~ 是随机负样本 / padding。召回训练用 in-batch negatives 就够了，所以把负样本位
    整个丢掉，只把正样本位喂给物品塔；其余字段维度保持 [B, 1, *]，满足
    PhoenixRetrievalModel.build_candidate_representation 的 shape 约定。
    """
    pos_batch = batch._replace(
        candidate_post_hashes=batch.candidate_post_hashes[:, :1, :],        # [B, 1, H]
        candidate_author_hashes=batch.candidate_author_hashes[:, :1, :],    # [B, 1, H]
        candidate_product_surface=batch.candidate_product_surface[:, :1],   # [B, 1]
    )
    pos_embeddings = RecsysEmbeddings(
        user_embeddings=embeddings.user_embeddings,
        history_post_embeddings=embeddings.history_post_embeddings,
        history_author_embeddings=embeddings.history_author_embeddings,
        candidate_post_embeddings=embeddings.candidate_post_embeddings[:, :1, :, :],     # [B, 1, H, D]
        candidate_author_embeddings=embeddings.candidate_author_embeddings[:, :1, :, :], # [B, 1, H, D]
    )
    return pos_batch, pos_embeddings


def loss_fn(
    batch: RecsysBatch,
    embeddings: RecsysEmbeddings,
    temperature: float = DEFAULT_TEMPERATURE,
) -> jax.Array:
    """
    双塔对比学习损失：in-batch sampled softmax。

    推导：
        user_repr  = UserTower(batch)               # [B, D]（已 L2 归一化）
        item_repr  = ItemTower(pos_candidates)      # [B, D]（已 L2 归一化）
        logits     = user_repr · item_repr.T / τ    # [B, B]，对角线=正样本相似度
        labels     = arange(B)                      # 每个用户的正样本就是同行的物品
        loss       = cross_entropy(logits, labels)

    直觉：同一 batch 里其他用户的正样本物品被当成当前用户的负样本，这就是
    「in-batch negatives」。相比离线固定采样，采样分布跟随数据分布，收敛更稳。

    注意事项：
      - `user_repr` 和 `item_repr` 都已在模型内部 L2 归一化，所以 logits 是余弦相似度。
      - 温度系数 τ 需要和余弦值域 [-1, 1] 匹配，太大（>1）学不动、太小（<0.01）梯度饱和。
      - 若同 batch 里多条样本对应同一 post，会出现「假负样本」；样本量足够大时可忽略，
        生产环境可考虑按 positive_post 去重后再组 batch。
    """
    model_config = make_model_config()
    model_config.initialize()
    model_config.fprop_dtype = jnp.float32  # 训练时用 float32 保证梯度精度
    model = model_config.make()

    # 用户塔：输入含全部 history 字段
    user_repr, _ = model.build_user_representation(batch, embeddings)  # [B, D]

    # 物品塔：只取候选位 0（正样本），squeeze 掉长度 1 的候选维
    pos_batch, pos_embeddings = _take_positive_candidate(batch, embeddings)
    item_repr, _ = model.build_candidate_representation(pos_batch, pos_embeddings)  # [B, 1, D]
    item_repr = item_repr[:, 0, :]  # [B, D]

    user_repr = user_repr.astype(jnp.float32)
    item_repr = item_repr.astype(jnp.float32)

    # In-batch 相似度矩阵（行：用户，列：batch 中所有用户的正样本物品）
    logits = jnp.matmul(user_repr, item_repr.T) / temperature  # [B, B]
    batch_size = logits.shape[0]
    labels = jnp.arange(batch_size)

    # 双向对比：user→item 和 item→user，两路对称，收敛更稳（SimCSE / CLIP 常用）
    log_prob_u2i = jax.nn.log_softmax(logits, axis=-1)
    log_prob_i2u = jax.nn.log_softmax(logits, axis=0)
    loss_u2i = -jnp.mean(log_prob_u2i[jnp.arange(batch_size), labels])
    loss_i2u = -jnp.mean(log_prob_i2u[labels, jnp.arange(batch_size)])

    return 0.5 * (loss_u2i + loss_i2u)


# ── 数据加载 ──────────────────────────────────────────────────────────────────

def make_simulated_batch(batch_size: int) -> RecsysBatch:
    """
    生成模拟训练 batch（不需要真实数据）。
    召回训练不需要 labels——正样本就是候选位 0，负样本来自 in-batch。
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
    return batch


def load_parquet_batch(parquet_path: str, batch_size: int) -> RecsysBatch:
    """
    从 Parquet 文件加载一个 batch。

    字段与 data_preprocessor._TRAIN_SAMPLE_SCHEMA 对齐。
    召回训练用不到 labels 列，这里直接忽略。
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("读取 Parquet 需要安装 pyarrow：uv add pyarrow")

    table = pq.read_table(parquet_path)
    df = table.to_pydict()
    n = min(batch_size, len(df["user_hashes"]))

    return RecsysBatch(
        user_hashes=np.array(df["user_hashes"][:n], dtype=np.int32),
        history_post_hashes=np.array(df["history_post_hashes"][:n], dtype=np.int32),
        history_author_hashes=np.array(df["history_author_hashes"][:n], dtype=np.int32),
        history_actions=np.array(df["history_actions"][:n], dtype=np.float32),
        history_product_surface=np.array(df["history_product_surface"][:n], dtype=np.int32),
        candidate_post_hashes=np.array(df["candidate_post_hashes"][:n], dtype=np.int32),
        candidate_author_hashes=np.array(df["candidate_author_hashes"][:n], dtype=np.int32),
        candidate_product_surface=np.array(df["candidate_product_surface"][:n], dtype=np.int32),
    )


def iter_parquet_dir(data_dir: str, batch_size: int):
    """遍历目录下所有 Parquet 文件，逐 batch 产出 RecsysBatch。"""
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
    path = os.path.join(ckpt_dir, f"retrieval_params_step{step}.npz")
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

    logger.info("=== Phoenix 召回模型训练开始 ===")
    logger.info(f"数据来源：{'模拟数据' if args.data_dir is None else args.data_dir}")
    logger.info(
        f"训练步数：{args.steps}，batch size：{args.batch_size}，"
        f"lr：{args.lr}，温度 τ：{args.temperature}"
    )

    if args.batch_size < 2:
        raise ValueError("召回训练需要 batch_size >= 2，否则 in-batch negatives 没有负样本")

    # 1. 初始化或加载嵌入表（与精排训练复用同一张表）
    emb_path = os.path.join(args.ckpt_dir, "embedding_tables.npz")
    if args.resume_emb and os.path.exists(emb_path):
        user_emb, post_emb, author_emb = load_embedding_tables(emb_path)
        logger.info(f"已加载嵌入表：{emb_path}")
    else:
        user_emb, post_emb, author_emb = init_embedding_tables()
        logger.info("随机初始化嵌入表")

    # 2. 初始化模型参数
    #    用闭包把 temperature 固化进去，避免被 jit 当成非静态参数
    temperature = args.temperature

    def loss_wrapper(batch, embeddings):
        return loss_fn(batch, embeddings, temperature=temperature)

    loss_transform = hk.without_apply_rng(hk.transform(loss_wrapper))

    dummy_batch = make_simulated_batch(batch_size=max(2, args.batch_size))
    dummy_embeddings = lookup_embeddings(dummy_batch, user_emb, post_emb, author_emb)

    rng = jax.random.PRNGKey(42)
    params = loss_transform.init(rng, dummy_batch, dummy_embeddings)

    if args.resume_params and os.path.exists(args.resume_params):
        params = load_checkpoint(args.resume_params)
        logger.info(f"已加载模型参数：{args.resume_params}")

    # 3. 初始化优化器
    optimizer = optax.adam(learning_rate=args.lr)
    opt_state = optimizer.init(params)

    # 4. JIT 编译 train_step
    @jax.jit
    def train_step(params, opt_state, batch, embeddings):
        loss_val, grads = jax.value_and_grad(
            lambda p: loss_transform.apply(p, batch, embeddings)
        )(params)
        updates, new_opt_state = optimizer.update(grads, opt_state)
        new_params = optax.apply_updates(params, updates)
        return new_params, new_opt_state, loss_val

    # 5. 训练循环
    logger.info("开始训练循环（首次 step 因 JIT 编译会较慢）...")
    step = 0
    loss_accum = 0.0

    while step < args.steps:
        if args.data_dir is not None:
            data_iter = iter_parquet_dir(args.data_dir, args.batch_size)
        else:
            data_iter = (make_simulated_batch(args.batch_size) for _ in range(args.steps))

        for batch in data_iter:
            if step >= args.steps:
                break

            # in-batch negatives 要求 batch 至少有 2 条样本；parquet 末尾可能不足
            if batch.user_hashes.shape[0] < 2:
                logger.debug("跳过 batch_size<2 的尾批（in-batch 需要至少 2 个样本）")
                continue

            embeddings = lookup_embeddings(batch, user_emb, post_emb, author_emb)

            params, opt_state, loss_val = train_step(params, opt_state, batch, embeddings)
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
    logger.info(f"  模型参数：{args.ckpt_dir}/retrieval_params_step{step}.npz")
    logger.info(f"  嵌入表  ：{emb_path}")
    logger.info("下一步：")
    logger.info("  1) 用物品塔离线对全库 post 预计算向量 → 灌入 FAISS/ScaNN")
    logger.info("  2) 在线请求用用户塔实时编码 user_repr，点积取 top-k 送入精排")


# ── 入口 ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Phoenix 召回模型训练（双塔 + in-batch negatives）")
    parser.add_argument(
        "--data-dir", type=str, default=None,
        help="Parquet 训练数据目录（不传则使用模拟数据；与 train_ranker 共用同一份 parquet）",
    )
    parser.add_argument("--ckpt-dir", type=str, default="./checkpoints_retrieval", help="检查点保存目录")
    parser.add_argument("--steps", type=int, default=200, help="训练总步数")
    parser.add_argument(
        "--batch-size", type=int, default=32,
        help="每步 batch 大小；召回对比学习建议较大（64+），以获得更强的 in-batch 负样本信号",
    )
    parser.add_argument("--lr", type=float, default=1e-4, help="Adam 学习率")
    parser.add_argument(
        "--temperature", type=float, default=DEFAULT_TEMPERATURE,
        help="对比学习温度系数 τ，logits 会除以它（默认 0.05）",
    )
    parser.add_argument("--log-every", type=int, default=20, help="每隔多少步打印一次 loss")
    parser.add_argument("--save-every", type=int, default=100, help="每隔多少步保存一次检查点")
    parser.add_argument("--resume-params", type=str, default=None, help="从此路径加载模型参数继续训练")
    parser.add_argument("--resume-emb", action="store_true", help="从 ckpt-dir 加载嵌入表继续训练")
    args = parser.parse_args()

    train(args)
