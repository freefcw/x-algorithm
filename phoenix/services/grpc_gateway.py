# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
Phoenix gRPC 网关 — home-mixer 与 Phoenix 模型之间的桥。

实现 proto/definitions/phoenix_recsys.proto 定义的两个 gRPC 服务：
    1. PhoenixPredictionService.PredictNextActions —— 精排：
       输入用户行为序列 + 候选帖子，输出每条帖子上 18 种行为的概率。
    2. PhoenixRetrievalService.Retrieve —— 召回：
       输入用户行为序列，从候选池中检索 Top-K 帖子（网外召回）。

与 HTTP 服务（ranker_service / retrieval_service）的区别：
    - HTTP 服务面向人和外部系统调试，字段是字符串 ID；
    - 本网关面向 home-mixer（Rust），协议、字段、概率格式严格对齐 proto 契约，
      并且真正消费请求里的用户行为序列（而不是 mock 特征）。

启动方式:
    uv run scripts/run_grpc_gateway.py                       # 随机权重（演示）
    uv run scripts/run_grpc_gateway.py \
        --ranker-checkpoint checkpoints/step-000200             # 加载完整训练产物
    uv run scripts/run_grpc_gateway.py \
        --retrieval-checkpoint checkpoints_retrieval/retrieval_params_step200.npz \
        --corpus-path indexes/retrieval_index.npz \
        --corpus-refresh-seconds 300                            # 真实候选池 + 定时热替换

候选池：不传 --corpus-path 时合成演示 ID（业务侧水合不到，只能本地跑通链路）；
传入 scripts/build_retrieval_index.py 产出的索引后，召回返回真实帖子 ID，索引文件
被替换后按 --corpus-refresh-seconds 周期热加载（见 services/retrieval_index.py）。

默认仅监听本机 127.0.0.1:50053（可用 --host/--port 或环境变量
PHOENIX_GRPC_HOST/PHOENIX_GRPC_PORT 修改）。如需供远程客户端调用，应在受控网络
边界内显式传入绑定地址，并配置相应的访问控制。
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import threading
import time
from concurrent import futures
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

import jax
import jax.numpy as jnp
import numpy as np

from data_preprocessor import hash_id_to_ints
from grok import TransformerConfig
from recsys_model import HashConfig, PhoenixModelConfig, RecsysBatch, RecsysEmbeddings
from recsys_retrieval_model import PhoenixRetrievalModelConfig
from runners import (
    ACTIONS,
    ModelRunner,
    RecsysInferenceRunner,
    RecsysRetrievalInferenceRunner,
    RetrievalModelRunner,
)
from services.inference_types import (
    CandidatePrediction,
    HistoryFeatures,
)
from services.model_contract import (
    ACTION_IDX_TO_ENUM,
    FEATURE_SCHEMA,
    NONZERO_WEIGHT_ACTION_ENUMS,
    supported_actions_header,
)
from services.recsys_proto import load_proto_modules
from services.retrieval_index import RetrievalIndex, RetrievalIndexError, now_ms

logger = logging.getLogger("grpc_gateway")

# ── 超参（与 scripts/train_ranker.py / data_preprocessor.py 保持一致）────────
EMB_SIZE = 128
HISTORY_LEN = 32
NUM_ACTIONS = len(ACTIONS)  # 19
TABLE_SIZE = 100_000
SURFACE_VOCAB = 16
NUM_HASHES = 2
RANK_CHUNK = 32  # 精排一行的候选槽位数；候选按 32 一行折成 [B, 32]
# 精排一次前向的行数桶：B 向上取到最近的桶，jit 只为这几种形状编译（桶内多余行
# 是全零 padding，输出丢弃）。最大桶 32 行 = 1024 条候选，超出的请求分多次前向。
RANK_BATCH_BUCKETS = (1, 2, 4, 8, 16, 32)
# 启动时预编译到多大的桶（含）。每个桶编译约 0.2-0.4 s（CPU），全部预热让首个大请求
# （home-mixer 一次送上千条候选很常见）不承担编译耗时。
RANK_WARMUP_MAX_BUCKET = RANK_BATCH_BUCKETS[-1]

# Python ACTIONS 下标 → proto ActionName 枚举值。
# 注意两边顺序不同：例如 quote 在 Python 里是下标 11，在 proto 里是枚举值 4。
LOG_PROBS_LEN = 19        # ActionName 枚举 0..=18
CONTINUOUS_LEN = 2        # ContinuousActionName 枚举 0..=1（1 = DWELL_TIME）
MIN_PROB = 1e-9

# Cross-language serving contract.  home-mixer rejects a response when any of
# these values is missing or incompatible with the request-side feature
# mapping.  Keep this explicit instead of inferring readiness from a model
# filename or from a successful gRPC call.
# v2: proto carries business string IDs; both training (data_preprocessor) and serving
# hash the same string, so no integer ID mapping exists anywhere in the chain.
SUPPORTED_ACTIONS = supported_actions_header()

def demo_object_id(index: int) -> str:
    """Return a deterministic ObjectId-shaped demo ID.

    The index is only used to make the local corpus reproducible.  Business
    IDs remain strings; no timestamp or ordering is encoded in the ID.
    """
    import hashlib

    return hashlib.md5(f"phoenix-demo-post-{index}".encode(), usedforsecurity=False).hexdigest()[:24]


def demo_author_id(index: int) -> str:
    """Return the same padded ObjectId used by Rust demo adapters."""
    return f"{201 + (index % 40):024x}"


# ── 嵌入表 ────────────────────────────────────────────────────────────────────


class EmbeddingTables:
    """三张哈希嵌入表（用户/帖子/作者），第 0 行为 padding（全零）。

    与 scripts/train_ranker.py 的 init/save 格式完全一致：
    未提供文件时随机初始化（seed=0），提供 embedding_tables.npz 时加载训练产物。
    """

    def __init__(self, path: Optional[str] = None):
        if path:
            tables = np.load(path)
            self.user = tables["user_emb_table"]
            self.post = tables["post_emb_table"]
            self.author = tables["author_emb_table"]
            logger.info("已加载嵌入表: %s", path)
        else:
            rng = np.random.default_rng(0)
            self.user = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
            self.post = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
            self.author = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
            self.user[0] = self.post[0] = self.author[0] = 0.0
            logger.warning("未提供 --emb-tables，随机初始化嵌入表（仅用于演示）")

    def lookup(self, batch: RecsysBatch) -> RecsysEmbeddings:
        return RecsysEmbeddings(
            user_embeddings=self.user[np.asarray(batch.user_hashes, dtype=np.intp)],
            history_post_embeddings=self.post[
                np.asarray(batch.history_post_hashes, dtype=np.intp)
            ],
            candidate_post_embeddings=self.post[
                np.asarray(batch.candidate_post_hashes, dtype=np.intp)
            ],
            history_author_embeddings=self.author[
                np.asarray(batch.history_author_hashes, dtype=np.intp)
            ],
            candidate_author_embeddings=self.author[
                np.asarray(batch.candidate_author_hashes, dtype=np.intp)
            ],
        )


# ── 特征转换：proto → RecsysBatch ────────────────────────────────────────────


def uas_to_history(uas) -> HistoryFeatures:
    """把 proto UserActionSequence 转成模型需要的历史特征（B=1）。

    proto 的 action_mask 位下标对应 ActionName 枚举值，
    这里转换为 Python ACTIONS 下标顺序的 0/1 向量。
    """
    post_hashes = np.zeros((1, HISTORY_LEN, NUM_HASHES), dtype=np.int32)
    author_hashes = np.zeros((1, HISTORY_LEN, NUM_HASHES), dtype=np.int32)
    actions = np.zeros((1, HISTORY_LEN, NUM_ACTIONS), dtype=np.float32)
    surface = np.zeros((1, HISTORY_LEN), dtype=np.int32)

    records = []
    if uas is not None and uas.HasField("user_actions_data"):
        container = uas.user_actions_data
        if container.HasField("ordered_aggregated_user_actions_list"):
            records = list(container.ordered_aggregated_user_actions_list.aggregated_user_actions)

    # 只保留最近 HISTORY_LEN 条（序列按时间升序，取尾部）
    records = records[-HISTORY_LEN:]

    for i, rec in enumerate(records):
        post_hashes[0, i] = hash_id_to_ints(rec.tweet_id, NUM_HASHES, TABLE_SIZE)
        author_hashes[0, i] = hash_id_to_ints(rec.author_id, NUM_HASHES, TABLE_SIZE)
        surface[0, i] = rec.product_surface % SURFACE_VOCAB
        mask = list(rec.action_mask)
        for py_idx, enum_val in enumerate(ACTION_IDX_TO_ENUM):
            if enum_val < len(mask) and mask[enum_val]:
                actions[0, i, py_idx] = 1.0

    return HistoryFeatures(post_hashes, author_hashes, actions, surface)


def build_batch(
    user_id: str,
    history: HistoryFeatures,
    candidate_post_ids: Sequence[str],
    candidate_author_ids: Sequence[str],
    num_candidates: int,
) -> RecsysBatch:
    """组装 RecsysBatch（B=1，候选不足 num_candidates 时补 padding）。

    所有 ID 都是业务字符串，与 data_preprocessor 训练侧用同一个 hash_id_to_ints。
    """
    return build_batch_rows(
        user_id,
        history,
        [list(zip(candidate_post_ids, candidate_author_ids))],
        num_candidates,
    )


def build_batch_rows(
    user_id: str,
    history: HistoryFeatures,
    rows: Sequence[Sequence[Tuple[str, str]]],
    num_candidates: int,
    num_rows: Optional[int] = None,
) -> RecsysBatch:
    """把同一用户的多组候选折成 [B, num_candidates] 的一个 batch。

    每行是一组候选 (post_id, author_id)，用户 / 历史特征按行复制；`rows` 不足
    `num_rows` 时补全零 padding 行，调用方丢弃这些行的输出。模型内候选之间、行之间
    互不注意，所以折行不改变任何一条候选的分数，只是把多次前向合成一次。
    """
    num_rows = len(rows) if num_rows is None else num_rows
    if num_rows < len(rows):
        raise ValueError(f"num_rows={num_rows} 小于候选行数 {len(rows)}")
    cand_post = np.zeros((num_rows, num_candidates, NUM_HASHES), dtype=np.int32)
    cand_author = np.zeros((num_rows, num_candidates, NUM_HASHES), dtype=np.int32)
    cand_surface = np.zeros((num_rows, num_candidates), dtype=np.int32)

    for r, row in enumerate(rows):
        if len(row) > num_candidates:
            raise ValueError(f"第 {r} 行有 {len(row)} 条候选，超过槽位 {num_candidates}")
        for i, (post_id, author_id) in enumerate(row):
            cand_post[r, i] = hash_id_to_ints(post_id, NUM_HASHES, TABLE_SIZE)
            cand_author[r, i] = hash_id_to_ints(author_id, NUM_HASHES, TABLE_SIZE)

    user_hashes = np.repeat(
        np.array([hash_id_to_ints(user_id, NUM_HASHES, TABLE_SIZE)], dtype=np.int32),
        num_rows,
        axis=0,
    )

    return RecsysBatch(
        user_hashes=user_hashes,
        history_post_hashes=np.repeat(history.post_hashes, num_rows, axis=0),
        history_author_hashes=np.repeat(history.author_hashes, num_rows, axis=0),
        history_actions=np.repeat(history.actions, num_rows, axis=0),
        history_product_surface=np.repeat(history.product_surface, num_rows, axis=0),
        candidate_post_hashes=cand_post,
        candidate_author_hashes=cand_author,
        candidate_product_surface=cand_surface,
    )


def empty_history() -> HistoryFeatures:
    """没有任何行为记录的历史特征（B=1）。"""
    return HistoryFeatures(
        post_hashes=np.zeros((1, HISTORY_LEN, NUM_HASHES), dtype=np.int32),
        author_hashes=np.zeros((1, HISTORY_LEN, NUM_HASHES), dtype=np.int32),
        actions=np.zeros((1, HISTORY_LEN, NUM_ACTIONS), dtype=np.float32),
        product_surface=np.zeros((1, HISTORY_LEN), dtype=np.int32),
    )


def rank_bucket(num_rows: int) -> int:
    """候选行数向上取到最近的批次桶；超过最大桶由调用方分批。"""
    for bucket in RANK_BATCH_BUCKETS:
        if bucket >= num_rows:
            return bucket
    raise ValueError(f"{num_rows} 行超过最大批次桶 {RANK_BATCH_BUCKETS[-1]}")


def _make_transformer_config() -> TransformerConfig:
    return TransformerConfig(
        emb_size=EMB_SIZE,
        widening_factor=2,
        key_size=64,
        num_q_heads=2,
        num_kv_heads=2,
        num_layers=2,
        attn_output_multiplier=0.125,
    )


def _hash_config() -> HashConfig:
    return HashConfig(
        num_user_hashes=NUM_HASHES,
        num_item_hashes=NUM_HASHES,
        num_author_hashes=NUM_HASHES,
    )


# ── 精排引擎 ──────────────────────────────────────────────────────────────────


class RankerEngine:
    """把 (UAS, 候选列表) 送进精排模型，返回逐候选的行为概率。

    候选按 `RANK_CHUNK` 折成多行、一次前向（见 `build_batch_rows`），前向函数用
    `jax.jit` 编译并在启动时对小批次桶预热，避免 eager 逐算子派发和首个请求承担
    编译耗时。推理仍在引擎锁内串行；并发靠多副本。
    """

    def __init__(
        self,
        tables: EmbeddingTables,
        checkpoint_path: Optional[str] = None,
        warmup_max_bucket: int = RANK_WARMUP_MAX_BUCKET,
    ):
        self._tables = tables
        self._lock = threading.Lock()

        model_config = PhoenixModelConfig(
            emb_size=EMB_SIZE,
            num_actions=NUM_ACTIONS,
            history_seq_len=HISTORY_LEN,
            candidate_seq_len=RANK_CHUNK,
            hash_config=_hash_config(),
            product_surface_vocab_size=SURFACE_VOCAB,
            model=_make_transformer_config(),
        )
        runner = RecsysInferenceRunner(
            runner=ModelRunner(model=model_config, bs_per_device=0.125),
            name="grpc_ranker",
        )
        runner.initialize(checkpoint_path=checkpoint_path)

        if checkpoint_path is not None:
            self.model_version = os.path.basename(checkpoint_path)
            logger.info("精排模型已加载检查点: %s", checkpoint_path)
        else:
            self.model_version = "random"
            logger.warning("精排模型使用随机初始化（未提供 --ranker-checkpoint）")

        self._runner = runner
        self._rank_jit = jax.jit(runner.rank_candidates)
        self._warm_up(warmup_max_bucket)

    def _warm_up(self, max_bucket: int) -> None:
        """为 <= max_bucket 的批次桶各编译一次，让首个真实请求不承担 jit 耗时。"""
        history = empty_history()
        for bucket in RANK_BATCH_BUCKETS:
            if bucket > max_bucket:
                break
            started = time.time()
            batch = build_batch_rows("", history, [], RANK_CHUNK, num_rows=bucket)
            output = self._rank_jit(self._runner.params, batch, self._tables.lookup(batch))
            jax.block_until_ready(output.scores)
            logger.info(
                "精排前向已编译：%d 行 x %d 候选（%.1f s）",
                bucket,
                RANK_CHUNK,
                time.time() - started,
            )

    def _rank_rows(
        self, user_id: str, history: HistoryFeatures, rows: Sequence[Sequence[Tuple[str, str]]]
    ) -> Tuple[np.ndarray, Optional[np.ndarray]]:
        """一次前向打完 rows 里的所有候选，返回 [B, RANK_CHUNK, ...] 的概率与连续预测。"""
        bucket = rank_bucket(len(rows))
        batch = build_batch_rows(user_id, history, rows, RANK_CHUNK, num_rows=bucket)
        embeddings = self._tables.lookup(batch)
        output = self._rank_jit(self._runner.params, batch, embeddings)
        probs = np.asarray(output.scores, dtype=np.float64)
        continuous = (
            None
            if output.continuous_preds is None
            else np.asarray(output.continuous_preds, dtype=np.float64)
        )
        return probs, continuous

    def predict(
        self, user_id: str, uas, candidates: Sequence[Tuple[str, str]]
    ) -> List[CandidatePrediction]:
        """返回每个候选的离散行为概率和可选连续预测，顺序与输入一致。"""
        history = uas_to_history(uas)
        predictions: List[CandidatePrediction] = []
        max_per_pass = RANK_BATCH_BUCKETS[-1] * RANK_CHUNK

        with self._lock:
            for start in range(0, len(candidates), max_per_pass):
                window = candidates[start : start + max_per_pass]
                rows = [window[i : i + RANK_CHUNK] for i in range(0, len(window), RANK_CHUNK)]
                probs, continuous = self._rank_rows(user_id, history, rows)
                for r, row in enumerate(rows):
                    for i in range(len(row)):
                        predictions.append(
                            CandidatePrediction(
                                action_probs=probs[r, i],
                                continuous_values=(
                                    None if continuous is None else continuous[r, i]
                                ),
                            )
                        )

        return predictions


# ── 召回引擎 ──────────────────────────────────────────────────────────────────


class RetrievalEngine:
    """双塔召回：用户塔编码 UAS，在候选池上做 Top-K 点积检索。

    候选池有两种来源：
    - ``corpus_path``：`scripts/build_retrieval_index.py` 离线用候选塔把真实帖子编码后
      写出的索引文件（`services/retrieval_index.py`）。启动时加载失败直接报错；之后
      可由 `refresh_from_path()` 按文件 mtime 热替换，替换在检索锁内原子完成。
    - 未提供时合成 ``corpus_size`` 条演示 ID 并现场编码。演示 ID 在业务侧水合不到，
      只用于本地跑通链路。

    索引必须由与本引擎相同 ``model_version`` 的检查点编码，否则拒绝加载。
    检索本身是对全量向量的暴力点积 + Top-K，十万级候选在 CPU 上是毫秒量级。
    """

    def __init__(
        self,
        tables: EmbeddingTables,
        checkpoint_path: Optional[str] = None,
        corpus_size: int = 2000,
        corpus_path: Optional[str] = None,
    ):
        self._tables = tables
        self._lock = threading.Lock()
        self._post_ids: List[str] = []
        self._author_ids: List[str] = []
        self.corpus_version = "empty"
        self._corpus_path = Path(corpus_path) if corpus_path else None
        self._corpus_mtime_ns: Optional[int] = None

        model_config = PhoenixRetrievalModelConfig(
            emb_size=EMB_SIZE,
            history_seq_len=HISTORY_LEN,
            candidate_seq_len=RANK_CHUNK,
            hash_config=_hash_config(),
            product_surface_vocab_size=SURFACE_VOCAB,
            model=_make_transformer_config(),
        )
        runner = RecsysRetrievalInferenceRunner(
            runner=RetrievalModelRunner(model=model_config, bs_per_device=0.125),
            name="grpc_retrieval",
        )
        runner.initialize(checkpoint_path=checkpoint_path)

        if checkpoint_path is not None:
            self.model_version = os.path.basename(checkpoint_path)
            logger.info("召回模型已加载检查点: %s", checkpoint_path)
        else:
            self.model_version = "random"
            logger.warning("召回模型使用随机初始化（未提供 --retrieval-checkpoint）")

        self._runner = runner
        if self._corpus_path is not None:
            # 启动阶段的索引问题必须让进程起不来，而不是带着空池对外服务。
            self.refresh_from_path(force=True)
        elif corpus_size > 0:
            self._build_demo_corpus(corpus_size)
        else:
            logger.warning("召回候选池为空（corpus_size=0 且未提供索引），Retrieve 将返回 0 条")

    # ── 候选池构建 ────────────────────────────────────────────────────────────

    def encode_posts(self, post_ids: Sequence[str], author_ids: Sequence[str]) -> np.ndarray:
        """用候选塔把 (post_id, author_id) 编码成归一化向量，返回 float32 [N, D]。

        离线索引构建（`scripts/build_retrieval_index.py`）和演示候选池走同一条路径，
        保证离线向量与在线用户塔来自同一份参数和同一套哈希。
        """
        if len(post_ids) != len(author_ids):
            raise ValueError("post_ids 与 author_ids 长度必须一致")
        if not post_ids:
            return np.zeros((0, EMB_SIZE), dtype=np.float32)

        history = empty_history()
        reps = []
        for start in range(0, len(post_ids), RANK_CHUNK):
            ids = list(post_ids[start : start + RANK_CHUNK])
            authors = list(author_ids[start : start + RANK_CHUNK])
            batch = build_batch("", history, ids, authors, RANK_CHUNK)
            embeddings = self._tables.lookup(batch)
            rep = np.asarray(self._runner.encode_candidates(batch, embeddings)[0])
            reps.append(rep[: len(ids)])
        return np.concatenate(reps, axis=0).astype(np.float32)

    def _build_demo_corpus(self, corpus_size: int) -> None:
        """合成演示候选池并用候选塔编码成向量。"""
        # Keep corpus identities on the same 24-hex wire contract as Home
        # Mixer.  Decimal IDs here would be rejected by PhoenixSource and make
        # the local retrieval path silently empty.
        post_ids = [demo_object_id(i) for i in range(corpus_size)]
        author_ids = [demo_author_id(i) for i in range(corpus_size)]
        logger.info("正在编码演示候选池（%d 条帖子）...", corpus_size)
        index = RetrievalIndex(
            post_ids=tuple(post_ids),
            author_ids=tuple(author_ids),
            embeddings=self.encode_posts(post_ids, author_ids),
            model_version=self.model_version,
            built_at_ms=now_ms(),
        )
        self.install_index(index, source="demo")

    def install_index(self, index: RetrievalIndex, source: str) -> None:
        """在检索锁内原子替换候选池；模型版本或向量维度不匹配时拒绝并保留旧池。"""
        if index.model_version != self.model_version:
            raise RetrievalIndexError(
                f"索引 {source} 由模型 {index.model_version!r} 编码，"
                f"当前召回模型是 {self.model_version!r}；请用同一 checkpoint 重建索引"
            )
        if index.dim != EMB_SIZE:
            raise RetrievalIndexError(
                f"索引 {source} 的向量维度 {index.dim} 与模型 EMB_SIZE={EMB_SIZE} 不一致"
            )
        corpus_version = f"{index.model_version}@{index.built_at_ms}:{len(index)}"
        with self._lock:
            self._post_ids = list(index.post_ids)
            self._author_ids = list(index.author_ids)
            # 位置下标只在本引擎内映射回 post_id，int32 足够且避免 x64 告警。
            self._runner.set_corpus(
                jnp.asarray(index.embeddings), jnp.arange(len(index), dtype=jnp.int32)
            )
            self.corpus_version = corpus_version
        logger.info("候选池就绪（%s）：%s", source, index.describe())

    def refresh_from_path(self, force: bool = False) -> bool:
        """索引文件 mtime 变化时重新加载；返回是否替换了候选池。

        加载或校验失败会抛出 `RetrievalIndexError`，调用方决定是启动失败（首次加载）
        还是记日志继续用旧池（定时刷新）。
        """
        if self._corpus_path is None:
            return False
        try:
            mtime_ns = os.stat(self._corpus_path).st_mtime_ns
        except OSError as exc:
            raise RetrievalIndexError(f"无法读取索引 {self._corpus_path}: {exc}") from exc
        if not force and mtime_ns == self._corpus_mtime_ns:
            return False
        index = RetrievalIndex.load(self._corpus_path)
        self.install_index(index, source=str(self._corpus_path))
        self._corpus_mtime_ns = mtime_ns
        return True

    @property
    def corpus_size(self) -> int:
        return len(self._post_ids)

    # ── 在线检索 ──────────────────────────────────────────────────────────────

    def retrieve(self, user_id: str, uas, max_results: int) -> List[Tuple[str, str, float]]:
        """返回 [(post_id, author_id, score)]，按相似度降序；候选池为空时返回空列表。"""
        history = uas_to_history(uas)

        with self._lock:
            top_k = min(max(max_results, 1), len(self._post_ids))
            if top_k == 0:
                return []
            batch = build_batch(user_id, history, [], [], RANK_CHUNK)
            embeddings = self._tables.lookup(batch)
            output = self._runner.retrieve(batch, embeddings, top_k=top_k)
            indices = np.asarray(output.top_k_indices[0])
            scores = np.asarray(output.top_k_scores[0])
            results = [
                (self._post_ids[int(idx)], self._author_ids[int(idx)], float(score))
                for idx, score in zip(indices, scores)
            ]
        return results


class CorpusRefresher:
    """后台线程：每隔 ``interval_seconds`` 检查一次索引文件，变化则热替换。

    刷新失败只记日志并保留旧候选池；连续失败会持续告警，由运维侧处理索引任务。
    """

    def __init__(self, engine: RetrievalEngine, interval_seconds: float):
        if interval_seconds <= 0:
            raise ValueError("interval_seconds 必须为正数")
        self._engine = engine
        self._interval = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="retrieval-corpus-refresher", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout: Optional[float] = None) -> None:
        self._stop.set()
        self._thread.join(timeout)

    def refresh_once(self) -> bool:
        try:
            refreshed = self._engine.refresh_from_path()
        except RetrievalIndexError as exc:
            logger.error("候选池刷新失败，继续使用旧索引：%s", exc)
            return False
        except Exception:  # noqa: BLE001 - 刷新线程不能因为未知异常退出
            logger.exception("候选池刷新出现未预期错误，继续使用旧索引")
            return False
        if refreshed:
            logger.info("候选池已热替换：%s", self._engine.corpus_version)
        return refreshed

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            self.refresh_once()


# ── gRPC 服务实现 ─────────────────────────────────────────────────────────────


def create_servicers(
    recsys_pb2,
    recsys_pb2_grpc,
    ranker: Any,
    retrieval: Any,
    supported_action_enums: Sequence[int] | None = None,
    continuous_dwell_supported: bool = True,
):
    """构造两个 servicer（在函数内定义类，因为基类来自运行时生成的模块）。"""
    # 没有 checkpoint metadata（随机权重 / 旧 checkpoint）时按 home-mixer 必需的
    # head 集合广播，来源是 model_contract 的唯一定义，不在这里另抄一份。
    supported = set(
        NONZERO_WEIGHT_ACTION_ENUMS
        if supported_action_enums is None
        else supported_action_enums
    )
    supported_actions = ",".join(str(value) for value in sorted(supported))

    def set_contract_metadata(context, engine) -> None:
        metadata = [
            ("feature-schema", FEATURE_SCHEMA),
            ("model-version", engine.model_version),
            ("random-weights", str(engine.model_version == "random").lower()),
            ("supported-actions", supported_actions),
        ]
        # 召回引擎额外报告当前候选池版本（`model@built_at_ms:size`），便于从
        # home-mixer 日志反查是哪一份索引回答了请求。home-mixer 只校验上面四项。
        corpus_version = getattr(engine, "corpus_version", None)
        if corpus_version:
            metadata.append(("corpus-version", str(corpus_version)))
        context.set_trailing_metadata(tuple(metadata))

    class PredictionServicer(recsys_pb2_grpc.PhoenixPredictionServiceServicer):
        def PredictNextActions(self, request, context):
            start = time.time()
            set_contract_metadata(context, ranker)
            candidates = [(c.tweet_id, c.author_id) for c in request.candidates]
            predictions = ranker.predict(
                request.user_id, request.user_action_sequence, candidates
            )

            distributions = []
            for (tweet_id, author_id), prediction in zip(candidates, predictions):
                probs = prediction.action_probs
                # 概率 → log 概率，并按 ActionName 枚举值排列（下标 0 为 UNSPECIFIED 占位）
                top_log_probs = [math.log(MIN_PROB)] * LOG_PROBS_LEN
                for py_idx, enum_val in enumerate(ACTION_IDX_TO_ENUM):
                    if enum_val not in supported:
                        continue
                    p = float(np.clip(probs[py_idx], MIN_PROB, 1.0))
                    top_log_probs[enum_val] = math.log(p)

                # 连续值下标 1 = DWELL_TIME；旧模型没有连续头时兼容离散占位。
                dwell_time = float(probs[18]) if continuous_dwell_supported else 0.0
                if (
                    continuous_dwell_supported
                    and
                    prediction.continuous_values is not None
                    and len(prediction.continuous_values) > 1
                ):
                    dwell_time = float(prediction.continuous_values[1])
                continuous = [0.0, dwell_time]

                distributions.append(
                    recsys_pb2.CandidateDistribution(
                        candidate=recsys_pb2.TweetInfo(
                            tweet_id=tweet_id, author_id=author_id
                        ),
                        top_log_probs=top_log_probs,
                        continuous_actions_values=continuous,
                    )
                )

            elapsed_ms = (time.time() - start) * 1000
            logger.info(
                "PredictNextActions: user=%s candidates=%d (%.1f ms)",
                request.user_id,
                len(candidates),
                elapsed_ms,
            )
            return recsys_pb2.PredictNextActionsResponse(
                distribution_sets=[
                    recsys_pb2.DistributionSet(candidate_distributions=distributions)
                ]
            )

    class RetrievalServicer(recsys_pb2_grpc.PhoenixRetrievalServiceServicer):
        def Retrieve(self, request, context):
            start = time.time()
            set_contract_metadata(context, retrieval)
            results = retrieval.retrieve(
                request.user_id, request.user_action_sequence, request.max_results or 100
            )

            candidates = [
                recsys_pb2.ScoredCandidate(
                    candidate=recsys_pb2.TweetInfo(tweet_id=post_id, author_id=author_id),
                    score=score,
                )
                for post_id, author_id, score in results
            ]

            elapsed_ms = (time.time() - start) * 1000
            logger.info(
                "Retrieve: user=%s returned=%d (%.1f ms)",
                request.user_id,
                len(candidates),
                elapsed_ms,
            )
            return recsys_pb2.RetrieveResponse(
                top_k_candidates=[recsys_pb2.ScoredCandidates(candidates=candidates)]
            )

    return PredictionServicer(), RetrievalServicer()


def load_ranker_contract(checkpoint_path: str | None) -> tuple[list[int] | None, bool]:
    """读取 checkpoint bundle 的 metadata；旧 checkpoint 默认兼容全部行为。"""
    if checkpoint_path is None:
        return None, True
    checkpoint = Path(checkpoint_path)
    bundle_dir = checkpoint if checkpoint.is_dir() else checkpoint.parent
    metadata_candidates = [bundle_dir / "metadata.json"]
    if not checkpoint.is_dir():
        match = re.search(r"model_params_step(\d+)", checkpoint.name)
        if match:
            metadata_candidates.insert(
                0, checkpoint.with_name(f"metadata_step{match.group(1)}.json")
            )
    metadata_path = next((path for path in metadata_candidates if path.exists()), None)
    if metadata_path is None:
        return None, True
    try:
        with metadata_path.open(encoding="utf-8") as f:
            metadata = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("无法读取 ranker metadata %s，兼容全部行为：%s", metadata_path, exc)
        return None, True
    supported = metadata.get("supported_action_enums")
    if supported is not None:
        supported = [int(value) for value in supported]
    observed = metadata.get("observed_actions")
    continuous = True if observed is None else "dwell_time" in observed
    return supported, continuous


def resolve_ranker_checkpoint(
    ranker_checkpoint: str | None, emb_tables_path: str | None
) -> tuple[str | None, str | None]:
    """解析 ranker bundle，并拒绝参数和 embedding 的跨版本组合。"""
    if ranker_checkpoint is None:
        return None, emb_tables_path

    checkpoint = Path(ranker_checkpoint)
    if checkpoint.is_dir():
        metadata_path = checkpoint / "metadata.json"
        params_path = checkpoint / "model_params.npz"
        bundle_embedding = checkpoint / "embedding_tables.npz"
        if not metadata_path.exists() or not params_path.exists() or not bundle_embedding.exists():
            raise FileNotFoundError(
                f"ranker checkpoint bundle 不完整，需要 {params_path.name}、"
                f"{bundle_embedding.name}、{metadata_path.name}"
            )
        with metadata_path.open(encoding="utf-8") as f:
            metadata = json.load(f)
        if (
            metadata.get("model_params") != params_path.name
            or metadata.get("embedding_tables") != bundle_embedding.name
        ):
            raise ValueError("checkpoint metadata 与 bundle 文件名不一致")
        if emb_tables_path is not None and Path(emb_tables_path).resolve() != bundle_embedding.resolve():
            raise ValueError("--ranker-checkpoint 目录与 --emb-tables 不属于同一个 checkpoint bundle")
        return str(params_path), str(bundle_embedding)

    if not checkpoint.exists():
        raise FileNotFoundError(f"ranker checkpoint 不存在：{checkpoint}")

    metadata_candidates = [checkpoint.parent / "metadata.json"]
    param_match = re.search(r"model_params_step(\d+)", checkpoint.name)
    if param_match:
        metadata_candidates.insert(
            0, checkpoint.with_name(f"metadata_step{param_match.group(1)}.json")
        )
    metadata_path = next((path for path in metadata_candidates if path.exists()), None)
    if metadata_path is not None:
        with metadata_path.open(encoding="utf-8") as f:
            metadata = json.load(f)
        embedding_name = metadata.get("embedding_tables")
        if not isinstance(embedding_name, str) or not embedding_name:
            raise ValueError(f"checkpoint metadata 缺少 embedding_tables：{metadata_path}")
        expected = checkpoint.parent / embedding_name
        if not expected.is_file():
            raise FileNotFoundError(f"checkpoint bundle 缺少 embedding 表：{expected}")
        if emb_tables_path is None:
            emb_tables_path = str(expected)
        elif Path(emb_tables_path).resolve() != expected.resolve():
            raise ValueError("模型参数与 embedding 表不属于同一个 checkpoint bundle")
    else:
        # 兼容旧的平铺格式，同时防止可识别的 step 被错误混用。
        emb_match = re.search(r"embedding_tables_step(\d+)", Path(emb_tables_path or "").name)
        if param_match and emb_match and param_match.group(1) != emb_match.group(1):
            raise ValueError("模型参数与 embedding 表 step 不一致")
    return str(checkpoint), emb_tables_path


HEALTH_SERVICE_NAMES = (
    "",  # 整体状态；k8s gRPC 探针默认查询空服务名
    "recsys.PhoenixPredictionService",
    "recsys.PhoenixRetrievalService",
)

# 收到 SIGTERM 后等待在途请求完成的最长时间。精排单次预算 5 s（home-mixer 侧），
# 这里略大于它；必须小于部署平台的终止宽限期。
SHUTDOWN_GRACE_SECONDS = 8.0


def register_health_service(server):
    """注册标准 grpc.health.v1 服务并标记 SERVING。

    只在模型、候选池和 jit 预热全部完成后才会走到这里（`serve` 先构造引擎再开端口），
    所以就绪 = 端口可连 + 健康检查 SERVING；没有"端口开了但模型没加载"的窗口。
    """
    from grpc_health.v1 import health, health_pb2, health_pb2_grpc

    servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
    for name in HEALTH_SERVICE_NAMES:
        servicer.set(name, health_pb2.HealthCheckResponse.SERVING)
    return servicer


def install_shutdown_handlers(server, health_servicer, grace_seconds: float) -> None:
    """SIGTERM / SIGINT：先把健康检查置为 NOT_SERVING 让负载均衡摘流，再优雅停机。"""
    import signal

    if threading.current_thread() is not threading.main_thread():
        return

    def _shutdown(signum, _frame):
        logger.info(
            "收到信号 %s，健康检查置为 NOT_SERVING，%.0f s 内排空在途请求", signum, grace_seconds
        )
        if health_servicer is not None:
            health_servicer.enter_graceful_shutdown()
        server.stop(grace_seconds)

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _shutdown)


def serve(
    port: int = 50053,
    ranker_checkpoint: Optional[str] = None,
    retrieval_checkpoint: Optional[str] = None,
    emb_tables_path: Optional[str] = None,
    corpus_size: int = 2000,
    host: str = "127.0.0.1",
    corpus_path: Optional[str] = None,
    corpus_refresh_seconds: float = 0.0,
) -> None:
    import grpc

    recsys_pb2, recsys_pb2_grpc = load_proto_modules()

    ranker_checkpoint, emb_tables_path = resolve_ranker_checkpoint(
        ranker_checkpoint, emb_tables_path
    )
    tables = EmbeddingTables(emb_tables_path)
    logger.info("正在初始化精排模型...")
    ranker = RankerEngine(tables, ranker_checkpoint)
    logger.info("正在初始化召回模型...")
    retrieval = RetrievalEngine(tables, retrieval_checkpoint, corpus_size, corpus_path)
    if corpus_path is None:
        logger.warning(
            "召回使用合成演示候选池（未提供 --corpus-path），召回结果在业务侧水合不到"
        )
    refresher = None
    if corpus_path is not None and corpus_refresh_seconds > 0:
        refresher = CorpusRefresher(retrieval, corpus_refresh_seconds)

    supported_action_enums, continuous_dwell_supported = load_ranker_contract(
        ranker_checkpoint
    )

    prediction_servicer, retrieval_servicer = create_servicers(
        recsys_pb2,
        recsys_pb2_grpc,
        ranker,
        retrieval,
        supported_action_enums,
        continuous_dwell_supported,
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    recsys_pb2_grpc.add_PhoenixPredictionServiceServicer_to_server(
        prediction_servicer, server
    )
    recsys_pb2_grpc.add_PhoenixRetrievalServiceServicer_to_server(
        retrieval_servicer, server
    )
    health_servicer = register_health_service(server)
    install_shutdown_handlers(server, health_servicer, SHUTDOWN_GRACE_SECONDS)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    if refresher is not None:
        refresher.start()
        logger.info("候选池刷新线程已启动：每 %.0f 秒检查 %s", corpus_refresh_seconds, corpus_path)
    logger.info("Phoenix gRPC gateway ready on %s:%d", host, port)
    logger.info(
        "  ranker=%s  retrieval=%s  corpus=%s",
        ranker.model_version,
        retrieval.model_version,
        getattr(retrieval, "corpus_version", "n/a"),
    )
    server.wait_for_termination()
    if refresher is not None:
        refresher.stop(timeout=1.0)
    logger.info("Phoenix gRPC gateway stopped")
