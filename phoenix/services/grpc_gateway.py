# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
Phoenix gRPC 网关 — recommendation-service 与 Phoenix 模型之间的桥。

实现 proto/definitions/recsys.proto 定义的两个 gRPC 服务：
    1. PhoenixPredictionService.PredictNextActions —— 精排：
       输入用户行为序列 + 候选帖子，输出每条帖子上 18 种行为的概率。
    2. PhoenixRetrievalService.Retrieve —— 召回：
       输入用户行为序列，从候选池中检索 Top-K 帖子（网外召回）。

与 HTTP 服务（ranker_service / retrieval_service）的区别：
    - HTTP 服务面向人和外部系统调试，字段是字符串 ID；
    - 本网关面向 recommendation-service（Rust），协议、字段、概率格式严格对齐 proto 契约，
      并且真正消费请求里的用户行为序列（而不是 mock 特征）。

启动方式:
    uv run scripts/run_grpc_gateway.py                       # 随机权重（演示）
    uv run scripts/run_grpc_gateway.py \
        --ranker-checkpoint checkpoints/model_params_step200.npz \
        --emb-tables checkpoints/embedding_tables.npz        # 加载训练产物

默认仅监听本机 127.0.0.1:50053（可用 --host/--port 或环境变量
PHOENIX_GRPC_HOST/PHOENIX_GRPC_PORT 修改）。如需供远程客户端调用，应在受控网络
边界内显式传入绑定地址，并配置相应的访问控制。
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
from concurrent import futures
from typing import List, Optional, Sequence, Tuple

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
    ACTION_IDX_TO_ENUM,
    CandidatePrediction,
    HistoryFeatures,
)
from services.recsys_proto import load_proto_modules

logger = logging.getLogger("grpc_gateway")

# ── 超参（与 scripts/train_ranker.py / data_preprocessor.py 保持一致）────────
EMB_SIZE = 128
HISTORY_LEN = 32
NUM_ACTIONS = len(ACTIONS)  # 19
TABLE_SIZE = 100_000
SURFACE_VOCAB = 16
NUM_HASHES = 2
RANK_CHUNK = 32  # 单次前向最多处理的候选数，超出部分分批

# Python ACTIONS 下标 → proto ActionName 枚举值。
# 注意两边顺序不同：例如 quote 在 Python 里是下标 11，在 proto 里是枚举值 4。
LOG_PROBS_LEN = 19        # ActionName 枚举 0..=18
CONTINUOUS_LEN = 2        # ContinuousActionName 枚举 0..=1（1 = DWELL_TIME）
MIN_PROB = 1e-9

# Cross-language serving contract.  recommendation-service rejects a response when any of
# these values is missing or incompatible with the request-side feature
# mapping.  Keep this explicit instead of inferring readiness from a model
# filename or from a successful gRPC call.
FEATURE_SCHEMA = "phoenix-id-actions-v1"
ID_MAPPING_VERSION = os.getenv("PHOENIX_ID_MAPPING_VERSION", "v1")
SUPPORTED_ACTIONS = ",".join(str(i) for i in range(1, 19))

# Snowflake 纪元（毫秒），用于给演示候选池合成"最近发布"的帖子 ID
TWITTER_EPOCH_MS = 1288834974657


def snowflake_id(timestamp_ms: int, sequence: int) -> int:
    return ((timestamp_ms - TWITTER_EPOCH_MS) << 22) | (sequence & 0x3F_FFFF)


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
        post_hashes[0, i] = hash_id_to_ints(str(rec.tweet_id), NUM_HASHES, TABLE_SIZE)
        author_hashes[0, i] = hash_id_to_ints(str(rec.author_id), NUM_HASHES, TABLE_SIZE)
        surface[0, i] = rec.product_surface % SURFACE_VOCAB
        mask = list(rec.action_mask)
        for py_idx, enum_val in enumerate(ACTION_IDX_TO_ENUM):
            if enum_val < len(mask) and mask[enum_val]:
                actions[0, i, py_idx] = 1.0

    return HistoryFeatures(post_hashes, author_hashes, actions, surface)


def build_batch(
    user_id: int,
    history: HistoryFeatures,
    candidate_post_ids: Sequence[int],
    candidate_author_ids: Sequence[int],
    num_candidates: int,
) -> RecsysBatch:
    """组装 RecsysBatch（B=1，候选不足 num_candidates 时补 padding）。"""
    cand_post = np.zeros((1, num_candidates, NUM_HASHES), dtype=np.int32)
    cand_author = np.zeros((1, num_candidates, NUM_HASHES), dtype=np.int32)
    cand_surface = np.zeros((1, num_candidates), dtype=np.int32)

    for i, (post_id, author_id) in enumerate(zip(candidate_post_ids, candidate_author_ids)):
        cand_post[0, i] = hash_id_to_ints(str(post_id), NUM_HASHES, TABLE_SIZE)
        cand_author[0, i] = hash_id_to_ints(str(author_id), NUM_HASHES, TABLE_SIZE)

    user_hashes = np.array(
        [hash_id_to_ints(str(user_id), NUM_HASHES, TABLE_SIZE)], dtype=np.int32
    )

    return RecsysBatch(
        user_hashes=user_hashes,
        history_post_hashes=history.post_hashes,
        history_author_hashes=history.author_hashes,
        history_actions=history.actions,
        history_product_surface=history.product_surface,
        candidate_post_hashes=cand_post,
        candidate_author_hashes=cand_author,
        candidate_product_surface=cand_surface,
    )


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
    """把 (UAS, 候选列表) 送进精排模型，返回逐候选的行为概率。"""

    def __init__(self, tables: EmbeddingTables, checkpoint_path: Optional[str] = None):
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

    def predict(
        self, user_id: int, uas, candidates: Sequence[Tuple[int, int]]
    ) -> List[CandidatePrediction]:
        """返回每个候选的离散行为概率和可选连续预测。"""
        history = uas_to_history(uas)
        predictions: List[CandidatePrediction] = []

        with self._lock:
            for start in range(0, len(candidates), RANK_CHUNK):
                chunk = candidates[start : start + RANK_CHUNK]
                batch = build_batch(
                    user_id,
                    history,
                    [c[0] for c in chunk],
                    [c[1] for c in chunk],
                    RANK_CHUNK,
                )
                embeddings = self._tables.lookup(batch)
                output = self._runner.rank(batch, embeddings)
                probs = np.asarray(output.scores[0], dtype=np.float64)
                continuous = (
                    None
                    if output.continuous_preds is None
                    else np.asarray(output.continuous_preds[0], dtype=np.float64)
                )
                for index in range(len(chunk)):
                    predictions.append(
                        CandidatePrediction(
                            action_probs=probs[index],
                            continuous_values=(
                                None if continuous is None else continuous[index]
                            ),
                        )
                    )

        return predictions


# ── 召回引擎 ──────────────────────────────────────────────────────────────────


class RetrievalEngine:
    """双塔召回：用户塔编码 UAS，在演示候选池上做 Top-K 点积检索。

    候选池是启动时合成的（Snowflake ID + 网外作者），
    物品向量由真实的候选塔前向计算得到，检索数学是真实的。
    生产环境应替换为离线构建的向量索引（FAISS/Milvus）。
    """

    def __init__(
        self,
        tables: EmbeddingTables,
        checkpoint_path: Optional[str] = None,
        corpus_size: int = 2000,
    ):
        self._tables = tables
        self._lock = threading.Lock()

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
        self._build_corpus(corpus_size)

    def _build_corpus(self, corpus_size: int) -> None:
        """合成演示候选池并用候选塔编码成向量。"""
        now_ms = int(time.time() * 1000)
        window_ms = 24 * 60 * 60 * 1000
        step_ms = max(window_ms // max(corpus_size, 1), 1)

        # 演示作者（只用于独立 Phoenix retrieval smoke）
        self._post_ids = np.array(
            [
                snowflake_id(now_ms - window_ms + step_ms * i, 500_000 + i)
                for i in range(corpus_size)
            ],
            dtype=np.int64,
        )
        self._author_ids = np.array(
            [201 + (i % 40) for i in range(corpus_size)], dtype=np.int64
        )

        logger.info("正在编码演示候选池（%d 条帖子）...", corpus_size)
        empty_history = HistoryFeatures(
            post_hashes=np.zeros((1, HISTORY_LEN, NUM_HASHES), dtype=np.int32),
            author_hashes=np.zeros((1, HISTORY_LEN, NUM_HASHES), dtype=np.int32),
            actions=np.zeros((1, HISTORY_LEN, NUM_ACTIONS), dtype=np.float32),
            product_surface=np.zeros((1, HISTORY_LEN), dtype=np.int32),
        )

        reps = []
        for start in range(0, corpus_size, RANK_CHUNK):
            ids = self._post_ids[start : start + RANK_CHUNK]
            authors = self._author_ids[start : start + RANK_CHUNK]
            batch = build_batch(0, empty_history, ids.tolist(), authors.tolist(), RANK_CHUNK)
            embeddings = self._tables.lookup(batch)
            rep = np.asarray(self._runner.encode_candidates(batch, embeddings)[0])
            reps.append(rep[: len(ids)])

        corpus_embeddings = np.concatenate(reps, axis=0).astype(np.float32)
        self._runner.set_corpus(corpus_embeddings, self._post_ids)
        logger.info("候选池就绪：%d 条帖子，向量维度 %d", corpus_size, corpus_embeddings.shape[1])

    def retrieve(self, user_id: int, uas, max_results: int) -> List[Tuple[int, int, float]]:
        """返回 [(post_id, author_id, score)]，按相似度降序。"""
        history = uas_to_history(uas)
        top_k = min(max(max_results, 1), len(self._post_ids))

        with self._lock:
            batch = build_batch(user_id, history, [], [], RANK_CHUNK)
            embeddings = self._tables.lookup(batch)
            output = self._runner.retrieve(batch, embeddings, top_k=top_k)

        indices = np.asarray(output.top_k_indices[0])
        scores = np.asarray(output.top_k_scores[0])

        results = []
        for idx, score in zip(indices, scores):
            idx = int(idx)
            results.append(
                (int(self._post_ids[idx]), int(self._author_ids[idx]), float(score))
            )
        return results


# ── gRPC 服务实现 ─────────────────────────────────────────────────────────────


def create_servicers(recsys_pb2, recsys_pb2_grpc, ranker: RankerEngine, retrieval: RetrievalEngine):
    """构造两个 servicer（在函数内定义类，因为基类来自运行时生成的模块）。"""

    def set_contract_metadata(context, engine) -> None:
        context.set_trailing_metadata(
            (
                ("feature-schema", FEATURE_SCHEMA),
                ("id-mapping-version", ID_MAPPING_VERSION),
                ("model-version", engine.model_version),
                ("random-weights", str(engine.model_version == "random").lower()),
                ("supported-actions", SUPPORTED_ACTIONS),
            )
        )

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
                    p = float(np.clip(probs[py_idx], MIN_PROB, 1.0))
                    top_log_probs[enum_val] = math.log(p)

                # 连续值下标 1 = DWELL_TIME；旧模型没有连续头时兼容离散占位。
                dwell_time = float(probs[18])
                if (
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
                "PredictNextActions: user=%d candidates=%d (%.1f ms)",
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
                "Retrieve: user=%d returned=%d (%.1f ms)",
                request.user_id,
                len(candidates),
                elapsed_ms,
            )
            return recsys_pb2.RetrieveResponse(
                top_k_candidates=[recsys_pb2.ScoredCandidates(candidates=candidates)]
            )

    return PredictionServicer(), RetrievalServicer()


def serve(
    port: int = 50053,
    ranker_checkpoint: Optional[str] = None,
    retrieval_checkpoint: Optional[str] = None,
    emb_tables_path: Optional[str] = None,
    corpus_size: int = 2000,
    artifacts_dir: Optional[str] = None,
    host: str = "127.0.0.1",
) -> None:
    import grpc

    recsys_pb2, recsys_pb2_grpc = load_proto_modules()

    if artifacts_dir:
        from services.published_pipeline import PublishedPipeline

        logger.info("正在从发布 artifact 初始化共享推理核心...")
        published_pipeline = PublishedPipeline(artifacts_dir)
        ranker = published_pipeline.ranker
        retrieval = published_pipeline.retrieval
    else:
        tables = EmbeddingTables(emb_tables_path)
        logger.info("正在初始化精排模型...")
        ranker = RankerEngine(tables, ranker_checkpoint)
        logger.info("正在初始化召回模型...")
        retrieval = RetrievalEngine(tables, retrieval_checkpoint, corpus_size)

    prediction_servicer, retrieval_servicer = create_servicers(
        recsys_pb2, recsys_pb2_grpc, ranker, retrieval
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    recsys_pb2_grpc.add_PhoenixPredictionServiceServicer_to_server(
        prediction_servicer, server
    )
    recsys_pb2_grpc.add_PhoenixRetrievalServiceServicer_to_server(
        retrieval_servicer, server
    )
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    logger.info("Phoenix gRPC gateway ready on %s:%d", host, port)
    logger.info(
        "  ranker=%s  retrieval=%s", ranker.model_version, retrieval.model_version
    )
    server.wait_for_termination()
