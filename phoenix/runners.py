# 版权所有 2026 X.AI Corp.
#
# 根据 Apache 许可证 2.0 版本（“许可证”）授权；
# 除非遵守许可证，否则您不得使用此文件。
# 您可以在以下网址获得许可证副本：
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# 除非适用法律要求或书面同意，否则根据许可证分发的软件
# 是按“原样”基础分发的，不附带任何形式明示或暗示的保证或条件。
# 请参阅许可证以了解管理权限和限制的特定语言。

import functools
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, NamedTuple, Optional, Tuple

import haiku as hk
import jax
import jax.numpy as jnp
import numpy as np

# 导入底层定义
from grok import TrainingState
from recsys_retrieval_model import PhoenixRetrievalModelConfig
from recsys_retrieval_model import RetrievalOutput as ModelRetrievalOutput

from recsys_model import (
    PhoenixModelConfig,
    RecsysBatch,
    RecsysEmbeddings,
    RecsysModelOutput,
)

# 初始化专门用于排名的日志记录器
rank_logger = logging.getLogger("rank")


def create_dummy_batch_from_config(
    hash_config: Any,
    history_len: int,
    num_candidates: int,
    num_actions: int,
    batch_size: int = 1,
) -> RecsysBatch:
    """
    根据配置创建用于初始化的全零模拟批次数据（RecsysBatch）。
    用于探测模型输入形状并触发 Haiku 的参数初始化逻辑。
    """
    return RecsysBatch(
        user_hashes=np.zeros((batch_size, hash_config.num_user_hashes), dtype=np.int32),
        history_post_hashes=np.zeros(
            (batch_size, history_len, hash_config.num_item_hashes), dtype=np.int32
        ),
        history_author_hashes=np.zeros(
            (batch_size, history_len, hash_config.num_author_hashes), dtype=np.int32
        ),
        history_actions=np.zeros((batch_size, history_len, num_actions), dtype=np.float32),
        history_product_surface=np.zeros((batch_size, history_len), dtype=np.int32),
        candidate_post_hashes=np.zeros(
            (batch_size, num_candidates, hash_config.num_item_hashes), dtype=np.int32
        ),
        candidate_author_hashes=np.zeros(
            (batch_size, num_candidates, hash_config.num_author_hashes), dtype=np.int32
        ),
        candidate_product_surface=np.zeros((batch_size, num_candidates), dtype=np.int32),
    )


def create_dummy_embeddings_from_config(
    hash_config: Any,
    emb_size: int,
    history_len: int,
    num_candidates: int,
    batch_size: int = 1,
) -> RecsysEmbeddings:
    """
    根据配置创建用于初始化的全零模拟嵌入数据（RecsysEmbeddings）。
    """
    return RecsysEmbeddings(
        user_embeddings=np.zeros(
            (batch_size, hash_config.num_user_hashes, emb_size), dtype=np.float32
        ),
        history_post_embeddings=np.zeros(
            (batch_size, history_len, hash_config.num_item_hashes, emb_size), dtype=np.float32
        ),
        candidate_post_embeddings=np.zeros(
            (batch_size, num_candidates, hash_config.num_item_hashes, emb_size),
            dtype=np.float32,
        ),
        history_author_embeddings=np.zeros(
            (batch_size, history_len, hash_config.num_author_hashes, emb_size), dtype=np.float32
        ),
        candidate_author_embeddings=np.zeros(
            (batch_size, num_candidates, hash_config.num_author_hashes, emb_size),
            dtype=np.float32,
        ),
    )


@dataclass
class BaseModelRunner(ABC):
    """
    模型运行器的基类。
    管理计算设备、随机数种子以及通用的模型初始化逻辑。
    """

    bs_per_device: float = 2.0  # 每个计算设备分配的 Batch Size
    rng_seed: int = 42

    @property
    @abstractmethod
    def model(self) -> Any:
        """返回具体的模型配置实例。"""
        pass

    @property
    def _model_name(self) -> str:
        """模型名称，用于日志记录。"""
        return "model"

    @abstractmethod
    def make_forward_fn(self):
        """创建 Haiku 转换后的前向传播函数。由子类实现具体逻辑。"""
        pass

    def initialize(self):
        """执行模型初始化：配置精度、计算 batch size 并封装 forward 函数。"""
        self.model.initialize()
        self.model.fprop_dtype = jnp.bfloat16 # 使用 bfloat16 以获得更好的推理性能
        num_local_gpus = len(jax.local_devices())

        # 动态计算总批次大小
        self.batch_size = max(1, int(self.bs_per_device * num_local_gpus))

        rank_logger.info(f"正在初始化 {self._model_name}...")
        self.forward = self.make_forward_fn()


@dataclass
class BaseInferenceRunner(ABC):
    """
    推理运行器基类。
    主要负责创建模拟数据，辅助模型的热启动或参数初始化。
    """

    name: str

    @property
    @abstractmethod
    def runner(self) -> BaseModelRunner:
        """返回关联的模型运行器。"""
        pass

    def _get_num_actions(self) -> int:
        """获取模型预测的动作数量。"""
        model_config = self.runner.model
        if hasattr(model_config, "num_actions"):
            return model_config.num_actions
        return 19 # 默认值

    def create_dummy_batch(self, batch_size: int = 1) -> RecsysBatch:
        """便捷方法：创建模拟批次。"""
        model_config = self.runner.model
        return create_dummy_batch_from_config(
            hash_config=model_config.hash_config,
            history_len=model_config.history_seq_len,
            num_candidates=model_config.candidate_seq_len,
            num_actions=self._get_num_actions(),
            batch_size=batch_size,
        )

    def create_dummy_embeddings(self, batch_size: int = 1) -> RecsysEmbeddings:
        """便捷方法：创建模拟嵌入。"""
        model_config = self.runner.model
        return create_dummy_embeddings_from_config(
            hash_config=model_config.hash_config,
            emb_size=model_config.emb_size,
            history_len=model_config.history_seq_len,
            num_candidates=model_config.candidate_seq_len,
            batch_size=batch_size,
        )

    @abstractmethod
    def initialize(self):
        """初始化推理运行器。必须由具体业务逻辑实现。"""
        pass


# 推荐系统关注的一系列用户互动动作名称
ACTIONS: List[str] = [
    "favorite_score",             # 点赞
    "reply_score",                # 回复
    "repost_score",               # 转发
    "photo_expand_score",         # 图片展开
    "click_score",                # 点击
    "profile_click_score",        # 个人资料点击
    "vqv_score",                  # 视频播放质量
    "share_score",                # 分享
    "share_via_dm_score",         # 私信分享
    "share_via_copy_link_score",  # 复制链接分享
    "dwell_score",                # 停留（是否停留超过阈值）
    "quote_score",                # 引用
    "quoted_click_score",         # 引用点击
    "follow_author_score",        # 关注作者
    "not_interested_score",       # 不感兴趣
    "block_author_score",         # 屏蔽作者
    "mute_author_score",          # 静音作者
    "report_score",               # 举报
    "dwell_time",                 # 具体停留时长
]


class RankingOutput(NamedTuple):
    """
    精排输出结果容器。
    封装了总分数矩阵、排序后的索引以及针对各项行为的细分概率预测值。
    """

    scores: jax.Array        # 原始概率分数 [B, C, num_actions]
    ranked_indices: jax.Array # 排序后的索引 [B, C]

    # 各项互动概率的细分字段
    p_favorite_score: jax.Array
    p_reply_score: jax.Array
    p_repost_score: jax.Array
    p_photo_expand_score: jax.Array
    p_click_score: jax.Array
    p_profile_click_score: jax.Array
    p_vqv_score: jax.Array
    p_share_score: jax.Array
    p_share_via_dm_score: jax.Array
    p_share_via_copy_link_score: jax.Array
    p_dwell_score: jax.Array
    p_quote_score: jax.Array
    p_quoted_click_score: jax.Array
    p_follow_author_score: jax.Array
    p_not_interested_score: jax.Array
    p_block_author_score: jax.Array
    p_mute_author_score: jax.Array
    p_report_score: jax.Array
    p_dwell_time: jax.Array


@dataclass
class ModelRunner(BaseModelRunner):
    """
    推荐精排模型运行器。
    负责具体的 Haiku 变换和参数初始化。
    """

    _model: PhoenixModelConfig = None  # type: ignore

    def __init__(self, model: PhoenixModelConfig, bs_per_device: float = 2.0, rng_seed: int = 42):
        self._model = model
        self.bs_per_device = bs_per_device
        self.rng_seed = rng_seed

    @property
    def model(self) -> PhoenixModelConfig:
        return self._model

    @property
    def _model_name(self) -> str:
        return "ranking model"

    def make_forward_fn(self):  # type: ignore
        """包装模型实例化和前向传播。"""
        def forward(batch: RecsysBatch, recsys_embeddings: RecsysEmbeddings):
            out = self.model.make()(batch, recsys_embeddings)
            return out

        return hk.transform(forward)

    def init(
        self, rng: jax.Array, data: RecsysBatch, embeddings: RecsysEmbeddings
    ) -> TrainingState:
        """执行 Haiku 的 init 过程，生成模型参数。"""
        assert self.forward is not None
        rng, init_rng = jax.random.split(rng)
        params = self.forward.init(init_rng, data, embeddings)
        return TrainingState(params=params)

    def load_or_init(
        self,
        init_data: RecsysBatch,
        init_embeddings: RecsysEmbeddings,
    ):
        """加载或初始化模型权重。当前实现仅支持随机初始化。"""
        rng = jax.random.PRNGKey(self.rng_seed)
        state = self.init(rng, init_data, init_embeddings)
        return state


@dataclass
class RecsysInferenceRunner(BaseInferenceRunner):
    """
    推荐精排推理运行器。
    实现了核心的 `rank` 方法，支持将模型 logits 转换为具体的排序结果。
    """

    _runner: ModelRunner

    def __init__(self, runner: ModelRunner, name: str):
        self.name = name
        self._runner = runner

    @property
    def runner(self) -> ModelRunner:
        return self._runner

    def initialize(self):
        """
        初始化推理环境：
        1. 实例化 ModelRunner。
        2. 生成模拟输入并触发参数初始化。
        3. 编译（JIT）精排推理函数。
        """
        runner = self.runner

        dummy_batch = self.create_dummy_batch(batch_size=1)
        dummy_embeddings = self.create_dummy_embeddings(batch_size=1)

        runner.initialize()

        state = runner.load_or_init(dummy_batch, dummy_embeddings)
        self.params = state.params

        # 使用 lru_cache 确保模型对象在一次应用中只被实例化一次
        @functools.lru_cache
        def model():
            return runner.model.make()

        def hk_forward(
            batch: RecsysBatch, recsys_embeddings: RecsysEmbeddings
        ) -> RecsysModelOutput:
            return model()(batch, recsys_embeddings)

        def hk_rank_candidates(
            batch: RecsysBatch, recsys_embeddings: RecsysEmbeddings
        ) -> RankingOutput:
            """
            模型前向传播并处理结果：
            1. 计算 Logits。
            2. 应用 Sigmoid 将 Logits 转换为概率分数。
            3. 以第一项互动行为（favorite）作为主排序依据生成索引。
            """
            output = hk_forward(batch, recsys_embeddings)
            logits = output.logits

            # Logits -> Probs
            probs = jax.nn.sigmoid(logits)

            # 提取第一个动作的分数作为排序基准
            primary_scores = probs[:, :, 0]

            # 降序排序索引
            ranked_indices = jnp.argsort(-primary_scores, axis=-1)

            # 组装完整的 RankingOutput
            return RankingOutput(
                scores=probs,
                ranked_indices=ranked_indices,
                p_favorite_score=probs[:, :, 0],
                p_reply_score=probs[:, :, 1],
                p_repost_score=probs[:, :, 2],
                p_photo_expand_score=probs[:, :, 3],
                p_click_score=probs[:, :, 4],
                p_profile_click_score=probs[:, :, 5],
                p_vqv_score=probs[:, :, 6],
                p_share_score=probs[:, :, 7],
                p_share_via_dm_score=probs[:, :, 8],
                p_share_via_copy_link_score=probs[:, :, 9],
                p_dwell_score=probs[:, :, 10],
                p_quote_score=probs[:, :, 11],
                p_quoted_click_score=probs[:, :, 12],
                p_follow_author_score=probs[:, :, 13],
                p_not_interested_score=probs[:, :, 14],
                p_block_author_score=probs[:, :, 15],
                p_mute_author_score=probs[:, :, 16],
                p_report_score=probs[:, :, 17],
                p_dwell_time=probs[:, :, 18],
            )

        # 转换为无状态的前向应用函数（去掉 RNG 依赖）
        rank_ = hk.without_apply_rng(hk.transform(hk_rank_candidates))
        self.rank_candidates = rank_.apply

    def rank(self, batch: RecsysBatch, recsys_embeddings: RecsysEmbeddings) -> RankingOutput:
        """执行推理：对给定批次进行评分排序。"""
        return self.rank_candidates(self.params, batch, recsys_embeddings)


def create_example_batch(
    batch_size: int,
    emb_size: int,
    history_len: int,
    num_candidates: int,
    num_actions: int,
    num_user_hashes: int = 2,
    num_item_hashes: int = 2,
    num_author_hashes: int = 2,
    product_surface_vocab_size: int = 16,
    num_user_embeddings: int = 100000,
    num_post_embeddings: int = 100000,
    num_author_embeddings: int = 100000,
) -> Tuple[RecsysBatch, RecsysEmbeddings]:
    """
    创建一个包含随机数据的示例批次，用于功能测试。
    
    模拟场景说明：
    1. 生成指定哈希范围内的用户 ID。
    2. 生成历史记录，并随机截断模拟不同长度的历史。
    3. 生成历史互动动作（0/1）及场景。
    4. 生成候选池数据。
    5. 生成对应的嵌入向量。
    """
    rng = np.random.default_rng(42)

    # 1. 生成用户哈希
    user_hashes = rng.integers(1, num_user_embeddings, size=(batch_size, num_user_hashes)).astype(
        np.int32
    )

    # 2. 生成历史记录哈希并应用随机截断
    history_post_hashes = rng.integers(
        1, num_post_embeddings, size=(batch_size, history_len, num_item_hashes)
    ).astype(np.int32)

    for b in range(batch_size):
        valid_len = rng.integers(history_len // 2, history_len + 1)
        history_post_hashes[b, valid_len:, :] = 0 # 0 表示填充位

    history_author_hashes = rng.integers(
        1, num_author_embeddings, size=(batch_size, history_len, num_author_hashes)
    ).astype(np.int32)
    for b in range(batch_size):
        valid_len = rng.integers(history_len // 2, history_len + 1)
        history_author_hashes[b, valid_len:, :] = 0

    # 3. 生成历史行为数据
    history_actions = (rng.random(size=(batch_size, history_len, num_actions)) > 0.7).astype(
        np.float32
    )

    history_product_surface = rng.integers(
        0, product_surface_vocab_size, size=(batch_size, history_len)
    ).astype(np.int32)

    # 4. 生成候选推文哈希
    candidate_post_hashes = rng.integers(
        1, num_post_embeddings, size=(batch_size, num_candidates, num_item_hashes)
    ).astype(np.int32)

    candidate_author_hashes = rng.integers(
        1, num_author_embeddings, size=(batch_size, num_candidates, num_author_hashes)
    ).astype(np.int32)

    candidate_product_surface = rng.integers(
        0, product_surface_vocab_size, size=(batch_size, num_candidates)
    ).astype(np.int32)

    # 组装批次
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

    # 5. 生成对应的嵌入向量
    embeddings = RecsysEmbeddings(
        user_embeddings=rng.normal(size=(batch_size, num_user_hashes, emb_size)).astype(np.float32),
        history_post_embeddings=rng.normal(
            size=(batch_size, history_len, num_item_hashes, emb_size)
        ).astype(np.float32),
        candidate_post_embeddings=rng.normal(
            size=(batch_size, num_candidates, num_item_hashes, emb_size)
        ).astype(np.float32),
        history_author_embeddings=rng.normal(
            size=(batch_size, history_len, num_author_hashes, emb_size)
        ).astype(np.float32),
        candidate_author_embeddings=rng.normal(
            size=(batch_size, num_candidates, num_author_hashes, emb_size)
        ).astype(np.float32),
    )

    return batch, embeddings


class RetrievalOutput(NamedTuple):
    """
    召回推理输出结果容器。
    封装了用户向量以及召回到的候选集索引和分数。
    """

    user_representation: jax.Array
    top_k_indices: jax.Array
    top_k_scores: jax.Array


@dataclass
class RetrievalModelRunner(BaseModelRunner):
    """
    召回模型运行器。
    管理召回模型（双塔架构）的 Haiku 变换和初始化。
    """

    _model: PhoenixRetrievalModelConfig = None  # type: ignore

    def __init__(
        self,
        model: PhoenixRetrievalModelConfig,
        bs_per_device: float = 2.0,
        rng_seed: int = 42,
    ):
        self._model = model
        self.bs_per_device = bs_per_device
        self.rng_seed = rng_seed

    @property
    def model(self) -> PhoenixRetrievalModelConfig:
        return self._model

    @property
    def _model_name(self) -> str:
        return "retrieval model"

    def make_forward_fn(self):  # type: ignore
        """包装召回前向传播和候选塔构建。"""
        def forward(
            batch: RecsysBatch,
            recsys_embeddings: RecsysEmbeddings,
            corpus_embeddings: jax.Array,
            top_k: int,
        ) -> ModelRetrievalOutput:
            model = self.model.make()
            # 执行检索
            out = model(batch, recsys_embeddings, corpus_embeddings, top_k)
            # 同时也确保候选塔的构建逻辑被包含在参数 init 过程中
            _ = model.build_candidate_representation(batch, recsys_embeddings)
            return out

        return hk.transform(forward)

    def init(
        self,
        rng: jax.Array,
        data: RecsysBatch,
        embeddings: RecsysEmbeddings,
        corpus_embeddings: jax.Array,
        top_k: int,
    ) -> TrainingState:
        assert self.forward is not None
        rng, init_rng = jax.random.split(rng)
        params = self.forward.init(init_rng, data, embeddings, corpus_embeddings, top_k)
        return TrainingState(params=params)

    def load_or_init(
        self,
        init_data: RecsysBatch,
        init_embeddings: RecsysEmbeddings,
        corpus_embeddings: jax.Array,
        top_k: int,
    ):
        rng = jax.random.PRNGKey(self.rng_seed)
        state = self.init(rng, init_data, init_embeddings, corpus_embeddings, top_k)
        return state


@dataclass
class RecsysRetrievalInferenceRunner(BaseInferenceRunner):
    """
    召回模型推理运行器。
    提供了三个核心能力：
    1. `encode_user`: 将用户特征映射为向量。
    2. `encode_candidates`: 将物品特征映射为向量（用于构建离线索引）。
    3. `retrieve`: 在全量池中执行在线检索。
    """

    _runner: RetrievalModelRunner = None  # type: ignore

    # 存储全局候选池的数据
    corpus_embeddings: jax.Array | None = None
    corpus_post_ids: jax.Array | None = None

    def __init__(self, runner: RetrievalModelRunner, name: str):
        self.name = name
        self._runner = runner
        self.corpus_embeddings = None
        self.corpus_post_ids = None

    @property
    def runner(self) -> RetrievalModelRunner:
        return self._runner

    def initialize(self):
        """初始化召回推理函数。"""
        runner = self.runner

        dummy_batch = self.create_dummy_batch(batch_size=1)
        dummy_embeddings = self.create_dummy_embeddings(batch_size=1)
        dummy_corpus = jnp.zeros((10, runner.model.emb_size), dtype=jnp.float32)
        dummy_top_k = 5

        runner.initialize()

        # 初始化参数
        state = runner.load_or_init(dummy_batch, dummy_embeddings, dummy_corpus, dummy_top_k)
        self.params = state.params

        @functools.lru_cache
        def model():
            return runner.model.make()

        # 定义封装函数，利用同一个模型参数提供不同功能
        def hk_encode_user(batch: RecsysBatch, recsys_embeddings: RecsysEmbeddings) -> jax.Array:
            m = model()
            user_rep, _ = m.build_user_representation(batch, recsys_embeddings)
            return user_rep

        def hk_encode_candidates(
            batch: RecsysBatch, recsys_embeddings: RecsysEmbeddings
        ) -> jax.Array:
            m = model()
            cand_rep, _ = m.build_candidate_representation(batch, recsys_embeddings)
            return cand_rep

        def hk_retrieve(
            batch: RecsysBatch,
            recsys_embeddings: RecsysEmbeddings,
            corpus_embeddings: jax.Array,
            top_k: int,
        ) -> "RetrievalOutput":
            m = model()
            return m(batch, recsys_embeddings, corpus_embeddings, top_k)

        # 转换为无状态 apply 函数
        encode_user_ = hk.without_apply_rng(hk.transform(hk_encode_user))
        encode_candidates_ = hk.without_apply_rng(hk.transform(hk_encode_candidates))
        retrieve_ = hk.without_apply_rng(hk.transform(hk_retrieve))

        self.encode_user_fn = encode_user_.apply
        self.encode_candidates_fn = encode_candidates_.apply
        self.retrieve_fn = retrieve_.apply

    def encode_user(self, batch: RecsysBatch, recsys_embeddings: RecsysEmbeddings) -> jax.Array:
        """编码用户。"""
        return self.encode_user_fn(self.params, batch, recsys_embeddings)

    def encode_candidates(
        self, batch: RecsysBatch, recsys_embeddings: RecsysEmbeddings
    ) -> jax.Array:
        """编码候选物品。"""
        return self.encode_candidates_fn(self.params, batch, recsys_embeddings)

    def set_corpus(
        self,
        corpus_embeddings: jax.Array,
        corpus_post_ids: jax.Array,
    ):
        """设置全量候选池索引，供在线检索使用。"""
        self.corpus_embeddings = corpus_embeddings
        self.corpus_post_ids = corpus_post_ids

    def retrieve(
        self,
        batch: RecsysBatch,
        recsys_embeddings: RecsysEmbeddings,
        top_k: int = 100,
        corpus_embeddings: Optional[jax.Array] = None,
    ) -> RetrievalOutput:
        """执行在线检索。"""
        if corpus_embeddings is None:
            corpus_embeddings = self.corpus_embeddings

        return self.retrieve_fn(self.params, batch, recsys_embeddings, corpus_embeddings, top_k)


def create_example_corpus(
    corpus_size: int,
    emb_size: int,
    seed: int = 123,
) -> Tuple[jax.Array, jax.Array]:
    """
    创建示例候选池全量数据，模拟海量推文库。
    
    返回：
        corpus_embeddings: 归一化的向量库 [N, D]
        corpus_post_ids: 物品 ID 列表 [N]
    """
    rng = np.random.default_rng(seed)

    # 生成并归一化
    corpus_embeddings = rng.normal(size=(corpus_size, emb_size)).astype(np.float32)
    norms = np.linalg.norm(corpus_embeddings, axis=-1, keepdims=True)
    corpus_embeddings = corpus_embeddings / np.maximum(norms, 1e-12)

    corpus_post_ids = np.arange(corpus_size, dtype=np.int64)

    return jnp.array(corpus_embeddings), jnp.array(corpus_post_ids)
