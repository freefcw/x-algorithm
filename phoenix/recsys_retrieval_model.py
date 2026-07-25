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

import logging
from dataclasses import dataclass
from typing import Any, NamedTuple, Optional, Tuple

import haiku as hk
import jax
import jax.numpy as jnp

from grok import TransformerConfig, Transformer
from recsys_model import (
    HashConfig,
    RecsysBatch,
    RecsysEmbeddings,
    block_history_reduce,
    block_user_reduce,
)

logger = logging.getLogger(__name__)

# 定义数值稳定性相关的常量
EPS = 1e-12
INF = 1e12


class RetrievalOutput(NamedTuple):
    """
    召回模型输出容器。
    user_representation: 用户塔生成的归一化向量 [B, D]
    top_k_indices: 检索到的前 K 个候选对象的索引 [B, K]
    top_k_scores: 对应的相似度分数 [B, K]
    """
    user_representation: jax.Array
    top_k_indices: jax.Array
    top_k_scores: jax.Array


@dataclass
class CandidateTower(hk.Module):
    """
    候选塔（物品塔）模块。
    负责将推文和作者的原始嵌入映射到一个共享的向量空间。
    """

    emb_size: int
    enable_linear_proj: bool = True
    name: Optional[str] = None

    def __call__(self, post_author_embedding: jax.Array) -> jax.Array:
        """
        将推文+作者嵌入投影到归一化的表示空间。

        参数:
            post_author_embedding: 拼接后的推文和作者嵌入
                形状: [B, C, num_hashes * D] 或 [B, num_hashes * D]

        返回:
            L2 归一化后的候选对象表示，用于相似度计算。
        """
        if not self.enable_linear_proj:
            candidate_representation = jnp.mean(post_author_embedding, axis=-2)
            candidate_norm_sq = jnp.sum(
                candidate_representation**2, axis=-1, keepdims=True
            )
            candidate_norm = jnp.sqrt(jnp.maximum(candidate_norm_sq, EPS))
            return (candidate_representation / candidate_norm).astype(
                post_author_embedding.dtype
            )

        # 1. 调整输入形状，合并哈希维度
        if len(post_author_embedding.shape) == 4:
            B, C, _, _ = post_author_embedding.shape
            post_author_embedding = jnp.reshape(post_author_embedding, (B, C, -1))
        else:
            B, _, _ = post_author_embedding.shape
            post_author_embedding = jnp.reshape(post_author_embedding, (B, -1))

        # 2. 定义两层线性投影层，中间使用 SiLU 激活函数
        embed_init = hk.initializers.VarianceScaling(1.0, mode="fan_out")

        proj_1 = hk.get_parameter(
            "candidate_tower_projection_1",
            [post_author_embedding.shape[-1], self.emb_size * 2],
            dtype=jnp.float32,
            init=embed_init,
        )

        proj_2 = hk.get_parameter(
            "candidate_tower_projection_2",
            [self.emb_size * 2, self.emb_size],
            dtype=jnp.float32,
            init=embed_init,
        )

        # 3. 前向计算
        hidden = jnp.dot(post_author_embedding.astype(proj_1.dtype), proj_1)
        hidden = jax.nn.silu(hidden)
        candidate_embeddings = jnp.dot(hidden.astype(proj_2.dtype), proj_2)

        # 4. L2 归一化：确保向量在单位超球面上，使点积等同于余弦相似度
        candidate_norm_sq = jnp.sum(candidate_embeddings**2, axis=-1, keepdims=True)
        candidate_norm = jnp.sqrt(jnp.maximum(candidate_norm_sq, EPS))
        candidate_representation = candidate_embeddings / candidate_norm

        return candidate_representation.astype(post_author_embedding.dtype)


@dataclass
class PhoenixRetrievalModelConfig:
    """
    召回模型配置类。
    复用了 Phoenix 精排模型的 Transformer 架构来处理用户历史。
    """

    model: TransformerConfig
    emb_size: int
    history_seq_len: int = 128
    candidate_seq_len: int = 32

    name: Optional[str] = None
    fprop_dtype: Any = jnp.bfloat16

    hash_config: HashConfig = None  # type: ignore

    product_surface_vocab_size: int = 16
    enable_linear_proj: bool = True

    _initialized: bool = False

    def __post_init__(self):
        if self.hash_config is None:
            self.hash_config = HashConfig()

    def initialize(self):
        self._initialized = True
        return self

    def make(self):
        if not self._initialized:
            logger.warning(f"PhoenixRetrievalModel {self.name} 尚未初始化，正在自动初始化。")
            self.initialize()

        return PhoenixRetrievalModel(
            model=self.model.make(),
            config=self,
            fprop_dtype=self.fprop_dtype,
        )


@dataclass
class PhoenixRetrievalModel(hk.Module):
    """
    基于 Transformer 用户编码的双塔召回模型。

    架构设计:
    - 用户塔 (User Tower): 利用 Phoenix Transformer 对用户信息及历史行为进行时序建模，
      输出代表用户兴趣的特征向量。
    - 候选塔 (Candidate Tower): 利用 MLP 将待选推文特征映射到同一空间。
    
    检索机制:
    通过计算用户向量与全量候选池（Corpus）向量的点积相似度，召回 Top-K 个物品。
    """

    model: Transformer
    config: PhoenixRetrievalModelConfig
    fprop_dtype: Any = jnp.bfloat16
    name: Optional[str] = None

    def _get_action_embeddings(
        self,
        actions: jax.Array,
    ) -> jax.Array:
        """将历史互动动作转换为嵌入向量。"""
        config = self.config
        _, _, num_actions = actions.shape
        D = config.emb_size

        embed_init = hk.initializers.VarianceScaling(1.0, mode="fan_out")
        action_projection = hk.get_parameter(
            "action_projection",
            [num_actions, D],
            dtype=jnp.float32,
            init=embed_init,
        )

        actions_signed = (2 * actions - 1).astype(jnp.float32)
        action_emb = jnp.dot(actions_signed.astype(action_projection.dtype), action_projection)

        valid_mask = jnp.any(actions, axis=-1, keepdims=True)
        action_emb = action_emb * valid_mask

        return action_emb.astype(self.fprop_dtype)

    def _single_hot_to_embeddings(
        self,
        input: jax.Array,
        vocab_size: int,
        emb_size: int,
        name: str,
    ) -> jax.Array:
        """分类特征查找表逻辑。"""
        embed_init = hk.initializers.VarianceScaling(1.0, mode="fan_out")
        embedding_table = hk.get_parameter(
            name,
            [vocab_size, emb_size],
            dtype=jnp.float32,
            init=embed_init,
        )

        input_one_hot = jax.nn.one_hot(input, vocab_size)
        output = jnp.dot(input_one_hot, embedding_table)
        return output.astype(self.fprop_dtype)

    def build_user_representation(
        self,
        batch: RecsysBatch,
        recsys_embeddings: RecsysEmbeddings,
    ) -> Tuple[jax.Array, jax.Array]:
        """
        构建用户塔的表示向量。
        
        逻辑:
            1. 聚合用户及历史哈希嵌入。
            2. 拼接成序列输入 Transformer。
            3. 对 Transformer 的输出进行 Mean Pooling（均值池化）处理。
            4. 最后执行 L2 归一化。
        """
        config = self.config
        hash_config = config.hash_config

        # 处理场景信息和动作信息
        history_product_surface_embeddings = self._single_hot_to_embeddings(
            batch.history_product_surface,
            config.product_surface_vocab_size,
            config.emb_size,
            "product_surface_embedding_table",
        )

        history_actions_embeddings = self._get_action_embeddings(batch.history_actions)

        # 合并哈希嵌入
        user_embeddings, user_padding_mask = block_user_reduce(
            batch.user_hashes,
            recsys_embeddings.user_embeddings,
            hash_config.num_user_hashes,
            config.emb_size,
            1.0,
        )

        history_embeddings, history_padding_mask = block_history_reduce(
            batch.history_post_hashes,
            recsys_embeddings.history_post_embeddings,
            recsys_embeddings.history_author_embeddings,
            history_product_surface_embeddings,
            history_actions_embeddings,
            hash_config.num_item_hashes,
            hash_config.num_author_hashes,
            1.0,
        )

        # 拼接序列: [用户, 历史...]
        embeddings = jnp.concatenate([user_embeddings, history_embeddings], axis=1)
        padding_mask = jnp.concatenate([user_padding_mask, history_padding_mask], axis=1)

        # 调用 Transformer
        model_output = self.model(
            embeddings.astype(self.fprop_dtype),
            padding_mask,
            candidate_start_offset=None,
        )

        user_outputs = model_output.embeddings

        # Mean Pooling: 仅对有效位置进行加权平均，忽略填充位置
        mask_float = padding_mask.astype(jnp.float32)[:, :, None]  # [B, T, 1]
        user_embeddings_masked = user_outputs * mask_float
        user_embedding_sum = jnp.sum(user_embeddings_masked, axis=1)  # [B, D]
        mask_sum = jnp.sum(mask_float, axis=1)  # [B, 1]
        user_representation = user_embedding_sum / jnp.maximum(mask_sum, 1.0)

        # L2 归一化
        user_norm_sq = jnp.sum(user_representation**2, axis=-1, keepdims=True)
        user_norm = jnp.sqrt(jnp.maximum(user_norm_sq, EPS))
        user_representation = user_representation / user_norm

        return user_representation, user_norm

    def build_candidate_representation(
        self,
        batch: RecsysBatch,
        recsys_embeddings: RecsysEmbeddings,
    ) -> Tuple[jax.Array, jax.Array]:
        """
        构建物品塔的表示向量。
        
        逻辑:
            1. 拼接推文和作者的原始嵌入。
            2. 通过 CandidateTower (MLP) 映射并归一化。
        """
        config = self.config

        candidate_post_embeddings = recsys_embeddings.candidate_post_embeddings
        candidate_author_embeddings = recsys_embeddings.candidate_author_embeddings

        post_author_embedding = jnp.concatenate(
            [candidate_post_embeddings, candidate_author_embeddings], axis=2
        )

        candidate_tower = CandidateTower(
            emb_size=config.emb_size,
            enable_linear_proj=config.enable_linear_proj,
        )
        candidate_representation = candidate_tower(post_author_embedding)

        # 生成物品有效性掩码
        candidate_padding_mask = (batch.candidate_post_hashes[:, :, 0] != 0).astype(jnp.bool_)

        return candidate_representation, candidate_padding_mask

    def __call__(
        self,
        batch: RecsysBatch,
        recsys_embeddings: RecsysEmbeddings,
        corpus_embeddings: jax.Array,
        top_k: int,
        corpus_mask: Optional[jax.Array] = None,
    ) -> RetrievalOutput:
        """
        执行端到端召回：计算当前用户在全量池中的 Top-K 结果。
        """
        # 1. 编码用户
        user_representation, _ = self.build_user_representation(batch, recsys_embeddings)

        # 2. 执行向量检索
        top_k_indices, top_k_scores = self._retrieve_top_k(
            user_representation, corpus_embeddings, top_k, corpus_mask
        )

        return RetrievalOutput(
            user_representation=user_representation,
            top_k_indices=top_k_indices,
            top_k_scores=top_k_scores,
        )

    def _retrieve_top_k(
        self,
        user_representation: jax.Array,
        corpus_embeddings: jax.Array,
        top_k: int,
        corpus_mask: Optional[jax.Array] = None,
    ) -> Tuple[jax.Array, jax.Array]:
        """
        在大规模候选池中通过内积（点积）查找前 K 个最相似的对象。
        """
        # 计算相似度矩阵: [B, D] * [D, N] -> [B, N]
        scores = jnp.matmul(user_representation, corpus_embeddings.T)

        # 如果提供了掩码，则将无效位置的分数设为极小值
        if corpus_mask is not None:
            scores = jnp.where(corpus_mask[None, :], scores, -INF)

        # 选取 Top-K
        top_k_scores, top_k_indices = jax.lax.top_k(scores, top_k)

        return top_k_indices, top_k_scores
