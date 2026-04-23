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
from jax.tree_util import register_dataclass

# 从 grok 模块导入底层的 Transformer 架构和层归一化工具
from grok import (
    TransformerConfig,
    Transformer,
    layer_norm,
)

logger = logging.getLogger(__name__)


@dataclass
class HashConfig:
    """
    基于哈希的嵌入配置。
    为了解决大规模 ID（如用户 ID、推文 ID）带来的参数量问题，
    系统通常使用多个哈希函数将 ID 映射到较小的嵌入表中。
    """
    num_user_hashes: int = 2     # 用户特征使用的哈希函数数量
    num_item_hashes: int = 2     # 物品（推文）特征使用的哈希函数数量
    num_author_hashes: int = 2   # 作者特征使用的哈希函数数量


# 必须注册为 JAX pytree，否则作为 @jax.jit 的参数传入时会被当成一个不透明的 leaf，
# 而 leaf 又不是 jax.Array，JIT 追踪阶段会抛：
#   TypeError: Error interpreting argument ... The problematic value is of type
#   <class 'recsys_model.RecsysEmbeddings'>
# RecsysBatch 本身是 NamedTuple，JAX 天然识别；RecsysEmbeddings 是 @dataclass，
# 需要显式 register_dataclass。所有字段均为数据字段（jax.Array），无元数据字段，
# 故 data_fields / meta_fields 由 JAX 按 dataclass 默认规则自动推断。
@register_dataclass
@dataclass
class RecsysEmbeddings:
    """
    预查询嵌入容器。
    在进入模型前，特征 ID 已根据哈希值从嵌入表中查出。
    这些嵌入随后会通过 block_*_reduce 函数合并成单一表示。
    """
    user_embeddings: jax.typing.ArrayLike           # 用户嵌入 [B, num_user_hashes, D]
    history_post_embeddings: jax.typing.ArrayLike    # 历史推文嵌入 [B, S, num_item_hashes, D]
    candidate_post_embeddings: jax.typing.ArrayLike  # 候选推文嵌入 [B, C, num_item_hashes, D]
    history_author_embeddings: jax.typing.ArrayLike  # 历史作者嵌入 [B, S, num_author_hashes, D]
    candidate_author_embeddings: jax.typing.ArrayLike# 候选作者嵌入 [B, C, num_author_hashes, D]


class RecsysModelOutput(NamedTuple):
    """
    推荐模型输出。
    logits: 对应每个候选对象的预测分数（通常针对多种互动行为，如点赞、转发等）。
    """
    logits: jax.Array


class RecsysBatch(NamedTuple):
    """
    推荐模型输入批次。
    包含所有原始特征数据（哈希值、动作、产品场景等），但不直接包含嵌入向量。
    嵌入向量通过 RecsysEmbeddings 独立传递。
    """
    user_hashes: jax.typing.ArrayLike                # 用户哈希 [B, num_user_hashes]
    history_post_hashes: jax.typing.ArrayLike         # 历史推文哈希 [B, S, num_item_hashes]
    history_author_hashes: jax.typing.ArrayLike       # 历史作者哈希 [B, S, num_author_hashes]
    history_actions: jax.typing.ArrayLike             # 历史互动行为 [B, S, num_actions]
    history_product_surface: jax.typing.ArrayLike     # 历史场景信息 [B, S]
    candidate_post_hashes: jax.typing.ArrayLike       # 候选推文哈希 [B, C, num_item_hashes]
    candidate_author_hashes: jax.typing.ArrayLike     # 候选作者哈希 [B, C, num_author_hashes]
    candidate_product_surface: jax.typing.ArrayLike   # 候选场景信息 [B, C]


def block_user_reduce(
    user_hashes: jnp.ndarray,
    user_embeddings: jnp.ndarray,
    num_user_hashes: int,
    emb_size: int,
    embed_init_scale: float = 1.0,
) -> Tuple[jax.Array, jax.Array]:
    """
    将多个用户哈希嵌入合并为一个统一的用户表示。
    
    逻辑：
        1. 将多个哈希嵌入拼接（reshape）。
        2. 通过一个线性投影层（proj_mat_1）将其压缩回目标维度 D。
        3. 检查第一个哈希值是否为 0，以此生成填充掩码（padding mask）。
    """
    B = user_embeddings.shape[0]
    D = emb_size

    # 将 [B, num_user_hashes, D] 转换为 [B, 1, num_user_hashes * D]
    user_embedding = user_embeddings.reshape((B, 1, num_user_hashes * D))

    # 初始化并应用投影矩阵
    embed_init = hk.initializers.VarianceScaling(embed_init_scale, mode="fan_out")
    proj_mat_1 = hk.get_parameter(
        "proj_mat_1",
        [num_user_hashes * D, D],
        dtype=jnp.float32,
        init=lambda shape, dtype: embed_init(list(reversed(shape)), dtype).T,
    )

    user_embedding = jnp.dot(user_embedding.astype(proj_mat_1.dtype), proj_mat_1).astype(
        user_embeddings.dtype
    )

    # 哈希 0 保留用于填充
    user_padding_mask = (user_hashes[:, 0] != 0).reshape(B, 1).astype(jnp.bool_)

    return user_embedding, user_padding_mask


def block_history_reduce(
    history_post_hashes: jnp.ndarray,
    history_post_embeddings: jnp.ndarray,
    history_author_embeddings: jnp.ndarray,
    history_product_surface_embeddings: jnp.ndarray,
    history_actions_embeddings: jnp.ndarray,
    num_item_hashes: int,
    num_author_hashes: int,
    embed_init_scale: float = 1.0,
) -> Tuple[jax.Array, jax.Array]:
    """
    合并历史记录中的多种嵌入（推文、作者、行为、场景）生成序列。
    
    逻辑：
        1. 展平并拼接推文与作者的多个哈希嵌入。
        2. 将行为嵌入和场景嵌入一并拼接。
        3. 通过线性投影（proj_mat_3）映射到统一维度 D。
    """
    B, S, _, D = history_post_embeddings.shape

    # 展平多哈希维度
    history_post_embeddings_reshaped = history_post_embeddings.reshape((B, S, num_item_hashes * D))
    history_author_embeddings_reshaped = history_author_embeddings.reshape(
        (B, S, num_author_hashes * D)
    )

    # 全特征拼接
    post_author_embedding = jnp.concatenate(
        [
            history_post_embeddings_reshaped,
            history_author_embeddings_reshaped,
            history_actions_embeddings,
            history_product_surface_embeddings,
        ],
        axis=-1,
    )

    # 投影至目标维度
    embed_init = hk.initializers.VarianceScaling(embed_init_scale, mode="fan_out")
    proj_mat_3 = hk.get_parameter(
        "proj_mat_3",
        [post_author_embedding.shape[-1], D],
        dtype=jnp.float32,
        init=lambda shape, dtype: embed_init(list(reversed(shape)), dtype).T,
    )

    history_embedding = jnp.dot(post_author_embedding.astype(proj_mat_3.dtype), proj_mat_3).astype(
        post_author_embedding.dtype
    )

    history_embedding = history_embedding.reshape(B, S, D)

    # 生成历史序列的有效性掩码
    history_padding_mask = (history_post_hashes[:, :, 0] != 0).reshape(B, S)

    return history_embedding, history_padding_mask


def block_candidate_reduce(
    candidate_post_hashes: jnp.ndarray,
    candidate_post_embeddings: jnp.ndarray,
    candidate_author_embeddings: jnp.ndarray,
    candidate_product_surface_embeddings: jnp.ndarray,
    num_item_hashes: int,
    num_author_hashes: int,
    embed_init_scale: float = 1.0,
) -> Tuple[jax.Array, jax.Array]:
    """
    合并候选推文的特征嵌入。
    
    逻辑与历史记录类似，但去掉了历史行为特征，因为候选推文尚未发生互动。
    """
    B, C, _, D = candidate_post_embeddings.shape

    candidate_post_embeddings_reshaped = candidate_post_embeddings.reshape(
        (B, C, num_item_hashes * D)
    )
    candidate_author_embeddings_reshaped = candidate_author_embeddings.reshape(
        (B, C, num_author_hashes * D)
    )

    post_author_embedding = jnp.concatenate(
        [
            candidate_post_embeddings_reshaped,
            candidate_author_embeddings_reshaped,
            candidate_product_surface_embeddings,
        ],
        axis=-1,
    )

    embed_init = hk.initializers.VarianceScaling(embed_init_scale, mode="fan_out")
    proj_mat_2 = hk.get_parameter(
        "proj_mat_2",
        [post_author_embedding.shape[-1], D],
        dtype=jnp.float32,
        init=lambda shape, dtype: embed_init(list(reversed(shape)), dtype).T,
    )

    candidate_embedding = jnp.dot(
        post_author_embedding.astype(proj_mat_2.dtype), proj_mat_2
    ).astype(post_author_embedding.dtype)

    candidate_padding_mask = (candidate_post_hashes[:, :, 0] != 0).reshape(B, C).astype(jnp.bool_)

    return candidate_embedding, candidate_padding_mask


@dataclass
class PhoenixModelConfig:
    """
    推荐系统精排模型配置。
    
    属性:
        model: Transformer 的核心配置
        emb_size: 嵌入维度
        num_actions: 预测的目标互动行为数量
        history_seq_len: 用户历史序列最大长度
        candidate_seq_len: 一次评分的候选集最大数量
        fprop_dtype: 计算精度（默认使用 bfloat16 以加速推理）
    """
    model: TransformerConfig
    emb_size: int
    num_actions: int
    history_seq_len: int = 128
    candidate_seq_len: int = 32

    name: Optional[str] = None
    fprop_dtype: Any = jnp.bfloat16

    hash_config: HashConfig = None  # type: ignore

    product_surface_vocab_size: int = 16

    _initialized = False

    def __post_init__(self):
        if self.hash_config is None:
            self.hash_config = HashConfig()

    def initialize(self):
        self._initialized = True
        return self

    def make(self):
        """创建并返回模型实例。"""
        if not self._initialized:
            logger.warning(f"PhoenixModel {self.name} 尚未初始化，正在自动初始化。")
            self.initialize()

        return PhoenixModel(
            model=self.model.make(),
            config=self,
            fprop_dtype=self.fprop_dtype,
        )


@dataclass
class PhoenixModel(hk.Module):
    """
    基于 Transformer 的推荐模型实现。
    
    核心流程：
        1. 输入处理：将各类哈希、分类特征转换为嵌入。
        2. 聚合：使用 block_reduce 系列函数合并特征。
        3. 拼接序列：将 [用户, 历史1, ..., 历史N, 候选1, ..., 候选M] 拼接成一条长序列。
        4. Transformer：处理序列，通过特殊的注意力掩码确保候选对象评分的独立性。
        5. 解码：将 Transformer 输出的候选对象特征投影到各互动行为的概率空间（logits）。
    """
    model: Transformer
    config: PhoenixModelConfig
    fprop_dtype: Any = jnp.bfloat16
    name: Optional[str] = None

    def _get_action_embeddings(
        self,
        actions: jax.Array,
    ) -> jax.Array:
        """
        将多热（multi-hot）历史动作向量转换为嵌入。
        
        逻辑：
            将 0/1 动作映射为 -1/1，然后通过线性变换投影到嵌入空间。
        """
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

        # 转换为符号表示 (-1, 1)
        actions_signed = (2 * actions - 1).astype(jnp.float32)

        action_emb = jnp.dot(actions_signed.astype(action_projection.dtype), action_projection)

        # 应用掩码，确保没有动作的位置输出为 0
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
        """通过查找表将单热（single-hot）索引转换为嵌入向量。"""
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

    def _get_unembedding(self) -> jax.Array:
        """获取解嵌入（unembedding）矩阵，用于将模型特征映射回 logits 预测值。"""
        config = self.config
        embed_init = hk.initializers.VarianceScaling(1.0, mode="fan_out")
        unembed_mat = hk.get_parameter(
            "unembeddings",
            [config.emb_size, config.num_actions],
            dtype=jnp.float32,
            init=embed_init,
        )
        return unembed_mat

    def build_inputs(
        self,
        batch: RecsysBatch,
        recsys_embeddings: RecsysEmbeddings,
    ) -> Tuple[jax.Array, jax.Array, int]:
        """
        从批次数据和预查嵌入中构建 Transformer 的最终输入序列。

        返回:
            embeddings: 拼接后的嵌入序列 [B, 1 + S + C, D]
            padding_mask: 序列掩码 [B, 1 + S + C]
            candidate_start_offset: 序列中候选对象开始的索引位置
        """
        config = self.config
        hash_config = config.hash_config

        # 1. 转换场景特征为嵌入
        history_product_surface_embeddings = self._single_hot_to_embeddings(
            batch.history_product_surface,
            config.product_surface_vocab_size,
            config.emb_size,
            "product_surface_embedding_table",
        )
        candidate_product_surface_embeddings = self._single_hot_to_embeddings(
            batch.candidate_product_surface,
            config.product_surface_vocab_size,
            config.emb_size,
            "product_surface_embedding_table",
        )

        # 2. 转换历史行为为嵌入
        history_actions_embeddings = self._get_action_embeddings(batch.history_actions)

        # 3. 合并各类哈希嵌入表示
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

        candidate_embeddings, candidate_padding_mask = block_candidate_reduce(
            batch.candidate_post_hashes,
            recsys_embeddings.candidate_post_embeddings,
            recsys_embeddings.candidate_author_embeddings,
            candidate_product_surface_embeddings,
            hash_config.num_item_hashes,
            hash_config.num_author_hashes,
            1.0,
        )

        # 4. 拼接生成长序列：[用户] + [历史行为...] + [待选物品...]
        embeddings = jnp.concatenate(
            [user_embeddings, history_embeddings, candidate_embeddings], axis=1
        )
        padding_mask = jnp.concatenate(
            [user_padding_mask, history_padding_mask, candidate_padding_mask], axis=1
        )

        # 计算候选集在序列中的起始位置，供注意力掩码使用
        candidate_start_offset = user_padding_mask.shape[1] + history_padding_mask.shape[1]

        return embeddings.astype(self.fprop_dtype), padding_mask, candidate_start_offset

    def __call__(
        self,
        batch: RecsysBatch,
        recsys_embeddings: RecsysEmbeddings,
    ) -> RecsysModelOutput:
        """
        执行精排前向传播。

        参数:
            batch: 包含原始哈希、动作等特征。
            recsys_embeddings: 预查出的嵌入向量。

        返回:
            每个候选对象针对各行为的 Logits。形状为 [B, num_candidates, num_actions]
        """
        # 1. 准备 Transformer 输入
        embeddings, padding_mask, candidate_start_offset = self.build_inputs(
            batch, recsys_embeddings
        )

        # 2. 调用 Transformer 堆栈进行上下文建模
        model_output = self.model(
            embeddings,
            padding_mask,
            candidate_start_offset=candidate_start_offset,
        )

        out_embeddings = model_output.embeddings

        # 3. 对输出进行归一化
        out_embeddings = layer_norm(out_embeddings)

        # 4. 仅提取候选对象对应的输出特征
        candidate_embeddings = out_embeddings[:, candidate_start_offset:, :]

        # 5. 通过解嵌入矩阵投影到分类空间（各互动行为的概率）
        unembeddings = self._get_unembedding()
        logits = jnp.dot(candidate_embeddings.astype(unembeddings.dtype), unembeddings)
        logits = logits.astype(self.fprop_dtype)

        return RecsysModelOutput(logits=logits)
