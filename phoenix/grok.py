# 版权所有 2026 X.AI Corp.
#
# 根据 Apache 许可证 2.0 版本（“许可证”）授权；
# 除非遵守许可证，否则您不得使用此文件。
# 您可以在以下网址获得许可证副本：
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# 除非适用法律要求或书面同意，否则根据许可证分发的软件
# 是按“原样”基础分发的，不附带任何形式的明示或暗示的保证或条件。
# 请参阅许可证以了解管理权限和限制的特定语言。

import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import NamedTuple

import haiku as hk
import jax
import jax.numpy as jnp

# 初始化日志记录器，用于调试和运行时信息记录
logger = logging.getLogger(__name__)


class TrainingState(NamedTuple):
    """
    训练状态容器。
    封装了模型在训练过程中的参数（params），便于状态管理和序列化。
    """
    params: hk.Params


def ffn_size(emb_size, widening_factor):
    """
    计算前馈网络（FFN）的中间层维度。
    
    参数:
        emb_size: 输入嵌入维度
        widening_factor: 扩展因子，通常为 4.0
        
    逻辑:
        1. 根据扩展因子计算初步大小，采用常用的 2/3 倍率优化（SwiGLU 等变体的常见做法）。
        2. 确保结果是 8 的倍数，以优化硬件（如 TPU/GPU）的计算效率。
    """
    _ffn_size = int(widening_factor * emb_size) * 2 // 3
    _ffn_size = _ffn_size + (8 - _ffn_size) % 8  # 向上取整到 8 的倍数
    logger.debug(f"emd_size: {emb_size} adjusted ffn_size: {_ffn_size}")
    return _ffn_size


def make_recsys_attn_mask(
    seq_len: int,
    candidate_start_offset: int,
    dtype: jnp.dtype = jnp.float32,
) -> jax.Array:
    """
    为推荐系统推理创建专门的注意力掩码。

    设计初衷:
    在推荐场景中，输入序列通常包含 [用户信息, 历史行为, 待选候选集]。
    - 位置 0 到 candidate_start_offset-1 (用户+历史): 使用因果注意力（causal attention），
      模拟时间顺序，即后面的行为只能看到前面的行为。
    - 位置 candidate_start_offset 之后 (候选集): 
      每个候选对象应独立评分。它们可以“看”到用户和历史背景，也可以看到自己（自注意力），
      但绝对不能看到其他候选对象，以防评分相互干扰。

    返回:
        形状为 [1, 1, seq_len, seq_len] 的张量，1 表示“允许关注”，0 表示“屏蔽”。
    """
    # 1. 首先为整个序列创建一个下三角因果掩码
    causal_mask = jnp.tril(jnp.ones((1, 1, seq_len, seq_len), dtype=dtype))

    # 2. 将候选集区域（右下角块）的注意力清零，阻断候选集之间的相互观察
    attn_mask = causal_mask.at[:, :, candidate_start_offset:, candidate_start_offset:].set(0)

    # 3. 恢复候选集的自注意力（对角线位置），确保每个候选对象能处理自身信息
    candidate_indices = jnp.arange(candidate_start_offset, seq_len)
    attn_mask = attn_mask.at[:, :, candidate_indices, candidate_indices].set(1)

    return attn_mask


def right_anchored_rope_positions(
    padding_mask: jax.Array,
    history_seq_len: int,
    num_user_prefix_tokens: int,
) -> jax.Array:
    """Keep the newest valid history item at a stable RoPE position."""
    history_start = num_user_prefix_tokens
    history_end = history_start + history_seq_len

    indices = jnp.arange(padding_mask.shape[1], dtype=jnp.int32)[None, :]
    history_length = padding_mask[:, history_start:history_end].sum(
        axis=1, dtype=jnp.int32
    )
    positions = jnp.where(
        (history_start <= indices) & (indices < history_end),
        history_end - history_length[:, None] + indices - history_start,
        indices,
    )
    positions = jnp.where(indices >= history_end, history_end, positions)
    return jnp.where(padding_mask, positions, 0).astype(jnp.float32)


class MHAOutput(NamedTuple):
    """多头注意力（Multi-Head Attention）操作的输出容器。"""
    embeddings: jax.Array


class DecoderOutput(NamedTuple):
    """解码器层（Decoder Layer）的输出容器。"""
    embeddings: jax.Array


class TransformerOutput(NamedTuple):
    """Transformer 堆栈的最终输出容器。"""
    embeddings: jax.Array


@dataclass
class TransformerConfig:
    """
    Transformer 架构的配置参数类。
    
    属性:
        emb_size: 模型隐藏层维度（D）
        key_size: 每个注意力头的维度（K）
        num_q_heads: Query 头的数量
        num_kv_heads: Key/Value 头的数量（支持分组查询注意力 GQA）
        num_layers: 堆叠层数
        widening_factor: FFN 层的扩展倍数
        attn_output_multiplier: 注意力输出的缩放系数
    """
    emb_size: int
    key_size: int
    num_q_heads: int
    num_kv_heads: int
    num_layers: int
    widening_factor: float = 4.0

    attn_output_multiplier: float = 1.0

    name: str | None = None

    def make(self) -> "Transformer":
        """根据当前配置实例化 Transformer 模块。"""
        return Transformer(
            num_q_heads=self.num_q_heads,
            num_kv_heads=self.num_kv_heads,
            widening_factor=self.widening_factor,
            key_size=self.key_size,
            attn_output_multiplier=self.attn_output_multiplier,
            num_layers=self.num_layers,
        )


def hk_rms_norm(
    x: jax.Array,
    fixed_scale=False,
) -> jax.Array:
    """
    应用 RMSNorm（均方根归一化）。
    相比 LayerNorm，RMSNorm 移除了平移不变性，计算更高效，常用于大型语言模型。
    """
    ln = RMSNorm(axis=-1, create_scale=not fixed_scale)
    return ln(x)


class Linear(hk.Linear):
    """
    自定义线性层，集成了 Haiku 的参数管理。
    
    相比标准 hk.Linear，它明确了权重和偏置的初始化方式，并处理了 fp32 到混合精度的转换。
    """
    def __init__(
        self,
        output_size: int,
        with_bias: bool = True,
        name: str | None = None,
    ):
        super().__init__(
            output_size=output_size,
            with_bias=with_bias,
            name=name,
        )

    def __call__(  # type: ignore
        self,
        inputs: jax.Array,
    ) -> jax.Array:
        """执行线性变换: outputs = inputs * W + b"""

        fprop_dtype = inputs.dtype # 获取输入的计算精度（如 bfloat16）
        if not inputs.shape:
            raise ValueError("输入不能是标量。")

        input_size = inputs.shape[-1]
        output_size = self.output_size

        # 获取或创建权重参数 W。从零训练时必须随机初始化：全零权重会让整层输出与梯度
        # 恒为 0，Transformer 永远无法离开初始点（加载 checkpoint 时初始值会被覆盖）。
        w = hk.get_parameter(
            "w",
            [input_size, output_size],
            jnp.float32,
            init=hk.initializers.TruncatedNormal(stddev=1.0 / math.sqrt(input_size)),
        )

        # 执行矩阵乘法，将权重转换为计算精度
        out = jnp.dot(inputs, w.astype(fprop_dtype))
        
        # 如果启用偏置，获取并加在结果上
        if self.with_bias:
            b = hk.get_parameter(
                "b", [self.output_size], jnp.float32, init=hk.initializers.Constant(0)
            )
            b = jnp.broadcast_to(b, out.shape)
            out = out + b.astype(fprop_dtype)

        return out


class RMSNorm(hk.RMSNorm):
    """
    RMSNorm 的具体实现。
    """
    def __init__(
        self,
        axis: int | Sequence[int] | slice,
        eps: float = 1e-5,
        name: str | None = None,
        create_scale: bool = True,
    ):
        super().__init__(axis, eps, create_scale=create_scale, name=name)

    def __call__(self, inputs: jax.Array):
        fprop_dtype = inputs.dtype
        param_shape = (inputs.shape[-1],)
        
        # 如果需要可学习的缩放因子
        if self.create_scale:
            scale = hk.get_parameter(
                "scale",
                param_shape,
                dtype=jnp.float32,
                init=hk.initializers.Constant(1),
            )
            scale = jnp.broadcast_to(scale.astype(jnp.float32), inputs.shape)
        else:
            scale = 1.0
            
        inputs = inputs.astype(jnp.float32) # 在 fp32 下执行归一化以保证数值稳定性
        scale = jnp.float32(scale)
        
        # 计算均方根
        mean_squared = jnp.mean(jnp.square(inputs), axis=[-1], keepdims=True)
        mean_squared = jnp.broadcast_to(mean_squared, inputs.shape)

        # 执行归一化操作
        normed_inputs = inputs * jax.lax.rsqrt(mean_squared + self.eps)

        # 应用缩放并转回目标计算精度
        outputs = scale * normed_inputs
        return outputs.astype(fprop_dtype)


def rotate_half(
    x: jax.Array,
) -> jax.Array:
    """
    将输入张量的特征维度对半拆分并进行旋转变换。
    这是旋转位置嵌入（RoPE）的核心步骤。
    """
    x1, x2 = jnp.split(x, 2, axis=-1)
    return jnp.concatenate((-x2, x1), axis=-1)


class RotaryEmbedding(hk.Module):
    """
    旋转位置嵌入 (RoPE) 的实现。
    参考: https://arxiv.org/abs/2104.09864
    
    RoPE 通过将查询和键旋转特定的角度，使得注意力机制能够捕捉到相对位置信息。
    """

    def __init__(
        self,
        dim: int,
        name: str | None = None,
        base_exponent: int = 10000,
    ):
        super().__init__(name)
        self.dim = dim
        self.base_exponent = base_exponent
        assert self.dim % 2 == 0 # 维度必须是偶数以便对半拆分

    def __call__(
        self,
        x: jax.Array,
        seq_dim: int,
        offset: jax.Array,
        const_position: int | None = None,
        t: jax.Array | None = None,
    ) -> jax.Array:
        fprop_dtype = x.dtype
        # 1. 计算每个维度的频率系数
        exponents = jnp.arange(0, self.dim, 2, dtype=jnp.float32)
        inv_freq = jnp.asarray(
            1.0 / (self.base_exponent ** (exponents / self.dim)), dtype=jnp.float32
        )

        if jnp.shape(offset) == ():
            # 偏移量可以是标量，也可以是每个 batch 一个偏移
            offset = jnp.expand_dims(offset, 0)

        # 2. 计算每个位置的相位（用于 sin 和 cos）
        if const_position:
            # 固定位置模式
            t = const_position * jnp.ones(
                (
                    1,
                    x.shape[seq_dim],
                ),
                dtype=jnp.float32,
            )
        elif t is None:
            # 正常序列模式
            t = jnp.arange(x.shape[seq_dim], dtype=jnp.float32) + jnp.expand_dims(offset, -1)
        
        # 相位计算: phase = t * inv_freq
        phase = jnp.einsum("bi,j->bij", t, inv_freq)
        phase = jnp.tile(phase, reps=(1, 2))[:, :, None, :]

        # 3. 应用旋转公式: x_rotated = x * cos(phase) + rotate_half(x) * sin(phase)
        x = x * jnp.cos(phase) + rotate_half(x) * jnp.sin(phase)
        x = x.astype(fprop_dtype)

        return x


class MultiHeadAttention(hk.Module):
    """
    多头注意力模块实现。
    支持：
    1. 标准多头注意力（MHA）和分组查询注意力（GQA）。
    2. 旋转位置嵌入（RoPE）。
    3. tanh 门控注意力（用于稳定训练）。
    """
    def __init__(
        self,
        num_q_heads: int,
        num_kv_heads: int,
        key_size: int,
        *,
        with_bias: bool = True,
        value_size: int | None = None,
        model_size: int | None = None,
        attn_output_multiplier: float = 1.0,
        name: str | None = None,
    ):
        super().__init__(name=name)
        self.num_q_heads = num_q_heads
        self.num_kv_heads = num_kv_heads
        self.key_size = key_size
        self.value_size = value_size or key_size
        self.model_size = model_size or key_size * num_q_heads
        self.attn_output_multiplier = attn_output_multiplier
        self.with_bias = with_bias

    def __call__(
        self,
        query: jax.Array,
        key: jax.Array,
        value: jax.Array,
        mask: jax.Array,
        positions: jax.Array | None = None,
    ) -> MHAOutput:
        projection = self._linear_projection

        # 检查 key/value 的 batch 和 序列长度 是否一致
        assert key.shape[:2] == value.shape[:2], f"key/value shape: {key.shape}/{value.shape}"

        # 验证 mask 维度合法性 [B, 1, T_query, T_key]
        if mask is not None:
            assert mask.ndim == 4
            assert mask.shape[0] in {1, query.shape[0]}
            assert mask.shape[1] == 1
            assert mask.shape[2] in {1, query.shape[1]}
            assert mask.shape[3] in {1, key.shape[1]}

        # 1. 线性投影生成 Q, K, V
        assert self.num_q_heads % self.num_kv_heads == 0
        query_heads = projection(query, self.key_size, self.num_q_heads, name="query")
        key_heads = projection(key, self.key_size, self.num_kv_heads, name="key")
        value_heads = projection(value, self.value_size, self.num_kv_heads, name="value")

        # 2. 应用 RoPE
        rotate = RotaryEmbedding(dim=self.key_size, base_exponent=int(1e4))
        key_heads = rotate(key_heads, seq_dim=1, offset=0, t=positions)
        query_heads = rotate(query_heads, seq_dim=1, offset=0, t=positions)

        b, t, h, d = query_heads.shape
        _, _, kv_h, _ = key_heads.shape
        assert h % kv_h == 0, f"query_heads {h} 必须是 kv_heads {kv_h} 的整数倍"

        # 针对 GQA 重新调整形状，支持多 Q 头对应一个 KV 头
        query_heads = jnp.reshape(query_heads, (b, t, kv_h, h // kv_h, d))

        # 3. 计算注意力权重 (Dot Product Attention)
        # 注意：注意力 softmax 始终在 fp32 下进行以维持精度
        attn_logits = jnp.einsum("...thHd,...Thd->...hHtT", query_heads, key_heads).astype(
            jnp.float32
        )
        
        # 应用缩放因子
        attn_logits *= self.attn_output_multiplier
        
        # 使用 tanh 限制注意力分数的幅度，防止溢出或数值不稳定
        max_attn_val = jnp.array(30.0, dtype=attn_logits.dtype)
        attn_logits = max_attn_val * jnp.tanh(attn_logits / max_attn_val)

        # 增加 Head 维度以匹配 mask 形状
        mask = mask[:, :, None, :, :]

        # 4. 应用掩码 (Masking)
        if mask is not None:
            if mask.ndim != attn_logits.ndim:
                raise ValueError(f"Mask 维度 {mask.ndim} 与 Logits 维度 {attn_logits.ndim} 不匹配")
            # 将被掩盖的位置设为一个极小值（接近负无穷），使 softmax 后权重趋近于 0
            attn_logits = jnp.where(mask, attn_logits, -1e30)
            
        attn_weights = jax.nn.softmax(attn_logits).astype(query.dtype)

        # 5. 加权求和 (Aggregation)
        attn = jnp.einsum("...hHtT,...Thd->...thHd", attn_weights, value_heads)
        leading_dims = attn.shape[:2]
        # 展平多头
        attn = jnp.reshape(attn, (*leading_dims, -1))

        # 6. 最后的线性变换，映射回模型维度
        final_projection = Linear(self.model_size, with_bias=False)
        return MHAOutput(final_projection(attn))

    @hk.transparent
    def _linear_projection(
        self,
        x: jax.Array,
        head_size: int,
        num_heads: int,
        name: str | None = None,
    ) -> jax.Array:
        """内部辅助函数：执行线性映射并调整张量形状。"""
        y = Linear(num_heads * head_size, with_bias=False, name=name)(x)
        *leading_dims, _ = x.shape
        return y.reshape((*leading_dims, num_heads, head_size))


@dataclass
class MHABlock(hk.Module):
    """
    MHA 块封装，负责注意力计算逻辑的组织。
    """
    num_q_heads: int
    num_kv_heads: int
    key_size: int
    attn_output_multiplier: float = 1.0

    @hk.transparent
    def __call__(
        self,
        inputs: jax.Array,  # [B, T, D]
        mask: jax.Array,  # [B, 1, T, T]
        positions: jax.Array | None = None,
    ) -> MHAOutput:
        _, _, model_size = inputs.shape
        assert mask.ndim == 4
        
        # 注意力计算
        def attn_block(query, key, value, mask) -> MHAOutput:
            return MultiHeadAttention(
                num_q_heads=self.num_q_heads,
                num_kv_heads=self.num_kv_heads,
                key_size=self.key_size,
                model_size=model_size,
                attn_output_multiplier=self.attn_output_multiplier,
            )(query, key, value, mask, positions=positions)

        # 在当前的简化实现中，Self-Attention 的 Q, K, V 均源自输入 inputs
        attn_output = attn_block(inputs, inputs, inputs, mask)
        return MHAOutput(embeddings=attn_output.embeddings)


@dataclass
class DenseBlock(hk.Module):
    """
    前馈神经网络 (FFN) 块。
    通常由两层线性变换和一个非线性激活函数组成。
    这里使用了类似 GLU 的门控结构（h_v 与 gelu(h_w1) 相乘）。
    """
    num_q_heads: int
    num_kv_heads: int
    key_size: int
    widening_factor: float = 4.0

    @hk.transparent
    def __call__(
        self,
        inputs: jax.Array,
    ) -> jax.Array:
        _, _, model_size = inputs.shape
        
        # 1. 计算门控分支 1
        h_v = Linear(
            ffn_size(model_size, self.widening_factor),
            with_bias=False,
            name="linear_v",
        )(inputs)
        
        # 2. 计算激活分支 2
        h_w1 = jax.nn.gelu(
            Linear(
                ffn_size(model_size, self.widening_factor),
                with_bias=False,
            )(inputs)
        )
        
        # 3. 元素级相乘（门控）并投影回原始维度
        h_dense = Linear(model_size, with_bias=False)(h_w1 * h_v)

        return h_dense


@dataclass
class DecoderLayer(hk.Module):
    """
    Transformer 解码器单层结构。
    包含：预归一化（Pre-Norm）下的注意力层和前馈网络层。
    """
    num_q_heads: int
    num_kv_heads: int
    key_size: int
    num_layers: int
    layer_index: int | None = None
    widening_factor: float = 4.0
    name: str | None = None
    attn_output_multiplier: float = 1.0

    def __call__(
        self,
        inputs: jax.Array,
        mask: jax.Array,
        padding_mask: jax.Array | None,
        positions: jax.Array | None = None,
    ) -> DecoderOutput:
        del padding_mask # 尚未使用的变量

        def layer_norm(x):
            return hk_rms_norm(x)

        h = inputs

        # 1. 注意力部分（带残差连接）
        attn_output = MHABlock(
            num_q_heads=self.num_q_heads,
            num_kv_heads=self.num_kv_heads,
            key_size=self.key_size,
            attn_output_multiplier=self.attn_output_multiplier,
        )(layer_norm(h), mask, positions=positions)
        
        h_attn = attn_output.embeddings
        h_attn = layer_norm(h_attn)
        h += h_attn # 残差连接

        # 2. 前馈网络部分（带残差连接）
        def base_dense_block(h):
            h = DenseBlock(
                num_q_heads=self.num_q_heads,
                num_kv_heads=self.num_kv_heads,
                key_size=self.key_size,
                widening_factor=self.widening_factor,
            )(h)
            return h

        h_dense = base_dense_block(layer_norm(h))
        h_dense = layer_norm(h_dense)
        h += h_dense # 残差连接

        return DecoderOutput(embeddings=h)


def layer_norm(x):
    """全局层归一化辅助函数。"""
    return hk_rms_norm(x)


@dataclass
class Transformer(hk.Module):
    """
    完整的 Transformer 堆栈模块。
    负责：
    1. 构建序列掩码（因果掩码或推荐系统专用掩码）。
    2. 循环堆叠多个 DecoderLayer。
    """
    num_q_heads: int
    num_kv_heads: int
    key_size: int
    widening_factor: float
    attn_output_multiplier: float
    num_layers: int
    name: str | None = None

    def __call__(
        self,
        embeddings: jax.Array,
        mask: jax.Array,
        candidate_start_offset: int | None = None,
        positions: jax.Array | None = None,
    ) -> TransformerOutput:
        """
        参数说明:
            embeddings: 输入嵌入序列 [B, T, D]
            mask: 填充掩码 [B, T]，True 表示有效位置
            candidate_start_offset: 推荐场景专用偏置。若提供，则激活推荐系统专用的注意力逻辑。
        """

        fprop_dtype = embeddings.dtype
        _, seq_len, _ = embeddings.shape
        padding_mask = mask.copy()
        mask = mask[:, None, None, :]  # 扩展维度以匹配注意力计算形状

        if candidate_start_offset is not None:
            # 应用推荐系统掩码：候选集仅关注背景和自身，互不干扰
            attn_mask = make_recsys_attn_mask(seq_len, candidate_start_offset, fprop_dtype)
            mask = mask * attn_mask
        else:
            # 应用标准因果掩码：仅允许关注历史信息
            causal_mask = jnp.tril(jnp.ones((1, 1, seq_len, seq_len))).astype(fprop_dtype)
            mask = mask * causal_mask

        h = embeddings

        # 循环应用每一层 DecoderLayer
        for i in range(self.num_layers):
            decoder_output = DecoderLayer(
                num_q_heads=self.num_q_heads,
                num_kv_heads=self.num_kv_heads,
                key_size=self.key_size,
                widening_factor=self.widening_factor,
                num_layers=self.num_layers,
                attn_output_multiplier=self.attn_output_multiplier,
                name=f"decoder_layer_{i}",
                layer_index=i,
            )(h, mask, padding_mask, positions=positions)
            h = decoder_output.embeddings

        return TransformerOutput(embeddings=h)
