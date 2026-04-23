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

import _setup_path  # noqa: F401

import logging

import numpy as np

from grok import TransformerConfig
from recsys_model import PhoenixModelConfig, HashConfig
from runners import RecsysInferenceRunner, ModelRunner, create_example_batch, ACTIONS


def main():
    """
    推荐系统精排演示脚本。
    该脚本展示了如何配置模型、初始化推理环境并对模拟用户进行候选物品排序。
    """
    # 1. 模型基础架构配置
    emb_size = 128            # 嵌入维度
    num_actions = len(ACTIONS) # 预测的互动行为总数（点赞、转发、停留等）
    history_seq_len = 32      # 用户历史记录的最大长度
    candidate_seq_len = 8     # 单词评分任务中的候选物品数量

    # 2. 哈希配置
    # 定义将不同实体 ID 映射到嵌入空间时使用的哈希函数数量
    hash_config = HashConfig(
        num_user_hashes=2,
        num_item_hashes=2,
        num_author_hashes=2,
    )

    # 3. 组装模型配置
    recsys_model = PhoenixModelConfig(
        emb_size=emb_size,
        num_actions=num_actions,
        history_seq_len=history_seq_len,
        candidate_seq_len=candidate_seq_len,
        hash_config=hash_config,
        product_surface_vocab_size=16,
        # 定义底层的 Transformer 参数
        model=TransformerConfig(
            emb_size=emb_size,
            widening_factor=2,
            key_size=64,
            num_q_heads=2,
            num_kv_heads=2,
            num_layers=2,
            attn_output_multiplier=0.125,
        ),
    )

    # 4. 创建推理运行器
    # Runner 负责处理底层计算设备的分配（如 GPU/TPU）以及 Haiku 参数的初始化
    inference_runner = RecsysInferenceRunner(
        runner=ModelRunner(
            model=recsys_model,
            bs_per_device=0.125, # 针对演示场景调小每设备的批次大小
        ),
        name="recsys_local",
    )

    print("正在初始化模型...")
    inference_runner.initialize()
    print("模型初始化成功!")

    # 5. 创建模拟业务场景数据
    print("\n" + "=" * 70)
    print("推荐系统精排演示（Phoenix Ranker Demo）")
    print("=" * 70)

    batch_size = 1
    example_batch, example_embeddings = create_example_batch(
        batch_size=batch_size,
        emb_size=emb_size,
        history_len=history_seq_len,
        num_candidates=candidate_seq_len,
        num_actions=num_actions,
        num_user_hashes=hash_config.num_user_hashes,
        num_item_hashes=hash_config.num_item_hashes,
        num_author_hashes=hash_config.num_author_hashes,
        product_surface_vocab_size=recsys_model.product_surface_vocab_size,
    )

    # 处理行为名称，用于结果展示
    action_names = [action.replace("_", " ").title() for action in ACTIONS]

    # 统计历史有效记录（非填充位）
    valid_history_count = int((example_batch.history_post_hashes[:, :, 0] != 0).sum())  # type: ignore
    print(f"\n用户历史包含 {valid_history_count} 条互动记录")
    print(f"正在对 {candidate_seq_len} 个候选推文进行排序评分...")

    # 6. 执行排序推理
    ranking_output = inference_runner.rank(example_batch, example_embeddings)

    # 7. 解析并展示结果
    scores = np.array(ranking_output.scores[0])         # 概率分数矩阵 [候选, 动作]
    ranked_indices = np.array(ranking_output.ranked_indices[0]) # 排序后的索引序列

    print("\n" + "-" * 70)
    print("精排结果（按预测‘点赞（Favorite）’概率降序排列）")
    print("-" * 70)

    for rank, idx in enumerate(ranked_indices):
        idx = int(idx)
        print(f"\n排名 {rank + 1}: ")
        print("  预测互动概率:")
        for action_idx, action_name in enumerate(action_names):
            prob = float(scores[idx, action_idx])
            # 使用简单的文本进度条展示概率大小
            bar = "█" * int(prob * 20) + "░" * (20 - int(prob * 20))
            print(f"    {action_name:24s}: {bar} {prob:.3f}")

    print("\n" + "=" * 70)
    print("演示结束!")
    print("=" * 70)


if __name__ == "__main__":
    # 配置日志输出
    logging.basicConfig(level=logging.INFO)
    main()
