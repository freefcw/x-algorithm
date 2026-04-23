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
from recsys_model import HashConfig
from recsys_retrieval_model import PhoenixRetrievalModelConfig
from runners import (
    RecsysRetrievalInferenceRunner,
    RetrievalModelRunner,
    create_example_batch,
    create_example_corpus,
    ACTIONS,
)


def main():
    """
    推荐系统召回（检索）演示脚本。
    演示如何使用双塔模型在大规模候选池中快速检索出用户感兴趣的推文。
    """
    # 1. 召回模型配置 - 复用精排的 Transformer 架构作为用户塔的核心
    emb_size = 128            # 向量空间维度
    num_actions = len(ACTIONS) 
    history_seq_len = 32      
    candidate_seq_len = 8     # 训练时的样本数量配置

    hash_config = HashConfig(
        num_user_hashes=2,
        num_item_hashes=2,
        num_author_hashes=2,
    )

    # 初始化召回模型配置
    retrieval_model_config = PhoenixRetrievalModelConfig(
        emb_size=emb_size,
        history_seq_len=history_seq_len,
        candidate_seq_len=candidate_seq_len,
        hash_config=hash_config,
        product_surface_vocab_size=16,
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

    # 2. 创建推理运行器
    inference_runner = RecsysRetrievalInferenceRunner(
        runner=RetrievalModelRunner(
            model=retrieval_model_config,
            bs_per_device=0.125,
        ),
        name="retrieval_local",
    )

    print("正在初始化召回模型...")
    inference_runner.initialize()
    print("召回模型初始化成功!")

    # 3. 创建模拟用户及其历史记录
    print("\n" + "=" * 70)
    print("召回系统演示（Phoenix Retrieval Demo）")
    print("=" * 70)

    batch_size = 2  # 同时演示两个用户的检索请求
    example_batch, example_embeddings = create_example_batch(
        batch_size=batch_size,
        emb_size=emb_size,
        history_len=history_seq_len,
        num_candidates=candidate_seq_len,
        num_actions=num_actions,
        num_user_hashes=hash_config.num_user_hashes,
        num_item_hashes=hash_config.num_item_hashes,
        num_author_hashes=hash_config.num_author_hashes,
        product_surface_vocab_size=16,
    )

    valid_history_count = int((example_batch.history_post_hashes[:, :, 0] != 0).sum())  # type: ignore
    print(f"\n用户历史总记录数: {valid_history_count}")

    # 第一步：构建全量候选池（模拟海量推文库）
    print("\n" + "-" * 70)
    print("第一步：构建候选集索引（Corpus）")
    print("-" * 70)

    corpus_size = 1000  # 模拟 1000 条推文的索引库
    corpus_embeddings, corpus_post_ids = create_example_corpus(
        corpus_size=corpus_size,
        emb_size=emb_size,
        seed=456,
    )
    print(f"池大小: {corpus_size} 个物品")
    print(f"向量库形状: {corpus_embeddings.shape}")

    # 将生成的索引库加载到运行器中
    inference_runner.set_corpus(corpus_embeddings, corpus_post_ids)

    # 第二步：执行向量检索召回 Top-K
    print("\n" + "-" * 70)
    print("第二步：执行向量相似度检索（Top-K）")
    print("-" * 70)

    top_k = 10
    retrieval_output = inference_runner.retrieve(
        example_batch,
        example_embeddings,
        top_k=top_k,
    )

    print(f"\n为 {batch_size} 位用户分别检索出前 {top_k} 个候选物品:")

    top_k_indices = np.array(retrieval_output.top_k_indices)
    top_k_scores = np.array(retrieval_output.top_k_scores)

    for user_idx in range(batch_size):
        print(f"\n  用户 {user_idx + 1}:")
        print(f"    {'Rank':<6} {'Post ID':<12} {'Similarity':<12}")
        print(f"    {'-' * 35}")
        for rank in range(top_k):
            post_id = top_k_indices[user_idx, rank]
            score = top_k_scores[user_idx, rank]
            # 文本进度条展示相似度得分
            bar = "█" * int((score + 1) * 10) + "░" * (20 - int((score + 1) * 10))
            print(f"    {rank + 1:<6} {post_id:<12} {bar} {score:.4f}")

    print("\n" + "=" * 70)
    print("演示结束!")
    print("=" * 70)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
