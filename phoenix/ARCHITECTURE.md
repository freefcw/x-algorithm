# Phoenix 推荐系统架构与流程文档

本文档详细描述了 Phoenix 推荐系统的系统架构、精排逻辑以及召回逻辑。

## 1. 系统架构图

系统采用模块化设计，底层基于 JAX/Haiku 实现，各组件职责清晰：

```mermaid
graph TD
    subgraph Scripts [应用脚本层]
        RR[run_ranker.py]
        RT[run_retrieval.py]
    end

    subgraph Runners [执行引擎层]
        R_R[RecsysInferenceRunner]
        R_T[RecsysRetrievalInferenceRunner]
        DB[Dummy Data / Example Batch]
    end

    subgraph Models [模型逻辑层]
        PM[PhoenixModel - 精排]
        PRM[PhoenixRetrievalModel - 召回]
        CT[CandidateTower - 物品塔]
    end

    subgraph Grok [基础算子层]
        T[Transformer Core]
        RoPE[Rotary Embedding]
        Mask[Recsys Attention Mask]
        Norm[RMSNorm / LayerNorm]
    end

    %% 依赖关系
    RR --> R_R
    RT --> R_T
    R_R --> PM
    R_T --> PRM
    PM --> T
    PRM --> T
    PRM --> CT
    T --> RoPE
    T --> Mask
    T --> Norm
```

---

## 2. 精排业务流程图 (Ranking Flow)

精排模型将用户信息、历史行为和候选集拼接为长序列，利用 Transformer 的上下文建模能力进行评分。

```mermaid
sequenceDiagram
    participant Data as 特征输入 (Hashes/Actions)
    participant Emb as 嵌入表 (Embeddings)
    participant Red as 哈希聚合 (Hash Reduction)
    participant Trans as Transformer (Grok)
    participant Out as Logits/概率输出

    Data->>Emb: 提供用户/历史/候选 ID
    Emb->>Red: 返回原始嵌入向量 (Multi-Hashes)
    Red->>Red: 执行线性投影 (D*N -> D)
    Red->>Trans: 拼接序列 [User, History, Candidates]
    Note over Trans: 应用 Recsys 专用掩码<br/>(候选集之间互不可见)
    Trans->>Out: 提取候选集位置特征
    Out->>Out: Unembedding 映射 + Sigmoid
    Out-->>Ranker: 返回各项行为概率 (Favorite/Reply/etc.)
```

---

## 3. 召回业务流程图 (Retrieval Flow)

召回系统采用双塔架构，通过用户塔和物品塔的向量化映射实现高效检索。

```mermaid
graph LR
    subgraph UserTower [用户塔]
        UH[用户 & 历史特征] --> UT[Transformer]
        UT --> MP[均值池化 Mean Pooling]
        MP --> LN1[L2 归一化]
        LN1 --> UV[用户向量 UV]
    end

    subgraph ItemTower [物品塔]
        IF[推文 & 作者特征] --> MLP[多层感知机]
        MLP --> LN2[L2 归一化]
        LN2 --> IV[物品向量 IV]
    end

    subgraph Retrieval [检索逻辑]
        UV --> Dot[点积相似度计算]
        IV --> Dot
        Dot --> TopK[Top-K 排序召回]
    end
```

---

## 4. 关键技术点
1.  **旋转位置嵌入 (RoPE):** 在 `grok.py` 中实现，使 Transformer 能够捕捉历史行为的相对时序特征。
2.  **推荐系统专用掩码:** 确保在一次 Transformer 前向计算中，多个候选推文能够同时评分且互不干扰。
3.  **多头哈希 (Multi-Hash):** 缓解了大规模 ID 的碰撞问题，提高了特征表示的鲁棒性。
4.  **双塔架构:** 支持离线预计算物品向量库，在线仅需编码用户向量，极大提升了召回效率。
