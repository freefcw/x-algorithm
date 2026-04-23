# Phoenix 系统总览

## 1. Phoenix 是什么

Phoenix 是一个两阶段推荐系统样例：

- 第一阶段做召回，从大规模候选池中取出少量高相关候选。
- 第二阶段做精排，对已召回候选进行更细的多目标打分与排序。

它不是一个“只有模型文件”的仓库，而是一个从模型、推理器、演示脚本到 HTTP 服务都具备的最小闭环。

## 2. 模块分层

```mermaid
graph TD
    subgraph Entry[入口层]
        A1[run_ranker.py]
        A2[run_retrieval.py]
        A3[api_server.py]
        A4[services/ranker_service.py]
        A5[services/retrieval_service.py]
    end

    subgraph Runner[推理执行层]
        B1[ModelRunner]
        B2[RecsysInferenceRunner]
        B3[RetrievalModelRunner]
        B4[RecsysRetrievalInferenceRunner]
    end

    subgraph Model[模型层]
        C1[PhoenixModel]
        C2[PhoenixRetrievalModel]
        C3[CandidateTower]
    end

    subgraph Core[基础算子层]
        D1[Transformer]
        D2[DecoderLayer]
        D3[MultiHeadAttention]
        D4[RotaryEmbedding]
        D5[make_recsys_attn_mask]
    end

    subgraph Support[服务配套层]
        E1[FeatureStore]
        E2[ModelRegistry]
        E3[MetricsCollector]
        E4[Config]
    end

    A1 --> B1
    A1 --> B2
    A2 --> B3
    A2 --> B4
    A3 --> B2
    A3 --> B4
    A4 --> E1
    A4 --> E2
    A4 --> E3
    A4 --> E4
    A5 --> E1
    A5 --> E2
    A5 --> E3
    A5 --> E4
    B1 --> C1
    B2 --> C1
    B3 --> C2
    B4 --> C2
    C1 --> D1
    C2 --> D1
    C2 --> C3
    D1 --> D2
    D2 --> D3
    D3 --> D4
    D1 --> D5
```

## 3. 端到端业务流

```mermaid
sequenceDiagram
    participant User as 用户请求
    participant Retrieval as 召回模型
    participant Corpus as 候选向量库
    participant Ranker as 精排模型
    participant Feed as 输出结果

    User->>Retrieval: 用户特征 + 历史行为
    Retrieval->>Retrieval: 编码用户向量
    Retrieval->>Corpus: 点积相似度检索 Top-K
    Corpus-->>Retrieval: 候选集合
    Retrieval-->>Ranker: 候选 IDs / 候选特征
    Ranker->>Ranker: 构造 [用户, 历史, 候选] 序列
    Ranker->>Ranker: 候选隔离 Transformer 打分
    Ranker-->>Feed: 多行为分数 + 排序索引
```

## 4. Phoenix 里的核心数据结构

| 类型 | 位置 | 作用 | 关键形状 |
| --- | --- | --- | --- |
| `HashConfig` | `recsys_model.py` | 定义用户、物品、作者各用多少个哈希 embedding | 用户/物品/作者哈希数 |
| `RecsysBatch` | `recsys_model.py` | 承载原始离散特征、动作特征、场景特征 | 用户 `[B, H_u]`，历史 `[B, S, ...]`，候选 `[B, C, ...]` |
| `RecsysEmbeddings` | `recsys_model.py` | 承载特征查表后的 embedding | 用户 `[B, H_u, D]`，历史/候选 `[B, T, H, D]` |
| `PhoenixModelConfig` | `recsys_model.py` | 精排模型配置 | `emb_size`、`num_actions`、`history_seq_len`、`candidate_seq_len` |
| `PhoenixRetrievalModelConfig` | `recsys_retrieval_model.py` | 召回模型配置 | `emb_size`、`history_seq_len` |
| `RankingOutput` | `runners.py` | 精排后的概率与排序索引 | `scores [B, C, A]`、`ranked_indices [B, C]` |
| `RetrievalOutput` | `recsys_retrieval_model.py` / `runners.py` | 召回结果 | 用户向量 `[B, D]`、Top-K 索引 `[B, K]` |

## 5. 代码主路径

### 5.1 精排主路径

```text
run_ranker.py
  -> RecsysInferenceRunner.initialize()
  -> PhoenixModel.__call__()
  -> PhoenixModel.build_inputs()
  -> Transformer.__call__(candidate_start_offset != None)
  -> logits -> sigmoid -> argsort
```

### 5.2 召回主路径

```text
run_retrieval.py
  -> RecsysRetrievalInferenceRunner.initialize()
  -> PhoenixRetrievalModel.build_user_representation()
  -> PhoenixRetrievalModel._retrieve_top_k()
  -> Top-K 索引和分数
```

## 6. 初始化和推理生命周期

```mermaid
flowchart TD
    A[构造 Config] --> B[创建 Runner]
    B --> C[create_dummy_batch / embeddings]
    C --> D[Haiku init 初始化参数]
    D --> E[缓存 model 实例]
    E --> F[生成 apply 函数]
    F --> G[接受真实 batch]
    G --> H[JAX/Haiku 前向推理]
    H --> I[业务后处理<br/>排序 / Top-K / HTTP 响应]
```

这里的关键点是：

- Phoenix 先用 dummy batch 触发 Haiku 参数创建。
- 然后把 `apply` 函数缓存起来，后续只喂真实数据。
- 当前仓库默认是“随机初始化参数 + 演示推理”，checkpoint 属于可选能力。

## 7. 当前系统边界

Phoenix 现在更像“推理框架样例 + 服务原型”，而不是完整生产系统：

- 没有训练入口、损失函数和数据管道。
- 没有真实 embedding lookup 服务，`services/feature_store.py` 主要是 mock。
- 没有真实 ANN 检索集成，`retrieval_service.py` 中 FAISS 仍是预留接口。
- 服务接口是可运行原型，但还不是生产级请求协议。

这些边界不是缺陷描述，而是阅读后续文档时必须带着的前提。
