# X "为你推荐" (For You) Feed 算法

本仓库是 X 平台"为你推荐"信息流核心推荐系统的**可运行移植版**。它把网内内容（来自你关注的账号）和网外内容（通过机器学习检索发现）结合起来，用基于 Grok 的 Transformer 模型统一排序。

> **注意：** Transformer 实现移植自 xAI 的 [Grok-1 开源版本](https://github.com/xai-org/grok-1)，并针对推荐场景做了调整。

## 项目现状 —— 请先读这一节

X 开源的是核心算法，不是完整的生产系统。原版依赖的内部服务（用户资料、内容存储、行为日志、内容安全）**没有**开源。本仓库用"trait 抽象的桩客户端 + 演示模式"补齐了缺口，让完整链路可以在本机真正跑起来：

| 能力 | 状态 |
|------|------|
| 全部编译通过（`cargo build --workspace`、`uv sync`） | 可用 |
| 本地运行精排 / 召回模型推理 | 可用（默认随机权重） |
| 模型以 HTTP 和 gRPC 服务方式对外提供 | 可用 |
| 训练自己的模型权重（模拟或真实数据） | 可用 |
| **端到端完整链路**（Thunder + Phoenix + Home Mixer）返回排序 Feed | 演示模式可用：`./scripts/run_demo.sh` |
| 接入真实数据的生产部署 | 需要集成开发——把 `home-mixer/clients/` 下的桩客户端替换为你平台的服务，缺口清单见[从演示到真实系统](docs/getting-started/06-从演示到真实系统.md) |

仓库不包含预训练权重。随机权重下链路能跑通、能排序，但分数要在你自己训练后才有业务意义（演示配置在 CPU 上几分钟即可训完）。

## 快速开始

前置：[Rust](https://rustup.rs/)、`protoc`（`brew install protobuf`）、[uv](https://docs.astral.sh/uv/)。演示不需要 Kafka、Redis 或 GPU。

```bash
# 一条命令：编译、启动三个服务、请求一次 Feed、打印结果
cd phoenix && uv sync --dev --group service && cd ..
./scripts/run_demo.sh
```

预期输出——一列混合两路召回的排序 Feed：

```text
#    帖子 ID                作者       得分         网内         来源
1    2079102310290007235  212      0.0031     否          Phoenix 网外
...
34   2079100111060730041  101      0.0008     是          Thunder 网内
共 35 条：网内 4 条 + 网外 31 条。链路打通。
```

演示先保留打分后的 Top 50，再把响应裁到 35 条（`RESULT_SIZE`）。上面的 4 / 31 只是某次随机权重快照，比例会变，两类来源都出现即可。

完整教程（装环境 → 模型演示 → 起服务 → 训练 → 端到端 → 生产缺口）见 **[docs/getting-started/](docs/getting-started/)**，文档总入口是 [docs/README.md](docs/README.md)。

## 系统架构

```
客户端请求 → HOME MIXER（编排层）
              ├─ 查询补全：用户行为序列 + 用户特征（关注列表等）
              ├─ 双路召回：THUNDER（网内帖子） + PHOENIX 检索（网外帖子）
              ├─ 数据补全：帖子文本、作者信息、视频时长等
              ├─ 过滤：去重 / 太旧 / 自己的帖子 / 拉黑作者 / 屏蔽词 / 已看过…
              ├─ 打分：Phoenix 模型预测行为概率 → 加权总分 → 作者多样性 → 网外降权
              ├─ 选择：按总分取 Top-K
              └─ 选择后过滤：内容安全审核、对话去重
            → 排序后的 Feed 响应
```

（更详细的架构图见英文版 [README.md](README.md)。）

## 组件

### Home Mixer

**位置：** [`home-mixer/`](home-mixer/)

组装"为你推荐"Feed 的编排层，基于 `CandidatePipeline` 框架，包含以下阶段：

| 阶段 | 描述 |
|------|------|
| 查询补全 (Query Hydrators) | 获取用户上下文（行为历史、关注列表） |
| 来源 (Sources) | 从 Thunder 和 Phoenix 检索候选 |
| 补全 (Hydrators) | 为候选补充附加数据 |
| 过滤 (Filters) | 移除不符合条件的候选 |
| 打分 (Scorers) | 预测互动概率并计算最终得分 |
| 选择 (Selector) | 按得分排序并选择前 K 个 |
| 选择后过滤 | 最终的可见性和去重检查 |
| 副作用 (Side Effects) | 缓存请求信息以供未来使用 |

服务器对外暴露 `ScoredPostsService`（排序帖子）和 `ForYouFeedService`（最终 Feed）。上游依赖（用户资料、帖子内容、行为日志、内容安全）通过 `home-mixer/clients/` 下的 trait 抽象——当前是带演示模式（`HOME_MIXER_MODE=demo`；`HOME_MIXER_DEMO=1` 是旧别名）的桩实现，设计上就是留给你替换为自己平台服务的。

### Thunder

**位置：** [`thunder/`](thunder/)

内存态帖子存储与实时摄入管道，跟踪所有用户的最新帖子：

- 从 Kafka 消费帖子创建/删除事件（也可以用 `--demo-seed-posts N` 生成演示数据启动，无需 Kafka）
- 为每个用户维护原创帖、回复/转发、视频帖三条时间线
- 为请求用户提供其关注账号的"网内"候选帖子
- 自动清理超过保留期的旧帖子

### Phoenix

**位置：** [`phoenix/`](phoenix/)

机器学习组件（Python ≥ 3.11 / JAX），两大功能：

1. **召回（双塔模型）**：用户塔把用户编码成向量，物品塔把帖子编码成向量，点积相似度取 Top-K，从全网发现相关内容。
2. **精排（候选隔离 Transformer）**：输入用户行为历史和候选帖子，输出每条帖子上各种行为（点赞、回复、转发、举报等）的概率；特殊的注意力掩码保证候选之间互不影响。

Phoenix 自带训练脚本（`phoenix/scripts/train_*.py`）、HTTP 服务和 gRPC 网关（`phoenix/scripts/run_grpc_gateway.py`，实现 home-mixer 消费的 `proto/definitions/phoenix_recsys.proto` 契约）。详见 [`phoenix/README.md`](phoenix/README.md)。

### Candidate Pipeline

**位置：** [`candidate-pipeline/`](candidate-pipeline/)

构建推荐流水线的可复用框架，定义了 `Source` / `Hydrator` / `Filter` / `Scorer` / `Selector` / `SideEffect` 六类 trait，尽可能并行执行，并带有可配置的容错和日志。

## 打分与排序

Phoenix 模型预测多种互动行为的概率，**RankingScorer** 把它们合成最终得分（内部按序做加权、作者多样性、网外降权）：

```
最终得分 = Σ (权重_i × P(行为_i))
```

正向行为（点赞、转发、分享）为正权重，负向行为（拉黑、静音、举报）为负权重。权重定义在 [`home-mixer/params/`](home-mixer/params/)——它们是对齐上游的开源默认值，不是 X 线上实时参数。

## 过滤

**打分前过滤器：**

| 过滤器 | 用途 |
|--------|------|
| `DropDuplicatesFilter` | 移除重复的帖子 ID |
| `CoreDataHydrationFilter` | 移除未能补全核心元数据的帖子 |
| `FirstStageEligibleFilter` | 移除业务一级判定为不可推荐的帖子（已删除 / 未公开 / 审核未过） |
| `AgeFilter` | 移除超过时限的旧帖子（读 `created_at_ms`，缺失时回退 ObjectId 时间戳） |
| `SelfTweetFilter` | 移除用户自己的帖子 |
| `PreviouslySeenPostsFilter` | 移除已经看过的帖子 |
| `PreviouslySeenPostsBackupFilter` | 请求只有曝光 ID、没有 seen_ids 时的备份去重 |
| `PreviouslyServedPostsFilter` | 移除本会话已投递过的帖子 |
| `ViewerMutedKeywordFilter` | 移除主文或引用文命中 viewer 屏蔽词的帖子 |
| `AuthorSocialgraphFilter` | 移除被拉黑/静音作者的帖子 |
| `VideoFilter` | 请求带 `exclude_videos` 时去掉视频帖 |
| `TopicIdsFilter` / `NewUserTopicIdsFilter` | 话题请求只保留对题的帖 |

**选择后过滤器：**

| 过滤器 | 用途 |
|--------|------|
| `VFFilter` | 移除已删除/垃圾/暴力等帖子；未能验证可见性的帖子默认也移除（`HOME_MIXER_VF_FAILURE_POLICY=fail_closed`） |
| `DedupConversationFilter` | 对同一对话线程的多个分支去重 |

引用 / 转推 / 订阅专用组件（`RetweetDeduplicationFilter`、`IneligibleSubscriptionFilter`、`AncillaryVFFilter`、`QuoteHydrator`、`SubscriptionHydrator`）已因目标产品没有这些概念而删除，对应共享字段保留为空。

## 关键设计决策

1. **无人工特征**：完全依赖 Transformer 从用户行为序列中学习相关性，不做人工特征工程。
2. **排序阶段候选隔离**：候选之间不可互相注意，保证每条帖子的分数与同批其他帖子无关，分数一致、可缓存。
3. **基于哈希的嵌入**：召回和精排都用多个哈希函数做嵌入查表，避免为海量 ID 建巨型嵌入表。
4. **多行为预测**：不预测单一"相关性"分，而是预测多种行为各自的概率。
5. **可组合的流水线架构**：执行、监控与业务逻辑分离，新增召回源/过滤器/打分器只需实现对应 trait。

## 文档

- **[docs/bootstrap/](docs/bootstrap/)** —— 分阶段启动手册：环境、编译、模型、端到端、真实数据、能力屏蔽与生产验收
- **[docs/getting-started/](docs/getting-started/)** —— 最短上手路径：装环境 → 模型演示 → 起服务 → 训练 → 端到端
- **[docs/README.md](docs/README.md)** —— 文档总入口，含各模块深入解读
- **[README.md](README.md)** —— 英文版（含完整架构图）

## 许可证

本项目采用 Apache License 2.0 许可证，详见 [LICENSE](LICENSE)。
