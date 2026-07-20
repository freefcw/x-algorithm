# X 推荐算法跨平台迁移深度分析与执行指南

状态：`historical`

本指南旨在深入分析如何将 X (原 Twitter) 开源的推荐算法系统 (`x-algorithm`) 移植并应用到私有的社交平台中。我们将全面评估让该项目“跑起来”所需的前置准备、依赖替换方案、数据流转需求以及功能的必要性。

注意：本文保留早期迁移规划视角。当前代码事实请优先看 [../README.md](../../README.md) 和各模块目录。

---

## 1. 核心架构拆解与运行原理

X 的推荐系统主要分为四大组件：
*   **Home Mixer (首页混排，主服务)**：基于 Rust gRPC 的后端编排层。负责获取用户特征，向 Thunder 和 Phoenix 发起召回请求，对返回的数据进行过滤、打分聚合，并返回最终 Feed 流。
*   **Thunder (实时内存缓存，网络内召回)**：基于 Rust。消费 Kafka 的推特创建/删除事件流，维护所有用户发布的最新内容，提供亚毫秒级别的“在网 (关注者)”内容查询。
*   **Phoenix (算法模型，精排与全局召回)**：基于 Python `JAX/Haiku`。提供双塔召回 (获取未关注的全局相关推文) 和 精排 (Grok Transformer 预估点赞、回复、转发等多目标概率)。
*   **Candidate Pipeline (候选流框架)**：Rust crate。负责标准化流水线 (并行化 Source 召回、Hydrator 补全、Filter 过滤、Scorer 打分)。

---

## 2. 让项目“跑起来”所需的准备工作

要让系统跑起来，最大的挑战在于 **X 的开源代码脱敏了内部大量的数据结构定义和中间件设施**，你需要准备以下基础设施和适配：

### 2.1 依赖替换与补齐 (Rust 后端)
开源代码中包含了大量的 `xai_*` 私有依赖，这些在外部实际上无法获取，必须进行 mock 或替换：

| 缺失的私有依赖 | 原始用途 | 你的平台可用替换方案 | 必须性 |
| :--- | :--- | :--- | :--- |
| `xai_thunder_proto` / `xai_home_mixer_proto` | Protobuf 协议定义 | 需要自己重新手写 `.proto` 定义，使用 `prost` 和 `tonic-build` 重新生成 Rust 代码 | **必须** |
| `xai_kafka` | 消费推文创建/删除/点赞等实时事件 | 替换为标准的开源 `rdkafka` (rust-rdkafka)，适配你平台的事件流 | **必须** |
| `xai_strato` | 分布式缓存读取工具 (获取关注列表、特征) | 替换为 `Redis` (使用 `redis-rs` 或 `moka` 做本地缓存) | **必须** |
| `xai_http_server` | 封装过的 HTTP/gRPC 服务端 | 用开源的 `axum` (HTTP) 和 `tonic` (gRPC) 从头编写 Server 入口代码 | **必须** |
| `xai_wily` / `xai_profiling` | 链路追踪层与监控报警 | 替换为 `tracing`, `opentelemetry` 和 `prometheus` | 非必须，早期可阉割 |
| `xai_visibility_filtering` | 内容安全、血腥黄暴过滤 | 替换为你平台的敏感词包判断逻辑或接入第三方安全审核 API | 非必须，早期可跳过 |

### 2.2 数据格式对接与准备
Phoenix 等模型完全依靠**特征序列化输入**。必须在你的平台上生成类似特征：

1.  **用户互动序列日志 (Action Sequence)**: 在你的社交平台中，必须建设用户埋点系统以记录：`P(like)`, `P(reply)`, `P(repost)`。并能够将用户最近 N 次行为转为模型所需的 Hash 特征序列。
2.  **实时的全量内容聚合**: Thunder 服务器不能只依赖数据库；你需要将你平台的后台服务产生的 "Create Post", "Delete Post" 事件同步推入一个 Kafka Topic 中供 Thunder 消费建立内存索引。
3.  **Phoenix 训练数据**: 项目中只公开了模型架构 (`recsys_model.py`)，**没有提供预训练权重 (Weights)**。你无法直接启动 Phoenix 做推理，必须抽取你平台的历史用户行为数据，按照模型输入格式从头训练模型。

---

## 3. 功能取舍：MVP 阶段可以抛弃什么？

平台初创期资源有限，可以按照以下路径进行功能精简（MVP）：

*   **只保留一部分的 Scorers**：X 预估 10 多种行为 (Dwell停留, Click, Block 等)。初期你只需要让 Phoenix 预估 `P(favorite/like)` 和 `P(reply)`，大幅减小模型训练难度。
*   **移除复杂的 Post-Filters**：诸如连续相似对话过滤 (`DedupConversationFilter`)、计费订阅屏蔽 (`IneligibleSubscriptionFilter`) 等可直接置空或删除，只保留基本的去重和黑名单屏蔽。
*   **统一召回路**：如果你的平台初期内容量不大 (< 10万 DAU)，可以砍掉复杂的 Phoenix 双塔召回，所有的全局推文直接使用简单的 Elasticsearch 热度/规则召回。**只保留 Phoenix 的 Transformer 精排模型**对混合后的候选流进行打分。

---

## 4. 推荐执行与迁移步骤 (Roadmap)

如果你决定开始重构，请按照以下 4 个阶段执行：

### 阶段一：重写通信协议 (Protobuf)
查阅 `home-mixer/main.rs` 和 `thunder/main.rs` 中涉及的数据结构，自行编写 `.proto` 文件：
1. 编写包含 `UserActionSequence`, `PostMetadata`, `UserFeatures` 的消息体。
2. 配置 `build.rs` 引入 `protoc` 生成 Rust 结构体。

### 阶段二：重写 Thunder (内容缓存)
1. 在你的平台搭建 Kafka 并发送测试事件包。
2. 移除雷霆模块关于 `xai_kafka` 的引用，集成 `rdkafka` 订阅你的话题，实时缓存在 Rust 的 DashMap（并发字典）中。
3. 暴露一个 gRPC 接口供外部查询 "User A 关注列表里最近的 100 条帖子"。

### 阶段三：Phoenix 模型训练跑通
1. 设置 Python `JAX/Haiku/uv` 环境 (根据 `phoenix/pyproject.toml`)。
2. 造一批 Fake Data (假的推文 Hash 序列和动作序列) 喂给 `run_ranker.py` 验证能够完成一次 Forward 前向计算。
3. 对接你平台真实数据的数据仓库，提取特征开始训练模型并保存 Checkpoint。

### 阶段四：组装 Home Mixer
1. 彻底移除所有 `xai_` 依赖。
2. 将数据获取操作 `QueryHydrators` 改写为直接向你的微服务或 Redis 发起 gRPC/TCP 请求以获取用户信息和帖子。
3. 编排管道：获取特征 -> 调 Thunder 拿关注帖 -> 调 Phoenix 打分 -> 排序 -> 返回给你的客户端。
