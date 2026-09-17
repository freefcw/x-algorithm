# 推荐链路正式上线就绪度评估（x-algorithm × rec-bff × mrpyq）

> **状态**：`design`（快照型评估，结论随代码演进需复核）
> **日期**：2026-09-17
> **读者**：推荐服务建设负责人、mrpyq feed/webapi 后端、数据平台、运维
> **评估范围**：本仓库（x-algorithm）、业务主仓库 mrpyq（`x-recommend` 分支，`b70e0d54e7`）、数据门面 rec-bff（`recommend/rec-bff`，独立 Go 仓库）
> **事实边界**：本文所有「代码事实」均取自三仓库当日快照并经实际验证：`cargo check --workspace` 通过，`cargo test --workspace` 403+ 用例全部通过；mrpyq `x-recommend` 分支相对 `dev` 仅含一个 ZSet 分页修复提交，推荐相关改造零进度。引用本仓库文件用相对路径，引用外部仓库以仓库名标注其内部路径。文中「建议」部分是评估结论，未经各方确认。
>
> **同日午后更新**：评估产出当日，另一批提交已解决部分发现，正文相应条目已标注：§2.2#6 训练编码错位（`5a504ef`）、§2.3 的 CI/k8s 脚手架（`5a9757b`）、thunder 指标暴露（`960800f`）、按方法调用级指标（`9604dc2`）、Redis Cluster 路由（`b43baae`）；§2.2#2 的静默 fail-open 已有金丝雀测试显性化（`8477de0`），端口迁移本身仍未做。§2.1、§2.2 其余各项、§3 数据接入、§4.1–4.3、§5 全部仍然有效。

---

## 0. 执行摘要

1. **代码本身健康**：workspace 编译、测试全绿；`degraded` 模式（mrpyq 候选 + 规则排序）在配置齐备后今天就能跑。真正的阻碍不是代码质量，而是 **8 项生产合同未闭合**（`production_ready` 模式因此拒绝启动）和**数据链路零接入**（行为事件流、曝光流、帖子语料、训练样本四条线都未开始）。
2. **文档已落后于架构**：x-algorithm 文档仍按「home-mixer 直连 mrpyq」描述多处缺口，实际已演进为 home-mixer → rec-bff → mrpyq 三层；原「皮维度收件箱未落地」「ViewerRelationService 未实现导致空 feed」两项已被 rec-bff 吸收。排期时须按三层架构评估，不要按旧文档。
3. **关键路径是数据，不是模型**：UAS 行为事件流（mrpyq 侧生产者零进度）→ 曝光流 → 帖子语料导出 → 训练样本积累。M1（规则 Feed）不需要等任何模型。
4. **性能最大瓶颈**：Phoenix 网关单进程串行推理（4 线程 + 引擎锁），以及 rec-bff → mrpyq 无缓存时单请求 600 路并发 `GetFeed` 扇出。延迟预算整体偏松（总预算 10 s、Phoenix 预测 5 s），上线前需按 P99 目标重调。
5. **一条硬性上线顺序约束**：mrpyq 把「不看」改成按皮存储（§5.1）与推荐侧 Strato 端口迁移**必须同批上线**，否则屏蔽过滤静默 fail-open（详见 [mrpyq-member-dimension-requirements.md](./implementation/mrpyq-member-dimension-requirements.md) §6.1）。

---

## 1. 三层架构现状与文档漂移

### 1.1 实际架构

```
home-mixer (x-algorithm, Rust)
    │  MRPYQ_RECOMMENDATION_DATA_ADDR（五个适配器共用一个地址）
    ▼
rec-bff (Go, gRPC :9000 / HTTP :8000)
    │  对上实现两份合同：RecommendationDataService + ViewerRelationService
    ▼
mrpyq (Feed / Account 进程)
    FeedService / FollowFeedService / NotSeeService / AccountService
```

### 1.2 文档漂移点（读旧文档时注意）

> **2026-09-17 更新**：本节指出的漂移已在同日的文档对齐中修复——getting-started/06、home-mixer/05 与 06、bootstrap/05、candidate-pipeline/03、home-mixer 手册等均已按三层架构改写。以下保留原始记录，供回溯。

x-algorithm 以下两处描述已过时（原文）：

| 旧文档说法 | 实际现状 |
| --- | --- |
| [getting-started/06](./getting-started/06-从演示到真实系统.md) 与 [home-mixer/06](./home-mixer/06-current-behavior-risks-roadmap.md)：「mrpyq 侧尚未实现 `ViewerRelationService`，调用失败后准入过滤器整批丢弃，非 demo feed 会是空的」 | rec-bff 自己实现了 `GetViewerRelations`（经 `ListNotSee` + `BatchGetMembersByKeys` 翻译成皮 id），rec-bff 部署后该 fail-closed 风险解除 |
| 同上：「mrpyq 侧目前按账号键读取，皮维度对齐尚未落地」（NETWORK 召回） | rec-bff 的 NETWORK 直接读皮维度 `ListMemberFollowInboxV1`，原需求文档 §2 的缺口已闭合 |

反向约束：rec-bff 实现的是**当前合同**（`account_id` 字段位）。x-algorithm 需求文档（`proposed`）要求改成 `viewer_member_id` / `creator_member_id` 收敛 / 截断报错——**两边定版前谁都不能单方面改**，改动清单见 rec-bff README「拆分剧本」。

### 1.3 mrpyq 侧实际进度

`x-recommend` 分支相对 `dev` 仅有一个「修复 ZSet 同分数据跨页重复」提交（利好收件箱分页读取），[mrpyq-member-dimension-requirements.md](./implementation/mrpyq-member-dimension-requirements.md) §5 列的底层改造（not-see 按皮存、not-allow-see 皮级判断、关系表存 member_id）**均未动工**。

---

## 2. x-algorithm 自身阻碍执行的问题

### 2.1 阻断级：`production_ready` 拒绝启动的 8 项合同

`home-mixer/runtime_config.rs:307`：business adapter contracts are not verified（caller identity、TES、UAS event schema/retention、Strato、VF、in-network/fallback、Phoenix metadata、served persist）。这是故意的门，不是缺陷。**「正式运行」的定义性工作就是逐项闭合这 8 项合同**，验收条件见 [home-mixer/06-current-behavior-risks-roadmap.md](./home-mixer/06-current-behavior-risks-roadmap.md) §6。当前可运行的最高模式是 `degraded`。

### 2.2 高风险（不修会出事故或空转）

| # | 问题 | 位置 / 证据 | 后果 |
| --- | --- | --- | --- |
| 1 | **Post-selection 过滤后不回补**：Selector 先留 50，VF / 会话去重后再截 35，删超 15 条响应就少于目标，只发 `result_underfilled` 告警 | `home-mixer/params/config.rs:15-19`；[home-mixer/06](./home-mixer/06-current-behavior-risks-roadmap.md) §4.1 | 上线初期 VF 拦截率高时 feed 变薄；回补需先定义是否重跑 VF / 预算 |
| 2 | **viewer 关系语义脆弱**：`MrpyqStratoClient` 把皮的 id 塞进 `account_id` 字段；mrpyq 若先按 §5.1 改成皮键而 rec-bff / 推荐侧未同步，**静默 fail-open**（名单恒空 → 该拦的全放行，无报错无告警） | `home-mixer/clients/mrpyq_adapters.rs:571`；[mrpyq-member-dimension-requirements.md](./implementation/mrpyq-member-dimension-requirements.md) §6.1 | 三方（mrpyq §5.1 + rec-bff 查询键 + 推荐侧端口迁移）同批上线，或推荐侧先把 Strato 端口退回 Disabled。同日已加金丝雀测试把空名单异常显性化（`8477de0`），但迁移本身未做 |
| 3 | **not-see 名单仍是账号级**：同一账号下所有皮共用一份「不看」；`blocked_by`（不让看）恒空（mrpyq 无反向列表） | rec-bff README「关注与拉黑怎么对应」；mrpyq §5.1/§5.2 未动工 | 换皮登录推荐过滤语义错位；反向拉黑完全不过滤 |
| 4 | **UAS 非法事件静默丢弃**：格式错的消息只记 warn + `invalid` 计数，offset 照常推进 | [uas-event-contract.md](./implementation/uas-event-contract.md) §2.4 | 埋点接错时无显性失败，模型输入悄悄变稀；监控必须盯 `uas_worker_events_total{outcome="invalid"}` |
| 5 | **曝光事件 at-most-once**：超出排空预算或 SIGKILL 会丢 | [getting-started/06](./getting-started/06-从演示到真实系统.md) 已知缺口 | 训练归因数据缺口；需用 `home_mixer_served_events_total{result="ok"}` 与成功响应数对账 |
| 6 | ~~**训练文档行为编码与在线合同错位**~~ **已解决**（同日 `5a504ef`）：`training_data_spec.md` 已改为 proto `ActionName` 编号，与在线合同一致 | [uas-event-contract.md](./implementation/uas-event-contract.md) §8.3 | 无 |

### 2.3 工程缺口（不阻断联调，阻断「正式」）

- ~~无 k8s manifest、无 CI 流水线~~ **部分解决**（同日 `5a9757b`）：`deploy/k8s/` 三份清单脚手架 + GitHub Actions CI（fmt / clippy / test）已入库；镜像与清单尚未在真实环境验证。
- ~~thunder 指标未暴露~~ **已解决**（同日 `960800f`）：thunder 管理端口暴露 `/healthz`、`/readyz`、`/metrics`。
- ~~调用级指标缺失~~ **已解决**（同日 `9604dc2`）：mrpyq / Redis / Phoenix 已有按方法拆分的调用指标；排障盲区解除。
- **`legacy-int-ids` 是默认 feature**：Thunder 整数 proto 无法承载真实 ObjectId，P3 改 string proto 前是演示包袱。
- ~~Redis 客户端只连单一端点~~ **已解决**（同日 `b43baae`）：支持 `HOME_MIXER_REDIS_CLUSTER_URLS` 原生 Cluster 路由（`CLUSTER SLOTS`，key 带 hash tag）。
- **Gizmoduck 是 `DisabledGizmoduckClient`**：响应 `screen_names` 为空，作者昵称 / 粉丝数缺展示层需确认可否接受。
- **followed_user_ids 恒空**：无关注列表合同，关系特征缺一路输入。
- **客户端 seen_ids / bloom filter 协议就绪但无人填写**（`proto/definitions/home_mixer.proto` 的 `seen_ids` / `bloom_filter_entries`）：去重目前全靠服务端 Redis 历史，跨端不同步。

---

## 3. 正式运行需推进的工作（按依赖排序）

### 3.1 工作流 A：数据接入（关键路径，lead time 最长）

| 项 | 内容 | 依赖方 | 状态 |
| --- | --- | --- | --- |
| A1 | **UAS 行为事件流**：mrpyq 建 Kafka topic 并实现生产者。后端可直接采集的只有 action 1（点赞）/ 2（评论）/ 18（举报）；6/9/10/11/14/16/17 依赖客户端埋点，mrpyq 是否有埋点转发入口待确认 | mrpyq + 客户端 | 消费端 `uas-worker` 已就绪并有 Redis 集成测试；**生产者零进度**。交付清单见 [uas-event-contract.md](./implementation/uas-event-contract.md) §6.2（broker、认证、分区数、事件 QPS、retention ≥ 7 天、时钟同步） |
| A2 | **服务端曝光事件 topic**：`ServedCandidatesKafkaSideEffect` 已实现（带 position / served_type / score，幂等键 `request_id`），需确定 topic、保留期、消费方与落表 | 推荐侧 + 数据平台 | 半成品：发得出，没人收。合同见 [served-candidates-event-contract.md](./implementation/served-candidates-event-contract.md) |
| A3 | **帖子语料导出**：全量 feed_id + creator_member_id + 文本 + created_at，供 Phoenix 离线编码建向量索引 | mrpyq | 未开始；网外个性化召回的前提 |
| A4 | **训练样本积累**：served + feedback 按 `request_id` + `post_id` 关联落表，规格 [training_data_spec.md](./training/training_data_spec.md)（**先修 §2.2#6 编码错位**） | 数据平台 | 未开始；没有它就没有第一版真模型 |

关键路径：A1 →（模型可参与）；A2 + A4 →（可训练）；A3 →（网外召回）。A1 / A2 可并行。

### 3.2 工作流 B：mrpyq 侧改造

按 [mrpyq-member-dimension-requirements.md](./implementation/mrpyq-member-dimension-requirements.md) §7 代价表：

1. 低代价接线（皮维度收件箱、creator_member_id）**已由 rec-bff 完成**。
2. 待做：保证所有发帖路径写 `member_id` 非空（空作者帖会被 `CoreDataHydrationFilter` 丢弃，等于悄悄丢候选）。
3. 待做：§5.2 not-allow-see 皮级判断（改一个 `SIsMember`，极低代价）、§5.3 关系表存 member_id（补一个字段）。
4. 中高代价：§5.1 not-see 按皮存储——**必须与推荐侧 Strato 端口迁移同批上线**（§2.2#2）。
5. **性能修复**（rec-bff README 已点名）：FALLBACK 用的 `ListFeedItemsByRecommend` 是客户端级重接口（每帖拉评论 / 点赞 / 审核图），需暴露只回 id + 分数的轻量 RPC（内部 `ListFeedsByRecommendV2` 即此形态）；`BatchGetFeeds` 每个 id 一个 goroutine 无上限，需 errgroup 限额。

### 3.3 工作流 C：基础设施与部署

1. Redis（单端点 / 代理；feed state 与 UAS 可共用或分开）、Kafka 集群与 topic 规划（partition key 建议 `user_id`，分区数由事件 QPS 定）。
2. k8s manifests + CI 流水线 + 两份 Dockerfile 真实构建验证。
3. 监控告警接入：home-mixer `:9090`、uas-worker `:9091`、Phoenix 网关 metrics 端口均有 `/metrics`；重点告警项：`uas_worker_consumer_lag`（不收敛=该加实例）、`invalid` 计数（非零=埋点错）、`result_underfilled`（feed 变薄）、`home_mixer_served_events_total` 对账缺口。
4. `uas-worker` 按分区多实例部署并压测（单实例串行是已知瓶颈，见 §4.4）。

### 3.4 工作流 D：安全与生产验收

1. 调用方身份：mTLS / service identity、viewer 防冒用、审计、密钥轮换——`production_ready` 解锁的硬条件。
2. VF 故障策略确认（默认 `fail_closed` 丢整批，产品是否接受）、灰度发布方案。
3. 客户端配合：自带 `grpc-timeout`、维护 seen_ids / bloom filter、分页协议（[uas-client-event-reporting.md](./implementation/uas-client-event-reporting.md)）。

### 3.5 里程碑

1. **M1 规则 Feed 上线**：`degraded` 模式 + rec-bff + Redis，`RuleFallbackScorer` 排序即可跑——「新鲜度 + 网内 + 互动」的全网骨架，**不等任何模型**。此阶段把 A2 曝光流搭起来开始攒数据。
2. **M2 个性化**：A1 事件流接通 → uas-worker 投影 → Phoenix 网关部署（可先用演示权重验证链路，注意非 demo 下随机权重会被 home-mixer 拒绝，见 §5.3）→ A4 样本足够后训练第一版 checkpoint。
3. **M3 发现型推荐**：A3 语料 + 向量索引替换演示候选池，`production_ready` 8 项合同逐项验收后解除启动拒绝。

---

## 4. 性能评估与优化建议

### 4.1 延迟预算现状：整体偏松，上线前重调

当前参数（`home-mixer/params/config.rs` 及 [home-mixer/06](./home-mixer/06-current-behavior-risks-roadmap.md) §3）：

| 环节 | 当前上限 | 评价 |
| --- | --- | --- |
| 单请求总预算 | 10 s | 兜底值；建议按客户端 P99 ≤ 300–500 ms 目标重定，客户端自带更短 `grpc-timeout` |
| Phoenix 召回 / 预测 | 3 s / 5 s | 演示期值；生产必须压到几百 ms 量级，否则超时即降级到规则排序，模型形同虚设 |
| mrpyq 单次 RPC / 召回跨页总预算 | 500 ms / 1500 ms | rec-bff 已反映「FALLBACK 和 BatchGet 容易顶满」（其 feed timeout 配 1500 ms） |
| TES / UAS / Strato / VF 等 | 500 ms | 合理 |

**核心风险不是单点慢，而是「超时 → 静默降级」链**：Phoenix 超时 → `RuleFallbackScorer`；VF 超时 → fail_closed 丢整批；viewer 关系失败 → 整批丢弃。性能不达标的表现是**空 feed 或无个性化**，而不是显式错误。上线前需满载压测验证降级路径的产出质量。

### 4.2 Phoenix 网关（最大瓶颈）

- `phoenix/services/grpc_gateway.py:997`：`ThreadPoolExecutor(max_workers=4)` + `RankerEngine` 内部 `threading.Lock`——**推理天然串行**，`phoenix_gateway_rpc_in_flight > 1` 即排队；当前靠多副本扩容。
- 检索是全量暴力点积 + Top-K：十万级候选 CPU 毫秒级可接受；语料到百万级必须换 FAISS / Milvus（已在 [operations/data_operations_runbook.md](./operations/data_operations_runbook.md) 规划，未落地）。
- 优化顺序：① 多副本 + LB 水平扩（零代码）；② 预测侧动态 batching（并发请求候选合并为一次前向，JAX 上收益大）；③ 语料上量后 ANN 索引；④ 长期迁移 `phoenix/xrex/` 生产引擎（Linux + CUDA，部署环境与本地不同，是独立决策）。
- Embedding 表在网关进程内（`EmbeddingTables`）：演示规格 100k×128；真实规模增长后内存与查表成本需重估。`model_version` 按参数内容哈希做索引 / 引擎版本一致性校验是已有的好设计，扩容时保留。

### 4.3 数据扇出路径（rec-bff → mrpyq）

- 无缓存时单次推荐请求 = **600 路并发 `GetFeed`**（400 网内 + 200 兜底，home-mixer 分 3 个 200 批并发），任一批非 NotFound 错误整批候选被丢。已有两层缓解：rec-bff `content_ttl: 10s` / `fallback_page_ttl: 30s`（`configs/config.yaml`）与 home-mixer 侧 2 s 内容缓存（`home-mixer/clients/mrpyq_adapters.rs:44`，上限 2 万条）。**根治靠工作流 B#5**（mrpyq 批量接口 + errgroup 限额）。
- FALLBACK 兜底池是**所有 viewer 共享**的，`fallback_page_ttl` 命中率会很高，轻量 RPC 优化收益直接；同时注意首页全站置顶帖会被 `ListFeedItemsByRecommend` 插进列表，rec-bff 目前不过滤。
- `GetViewerRelations` 每请求 3 跳（GetMember → ListNotSee 翻页至多 16 页 → BatchGetMembersByKeys）：多数用户 1 页可接受；重度用户（接近 3000 条不看）到 16 页 × 200——建议 rec-bff 按 viewer 加短缓存（其 README 已建议），而非放宽 500 ms 预算。

### 4.4 home-mixer 与投影链路

- TopK 50 → 截 35 两段式 + 不回补（§2.2#1）：被丢的候选已付过全部水合与打分成本，高拦截率下既缺量又浪费。
- `uas-worker` 单实例串行、每事件一次 Redis 往返：事件 QPS 上来后按分区加实例（ZSET 写入幂等保证多实例安全），以 `uas_worker_consumer_lag` 收敛为扩容信号。
- Feed state 每请求加载一份快照（500 条下发历史 + 50 时间戳）：Redis 需就近部署，往返次数直接影响 P99。
- ~~观测盲区~~ **已解决**（同日 `9604dc2`）：按方法拆分的上游调用指标已补齐，可先看指标再谈优化。

### 4.5 暂不需要优化

- thunder（非 demo 主链路不依赖；若最终不用实时网内缓存可整体不上，省一套 Kafka + 内存索引运维）。
- vm-ranker / MoE / Ads / Grox：默认关闭，均有开关，不影响主线。

---

## 5. Phoenix 准备工作清单

### 5.1 数据准备（全部前置，无捷径）

1. **行为序列数据（在线）**：UAS 事件流（工作流 A1）→ Redis ZSET → 网关取最近 32 条聚合记录。字段规格已完全文档化：[phoenix/docs/真实数据接入指引.md](../phoenix/docs/真实数据接入指引.md) 第二章（`user_hashes [B,2]`、`history_actions [B,32,19]`、MD5 多哈希、0 为 padding 等）。三个 ID 必须同在皮空间，混入 account_id 等于给模型喂噪音。
2. **帖子语料（离线）**：A3 导出 → `phoenix/scripts/build_retrieval_index.py` 用候选塔编码 → 索引文件 → 网关 `corpus_path` 加载 + `CorpusRefresher` 按 mtime 热替换。索引必须与网关同 `model_version` 的 checkpoint 编码，版本错配拒载（保护性设计，不是障碍）。
3. **训练样本（离线）**：A2 + A4 归因 → `phoenix/scripts/build_training_inputs.py` → `train_ranker.py` / `train_retrieval.py`。**先修 §2.2#6 的行为编码错位**，否则样本作废。
4. **行为集合收缩决策**：第一版模型 head 只有服务端可采的 1（点赞，权重 0.5）/ 2（评论，5.0）/ 18（举报，-234.0）；客户端埋点上线后按 [phoenix-training-data-decisions.md](./implementation/phoenix-training-data-decisions.md) §1.4 加回其余 head。**不阻塞上线，但要写进排期**。

### 5.2 训练与产物管理

- 现有 `phoenix/checkpoints/recommendation-v1/v2`（step-100~1000）是演示训练产物；真实训练要建立「日志 → 样本 → checkpoint → 评估 → 发版」例行任务（[phoenix/docs/训练指引.md](../phoenix/docs/训练指引.md) + [operations/data_operations_runbook.md](./operations/data_operations_runbook.md) 的索引切版流程）。
- 离线评估用 `phoenix/scripts/eval_ranker.py`；上线验收要求 **offline 与 gRPC 输出一致性**验证（[home-mixer/06](./home-mixer/06-current-behavior-risks-roadmap.md) §6.4 明确要求，不能只拿 LFS pointer 或 Demo fixture 验收）。

### 5.3 Serving 部署决策

1. checkpoint 接线 `model_registry`；`random-weights` 元数据在非 demo 下被 home-mixer 拒绝——**上线前必须换成真训练产物**。
2. GPU / CPU 决策：当前 JAX 网关 CPU 可跑演示规格（2 层 transformer、emb 128）；真实规模 + 低延迟目标下要么多副本 CPU 要么上 GPU；`xrex/` 生产引擎是长期选项。
3. 容量规划：按 QPS × 每请求 50 候选（网关按 32 分块 ≈ 2 次前向）估算，用 `phoenix_gateway_rpc_in_flight` 排队深度定副本数。
4. Fallback 演练：网关挂 / 慢 / 返回随机权重时 home-mixer 自动退化为规则排序——行为现在就有，验收时用故障注入确认。

### 5.4 明确不做的

- MoE 召回、StableHLO bundle、加密 checkpoint：上游同步评估已结论延期 / 不适用（[update/20260907.md](./update/20260907.md)）。
- 19 个行为 head 全量启用：跟 §5.1#4 收缩决策走。
- 精排替代方案调研（`phoenix/docs/精排模型替代方案指引.md`）：数据闭环跑通前是纸上谈兵。

---

## 6. 附录：上线检查清单（可勾选）

**M1（规则 Feed）**

- [ ] rec-bff 部署并对 mrpyq Feed / Account 连通（stable 入口，k8s Service / VIP）
- [ ] home-mixer `HOME_MIXER_MODE=degraded` + `MRPYQ_RECOMMENDATION_DATA_ADDR` + `HOME_MIXER_REDIS_URL` 启动
- [ ] Redis 单端点 / 代理确认；feed state key 前缀、TTL 按配置文档设定
- [ ] 监控接入：home-mixer `:9090`（healthz / readyz / metrics）
- [ ] 曝光事件 topic 确定并落表（A2）
- [ ] `result_underfilled` 告警验证（VF 拦截率对 feed 厚度的影响）

**M2（个性化）**

- [ ] mrpyq UAS 生产者上线（A1）：格式自检 → Kafka 试跑 → `invalid` = 0 → 幂等回归（[uas-event-contract.md](./implementation/uas-event-contract.md) §7）
- [ ] uas-worker 多实例 + `consumer_lag` 收敛验证
- [ ] Phoenix 网关部署 + 真实 checkpoint + offline/gRPC 一致性验收
- [ ] Strato 端口迁移与 mrpyq §5.1 **同批上线**（§2.2#2）；发布前后各跑一次金丝雀 `cargo test -p home-mixer --test viewer_relation_canary -- --ignored`（需预置拉黑关系的测试皮，见 [mrpyq-member-dimension-requirements.md](./implementation/mrpyq-member-dimension-requirements.md) §6.2）
- [ ] 训练样本管线跑通第一版 checkpoint（A4，编码口径按修订后的 [training_data_spec.md](./training/training_data_spec.md)：日志层一律 proto `ActionName` 编号，ID 为 24-hex 字符串）

**M3（发现型推荐 + 生产验收）**

- [ ] 全量语料导出（A3）+ 向量索引构建 + 切版演练
- [ ] 调用方身份 / mTLS / viewer 绑定 / 审计（§3.4）
- [ ] VF 故障策略产品确认；灰度发布方案
- [ ] `production_ready` 8 项合同逐项验收，解除启动拒绝
- [ ] 满载压测：降级路径产出质量 + P99 达标
