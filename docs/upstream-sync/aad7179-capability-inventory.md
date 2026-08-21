# 提交 aad7179 能力清点与 U0–U3 分类（2026-08-19 上游快照）

> 文档状态：已按 §4 落地，结果见 [`../update/20260819.md`](../update/20260819.md)
> 上游提交：`aad7179773944e17eb8798bbbf0231d6cd6c1ffc`（2026-08-19）
> 上游父提交（当前已吸收锚点）：`11a71f87d6a7fc4c1e8159dad8f3c5ff90a0f7ed`
> 本地目标分支：`feature/migrate-20260515`
> 迁移规则：[`upstream-first-maintenance.md`](./upstream-first-maintenance.md)

## 1. 提交概览

79 个文件，+3303/-651。四组主题：

1. **Phoenix 引擎 SID 链路补全**（Rust）：history 侧 semantic IDs 从 proto 与 Arrow 双路径真正写入输入缓冲（此前恒零填充），提取公共 `stamp_semantic_ids`，加覆盖率指标。
2. **引擎服务合同演进**：`NextActionDistribution.rewardOutputs`（reward 模型输出）、`ActionName` 重编号（搜索相关 161/162 → 105/106，占位符换成 `P_OPEN_LINK_P10..P90` 分位头）、`ScoreInfo.rewardRerankSlotProb`、`SlateContext` SID 字段；checkpoint 存储接入 xai-o2 后端。
3. **xrex 训练/推理升级**：Muon 优化器（新）、DPA 商品键哈希与嵌入表、模型指标分位桶、model_runner 热切换重写、BoolFeature 公开排序（stale 前移为 0）、stale-post 配置沿 serving 链路原生转发（与本地 `43616cf` 系列收敛）。
4. **home-mixer 文本过滤**：静音关键词过滤器改名 viewer 作用域 + Following 变体（追加祖先文本匹配）、引用帖文本独立补水器、会话间隔祖先补水器改进。

## 2. 分支对照事实

| 事实 | 证据/影响 |
|---|---|
| 79 个文件中 48 个本地与上游父提交**逐字节一致**（xrex 训练/推理/CUDA/cutedsl 主体、引擎 proto、sid_client.rs、engine util.rs 等） | 逐文件 hash 比对；这些可整体采用 |
| 本地分叉仅 5 个 phoenix 文件，且分叉内容都是已记录的 c65aa17 适配（server_factory、stale-post 校验、SAFETY_BIT 集中化） | diff 量化：util.rs 108 行、python.rs 29、feature_config.py 18、model_runner.py 12、sid_retrieval_runner.py 6 |
| `checkpoint_store.rs` 新增 `XaiO2Store` 依赖未公开 crate `xai_o2`（上游 Cargo.toml 亦无此依赖声明） | grep 全仓无 `xai_o2`；上游发布快照自身不可编译的部分，按既定规则不导入 |
| 上游 `BoolFeature` 公开排序为 `stale=0, followed=1, following=2`，与本地 c65aa17 临时钉住的 `followed=0, following=1, stale=2` **冲突**；`STALE_POST_14D_TTL_SEC` 1_209_600 → 1_213_200 | `feature_config.py` diff；Rust 侧常量由 `build.rs` 从该 py 文件生成，自动跟随 |
| 上游已在 serving 构造点原生转发 `enable_stale_post`（读 `model_config.feature_prep`），与本地 `server_factory` 注入语义等价 | model_runner.py diff；本地工厂继续独占构造入口（AST 测试锁定 4 个调用点） |
| `mm_embedding_client.rs` 上游**恢复**了 b089ce6 移除的 `MM_EMBEDDING_TTL_SECS` 环境变量覆盖 | 上游 flip-flop；b089ce6 的 B1 落地（`2ffff90`）将被本提交取代 |
| 本地 `CoreDataCandidateHydrator` 已从 TES core-data 填充 `quoted_tweet_text`（测试 `core_data_candidate_hydrator.rs:132`） | 上游 `QuotedPostTextHydrator` 属"本地已等价覆盖" |
| 本地无 `ancestor_texts` 字段、无 `conversation_gap_ancestor_hydrator`、`quoted_tweet_text` 为 `String`（上游改为 `Option<String>`） | 候选模型 diff |
| 本地 `MutedKeywordFilter` 语义已等价上游 `ViewerMutedKeywordFilter`（正文+引用文本匹配，`post_text` 本地重写版） | 两文件对照 |

## 3. 能力清单与分类

### P. phoenix 引擎（Rust + 合同）

| 编号 | 能力 | 分类 | 处置 |
|---|---|---|---|
| P1 | 引擎 proto：`RewardOutputs`、`ActionName` 重编号与 `P_OPEN_LINK_P10..P90`、`ScoreInfo.rewardRerankSlotProb`、`SlateContext.sid*`；python 侧 proto 副本同步 + `hatch_build` 缺文件跳过的 `XAI_PROTO_SKIP_MISSING` 开关 | **U0** | 整体采用（两 proto 副本 + hatch_build 本地均与父提交一致）。注意 `CLIENT_TWEET_RELEVANT_TO_SEARCH` 161/162 → 105/106 是**破坏性重编号**：本地网关 proto（`proto/definitions/recsys.proto`）是独立合同不受影响；xrex 侧同批采用保持一致 |
| P2 | `sid_client::fill_semantic_ids` 公开化 + 单测；engine util.rs history semantic_ids 双路径写入 + `record_sid_coverage`；common util.rs 提取 `stamp_semantic_ids`、history 写入、列存路径放宽 arity 检查 | **U0** | 采用。common util.rs 与 python.rs 走三方合并，保留本地 SAFETY_BIT 集中化与 stale 校验。引擎 SID 链路补全后，b089ce6 A5（home-mixer slate SID 统计）的数据源前提部分就位，但 A5 仍依附 SlateContext 决策，维持 U3 |
| P3 | checkpoint_store：`XaiO2Store` 后端、`Store` 枚举、`get_store`、`XAI_RECSYS_S3_BACKEND` 开关 | **U3** | 依赖未公开 `xai_o2` crate。重入条件：crate 公开或以 U1 适配器替代 |
| P4 | checkpoint_store/storage_util 可移植修正：O2 endpoint 缺失时显式报错（原为假默认值 `o2.example.invalid`）、multipart part 16→32MB、`env_usize` 容错解析 | **U0** | 采用（剥离 P3 后的子集）；不引入 `get_store` 调用切换 |
| P5 | `mm_embedding_client` 恢复 env TTL 覆盖 | **U0**（跟随上游终态） | 采用并取代 `2ffff90`；在 20260819 记录中说明 flip-flop |
| P6 | 引擎 Cargo.toml dev-deps（rcgen/serial_test/tempfile）+ phoenix/Cargo.toml 对应声明 | **U0** | 采用；仅为上游新增测试服务，合入后按需保留 |

### X. xrex 训练/推理（Python）

| 编号 | 能力 | 分类 | 处置 |
|---|---|---|---|
| X1 | Muon 优化器（新 240 行：Newton-Schulz 正交化、qkv split-fused、consistent RMS）+ `dense_optim.py` 配置 + rowwise_adagrad 更新 + trainer/xrecsys 配置接线 | **U0** | 整体采用（全部 SAME 文件 + 两个新文件） |
| X2 | `feature_config.py`：BoolFeature 重排（stale=0/followed=1/following=2）+ DPA 键特征（11/12）+ `OPTIONAL_COLUMNS` + TTL 值更新 | **U0**（跟随上游公开合同） | 采用并同步更新 `test_feature_config_schema.py` 的本地钉值；c65aa17 的"本地钉住顺序"记录由本提交取代（见 20260819 记录）。Rust 侧 build.rs 自动跟随 |
| X3 | `recsys_batch.py`：空值填零（bool 列填 False）、DPA 双哈希写入、`zero_stale_post_14d_candidate_counts` 开关 | **U0** | 整体采用（SAME） |
| X4 | `model_runner.py` 367 行重写（热切换周期、服务重载）+ `sid_retrieval_runner.py` 更新 | **U0**（语义合并） | 应用上游改动但保留本地 `create_recsys_server` 构造点（不带上游新增的 `num_post_bool_features`/`enable_stale_post` kwargs——工厂已注入）；合并后 AST 测试的 4 个工厂调用点保持不变 |
| X5 | 模型层：DPA 商品嵌入表与输入注入、指标分位桶统计、attention/feature_prep 更新；数据层：kafkaloader +142、retrieval_dataset 等 | **U0** | 整体采用（SAME） |
| X6 | configs：xrecsys muon 默认配置、stale-post 沿 gen-recs/sid/two-tower 配置转发 | **U0** | 整体采用（SAME） |

### H. home-mixer

| 编号 | 能力 | 分类 | 处置 |
|---|---|---|---|
| H1 | `muted_keyword_filter` → `viewer_muted_keyword_filter` 改名（结构体同步改名） | **U0**（命名对齐） | 采用：本地实现语义已等价，仅做文件/类型改名与装配点更新 |
| H2 | `FollowingViewerMutedKeywordFilter`（追加 `ancestor_texts` 匹配） | **U3** | 依附会话间隔线路（`ancestor_texts` 字段 + `conversation_gap_ancestor_hydrator` 本地均不存在）；数据源就位前退化为 H1 |
| H3 | `QuotedPostTextHydrator`（TES 拉取引用帖文本） | **已覆盖** | 本地 `CoreDataCandidateHydrator` 已从 TES core-data 填充 `quoted_tweet_text`（测试锁定）；不重复引入 |
| H4 | `conversation_gap_ancestor_hydrator` 改进（`ancestor_texts` 填充 + 扩展失败时回落原祖先列表） | **U3** | 本地无该 hydrator（会话间隔线路整体未引入）；失败回落语义随线路重入一并采用 |
| H5 | `candidate.rs`：`ancestor_texts` 字段 + `quoted_tweet_text` 改 `Option<String>` | **U3** | 依附 H2/H4 线路；Option 化有本地 ripple（filters/hydrators/query_builder），随线路一起做 |

### G. grox（U3，P6-B deferred）

ptos 分类器扩展（`SafetyPtosPolicyClassifier` 接入 `prior_nsfw` 先验，新文件 +34）、交叉验证任务口径、policy/safemodel 任务调整。全部走 P6-B 重入条件，不落地。

### V. visibility-filtering（U3，既定不采用）

`dark_traffic_setup.rs`（新，109 行：dark traffic 对比装配）、gizmoduck_client/tes_hydrator/main 接线。本地无该模块，仅记录存在性。

## 4. 落地批次

1. `phoenix: adopt aad7179 engine contract and history SID wiring`（P1+P2+P4+P5+P6；checkpoint_store/storage_util 仅可移植子集）
2. `phoenix: adopt aad7179 xrex training and inference updates`（X1–X6，含 schema 测试更新）
3. `home-mixer: rename muted keyword filter to viewer-scoped`（H1）
4. `docs: record aad7179 adoption outcome`（20260819 记录 + 锚点前移 + 20260817 B1 条目补注 flip-flop）

## 5. 验证

- phoenix workspace：`cargo test -p xai-recsys -p xai-recsys-engine -p xai-recsys-proto`（环境允许范围内；CUDA/RDMA 相关 crate 仅编译检查）。
- `cd phoenix && uv run pytest`（xrex + 合同测试全量）。
- 根 workspace：`cargo test --workspace`（H1 改名）。
- `./scripts/run_demo.sh` 端到端不回归。
