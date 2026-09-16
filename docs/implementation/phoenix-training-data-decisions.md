# Phoenix 训练数据决策记录（2026-09-16）

> **状态**：`decided`（决策已定，派生的代码改动见 §7，未全部落地）
> **读者**：推荐服务开发、埋点侧、数据 / 训练侧
> **配套文档**：行为事件流 [uas-event-contract.md](./uas-event-contract.md)；曝光事件流 [served-candidates-event-contract.md](./served-candidates-event-contract.md)；训练样本格式 [training/training_data_spec.md](../training/training_data_spec.md)
> **为什么单独一份**：这五项决策横跨埋点、home-mixer 权重合同、Phoenix 训练配方和数据侧 join 任务，任何一方单独改都会造成 train/serve 或 gateway/home-mixer 不一致。这里是唯一真源，改动先改这里再改代码。

---

## 1. 行为字典与 head 集合

**决策**：方案 A——只保留埋点能采到的行为作为模型 head；其余 head 精排权重置 0、从 home-mixer 必需列表移除；将来采到后加回并重训。

### 1.1 第一版（v1）head 集合

按 proto `ActionName` 编号：

- **v1 启用（后端可直接采集）**：`1` 点赞、`2` 评论、`18` 举报。
- **后续补上（需要客户端埋点补充，v1 不启用）**：`14` 关注作者、`16` 拉黑作者（后端无法单独确认来自哪条帖子，需客户端带 `tweet_id` 上报）、`5` 点开大图、`6` 点击详情、`8` 视频有效观看、`9` 分享、`10` 私信分享、`11` 复制链接、`17` 静音作者。
- **不采集（产品没有可用的采集途径）**：`15` 不感兴趣。权重置 0、从必需列表移除；将来产品提供该入口时按 §1.4 加回。
- **权重本来就是 0、无需决策**：`7` 点作者头像、`12` 停留（离散）、`3` 转发、`4` 引用、`13` 引用点击。
- **连续 head `dwell_time`**：需要 `dwell_seconds` 字段，UAS 事件当前不携带；v1 不启用（`CONT_DWELL_TIME_WEIGHT = 0.004`，输出恒 0 不影响排序）。

### 1.2 v1 排序公式

只剩三个非零权重 head：

```text
score = 0.5·P(点赞) + 5.0·P(评论) − 234.0·P(举报)
```

举报的权重量级是点赞的近 500 倍，而举报是极稀疏事件：第一版模型对它的估计会很不稳定，一个略高的 P(举报) 就足以把帖子压到底。上线前要看举报 head 的校准（预测均值 vs 实际举报率），必要时在本文里下调 `REPORT_WEIGHT` 或对该 head 做截断；`RuleFallbackScorer` 仍是兜底。权重改动只在这份决策记录里改，不要各处散改。

### 1.3 三处必须一致的配置（派生值）

- 训练：`scripts/train_ranker.py --observed-actions favorite,reply,report`。metadata 会写出 `supported_action_enums = [1, 2, 18]`，网关原样广播。
- home-mixer：`clients/phoenix_prediction_client.rs::REQUIRED_SUPPORTED_ACTIONS` 改为 `[1, 2, 18]`；`params/param.rs` 中 `CLICK_WEIGHT`、`SHARE_WEIGHT`、`SHARE_VIA_DM_WEIGHT`、`SHARE_VIA_COPY_LINK_WEIGHT`、`PHOTO_EXPAND_WEIGHT`、`VQV_WEIGHT`、`FOLLOW_AUTHOR_WEIGHT`、`BLOCK_AUTHOR_WEIGHT`、`MUTE_AUTHOR_WEIGHT`、`NOT_INTERESTED_WEIGHT` 置 `0.0`，注释保留原值以便恢复。
- Phoenix 网关：`services/model_contract.py::NONZERO_WEIGHT_ACTION_ENUMS` 改为 `(1, 2, 18)`；`grpc_gateway.create_servicers` 无 metadata 时的默认支持集合改为引用该常量。

### 1.4 加回一个 head 的流程

1. 埋点上线该行为，按 `uas-event-contract.md` 发到 UAS topic，确认 `uas-worker` 统计里 `invalid = 0`。
2. 累计至少一个完整归因窗口周期以上的数据（建议 ≥ 14 天）。
3. 重训：`--observed-actions` 加上该行为，产出新 checkpoint。
4. **同一个 PR** 里改 §1.3 三处：恢复权重、扩必需列表、扩 `NONZERO_WEIGHT_ACTION_ENUMS`，并用 `PHOENIX_EXPECTED_MODEL_VERSION` 钉住新 checkpoint 灰度。
5. 更新本文 §1.1。

**理由**：没有标签的 head 训出来是"永远接近 0"的噪音；举报 −234、静音 −58.8 这种权重下，一个没有数据支撑的负向 head 抖一下就能主导排序。要求网关支持采不到的 head 只会逼训练侧用全零标签凑数。

---

## 2. 归因窗口

**决策**：作为配置项，默认 **30 分钟**，允许调到 60 分钟。

- 定义：曝光事件 `request_time_ms = T`，同一皮对同一帖子在 `[T, T + W]` 内的行为算这次曝光的标签；窗口内没有任何行为的曝光是负样本。
- 同一帖子在窗口内被多次曝光时，行为归到**最近一次**曝光。
- 配置位置：离线归因 join 任务的参数 `--attribution-window-minutes`（数据侧任务待建，`data_preprocessor.py` 的曝光负样本扩展同名参数）。改变窗口必须重跑受影响日期的样本，不能新旧窗口的样本混训。
- 样本定型延迟 = W：30 分钟对每日训练可忽略。

**理由**：训练规格文档示例 SQL 用 30 分钟；短于 5 分钟会把滞后的点赞 / 评论 / 关注判成负样本，长于 1 小时开始把搜索、作者主页等其他入口的行为算到推荐曝光头上。60 分钟是在正样本率和跨入口污染之间可接受的另一档，作为可调参数留给数据侧按实际分布决定。

---

## 3. `product_surface`

**决策**：保留字段，埋点按入口填写，沿用 `uas-event-contract.md` 已定的码表。

- 码表（proto / UAS 事件 / 训练三方一致）：`0` 首页推荐、`1` 关注流、`2` 搜索、`3` 话题；`4..=15` 预留。需要新入口（如作者主页）时在预留区分配新编号，**先改 `uas-event-contract.md` 再改埋点**，不要复用已有编号。
- 消费端：省略时当 `0`（`uas-worker`），历史序列聚合保留同帖最早一条的 `product_surface`；训练侧照事件值使用，`SURFACE_VOCAB = 16` 覆盖全部编号。
- 已知取舍：`0` 同时表示"首页推荐"和"未填"。埋点侧因此必须显式填写，不能靠省略；数据侧如需区分可用事件是否含该字段判断。
- 曝光事件（served）当前不带 `product_surface`：home-mixer 只服务推荐流，候选侧 `candidate_product_surface` 恒为 0，暂不需要。

**理由**：只有推荐流一个入口时填 0 和填 1 没区别；一旦行为来自多个入口，这个字段是唯一能区分"在推荐流点赞"和"从搜索点进去点赞"的信息。现在留出码表比以后改事件格式便宜得多。

---

## 4. 影子流量

**决策**：曝光事件照常记录（`is_shadow_traffic` 字段透传），**训练默认排除**。

- 落地：归因 join 任务加过滤 `is_shadow_traffic = false`；离线评估、排序对比、故障回放可单独使用影子事件。
- 当前没有调用方设置该标记，本决策不影响首发。

**理由**：影子请求下发的帖子用户没有看到，进训练会把整批候选记成"看到但没互动"的假负样本，系统性压低这批帖子的分。

---

## 5. Kafka topic

**决策**：

- 曝光 topic：`home-mixer.served-candidates`，分区键 `viewer_id`（生产端已实现）。
- 分区数：**首版 6**；QPS 明确后再调（只能增不能减，见 §5.2）。与 UAS topic 分区数一致是加分项，不是前提。
- 保留：Kafka 7 天；数仓 ≥ 90 天。
- 消费方按 `request_id` 去重覆盖。

### 5.1 为什么分区数可以以后定

- 单分区吞吐通常在 10 MB/s 量级；一条曝光事件约 10 KB（35 条候选），单分区就能承载约 1000 请求/秒，远超首发流量。分区数首先影响的是**消费端并行度**，其次才是吞吐。
- 归因 join 在数仓按 `(viewer_id, post_id)` 完成，不依赖 Kafka 分区对齐。跨语言 producer（home-mixer 用 librdkafka，mrpyq 用 Go 客户端）默认分区哈希算法不同，即使分区数相同也未必落到同一分区；因此不要把"曝光和行为同分区"当成设计前提。
- 分区数只能增加不能减少；增加会改变 key → 分区映射，但消费方按 `request_id` 幂等、不依赖顺序，所以事后加分区没有代价。

### 5.2 不同量级的影响

按"曝光事件 ≈ 请求 QPS × 10 KB（zstd 压缩后约 1/5–1/8）"估：

- **< 100 请求/秒**：约 1 MB/s、每天 < 90 GB 未压缩（压缩后 10–20 GB）；6 分区足够，单个消费实例即可。
- **100–1000 请求/秒**：每天 90 GB–1 TB 未压缩；分区 12–24，消费端和数仓摄入需要多实例；Kafka 7 天保留要按压缩后体积预留磁盘（约 100 GB–1 TB）。
- **> 1000 请求/秒**：考虑给事件瘦身（去掉 `weighted_score` / `created_at_ms` 等可回查字段）或按候选拆分消息；数仓侧改为流式摄入。

### 5.3 峰值 QPS 怎么估

```text
日请求量 ≈ DAU × 每人每日刷新次数（首屏 + 下翻，一般 5–20）
平均 QPS ≈ 日请求量 / 86400
峰值 QPS ≈ 平均 QPS × 3（晚高峰系数，社区类产品常见 2–4）
```

例：10 万 DAU × 10 次 ≈ 100 万请求/天 ≈ 12 请求/秒平均、约 40 请求/秒峰值，落在第一档。上线后用 home-mixer 的 RPC 指标（`GetScoredPosts` / `GetForYouFeed` 计数）替换估算值。

---

## 6. 待确认

- UAS topic 的分区数与预计 QPS（用于 §5 校核，不阻塞建 topic）。

## 7. 派生的代码改动（待执行）

1. `home-mixer/clients/phoenix_prediction_client.rs`：`REQUIRED_SUPPORTED_ACTIONS = [1, 2, 18]`，测试同步。
2. `home-mixer/params/param.rs`：§1.3 列出的权重置 0，注释记录原值与恢复条件（本文 §1.4）。
3. `phoenix/services/model_contract.py`：`NONZERO_WEIGHT_ACTION_ENUMS = (1, 2, 18)`；`grpc_gateway.create_servicers` 默认支持集合改为引用该常量；契约测试同步。
4. `phoenix/scripts/train_ranker.py` 文档 / 训练指引：写明 v1 `--observed-actions` 取值。
5. 数据侧归因 join 任务：`--attribution-window-minutes`（默认 30）、`is_shadow_traffic = false` 过滤。
