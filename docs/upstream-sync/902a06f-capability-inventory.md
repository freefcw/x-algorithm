# 提交 `902a06f` 能力清点与吸收结果

> 清点日期：2026-09-07
> 上游范围：`9b0dc31..902a06f`
> 本地目标分支：`feature/migrate-20260515`
> 结果：Phoenix 检索数据集路径构造、日志 formatter 结构化字段两项对齐性吸收；home-mixer 的 MoE 共分流实验改动与 visibility-filtering 全部无本地落点

## 1. 范围概览

上游单个常规快照 `902a06f`，25 个文件、+480/-179。按目录：visibility-filtering 12、phoenix 8、home-mixer 5。

本地无 visibility-filtering 模块，那 12 个文件直接不适用。home-mixer 5 个文件里，2 个的宿主组件本地未迁移、1 个只改了同步时间戳注释、1 个依赖本地不存在的 `SlateContext` 模型；唯一有同名文件的 `author_cold_start.rs`（275 行，本轮最大改动）改的是本地不存在的实验分流机制。phoenix 8 个文件里 6 个落在未迁移的模块上。

**本轮是低产出的一轮。**两项吸收都不改变运行时行为，价值只在于把这两个文件保持与上游逐字节一致，使下一轮 diff 仍是小 delta。如果只按业务收益取舍，本轮可以什么都不做。

## 2. 已吸收

### P1：检索数据集 SID 快照路径抽出 `_sid_window()`

吸收文件：`xrex/data/retrieval_dataset.py`

上游去掉了 `_idx()` 里 `sub.replace("post_sid_v5_256x6_snapshots", "post_sid_v8_256x6_snapshots")` 这个字符串替换 hack，改为常量 `_SID_SNAPSHOTS = "post_sid_v8_256x6_snapshots"` 加一个返回主备两条路径的 `_sid_window(filename)`。6 个枚举成员（`HOME`、`IMMERSIVE2Day`、`EVERGREEN`、`IMMERSIVENSFW`、`IMMERSIVE4Day`、`IMAGINE`）从各写两行完整路径收敛成一行。

**路径结果完全等价**，用一次性脚本逐条核过全部 9 条构造路径（6 个 `_sid_window` 成员的主备各一条，加仍走 `_idx()` 的 `TAIL` 与两组 `relevant_ads` 索引），无一不一致。两处容易看错的地方：

- 旧 `_idx()` 的替换对 `post_sid_v5_256x6_snapshots_backup` **生效**（前缀匹配），所以备份路径原本就是 v8；新写法由 `f"{_SID_SNAPSHOTS}_backup"` 显式拼出，结果相同。
- 旧 `_idx()` 的替换对 `post_sid_v5_256x6_tail_snapshots` **不生效**（中间隔着 `_tail_`，不构成子串），`TAIL` 一直指向 v5；新 `_idx()` 不再做替换，`TAIL` 仍是 v5，行为不变。

### P2：日志 formatter 渲染 `extra=` 结构化字段

吸收文件：`xrex/utils/log_util.py`

`get_formatter()` 增加 `_format_extra_kv()`：把 `LogRecord` 上的非标准属性渲染成 `k=v` 追加到日志首行末尾，`event` 排在最前，bool 渲染为 `true`/`false`，非标量走 `json.dumps`。多行日志（traceback）不受影响——kv 插在首行与换行之间。

本地和上游整个 phoenix 树目前都**没有任何 `logging(..., extra={...})` 调用方**，这是先行于消费方的基础设施。行为中性：记录上没有额外属性时 `_format_extra_kv()` 返回空串，输出与改前逐字节相同。

冒烟验证过三种形态：无 extra 的行输出不变；带 extra 渲染为 `event=sync n=3 ok=true cfg={"a":1}`；`logger.exception` 的 traceback 仍在 kv 之后另起行。

### 顺带：修正上一轮遗留的 `Cargo.lock` 漂移

上一轮 `3461491`（服务端 SID 查询客户端下线）从 `crates/serving/xai-recsys-engine/Cargo.toml` 删掉了 `xai-recsys-sid-proto = { workspace = true }`，但 `phoenix/Cargo.lock` 里 `xai-recsys-engine` 的依赖列表没跟着重新生成。本轮跑 `cargo test` 时被自动修正，净改动一行。

workspace 层面的 `xai-recsys-sid-proto` 成员声明与依赖定义**保持不动**——`reference/` 下两个 SID 服务仍在用，上一轮已明确保留。

## 3. 延期或不吸收

| 能力 | 分类 | 处置 |
|---|---|---|
| `home-mixer/scorers/author_cold_start.rs`（本轮最大改动）：抽出 `ColdStartParams` 一次性读参；treatment 臂的语料判定增加 `is_phoenix_moe(c)` 约束；`author_corpus` 计算提到 `enabled` 早退之前，使对照/实验两臂的 bucket impression 对称 | 不适用 | 本地 `AuthorColdStart` 是精简移植：读静态 `ColdStartConfig`（`params::` 常量）而非 feature switch，没有 `ViewerArm` / `AuthorCorpus` / `ExperimentBucketImpressor` 概念，三项改动都没有落点。`ColdStartParams` 那部分的本地等价物 `ColdStartConfig` 早已存在 |
| `home-mixer/candidate_hydrators/engagement_counts_hydrator.rs`：`enable()` 去掉 `EnableViewerColdStart` 分支与 `has_cached_posts` 条件 | 不适用 | 本地无 `EngagementCountsClient`，该 hydrator 整体未迁移 |
| `home-mixer/side_effects/mutual_follow_stats_side_effect.rs`：`moe_cluster` 提前求值 | 不适用 | 本地无该 side effect |
| `home-mixer/models/candidate.rs`：`SlateContext` 新增 `exact_k` / `exact_gap` | U3 | 本地 home-mixer 没有 `SlateContext` 模型，`scorers/ranking_scorer.rs:382` 已注明未引入 SlateContext 持久化。与 20260903 记为 U3 的 SID recon 模式同族，本地无生产者也无消费方 |
| `home-mixer/params/param.rs` | 不适用 | 只改了 `last sync` 时间戳注释 |
| `phoenix/xrex/train/recsys_bundle_export.py`：新增 `restamp_manifest`；前向输出改走 bf16 往返再回 f32 | U3 | 本地无该模块。StableHLO bundle 导出在 20260903 已记为 U3 |
| `phoenix/xrex/train/trainer_recsys.py`：写 bundle 时对 MANIFEST 重打时间戳 | U3 | 依赖上一行那个本地不存在的模块，本地 `trainer_recsys.py` 也没有 `_write_stablehlo_bundle_files` |
| `phoenix/python/training/xai-checkpointing/`：`dek.py` 给 KMS unwrap 加瞬时错误退避重试；`orbax_encrypted.py` 提交前断言 `_DEK` 存在 | 不适用 | 加密 checkpoint 模块依赖 `xai_kms`，本地未迁移 |
| 两个 `recsys.proto`：`PredictNextActionsRequest.experiment_overrides`（field 21）、`SlateContext.exactK`/`exactGap`、`ADS_MMP_CLICK`(254) 取代 `PLACE_HOLDER_254` | 已记永久分歧 / U3 | 本地 proto 冻结在 `24c6094`。`ActionName` 204–252 号段是 20260903 写死的永久分歧，不再逐次评估；`experiment_overrides` 见第 4 节；`exactK`/`exactGap` 与 `ADS_MMP_CLICK` 本地都无消费方 |
| visibility-filtering 12 个文件（dark traffic 配置、TES hydrator、twemcache 等） | 不适用 | 本地无该目录 |

## 4. 上一轮 U3 理由的修订

[`9b0dc31-capability-inventory.md`](./9b0dc31-capability-inventory.md) 第 3 节把 home-mixer 那 6 个文件判 U3 的依据之一写成「`experiment_overrides` 不在任何开源 proto 里」。**这半条理由本轮作废**：`902a06f` 把 `map<string, string> experiment_overrides = 21` 补进了 `PredictNextActionsRequest`。

成因是上一轮上游开源快照自身不一致——`9b0dc31` 的 `home-mixer/util/phoenix_request.rs:139` 已经在填这个字段，proto 还没跟上，`902a06f` 补齐了。

**结论不变，依据换成另一半**：本地没有 `util/phoenix_request.rs`（该字段唯一的填充点）也没有 `util/strato_context.rs`；`params/param.rs` 是 170 行的裁剪镜像（上游 1160 行），按维护策略第 6 条，不为尚未落地的能力添加参数。下轮再评估这组文件时以本节为准，不要引用上一轮那条已作废的理由。

## 5. 本地补丁登记表

本轮上游改动的 25 个文件与 [`9b0dc31-capability-inventory.md`](./9b0dc31-capability-inventory.md) 第 4 节登记的 7 个位置**无交集**（逐条核对：两个 `ranker_attention*_fa4.py`、`emb_table.rs`、`model_runner.py`、`sid_retrieval_runner.py`、`gen_recs_runner.py`、`launch_inference.py` 本轮均未被上游触及）。登记表原样结转，内容不变。

## 6. 验证

- 根 workspace：`cargo fmt --all -- --check` 通过。
- 根 workspace：`cargo test --workspace`，258 通过。较上一锚点 +3，来自本地提交 `805e0fb`（thunder Kafka 与帖子存储运行语义修复带的 3 个测试），与本轮同步无关——本轮只改 Python。
- Phoenix：`cargo fmt --all -- --check` 通过。
- Phoenix：`PYO3_PYTHON="$PWD/.venv/bin/python3" cargo test --workspace`，127 通过、3 ignored（与上一锚点持平）。
- Phoenix：`.venv/bin/python3 -m pytest tests/ -q`，100 通过（与上一锚点持平）。
- `git diff --check` 通过。
- 两个吸收文件与上游 `902a06f` 逐字节一致（`git diff 902a06f -- <file>` 为空）。

本轮**没有新增单测**。P1 是路径构造的等价重写，等价性用一次性脚本核对 9 条路径后确认，不留测试；P2 本地无调用方，只做冒烟验证。两项都是 U0 逐字节对齐，不属于需要回归测试的 U2 扩展。

## 7. 锚点结论

本轮吸收 `retrieval_dataset.py` 路径构造与 `log_util.py` 结构化日志渲染两项，均为行为中性的上游对齐，没有引入任何新的运行时能力。同步锚点从 `9b0dc31` 推进到 `902a06f`。

home-mixer 的 MoE 共分流实验（`author_cold_start.rs` 两臂 impression 对称、treatment 臂 MoE 约束）、StableHLO bundle 导出、加密 checkpoint、`experiment_overrides` 透传仍在 U3 或不适用，本轮没有任何一项状态发生变化。
