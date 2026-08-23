# 提交 28e414f 能力清点与 U0–U3 分类（2026-08-21 上游快照）

> 文档状态：清点完成，U0 已全部落地
> 上游提交：`28e414f535e4b5a50ca12ee87674e7649e50c7ad`（2026-08-21）
> 上游父提交（此前已吸收锚点）：`d0cef2f943084ee0d4310378031c9c2c37d67f12`
> 本地目标分支：`feature/migrate-20260515`
> 迁移规则：[`upstream-first-maintenance.md`](./upstream-first-maintenance.md)
> 迁移结果：[`../update/20260823.md`](../update/20260823.md)

## 1. 提交概览

35 个文件，+856/-152。四组内容：

1. **引擎指标修复**（xai-recsys-engine）：deadline shed 拒绝不再被 finish 双计。
2. **proto 合同扩展**：slateContext 字段 + ADS MACT/view-through 枚举。
3. **训练侧广告转化能力**（xrex）：view-through heads、按 source 拆分头训练、delayed feedback 样本、分块前向、checkpoint 加固、配置与文档。
4. **已剥离模块的小改**（grox、visibility-filtering）与新增 xai-o2 crate。

## 2. 能力清单与分类

| 编号 | 能力 | 分类 | 处置 |
|---|---|---|---|
| E1 | `request_metrics.rs`：`guard.record_reject` + `already_rejected`，消除 deadline shed → inflight_cap/checkpoint_loading 的双计；附回归测试 | **U0** | 整体采用，文件与上游终态逐字节一致 |
| E2 | `recsys.proto`：`NextActionDistribution.slateContext=9`；ActionName 171–190（MACT 点击/视图 12 个 + view-through 转化 8 个） | **U0** | 两份 proto 同步采用；Python pb2 构建期重生成；为 T3 SlateContext 与广告特征的前置合同 |
| X1 | view-through/delayed-feedback 训练能力：constants 映射与 `ads_slim` feed、`conversion_keep_mask`/`sample_source` 数据链、模型开关 `train_view_through_heads`/`split_head_training_by_source`/`mact_in_app_loss_weight`/`metric_mask_keys`/`delayed_sample_mask` | **U0** | 整体采用；所有开关默认关闭，不启用即禁用 |
| X2 | trainer 候选塔 `_forward_chunked`：按 65536 行分块前向（仅 total_samples 整除 world_size 时启用），语义不变 | **U0** | 整体采用 |
| X3 | checkpoint 加固：`ORBAX_TMP_DIR_SUFFIX` 常量化；`_is_loadable_checkpoint` 校验 Orbax payload 已提交才可发现 | **U0** | 整体采用 |
| X4 | `launch_inference.py` hotswap `--hotswap_malloc_trim`（默认开，jemalloc 下 no-op） | **U0** | 整体采用 |
| X5 | 配置：两塔 `qk_norm`/`attn_logit_cap=-1`/`right_anchored_rope`；nano 改用 `_GB300_OVERRIDES` 学习率+优化器；两塔删除无用 `num_continuous_actions`；retrieval EVERGREEN 路径更新 | **U0** | 整体采用（nano 训练配方随上游变化） |
| X6 | TRAINING.md / README / reference README 的 Muon 披露改写 | **U0** | TRAINING/reference 整体采用；README 手工合并（保留本地仓库说明块） |
| G1 | grox：移除 grok-4.5-internal dial 与模型名、reply_spam 阈值 60k→80k | N/A | 本地 grox 已剥离 ptos/reply_spam 流程，无落点 |
| V1 | visibility-filtering/dark_traffic_setup.rs 调整 | N/A | 本地已整目录剥离该模块 |
| O1 | 新增 `crates/storage/xai-o2` crate：`o2_client_builder.rs`（TLS/rustls 客户端构建器）、base_client 小改、workspace 成员与依赖（pem/rcgen） | **U3** | 阻塞点：本地无 O2 存储消费方，引入需连带 workspace 依赖树。重入条件：出现真实的 O2 接入需求时按 U1 引入适配层 |

## 3. 手工合并说明

- `phoenix/xrex/data/recsys/feature_config.py`：保留本地 U2 扩展 `OPTIONAL_BOOL_FEATURE_NAMES`（旧数据集 bool 列可选），叠加上游 `OPTIONAL_COLUMNS` 新增的 `is_delayed_feedback`/`conversionKeepMask`。
- `phoenix/README.md`：保留顶部"本地仓库说明"块，优化器披露段采用上游终态。
- 未采纳文件：`phoenix/Cargo.toml`、`phoenix/Cargo.lock`(增量纯为 xai-o2 依赖树)。
