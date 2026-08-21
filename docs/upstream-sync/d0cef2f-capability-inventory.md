# 提交 d0cef2f 能力清点与 U0–U3 分类（2026-08-20 上游快照）

> 文档状态：清点完成，emb_table 已落地
> 上游提交：`d0cef2f943084ee0d4310378031c9c2c37d67f12`（2026-08-20）
> 上游父提交（当前已吸收锚点）：`aad7179773944e17eb8798bbbf0231d6cd6c1ffc`
> 本地目标分支：`feature/migrate-20260515`
> 迁移规则：[`upstream-first-maintenance.md`](./upstream-first-maintenance.md)

## 1. 提交概览

14 个文件，+479/-200。三组内容：

1. **AI 趋势反馈上下文**（home-mixer）：新 hydrator 为 LLM 反馈采样挑选趋势目标，配套候选字段、参数开关、排序器与 URT  marshalling。
2. **Served SlateContext 回路**（home-mixer）：候选可携带服务端返回的 slate context，排序器优先复用而非重算。
3. **引擎 embedding 表重构**（phoenix）：`load_tensor` 拆分为可测试的纯 Rust 核心 + 薄 PyO3 包装。

## 2. 分支对照事实

| 事实 | 证据/影响 |
|---|---|
| `emb_table.rs` 本地与上游父提交逐字节一致 | hash 比对；可整体采用 |
| 趋势 hydrator 依赖 `StratoClient::batch_get_tweet_ai_trend`/`batch_get_ai_trend_name`（AI 趋势数据，未公开后端） | 本地 `strato_client.rs` 无此方法族 |
| 本地无 `SlateContext`（`ranking_scorer.rs:382` 注释记录的暂缓），无 `util/urt/`（post_marshaller 无落点） | grep/目录列表 |
| 引擎 proto 的 `SlateContext.sid*` 与响应侧合同已随 aad7179 就位 | b515fe1 |

## 3. 能力清单与分类

| 编号 | 能力 | 分类 | 处置 |
|---|---|---|---|
| T1 | `emb_table.rs`：`load_tensor` 拆分为纯 Rust `load_tensor_into`（&str/slice、`Result<_, String>`，可单测）+ 薄 PyO3 包装；错误路径去 PyValueError 化；`is_multiple_of` 现代化 | **U0** | 整体采用，文件与上游终态逐字节一致 |
| T2 | `AiTrendFeedbackContextHydrator`（157 行）：仅原创帖、非 topic 请求、非纯网内时启用；Strato 批量查帖子趋势 ID → 按频次取 top-2 趋势（平票取小 ID）→ 每趋势随机抽 1 帖 → 批量查趋势名 → 写入 `ai_trend_name`/`ai_trend_id` | **U3** | 阻塞点：AI 趋势的 Strato 数据合同未公开（`batch_get_tweet_ai_trend`/`batch_get_ai_trend_name` 无本地实现）。选择算法语义已记录，重入条件：趋势数据客户端合同落地 |
| T3 | served slate context 回路：`PostCandidate.served_slate_context`、`From<xai_recsys_proto::SlateContext>` 转换（含 sid 字段）、`UseServedSlateContext` 开关、ranking_scorer 优先复用 served context（其次缓存、最后重算）、phoenix_scorer 从预测响应存取 | **U3** | 依附 A5 SlateContext 线路；但引擎侧合同（请求/响应 proto）已就位，本项记录了消费侧语义，A5 重入时与服务端回路一并评估 |
| T4 | URT `post_marshaller` 把 `ai_trend_name`/`id` 写入响应 | **U3** | 本地无 `util/urt/` 模块；依附 T2 |
| T5 | grox：`grok_sampler/llm.py` 采样增强、data_types/post_mapper 小改、ptos classifier 微调 | **U3** | P6-B 线路 |

## 4. 落地

- `phoenix: adopt d0cef2f emb_table testability split`（T1）。
- 其余 T2–T5 记录为 U3，语义已固化在 §3，重入时无需重读上游 diff。

## 5. 验证

- `PYO3_PYTHON=.venv/bin/python3 cargo test --workspace`（phoenix）：116 通过，含 emb_table 与 SID 相关测试。
- 根 workspace `cargo test` 与 `run_demo.sh` 不回归（T1 为引擎内部重构，demo 链路不经由该 crate 运行）。
