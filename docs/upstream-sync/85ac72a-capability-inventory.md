# 提交 `7ba7768` / `85ac72a` 能力清点与吸收结果

> 清点日期：2026-09-03
> 上游范围：`6384ca7..85ac72a`
> 本地目标分支：`feature/migrate-20260515`
> 结果：Phoenix 引擎 search query embedding 布局与两处运行时健壮性改动吸收；训练侧加密 checkpoint、StableHLO 导出、SID recon 与 backbone score 链路按外部合同延期

## 1. 范围概览

两个上游提交共涉及 56 个文件，增加 3975 行、删除 3316 行。体量最大的两块是 visibility-filtering 规则重组（约 2800 行搬迁）和训练侧加密 checkpoint 收尾，两者在本地都没有落点。真正有本地代码落点的是 Phoenix Rust 引擎的 search query embedding 布局、多模态 embedding 客户端错误处理，以及 xrex 两处与外部依赖无关的小修。

## 2. 已吸收

### P1：search query embedding 单向量化与批缓冲清零

吸收文件：

- `phoenix/crates/common/xai-recsys/src/util.rs`（上游 `7ba7768`）
- `phoenix/crates/serving/xai-recsys-engine/src/python.rs`（上游 `85ac72a`）

吸收内容：

- `InputBuffer::new_with_candidates` 不再按 `candidate_seq_len` 复制 search query embedding，只保留一份原始向量；维度不匹配时留空并记录错误。
- 新增 `repeat_query_into` 与 `InputBuffer::num_real_candidates`，由批准备阶段按真实候选数展开。
- 批准备阶段写入前对 `candidate_embeddings` 分片与 search query 分片清零。

两个上游提交必须合并吸收：`repeat_query_into` 只写前缀，缺少 `85ac72a` 的清零会把复用批缓冲的残留数据暴露给模型。本地按单次提交落地。

吸收动机（除对齐上游外的本地收益）：

- 每请求 search query 缓冲从 `candidate_seq_len × dim` 降到 `dim`。
- 批准备阶段原本是裸 `copy_from_slice`，长度不等会 panic 掉 rayon worker；改为长度校验后跳过。
- 复用批缓冲的尾部与 padding 行不再保留上一批残留值。

改动面：`InputBuffer::candidate_search_query_embeddings` 在本地只有 `python.rs` 一个消费点。上游随附 `search_query_stays_one_vector`、`repeat_query_into_writes_prefix_and_keeps_tail` 两个单测。

上游两个单测只覆盖零件，不覆盖批准备阶段的接线，而 `num_real_candidates` 决定有多少候选拿得到 query 向量，上游没有为它留测试。本地补 `num_real_candidates_counts_non_padding_slots`，覆盖空候选、少于 `candidate_seq_len`、恰好填满、超出被截断四种形态。批准备阶段本身（`RankingBatchPrep`）仍无测试，需要 Python 解释器与 numpy 缓冲，脚手架成本高于收益，且该文件本来就没有这类测试，不在本轮处理。

### P2：多模态 embedding 拷贝改为返回错误

吸收文件：`phoenix/crates/serving/xai-recsys-mm-server/src/mm_embedding_client.rs`

吸收内容：

- `assert!(emb_dim > 0)` 改为 `anyhow::ensure!`，`emb_dim` 非法时返回 Error 而非 panic。
- 拆出 `fetch_mm_embeddings_into`，校验目标缓冲长度是 `emb_dim` 的整数倍；`fetch_mm_embeddings_sync` 成为它的分配版包装。
- 保留上游对 in-process cache 命中/缺失的回归断言。

`fetch_mm_embeddings_into` 目前上下游都没有外部调用方，吸收它只为与上游保持同形。

### P3：xrex 两处与外部依赖无关的小修

- `phoenix/xrex/utils/checkpointing.py`：`wait_until_finished()` 不再在没有 checkpointer 时隐式构造一个再等待。只取该行，不引入上游同批次的 `orbax_encrypted` 分支。
- `phoenix/xrex/driver/hooks.py`：`WandbHook.on_step` 的 metrics 过滤移入线程池，主线程每步少一次 dict 推导。与上游改法一致。

## 3. 延期或不吸收

| 能力 | 分类 | 处置 |
|---|---|---|
| backbone score 链路（proto `returnBackboneScores` / `backboneTopLogProbs` / `backboneContinuousValues`，Home Mixer query、candidate、scorer、server 五处） | U3 | 开源树内没有任何代码写这两个字段，生产者不在开源范围；本地 Home Mixer 也没有 `phoenix_request.rs`、`candidate_scores()` 与 `served_slate_context`。与既有的 `reranker_head_tag`、`PageDecode` 同类 |
| 加密 checkpoint 收尾（`orbax_encrypted.py`、`dek.py`、`encrypted_kvstore.py`、`load.py` 重构、`checkpoint_write.py` 的 KMS 分支、`misc.py` 的加密配置字段、`trainer.py` 的 `_restored_encrypted`） | U3 | 本地 `xai_checkpointing` 不含 `dek.py` / `encrypted_kvstore.py`，`load.py` 无 `_is_encrypted_tree`，无 `xai_kms`。KMS、认证、密钥轮换、审计、恢复合同均未变化 |
| StableHLO bundle 导出（`recsys_bundle_export.py`、`RecsysTrainer.export_stablehlo_bundle`） | U3 | 依赖完整 copy_port 发布链路与 jax export 环境；引擎侧加载 `export/` 的代码不在本范围内，本地无法独立验证 |
| SID recon 模式（`recsys_sid.py`、`recsys_model.py` 的 `sid_embedding_mode`、`xrecsys.py` 的 `sid_decoder_path`） | U3 | 需要外部 `codebook.safetensors`（`stages` 与 `dec_*`）产物，本地无生成方也无回归可验证 |
| `xrecsys_two_tower.py` combined base 的 `qk_norm` / `attn_logit_cap` 翻转 | 训练口径 | 是超参取向而非缺陷；本地另一变体已是 `True` / `-1`。是否统一 base 由训练侧决定，不随通用同步一起改 |
| Home Mixer `param.rs` 的 `PhoenixRetrievalAggregationType`、`AdsBlenderType` 默认值翻转 | 不适用 | 本地 `param.rs` 已裁剪，这两个 param 不存在 |
| Grox `[Card Title]` 文案、Kafka SCRAM 改多区域 producer | 不适用 | 本地 Grox 已重构为 `grox/src/grox/`，对应模块不存在 |
| visibility-filtering 规则重组、scarecrow XReview 上报 | 不适用 | 本地无这两个目录 |
| proto `DpaProductInfo.sid_codes` | U3 | 本地 DPA 走 `recsys_ads_dpa` 编译开关，无消费方 |

## 4. 记录一处永久分歧：`ActionName` 204–252 号段

`85ac72a` 给 `ActionName` 加了 `reserved 214 to 252`，同时上游 204–213 是 `ADS_*_CONVERSION_DELAYED`、254 是 `ADS_MMP_CLICK`。

本地这段是一整片 `ADS_RAW_ENGAGEMENT_TYPE_*`（204–252 共 49 个值），254 是 `PLACE_HOLDER_254`。也就是说本地正在使用的号段，上游已正式标记为保留。

这不是缺陷，但它把「将来是否追齐上游 action 号段」从可选项变成了 wire 不兼容的破坏性变更。**本地决定保留 204–252 的自有语义**，后续同步不再逐次重新评估该号段；只有在业务明确要求与上游模型共享 action 索引空间时，才作为一次独立的协议迁移处理。

## 5. 验证

- 根 workspace：`cargo fmt --all -- --check` 通过。
- 根 workspace：`cargo test --workspace`，255 通过。
- Phoenix：`cargo fmt --all -- --check` 通过。
- Phoenix：`PYO3_PYTHON="$PWD/.venv/bin/python3" cargo test --workspace`，128 通过、3 ignored（较上一锚点 +3：P1 随附的两个上游单测，加本地补的 `num_real_candidates` 单测）。
- Phoenix：`.venv/bin/python3 -m pytest tests/ -q`，100 通过（较上一锚点 +8：`_should_keep` 六个保留判定单测，加两个 checkpoint 模块独立导入回归测试）。
- `git diff --check` 通过。

原先记录的「`xrex/utils/checkpointing.py` 与 `xrex/driver/hooks.py` 只做到 `py_compile` 校验」判断有误，本轮已查清并修复：

- 根因不是 orbax 与 jax 版本不匹配，而是 `xrex/utils/checkpointing.py` 少了 `from xai_checkpointing import fix_jax`。`fix_jax` 是仓库内既有补丁，把 jax 0.8.1 移除的 `jax.lib.xla_extension.XlaRuntimeError` 映射回 `jax.errors.JaxRuntimeError`；另外两处导入 orbax 的文件（`xai_checkpointing/load.py`、`reference/repack_checkpoint.py`）都先导入了它，只有这一处漏了。`orbax-checkpoint==0.9.1` 与 `jax==0.8.1` 是上游有意组合，依赖声明无需改动。
- 训练主路径未受影响：`trainer.py` 先导入 `xai_checkpointing.load`，补丁在导入 `checkpointing` 之前已生效。受影响的只有单独导入这两个模块，也就是单测与隔离验证本身。
- 已补齐该导入，`xrex/utils/checkpointing.py` 与 `xrex/train/checkpoint_write.py` 现均可单独导入，并加子进程回归测试守护（这个导入看似未使用，容易被当成冗余删除）。
- `xrex/driver/hooks.py` 不依赖 orbax，原说明把它一并归因是错的；本轮改动的 `_should_keep` 无外部依赖，已补单测。

## 6. 锚点结论

本轮吸收 Phoenix 引擎的 search query embedding 布局与两处运行时健壮性改动，以及 xrex 两处不依赖外部服务的小修。同步锚点从 `6384ca7` 推进到 `85ac72a`；这不表示本地已启用上游的加密 checkpoint、StableHLO 导出、SID recon、backbone score 或 visibility-filtering 能力。
