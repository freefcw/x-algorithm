# 提交 `49815da` 能力清点与吸收结果

> 上游范围：`902a06f..49815da`；可移植项已吸收，GPU 仅完成源码移植，未在本机验证。

提交规模：78 files，`+10938/-982`。本清单覆盖其中 Phoenix/crates/python 与本地可见依赖；home-mixer、candidate-pipeline、visibility-filtering、grox 变化按各自边界处置。

| 上游变化 | 分类 | 处置与证据 |
|---|---|---|
| `phoenix/xrex/cutedsl/ranker_attention_fa4.py`、`ranker_attention_varlen_fa4.py` 及 `ranker_fa4/{ampere_helpers,flash_bwd,flash_bwd_preprocess,flash_bwd_sm90,flash_fwd,flash_fwd_sm90}.py` | U0 | 已移植 SM80/SM90/SM100 架构选择、前反向 kernel 与 layout；保留本地 `checkpoint_name(..., "attn_outputs")` U2 补丁。CUDA kernel 未在 macOS 执行。 |
| `phoenix/xrex/models/recsys_attention.py` | U0 | 已同步架构兼容断言。 |
| `phoenix/xrex/cutedsl/ranker_fa4/{block_sparse_utils,flash_bwd_postprocess,mask}.py`、`phoenix/xrex/utils/aot.py` | U0 | 吸收 block-list bridge、Ampere helper、mask 扩展与 Cutlass device digest/AOT cache 版本更新；仅完成源码级验证。 |
| `phoenix/NOTICE` | U0 | 同步新增 CuTeDSL 文件的第三方来源清单。 |
| `phoenix/xrex/train/recsys_bundle_export.py` | U3 | 本地模块已删除，StableHLO bundle 依赖未公开服务链路，不重新引入。 |
| `crates/common/xai-recsys/src/util.rs` | U0 | 吸收 `InputBuffer: Default`，不恢复本地删除的 conversion-asset 映射。 |
| `crates/serving/xai-recsys-engine/src/copy_port_client.rs` | U0/U2 | 吸收多前缀 checkpoint、bundle manifest、命名文件下载及测试；保留本地移除 shard schedule 打乱的 U2。 |
| 两份 `xai-recsys` proto | U0 | 吸收 `TweetInfo.contentFeatures` / `quotedContentFeatures` 与 `ContentFeatures` 消息；其余生产字段不在 slim 契约中。 |
| `python/common/xai-configlib/src/xai_configlib/__init__.py` | U0 | 吸收 class reference 反序列化。 |
| `home-mixer`、`visibility-filtering`、`grox` | U3/不适用 | 无本地对应模块或不在本次所有权范围。 |

源码导入与 Phoenix Rust/Python 受影响测试用于验证；Linux/NVIDIA、未公开服务依赖仍为 U3，不能宣称运行时完成。
