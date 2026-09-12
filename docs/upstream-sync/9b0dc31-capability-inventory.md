# 提交 `2a38187` / `9b0dc31` 能力清点与吸收结果

> 清点日期：2026-09-04
> 上游范围：`85ac72a..9b0dc31`
> 本地目标分支：`feature/migrate-20260515`
> 结果：Phoenix 服务端 SID 查询客户端下线、pinned D2H 转默认开、checkpoint 多连接下载三项吸收；FA4 block-sparse 注意力重写与随附 remat 策略按「本地无配置选中」不吸收

## 1. 范围概览

上游两个提交：`2a38187` 是社区 PR（VF 查询前给 `in_network_ids` 去重，2 行），`9b0dc31` 是常规批量开源快照，74 个文件、+1956/-1926。

按目录分：visibility-filtering 40、phoenix 15、grox 10、home-mixer 6、abuse-enforcement-service 3。前后四块本地都没有落点（详见第 3 节），真正有落点的只有 phoenix 这 15 个文件，其中 9 个是同一条链路——服务端 SID 查询客户端的整体下线。

`2a38187` 不适用：上游 `in_network_ids` / `oon_ids` 会重复塞入转推、引用推的 ID 所以需要去重，本地这两个列表只装 `candidate.tweet_id`（`home-mixer/candidate_hydrators/vf_candidate_hydrator.rs:62-70`），转推、引用推走独立的 `ancillary_ids` 通道且已经用 `HashSet` 去过重。重复的来源在本地不存在。

## 2. 已吸收

### P1：服务端 SID 查询客户端下线

吸收文件：

- 删除 `phoenix/crates/serving/xai-recsys-engine/src/sid_client.rs`（189 行）
- `src/lib.rs`、`src/python.rs`、`Cargo.toml`、`pyproject.toml`、`xai_recsys_engine.pyi`
- `xrex/inference/model_runner.py`、`sid_retrieval_runner.py`、`launch_inference.py`

历史 SID 现在一律从请求里读，引擎不再需要一个 `SidLookupService` 客户端。跟着下线的还有两个预测服务的 `sid_client` 构造参数和喂它的 `--sid_endpoint` 命令行开关。

**保留** `crates/serving/xai-recsys-sid-proto/` crate 与 workspace 成员——上游也没删，`reference/` 下的两个 SID 服务仍然依赖这份 proto。

删掉的 `sid_client.rs` 带了一个单测 `semantic_ids_are_shifted_and_missing_values_remain_padding`，覆盖 0-indexed → 1-indexed `uint16`（`-1` 表示缺失）的换算。这个契约没有丢覆盖：现在真正在服役的请求侧路径由 `xai-recsys-engine/src/util.rs` 的 `proto_history_semantic_ids_are_read_from_tweet_info` 守着，断言的是同一组 `[0, 7, -1] → [1, 8, 0]`。macOS 上 Phoenix workspace 单测因此从 128 降到 127。

**顺带修了上游自己的两处文档漂移**（上游删了能力但没改文档）：

- `phoenix/QUICKSTART.md`：检索服务的启动命令还在传 `--sid_endpoint localhost:50061`。这个参数已经不存在，照着文档跑会直接 `unrecognized arguments` 挂掉。一并删掉上面那句已无消费方的 `sid_index_server.py` 启动。
- `phoenix/reference/README.md`：还写着「服务引擎的 `PySemanticIdClient` 对接这两个服务」。改为说明这两个服务不再是跑模型的必需件，是给需要自己解析 SID 码的调用方用的。

两个 reference SID 服务本身保留，它们仍然实现完整的 `SidLookupService` 契约。

### P2：pinned 内存 D2H 转为默认开启

吸收文件：`xrex/inference/model_runner.py`、`xrex/inference/launch_inference.py`

`use_pinned_d2h` 默认从 `False` 翻成 `True`。走 CUDA pinned host memory 做设备到主机的拷贝，约 50 GB/s 对 JAX 原生的约 3 GB/s，每次推理省约 29ms。`--use_pinned_d2h false` 仍可退回。

内部核对过一处：这条路径 `import cupy`，而 `cupy-cuda12x` 在 `pyproject.toml` 里带 `sys_platform == 'linux'` 标记，非 Linux 装不上。但 QUICKSTART 开头写明要求是「Linux with an NVIDIA GPU」，唯一受支持的平台上 cupy 必然存在，所以不需要加保护分支。

### P3：checkpoint 走多条 HTTP/2 连接下载

吸收文件：`xai-recsys-engine/src/checkpoint_proxy.rs`、`src/emb_table.rs`

一条 HTTP/2 连接只有一个流控窗口，之前调大 `download_concurrency` 只是把更多 chunk 排在同一个上限后面，不会更快。改为每个 trainer 开 `download_concurrency` 条连接，chunk 在连接间轮转。

连带的结构调整：`resolve_and_connect` 与 `assign_trainer_channels` 的返回值从 `Vec<Channel>` 变成 `Vec<(SocketAddr, Channel)>`。原因是给同一个 trainer 开第二条连接，需要第一条连接解析到的那个地址。`emb_table::get_channels` 里的 HTTP/2 窗口配置抽成 `apply_copy_port_http2`，让首连和补开的连接用同一套参数。

本地补了一个单测 `assigned_slice_keeps_each_channel_paired_with_its_trainer_address`：`checkpoint_proxy.rs` 原本一个测试都没有，而这次改动的要害就是「切片之后每条 channel 还跟着自己的地址」——配错了不会编译失败，只会在运行时把连接开到别的 trainer 上。用 `Endpoint::connect_lazy()` 造 channel，不需要真实服务端。

该模块是 `#[cfg(target_os = "linux")]`，macOS 上不参与编译，这个测试只在 Linux 上运行。本地验证时临时解开 cfg 门跑通后已还原。

## 3. 延期或不吸收

| 能力 | 分类 | 处置 |
|---|---|---|
| FA4 block-sparse 注意力重写（`cutedsl/ranker_attention_fa4.py` 214 行、`models/recsys_attention.py` 22 行）：`build_dense_block_sparse_layout` 改为按 `segment_ids` 算出真实历史长度，产出 fwd/bwd 两套 layout 与有效块上下界，不再对 padding 一视同仁 | 不吸收 | 本地 `xrex/configs/` 里没有任何配置选中 `cutedsl_ranker_attn`（实际在用的是 `cutedsl_ranker_varlen_attn` ×2、`pallas_ranker_varlen_attn` ×5、`jax_attn` ×4、`pallas_ranker_attn` ×2），且该 kernel 硬断言 `GpuArch.GB200/GB300`。本地无配置可达、无硬件可验，改了也是死代码 |
| `models/remat.py` 的 `SAVE_H100_RECSYS`（=32）remat 策略 | 不吸收 | 与上一条同批次。策略里的 `cutedsl_attn_outputs` 这个名字在上游整个开源树中无人产出（`cutedsl/` 下没有 `checkpoint_name` 调用），本地两处 `checkpoint_name` 用的是 `attn_outputs`。抄过来就是一条永远匹配不上的策略项，且没有配置选中 `SAVE_H100_RECSYS` |
| `crates/common/xai-recsys/src/util.rs` 的 `conv_asset_map` / `is_web_conv_row` / `conv_asset_ids_for_batch`（约 150 行） | U3 | 上游开源树内没有任何调用方。与上一轮吸收的 `fetch_mm_embeddings_into` 的区别：那个是在重构本地已有的代码，这个是引入一项本地没有消费方的新能力（广告转化资产映射）。等出现可执行的调用契约再进 |
| home-mixer 6 个文件（`experiment_overrides` 透传等） | U3 | 依赖闭源 proto（`experiment_overrides` 不在任何开源 proto 里）与本地不存在的 `util/strato_context.rs`、`util/phoenix_request.rs`。本地 `params/param.rs` 是 170 行的裁剪镜像（上游 1160 行），按维护策略第 6 条，不为尚未落地的能力添加参数 |
| visibility-filtering 40 个文件（`SafetyLabelMap` 收敛为 `HashSet`、去掉 `TakedownFeature` 与 `created_at_secs`） | 不适用 | 本地无该目录，且属于其内部死代码清理 |
| grox 10 个文件（`flows/`） | 不适用 | 本地 grox 是 6 文件的移植骨架，对应模块不存在 |
| abuse-enforcement-service 3 个文件 | 不适用 | mock 阈值调整，两个档位取值相同（12.34），本地无该目录 |

## 4. 本地补丁登记表

上一轮同步暴露过一个具体的失误：`ranker_attention_fa4.py` 里有两行本地加的 `checkpoint_name`，藏在上游函数体内部，上游这次把整个文件重写了才被动发现；而同样的补丁在 `ranker_attention_varlen_fa4.py` 里还有一份，第一遍没看到——因为那个文件上游这次没动，diff 里不出现。

**嵌在上游函数体内部的本地改动，只在上游恰好改到同一个文件时才会被 diff 提醒。** 登记如下，每轮同步先对照这张表，不依赖 diff 主动报警：

| 位置 | 本地改动 | 类型 | 上游改到时怎么办 |
|---|---|---|---|
| `xrex/cutedsl/ranker_attention_fa4.py` `_attention_fwd` | 给 `out` / `lse` 加 `checkpoint_name(..., "attn_outputs")` | U2 | `49815da` 重写后已重新植入；后续上游改动仍需保留该标签 |
| `xrex/cutedsl/ranker_attention_varlen_fa4.py` `_attention_fwd` | 同上 | U2 | `49815da` 重写后已重新植入；这是实际配置选中的 varlen 路径 |
| `crates/serving/xai-recsys-engine/src/emb_table.rs` `load_tensor_no_resharding` | 移除 `shuffle_sharded_schedule` / `restore_piece_order` 调度打乱 | U2 | 本轮上游改的是同文件的 `get_channels`，区域不重叠。若上游改到该函数体，需重新判断是否保留移除 |
| `xrex/inference/model_runner.py:4039`、`:4704`；`sid_retrieval_runner.py:383`；`gen_recs_runner.py:317` | 四处服务构造走本地 `server_factory.create_recsys_server(...)` 包装，上游是直接 `xai_recsys_engine.RecsysRetrievalPredictorServer(...)` | U2 | 上游改构造参数时，本地只需同步 kwargs，不要跟着改回直接构造。`tests/test_feature_config_schema.py` 有 AST 扫描守着（断言无直接构造、工厂调用恰好 4 处） |
| `xrex/inference/launch_inference.py` `--hotswap_malloc_trim` | 本地保留，上游已移除 malloc_trim 相关路径 | U2 | 上游若再动热切换路径，确认该开关仍有落点 |

## 5. 验证

- 根 workspace：`cargo fmt --all -- --check` 通过。
- 根 workspace：`cargo test --workspace`，255 通过（与上一锚点持平，本轮未触及）。
- Phoenix：`cargo fmt --all -- --check` 通过。
- Phoenix：`PYO3_PYTHON="$PWD/.venv/bin/python3" cargo test --workspace`，127 通过、3 ignored（较上一锚点 −1：随 `sid_client.rs` 一起删除的单测，契约覆盖已由请求侧单测承接，见 P1）。
- Phoenix：`.venv/bin/python3 -m pytest tests/ -q`，100 通过（与上一锚点持平）。
- `checkpoint_proxy` 是 Linux-only 模块，macOS 不参与编译。本轮临时解开 `lib.rs` 的两处 cfg 门做过完整 `cargo check` 与单测（76 通过），随后已还原 `lib.rs` 并复验无 diff。新增的 `assigned_slice_keeps_each_channel_paired_with_its_trainer_address` 只在 Linux 构建中计入。

macOS 上跑 Rust 单测需要给测试二进制补 rpath，否则 pyo3 链接的 Python framework 找不到：

```bash
RUSTFLAGS="-C link-arg=-Wl,-rpath,$(python3 -c 'import sysconfig; print(sysconfig.get_config_var("PYTHONFRAMEWORKPREFIX"))')" \
  cargo test -p xai-recsys-engine
```

## 6. 锚点结论

本轮吸收 Phoenix 服务端 SID 查询客户端下线、pinned D2H 默认开启、checkpoint 多连接下载三项，另修正上游遗留的两处文档漂移。同步锚点从 `85ac72a` 推进到 `9b0dc31`；这不表示本地已启用上游的 FA4 block-sparse 注意力重写、`SAVE_H100_RECSYS` remat 策略或广告转化资产映射。
