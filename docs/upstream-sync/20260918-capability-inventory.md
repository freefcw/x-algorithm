# 上游 `fad2f71`–`c279172` 能力清点与吸收结果

> 上游范围：`2d4a03c..c279172`；本地选择性吸收，未整体 cherry-pick。

本轮包含 `fad2f71`、`42266f3`、`c279172` 三个快照，合计 100 个文件、约 `+4303/-3986` 行。大部分变化属于上游广告/训练数据、安全服务或私有网络栈，不能在当前 portable 分支中只移植半套合同。

| 上游变化 | 分类 | 本地处置 |
|---|---|---|
| `phoenix/crates/serving/xai-recsys-engine/src/mem_util.rs` 的 huge-page `madvise` 长度计算 | U0 | 已吸收。对小于地址对齐偏移的 buffer 使用饱和减法并跳过零长度调用，避免 Linux 下溢。 |
| copy-port 的 `send_entries` / `ready_call_parse` 诊断上下文 | U0 | 已吸收。tensor、checksum、rank 和 shard 名称进入超时/传输错误日志；不改变数据路径。 |
| Phoenix Rust `thrift` `v0.23.0` git 依赖升级为 crates.io `v0.24.0` | U0 | 已吸收并更新 `phoenix/Cargo.lock`；workspace 编译通过。 |
| `xai-recsys/src/util.rs` 将 `Gender` 直接写入模型特征 | U4 / 本地契约差异 | 未吸收。当前分支的特征编码明确为 `Male→2`、`Female→1`，改为 proto 原值会改变既有模型输入语义；保留本地映射。 |
| `home-mixer` `FavHoldoutFilter` 与 `EnableFavHoldout` | U3 | 未吸收。当前 `PostCandidate` 没有该上游装配所需的收藏计数/实验合同，不能新增默认关闭但无法验证的业务开关。 |
| VM Ranker `Dscp::InferenceCritical` 与广告 Brand Safety 默认值调整 | U3 | 未吸收。依赖私有网络 QoS API 和线上实验配置，当前公开 workspace 无相同部署合同。 |
| `phoenix/xrex` purchase-value、delayed ads、head masking、H100 配置与数据列 | U3 | 未吸收。依赖上游生产训练列、checkpoint 和广告标签；本地没有可验收的数据生产者。 |
| `recsys.proto` 的 `EngagementCounts`、`SearchLexicalMatch` 及广告字段 | U3 | 未吸收。当前 slim proto 没有对应请求/响应消费者，不补半套协议。 |
| `visibility-filtering` 新 action 解码与 twemcache 替换 | 不适用 / U3 | 未吸收。当前分支不装配该服务，也没有可用的 `xai_x_thrift` / cache 服务合同。 |
| Grox、abuse-enforcement、under-the-hood、takedowns 变化 | 不适用 / U3 | 未吸收。它们不属于当前推荐链路的 portable 主路径。 |

## 验证

- `cd phoenix && cargo check --workspace`：通过。
- `cd phoenix && PYO3_PYTHON=$PWD/.venv/bin/python3 cargo test -p xai-recsys -p xai-recsys-engine`：通过，`28 + 81` 个测试通过，另有 `1` 个既有 ignored 测试。
- 未宣称广告训练、私有安全链路、Dscp QoS 或 Linux/NVIDIA 运行验收完成。
