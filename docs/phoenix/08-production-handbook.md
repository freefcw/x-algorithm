# Phoenix 正式上线手册

> 本手册面向“准备真实数据、训练 Phoenix、接入服务并正式上线”的完整流程。
> 内容以当前仓库代码为准，重点覆盖 `phoenix/xrex/` 生产引擎；仓库不再提供本地 Demo 或旧 gateway。

## 0. 先读结论：当前只维护 xrex 生产主线

`phoenix/` 当前保留生产引擎和相关测试：

| 路径 | 用途 | 上线定位 |
| --- | --- | --- |
| `phoenix/xrex/` + `phoenix/crates/` | 生产模型、训练器、Rust 推理引擎 | Linux + NVIDIA CUDA 的生产主线 |

本手册的正式主线是：

```text
曝光/行为日志
    -> 训练样本 ETL
    -> xrex Parquet/Kafka dump
    -> Phoenix ranking / retrieval 训练
    -> checkpoint 和 retrieval index
    -> xrex gRPC 推理服务
    -> Home Mixer 或其他推荐编排服务
```

当前仓库**没有**提供完整的生产数据源、训练调度、业务评估、模型发布平台和集群编排。这些部分需要业务侧补齐。

## 1. 正式上线前的系统边界

生产链路至少需要以下组件：

1. 曝光日志和用户行为日志；
2. 用户、帖子、作者及安全特征；
3. 多模态帖子 embedding 服务或离线快照；
4. Semantic ID（SID）训练、分配和查询服务；
5. 训练任务、checkpoint 对象存储和版本管理；
6. Linux + NVIDIA 的 ranking / retrieval gRPC 服务；
7. 召回候选池或向量索引的定期更新任务；
8. 离线评估、灰度、监控和回滚系统。

```text
┌──────────────┐       ┌────────────────────┐
│ 曝光/行为日志 │──────▶│ 训练样本 ETL       │
└──────────────┘       └─────────┬──────────┘
                                 ▼
                         ┌────────────────┐
                         │ Phoenix Trainer│
                         └───────┬────────┘
                                 ▼
                  ┌──────────────────────────┐
                  │ Ranking / Retrieval CKPT │
                  └────────────┬─────────────┘
                               ▼
                    ┌──────────────────────┐
                    │ xrex gRPC 推理引擎   │
                    └──────────┬───────────┘
                               ▼
                         ┌────────────┐
                         │ Home Mixer │
                         └────────────┘
```

## 2. 环境准备

生产 `xrex` 路径要求 Linux、NVIDIA GPU、CUDA 12、Python 3.11+、`uv`、Rust 和 `protoc >= 3.15`。Ubuntu 可先安装：

```bash
apt update && apt install -y \
  build-essential ca-certificates cmake curl pkg-config unzip \
  libibverbs-dev libnl-3-dev libnl-route-3-dev libclang-dev libnuma-dev
```

安装 Python 依赖并构建生产引擎：

```bash
cd phoenix
uv sync --extra engine
export PYTHONPATH=$PWD
```

使用组合双塔和 FA4 attention 配置时：

```bash
uv sync --extra engine --extra fa4
```

验证 GPU 和引擎环境：

```bash
uv run python -c 'import jax; print(jax.devices())'
uv run pytest tests/engine
```

这只能证明当前引擎测试和运行环境可用，不能替代真实 checkpoint、索引和 contract test 验收。

## 3. 选择模型配置

配置位于：

- `phoenix/xrex/configs/xrecsys.py`
- `phoenix/xrex/configs/xrecsys_two_tower.py`

常用配置：

| 用途 | 配置名 |
| --- | --- |
| 精排生产 | `home_direct_packed` |
| 精排 GB300 | `home_direct_packed_gb300` |
| 精排单卡验证 | `home_direct_packed_nano` |
| 双塔生产 | `xrecsys_two_tower` |
| 双塔 GB300 | `xrecsys_two_tower_combined_gb300` |
| 双塔单卡验证 | `xrecsys_two_tower_nano` |

`nano` 配置只用于验证训练和服务流程，不能直接作为生产模型。生产配置在 embedding dimension、层数、词表、序列长度、候选数、attention kernel、索引规模和 batch size 上都不同。

## 4. 生产训练数据契约

### 4.1 不要把旧版 32/8 数据规格当成 xrex 生产规格

当前 `xrex` 生产训练器读取的事实来源是：

- `phoenix/reference/dump_gen.py`
- `phoenix/xrex/data/parquet_recsys.py`
- `phoenix/xrex/data/recsys/recsys_batch.py`

生产数据应按 xrex 的 Parquet/Kafka dump 契约生成，而不是直接套用旧文档的 `[B, 32, ...]`、`[B, 8, ...]` 张量格式。

### 4.2 文件布局

```text
offline_kafka_dump/
├── .valid_batches.json
├── partition=0/
│   ├── 0/batch_0.parquet
│   └── 0/batch_1.parquet
├── partition=1/
│   └── ...
└── ...
```

批次路径规则是：

```text
partition={partition_id}/{batch_id // 2000}/batch_{batch_id}.parquet
```

`.valid_batches.json` 至少包含：

```json
{
  "min_valid_batch": 0,
  "max_valid_batch": 1234,
  "num_partitions": 16
}
```

每个 Parquet footer 还需要：

```text
min_kafka_timestamp_ms
max_kafka_timestamp_ms
```

否则 xrex 的时间范围过滤不能正常工作。

### 4.3 核心生产列

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `userId` | `int64` | 用户全局数值 ID |
| `length` | `int64` | 有效序列长度 |
| `tweetIdSeq` | fixed list `int64` | 历史和候选帖子 ID |
| `authorIdSeq` | fixed list `int64` | 对应作者 ID |
| `promotedIdSeq` | fixed list `int64` | 推广内容 ID，没有时为 0 |
| `impressedTimeMsSeq` | fixed list `int64` | 曝光/行为时间，毫秒 |
| `clientAppIdSeq` | fixed list `int64` | 客户端应用 ID |
| `ipAddressSeq` | fixed list `int64` | 离散化 IP ID |
| `timezoneSeq` | fixed list `int32` | 时区 |
| `productSurfaceSeq` | fixed list `int32` | 产品场景 |
| `lineItemObjectiveSeq` | fixed list `int16` | 广告目标 |
| `safetyLabelMaskSeq` | fixed list `int64` | 安全标签 |
| `favCountSeq` | fixed list `int64` | 点赞计数 |
| `replyCountSeq` | fixed list `int64` | 回复计数 |
| `repostCountSeq` | fixed list `int64` | 转发计数 |
| `quoteCountSeq` | fixed list `int64` | 引用计数 |
| `viewCountSeq` | fixed list `int64` | 浏览计数 |
| `paddingMask` | fixed list `bool` | 有效序列位置 |
| `newEventMask` | fixed list `bool` | 候选/新事件位置 |
| `actionNameMultiHotSeqSeq` | `[sequence, 64] bool` | 64 维离散行为目标 |
| `continuousActionValuesSeqSeq` | `[sequence, 8] float32` | 连续行为值 |
| `sampleWeight` | `float32` | 样本权重 |
| `kafka_timestamp_ms` | `int64` | Kafka 时间 |
| `kafka_offset` | `int64` | Kafka offset |

用户画像列包括 `userGender`、`userAgeBracket`、`userState`、`userLanguageCode`、`userCountryCode`、经纬度、DMA、年龄、推断性别和 `installedAppsMultiHot`。

生产配置的典型几何是最多 1022 个历史位置和 64 个候选位置，具体以 config 为准。

## 5. 从业务日志构造训练样本

### 5.1 曝光日志

至少保存：

```text
user_id
impression_time_ms
post_id
author_id
position
product_surface
request_id / impression_id
model_version
candidate_source
```

建议同时保存召回分、精排分、最终展示位置、是否实际返回客户端和 session 信息。

### 5.2 行为日志

至少保存：

```text
user_id
post_id
event_time_ms
action_name
dwell_time_ms 或 dwell_time_seconds
request_id / impression_id
```

行为必须能关联到“用户 + 帖子 + 曝光时间窗口”。

### 5.3 历史序列

对每一条曝光：

1. 只取曝光时间之前的行为；
2. 按时间倒序排列；
3. 按配置截断最大历史长度；
4. 不足位置补 padding；
5. 使用 `newEventMask` 区分历史和候选。

必须满足：

```text
历史行为时间 < 曝光时间
标签行为时间 >= 曝光时间
```

不能把用户当天全部行为直接作为 history，否则会产生未来信息泄漏。

### 5.4 帖子和作者特征

至少准备：

```text
post_id
author_id
created_at_ms
post_text / media references
is_deleted
is_sensitive
recommendation_eligible
fav_count
reply_count
repost_count
quote_count
view_count
```

## 6. 多模态 embedding 和 Semantic ID

### 6.1 多模态 embedding

生产帖子需要统一版本的多模态 embedding，通常是 1024 维单位向量：

```text
post -> multimodal embedding [1024]
```

参考代码：

- `phoenix/reference/mm_encoder.py`
- `phoenix/reference/mm_snapshot_gen.py`

生产侧必须固定 embedding 模型版本、维度、归一化规则和 post ID 映射。训练数据、召回候选索引和线上服务必须使用一致的 embedding 版本。

### 6.2 Semantic ID

当前 SID 约定是 6 层、每层 256 个 code：

```text
post embedding -> RQ/RQ-VAE codebook -> [sid_0, ..., sid_5]
```

参考代码：

- 训练：`phoenix/reference/sid_codebook.py`
- 分配：`phoenix/reference/sid_assign.py`
- 查询：`phoenix/reference/sid_index_server.py`

约定：

- 服务 wire 层使用 0-based code，范围 `[0, 256)`；
- 模型输入 buffer 使用 1-based code；
- `0` 表示 missing；
- 不能把服务层 code 原样写入模型 buffer。

SID 快照至少需要：

```text
post_id
post_sid: fixed-width list<int32>[6]
author_id
```

训练 dump 的 `semanticIdSeq` 必须和线上 SID 服务描述同一批帖子。

## 7. 用合成数据验证生产格式

接真实数据前，先跑通仓库提供的生产格式参考链路：

```bash
cd phoenix

uv run python reference/world_snapshots.py \
  --out ./synth_index \
  --seed 20260721

export PHOENIX_INDEX_BASE=$PWD/synth_index

uv run python reference/dump_gen.py \
  --out ./synth_dump \
  --seed 20260721 \
  --num-rows 12288 \
  --partitions 4 \
  --rows-per-file 1024 \
  --sid ./synth_index/sid_snapshot/post_sid_v5_256x6.parquet \
  --self-check
```

这一步验证 Parquet schema、分区布局、batch metadata、footer timestamp、SID、padding 和 history/candidate 边界。合成值不是生产数据，也不能用于判断业务效果。

## 8. 训练精排和召回

### 8.1 精排

```bash
uv run python reference/train_synth.py \
  --data ./synth_dump \
  --steps 500 \
  --out "$PWD/checkpoints" \
  --metrics
```

### 8.2 双塔召回

```bash
uv run python reference/train_synth.py \
  --config xrecsys_two_tower_nano_offline_kafka_dump \
  --data ./synth_dump \
  --steps 500 \
  --out "$PWD/checkpoints"
```

`reference/train_synth.py` 是单卡验证 launcher。接入真实数据后，应由生产调度器设置 dataset path、config、步数、GPU 数量、checkpoint 目录和评估任务。

训练运行必须记录：

```text
config_name
dataset_version
data_start_time / data_end_time
embedding_version
sid_codebook_version
code version / git commit
GPU type
batch size
learning rate
optimizer
step
loss
checkpoint path
```

排序旗舰配置和 nano 使用 Muon dense optimizer；其他配置使用 AdamW；embedding table 使用 sparse rowwise AdaGrad。不要跨配置自行混用 optimizer 配方。

## 9. 评估门禁

训练 loss 下降不能证明模型可以上线。至少需要以下离线指标。

### 精排

```text
favorite / reply / click AUC 和 LogLoss
dwell MAE / RMSE
NDCG@K
MRR
分用户、分 surface、分内容类型指标
新用户和老用户指标
```

### 召回

```text
Recall@K
HitRate@K
MRR@K
正样本进入候选池的比例
新帖子召回率
冷启动召回率
```

### 数据质量

```text
缺失 user/post/author 比例
SID 覆盖率
embedding 覆盖率
history 为空比例
candidate 为空比例
label 全零比例
未来行为泄漏数量
重复曝光数量
异常 dwell 比例
非法 action 数量
```

训练集、验证集和测试集应按时间切分，避免同一曝光或未来行为泄漏到训练输入。

## 10. Checkpoint 和发布包

训练 checkpoint 通常包含：

```text
params
embedding tables / embedding state
optimizer state
post_embeddings              # retrieval 使用
step
config
metadata
checksums
```

续训需要 optimizer state；发布服务通常只需要推理所需的参数、embedding state 和 retrieval index。仓库提供 checkpoint repack 工具：

```bash
uv run python reference/repack_checkpoint.py \
  --src /models/full_checkpoint \
  --dst /models/publishable_checkpoint
```

发布版本必须绑定：

```text
model_version
training_data_version
embedding_version
sid_version
config_name
checksum
```

ranking checkpoint、retrieval checkpoint、候选索引、embedding 和 SID 必须版本配套。不能只替换其中一个文件。

## 11. 启动 xrex 生产推理服务

### 11.1 精排

```bash
XLA_PYTHON_CLIENT_MEM_FRACTION=0.80 \
uv run python xrex/inference/launch_inference.py \
  --driver local \
  --service_type ranking \
  --config_name home_direct_packed \
  --checkpoint_path /models/ranking/checkpoint \
  --grpc_port 9988 \
  --metrics_port 9091 \
  --num_devices_per_process 1 \
  --bs_per_device 1 \
  --allow_random_init false \
  attn_impl=pallas_ranker_attn_infer
```

### 11.2 召回

```bash
XLA_PYTHON_CLIENT_MEM_FRACTION=0.80 \
uv run python xrex/inference/launch_inference.py \
  --driver local \
  --service_type retrieval \
  --config_name xrecsys_two_tower \
  --checkpoint_path /models/retrieval/checkpoint \
  --grpc_port 9990 \
  --metrics_port 9092 \
  --num_devices_per_process 1 \
  --bs_per_device 1 \
  --allow_random_init false
```

实际生产参数必须根据 GPU、序列长度、batch、attention kernel 和压测结果确定，不能直接照搬 nano QUICKSTART 参数。

### 11.3 SID 查询服务

如果调用方需要通过 post ID 查询 SID：

```bash
uv run python reference/sid_index_server.py \
  --parquet /index/sid_snapshot/post_sid_v5_256x6.parquet \
  --port 50061
```

它提供 `SidLookupService.LookupSids`。SID 服务不会自动补齐线上请求中缺失的用户、帖子或作者特征。

## 12. Home Mixer 接入边界

仓库已删除旧的本地演示/兼容网关及其 Docker/Kubernetes 部署文件。当前生产入口是
`xrex/inference/launch_inference.py`，分别启动 ranking 和 retrieval 服务；它们使用
xrex native engine 与生产 checkpoint，不使用旧网关的 `.npz` 参数和本地 retrieval index。

不能直接假定 Home Mixer 当前 Phoenix gRPC 请求可以无修改调用 xrex。当前没有
`phoenix-gateway` 镜像或 Kubernetes 清单，避免把未适配的 xrex 服务部署成旧网关。

正式接入前必须做 contract test，确认：

```text
Home Mixer request
  -> xrex ranking request 是否等价
  -> 是否包含完整 history
  -> 是否包含 SID
  -> 是否包含 user profile
  -> 是否包含 candidate feature
  -> response action taxonomy 是否一致
```

因此，当前可靠判断是：

- Home Mixer 的旧演示 Gateway 已不再由本仓库提供；
- xrex 生产引擎需要协议适配或独立 production caller；
- xrex ranking/retrieval 需要分别编排，不能共用一个 `phoenix-gateway` Deployment；
- 真实 GPU、模型挂载、资源、滚动更新和压测还需要在目标环境验证。

## 13. 正式上线验收清单

### 数据

- [ ] 曝光可关联到 request/session；
- [ ] 行为可以归因到曝光；
- [ ] history 没有未来行为；
- [ ] Parquet schema 和 footer 通过 self-check；
- [ ] 分区和 `.valid_batches.json` 正确；
- [ ] SID 和 embedding 覆盖率达标；
- [ ] action taxonomy 一致；
- [ ] ID 可以稳定转换为 xrex 数值 ID。

### 训练

- [ ] 按时间切分 train/validation/test；
- [ ] AUC、NDCG、Recall 等达到业务门槛；
- [ ] 各用户和内容分桶没有严重退化；
- [ ] checkpoint 可以恢复；
- [ ] checksum 校验通过；
- [ ] retrieval checkpoint 与 index 版本一致。

### 服务

- [ ] ranking 和 retrieval 都能启动；
- [ ] gRPC health check 通过；
- [ ] warmup 成功；
- [ ] 延迟、并发和显存达到目标；
- [ ] 模型加载失败不会误接流量；
- [ ] model version 可观测；
- [ ] 旧版本可以回滚。

### 业务

- [ ] 召回为空时有冷启动/规则降级；
- [ ] Phoenix 超时或失败时有规则降级；
- [ ] 新帖子有 embedding/SID 策略；
- [ ] 敏感内容和 viewer visibility 正确处理；
- [ ] 曝光事件可靠落库；
- [ ] CTR、互动率、停留时长可以按 model version 对账；
- [ ] 已看内容和重复内容过滤正常。

## 14. 推荐执行顺序

```text
1. 准备 Linux + CUDA + xrex engine
2. 用合成数据跑通 dump、训练、checkpoint、gRPC
3. 用真实数据生成 1,000～10,000 行 xrex Parquet
4. 先做 schema/self-check，不急于扩大训练
5. 用 nano 配置训练真实小样本
6. 验证 ranking 和 retrieval 的真实 gRPC 链路
7. 建立离线指标和数据质量门禁
8. 切生产配置训练和压测
9. 完成 Home Mixer/xrex contract test
10. 灰度：1% -> 5% -> 25% -> 100%
```

距离正式上线当前仍需业务侧补齐四项：

1. 真实业务数据到 xrex Parquet 的 ETL；
2. 生产级离线评估和质量门禁；
3. xrex gRPC 与 Home Mixer 的协议适配；
4. 训练、发布、灰度、监控和回滚编排。

## 15. 相关文档

- [Phoenix README](../../phoenix/README.md)：生产引擎总览、环境、测试和保留工具
- [Phoenix 训练与数据](./06-training-and-data.md)：训练输入、产物和数据边界
- [Phoenix 训练与数据分析](06-training-and-data.md)：训练契约和现有缺口
- [Phoenix 真实数据接入](07-real-data-integration.md)：曝光、UAS 和 xrex 训练输入的数据合同
- [生产验收和故障排查](../bootstrap/07-生产化验收和故障排查.md)：整个推荐系统的上线验收
