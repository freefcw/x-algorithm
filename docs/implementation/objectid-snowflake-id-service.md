# ObjectId → Snowflake 统一身份服务

状态：代码迁移完成，待真实历史映射导入与生产切换验收。

## 目标

外部请求、事件和训练输入可以继续使用 24 位小写 hex ObjectId；进入推荐系统内部模块前统一解析为持久化 Snowflake。Home Mixer、Phoenix/xrex、Thunder 和 VM Ranker 的内部业务 ID 使用同一个 Snowflake，不为每个系统建立独立映射。

```text
外部 ObjectId
    → ID Ingress / Registry
    → 内部 Snowflake
    → 推荐系统各模块
    → ID Egress / Registry
    → 外部 ObjectId
```

## 映射存储

`id-service` 使用 append-only JSONL 文件作为持久化源。每条记录包含：

```text
object_id, entity_kind, snowflake_id
```

启动时建立索引；写入使用 `sync_data`，进程重启后可以恢复。ObjectId 与 Snowflake 都有唯一约束，映射一旦生成不复用、不覆盖。

`entity_kind` 当前为 `User` / `Post`，只用于防止实体语义混用；不是按系统分配多套 ID。

## 缓存策略

- 用户：进程生命周期内缓存正向和反向映射。
- 帖子：按 ObjectId 的秒级创建时间缓存 30 天；过期记录仍保留在磁盘，查询时从持久化索引恢复。
- 单查：`resolve_one` / `reverse_one`。
- 批查：`resolve_batch` / `reverse_batch`。
- 推荐请求应在入口批量解析，不能让下游模块自行查表。

## Snowflake 规则

使用当前 xrex 兼容的布局：41 位毫秒时间、10 位 worker、12 位 sequence，epoch 为 `1288834974657`。ObjectId 只有秒级时间时，转换为秒乘 1000；允许最多约 1 秒的年龄误差。ID 限制在正的 signed-64 范围内，因为 xrex 同时使用 `int64` 和 `uint64`。

如果上游已经提供可信 Snowflake，迁移时必须保留原值，不能按 ObjectId 重新分配，否则已有 xrex checkpoint、UAS 和 retrieval index 的模型输入会改变。

## 边界与迁移

### 与 `main` 的基线关系

`main` 已经采用数值型内部 ID：Home Mixer 领域对象主要使用 `u64`，Thunder 的 `PostStore` 使用 `i64`，Phoenix/xrex proto 使用 `int64`/`uint64`。当前分支的 `ObjectId` 领域模型是后续分支改造，不应继续作为内部模型扩散。

整理后的目标是恢复 `main` 风格的内部数值模型，同时保留外部兼容合同：

```text
外部 ObjectId
    ↓ 入口 Registry resolve
内部 Snowflake (u64 / i64 wire-compatible)
    ↓ Home Mixer / Phoenix / Thunder / VM Ranker
外部兼容适配器需要时
    ↓ Registry reverse
外部 ObjectId
```

边界规则：

- UAS、业务帖子查询、TES/Strato/mrpyq 和已有 JSON/Kafka/Redis 外部合同继续使用 ObjectId；进入 xrex 的训练 Parquet 必须先 resolve 为 Snowflake。
- Phoenix、Thunder、VM Ranker 的内部状态、索引、embedding key、xrex 请求和内部事件使用 Snowflake。
- Phoenix 对 Home Mixer 的兼容 gRPC 如果仍是字符串 ObjectId，则只在 Phoenix adapter 的入口转换；不能把字符串继续带入 xrex。
- 禁止 MD5、截断、取低位等不可逆映射；禁止各模块自行分配 ID。历史数据必须通过 trusted Snowflake 导入，不能在线重新分配。
- Registry 只有一个写入权威。独立服务运行时，Home Mixer/Thunder/Phoenix/VM Ranker 通过 Registry Client 访问，不直接共享并发写入同一个 JSONL 文件。

### 执行阶段

1. **基线与 Registry**：保留 `id-service` 的持久化、单查、批查、反查和缓存测试；补充唯一写入权威、并发分配和失效策略。
2. **内部类型恢复**：以 `main` 的数值型 `PostCandidate`、`ScoredPostsQuery` 和 `PostStore` 为目标，建立 `SnowflakeId`/`u64` 的明确类型边界；外部 ObjectId 不进入这些模型。
3. **Phoenix 入口**：UAS、候选和 viewer 在 Phoenix adapter 入口批量 resolve；xrex ranking/retrieval 全部使用 Registry Snowflake；响应通过反向索引恢复外部合同。
4. **Thunder 入口**：Kafka 外部事件保持原合同，在写入 `PostStore` 前 resolve；Thunder 查询、索引和返回结果使用 Snowflake，兼容层负责 ObjectId 反查。
5. **VM Ranker 与事件**：embedding store、ranker 请求、served/训练内部事件统一使用 Snowflake；外部落盘格式按原合同反查或保持明确版本。
6. **切换与清理**：删除 MD5/零填充 demo 转换和重复 Registry；逐条运行跨重启、批量一致性、反查、旧数据兼容和失败闭环测试。

每个阶段必须先通过对应测试后再迁移下一条链路，不能通过修改类型别名跳过入口/出站转换。

配置：

- id-service 默认只接受已有 mapping 或 `trusted_snowflake_id`；只有明确传入 `--allow-allocation` 才会为新 ObjectID 分配 Snowflake。生产环境如果要对齐 main、Thunder 或 Phoenix index，应保持关闭并先导入 trusted mapping。
- `ID_REGISTRY_URL`：Phoenix adapter 访问统一 Registry 的地址，默认 `http://127.0.0.1:50070`。
- `HOME_MIXER_ID_REGISTRY_URL`：Home Mixer Registry client 地址，默认 `http://127.0.0.1:50070`。

Home Mixer 启动会校验 Registry URL；实际 Registry 不可用时，首次需要解析身份的请求 fail-closed。Home Mixer 只使用 Registry Client，不直接打开或写入映射文件。

切换完成的验收条件：

1. 同一 ObjectId 跨重启得到同一 Snowflake；
2. 单查与批查结果一致；
3. 用户映射命中内存缓存，30 天内帖子命中内存缓存；
4. 未知/非法映射 fail-closed；
5. xrex checkpoint、retrieval index 和 Registry 使用同一版本；
6. 对外响应仍能通过反向索引恢复原始 ObjectId。

当前状态：Registry crate 已完成单写持久化、批量原子提交、可信 Snowflake 导入和 Client 响应校验；Home Mixer 领域模型以及 Thunder、VM Ranker、Phoenix/xrex 在线协议均使用数值 ID，ObjectId 只保留在公开 RPC、Redis、mrpyq 和事件边界。Phoenix 训练输入会通过 Registry 输出 Snowflake，并生成带 mapping version 与 SHA-256 的 identity contract；adapter 启动时必须绑定该 contract 和显式 model version，Home Mixer 会校验返回 metadata。部署已提供单副本 ID Registry StatefulSet、PVC 和共享服务地址，跨边界回归测试覆盖 Registry → Home Mixer → Thunder/VM Ranker → reverse，Phoenix 测试覆盖训练输出 → 在线 translator。代码迁移已闭环；真实 trusted mapping、历史 Redis 数据和已发布 checkpoint/retrieval index 仍需在生产切换前按环境导入并验收。
