# ObjectId → Snowflake 统一身份服务

状态：开发阶段已切换到 Redis 主存储，待按真实千万级数据量压测内存和吞吐。

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

生产 `id-service` 默认使用 Redis 作为共享持久化存储，不再把全量映射加载到每个服务副本的内存中。Redis 适配器带有有界进程内缓存，命中时作为二级存储，未命中再访问 Redis；写入 Redis 成功后回填缓存。开发调试可将 `ID_REGISTRY_REDIS_ENABLED=false` 切换到纯内存模式，此时只在当前进程内保存映射，适合单进程本地运行，不提供跨副本或跨重启的一致性。映射不再集中放进一个 Hash，而是使用按 CRC16 分片的独立 key；默认 256 个逻辑分片，每个 key 通过 hash-tag 固定到对应 slot：

```text
id-registry:v2:object:{037}:user:<object_id>       -> <snowflake_id>
id-registry:v2:object:{142}:post:<object_id>       -> <snowflake_id>
id-registry:v2:snowflake:{091}:<snowflake_id>       -> user:<object_id> / post:<object_id>
```

正向 key 按 `entity_kind:object_id` 分片，反向 key 按 Snowflake 分片，因此单次读只访问一个 key，批量读按 slot 分组 pipeline；不会产生两千万 field 集中在一个 big key、一个 slot 或一个分片上的问题。分片数是 schema 的一部分，不能在线随意修改。

正向和反向索引位于不同 key，无法在 Redis Cluster 中用跨 slot Lua 做单事务提交。写入采用“反向先写、正向后写”的两阶段 `SET NX` 语义（Lua 脚本，返回 1 新建 / 0 同值已存在 / -1 冲突）：已有相同值视为幂等；反向阶段冲突表示该 Snowflake 已属于别的对象（`SnowflakeTaken`），正向阶段冲突表示该对象已绑定别的 Snowflake（`MappingConflict`）；正向写入发生冲突时，仅删除本次刚创建、且仍等于本次值的反向 key。已有映射不可覆盖。

### 反向 orphan 的语义

进程在两阶段之间崩溃或超时，会留下只有反向 key、没有正向 key 的 orphan（`snowflake:{..}:S -> post:<oid>`）。对它的处理规则：

- 读路径 fail-closed：`find_by_snowflake` / `find_by_snowflake_batch` 在反向命中后必须再读对应正向 key 并校验其值等于该 Snowflake；不一致或缺失时视为“不存在”（`/v1/reverse` 返回 404，不进本地缓存），记 warn 日志 `orphan reverse mapping` 并累加 `id_service_orphan_reverse_mappings_total`。这样 `resolve(reverse(x)) != x` 的双射破裂不会暴露给调用方。
- 同对象的 trusted 再导入会补齐正向 key（反向 reserve 返回 0，正向 reserve 返回 1），单条与批量路径一致；批量预检对 orphan 视为不存在，因此不会误报 409。
- 分配路径若抽到的 Snowflake 恰好撞上 orphan，反向阶段返回 -1，分配器按下文规则重抽序列。
- 服务**不会**在读或写路径上自动覆写或删除 orphan：它与正在进行中的两阶段写入在 Redis 里无法区分，自动清理有竞态。orphan 会一直占用该 Snowflake，直到用离线修复工具（扫描反向 key、校验正向 key、按业务决定补齐或删除）处理；当前仓库尚未提供该工具。

### 元数据

```text
id-registry:v2:metadata:mapping_version   -> 2            （身份合同版本 MAPPING_VERSION）
id-registry:v2:metadata:storage_schema    -> sharded-256  （存储布局版本 STORAGE_SCHEMA）
```

两者解耦：`mapping_version` 是对外身份合同版本（响应字段 `mapping_version`，客户端会校验），`storage_schema` 只描述 Redis key 布局。服务启动时对两个 key 各执行一次 `MULTI SETNX + GET EXEC`：首个副本写入，之后的副本只读回校验，值不一致时拒绝启动（`MappingVersionMismatch` / `StorageSchemaMismatch`），避免旧 Hash schema 或别的布局被静默当成当前布局使用。`/readyz` 只做 `GET` 校验，不会重建缺失的元数据：元数据缺失返回 503 “metadata is missing”。旧的 `v1` Hash 不做在线兼容，切换前必须单独完成迁移或重建。

### 分配序列

Snowflake 分配序列是 Redis 中按 `(worker_id, ObjectId 秒)` 分片的 counter（`{prefix}:sequence:{worker}:{second}`，`INCRBY n` + `EXPIRE` 在一个 Lua 脚本里完成，TTL 2 天）。counter 在 Redis 而不在进程内，因此多副本共享同一个 worker id 也不会重复分配；不同 worker id 只是让分配结果可追溯到副本。序列与已存在 Snowflake 的关系：

- trusted 导入成功后，若 Snowflake 解码出的 worker 等于本服务的 `worker_id`，服务会把对应 `(worker, second)` counter 推到至少 `offset + 1`（只涨不降，`offset = 毫秒内偏移 * 4096 + sequence`），使同一秒后续分配直接跳过已占用的值；
- 分配得到的 Snowflake 若仍撞上已存在的反向 key（其他 worker 导入的值、TTL 过期后迟到的同秒 ObjectId、或 orphan），分配器先重读对象（别的写者可能已抢先绑定），否则重抽序列，最多 `MAX_ALLOCATION_ATTEMPTS = 16` 次，用尽后返回 409 `SnowflakeTaken`；
- 被整批拒绝的请求可能浪费几个序号；序号只是分配草稿，不是已生效身份，无副作用。

Redis 应启用 AOF、复制和定期备份。`ID_REGISTRY_REDIS_URL` 用于单 Redis、代理、Sentinel 暴露地址或托管 Redis 入口；`ID_REGISTRY_REDIS_CLUSTER_URLS` 用于原生 Redis Cluster seed URLs，两个配置不能同时设置。

`entity_kind` 当前为 `User` / `Post`，只用于防止实体语义混用；不是按系统分配多套 ID。

旧的 `IdRegistry` JSONL 实现已删除：它没有外部调用方，且其单写者批量原子语义与 Redis 版并发语义容易互相误导；需要本地 fixture 时直接用 `RedisIdRegistry::with_store` 注入内存 `MappingStore`。

## 缓存策略

Redis-backed registry 只保留有上限的进程内热点缓存：

- `ID_REGISTRY_CACHE_CAPACITY` 同时限制正向和反向缓存，默认每个方向 100,000 条；
- 用户映射默认不按时间过期，但受容量上限约束；
- 帖子映射按 ObjectId 的秒级创建时间缓存 30 天，同时受容量上限约束；
- Redis 才是完整映射的 source of truth，缓存淘汰不会丢数据；
- 单查：`resolve_one` / `reverse_one`；
- 批查：`resolve_batch` / `reverse_batch`；
- 推荐请求应在入口批量解析，不能让下游模块自行查表；
- 批量 allocation 语义：整批先按 `(entity_kind, object_id)` 去重，再把能预见的冲突全部查清（已有映射和传入的可信 ID 对不上、可信 ID 已被别的对象占用、批内自相矛盾、关闭分配时存在未知 ID），任何一项冲突就整批拒绝（409；allocation 关闭时为 404；关闭 trusted 导入时带 `trusted_snowflake_id` 为 403），一个映射都不写，调用方可以放心修数据后重试。只有并发窗口（别的副本恰好在这批检查和写入之间抢先写了同一批对象）可能留下部分写入，此时重读获胜映射并返回，重试依然安全。
- 在线协议把读写语义分开：`Resolve/ResolveBatch` 只读取已有 mapping，未知 ObjectId 永远返回 404；只有显式的 `Allocate/AllocateBatch` 才允许为新对象创建 mapping。Home Mixer 身份摄入和 UAS worker 才能调用 Allocate，普通下游、存储适配器和出口只能调用 Resolve/Reverse。
- 往返次数（Single 模式，每一轮是一个 pipeline，与 id 数量无关）：正向批查 1 轮；反查 2 轮（反向 GET + 正向校验 GET）；批量写 = 正向查 1 轮 + trusted 预检 1 轮（有 trusted 项时）+ 序列 `INCRBY` 1 轮（有分配项时）+ 写入 3 轮（反向 reserve、正向 reserve、冲突补偿删除，后两轮只含需要的项）+ 序列下限 1 轮（trusted 项落在本 worker 时）；分配撞车时每次重抽再加 1 轮重读 + 1 轮序列 + 3 轮写。Cluster 模式下每一轮按 slot 分组成多个 pipeline 并发执行，往返次数与 Single 模式相同，只是并发扇出。Lua 用 `EVALSHA` 预加载脚本；遇到 `NOSCRIPT`（脚本被 flush 或新节点）会重新 `SCRIPT LOAD` 并重试一次。
- 超时预算：`ID_REGISTRY_REDIS_REQUEST_TIMEOUT_MS`（默认 500）约束的是**单个 Redis pipeline**，一次批量写最多要串行经过约 5～8 轮；Home Mixer Registry gRPC 调用和 Phoenix `IdentityRegistryClient` 都有客户端 deadline，Home Mixer gRPC 失败不会再发 HTTP 请求。在线只读/分配少量 id 的请求通常一两轮即可，但迁移批量导入应使用更大的客户端超时或更小的批（`--max-batch-size` 默认 10000 只是硬上限），并且服务端单次 Redis 超时不应大于客户端预算的一小部分，否则客户端会先超时而服务端仍在写入。

内存预算按每条本地缓存约 100～250 bytes 粗估。默认容量约占 20～50 MB 加 Rust 进程基础开销；千万级全量映射不会等比例复制到每个 Registry 副本。Redis 本身按两个独立索引 key 约 250～600 bytes / mapping 预估，千万级数据应先按约 3～6 GB 数据、8～16 GB Redis 实例做压测起点，并保留 AOF、复制和碎片余量。分片后单 key 只承载一条映射，resharding、RDB/AOF rewrite 和 `--bigkeys` 不再被一个超大 Hash 拖住；总吞吐仍需按分片和节点数压测。

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
- Registry 只有一个写入权威。独立服务运行时，Home Mixer/Thunder/Phoenix/VM Ranker 通过 Registry Client 访问，不直接操作 Redis 映射 key，也不共享并发写入本地文件。

### 执行阶段

1. **基线与 Registry**：保留 `id-service` 的持久化、单查、批查、反查和缓存测试；补充唯一写入权威、并发分配和失效策略。
2. **内部类型恢复**：以 `main` 的数值型 `PostCandidate`、`ScoredPostsQuery` 和 `PostStore` 为目标，建立 `SnowflakeId`/`u64` 的明确类型边界；外部 ObjectId 不进入这些模型。
3. **Phoenix 入口**：UAS、候选和 viewer 在 Phoenix adapter 入口批量 resolve；xrex ranking/retrieval 全部使用 Registry Snowflake；响应通过反向索引恢复外部合同。
4. **Thunder 入口**：Kafka 外部事件保持原合同，在写入 `PostStore` 前 resolve；Thunder 查询、索引和返回结果使用 Snowflake，兼容层负责 ObjectId 反查。
5. **VM Ranker 与事件**：embedding store、ranker 请求、served/训练内部事件统一使用 Snowflake；外部落盘格式按原合同反查或保持明确版本。
6. **切换与清理**：删除 MD5/零填充 demo 转换和重复 Registry；逐条运行跨重启、批量一致性、反查、旧数据兼容和失败闭环测试。

每个阶段必须先通过对应测试后再迁移下一条链路，不能通过修改类型别名跳过入口/出站转换。

配置：

- `ID_REGISTRY_REDIS_ENABLED` / `--redis-enabled`：是否启用 Redis，默认 `true`。关闭时使用进程内 `MemoryMappingStore`，适合本地开发调试；映射和序列计数不会跨进程或重启持久化；
- `ID_REGISTRY_REDIS_URL` / `--redis-url`：单 Redis 连接入口（Redis 模式下必填）；
- `ID_REGISTRY_REDIS_CLUSTER_URLS` / `--redis-cluster-urls`：逗号分隔的原生 Redis Cluster seed URLs；与单 Redis URL 互斥；
- `ID_REGISTRY_REDIS_KEY_PREFIX` / `--redis-key-prefix`：默认 `id-registry:v2`；schema 固定为 256 个分片；
- `ID_REGISTRY_CACHE_CAPACITY` / `--cache-capacity`：每个方向的本地缓存上限，默认 `100000`；
- `ID_REGISTRY_REDIS_CONNECT_TIMEOUT_MS`：Redis 连接超时，默认 `2000`（单节点与 Cluster 路径都生效）；
- `ID_REGISTRY_REDIS_REQUEST_TIMEOUT_MS`：单个 Redis pipeline 的超时，默认 `500`（见上文超时预算）；
- `ID_REGISTRY_GRPC_LISTEN` / `--grpc-listen`：gRPC 主监听地址，生产清单使用 `0.0.0.0:50072`（本地默认值由二进制提供）；
- `ID_REGISTRY_HTTP_LISTEN` / `--http-listen`：HTTP 兼容监听地址，生产清单保留 `0.0.0.0:50070` 供旧客户端使用；
- 旧 `ID_REGISTRY_LISTEN` / `--listen` 在新二进制中作为隐藏的 HTTP 监听别名保留（优先于 `--http-listen`），并打印弃用告警。升级时先部署同时开放 50070 HTTP 与 50072 gRPC 的实例，再把客户端切换到 `ID_REGISTRY_GRPC_ADDR`；旧 HTTP 客户端可继续使用 50070，完成迁移后再下线兼容入口；
- `ID_REGISTRY_WORKER_ID` / `--worker-id`：Snowflake worker 标识（0..=1023），默认 `0`。序列 counter 在 Redis，多副本共享同一 worker id 也安全；使用不同 worker id 只是为了让分配结果可追溯到副本；
- `ID_REGISTRY_ALLOW_ALLOCATION` / `--allow-allocation`：默认关闭。该开关只控制显式 `Allocate/AllocateBatch`，不会改变 `Resolve/ResolveBatch` 的只读语义。新服务可在受控在线 Registry 开启，调用方仍必须使用 Allocate；普通查询未知 ObjectId 仍返回 404；历史数据导入仍使用 trusted import；
- `ID_REGISTRY_ALLOW_TRUSTED_IMPORT` / `--allow-trusted-import`：默认关闭。关闭时任何带 `trusted_snowflake_id` 的请求（单条或批量任一项）在读写之前整批返回 403，防止普通调用方绕过分配权威直接指定 Snowflake。只在迁移导入专用副本上开启，在线只读副本与 k8s 清单都不开；
- `ID_REGISTRY_MAX_BATCH_SIZE` / `--max-batch-size`：单次批量请求的 id 数上限，默认 `10000`，超限返回 413；
- gRPC 请求与响应消息上限固定为 `4 MiB`；批量数量和 protobuf 消息大小同时受限，避免超大字符串或批次占满服务端内存；
- `RUST_LOG`：日志过滤（`env_logger` 语法，如 `info` 或 `id_service=debug`）；`ID_REGISTRY_LOG_FORMAT`：`text`（默认）或 `json`，与 Home Mixer 的 `HOME_MIXER_LOG_FORMAT` 同一格式；
- `ID_REGISTRY_GRPC_ADDR`：Phoenix adapter 访问 Registry 的 gRPC 主地址，生产清单为 `127.0.0.1:50072`；
- `ID_REGISTRY_URL`：旧 Phoenix / 迁移工具访问 Registry 的 HTTP 兼容地址，生产清单为 `http://127.0.0.1:50070`；默认 Phoenix adapter 使用 gRPC，gRPC 失败不会再切换到 HTTP。
- `HOME_MIXER_ID_REGISTRY_GRPC_ADDR`：Home Mixer Registry gRPC 地址，生产清单为 `http://id-registry:50072`；Home Mixer gRPC 失败即返回错误，不执行 HTTP 重试。

Home Mixer 启动会校验 Registry gRPC 地址；实际 Registry 不可用时，首次需要解析身份的请求 fail-closed。Home Mixer 只使用 Registry Client，不直接打开或写入映射文件。

### 运行状态、日志与停机

- `/healthz`、`/readyz` 与 `/metrics` 只在 HTTP 兼容端口提供；gRPC 主端口只承载 Registry RPC。
- `/healthz` 只表示进程存活。
- `/readyz` 只读地 `GET` `mapping_version` 与 `storage_schema` 两个元数据 key 并比对常量；Redis 不可用、元数据缺失或不匹配都返回 503，供负载均衡器摘除当前副本。探测不会写 Redis，也不会重建元数据（元数据只在启动时用 `SETNX` 写入一次）。
- `/metrics` 与业务接口同一监听端口，Prometheus 文本格式：`id_service_requests_total{route,status}`、`id_service_conflicts_total{kind="mapping"|"snowflake_taken"|"entity_kind"}`、`id_service_trusted_imports_total`、`id_service_allocations_total`、`id_service_redis_errors_total`、`id_service_orphan_reverse_mappings_total`、`id_service_cache_entries{direction="object"|"snowflake"}`、`id_service_build_info{version}`；Home Mixer 与 uas-worker 额外通过 `home_mixer_client_calls_total{client="id_registry",method="resolve_grpc|allocate_grpc|reverse_grpc",result="ok|mapping_miss|reverse_miss|error"}` 观测调用结果和耗时。
- 日志：启动打印配置摘要（Redis URL 中的用户名/密码已脱敏为 `***@`）、ready 地址、每次 trusted 导入（info）、每次冲突（warn）、每个 Redis 错误（error，带操作名如 `insert reverse`）、orphan 反向记录（warn）、收到信号与停止（info）。
- 优雅停机：收到 SIGTERM 或 Ctrl-C 后停止接受新连接，等待在途请求完成再退出（退出码 0）。

HTTP 状态码按错误变体映射，不做消息子串匹配：

| 状态 | 错误 |
| --- | --- |
| 400 | ObjectId 非法、Snowflake 超出正 int64 / 早于 epoch、worker id 非法 |
| 403 | 服务未开 `--allow-trusted-import` 而请求带 `trusted_snowflake_id` |
| 404 | 分配关闭且存在未知 ObjectId；反查未知 Snowflake |
| 409 | 对象已绑定别的 Snowflake；Snowflake 已属于别的对象；entity_kind 不匹配 |
| 413 | 批量超过 `--max-batch-size` |
| 503 | Redis 不可用 / 超时、mapping version 或 storage schema 不匹配、元数据缺失 |
| 500 | 同秒序列耗尽、Redis 中记录损坏 |

切换完成的验收条件：

1. 同一 ObjectId 跨重启得到同一 Snowflake；
2. 单查与批查结果一致；
3. 用户映射命中内存缓存，30 天内帖子命中内存缓存；
4. 未知/非法映射 fail-closed；
5. xrex checkpoint、retrieval index 和 Registry 使用同一版本；
6. 对外响应仍能通过反向索引恢复原始 ObjectId。

当前状态：Registry crate 的 JSONL 遗留实现已删除；gRPC 是内部主协议，HTTP 仅作为旧客户端兼容与运维入口；Redis 版本拆为 `MappingStore` 存储端口、分片独立 key Redis adapter 和应用服务；服务入口支持单 Redis endpoint 与原生 Redis Cluster（连接与响应超时均显式配置），批量读写每轮一个 pipeline（Cluster 按 slot 分组并发），并使用有上限的双向本地缓存。全局 Snowflake 唯一性、两阶段写入、trusted 导入门禁、分配序列对已存在 Snowflake 的感知与有界重抽、mapping version 与 storage schema 元数据校验、只读 readiness、fail-closed 反查、批量写前整体校验（确定性冲突下整批不提交）、日志 / `/metrics` / 优雅停机、HTTP 层与应用层回归测试已补齐。开发阶段暂不处理旧 v1 Hash schema 兼容、反向 orphan 的离线修复工具和 Redis 故障切换后的历史数据恢复；上线前必须单独完成这三类验收。Home Mixer 领域模型以及 Thunder、VM Ranker、Phoenix/xrex 在线协议均使用数值 ID，ObjectId 只保留在公开 RPC、Redis、mrpyq 和事件边界。开发阶段先用 Redis 观察千万级映射的实际内存、命中率和吞吐，再决定是否引入 MongoDB 作为大规模冷数据主库。
