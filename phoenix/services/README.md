# Phoenix 拆分服务架构

本目录包含 Phoenix 推荐系统的生产化拆分服务实现。

## 架构概览

```
┌─────────────────────────────────────────────────────────────────┐
│                     API Gateway / Load Balancer                  │
│                     (Nginx / Traefik / AWS ALB)                │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              │                               │
              ▼                               ▼
┌─────────────────────────────┐    ┌─────────────────────────────┐
│    Ranker Service           │    │    Retrieval Service        │
│    (精排服务)                │    │    (召回服务)              │
│    Port: 8081               │    │    Port: 8082               │
│    Metrics: 9091            │    │    Metrics: 9092            │
├─────────────────────────────┤    ├─────────────────────────────┤
│  • 候选物品评分排序          │    │  • 从海量候选中检索 Top-K   │
│  • 多行为预测 (点赞/回复等)  │    │  • 用户向量编码             │
│  • 多哈希特征融合           │    │  • 向量索引 (FAISS/Milvus)  │
│  • 候选隔离 Attention        │    │  • 候选池热更新              │
└──────────────┬──────────────┘    └──────────────┬──────────────┘
               │                                  │
               └──────────────┬───────────────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │   Feature Store     │
                    │   (特征服务)         │
                    │   Mock/Redis/定制    │
                    └─────────────────────┘
```

## 服务拆分优势

| 维度 | 单体服务 | 拆分服务 |
|------|----------|----------|
| **扩容策略** | 整体扩容 | 召回可独立高副本，精排按 GPU 限制 |
| **资源隔离** | 共享资源 | 召回用 CPU，精排用 GPU |
| **迭代速度** | 耦合发布 | 独立部署，互不影响 |
| **故障域** | 单点故障 | 召回失效可降级为全量候选 |

## 文件结构

```
services/
├── __init__.py              # 包初始化
├── config.py                # 统一配置管理
├── feature_store.py         # 特征存储抽象
├── model_registry.py        # Checkpoint 加载/热更新
├── metrics.py               # Prometheus 监控指标
├── ranker_service.py        # 精排服务主入口
├── retrieval_service.py     # 召回服务主入口
└── README.md               # 本文档
```

## 快速启动

以下命令在 `phoenix/` 目录下执行。

### 1. 安装依赖

```bash
uv sync --group service
```

### 2. 启动单个服务

**精排服务:**
```bash
uv run scripts/run_services.py ranker
```

**召回服务:**
```bash
uv run scripts/run_services.py retrieval
```

### 3. 开发模式 (同时启动两个服务)

```bash
uv run scripts/run_services.py all
```

服务将启动在:
- Ranker: http://localhost:8081
- Retrieval: http://localhost:8082
- Ranker Metrics: http://localhost:9091
- Retrieval Metrics: http://localhost:9092

### 4. 加载 Checkpoint (可选)

Checkpoint 是 HTTP 服务对应策略产出的 `.npz` 文件：

```bash
uv run scripts/run_services.py ranker --ranker-checkpoint ./checkpoints/model_params_step200.npz
uv run scripts/run_services.py retrieval --retrieval-checkpoint ./checkpoints/retrieval_params_step200.npz
```

## gRPC 网关（供 home-mixer 调用）

上面的 HTTP 服务面向人工调试和外部系统。推荐主链路中，home-mixer（Rust）通过
`proto/definitions/phoenix_recsys.proto` 定义的 gRPC 协议调用 Phoenix，对应服务是：

```bash
uv run scripts/run_grpc_gateway.py                # 随机权重，监听 50053（供 home-mixer 调用）
uv run scripts/run_grpc_gateway.py \
    --ranker-checkpoint checkpoints/step-000200  # 加载完整 checkpoint bundle
uv run scripts/run_grpc_gateway.py --metrics-port 9093   # 另开 Prometheus /metrics（phoenix_gateway_* 指标）
```

容器镜像见仓库 `deploy/docker/phoenix-gateway.Dockerfile`（以 `phoenix/` 为构建上下文）。

实现见 `services/grpc_gateway.py`：真正消费请求里的用户行为序列构造模型输入，
候选池在启动时合成并用候选塔编码（生产环境应替换为离线向量索引）。
端到端用法见仓库的 [极简 Phoenix 执行计划](../../docs/implementation/slim-phoenix-recommendation.md)。

## API 接口

### 精排服务 (Port 8081)

#### POST /v1/rank
对候选物品进行评分排序。

**Request:**
```json
{
  "user_id": "user_12345",
  "candidate_ids": ["post_001", "post_002", "post_003"],
  "history_len": 32
}
```

**Response:**
```json
{
  "user_id": "user_12345",
  "candidates": [
    {
      "candidate_id": "post_002",
      "rank": 1,
      "favorite_prob": 0.85,
      "reply_prob": 0.23,
      "repost_prob": 0.15,
      "click_prob": 0.92,
      "dwell_prob": 0.78,
      "overall_score": 0.56
    }
  ],
  "inference_time_ms": 15.3,
  "model_version": "1712812345"
}
```

#### GET /health
健康检查，返回服务状态和模型版本。

### 召回服务 (Port 8082)

#### POST /v1/retrieve
从海量候选中检索 Top-K 物品。

**Request:**
```json
{
  "user_id": "user_12345",
  "top_k": 100,
  "history_len": 32
}
```

**Response:**
```json
{
  "user_id": "user_12345",
  "top_k": 100,
  "results": [
    {"rank": 1, "post_id": "789", "similarity": 0.92},
    {"rank": 2, "post_id": "456", "similarity": 0.87}
  ],
  "corpus_size": 10000,
  "inference_time_ms": 8.5,
  "model_version": "1712812345"
}
```

#### POST /v1/encode_user
编码用户特征为向量 (用于离线预计算)。

#### GET /health
健康检查。

## 配置说明

通过环境变量配置服务:

### 精排服务

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `RANKER_PORT` | 8081 | 服务端口 |
| `RANKER_METRICS_PORT` | 9091 | 监控端口 |
| `RANKER_CHECKPOINT_PATH` | None | Checkpoint 路径 |
| `RANKER_MAX_BATCH` | 32 | 最大批处理大小 |
| `ENABLE_METRICS` | true | 是否启用 Prometheus |

### 召回服务

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `RETRIEVAL_PORT` | 8082 | 服务端口 |
| `RETRIEVAL_METRICS_PORT` | 9092 | 监控端口 |
| `RETRIEVAL_CHECKPOINT_PATH` | None | Checkpoint 路径 |
| `VECTOR_INDEX_TYPE` | faiss | 向量索引类型 |
| `FAISS_INDEX_PATH` | None | FAISS 索引文件路径 |
| `MILVUS_HOST` | localhost | Milvus 地址 |
| `CORPUS_REFRESH_INTERVAL` | 300 | 候选池刷新间隔(秒) |

## 生产化改造清单

当前实现使用 Mock 特征存储，生产环境需要替换:

### 1. 特征存储 (feature_store.py)

- [ ] 实现 `RedisFeatureStore`: 从 Redis 读取预计算 embedding
- [ ] 实现 `FeatureStoreClient`: 对接企业级特征平台 (Feature Store)
- [ ] 添加缓存层: 高频用户特征本地缓存

### 2. 向量索引 (retrieval_service.py)

- [ ] FAISS 集成: 使用 `faiss.read_index()` 加载 IVF/PQ 索引
- [ ] Milvus 集成: 使用 `pymilvus` 客户端
- [ ] 索引热更新: 定时刷新候选池，支持蓝绿切换

### 3. Checkpoint 管理 (model_registry.py)

- [ ] 支持 S3/GCS 路径: 使用 `boto3`/`google-cloud-storage`
- [ ] 版本管理: A/B 测试支持，流量灰度
- [ ] 热更新: 无停机模型更新

### 4. 部署方式

**Docker:**
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install uv && uv sync --group service
EXPOSE 8081 9091
CMD ["uv", "run", "scripts/run_services.py", "ranker"]
```

**Kubernetes:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: phoenix-ranker
spec:
  replicas: 2
  selector:
    matchLabels:
      app: phoenix-ranker
  template:
    spec:
      containers:
      - name: ranker
        image: phoenix:ranker-v1.0
        ports:
        - containerPort: 8081
        - containerPort: 9091
        env:
        - name: RANKER_CHECKPOINT_PATH
          value: "/models/model_params_step200.npz"
```

### 5. 监控告警

Prometheus 指标:
- `request_latency_seconds`: P50/P99 延迟
- `inference_latency_seconds`: 模型推理时间
- `batch_size`: 批处理大小分布

Grafana Dashboard 推荐维度:
- 按 user_id 分桶的延迟热力图
- 模型版本切换事件标注
- 召回率 vs 候选池大小趋势

## 性能优化建议

1. **动态批处理**: 当前为单请求处理，生产环境应实现请求合并 (Batcher)
2. **GPU 利用率**: JAX 模型使用 `jax.pmap` 支持多 GPU
3. **特征预热**: 服务启动时预加载高频用户特征
4. **向量缓存**: 召回服务缓存近期查询结果 (类似近似近邻的 Cache)

## 开发测试

```bash
# 启动所有服务
uv run scripts/run_services.py all

# 另一个终端调用 API
curl -X POST http://localhost:8081/v1/rank \
  -H "Content-Type: application/json" \
  -d '{"user_id": "test", "candidate_ids": ["a", "b", "c"]}'

curl -X POST http://localhost:8082/v1/retrieve \
  -H "Content-Type: application/json" \
  -d '{"user_id": "test", "top_k": 10}'
```

## 架构决策记录 (ADR)

### 为什么选择 FastAPI + Uvicorn?
- **生态**: Python ML 社区主流选择
- **性能**: ASGI 异步，支持高并发
- **类型**: Pydantic 模型自动生成 OpenAPI 文档

### 为什么拆分服务?
- **扩容**: 召回 QPS 远高于精排，需要独立扩容
- **资源**: 召回可用 CPU 集群，精排需要 GPU
- **延迟**: 召回允许 50-100ms，精排要求 <20ms

### 为什么使用 Mock 特征存储?
- **演示**: 避免外部依赖，开箱即用
- **抽象**: 接口清晰，易于替换为真实实现
