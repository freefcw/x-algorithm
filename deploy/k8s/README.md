# deploy/k8s — Kubernetes 部署脚手架

> **状态：未验证脚手架。** 清单从代码中的真实端口与探针契约编写，但从未在真实
> 集群 apply 过；首次在 h201 上验证时按下面的清单逐项核对。

## 文件

| 文件 | 内容 | 端口 |
|---|---|---|
| `home-mixer.yaml` | 推荐服务 Deployment + gRPC Service | 50051 (gRPC)、9090 (admin) |
| `uas-worker.yaml` | 行为序列投影 job Deployment | 9091 (admin) |
| `phoenix-gateway.yaml` | 模型服务 Deployment + gRPC Service | 50053 (gRPC, 含 grpc.health.v1)、9093 (metrics) |

`thunder` 按主干计划不部署（整数 proto 无法承载真实 ObjectId），清单未提供。

## 首次在 h201 验证时的核对项

1. **镜像**：三份清单里的 `image:` 都是占位（`home-mixer:dev` / `phoenix-gateway:dev`），
   先构建并推送到 h201 可达的 registry（构建命令见 `deploy/docker/` 两个 Dockerfile 头部注释）。
2. **业务依赖地址**：
   - `MRPYQ_RECOMMENDATION_DATA_ADDR`：非 demo 模式硬依赖，缺失会启动失败；
   - Redis：单端点填 `HOME_MIXER_REDIS_URL`；原生集群填
     `HOME_MIXER_REDIS_CLUSTER_URLS`（逗号分隔种子 URL，此时 URL 可不填）。
     key 已按用户 hash-tag（`{user_id}`），pipeline/MULTI 天然单 slot；
   - Kafka：`UAS_KAFKA_BROKERS` / `UAS_KAFKA_TOPIC`（uas-worker 必填，否则停留在
     stdin 模式且永不就绪）；SASL/SSL 变量见 `docs/home-mixer/07-config-and-params.md`。
3. **探针**：home-mixer / uas-worker 走 HTTP（`/healthz` `/readyz`，管理端口）；
   phoenix-gateway 走 gRPC 探针（镜像实现了 `grpc.health.v1`）。
4. **优雅停机**：home-mixer `terminationGracePeriodSeconds`（30）必须大于
   `--shutdown-delay-secs + --drain-timeout-secs`（默认 0 + 20 s）；
   uas-worker 取 60 s（Kafka 消费位移提交）。
5. **Prometheus**：Pod annotation 已带 `prometheus.io/scrape`；如果你的采集器用
   ServiceMonitor / PodMonitor，改成对应 CRD。
6. **资源与副本数**：全部是占位值，按 h201 实测调整。

## 验证步骤（建议顺序）

```bash
kubectl apply -f deploy/k8s/phoenix-gateway.yaml
kubectl rollout status deploy/phoenix-gateway
kubectl apply -f deploy/k8s/home-mixer.yaml
kubectl rollout status deploy/home-mixer
# 冒烟：对 home-mixer Service 调一次 GetScoredPosts（demo-client 改地址）
kubectl apply -f deploy/k8s/uas-worker.yaml
kubectl rollout status deploy/uas-worker
# 检查三者的 /metrics 都能抓到（home_mixer_* / uas_worker_* / python 进程指标）
```

任何一项失败，优先看 Pod 事件与启动日志（`HOME_MIXER_LOG_FORMAT=json` 已默认）。
