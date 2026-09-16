# syntax=docker/dockerfile:1.7
#
# Phoenix gRPC 网关镜像（services/grpc_gateway.py），供 home-mixer 调用精排 / 召回。
#
# 构建（以 phoenix/ 为上下文，phoenix/.dockerignore 排除 target/、checkpoints/、data/ 等）：
#   docker build -f deploy/docker/phoenix-gateway.Dockerfile -t phoenix-gateway:dev phoenix
#
# 运行（模型产物与召回索引通过卷挂载，路径走脚本已支持的环境变量）：
#   docker run --rm -p 50053:50053 -p 9093:9093 \
#     -v /srv/phoenix/models:/models:ro \
#     -e RANKER_CHECKPOINT_PATH=/models/ranker/step-000200 \
#     -e RETRIEVAL_CHECKPOINT_PATH=/models/retrieval/retrieval_params_step200.npz \
#     -e EMB_TABLES_PATH=/models/retrieval/embedding_tables.npz \
#     -e RETRIEVAL_CORPUS_PATH=/models/index/retrieval_index.npz \
#     -e RETRIEVAL_CORPUS_REFRESH_SECONDS=300 \
#     phoenix-gateway:dev
#
# 不挂模型时以随机权重启动，仅供本地跑通链路：非 demo 的 home-mixer 会拒绝
# random-weights=true 的响应。
#
# 依赖按 pyproject.toml 原样安装：Linux 下基础依赖是 jax[cuda12]，镜像因此带
# CUDA 运行库（体积数 GB）；没有 GPU 时 JAX 回退 CPU 并打一条告警。要做纯 CPU
# 小镜像需要在 pyproject 层面拆出 CPU 依赖组，属于单独决策。

FROM python:3.12-slim-bookworm AS runtime

# uv 二进制直接从官方镜像拷入，不经 pip。minor 版本要能读 uv.lock 的 revision
# （当前 lock 由 uv 0.12 生成）；升级本地 uv 后同步这里。
COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /uvx /usr/local/bin/

# 先建用户、以该用户安装：依赖层（含数 GB 的 JAX/CUDA）直接属于 phoenix，
# 不需要事后 chown -R —— 那会把整个 .venv 再复制进一个新层，镜像体积翻倍。
RUN useradd --system --uid 10001 --user-group --create-home phoenix \
 && mkdir -p /app && chown phoenix:phoenix /app
USER phoenix
WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_CACHE_DIR=/home/phoenix/.cache/uv \
    UV_NO_PROGRESS=1

# 依赖层：只拷 lock 与本地 path 依赖（xai-proto / xai-configlib / xai-gimmick 在
# python/ 下），源码变动不触发重装。--frozen 严格按 uv.lock，不重新解析。
COPY --chown=phoenix:phoenix pyproject.toml uv.lock ./
COPY --chown=phoenix:phoenix python ./python
RUN --mount=type=cache,target=/home/phoenix/.cache/uv,uid=10001,gid=10001 \
    uv sync --frozen --no-dev --group service --no-install-project

# 源码层：演示链路模型代码、services/、scripts/、proto 定义（.dockerignore 排除了
# .venv，不会覆盖上一层装好的环境）。
COPY --chown=phoenix:phoenix . .

# 容器内必须监听 0.0.0.0；脚本默认 127.0.0.1 只适合本机。
ENV PHOENIX_GRPC_HOST=0.0.0.0 \
    PHOENIX_GRPC_PORT=50053 \
    PHOENIX_METRICS_PORT=9093 \
    PATH="/app/.venv/bin:${PATH}"

# 50053 gRPC（含 grpc.health.v1，k8s 可用 grpc 探针）；9093 Prometheus /metrics。
EXPOSE 50053 9093

# 启动顺序：加载模型 → jit 预热 → 开 gRPC 端口并标记 SERVING；就绪 = 端口可连。
# 收到 SIGTERM 先把健康检查置 NOT_SERVING，再在 8 s 内排空（SHUTDOWN_GRACE_SECONDS）。
ENTRYPOINT ["python", "scripts/run_grpc_gateway.py"]
