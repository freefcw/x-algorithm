# syntax=docker/dockerfile:1.7
#
# home-mixer 镜像：同时包含推荐服务 `home-mixer` 与行为投影 job `uas-worker`。
#
# 构建（仓库根目录为上下文，根 .dockerignore 排除 phoenix/、target/ 等）：
#   docker build -f deploy/docker/home-mixer.Dockerfile -t home-mixer:dev .
#
# 运行推荐服务（配置全部走环境变量，见 docs/home-mixer/07-config-and-params.md §3）：
#   docker run --rm -p 50051:50051 -p 9090:9090 \
#     -e MRPYQ_RECOMMENDATION_DATA_ADDR=http://mrpyq:9000 \
#     -e HOME_MIXER_REDIS_URL=redis://redis:6379/ \
#     home-mixer:dev
#
# 运行投影 job（同一镜像，换入口）：
#   docker run --rm -p 9091:9091 \
#     -e HOME_MIXER_REDIS_URL=redis://redis:6379/ \
#     -e UAS_KAFKA_BROKERS=kafka:9092 -e UAS_KAFKA_TOPIC=uas-events \
#     --entrypoint /usr/local/bin/uas-worker home-mixer:dev
#
# 两个二进制都带 `kafka-ssl` feature：librdkafka 内置 OpenSSL，SSL / SASL_SSL /
# SCRAM 可用；本地开发用的 `--features kafka` 只能 PLAINTEXT。

# 与本地开发工具链同 minor（rustc 1.95）；Cargo.lock 由它生成，--locked 才能通过。
ARG RUST_VERSION=1.95

# ── 构建阶段 ──────────────────────────────────────────────────────────────────
FROM rust:${RUST_VERSION}-slim-bookworm AS builder

# protobuf-compiler：proto/build.rs 需要 protoc；
# build-essential + cmake：rdkafka-sys 从源码编译 librdkafka 与 zstd；
# zlib1g-dev / libssl-dev：librdkafka 的 libz 与 ssl feature 链接系统库。
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential cmake pkg-config protobuf-compiler zlib1g-dev libssl-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# registry / target 用 BuildKit cache mount：依赖与增量产物跨构建复用，
# 产物拷到 /out 以便运行阶段从非缓存路径取。
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p home-mixer --features kafka-ssl \
      --bin home-mixer --bin uas-worker \
 && mkdir -p /out \
 && cp target/release/home-mixer target/release/uas-worker /out/

# ── 运行阶段 ──────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# ca-certificates：SASL_SSL / SSL 连接 broker 校验证书；libssl3：kafka-ssl 的运行时依赖。
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libssl3 \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --user-group --create-home --home-dir /home/home-mixer home-mixer

COPY --from=builder /out/home-mixer /out/uas-worker /usr/local/bin/

USER home-mixer
WORKDIR /home/home-mixer

# 容器内默认结构化日志；RUST_LOG 仍控制级别。
ENV RUST_LOG=info \
    HOME_MIXER_LOG_FORMAT=json

# 50051 gRPC；9090 home-mixer 管理端口（/healthz /readyz /metrics）；
# 9091 uas-worker 管理端口（UAS_WORKER_METRICS_PORT）。
EXPOSE 50051 9090 9091

# k8s 探针：readinessProbe httpGet :9090/readyz，livenessProbe httpGet :9090/healthz。
# terminationGracePeriodSeconds 需大于 --shutdown-delay-secs + --drain-timeout-secs（默认 0 + 20）。
ENTRYPOINT ["/usr/local/bin/home-mixer"]
