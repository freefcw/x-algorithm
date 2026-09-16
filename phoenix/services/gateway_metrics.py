# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
gRPC 网关的 Prometheus 指标。

`services/metrics.py` 面向 FastAPI 的两个调试服务，指标名是通用的 ``request_*``；网关
是 home-mixer 主链路上的依赖，这里用独立注册表和 ``phoenix_gateway_`` 前缀，回答三个
问题：

1. 请求有没有进来、多久、以什么状态结束（``rpc_requests_total`` / ``rpc_duration_seconds``）；
2. 引擎是不是在排队——网关是 4 线程 + 引擎锁串行，``rpc_in_flight`` 高于 1 就说明请求在等锁，
   ``engine_duration_seconds`` 把"等锁 + 前向"的时间单独记出来；
3. 每次请求送了多少候选、召回返回多少、候选池现在多大。

注册表是显式实例而不是进程默认表，测试可以各建各的；``start_http_server`` 只在网关配置了
``--metrics-port`` 时调用。
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator, Optional

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Info,
    generate_latest,
    start_http_server,
)

# 与 home-mixer 侧 Phoenix 调用预算对齐：召回 3 s、精排 5 s。
RPC_DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
# home-mixer 一次送来的候选量：网内召回上限 400 + 网外 1000。
CANDIDATE_BUCKETS = (1, 8, 32, 64, 128, 256, 512, 1024, 2048)


class GatewayMetrics:
    """网关进程的全部指标；一个进程一个实例。"""

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry if registry is not None else CollectorRegistry()
        self.rpc_requests = Counter(
            "phoenix_gateway_rpc_requests_total",
            "Completed RPCs by method and terminal gRPC status code",
            ["rpc", "code"],
            registry=self.registry,
        )
        self.rpc_duration = Histogram(
            "phoenix_gateway_rpc_duration_seconds",
            "Wall-clock time from RPC entry to the response",
            ["rpc"],
            buckets=RPC_DURATION_BUCKETS,
            registry=self.registry,
        )
        self.rpc_in_flight = Gauge(
            "phoenix_gateway_rpc_in_flight",
            "RPCs currently executing (including those waiting on the engine lock)",
            ["rpc"],
            registry=self.registry,
        )
        self.engine_duration = Histogram(
            "phoenix_gateway_engine_duration_seconds",
            "Time spent in the ranker / retrieval engine call, lock wait included",
            ["engine"],
            buckets=RPC_DURATION_BUCKETS,
            registry=self.registry,
        )
        self.rank_candidates = Histogram(
            "phoenix_gateway_rank_candidates",
            "Candidates per PredictNextActions request",
            buckets=CANDIDATE_BUCKETS,
            registry=self.registry,
        )
        self.retrieval_returned = Histogram(
            "phoenix_gateway_retrieval_returned",
            "Candidates returned per Retrieve request",
            buckets=CANDIDATE_BUCKETS,
            registry=self.registry,
        )
        self.corpus_size = Gauge(
            "phoenix_gateway_corpus_size",
            "Posts in the retrieval corpus currently installed",
            registry=self.registry,
        )
        self.model_info = Info(
            "phoenix_gateway_model",
            "Model versions the gateway advertises in its serving metadata",
            registry=self.registry,
        )

    def set_model_versions(self, ranker_version: str, retrieval_version: str) -> None:
        self.model_info.info(
            {"ranker_version": ranker_version, "retrieval_version": retrieval_version}
        )

    @contextmanager
    def observe_rpc(self, rpc: str) -> Iterator[None]:
        """记一次 RPC：在途 +1/-1、耗时、终态码。异常按 gRPC 码（有 ``code()`` 时）或 UNKNOWN 计。"""
        self.rpc_in_flight.labels(rpc).inc()
        started = time.perf_counter()
        code = "OK"
        try:
            yield
        except BaseException as exc:  # noqa: BLE001 - 只为记账，原样重新抛出
            code = _status_name(exc)
            raise
        finally:
            self.rpc_duration.labels(rpc).observe(time.perf_counter() - started)
            self.rpc_requests.labels(rpc, code).inc()
            self.rpc_in_flight.labels(rpc).dec()

    @contextmanager
    def observe_engine(self, engine: str) -> Iterator[None]:
        started = time.perf_counter()
        try:
            yield
        finally:
            self.engine_duration.labels(engine).observe(time.perf_counter() - started)

    def exposition(self) -> bytes:
        return generate_latest(self.registry)

    def start_http_server(self, port: int, addr: str = "0.0.0.0") -> None:
        start_http_server(port, addr=addr, registry=self.registry)


def _status_name(exc: BaseException) -> str:
    """grpc.RpcError / abort 抛出的异常带 ``code()``；其他异常在 grpcio 里落成 UNKNOWN。"""
    code = getattr(exc, "code", None)
    if callable(code):
        try:
            value = code()
        except Exception:  # noqa: BLE001
            return "UNKNOWN"
        name = getattr(value, "name", None)
        if isinstance(name, str) and name:
            return name
    return "UNKNOWN"
