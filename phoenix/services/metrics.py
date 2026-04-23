# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
监控指标收集

基于 Prometheus 客户端，收集:
- 请求延迟 (P50/P99)
- 请求数量 (按状态码分类)
- 模型推理时间
- 批处理大小
"""

import functools
import logging
import time
from contextlib import contextmanager
from typing import Callable, Optional

# 可选导入，如果没有安装 prometheus_client 则使用空实现
try:
    from prometheus_client import Counter, Histogram, Info, start_http_server, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logging.getLogger("metrics").warning("prometheus_client not installed, metrics disabled")

logger = logging.getLogger("metrics")


class MetricsCollector:
    """指标收集器"""
    
    def __init__(self, service_name: str, enabled: bool = True, port: int = 9090):
        self.service_name = service_name
        self.enabled = enabled and PROMETHEUS_AVAILABLE
        self.port = port
        self.registry = CollectorRegistry() if self.enabled else None
        
        if self.enabled:
            self._init_metrics()
            self._start_server()
    
    def _init_metrics(self) -> None:
        """初始化指标"""
        # 请求延迟
        self.request_latency = Histogram(
            "request_latency_seconds",
            "Request latency",
            ["method", "endpoint", "status"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            registry=self.registry,
        )
        
        # 请求总数
        self.request_count = Counter(
            "request_total",
            "Total requests",
            ["method", "endpoint", "status"],
            registry=self.registry,
        )
        
        # 推理延迟
        self.inference_latency = Histogram(
            "inference_latency_seconds",
            "Model inference latency",
            ["model_type"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
            registry=self.registry,
        )
        
        # 批处理大小
        self.batch_size = Histogram(
            "batch_size",
            "Batch size distribution",
            ["model_type"],
            buckets=[1, 2, 4, 8, 16, 32, 64, 128],
            registry=self.registry,
        )
        
        # 服务信息
        self.service_info = Info(
            "service_info",
            "Service information",
            registry=self.registry,
        )
        self.service_info.info({"name": self.service_name, "version": "1.0.0"})
        
        logger.info(f"Initialized metrics for {self.service_name}")
    
    def _start_server(self) -> None:
        """启动 Prometheus HTTP 服务器"""
        if self.enabled:
            start_http_server(self.port, registry=self.registry)
            logger.info(f"Metrics server started on port {self.port}")
    
    @contextmanager
    def record_request(self, method: str, endpoint: str):
        """记录请求延迟的上下文管理器"""
        start = time.time()
        status = "error"
        try:
            yield
            status = "success"
        except Exception:
            status = "error"
            raise
        finally:
            if self.enabled:
                latency = time.time() - start
                self.request_latency.labels(method, endpoint, status).observe(latency)
                self.request_count.labels(method, endpoint, status).inc()
    
    @contextmanager
    def record_inference(self, model_type: str):
        """记录推理时间的上下文管理器"""
        start = time.time()
        try:
            yield
        finally:
            if self.enabled:
                latency = time.time() - start
                self.inference_latency.labels(model_type).observe(latency)
    
    def record_batch_size(self, model_type: str, size: int) -> None:
        """记录批处理大小"""
        if self.enabled:
            self.batch_size.labels(model_type).observe(size)


class NoOpMetricsCollector:
    """空实现的指标收集器 (当 prometheus 不可用或禁用时)"""
    
    def __init__(self, *args, **kwargs):
        pass
    
    @contextmanager
    def record_request(self, method: str, endpoint: str):
        yield
    
    @contextmanager
    def record_inference(self, model_type: str):
        yield
    
    def record_batch_size(self, model_type: str, size: int) -> None:
        pass


def create_metrics_collector(
    service_name: str,
    enabled: bool = True,
    port: int = 9090,
) -> MetricsCollector:
    """工厂函数创建指标收集器"""
    if not enabled or not PROMETHEUS_AVAILABLE:
        return NoOpMetricsCollector()
    return MetricsCollector(service_name, enabled, port)
