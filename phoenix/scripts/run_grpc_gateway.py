#!/usr/bin/env python3
"""
Phoenix gRPC 网关启动脚本。

实现 proto/definitions/phoenix_recsys.proto 的精排 / 召回 gRPC 服务，供 home-mixer 调用。

用法:
    # 随机权重（演示，无需任何文件）
    uv run scripts/run_grpc_gateway.py

    # 加载本地训练产物（scripts/train_ranker.py 的输出）
    uv run scripts/run_grpc_gateway.py \
        --ranker-checkpoint checkpoints/step-000200

    # 真实候选池：离线索引 + 每 5 分钟检查一次文件是否被重建
    uv run scripts/run_grpc_gateway.py \
        --retrieval-checkpoint checkpoints_retrieval/retrieval_params_step200.npz \
        --emb-tables checkpoints_retrieval/embedding_tables.npz \
        --corpus-path indexes/retrieval_index.npz \
        --corpus-refresh-seconds 300

    # 暴露 Prometheus 指标（默认关闭）
    uv run scripts/run_grpc_gateway.py --metrics-port 9093

依赖:
    uv sync --group service   # 包含 grpcio / grpcio-tools
"""

import _setup_path  # noqa: F401

import argparse
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    parser = argparse.ArgumentParser(description="Phoenix gRPC gateway")
    parser.add_argument(
        "--host",
        default=os.getenv("PHOENIX_GRPC_HOST", "127.0.0.1"),
        help="gRPC 监听地址（默认 127.0.0.1；可用 PHOENIX_GRPC_HOST 修改）",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("PHOENIX_GRPC_PORT", "50053")),
        help="gRPC 监听端口（默认 50053）",
    )
    parser.add_argument(
        "--ranker-checkpoint",
        default=os.getenv("RANKER_CHECKPOINT_PATH"),
        help="精排 checkpoint 文件或 bundle 目录（推荐使用 train_ranker.py 产出的 step-* 目录）",
    )
    parser.add_argument(
        "--retrieval-checkpoint",
        default=os.getenv("RETRIEVAL_CHECKPOINT_PATH"),
        help="召回模型检查点（train_retrieval.py 产出的 retrieval_params_step*.npz）",
    )
    parser.add_argument(
        "--emb-tables",
        default=os.getenv("EMB_TABLES_PATH"),
        help="旧格式嵌入表文件；使用 bundle 目录时不要传此参数",
    )
    parser.add_argument(
        "--corpus-size",
        type=int,
        default=2000,
        help="演示候选池大小（默认 2000）；提供 --corpus-path 时忽略",
    )
    parser.add_argument(
        "--corpus-path",
        default=os.getenv("RETRIEVAL_CORPUS_PATH"),
        help=(
            "scripts/build_retrieval_index.py 产出的召回索引（.npz）。"
            "不传则合成演示候选池，召回 ID 在业务侧水合不到"
        ),
    )
    parser.add_argument(
        "--corpus-refresh-seconds",
        type=float,
        default=float(os.getenv("RETRIEVAL_CORPUS_REFRESH_SECONDS", "0")),
        help="索引文件热替换的检查周期（秒）；0 表示只在启动时加载一次",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=int(os.getenv("PHOENIX_METRICS_PORT", "0")),
        help="Prometheus /metrics 端口（监听 0.0.0.0；默认 0 不开启，生产建议 9093）",
    )
    args = parser.parse_args()
    if args.corpus_refresh_seconds < 0:
        parser.error("--corpus-refresh-seconds 不能为负数")
    if args.corpus_refresh_seconds > 0 and not args.corpus_path:
        parser.error("--corpus-refresh-seconds 需要同时提供 --corpus-path")
    if args.metrics_port < 0 or args.metrics_port > 65535:
        parser.error("--metrics-port 必须在 0..65535 之间")

    from services.grpc_gateway import serve

    serve(
        host=args.host,
        port=args.port,
        ranker_checkpoint=args.ranker_checkpoint,
        retrieval_checkpoint=args.retrieval_checkpoint,
        emb_tables_path=args.emb_tables,
        corpus_size=args.corpus_size,
        corpus_path=args.corpus_path,
        corpus_refresh_seconds=args.corpus_refresh_seconds,
        metrics_port=args.metrics_port,
    )


if __name__ == "__main__":
    main()
