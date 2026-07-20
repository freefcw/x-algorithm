#!/usr/bin/env python3
"""
Phoenix gRPC 网关启动脚本。

实现 recsys.proto 的精排 / 召回 gRPC 服务，供 home-mixer 直接调用。

用法:
    # 随机权重（演示，无需任何文件）
    uv run scripts/run_grpc_gateway.py

    # 加载训练产物（scripts/train_ranker.py 的输出）
    uv run scripts/run_grpc_gateway.py \
        --ranker-checkpoint checkpoints/model_params_step200.npz \
        --emb-tables checkpoints/embedding_tables.npz

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
        "--port",
        type=int,
        default=int(os.getenv("PHOENIX_GRPC_PORT", "50053")),
        help="gRPC 监听端口（默认 50053）",
    )
    parser.add_argument(
        "--ranker-checkpoint",
        default=os.getenv("RANKER_CHECKPOINT_PATH"),
        help="精排模型检查点（train_ranker.py 产出的 model_params_step*.npz）",
    )
    parser.add_argument(
        "--retrieval-checkpoint",
        default=os.getenv("RETRIEVAL_CHECKPOINT_PATH"),
        help="召回模型检查点（train_retrieval.py 产出的 retrieval_params_step*.npz）",
    )
    parser.add_argument(
        "--emb-tables",
        default=os.getenv("EMB_TABLES_PATH"),
        help="嵌入表文件（embedding_tables.npz），不传则随机初始化",
    )
    parser.add_argument(
        "--corpus-size",
        type=int,
        default=2000,
        help="演示候选池大小（默认 2000）",
    )
    args = parser.parse_args()

    from services.grpc_gateway import serve

    serve(
        port=args.port,
        ranker_checkpoint=args.ranker_checkpoint,
        retrieval_checkpoint=args.retrieval_checkpoint,
        emb_tables_path=args.emb_tables,
        corpus_size=args.corpus_size,
    )


if __name__ == "__main__":
    main()
