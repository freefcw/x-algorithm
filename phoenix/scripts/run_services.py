#!/usr/bin/env python3
"""
Phoenix 服务启动脚本

支持启动单个服务或同时启动多个服务 (用于开发)。

用法:
    python run_services.py ranker      # 启动精排服务
    python run_services.py retrieval   # 启动召回服务
    python run_services.py all         # 同时启动两个服务 (开发模式)
"""

import _setup_path  # noqa: F401

import argparse
import logging
import multiprocessing
import os
import sys
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("run_services")


def run_ranker():
    """运行精排服务"""
    logger.info("Starting Ranker Service...")
    os.environ.setdefault("RANKER_PORT", "8081")
    os.environ.setdefault("RANKER_METRICS_PORT", "9091")
    
    from services.ranker_service import app
    import uvicorn
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ["RANKER_PORT"]),
        log_level="info",
    )


def run_retrieval():
    """运行召回服务"""
    logger.info("Starting Retrieval Service...")
    os.environ.setdefault("RETRIEVAL_PORT", "8082")
    os.environ.setdefault("RETRIEVAL_METRICS_PORT", "9092")
    
    from services.retrieval_service import app
    import uvicorn
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ["RETRIEVAL_PORT"]),
        log_level="info",
    )


def run_all():
    """同时启动两个服务 (开发模式)"""
    logger.info("Starting all services in development mode...")
    
    # 使用进程启动两个服务
    processes = []
    
    ranker_proc = multiprocessing.Process(target=run_ranker)
    retrieval_proc = multiprocessing.Process(target=run_retrieval)
    
    processes.append(ranker_proc)
    processes.append(retrieval_proc)
    
    for p in processes:
        p.start()
        time.sleep(2)  # 等待第一个服务启动
    
    logger.info("\n" + "=" * 60)
    logger.info("All services started!")
    logger.info("Ranker:     http://localhost:8081")
    logger.info("Retrieval:  http://localhost:8082")
    logger.info("Ranker Metrics:    http://localhost:9091")
    logger.info("Retrieval Metrics: http://localhost:9092")
    logger.info("=" * 60 + "\n")
    
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("\nShutting down all services...")
        for p in processes:
            p.terminate()
            p.join()


def main():
    parser = argparse.ArgumentParser(description="Phoenix Services")
    parser.add_argument(
        "service",
        choices=["ranker", "retrieval", "all"],
        help="Which service to start",
    )
    parser.add_argument(
        "--ranker-checkpoint",
        help="Path to ranker checkpoint",
    )
    parser.add_argument(
        "--retrieval-checkpoint",
        help="Path to retrieval checkpoint",
    )
    
    args = parser.parse_args()
    
    if args.ranker_checkpoint:
        os.environ["RANKER_CHECKPOINT_PATH"] = args.ranker_checkpoint
    if args.retrieval_checkpoint:
        os.environ["RETRIEVAL_CHECKPOINT_PATH"] = args.retrieval_checkpoint
    
    if args.service == "ranker":
        run_ranker()
    elif args.service == "retrieval":
        run_retrieval()
    elif args.service == "all":
        run_all()


if __name__ == "__main__":
    main()
