# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
Phoenix 精排服务 (独立部署)

提供候选物品评分排序的 HTTP API 接口。

架构：策略模式
    - 服务层 (本文件) 只持有 `RankingStrategy` 接口；
    - 具体打分由 `services.ranker_strategies.*` 实现；
    - 通过配置切换 phoenix / rule / deepctr / llm，主策略失败可降级到 fallback。

启动方式:
    # 开发模式（默认 phoenix 策略，无 checkpoint 时回落到随机初始化）
    python -m services.ranker_service

    # 使用规则兜底策略（零模型）
    RANKER_STRATEGY=rule python -m services.ranker_service

    # 主模型 + 规则兜底
    RANKER_STRATEGY=phoenix RANKER_FALLBACK_STRATEGY=rule \
        python -m services.ranker_service

    # 生产模式
    uvicorn services.ranker_service:app --host 0.0.0.0 --port 8081 --workers 1

主要环境变量:
    RANKER_PORT: 服务端口 (默认 8081)
    RANKER_STRATEGY: 主策略 (phoenix|rule|deepctr|llm)
    RANKER_FALLBACK_STRATEGY: 兜底策略名 (可选)
    RANKER_CHECKPOINT_PATH: phoenix/deepctr 策略的模型权重路径
    RANKER_RULE_POST_META: rule 策略可选的 post 元数据 parquet
    FEATURE_SERVICE_URL: 特征服务地址
    ENABLE_METRICS: 是否启用监控 (默认 true)
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import List, Optional

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# 修正导入路径
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners import ACTIONS

from services.config import RankerServiceConfig
from services.feature_store import FeatureStore, create_feature_store
from services.metrics import create_metrics_collector
from services.ranker_strategies import (
    RankingStrategy,
    StrategyError,
    create_strategy,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ranker_service")

# 全局配置
_config: Optional[RankerServiceConfig] = None

# 全局组件
_strategy: Optional[RankingStrategy] = None
_feature_store: Optional[FeatureStore] = None
_metrics = None


# ==================== Pydantic 模型 ====================


class RankRequest(BaseModel):
    """精排请求"""
    user_id: str
    candidate_ids: List[str]
    history_len: int = 32

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "user_12345",
                "candidate_ids": ["post_001", "post_002", "post_003"],
                "history_len": 32,
            }
        }


class CandidateScore(BaseModel):
    """单个候选得分"""
    candidate_id: str
    rank: int
    favorite_prob: float
    reply_prob: float
    repost_prob: float
    click_prob: float
    dwell_prob: float
    overall_score: float


class RankResponse(BaseModel):
    """精排响应"""
    user_id: str
    candidates: List[CandidateScore]
    inference_time_ms: float
    strategy: str
    model_version: Optional[str] = None


class HealthResponse(BaseModel):
    """健康检查"""
    status: str
    strategy_ready: bool
    feature_store_ready: bool
    strategy: Optional[str] = None


# ==================== 服务生命周期 ====================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global _config, _strategy, _feature_store, _metrics

    _config = RankerServiceConfig.from_env()
    logger.info(
        "Starting Ranker Service on %s:%s (strategy=%s, fallback=%s)",
        _config.host,
        _config.port,
        _config.strategy,
        _config.fallback_strategy,
    )

    _metrics = create_metrics_collector(
        "ranker",
        enabled=_config.enable_metrics,
        port=_config.metrics_port,
    )

    if _config.environment == "production" and _config.feature_backend == "mock":
        raise RuntimeError("production ranker requires a real feature backend")
    _feature_store = create_feature_store(
        _config.feature_backend,
        emb_size=_config.model.emb_size,
    )

    with _metrics.record_inference("init"):
        _strategy = create_strategy(
            name=_config.strategy,
            config=_config,
            feature_store=_feature_store,
            fallback=_config.fallback_strategy,
        )
        await _strategy.warmup()

    logger.info("Ranker Service ready (strategy=%s)", _strategy.name)

    yield

    logger.info("Shutting down Ranker Service...")
    if _strategy is not None:
        await _strategy.close()


app = FastAPI(
    title="Phoenix Ranker Service",
    description="候选物品精排服务（策略模式，支持 phoenix/rule/deepctr/llm 切换）",
    version="1.1.0",
    lifespan=lifespan,
)


# ==================== API 端点 ====================


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """健康检查"""
    return HealthResponse(
        status="healthy" if _strategy else "unhealthy",
        strategy_ready=_strategy is not None,
        feature_store_ready=_feature_store is not None,
        strategy=_strategy.name if _strategy else None,
    )


@app.get("/")
async def root():
    """服务信息"""
    return {
        "service": "Phoenix Ranker Service",
        "strategy": _strategy.name if _strategy else None,
        "docs": "/docs",
        "health": "/health",
        "rank": "POST /v1/rank",
    }


@app.post("/v1/rank", response_model=RankResponse)
async def rank_candidates(request: RankRequest):
    """
    精排接口

    对候选物品进行评分排序，返回按综合得分排序的结果。
    实际的打分策略由 `RANKER_STRATEGY` 决定。
    """
    if _strategy is None or _feature_store is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    start_time = time.time()

    with _metrics.record_request("POST", "/v1/rank"):
        _metrics.record_batch_size("ranker", len(request.candidate_ids))

        try:
            with _metrics.record_inference("ranker"):
                result = await _strategy.score(
                    user_id=request.user_id,
                    candidate_ids=request.candidate_ids,
                    history_len=request.history_len,
                )
        except StrategyError as exc:
            logger.exception("strategy failed: %s", exc)
            raise HTTPException(status_code=500, detail=f"ranking failed: {exc}") from exc

        candidates = _assemble_candidates(request.candidate_ids, result.scores, result.ranked_indices)

        inference_time = (time.time() - start_time) * 1000

        return RankResponse(
            user_id=request.user_id,
            candidates=candidates,
            inference_time_ms=inference_time,
            strategy=result.strategy_name,
            model_version=result.model_version,
        )


# ==================== 内部工具 ====================


# ACTIONS 索引（与 runners.ACTIONS 对齐，在模块加载时计算一次）
_IDX_FAVORITE = ACTIONS.index("favorite_score")
_IDX_REPLY = ACTIONS.index("reply_score")
_IDX_REPOST = ACTIONS.index("repost_score")
_IDX_CLICK = ACTIONS.index("click_score")
_IDX_DWELL = ACTIONS.index("dwell_score")


def _assemble_candidates(
    candidate_ids: List[str],
    scores: np.ndarray,
    ranked_indices: np.ndarray,
) -> List[CandidateScore]:
    """
    把 `StrategyResult.scores` (shape=[C, num_actions]) 组装为对外响应。

    - scores: [C, A]
    - ranked_indices: [C]（降序排列后的候选索引）
    """
    out: List[CandidateScore] = []
    for rank, idx in enumerate(ranked_indices):
        idx = int(idx)
        if idx < 0 or idx >= len(candidate_ids):
            continue

        row = scores[idx]
        overall = float(
            row[_IDX_FAVORITE] * 0.4
            + row[_IDX_REPLY] * 0.2
            + row[_IDX_REPOST] * 0.2
            + row[_IDX_CLICK] * 0.1
            + row[_IDX_DWELL] * 0.1
        )

        out.append(
            CandidateScore(
                candidate_id=candidate_ids[idx],
                rank=rank + 1,
                favorite_prob=float(row[_IDX_FAVORITE]),
                reply_prob=float(row[_IDX_REPLY]),
                repost_prob=float(row[_IDX_REPOST]),
                click_prob=float(row[_IDX_CLICK]),
                dwell_prob=float(row[_IDX_DWELL]),
                overall_score=overall,
            )
        )
    return out


if __name__ == "__main__":
    import uvicorn

    config = RankerServiceConfig.from_env()
    uvicorn.run(app, host=config.host, port=config.port)
