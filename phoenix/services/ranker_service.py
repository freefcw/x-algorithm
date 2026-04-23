# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
Phoenix 精排服务 (独立部署)

提供候选物品评分排序的 HTTP API 接口。

启动方式:
    # 开发模式
    python -m services.ranker_service
    
    # 生产模式
    uvicorn services.ranker_service:app --host 0.0.0.0 --port 8081 --workers 1

环境变量:
    RANKER_PORT: 服务端口 (默认 8081)
    RANKER_CHECKPOINT_PATH: 模型权重路径
    FEATURE_SERVICE_URL: 特征服务地址
    ENABLE_METRICS: 是否启用监控 (默认 true)
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List, Optional

import jax
import jax.numpy as jnp
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# 修正导入路径
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from grok import TransformerConfig
from recsys_model import PhoenixModelConfig, HashConfig
from runners import RecsysInferenceRunner, ModelRunner, ACTIONS

from services.config import RankerServiceConfig
from services.feature_store import create_feature_store, FeatureStore
from services.model_registry import create_model_registry
from services.metrics import create_metrics_collector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ranker_service")

# 全局配置
_config: Optional[RankerServiceConfig] = None

# 全局组件
_ranker: Optional[RecsysInferenceRunner] = None
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
    model_version: Optional[str] = None


class HealthResponse(BaseModel):
    """健康检查"""
    status: str
    model_ready: bool
    feature_store_ready: bool
    model_version: Optional[str] = None


# ==================== 服务初始化 ====================

def _init_model(config: RankerServiceConfig) -> RecsysInferenceRunner:
    """初始化精排模型"""
    logger.info("Initializing ranker model...")
    
    mc = config.model
    hash_config = HashConfig(
        num_user_hashes=mc.num_user_hashes,
        num_item_hashes=mc.num_item_hashes,
        num_author_hashes=mc.num_author_hashes,
    )
    
    model_config = PhoenixModelConfig(
        emb_size=mc.emb_size,
        num_actions=len(ACTIONS),
        history_seq_len=mc.history_seq_len,
        candidate_seq_len=max(mc.candidate_seq_len, config.max_batch_size),
        hash_config=hash_config,
        product_surface_vocab_size=mc.product_surface_vocab_size,
        model=TransformerConfig(
            emb_size=mc.emb_size,
            widening_factor=mc.widening_factor,
            key_size=mc.key_size,
            num_q_heads=mc.num_q_heads,
            num_kv_heads=mc.num_kv_heads,
            num_layers=mc.num_layers,
            attn_output_multiplier=mc.attn_output_multiplier,
        ),
    )
    
    runner = RecsysInferenceRunner(
        runner=ModelRunner(model=model_config, bs_per_device=0.125),
        name="ranker_prod",
    )
    runner.initialize()
    
    # 加载 checkpoint (如果提供)
    registry = create_model_registry(config.checkpoint_path, allow_random_init=True)
    if registry.get_params() is not None:
        runner.params = registry.get_params()
        logger.info("Loaded model from checkpoint")
    else:
        logger.warning("Using random initialization (no checkpoint provided)")
    
    return runner


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global _config, _ranker, _feature_store, _metrics
    
    # 加载配置
    _config = RankerServiceConfig.from_env()
    logger.info(f"Starting Ranker Service on {_config.host}:{_config.port}")
    
    # 初始化监控
    _metrics = create_metrics_collector(
        "ranker",
        enabled=_config.enable_metrics,
        port=_config.metrics_port,
    )
    
    # 初始化特征存储
    _feature_store = create_feature_store("mock", emb_size=_config.model.emb_size)
    
    # 初始化模型 (可能耗时)
    with _metrics.record_inference("init"):
        _ranker = _init_model(_config)
    
    logger.info("Ranker Service ready!")
    
    yield
    
    logger.info("Shutting down Ranker Service...")


app = FastAPI(
    title="Phoenix Ranker Service",
    description="候选物品精排服务",
    version="1.0.0",
    lifespan=lifespan,
)


# ==================== API 端点 ====================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """健康检查"""
    registry = create_model_registry(_config.checkpoint_path if _config else None)
    version = registry.current_version.version if registry.current_version else None
    
    return HealthResponse(
        status="healthy" if _ranker else "unhealthy",
        model_ready=_ranker is not None,
        feature_store_ready=_feature_store is not None,
        model_version=version,
    )


@app.get("/")
async def root():
    """服务信息"""
    return {
        "service": "Phoenix Ranker Service",
        "docs": "/docs",
        "health": "/health",
        "rank": "POST /v1/rank",
    }


@app.post("/v1/rank", response_model=RankResponse)
async def rank_candidates(request: RankRequest):
    """
    精排接口
    
    对候选物品进行评分排序，返回按综合得分排序的结果。
    """
    if _ranker is None or _feature_store is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    import time
    start_time = time.time()
    
    with _metrics.record_request("POST", "/v1/rank"):
        # 构建输入数据
        batch, embeddings = await _feature_store.build_recsys_batch(
            user_id=request.user_id,
            candidate_ids=request.candidate_ids,
            history_len=request.history_len,
            num_actions=len(ACTIONS),
            num_user_hashes=_config.model.num_user_hashes,
            num_item_hashes=_config.model.num_item_hashes,
            num_author_hashes=_config.model.num_author_hashes,
            product_surface_vocab_size=_config.model.product_surface_vocab_size,
        )
        
        # 记录批大小
        _metrics.record_batch_size("ranker", len(request.candidate_ids))
        
        # 执行推理
        with _metrics.record_inference("ranker"):
            output = _ranker.rank(batch, embeddings)
        
        # 解析结果
        scores = np.array(output.scores[0])  # [num_candidates, num_actions]
        ranked_indices = np.array(output.ranked_indices[0])
        
        # 组装响应
        candidates = []
        for rank, idx in enumerate(ranked_indices):
            idx = int(idx)
            cand_id = request.candidate_ids[idx]
            
            # 计算综合得分 (可配置权重)
            overall = (
                scores[idx, 0] * 0.4 +   # favorite
                scores[idx, 1] * 0.2 +   # reply
                scores[idx, 2] * 0.2 +   # repost
                scores[idx, 4] * 0.1 +   # click
                scores[idx, 10] * 0.1   # dwell
            )
            
            candidates.append(CandidateScore(
                candidate_id=cand_id,
                rank=rank + 1,
                favorite_prob=float(scores[idx, 0]),
                reply_prob=float(scores[idx, 1]),
                repost_prob=float(scores[idx, 2]),
                click_prob=float(scores[idx, 4]),
                dwell_prob=float(scores[idx, 10]),
                overall_score=float(overall),
            ))
        
        inference_time = (time.time() - start_time) * 1000
        
        registry = create_model_registry(_config.checkpoint_path)
        version = registry.current_version.version if registry.current_version else "random"
        
        return RankResponse(
            user_id=request.user_id,
            candidates=candidates,
            inference_time_ms=inference_time,
            model_version=version,
        )


if __name__ == "__main__":
    import uvicorn
    config = RankerServiceConfig.from_env()
    uvicorn.run(app, host=config.host, port=config.port)
