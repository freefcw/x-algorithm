# 版权所有 2026 X.AI Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。
# 您可以在以下网址获得许可证副本：
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# 除非适用法律要求或书面同意，否则根据许可证分发的软件
# 是按"原样"基础分发的，不附带任何形式明示或暗示的保证或条件。
# 请参阅许可证以了解管理权限和限制的特定语言。

"""
Phoenix 推荐模型推理服务 (FastAPI)

提供精排和召回的 HTTP API 接口。

启动方式:
    uvicorn api_server:app --host 0.0.0.0 --port 8080 --reload
    
或使用 gunicorn 多进程:
    gunicorn -k uvicorn.workers.UvicornWorker -w 1 api_server:app --bind 0.0.0.0:8080

注意: JAX 在 GPU 环境下建议使用单进程，多进程需要处理设备分配。
"""

import logging
from contextlib import asynccontextmanager
from typing import List, Optional

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from grok import TransformerConfig
from recsys_model import HashConfig, PhoenixModelConfig
from recsys_retrieval_model import PhoenixRetrievalModelConfig
from runners import (
    RecsysInferenceRunner,
    RecsysRetrievalInferenceRunner,
    ModelRunner,
    RetrievalModelRunner,
    create_example_batch,
    create_example_corpus,
    ACTIONS,
)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("phoenix_api")

# 全局模型实例
_ranker: Optional[RecsysInferenceRunner] = None
_retrieval: Optional[RecsysRetrievalInferenceRunner] = None


# ==================== Pydantic 请求/响应模型 ====================

class RankRequest(BaseModel):
    """精排请求"""
    user_id: str
    history_len: int = 32
    num_candidates: int = 8
    # 实际生产环境这里会有用户特征、候选列表等


class RankResult(BaseModel):
    """单个候选的排序结果"""
    candidate_idx: int
    favorite_prob: float
    reply_prob: float
    repost_prob: float
    click_prob: float
    dwell_prob: float


class RankResponse(BaseModel):
    """精排响应"""
    user_id: str
    ranked_results: List[RankResult]
    total_candidates: int


class RetrieveRequest(BaseModel):
    """召回请求"""
    user_id: str
    history_len: int = 32
    top_k: int = 10


class RetrieveResult(BaseModel):
    """单个召回结果"""
    rank: int
    post_id: int
    similarity: float


class RetrieveResponse(BaseModel):
    """召回响应"""
    user_id: str
    top_k: int
    results: List[RetrieveResult]


class HealthResponse(BaseModel):
    """健康检查响应"""
    status: str
    ranker_ready: bool
    retrieval_ready: bool


# ==================== 模型初始化 ====================

def _create_ranker() -> RecsysInferenceRunner:
    """创建并初始化精排模型"""
    emb_size = 128
    num_actions = len(ACTIONS)
    history_seq_len = 32
    candidate_seq_len = 8
    
    hash_config = HashConfig(
        num_user_hashes=2,
        num_item_hashes=2,
        num_author_hashes=2,
    )
    
    model_config = PhoenixModelConfig(
        emb_size=emb_size,
        num_actions=num_actions,
        history_seq_len=history_seq_len,
        candidate_seq_len=candidate_seq_len,
        hash_config=hash_config,
        product_surface_vocab_size=16,
        model=TransformerConfig(
            emb_size=emb_size,
            widening_factor=2,
            key_size=64,
            num_q_heads=2,
            num_kv_heads=2,
            num_layers=2,
            attn_output_multiplier=0.125,
        ),
    )
    
    runner = RecsysInferenceRunner(
        runner=ModelRunner(model=model_config, bs_per_device=0.125),
        name="ranker_api",
    )
    runner.initialize()
    logger.info("Ranker model initialized")
    return runner


def _create_retrieval() -> RecsysRetrievalInferenceRunner:
    """创建并初始化召回模型"""
    emb_size = 128
    num_actions = len(ACTIONS)
    history_seq_len = 32
    candidate_seq_len = 8
    
    hash_config = HashConfig(
        num_user_hashes=2,
        num_item_hashes=2,
        num_author_hashes=2,
    )
    
    model_config = PhoenixRetrievalModelConfig(
        emb_size=emb_size,
        history_seq_len=history_seq_len,
        candidate_seq_len=candidate_seq_len,
        hash_config=hash_config,
        product_surface_vocab_size=16,
        model=TransformerConfig(
            emb_size=emb_size,
            widening_factor=2,
            key_size=64,
            num_q_heads=2,
            num_kv_heads=2,
            num_layers=2,
            attn_output_multiplier=0.125,
        ),
    )
    
    runner = RecsysRetrievalInferenceRunner(
        runner=RetrievalModelRunner(model=model_config, bs_per_device=0.125),
        name="retrieval_api",
    )
    runner.initialize()
    
    # 加载模拟候选池 (生产环境这里会从向量数据库加载)
    corpus_size = 1000
    corpus_embeddings, corpus_post_ids = create_example_corpus(
        corpus_size=corpus_size, emb_size=emb_size, seed=456
    )
    runner.set_corpus(corpus_embeddings, corpus_post_ids)
    logger.info(f"Retrieval model initialized with corpus size={corpus_size}")
    
    return runner


# ==================== FastAPI 生命周期 ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global _ranker, _retrieval
    
    logger.info("Initializing Phoenix models...")
    _ranker = _create_ranker()
    _retrieval = _create_retrieval()
    logger.info("All models ready!")
    
    yield
    
    logger.info("Shutting down...")


app = FastAPI(
    title="Phoenix Recommendation API",
    description="推荐系统精排和召回服务",
    version="1.0.0",
    lifespan=lifespan,
)


# ==================== API 端点 ====================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """健康检查端点"""
    return HealthResponse(
        status="healthy",
        ranker_ready=_ranker is not None,
        retrieval_ready=_retrieval is not None,
    )


@app.post("/v1/rank", response_model=RankResponse)
async def rank_candidates(request: RankRequest):
    """
    精排接口：对候选集进行评分排序
    
    实际生产环境需要:
    1. 从特征服务获取用户 embedding
    2. 从候选服务获取待排序候选列表
    """
    if _ranker is None:
        raise HTTPException(status_code=503, detail="Ranker not initialized")
    
    # 创建模拟数据 (生产环境替换为真实特征)
    batch, embeddings = create_example_batch(
        batch_size=1,
        emb_size=_ranker.runner.model.emb_size,
        history_len=request.history_len,
        num_candidates=request.num_candidates,
        num_actions=len(ACTIONS),
        num_user_hashes=_ranker.runner.model.hash_config.num_user_hashes,
        num_item_hashes=_ranker.runner.model.hash_config.num_item_hashes,
        num_author_hashes=_ranker.runner.model.hash_config.num_author_hashes,
        product_surface_vocab_size=_ranker.runner.model.product_surface_vocab_size,
    )
    
    # 执行推理
    output = _ranker.rank(batch, embeddings)
    
    # 组装响应
    scores = np.array(output.scores[0])  # [num_candidates, num_actions]
    ranked_indices = np.array(output.ranked_indices[0])
    
    results = []
    for rank, idx in enumerate(ranked_indices):
        idx = int(idx)
        results.append(RankResult(
            candidate_idx=idx,
            favorite_prob=float(scores[idx, 0]),
            reply_prob=float(scores[idx, 1]),
            repost_prob=float(scores[idx, 2]),
            click_prob=float(scores[idx, 4]),
            dwell_prob=float(scores[idx, 10]),
        ))
    
    return RankResponse(
        user_id=request.user_id,
        ranked_results=results,
        total_candidates=request.num_candidates,
    )


@app.post("/v1/retrieve", response_model=RetrieveResponse)
async def retrieve_candidates(request: RetrieveRequest):
    """
    召回接口：从海量候选中检索 Top-K
    
    实际生产环境需要:
    1. 从特征服务获取用户 embedding
    2. 从向量索引(FAISS/Milvus)检索相似物品
    """
    if _retrieval is None:
        raise HTTPException(status_code=503, detail="Retrieval not initialized")
    
    # 创建模拟数据 (生产环境替换为真实特征)
    batch, embeddings = create_example_batch(
        batch_size=1,
        emb_size=_retrieval.runner.model.emb_size,
        history_len=request.history_len,
        num_candidates=_retrieval.runner.model.candidate_seq_len,
        num_actions=len(ACTIONS),
        num_user_hashes=_retrieval.runner.model.hash_config.num_user_hashes,
        num_item_hashes=_retrieval.runner.model.hash_config.num_item_hashes,
        num_author_hashes=_retrieval.runner.model.hash_config.num_author_hashes,
        product_surface_vocab_size=_retrieval.runner.model.product_surface_vocab_size,
    )
    
    # 执行检索
    retrieval_output = _retrieval.retrieve(batch, embeddings, top_k=request.top_k)
    
    # 组装响应
    top_k_indices = np.array(retrieval_output.top_k_indices[0])
    top_k_scores = np.array(retrieval_output.top_k_scores[0])
    
    results = []
    for rank in range(request.top_k):
        results.append(RetrieveResult(
            rank=rank + 1,
            post_id=int(top_k_indices[rank]),
            similarity=float(top_k_scores[rank]),
        ))
    
    return RetrieveResponse(
        user_id=request.user_id,
        top_k=request.top_k,
        results=results,
    )


@app.get("/")
async def root():
    """根路径提示"""
    return {
        "service": "Phoenix Recommendation API",
        "docs": "/docs",
        "endpoints": {
            "health": "/health",
            "rank": "POST /v1/rank",
            "retrieve": "POST /v1/retrieve",
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
