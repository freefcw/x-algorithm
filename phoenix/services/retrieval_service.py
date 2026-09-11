# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
Phoenix 召回服务 (独立部署)

提供从海量候选中检索 Top-K 物品的 HTTP API 接口。

启动方式:
    # 开发模式
    python -m services.retrieval_service
    
    # 生产模式
    uvicorn services.retrieval_service:app --host 0.0.0.0 --port 8082 --workers 1

环境变量:
    RETRIEVAL_PORT: 服务端口 (默认 8082)
    RETRIEVAL_CHECKPOINT_PATH: 模型权重路径
    FAISS_INDEX_PATH: FAISS 索引文件路径
    FEATURE_SERVICE_URL: 特征服务地址
    ENABLE_METRICS: 是否启用监控 (默认 true)
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List, Optional, Tuple

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
from recsys_retrieval_model import PhoenixRetrievalModelConfig
from recsys_model import HashConfig
from runners import (
    RecsysRetrievalInferenceRunner,
    RetrievalModelRunner,
    ACTIONS,
    create_example_corpus,
)

from services.config import RetrievalServiceConfig
from services.feature_store import create_feature_store, FeatureStore
from services.model_registry import create_model_registry
from services.metrics import create_metrics_collector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("retrieval_service")

# 全局配置
_config: Optional[RetrievalServiceConfig] = None

# 全局组件
_retrieval: Optional[RecsysRetrievalInferenceRunner] = None
_feature_store: Optional[FeatureStore] = None
_metrics = None


# ==================== Pydantic 模型 ====================

class RetrieveRequest(BaseModel):
    """召回请求"""
    user_id: str
    top_k: int = 100
    history_len: int = 32
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "user_12345",
                "top_k": 100,
                "history_len": 32,
            }
        }


class RetrieveResult(BaseModel):
    """单个召回结果"""
    rank: int
    post_id: str
    similarity: float


class RetrieveResponse(BaseModel):
    """召回响应"""
    user_id: str
    top_k: int
    results: List[RetrieveResult]
    corpus_size: int
    inference_time_ms: float
    model_version: Optional[str] = None


class UserEmbeddingRequest(BaseModel):
    """用户编码请求"""
    user_id: str
    history_len: int = 32


class UserEmbeddingResponse(BaseModel):
    """用户编码响应"""
    user_id: str
    embedding_shape: List[int]
    embedding_sample: List[float]  # 只返回前几个值作为示例


class HealthResponse(BaseModel):
    """健康检查"""
    status: str
    model_ready: bool
    corpus_loaded: bool
    corpus_size: int = 0
    feature_store_ready: bool
    model_version: Optional[str] = None


# ==================== 向量索引抽象 ====================

class VectorIndex:
    """向量索引抽象 (支持 FAISS/Milvus/内存)"""
    
    def __init__(self, config: RetrievalServiceConfig):
        self.config = config
        self.corpus_embeddings: Optional[jnp.ndarray] = None
        self.corpus_ids: Optional[jnp.ndarray] = None
        self._corpus_size = 0
    
    def load(self) -> None:
        """加载候选池"""
        if self.config.environment == "production" and not self.config.vector_index_path:
            raise RuntimeError("production retrieval requires a configured vector index")
        if self.config.vector_index_type == "faiss" and self.config.vector_index_path:
            self._load_faiss()
        elif self.config.environment == "production":
            raise RuntimeError("production retrieval requires a supported vector index")
        else:
            self._load_mock()
    
    def _load_faiss(self) -> None:
        """从 FAISS 索引加载"""
        try:
            import faiss
            faiss.read_index(self.config.vector_index_path)
            if self.config.environment == "production":
                raise RuntimeError(
                    "FAISS index loading is not wired to the retrieval corpus yet; "
                    "use the gRPC gateway or implement the index adapter before production"
                )
            logger.warning("FAISS index is not wired to this HTTP service; using demo corpus")
            self._load_mock()
        except ImportError as exc:
            if self.config.environment == "production":
                raise RuntimeError("production retrieval requires the faiss dependency") from exc
            logger.warning("faiss not installed, falling back to mock")
            self._load_mock()
    
    def _load_mock(self) -> None:
        """加载模拟候选池"""
        corpus_size = 10000
        self.corpus_embeddings, self.corpus_ids = create_example_corpus(
            corpus_size=corpus_size,
            emb_size=self.config.model.emb_size,
            seed=456,
        )
        self._corpus_size = corpus_size
        logger.info(f"Loaded mock corpus with {corpus_size} items")
    
    @property
    def corpus_size(self) -> int:
        return self._corpus_size
    
    def get_corpus(self) -> Tuple[jnp.ndarray, jnp.ndarray]:
        return self.corpus_embeddings, self.corpus_ids


# ==================== 服务初始化 ====================

def _init_model(config: RetrievalServiceConfig) -> RecsysRetrievalInferenceRunner:
    """初始化召回模型"""
    logger.info("Initializing retrieval model...")
    
    mc = config.model
    hash_config = HashConfig(
        num_user_hashes=mc.num_user_hashes,
        num_item_hashes=mc.num_item_hashes,
        num_author_hashes=mc.num_author_hashes,
    )
    
    model_config = PhoenixRetrievalModelConfig(
        emb_size=mc.emb_size,
        history_seq_len=mc.history_seq_len,
        candidate_seq_len=mc.candidate_seq_len,
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
    
    runner = RecsysRetrievalInferenceRunner(
        runner=RetrievalModelRunner(model=model_config, bs_per_device=0.125),
        name="retrieval_prod",
    )
    runner.initialize()
    
    # 加载 checkpoint (如果提供)
    allow_random_init = config.environment != "production"
    registry = create_model_registry(
        config.checkpoint_path, allow_random_init=allow_random_init
    )
    if registry.get_params() is not None:
        runner.params = registry.get_params()
        logger.info("Loaded model from checkpoint")
    else:
        logger.warning("Using random initialization (no checkpoint provided)")
    
    return runner


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global _config, _retrieval, _feature_store, _metrics
    
    # 加载配置
    _config = RetrievalServiceConfig.from_env()
    logger.info(f"Starting Retrieval Service on {_config.host}:{_config.port}")
    
    # 初始化监控
    _metrics = create_metrics_collector(
        "retrieval",
        enabled=_config.enable_metrics,
        port=_config.metrics_port,
    )
    
    # 初始化特征存储
    if _config.environment == "production" and _config.feature_backend == "mock":
        raise RuntimeError("production retrieval requires a real feature backend")
    _feature_store = create_feature_store(
        _config.feature_backend,
        emb_size=_config.model.emb_size,
    )
    
    # 初始化模型
    with _metrics.record_inference("init"):
        _retrieval = _init_model(_config)
    
    # 加载候选池
    vector_index = VectorIndex(_config)
    vector_index.load()
    _retrieval.set_corpus(
        vector_index.corpus_embeddings,
        vector_index.corpus_ids,
    )
    
    logger.info(f"Retrieval Service ready! Corpus size: {vector_index.corpus_size}")
    
    yield
    
    logger.info("Shutting down Retrieval Service...")


app = FastAPI(
    title="Phoenix Retrieval Service",
    description="候选物品召回服务",
    version="1.0.0",
    lifespan=lifespan,
)


# ==================== API 端点 ====================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """健康检查"""
    corpus_size = 0
    if _retrieval and _retrieval.corpus_embeddings is not None:
        corpus_size = len(_retrieval.corpus_embeddings)
    
    registry = create_model_registry(_config.checkpoint_path if _config else None)
    version = registry.current_version.version if registry.current_version else None
    
    return HealthResponse(
        status="healthy" if _retrieval else "unhealthy",
        model_ready=_retrieval is not None,
        corpus_loaded=_retrieval is not None and _retrieval.corpus_embeddings is not None,
        corpus_size=corpus_size,
        feature_store_ready=_feature_store is not None,
        model_version=version,
    )


@app.get("/")
async def root():
    """服务信息"""
    return {
        "service": "Phoenix Retrieval Service",
        "docs": "/docs",
        "health": "/health",
        "retrieve": "POST /v1/retrieve",
        "encode_user": "POST /v1/encode_user",
    }


@app.post("/v1/retrieve", response_model=RetrieveResponse)
async def retrieve_candidates(request: RetrieveRequest):
    """
    召回接口
    
    从海量候选池中检索与用户最相关的 Top-K 物品。
    """
    if _retrieval is None or _feature_store is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    import time
    start_time = time.time()
    
    with _metrics.record_request("POST", "/v1/retrieve"):
        # 创建 batch (召回不需要提供候选列表，只需要用户特征)
        # 使用最少的候选数量来构建 batch 结构
        num_candidates = _config.model.candidate_seq_len if _config else 8
        
        from runners import create_example_batch
        batch, embeddings = create_example_batch(
            batch_size=1,
            emb_size=_config.model.emb_size,
            history_len=request.history_len,
            num_candidates=num_candidates,
            num_actions=len(ACTIONS),
            num_user_hashes=_config.model.num_user_hashes,
            num_item_hashes=_config.model.num_item_hashes,
            num_author_hashes=_config.model.num_author_hashes,
            product_surface_vocab_size=_config.model.product_surface_vocab_size,
        )
        
        _metrics.record_batch_size("retrieval", 1)
        
        # 执行检索
        with _metrics.record_inference("retrieval"):
            output = _retrieval.retrieve(batch, embeddings, top_k=request.top_k)
        
        # 解析结果
        top_k_indices = np.array(output.top_k_indices[0])
        top_k_scores = np.array(output.top_k_scores[0])
        
        corpus_size = len(_retrieval.corpus_embeddings) if _retrieval.corpus_embeddings is not None else 0
        
        results = []
        for rank in range(request.top_k):
            idx = int(top_k_indices[rank])
            # 将 corpus_id 转换为字符串 post_id
            post_id = str(idx) if _retrieval.corpus_post_ids is None else str(int(_retrieval.corpus_post_ids[idx]))
            results.append(RetrieveResult(
                rank=rank + 1,
                post_id=post_id,
                similarity=float(top_k_scores[rank]),
            ))
        
        inference_time = (time.time() - start_time) * 1000
        
        registry = create_model_registry(_config.checkpoint_path)
        version = registry.current_version.version if registry.current_version else "random"
        
        return RetrieveResponse(
            user_id=request.user_id,
            top_k=request.top_k,
            results=results,
            corpus_size=corpus_size,
            inference_time_ms=inference_time,
            model_version=version,
        )


@app.post("/v1/encode_user", response_model=UserEmbeddingResponse)
async def encode_user(request: UserEmbeddingRequest):
    """
    用户编码接口
    
    将用户特征编码为向量，可用于离线预计算或调试。
    """
    if _retrieval is None or _feature_store is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    # 构建输入
    from runners import create_example_batch
    num_candidates = _config.model.candidate_seq_len if _config else 8
    
    batch, embeddings = create_example_batch(
        batch_size=1,
        emb_size=_config.model.emb_size,
        history_len=request.history_len,
        num_candidates=num_candidates,
        num_actions=len(ACTIONS),
        num_user_hashes=_config.model.num_user_hashes,
        num_item_hashes=_config.model.num_item_hashes,
        num_author_hashes=_config.model.num_author_hashes,
        product_surface_vocab_size=_config.model.product_surface_vocab_size,
    )
    
    # 编码用户
    user_embedding = _retrieval.encode_user(batch, embeddings)
    user_emb_np = np.array(user_embedding[0])  # [num_user_hashes, emb_size]
    
    return UserEmbeddingResponse(
        user_id=request.user_id,
        embedding_shape=list(user_emb_np.shape),
        embedding_sample=user_emb_np.flatten()[:10].tolist(),
    )


if __name__ == "__main__":
    import uvicorn
    config = RetrievalServiceConfig.from_env()
    uvicorn.run(app, host=config.host, port=config.port)
