# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
服务配置管理

支持从环境变量、配置文件加载配置。
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ModelConfig:
    """模型架构配置"""
    emb_size: int = 128
    widening_factor: int = 2
    key_size: int = 64
    num_q_heads: int = 2
    num_kv_heads: int = 2
    num_layers: int = 2
    attn_output_multiplier: float = 0.125
    
    # 哈希配置
    num_user_hashes: int = 2
    num_item_hashes: int = 2
    num_author_hashes: int = 2
    
    # 序列长度
    history_seq_len: int = 32
    candidate_seq_len: int = 8
    product_surface_vocab_size: int = 16


@dataclass
class RankerServiceConfig:
    """精排服务配置"""
    # 服务配置
    host: str = "0.0.0.0"
    port: int = 8081
    workers: int = 1  # JAX GPU 建议单进程
    
    # 模型配置
    model: ModelConfig = field(default_factory=ModelConfig)
    
    # Checkpoint 路径
    checkpoint_path: Optional[str] = None  # 默认随机初始化
    
    # 批处理优化
    max_batch_size: int = 32
    batch_timeout_ms: float = 5.0  # 动态批处理等待时间
    
    # 特征服务地址
    feature_service_url: str = "http://localhost:8090"
    
    # 监控
    enable_metrics: bool = True
    metrics_port: int = 9091

    # ── 精排策略（策略模式）──
    # 主策略: phoenix (方案B) | rule (方案A) | deepctr (方案C) | llm (方案D)
    strategy: str = "phoenix"
    # 可选兜底策略；主策略初始化或推理异常时自动降级
    fallback_strategy: Optional[str] = None
    # 规则策略可选：post 元数据 parquet，提供 post_id / publish_time 字段用于新鲜度打分
    rule_post_metadata_path: Optional[str] = None

    @classmethod
    def from_env(cls) -> "RankerServiceConfig":
        """从环境变量加载配置"""
        return cls(
            host=os.getenv("RANKER_HOST", "0.0.0.0"),
            port=int(os.getenv("RANKER_PORT", "8081")),
            workers=int(os.getenv("RANKER_WORKERS", "1")),
            checkpoint_path=os.getenv("RANKER_CHECKPOINT_PATH"),
            max_batch_size=int(os.getenv("RANKER_MAX_BATCH", "32")),
            feature_service_url=os.getenv("FEATURE_SERVICE_URL", "http://localhost:8090"),
            enable_metrics=os.getenv("ENABLE_METRICS", "true").lower() == "true",
            metrics_port=int(os.getenv("RANKER_METRICS_PORT", "9091")),
            strategy=os.getenv("RANKER_STRATEGY", "phoenix"),
            fallback_strategy=os.getenv("RANKER_FALLBACK_STRATEGY") or None,
            rule_post_metadata_path=os.getenv("RANKER_RULE_POST_META") or None,
        )


@dataclass
class RetrievalServiceConfig:
    """召回服务配置"""
    # 服务配置
    host: str = "0.0.0.0"
    port: int = 8082
    workers: int = 1
    
    # 模型配置
    model: ModelConfig = field(default_factory=ModelConfig)
    
    # Checkpoint 路径
    checkpoint_path: Optional[str] = None
    
    # 向量索引配置 (FAISS/Milvus)
    vector_index_type: str = "faiss"  # 或 "milvus"
    vector_index_path: Optional[str] = None  # FAISS 索引文件路径
    milvus_host: str = "localhost"
    milvus_port: int = 19530
    milvus_collection: str = "item_embeddings"
    
    # 候选池配置
    corpus_refresh_interval_s: int = 300  # 5分钟刷新一次候选池
    
    # 批处理
    max_batch_size: int = 64
    
    # 特征服务
    feature_service_url: str = "http://localhost:8090"
    
    # 监控
    enable_metrics: bool = True
    metrics_port: int = 9092
    
    @classmethod
    def from_env(cls) -> "RetrievalServiceConfig":
        """从环境变量加载配置"""
        return cls(
            host=os.getenv("RETRIEVAL_HOST", "0.0.0.0"),
            port=int(os.getenv("RETRIEVAL_PORT", "8082")),
            workers=int(os.getenv("RETRIEVAL_WORKERS", "1")),
            checkpoint_path=os.getenv("RETRIEVAL_CHECKPOINT_PATH"),
            vector_index_type=os.getenv("VECTOR_INDEX_TYPE", "faiss"),
            vector_index_path=os.getenv("FAISS_INDEX_PATH"),
            milvus_host=os.getenv("MILVUS_HOST", "localhost"),
            milvus_port=int(os.getenv("MILVUS_PORT", "19530")),
            milvus_collection=os.getenv("MILVUS_COLLECTION", "item_embeddings"),
            corpus_refresh_interval_s=int(os.getenv("CORPUS_REFRESH_INTERVAL", "300")),
            max_batch_size=int(os.getenv("RETRIEVAL_MAX_BATCH", "64")),
            feature_service_url=os.getenv("FEATURE_SERVICE_URL", "http://localhost:8090"),
            enable_metrics=os.getenv("ENABLE_METRICS", "true").lower() == "true",
            metrics_port=int(os.getenv("RETRIEVAL_METRICS_PORT", "9092")),
        )


@dataclass
class FeatureServiceConfig:
    """特征服务配置 (Mock/Redis/等)"""
    host: str = "0.0.0.0"
    port: int = 8090
    
    # 后端存储类型
    backend: str = "mock"  # 可选: "mock", "redis", "feature_store"
    
    # Redis 配置 (如果使用)
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    
    # Embedding 维度
    emb_size: int = 128
    
    @classmethod
    def from_env(cls) -> "FeatureServiceConfig":
        return cls(
            host=os.getenv("FEATURE_HOST", "0.0.0.0"),
            port=int(os.getenv("FEATURE_PORT", "8090")),
            backend=os.getenv("FEATURE_BACKEND", "mock"),
            redis_host=os.getenv("REDIS_HOST", "localhost"),
            redis_port=int(os.getenv("REDIS_PORT", "6379")),
            redis_db=int(os.getenv("REDIS_DB", "0")),
            emb_size=int(os.getenv("EMB_SIZE", "128")),
        )
