# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
特征服务抽象层

提供统一的特征获取接口，支持多种后端实现:
- Mock: 本地随机生成 (开发和测试)
- Redis: 从 Redis 读取预计算 embedding
- FeatureStore: 对接企业级特征平台
"""

import abc
import logging
from typing import Dict, List, Optional, Tuple

import numpy as np

from recsys_model import RecsysBatch, RecsysEmbeddings

logger = logging.getLogger("feature_store")


class FeatureStore(abc.ABC):
    """特征存储抽象基类"""
    
    @abc.abstractmethod
    async def get_user_features(
        self, user_id: str
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        获取用户特征
        
        Returns:
            (user_hashes, history_post_hashes, history_author_hashes, 
             history_actions, history_product_surface)
        """
        pass
    
    @abc.abstractmethod
    async def get_candidate_embeddings(
        self, candidate_ids: List[str], emb_size: int
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        获取候选物品 embedding
        
        Returns:
            (post_embeddings, author_embeddings, product_surface)
        """
        pass
    
    @abc.abstractmethod
    async def get_user_embeddings(
        self, user_id: str, num_user_hashes: int, emb_size: int
    ) -> np.ndarray:
        """获取用户 embedding"""
        pass
    
    @abc.abstractmethod
    async def get_item_embeddings(
        self, item_ids: List[str], num_item_hashes: int, emb_size: int
    ) -> np.ndarray:
        """获取物品 embedding"""
        pass
    
    @abc.abstractmethod
    async def get_author_embeddings(
        self, author_ids: List[str], num_author_hashes: int, emb_size: int
    ) -> np.ndarray:
        """获取作者 embedding"""
        pass

    @abc.abstractmethod
    async def build_recsys_batch(
        self,
        user_id: str,
        candidate_ids: List[str],
        history_len: int,
        num_actions: int,
        num_user_hashes: int,
        num_item_hashes: int,
        num_author_hashes: int,
        product_surface_vocab_size: int,
    ) -> Tuple[RecsysBatch, RecsysEmbeddings]:
        """
        组装完整 (RecsysBatch, RecsysEmbeddings)，供精排策略直接消费。

        各后端可按需要复用更细粒度的 get_* 接口拼接，也可以在此处做批量优化。
        """
        pass


class MockFeatureStore(FeatureStore):
    """Mock 特征存储 - 用于开发和测试"""
    
    def __init__(self, emb_size: int = 128, seed: int = 42):
        self.emb_size = emb_size
        self.rng = np.random.default_rng(seed)
        self._cache: Dict[str, np.ndarray] = {}
        logger.info("Initialized MockFeatureStore")
    
    def _get_or_create_embedding(self, key: str) -> np.ndarray:
        """获取或创建 embedding (模拟缓存)"""
        if key not in self._cache:
            self._cache[key] = self.rng.normal(size=self.emb_size).astype(np.float32)
        return self._cache[key]
    
    async def get_user_features(
        self, user_id: str, history_len: int = 32, num_actions: int = 19
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """生成模拟用户特征"""
        batch_size = 1
        
        # 用户哈希 (复用 seed 确保一致性)
        user_seed = hash(user_id) % (2**31)
        rng = np.random.default_rng(user_seed)
        
        user_hashes = rng.integers(1, 100000, size=(batch_size, 2)).astype(np.int32)
        
        # 历史记录
        valid_len = rng.integers(history_len // 2, history_len + 1)
        history_post_hashes = rng.integers(1, 100000, size=(batch_size, history_len, 2)).astype(np.int32)
        history_post_hashes[0, valid_len:, :] = 0  # 填充位
        
        history_author_hashes = rng.integers(1, 100000, size=(batch_size, history_len, 2)).astype(np.int32)
        history_author_hashes[0, valid_len:, :] = 0
        
        history_actions = (rng.random(size=(batch_size, history_len, num_actions)) > 0.7).astype(np.float32)
        history_product_surface = rng.integers(0, 16, size=(batch_size, history_len)).astype(np.int32)
        
        return (
            user_hashes,
            history_post_hashes,
            history_author_hashes,
            history_actions,
            history_product_surface,
        )
    
    async def get_candidate_embeddings(
        self, candidate_ids: List[str], emb_size: int
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """生成模拟候选 embedding"""
        num_candidates = len(candidate_ids)
        batch_size = 1
        num_item_hashes = 2
        num_author_hashes = 2
        
        post_embeddings = np.zeros(
            (batch_size, num_candidates, num_item_hashes, emb_size), dtype=np.float32
        )
        author_embeddings = np.zeros(
            (batch_size, num_candidates, num_author_hashes, emb_size), dtype=np.float32
        )
        product_surface = np.zeros((batch_size, num_candidates), dtype=np.int32)
        
        for i, cand_id in enumerate(candidate_ids):
            post_embeddings[0, i] = self._get_or_create_embedding(f"post:{cand_id}")
            author_embeddings[0, i] = self._get_or_create_embedding(f"author:{cand_id}")
            product_surface[0, i] = hash(cand_id) % 16
        
        return post_embeddings, author_embeddings, product_surface
    
    async def get_user_embeddings(
        self, user_id: str, num_user_hashes: int, emb_size: int
    ) -> np.ndarray:
        """生成模拟用户 embedding"""
        embeddings = np.zeros((1, num_user_hashes, emb_size), dtype=np.float32)
        for h in range(num_user_hashes):
            embeddings[0, h] = self._get_or_create_embedding(f"user:{user_id}:hash{h}")
        return embeddings
    
    async def get_item_embeddings(
        self, item_ids: List[str], num_item_hashes: int, emb_size: int
    ) -> np.ndarray:
        """生成模拟物品 embedding"""
        batch_size = 1
        num_items = len(item_ids)
        embeddings = np.zeros((batch_size, num_items, num_item_hashes, emb_size), dtype=np.float32)
        for i, item_id in enumerate(item_ids):
            for h in range(num_item_hashes):
                embeddings[0, i, h] = self._get_or_create_embedding(f"item:{item_id}:hash{h}")
        return embeddings
    
    async def get_author_embeddings(
        self, author_ids: List[str], num_author_hashes: int, emb_size: int
    ) -> np.ndarray:
        """生成模拟作者 embedding"""
        batch_size = 1
        num_authors = len(author_ids)
        embeddings = np.zeros((batch_size, num_authors, num_author_hashes, emb_size), dtype=np.float32)
        for i, author_id in enumerate(author_ids):
            for h in range(num_author_hashes):
                embeddings[0, i, h] = self._get_or_create_embedding(f"author:{author_id}:hash{h}")
        return embeddings
    
    async def build_recsys_batch(
        self,
        user_id: str,
        candidate_ids: List[str],
        history_len: int,
        num_actions: int,
        num_user_hashes: int,
        num_item_hashes: int,
        num_author_hashes: int,
        product_surface_vocab_size: int,
    ) -> Tuple[RecsysBatch, RecsysEmbeddings]:
        """
        构建完整的 RecsysBatch 和 RecsysEmbeddings
        
        这是对外提供的主要接口，将多个特征获取调用封装在一起。
        """
        batch_size = 1
        num_candidates = len(candidate_ids)
        
        # 获取用户特征
        (
            user_hashes,
            history_post_hashes,
            history_author_hashes,
            history_actions,
            history_product_surface,
        ) = await self.get_user_features(user_id, history_len, num_actions)
        
        # 获取候选特征
        candidate_post_hashes = np.zeros((batch_size, num_candidates, num_item_hashes), dtype=np.int32)
        candidate_author_hashes = np.zeros((batch_size, num_candidates, num_author_hashes), dtype=np.int32)
        candidate_product_surface = np.zeros((batch_size, num_candidates), dtype=np.int32)
        
        for i, cand_id in enumerate(candidate_ids):
            candidate_post_hashes[0, i] = hash(cand_id) % 100000 + 1
            candidate_author_hashes[0, i] = (hash(cand_id) // 100000) % 100000 + 1
            candidate_product_surface[0, i] = hash(cand_id) % product_surface_vocab_size
        
        # 构建 batch
        batch = RecsysBatch(
            user_hashes=user_hashes,
            history_post_hashes=history_post_hashes,
            history_author_hashes=history_author_hashes,
            history_actions=history_actions,
            history_product_surface=history_product_surface,
            candidate_post_hashes=candidate_post_hashes,
            candidate_author_hashes=candidate_author_hashes,
            candidate_product_surface=candidate_product_surface,
        )
        
        # 获取 embeddings
        user_embeddings = await self.get_user_embeddings(user_id, num_user_hashes, self.emb_size)
        history_post_embeddings = await self.get_item_embeddings(
            [f"hist_{i}" for i in range(history_len)], num_item_hashes, self.emb_size
        )
        history_author_embeddings = await self.get_author_embeddings(
            [f"hist_author_{i}" for i in range(history_len)], num_author_hashes, self.emb_size
        )
        candidate_post_embeddings = await self.get_item_embeddings(
            candidate_ids, num_item_hashes, self.emb_size
        )
        candidate_author_embeddings = await self.get_author_embeddings(
            candidate_ids, num_author_hashes, self.emb_size
        )
        
        # 调整维度
        embeddings = RecsysEmbeddings(
            user_embeddings=user_embeddings,
            history_post_embeddings=history_post_embeddings,
            history_author_embeddings=history_author_embeddings,
            candidate_post_embeddings=candidate_post_embeddings,
            candidate_author_embeddings=candidate_author_embeddings,
        )
        
        return batch, embeddings


def create_feature_store(backend: str = "mock", **kwargs) -> FeatureStore:
    """工厂函数创建特征存储"""
    if backend == "mock":
        return MockFeatureStore(**kwargs)
    # TODO: 添加 Redis 和 FeatureStore 后端
    raise ValueError(f"Unknown backend: {backend}")
