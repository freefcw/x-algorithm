# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
方案 B：Phoenix 自研精排策略

将原 `services/ranker_service.py::_init_model` 里的初始化链路搬过来，
统一成 `RankingStrategy` 接口，便于与其他策略对等切换。
"""

from __future__ import annotations

import logging
from typing import List, Optional

import numpy as np

from grok import TransformerConfig
from recsys_model import HashConfig, PhoenixModelConfig
from runners import ACTIONS, ModelRunner, RecsysInferenceRunner

from services.config import RankerServiceConfig
from services.feature_store import FeatureStore
from services.model_registry import create_model_registry
from services.ranker_strategies.base import (
    RankingStrategy,
    StrategyError,
    StrategyResult,
)

logger = logging.getLogger("ranker_strategies.phoenix")


class PhoenixStrategy(RankingStrategy):
    """基于仓库内置 Phoenix Transformer 精排模型的策略。"""

    name = "phoenix"

    def __init__(
        self,
        config: RankerServiceConfig,
        feature_store: FeatureStore,
    ):
        self._config = config
        self._feature_store = feature_store
        self._runner: Optional[RecsysInferenceRunner] = None
        self._model_version: Optional[str] = None
        self._initialize()

    # ---------------------------------------------------------------- init

    def _initialize(self) -> None:
        """构造模型、加载 checkpoint（若有）。"""
        mc = self._config.model
        hash_config = HashConfig(
            num_user_hashes=mc.num_user_hashes,
            num_item_hashes=mc.num_item_hashes,
            num_author_hashes=mc.num_author_hashes,
        )
        model_config = PhoenixModelConfig(
            emb_size=mc.emb_size,
            num_actions=len(ACTIONS),
            history_seq_len=mc.history_seq_len,
            candidate_seq_len=max(mc.candidate_seq_len, self._config.max_batch_size),
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
            name="ranker_phoenix",
        )
        runner.initialize()

        # 加载 checkpoint 或回落到随机初始化
        allow_random_init = self._config.environment != "production"
        registry = create_model_registry(
            self._config.checkpoint_path, allow_random_init=allow_random_init
        )
        if registry.get_params() is not None:
            runner.params = registry.get_params()
            self._model_version = (
                registry.current_version.version if registry.current_version else "loaded"
            )
            logger.info("Phoenix strategy: loaded checkpoint %s", self._model_version)
        else:
            self._model_version = "random"
            logger.warning(
                "Phoenix strategy: using random initialization (no checkpoint provided)"
            )

        self._runner = runner

    # ---------------------------------------------------------------- score

    async def score(
        self,
        user_id: str,
        candidate_ids: List[str],
        history_len: int = 32,
    ) -> StrategyResult:
        if self._runner is None:
            raise StrategyError("PhoenixStrategy not initialized")

        mc = self._config.model
        try:
            batch, embeddings = await self._feature_store.build_recsys_batch(
                user_id=user_id,
                candidate_ids=candidate_ids,
                history_len=history_len,
                num_actions=len(ACTIONS),
                num_user_hashes=mc.num_user_hashes,
                num_item_hashes=mc.num_item_hashes,
                num_author_hashes=mc.num_author_hashes,
                product_surface_vocab_size=mc.product_surface_vocab_size,
            )
            output = self._runner.rank(batch, embeddings)
        except Exception as exc:  # 模型/特征任一环节出错都视为策略失败
            raise StrategyError(f"phoenix inference failed: {exc}") from exc

        # 只取 batch 中第一条（服务端固定 batch_size=1）
        scores = np.asarray(output.scores[0], dtype=np.float32)         # [C, A]
        ranked_indices = np.asarray(output.ranked_indices[0], dtype=np.int32)  # [C]

        # 截断到实际候选数（模型 candidate_seq_len 可能 > 请求候选数）
        n = len(candidate_ids)
        scores = scores[:n]
        ranked_indices = np.asarray(
            [idx for idx in ranked_indices if int(idx) < n][:n], dtype=np.int32
        )

        return StrategyResult(
            scores=scores,
            ranked_indices=ranked_indices,
            strategy_name=self.name,
            model_version=self._model_version,
        )
