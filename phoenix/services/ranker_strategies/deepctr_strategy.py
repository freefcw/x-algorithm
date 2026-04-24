# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
方案 C：DeepCTR（DIN / DCN / DeepFM 等）精排策略适配器

当前为接入占位实现：
    - 若环境未安装 deepctr-torch 或未提供 checkpoint，构造函数抛 StrategyError，
      上层工厂可决定是否降级到其他策略；
    - 提供了 `_predict` 钩子供实际部署时填充，默认抛 NotImplementedError。

对完整训练与数据转换流程，参考 `docs/精排模型替代方案指引.md` 第 4 章。
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

import numpy as np

from recsys_model import RecsysBatch
from runners import ACTIONS

from services.config import RankerServiceConfig
from services.feature_store import FeatureStore
from services.ranker_strategies.base import (
    RankingStrategy,
    StrategyError,
    StrategyResult,
)

logger = logging.getLogger("ranker_strategies.deepctr")


class DeepCTRStrategy(RankingStrategy):
    """DeepCTR-Torch 精排模型接入占位实现。"""

    name = "deepctr"

    def __init__(
        self,
        config: RankerServiceConfig,
        feature_store: FeatureStore,
        model_kind: str = "din",
    ):
        self._config = config
        self._feature_store = feature_store
        self._model_kind = model_kind
        self._model: Optional[Any] = None
        self._model_version: Optional[str] = None
        self._initialize()

    def _initialize(self) -> None:
        try:
            import torch  # type: ignore[import-not-found]  # noqa: F401
            import deepctr_torch  # type: ignore[import-not-found]  # noqa: F401
        except ImportError as exc:
            raise StrategyError(
                "DeepCTRStrategy requires `torch` and `deepctr-torch`; "
                "安装：uv add torch deepctr-torch"
            ) from exc

        ckpt = self._config.checkpoint_path
        if not ckpt:
            raise StrategyError(
                "DeepCTRStrategy requires RANKER_CHECKPOINT_PATH to point to a "
                "trained DeepCTR checkpoint (.pt/.pth)."
            )

        # 真实场景下在此处按 `model_kind` 构造对应模型并加载权重。
        # 目前作为占位抛出 NotImplementedError，避免静默加载错误权重。
        raise StrategyError(
            "DeepCTRStrategy model construction is not wired yet; "
            "请参考 docs/精排模型替代方案指引.md §4 完成 feature_columns / "
            "state_dict 加载，并在此方法中赋值 self._model。"
        )

    async def score(
        self,
        user_id: str,
        candidate_ids: List[str],
        history_len: int = 32,
    ) -> StrategyResult:
        if self._model is None:
            raise StrategyError("DeepCTRStrategy not initialized")

        mc = self._config.model
        try:
            batch, _ = await self._feature_store.build_recsys_batch(
                user_id=user_id,
                candidate_ids=candidate_ids,
                history_len=history_len,
                num_actions=len(ACTIONS),
                num_user_hashes=mc.num_user_hashes,
                num_item_hashes=mc.num_item_hashes,
                num_author_hashes=mc.num_author_hashes,
                product_surface_vocab_size=mc.product_surface_vocab_size,
            )
            primary = self._predict(batch, candidate_ids)
        except Exception as exc:
            raise StrategyError(f"deepctr inference failed: {exc}") from exc

        scores = np.tile(primary[:, None], (1, len(ACTIONS))).astype(np.float32)
        ranked_indices = np.argsort(-primary, kind="stable").astype(np.int32)

        return StrategyResult(
            scores=scores,
            ranked_indices=ranked_indices,
            strategy_name=self.name,
            model_version=self._model_version,
        )

    def _predict(self, batch: RecsysBatch, candidate_ids: List[str]) -> np.ndarray:
        """
        返回 shape=[num_candidates] 的主概率（如 favorite）。

        预期实现：
            - 将 `batch.user_hashes[:, 0]` / `candidate_post_hashes[:, :, 0]` 等平铺
              成 DeepCTR 期望的 feature dict；
            - 调用 `self._model.predict(feed_dict, batch_size=...)`；
            - 返回 numpy 数组。
        """
        raise NotImplementedError(
            "DeepCTRStrategy._predict 需按实际模型特征列实现，参考文档 §4。"
        )
