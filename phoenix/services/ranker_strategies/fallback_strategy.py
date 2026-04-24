# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
组合策略：主策略异常时降级到备用策略。

典型用法：
    primary = PhoenixStrategy(...)
    fallback = RuleStrategy(...)
    strategy = FallbackStrategy(primary=primary, fallback=fallback)
"""

from __future__ import annotations

import logging
from typing import List

from services.ranker_strategies.base import (
    RankingStrategy,
    StrategyError,
    StrategyResult,
)

logger = logging.getLogger("ranker_strategies.fallback")


class FallbackStrategy(RankingStrategy):
    """任一 `StrategyError` 都会触发一次向 fallback 的降级。"""

    name = "fallback"

    def __init__(self, primary: RankingStrategy, fallback: RankingStrategy):
        self._primary = primary
        self._fallback = fallback
        self.name = f"{primary.name}+{fallback.name}"

    async def score(
        self,
        user_id: str,
        candidate_ids: List[str],
        history_len: int = 32,
    ) -> StrategyResult:
        try:
            return await self._primary.score(user_id, candidate_ids, history_len)
        except StrategyError as exc:
            logger.warning(
                "primary strategy %s failed: %s; falling back to %s",
                self._primary.name,
                exc,
                self._fallback.name,
            )
            result = await self._fallback.score(user_id, candidate_ids, history_len)
            # 标注降级来源，便于监控链路
            return StrategyResult(
                scores=result.scores,
                ranked_indices=result.ranked_indices,
                strategy_name=f"{self._primary.name}->{self._fallback.name}",
                model_version=result.model_version,
            )

    async def warmup(self) -> None:
        await self._primary.warmup()
        await self._fallback.warmup()

    async def close(self) -> None:
        await self._primary.close()
        await self._fallback.close()
