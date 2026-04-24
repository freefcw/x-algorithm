# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
方案 D：LLM-as-Ranker 占位策略

作为对比组保留接口。当前实现为桩：直接抛 StrategyError，避免静默用未实现的
打分结果误导上游。真正启用需补齐以下其一：
    - D.1 Zero-shot prompt：调用远程 LLM API，接收排序结果；
    - D.2 LLM embedding：离线预计算 post embedding，线上仅做点积；
    - D.3 蒸馏：LLM 打软标签训练 Phoenix，本文件无需扩展。
"""

from __future__ import annotations

import logging
from typing import List, Optional

from services.config import RankerServiceConfig
from services.feature_store import FeatureStore
from services.ranker_strategies.base import (
    RankingStrategy,
    StrategyError,
    StrategyResult,
)

logger = logging.getLogger("ranker_strategies.llm")


class LLMStrategy(RankingStrategy):
    """LLM 精排占位策略。"""

    name = "llm"

    def __init__(
        self,
        config: RankerServiceConfig,
        feature_store: FeatureStore,
        mode: str = "zero_shot",
        endpoint: Optional[str] = None,
    ):
        self._config = config
        self._feature_store = feature_store
        self._mode = mode
        self._endpoint = endpoint
        logger.info("LLMStrategy(mode=%s, endpoint=%s) 未接入具体实现", mode, endpoint)

    async def score(
        self,
        user_id: str,
        candidate_ids: List[str],
        history_len: int = 32,
    ) -> StrategyResult:
        raise StrategyError(
            "LLMStrategy 当前为占位实现；请按 docs/精排模型替代方案指引.md §5 "
            "选择 D.1 / D.2 / D.3 之一并在此类中补齐。"
        )
