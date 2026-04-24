# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
精排打分策略集合（Strategy Pattern）

设计目标：
    让 `services/ranker_service.py` 只依赖 `RankingStrategy` 抽象，
    上层业务不感知具体实现（Phoenix / 规则 / DeepCTR / LLM）。

切换方式（环境变量）：
    RANKER_STRATEGY=phoenix|rule|deepctr|llm
    RANKER_FALLBACK_STRATEGY=rule                # 可选，主策略异常时降级
"""

from services.ranker_strategies.base import (
    RankingStrategy,
    StrategyResult,
    StrategyError,
)
from services.ranker_strategies.factory import create_strategy

__all__ = [
    "RankingStrategy",
    "StrategyResult",
    "StrategyError",
    "create_strategy",
]
