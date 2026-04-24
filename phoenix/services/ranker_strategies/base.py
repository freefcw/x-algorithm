# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
精排策略抽象基类与统一输出结构。

所有具体策略（Phoenix / 规则 / DeepCTR / LLM）必须实现 `RankingStrategy.score`，
返回统一的 `StrategyResult`，上层服务不感知具体打分实现。
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import List, Optional

import numpy as np


class StrategyError(Exception):
    """策略执行异常（上游可据此决定是否降级）。"""


@dataclass
class StrategyResult:
    """
    统一的策略打分结果。

    Attributes:
        scores: shape=[num_candidates, num_actions] 的概率矩阵。
                单目标策略可将主分数在 num_actions 维上复制。
        ranked_indices: shape=[num_candidates] 的排序后索引（降序）。
        strategy_name: 策略名，用于日志/监控。
        model_version: 若策略有模型权重，返回版本标识；否则为 None。
    """

    scores: np.ndarray
    ranked_indices: np.ndarray
    strategy_name: str
    model_version: Optional[str] = None


class RankingStrategy(abc.ABC):
    """
    精排打分策略抽象。

    设计约束：
        1. 策略对外只暴露 `score` 一个方法，输入仅依赖 `user_id` + `candidate_ids`；
           具体的特征获取由策略自身调用 `FeatureStore` 完成，上层无需感知。
        2. 策略内部可以是同步模型推理，但对外统一为 `async def`，
           方便未来接入远程模型 / LLM 等 I/O 密集型后端。
        3. 策略应在 `__init__` 中完成所有一次性开销（如模型加载 / JIT），
           `score` 本身要尽可能轻。
    """

    name: str = "base"

    @abc.abstractmethod
    async def score(
        self,
        user_id: str,
        candidate_ids: List[str],
        history_len: int = 32,
    ) -> StrategyResult:
        """对候选集打分并返回 `StrategyResult`。"""
        raise NotImplementedError

    async def warmup(self) -> None:
        """可选：预热钩子。默认空实现。"""
        return None

    async def close(self) -> None:
        """可选：关闭钩子，释放资源（如远程连接）。默认空实现。"""
        return None
