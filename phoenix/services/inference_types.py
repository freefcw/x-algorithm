"""Transport-neutral inference values shared by Phoenix entrypoints."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

# Model ACTIONS index to recsys.proto ActionName enum value.
ACTION_IDX_TO_ENUM = [1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 4, 13, 14, 15, 16, 17, 18]


@dataclass
class HistoryFeatures:
    post_hashes: np.ndarray
    author_hashes: np.ndarray
    actions: np.ndarray
    product_surface: np.ndarray


@dataclass
class CandidatePrediction:
    action_probs: np.ndarray
    continuous_values: np.ndarray | None = None
