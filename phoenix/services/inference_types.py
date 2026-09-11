"""Transport-neutral inference values shared by Phoenix entrypoints."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from services.model_contract import ACTION_IDX_TO_ENUM


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
