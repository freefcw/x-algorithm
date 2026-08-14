# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
from __future__ import annotations

from dataclasses import replace
from typing import Any, Protocol

import jax

from xrex.models.model_utils import Parameter


def _lookup(embedding_table_param: Parameter, token_ids: jax.Array) -> Parameter:
    return replace(embedding_table_param, x=embedding_table_param.x[token_ids])


class RecsysEmbeddingOptimizer(Protocol):
    def init(self, params: dict[str, Parameter]) -> Any: ...

    def transform_embeddings(
        self,
        embeddings: Any,
        token_ids: jax.Array,
        state: Any,
    ) -> tuple[Any, Any]:
        return embeddings, None

    def sparse_update(
        self,
        grads: Parameter,
        full_state: Any,
        full_emb_table: Parameter,
        unique_tokens: jax.Array,
        num_unique: int,
        lr: float,
        valid_step: jax.Array,
        carry: Any = None,
    ) -> tuple[Parameter, Any, dict[str, jax.Array]]: ...
