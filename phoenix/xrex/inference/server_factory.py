# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, TypeVar


class StalePostServingConfig(Protocol):
    enable_stale_post: bool


ServerT = TypeVar("ServerT")


def create_recsys_server(
    server_type: Callable[..., ServerT],
    runner: StalePostServingConfig,
    /,
    *args: Any,
    **kwargs: Any,
) -> ServerT:
    """Construct a predictor server with the shared post-feature contract."""
    reserved = {"num_post_bool_features", "enable_stale_post"} & kwargs.keys()
    if reserved:
        names = ", ".join(sorted(reserved))
        raise TypeError(f"shared server feature arguments cannot be overridden: {names}")

    from xrex.data.recsys import recsys_batch

    return server_type(
        *args,
        num_post_bool_features=recsys_batch.POST_BOOL_FEATURE_SIZE,
        enable_stale_post=runner.enable_stale_post,
        **kwargs,
    )
