"""Shared contract between training artifacts and Phoenix serving."""

from __future__ import annotations

from collections.abc import Iterable

# Cross-language serving contract. Keep these values independent from any server implementation.
FEATURE_SCHEMA = "phoenix-string-id-actions-v2"
ACTION_IDX_TO_ENUM = (1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 4, 13, 14, 15, 16, 17, 18)
ALL_ACTION_ENUMS = tuple(range(1, 19))


def supported_actions_header(action_enums: Iterable[int] | None = None) -> str:
    """Format supported proto ActionName values for gRPC metadata."""
    values = ALL_ACTION_ENUMS if action_enums is None else sorted(set(action_enums))
    return ",".join(str(value) for value in values)
