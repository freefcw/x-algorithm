"""Shared contract between training artifacts and Phoenix serving."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

# Cross-language serving contract. Keep these values independent from any server implementation.
FEATURE_SCHEMA = "phoenix-string-id-actions-v2"
ACTION_IDX_TO_ENUM = (1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 4, 13, 14, 15, 16, 17, 18)
ALL_ACTION_ENUMS = tuple(range(1, 19))
# Heads with a non-zero ranking contribution in Home Mixer.  Metadata may
# advertise additional trained heads, but these must always be present.
# v1 head set (docs/implementation/phoenix-training-data-decisions.md §1):
# favorite (1), reply (2), report (18) -- the only actions the platform can
# collect today.  Must match home-mixer `REQUIRED_SUPPORTED_ACTIONS` and the
# `--observed-actions favorite,reply,report` training recipe; change all three
# in one PR (decisions doc §1.4).
NONZERO_WEIGHT_ACTION_ENUMS = (1, 2, 18)

# 63-bit non-negative; 0 is reserved for xrex padding and bumped to 1.
_OBJECT_ID_HASH_MASK = 0x7FFF_FFFF_FFFF_FFFF


def object_id_to_u64_hash(oid_hex: str) -> int:
    """Derive a stable int64 from a 24-char lowercase ObjectId hex string.

    Hashes the 12 raw bytes (not the hex text). Shared with Rust `ObjectId::to_u64_hash`.
    Used by xrex adapters and wrapping_mul buckets, not by the slim embedding tables.
    """
    # Match Rust `ObjectId::parse` exactly: no whitespace, separators, or
    # uppercase hex are accepted.  `bytes.fromhex` itself is intentionally
    # more permissive (it ignores ASCII whitespace), so validate every
    # character before decoding.
    if len(oid_hex) != 24 or any(ch not in "0123456789abcdef" for ch in oid_hex):
        raise ValueError(f"object id must be 24 lowercase hex chars, got {oid_hex!r}")
    raw = bytes.fromhex(oid_hex)
    digest = hashlib.md5(raw, usedforsecurity=False).digest()
    hashed = int.from_bytes(digest[:8], "big") & _OBJECT_ID_HASH_MASK
    return 1 if hashed == 0 else hashed


def supported_actions_header(action_enums: Iterable[int] | None = None) -> str:
    """Format supported proto ActionName values for gRPC metadata."""
    values = (
        NONZERO_WEIGHT_ACTION_ENUMS
        if action_enums is None
        else sorted(set(action_enums))
    )
    return ",".join(str(value) for value in values)
