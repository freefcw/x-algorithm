"""Shared contract between training artifacts and Phoenix serving."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable, Sequence
from pathlib import Path

import numpy as np

# Cross-language serving contract. Keep these values independent from any server implementation.
FEATURE_SCHEMA = "phoenix-snowflake-id-actions-v3"
IDENTITY_MAPPING_VERSION = 1
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

# The `model-version` value home-mixer sees (and can pin through
# PHOENIX_EXPECTED_MODEL_VERSION) and the value a retrieval index is bound to.
# Bound to artifact *content*, not file names: two trainings that both stop at
# step 200 produce different versions, and swapping only the embedding tables
# changes the version too.  "random" is reserved for uninitialised engines.
RANDOM_MODEL_VERSION = "random"
_MODEL_VERSION_DIGEST_HEX = 12
_BUNDLE_PARAMS_FILENAME = "model_params.npz"
# Only the tables themselves take part in serving; the Adagrad accumulators that
# train_ranker stores next to them do not, so a bundle's embedding file and the
# tables-only copy train_retrieval writes must hash the same.
EMBEDDING_TABLE_KEYS: tuple[str, ...] = ("user_emb_table", "post_emb_table", "author_emb_table")


def _digest_npz_arrays(
    digest, path: str | os.PathLike[str], keys: Sequence[str] | None
) -> None:
    """Feed name, dtype, shape and bytes of the selected arrays into ``digest``.

    Hashing array contents (not the file bytes) keeps the version stable across
    re-saves of identical weights -- an ``.npz`` is a zip whose entry timestamps
    change on every write -- and independent of extra arrays stored alongside.
    """
    with np.load(path, allow_pickle=False) as data:
        names = sorted(data.files) if keys is None else [key for key in keys if key in data.files]
        if not names:
            raise ValueError(f"{path} holds none of the arrays expected for a model version")
        for name in names:
            array = np.ascontiguousarray(data[name])
            digest.update(name.encode("utf-8"))
            digest.update(str(array.dtype).encode("ascii"))
            digest.update(str(array.shape).encode("ascii"))
            digest.update(array.tobytes())


def artifact_digest(
    params_path: str | os.PathLike[str],
    emb_tables_path: str | os.PathLike[str] | None = None,
) -> str:
    """sha256 over every array of the parameter file, then the three embedding tables."""
    digest = hashlib.sha256()
    _digest_npz_arrays(digest, params_path, keys=None)
    if emb_tables_path is not None:
        _digest_npz_arrays(digest, emb_tables_path, keys=EMBEDDING_TABLE_KEYS)
    return digest.hexdigest()[:_MODEL_VERSION_DIGEST_HEX]


def checkpoint_model_version(
    params_path: str | os.PathLike[str],
    emb_tables_path: str | os.PathLike[str] | None = None,
) -> str:
    """``<label>@<digest>`` for a checkpoint plus the embedding tables it is served with.

    label: the bundle directory name for ``step-*/model_params.npz`` bundles, otherwise
    the parameter file stem (``retrieval_params_step200``).  digest: content hash of the
    parameter arrays followed by the embedding tables when they are used.  Training
    writes the same value into ``metadata.json`` so operators can read it without
    starting a gateway; the gateway recomputes it from what it actually loaded.
    """
    params = Path(params_path)
    label = params.parent.name if params.name == _BUNDLE_PARAMS_FILENAME else params.stem
    version = f"{label}@{artifact_digest(params, emb_tables_path)}"
    if version == RANDOM_MODEL_VERSION or not label:
        raise ValueError(f"invalid model version label derived from {params}")
    return version


def supported_actions_header(action_enums: Iterable[int] | None = None) -> str:
    """Format supported proto ActionName values for gRPC metadata."""
    values = (
        NONZERO_WEIGHT_ACTION_ENUMS
        if action_enums is None
        else sorted(set(action_enums))
    )
    return ",".join(str(value) for value in values)
