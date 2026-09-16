# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
召回向量索引文件格式（离线编码 → 在线网关）。

`scripts/build_retrieval_index.py` 用候选塔把真实帖子编码成向量后写出一个 ``.npz``，
`services/grpc_gateway.RetrievalEngine` 启动时加载，并按 mtime 定期热替换。文件是
离线任务和在线服务之间唯一的合同，所以这里集中做三件事：

1. 固定字段和 dtype（`schema_version` 只增不改）；
2. 校验 ID 形状（24 位小写 hex ObjectId，与 home-mixer 边界一致）、去重、向量有限；
3. 原子写（先写临时文件再 ``os.replace``），保证网关的刷新线程永远读不到半个文件。

索引和 retrieval checkpoint 一一对应：``model_version`` 记录编码所用的检查点，网关
拒绝加载与自身模型版本不一致的索引，避免"新模型 + 旧向量"这种静默错配。
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np

INDEX_SCHEMA_VERSION = 1

_OBJECT_ID_RE = re.compile(r"^[0-9a-f]{24}$")


def is_object_id(value: str) -> bool:
    """与 Rust `ObjectId::parse` 同源的形状校验：24 位小写 hex，全零（nil）不算合法。"""
    return bool(_OBJECT_ID_RE.match(value)) and value != "0" * 24


class RetrievalIndexError(ValueError):
    """索引文件不满足合同（字段缺失、ID 非法、形状不一致、版本不识别）。"""


@dataclass(frozen=True)
class RetrievalIndex:
    """一份可直接 ``set_corpus`` 的物品向量表。"""

    post_ids: tuple[str, ...]
    author_ids: tuple[str, ...]
    embeddings: np.ndarray  # float32 [N, D]
    model_version: str
    built_at_ms: int

    def __post_init__(self) -> None:
        self.validate()

    def __len__(self) -> int:
        return len(self.post_ids)

    @property
    def dim(self) -> int:
        return int(self.embeddings.shape[1])

    def describe(self) -> str:
        return (
            f"{len(self)} posts, dim {self.dim}, model {self.model_version}, "
            f"built_at_ms {self.built_at_ms}"
        )

    def validate(self) -> None:
        if not isinstance(self.embeddings, np.ndarray) or self.embeddings.ndim != 2:
            raise RetrievalIndexError("embeddings must be a 2-D array [N, D]")
        if self.embeddings.dtype != np.float32:
            raise RetrievalIndexError(f"embeddings must be float32, got {self.embeddings.dtype}")
        n = self.embeddings.shape[0]
        if len(self.post_ids) != n or len(self.author_ids) != n:
            raise RetrievalIndexError(
                f"post_ids ({len(self.post_ids)}), author_ids ({len(self.author_ids)}) and "
                f"embeddings ({n}) must have the same length"
            )
        if not np.all(np.isfinite(self.embeddings)):
            raise RetrievalIndexError("embeddings contain NaN or Inf")
        if not self.model_version or not self.model_version.strip():
            raise RetrievalIndexError("model_version must be non-empty")
        if self.built_at_ms <= 0:
            raise RetrievalIndexError("built_at_ms must be positive")
        seen: set[str] = set()
        for post_id, author_id in zip(self.post_ids, self.author_ids):
            if not is_object_id(post_id):
                raise RetrievalIndexError(f"post_id is not a 24-hex ObjectId: {post_id!r}")
            if not is_object_id(author_id):
                raise RetrievalIndexError(
                    f"author_id is not a 24-hex ObjectId: {author_id!r} (post {post_id})"
                )
            if post_id in seen:
                raise RetrievalIndexError(f"duplicate post_id: {post_id}")
            seen.add(post_id)

    # ── 文件 I/O ────────────────────────────────────────────────────────────

    def save(self, path: str | os.PathLike[str]) -> Path:
        """原子写出 ``.npz``；返回最终路径。"""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        try:
            with open(tmp_path, "wb") as f:
                np.savez(
                    f,
                    schema_version=np.int64(INDEX_SCHEMA_VERSION),
                    post_ids=np.asarray(self.post_ids, dtype="U24"),
                    author_ids=np.asarray(self.author_ids, dtype="U24"),
                    embeddings=self.embeddings,
                    model_version=np.str_(self.model_version),
                    built_at_ms=np.int64(self.built_at_ms),
                )
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, target)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
        return target

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "RetrievalIndex":
        try:
            with np.load(path, allow_pickle=False) as data:
                keys = set(data.files)
                required = {
                    "schema_version",
                    "post_ids",
                    "author_ids",
                    "embeddings",
                    "model_version",
                    "built_at_ms",
                }
                missing = sorted(required - keys)
                if missing:
                    raise RetrievalIndexError(f"index is missing fields: {', '.join(missing)}")
                schema_version = int(data["schema_version"])
                if schema_version != INDEX_SCHEMA_VERSION:
                    raise RetrievalIndexError(
                        f"unsupported index schema_version {schema_version}; "
                        f"this gateway reads version {INDEX_SCHEMA_VERSION}"
                    )
                return cls(
                    post_ids=tuple(str(value) for value in data["post_ids"].tolist()),
                    author_ids=tuple(str(value) for value in data["author_ids"].tolist()),
                    embeddings=np.ascontiguousarray(data["embeddings"], dtype=np.float32),
                    model_version=str(data["model_version"]),
                    built_at_ms=int(data["built_at_ms"]),
                )
        except (OSError, ValueError, KeyError) as exc:
            if isinstance(exc, RetrievalIndexError):
                raise
            raise RetrievalIndexError(f"cannot read retrieval index {path}: {exc}") from exc


def now_ms() -> int:
    return int(time.time() * 1000)
