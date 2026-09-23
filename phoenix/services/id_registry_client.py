"""Standalone Python client for the Identity Registry service.

The Registry is the single write authority for ObjectId ↔ Snowflake mappings.
This client is transport-only: gRPC is the production protocol, HTTP remains
available for migration tooling. Consumers validate every row (identity,
entity kind, mapping version, Snowflake range) before accepting it; the
adapter itself never calls the Registry because Home Mixer and xrex already
exchange numeric SnowflakeIds.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any, cast

import grpc

from services.model_contract import IDENTITY_MAPPING_VERSION


class IdentityRegistryClient:
    """Small synchronous client for the process-independent ID Registry."""

    def __init__(
        self,
        endpoint: str | None = None,
        timeout_seconds: float = 0.5,
        grpc_endpoint: str | None = None,
        allocation_token: str | None = None,
    ) -> None:
        # An explicit endpoint preserves the HTTP-only compatibility constructor
        # used by migration tools. Production defaults to gRPC; an RPC failure
        # is returned directly instead of issuing a second HTTP request.
        self._http_endpoint = endpoint.rstrip("/") if endpoint else None
        self._timeout_seconds = timeout_seconds
        self._grpc_endpoint = None if endpoint else (
            grpc_endpoint
            or os.getenv("ID_REGISTRY_GRPC_ADDR", "127.0.0.1:50072")
        ).removeprefix("http://").removeprefix("https://")
        self._allocation_token = allocation_token or os.getenv("ID_REGISTRY_ALLOCATION_TOKEN")
        self._grpc_stub = None
        if self._grpc_endpoint:
            from xai_proto import id_registry_pb2_grpc

            self._grpc_stub = id_registry_pb2_grpc.IdentityRegistryServiceStub(
                grpc.insecure_channel(self._grpc_endpoint)
            )
        self._cache: dict[tuple[str, str], int] = {}

    @staticmethod
    def _entity_kind_value(entity_kind: str, id_registry_pb2: Any) -> int:
        _validate_entity_kind(entity_kind)
        return {"User": id_registry_pb2.USER, "Post": id_registry_pb2.POST}[entity_kind]

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("ID Registry request deadline exceeded")
        return remaining

    def _resolve_batch_grpc(
        self,
        ids: list[tuple[str, str, int | None]],
        *,
        allocate: bool = False,
        timeout: float | None = None,
    ) -> list[tuple[str, str, int]]:
        from xai_proto import id_registry_pb2

        if self._grpc_stub is None:
            raise RuntimeError("ID Registry gRPC is not configured")
        request_ids = []
        for object_id, entity_kind, provided in ids:
            _validate_object_id(object_id)
            request_ids.append(
                id_registry_pb2.ResolveRequest(
                    object_id=object_id,
                    entity_kind=self._entity_kind_value(entity_kind, id_registry_pb2),
                    **({"snowflake_id": provided} if provided is not None else {}),
                )
            )
        request = id_registry_pb2.ResolveBatchRequest(ids=request_ids)
        # ResolveBatch is read-only on the server; provided ids and new
        # mappings must go through AllocateBatch.
        rpc = self._grpc_stub.AllocateBatch if allocate else self._grpc_stub.ResolveBatch
        metadata = (
            (("x-id-registry-allocation-token", self._allocation_token),)
            if allocate and self._allocation_token
            else None
        )
        rpc_timeout = self._timeout_seconds if timeout is None else timeout
        if metadata is None:
            response = rpc(request, timeout=rpc_timeout)
        else:
            response = rpc(request, timeout=rpc_timeout, metadata=metadata)
        if len(response.rows) != len(ids):
            raise RuntimeError("ID Registry returned a mismatched gRPC batch size")
        rows: list[tuple[str, str, int]] = []
        for row, (object_id, entity_kind, provided) in zip(response.rows, ids, strict=True):
            returned_kind = _entity_kind_name(row.entity_kind, id_registry_pb2)
            if row.object_id != object_id or returned_kind != entity_kind:
                raise RuntimeError("ID Registry returned a mismatched gRPC identity")
            if row.mapping_version != IDENTITY_MAPPING_VERSION:
                raise RuntimeError("ID Registry returned an unsupported mapping_version")
            value = row.snowflake_id
            if value <= 0 or value > 0x7FFF_FFFF_FFFF_FFFF:
                raise RuntimeError("ID Registry returned an invalid SnowflakeId")
            if provided is not None and value != provided:
                raise RuntimeError("ID Registry returned a mismatched provided SnowflakeId")
            rows.append((row.object_id, returned_kind, value))
        return rows

    def resolve(self, object_id: str, entity_kind: str) -> int:
        key = (entity_kind, object_id)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        self.resolve_batch([(object_id, entity_kind)])
        return self._cache[key]

    def resolve_batch(self, ids: list[tuple[str, str]]) -> dict[tuple[str, str], int]:
        """Resolve identities and return a mapping keyed by ``(kind, ObjectId)``."""
        self._resolve_or_allocate_batch(
            [(object_id, entity_kind, None) for object_id, entity_kind in ids],
            allocate=False,
        )
        return {
            (entity_kind, object_id): self._cache[(entity_kind, object_id)]
            for object_id, entity_kind in ids
        }

    def allocate_batch(
        self,
        ids: list[tuple[str, str, int | None]],
    ) -> None:
        """Allocate mappings, registering any caller-provided Snowflake as-is.

        Items with a provided snowflake id keep that exact value; items
        without one receive a freshly allocated id. This always goes through
        Allocate/AllocateBatch and requires a replica with allocation
        enabled.
        """
        self._resolve_or_allocate_batch(ids, allocate=True)

    def _resolve_or_allocate_batch(
        self,
        ids: list[tuple[str, str, int | None]],
        *,
        allocate: bool,
    ) -> None:
        deadline = time.monotonic() + self._timeout_seconds
        for object_id, entity_kind, provided in ids:
            _validate_object_id(object_id)
            _validate_entity_kind(entity_kind)
            if provided is not None:
                _validate_snowflake_id(provided)
        unique = list(dict.fromkeys(ids))
        missing = []
        for item in unique:
            cached = self._cache.get((item[0], item[1]))
            # A provided id must still be checked against the registry when
            # a stale or previously uncached entry exists.
            if cached is None or (item[2] is not None and cached != item[2]):
                missing.append(item)
        if not missing:
            return
        if self._grpc_stub is not None:
            try:
                rows = self._resolve_batch_grpc(
                    missing, allocate=allocate, timeout=self._remaining(deadline)
                )
            except grpc.RpcError as exc:
                raise RuntimeError(f"ID Registry gRPC request failed: {exc}") from exc
            for object_id, entity_kind, value in rows:
                self._cache[(entity_kind, object_id)] = value
            return
        if self._http_endpoint is None:
            raise RuntimeError("ID Registry gRPC is not configured")
        payload = {
            "ids": [
                {
                    "object_id": object_id,
                    "entity_kind": entity_kind,
                    **({"snowflake_id": provided} if provided is not None else {}),
                }
                for object_id, entity_kind, provided in missing
            ]
        }
        headers = {"content-type": "application/json"}
        if allocate and self._allocation_token:
            headers["x-id-registry-allocation-token"] = self._allocation_token
        request = urllib.request.Request(
            f"{self._http_endpoint}/v1/{'allocate' if allocate else 'resolve'}:batch",
            data=json.dumps(payload).encode(),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._remaining(deadline)) as response:
                rows = json.loads(response.read())
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"ID Registry is unavailable: {exc}") from exc
        if not isinstance(rows, list) or len(rows) != len(missing):
            raise RuntimeError("ID Registry returned a mismatched batch size")
        for row, (object_id, entity_kind, provided) in zip(rows, missing, strict=True):
            if not isinstance(row, dict):
                raise RuntimeError("ID Registry returned a malformed row")
            row = cast(dict[str, Any], row)
            if row.get("object_id") != object_id or row.get("entity_kind") != entity_kind:
                raise RuntimeError("ID Registry returned a mismatched identity")
            if row.get("mapping_version") != IDENTITY_MAPPING_VERSION:
                raise RuntimeError("ID Registry returned an unsupported mapping_version")
            value = row.get("snowflake_id")
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value <= 0
                or value > 0x7FFF_FFFF_FFFF_FFFF
            ):
                raise RuntimeError("ID Registry returned an invalid SnowflakeId")
            if provided is not None and value != provided:
                raise RuntimeError("ID Registry returned a mismatched provided SnowflakeId")
            self._cache[(entity_kind, object_id)] = value

    def _reverse_batch_grpc(
        self,
        ids: list[tuple[int, str]],
        *,
        timeout: float | None = None,
    ) -> list[str]:
        from xai_proto import id_registry_pb2

        if self._grpc_stub is None:
            raise RuntimeError("ID Registry gRPC is not configured")
        request_ids = []
        for snowflake_id, entity_kind in ids:
            _validate_snowflake_id(snowflake_id)
            request_ids.append(
                id_registry_pb2.ReverseRequest(
                    snowflake_id=snowflake_id,
                    entity_kind=self._entity_kind_value(entity_kind, id_registry_pb2),
                )
            )
        response = self._grpc_stub.ReverseBatch(
            id_registry_pb2.ReverseBatchRequest(ids=request_ids),
            timeout=self._timeout_seconds if timeout is None else timeout,
        )
        if len(response.rows) != len(ids):
            raise RuntimeError("ID Registry returned a mismatched gRPC batch size")
        result: list[str] = []
        for row, (snowflake_id, entity_kind) in zip(response.rows, ids, strict=True):
            returned_kind = _entity_kind_name(row.entity_kind, id_registry_pb2)
            if row.snowflake_id != snowflake_id or returned_kind != entity_kind:
                raise RuntimeError("ID Registry returned a mismatched gRPC identity")
            if row.mapping_version != IDENTITY_MAPPING_VERSION:
                raise RuntimeError("ID Registry returned an unsupported mapping_version")
            if not isinstance(row.object_id, str):
                raise RuntimeError("ID Registry returned an invalid ObjectId")
            try:
                _validate_object_id(row.object_id)
            except ValueError as exc:
                raise RuntimeError(f"ID Registry returned an invalid ObjectId: {exc}") from exc
            result.append(row.object_id)
        return result

    def reverse(self, snowflake_id: int, entity_kind: str) -> str:
        return self.reverse_batch([(snowflake_id, entity_kind)])[0]

    def reverse_batch(self, ids: list[tuple[int, str]]) -> list[str]:
        """Reverse SnowflakeIds to ObjectIds, checking order, kind, and version."""
        deadline = time.monotonic() + self._timeout_seconds
        if not ids:
            return []
        for snowflake_id, entity_kind in ids:
            _validate_snowflake_id(snowflake_id)
            _validate_entity_kind(entity_kind)
        if self._grpc_stub is not None:
            try:
                return self._reverse_batch_grpc(ids, timeout=self._remaining(deadline))
            except grpc.RpcError as exc:
                raise RuntimeError(f"ID Registry gRPC request failed: {exc}") from exc
        if self._http_endpoint is None:
            raise RuntimeError("ID Registry gRPC is not configured")
        payload = {
            "ids": [
                {"snowflake_id": snowflake_id, "entity_kind": entity_kind}
                for snowflake_id, entity_kind in ids
            ]
        }
        request = urllib.request.Request(
            f"{self._http_endpoint}/v1/reverse:batch",
            data=json.dumps(payload).encode(),
            headers={"content-type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._remaining(deadline)) as response:
                rows = json.loads(response.read())
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"ID Registry is unavailable: {exc}") from exc
        if not isinstance(rows, list) or len(rows) != len(ids):
            raise RuntimeError("ID Registry returned a mismatched batch size")
        object_ids = []
        for row, (snowflake_id, entity_kind) in zip(rows, ids, strict=True):
            if not isinstance(row, dict):
                raise RuntimeError("ID Registry returned a malformed row")
            row = cast(dict[str, Any], row)
            if row.get("snowflake_id") != snowflake_id:
                raise RuntimeError("ID Registry returned a mismatched SnowflakeId")
            if row.get("entity_kind") != entity_kind:
                raise RuntimeError("ID Registry returned a mismatched entity kind")
            if row.get("mapping_version") != IDENTITY_MAPPING_VERSION:
                raise RuntimeError("ID Registry returned an unsupported mapping_version")
            object_id = row.get("object_id")
            if not isinstance(object_id, str):
                raise RuntimeError("ID Registry returned a missing ObjectId")
            try:
                _validate_object_id(object_id)
            except ValueError as exc:
                raise RuntimeError(f"ID Registry returned an invalid ObjectId: {exc}") from exc
            object_ids.append(object_id)
        return object_ids


def _validate_object_id(value: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 24
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValueError(f"invalid ObjectId: {value!r}")


def _validate_entity_kind(value: str) -> None:
    if value not in {"User", "Post"}:
        raise ValueError(f"entity_kind must be User or Post, got {value!r}")


def _validate_snowflake_id(value: int) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
        or value > 0x7FFF_FFFF_FFFF_FFFF
    ):
        raise ValueError(f"invalid SnowflakeId: {value!r}")


def _entity_kind_name(value: int, id_registry_pb2: Any) -> str:
    try:
        return {id_registry_pb2.USER: "User", id_registry_pb2.POST: "Post"}[value]
    except (KeyError, AttributeError) as exc:
        raise RuntimeError("ID Registry returned an invalid entity_kind") from exc
