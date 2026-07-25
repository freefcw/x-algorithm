from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from .contracts import WorkItem, WorkResult


class Source(Protocol):
    def poll(self) -> AsyncIterator[WorkItem]: ...


class Sink(Protocol):
    async def write(self, result: WorkResult) -> None: ...


class InMemorySink:
    def __init__(self) -> None:
        self.results: list[WorkResult] = []

    async def write(self, result: WorkResult) -> None:
        self.results.append(result)
