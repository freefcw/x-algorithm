from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class TaskStatus(str, Enum):
    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"


class WorkStatus(str, Enum):
    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class TaskOutcome:
    status: TaskStatus
    output: dict[str, Any]
    reason: str | None = None

    @classmethod
    def success(cls, output: dict[str, Any]) -> TaskOutcome:
        return cls(status=TaskStatus.SUCCESS, output=output)

    @classmethod
    def skipped(cls, reason: str) -> TaskOutcome:
        return cls(status=TaskStatus.SKIPPED, output={}, reason=reason)

    @classmethod
    def failed(cls, reason: str) -> TaskOutcome:
        return cls(status=TaskStatus.FAILED, output={}, reason=reason)


@dataclass(frozen=True)
class WorkItem:
    id: str
    eligibilities: frozenset[str]
    attributes: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "eligibilities": sorted(self.eligibilities),
            "attributes": self.attributes,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> WorkItem:
        return cls(
            id=str(value["id"]),
            eligibilities=frozenset(str(item) for item in value.get("eligibilities", [])),
            attributes=dict(value.get("attributes", {})),
        )


@dataclass(frozen=True)
class WorkResult:
    id: str
    plan: str
    status: WorkStatus
    outputs: dict[str, dict[str, Any]]
    task_statuses: dict[str, TaskStatus]
    errors: dict[str, str]
    started_at: str
    finished_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "plan": self.plan,
            "status": self.status.value,
            "outputs": self.outputs,
            "task_statuses": {
                name: status.value for name, status in self.task_statuses.items()
            },
            "errors": self.errors,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> WorkResult:
        return cls(
            id=str(value["id"]),
            plan=str(value["plan"]),
            status=WorkStatus(value["status"]),
            outputs={
                str(name): dict(output)
                for name, output in dict(value.get("outputs", {})).items()
            },
            task_statuses={
                str(name): TaskStatus(status)
                for name, status in dict(value.get("task_statuses", {})).items()
            },
            errors={
                str(name): str(error)
                for name, error in dict(value.get("errors", {})).items()
            },
            started_at=str(value["started_at"]),
            finished_at=str(value["finished_at"]),
        )
