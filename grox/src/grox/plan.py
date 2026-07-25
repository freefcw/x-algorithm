from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

from .contracts import TaskOutcome, TaskStatus, WorkItem, WorkResult, WorkStatus
from .task import Task


@dataclass(frozen=True)
class TaskDefinition:
    task: Task
    dependencies: tuple[str, ...] = ()


class Plan:
    def __init__(
        self,
        name: str,
        required_eligibility: str,
        tasks: dict[str, TaskDefinition],
    ) -> None:
        if not name:
            raise ValueError("plan name must not be empty")
        if not tasks:
            raise ValueError("plan must contain at least one task")
        self.name = name
        self.required_eligibility = required_eligibility
        self.tasks = dict(tasks)
        self._validate_graph()

    async def execute(self, item: WorkItem) -> WorkResult:
        started_at = _now()
        if self.required_eligibility not in item.eligibilities:
            return WorkResult(
                id=item.id,
                plan=self.name,
                status=WorkStatus.SKIPPED,
                outputs={},
                task_statuses={},
                errors={},
                started_at=started_at,
                finished_at=_now(),
            )

        scheduled: dict[str, asyncio.Task[TaskOutcome]] = {}

        def schedule(task_name: str) -> asyncio.Task[TaskOutcome]:
            existing = scheduled.get(task_name)
            if existing is not None:
                return existing

            async def run_task() -> TaskOutcome:
                definition = self.tasks[task_name]
                dependency_outputs: dict[str, dict] = {}
                for dependency in definition.dependencies:
                    outcome = await schedule(dependency)
                    if outcome.status != TaskStatus.SUCCESS:
                        return TaskOutcome.skipped(
                            f"dependency {dependency} did not complete successfully"
                        )
                    dependency_outputs[dependency] = outcome.output
                try:
                    return await definition.task.run(item, dependency_outputs)
                except Exception as error:  # noqa: BLE001 - converted to a result envelope
                    return TaskOutcome.failed(str(error))

            scheduled[task_name] = asyncio.create_task(run_task(), name=task_name)
            return scheduled[task_name]

        for task_name in self.tasks:
            schedule(task_name)
        outcomes = dict(zip(scheduled, await asyncio.gather(*scheduled.values()), strict=True))
        task_statuses = {name: outcome.status for name, outcome in outcomes.items()}
        outputs = {
            name: outcome.output
            for name, outcome in outcomes.items()
            if outcome.status == TaskStatus.SUCCESS
        }
        errors = {
            name: outcome.reason or "task failed"
            for name, outcome in outcomes.items()
            if outcome.status == TaskStatus.FAILED
        }
        status = _work_status(task_statuses.values())
        return WorkResult(
            id=item.id,
            plan=self.name,
            status=status,
            outputs=outputs,
            task_statuses=task_statuses,
            errors=errors,
            started_at=started_at,
            finished_at=_now(),
        )

    def _validate_graph(self) -> None:
        for task_name, definition in self.tasks.items():
            for dependency in definition.dependencies:
                if dependency not in self.tasks:
                    raise ValueError(
                        f"task {task_name} has unknown dependency {dependency}"
                    )

        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(task_name: str) -> None:
            if task_name in visiting:
                raise ValueError(f"dependency cycle includes {task_name}")
            if task_name in visited:
                return
            visiting.add(task_name)
            for dependency in self.tasks[task_name].dependencies:
                visit(dependency)
            visiting.remove(task_name)
            visited.add(task_name)

        for task_name in self.tasks:
            visit(task_name)


def _work_status(statuses) -> WorkStatus:
    statuses = tuple(statuses)
    if TaskStatus.FAILED in statuses:
        return WorkStatus.FAILED
    if statuses and all(status == TaskStatus.SKIPPED for status in statuses):
        return WorkStatus.SKIPPED
    return WorkStatus.SUCCESS


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
