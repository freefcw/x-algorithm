import asyncio

import pytest

from grox.contracts import TaskOutcome, TaskStatus, WorkItem, WorkStatus
from grox.plan import Plan, TaskDefinition
from grox.task import Task


class RecordingTask(Task):
    def __init__(self, name: str, calls: list[str], outcome: TaskOutcome):
        self.name = name
        self.calls = calls
        self.outcome = outcome

    async def run(self, item: WorkItem, dependency_outputs: dict[str, dict]) -> TaskOutcome:
        self.calls.append(self.name)
        return self.outcome


class FailingTask(Task):
    name = "failing"

    async def run(self, item: WorkItem, dependency_outputs: dict[str, dict]) -> TaskOutcome:
        raise RuntimeError("model unavailable")


def run(plan: Plan, item: WorkItem):
    return asyncio.run(plan.execute(item))


def test_eligible_plan_runs_dependencies_before_dependents():
    calls: list[str] = []
    plan = Plan(
        name="metadata",
        required_eligibility="metadata",
        tasks={
            "load": TaskDefinition(
                task=RecordingTask(
                    "load", calls, TaskOutcome.success({"text": "hello"})
                )
            ),
            "measure": TaskDefinition(
                task=RecordingTask(
                    "measure", calls, TaskOutcome.success({"length": 5})
                ),
                dependencies=("load",),
            ),
        },
    )

    result = run(
        plan,
        WorkItem(id="post-1", eligibilities=frozenset({"metadata"}), attributes={}),
    )

    assert result.status == WorkStatus.SUCCESS
    assert calls == ["load", "measure"]
    assert result.outputs == {"load": {"text": "hello"}, "measure": {"length": 5}}


def test_ineligible_plan_returns_skipped_without_running_tasks():
    calls: list[str] = []
    plan = Plan(
        name="metadata",
        required_eligibility="metadata",
        tasks={
            "load": TaskDefinition(
                task=RecordingTask("load", calls, TaskOutcome.success({}))
            )
        },
    )

    result = run(plan, WorkItem(id="post-1", eligibilities=frozenset(), attributes={}))

    assert result.status == WorkStatus.SKIPPED
    assert calls == []


def test_skipped_dependency_skips_downstream_task():
    calls: list[str] = []
    plan = Plan(
        name="metadata",
        required_eligibility="metadata",
        tasks={
            "filter": TaskDefinition(
                task=RecordingTask("filter", calls, TaskOutcome.skipped("not applicable"))
            ),
            "emit": TaskDefinition(
                task=RecordingTask("emit", calls, TaskOutcome.success({"emitted": True})),
                dependencies=("filter",),
            ),
        },
    )

    result = run(
        plan,
        WorkItem(id="post-1", eligibilities=frozenset({"metadata"}), attributes={}),
    )

    assert result.status == WorkStatus.SKIPPED
    assert calls == ["filter"]
    assert result.task_statuses["emit"] == TaskStatus.SKIPPED


def test_task_exception_is_reported_and_skips_dependents():
    calls: list[str] = []
    plan = Plan(
        name="metadata",
        required_eligibility="metadata",
        tasks={
            "load": TaskDefinition(task=FailingTask()),
            "emit": TaskDefinition(
                task=RecordingTask("emit", calls, TaskOutcome.success({})),
                dependencies=("load",),
            ),
        },
    )

    result = run(
        plan,
        WorkItem(id="post-1", eligibilities=frozenset({"metadata"}), attributes={}),
    )

    assert result.status == WorkStatus.FAILED
    assert "model unavailable" in result.errors["load"]
    assert result.task_statuses["emit"] == TaskStatus.SKIPPED
    assert calls == []


def test_plan_rejects_unknown_dependencies():
    with pytest.raises(ValueError, match="unknown dependency"):
        Plan(
            name="invalid",
            required_eligibility="metadata",
            tasks={
                "emit": TaskDefinition(
                    task=RecordingTask("emit", [], TaskOutcome.success({})),
                    dependencies=("missing",),
                )
            },
        )


def test_plan_rejects_dependency_cycles():
    with pytest.raises(ValueError, match="dependency cycle"):
        Plan(
            name="cycle",
            required_eligibility="metadata",
            tasks={
                "a": TaskDefinition(
                    task=RecordingTask("a", [], TaskOutcome.success({})),
                    dependencies=("b",),
                ),
                "b": TaskDefinition(
                    task=RecordingTask("b", [], TaskOutcome.success({})),
                    dependencies=("a",),
                ),
            },
        )
