from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from .contracts import TaskOutcome, WorkItem, WorkResult
from .plan import Plan, TaskDefinition
from .task import Task


class NormalizeTextTask(Task):
    name = "normalize"

    async def run(
        self,
        item: WorkItem,
        dependency_outputs: dict[str, dict],
    ) -> TaskOutcome:
        text = str(item.attributes.get("text", ""))
        return TaskOutcome.success({"normalized_text": " ".join(text.split())})


class MeasureTextTask(Task):
    name = "measure"

    async def run(
        self,
        item: WorkItem,
        dependency_outputs: dict[str, dict],
    ) -> TaskOutcome:
        normalized = str(dependency_outputs["normalize"]["normalized_text"])
        return TaskOutcome.success(
            {
                "character_count": len(normalized),
                "word_count": len(normalized.split()),
            }
        )


def build_demo_plan() -> Plan:
    return Plan(
        name="demo_metadata",
        required_eligibility="demo_metadata",
        tasks={
            "normalize": TaskDefinition(task=NormalizeTextTask()),
            "measure": TaskDefinition(
                task=MeasureTextTask(), dependencies=("normalize",)
            ),
        },
    )


def run_demo(input_path: Path, output_path: Path) -> WorkResult:
    item = WorkItem.from_dict(json.loads(input_path.read_text(encoding="utf-8")))
    result = asyncio.run(build_demo_plan().execute(item))
    output_path.write_text(
        json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the standalone Grox DAG demo")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    run_demo(args.input, args.output)


if __name__ == "__main__":
    main()
