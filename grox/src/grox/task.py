from abc import ABC, abstractmethod

from .contracts import TaskOutcome, WorkItem


class Task(ABC):
    name: str

    @abstractmethod
    async def run(
        self,
        item: WorkItem,
        dependency_outputs: dict[str, dict],
    ) -> TaskOutcome:
        """Execute one task without mutating shared execution state."""
