"""
DAG Scheduler — topological sort with parallel batching.

Reads the task_graph produced by the PM Agent and converts it into an
ordered schedule of execution batches. Tasks within a batch run concurrently;
batches run sequentially.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any

from src.observability.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ScheduledTask:
    """A task ready for dispatch."""

    task_id: str
    name: str
    agent: str
    can_run_parallel: bool
    depends_on: list[str]


class SchedulerError(Exception):
    """Raised when the task graph is invalid."""

    pass


class DAGScheduler:
    """Converts a task_graph into ordered execution batches.

    Algorithm:
        1. Build adjacency list and in-degree map from task_graph
        2. Validate: no cycles, all depends_on references exist
        3. Kahn's algorithm for topological sort
        4. Group into batches: tasks with in-degree 0 at the same level
           AND can_run_parallel=True run together

    Example:
        task_graph: A→B, A→C (parallel), B→D, C→D
        Schedule: [[A], [B, C], [D]]
    """

    def build_schedule(
        self, task_graph: list[dict[str, Any]]
    ) -> list[list[ScheduledTask]]:
        """Convert a task_graph into ordered parallel batches.

        Args:
            task_graph: List of task nodes from PM Agent output.

        Returns:
            List of batches. Each batch is a list of ScheduledTask objects
            that can execute concurrently.

        Raises:
            SchedulerError: If the graph contains cycles or invalid references.
        """
        if not task_graph:
            raise SchedulerError("Empty task graph — nothing to schedule")

        # Build lookup and validate
        tasks: dict[str, dict[str, Any]] = {}
        for task in task_graph:
            task_id = task["task_id"]
            if task_id in tasks:
                raise SchedulerError(f"Duplicate task_id: {task_id}")
            tasks[task_id] = task

        # Validate all depends_on references exist
        for task in task_graph:
            for dep in task.get("depends_on", []):
                if dep not in tasks:
                    raise SchedulerError(
                        f"Task '{task['task_id']}' depends on unknown task '{dep}'"
                    )

        # Build in-degree map and adjacency list
        in_degree: dict[str, int] = {t["task_id"]: 0 for t in task_graph}
        dependents: dict[str, list[str]] = defaultdict(list)

        for task in task_graph:
            for dep in task.get("depends_on", []):
                dependents[dep].append(task["task_id"])
                in_degree[task["task_id"]] += 1

        # Kahn's algorithm — level-by-level topological sort
        schedule: list[list[ScheduledTask]] = []
        queue: deque[str] = deque(
            tid for tid, deg in in_degree.items() if deg == 0
        )
        processed = 0

        while queue:
            # All tasks in the current queue have resolved dependencies
            current_level = list(queue)
            queue.clear()

            # Split into parallel and sequential
            parallel_tasks: list[ScheduledTask] = []
            sequential_tasks: list[ScheduledTask] = []

            for tid in current_level:
                task_data = tasks[tid]
                scheduled = ScheduledTask(
                    task_id=tid,
                    name=task_data.get("name", tid),
                    agent=task_data["agent"],
                    can_run_parallel=task_data.get("can_run_parallel", False),
                    depends_on=task_data.get("depends_on", []),
                )

                if scheduled.can_run_parallel:
                    parallel_tasks.append(scheduled)
                else:
                    sequential_tasks.append(scheduled)

            # Group parallel tasks into one batch, sequential into individual batches
            if parallel_tasks:
                schedule.append(parallel_tasks)
            for task in sequential_tasks:
                schedule.append([task])

            # Update in-degrees for next level
            for tid in current_level:
                processed += 1
                for dependent in dependents[tid]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

        # Cycle detection
        if processed != len(task_graph):
            unprocessed = [
                tid for tid, deg in in_degree.items() if deg > 0
            ]
            raise SchedulerError(
                f"Cycle detected in task graph. Unresolved tasks: {unprocessed}"
            )

        logger.info(
            "Schedule built",
            extra={
                "total_tasks": len(task_graph),
                "total_batches": len(schedule),
                "parallel_batches": sum(1 for b in schedule if len(b) > 1),
            },
        )

        return schedule
