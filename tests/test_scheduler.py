"""
Unit tests for the DAG scheduler.
"""

from __future__ import annotations

import pytest

from src.orchestrator.scheduler import DAGScheduler, SchedulerError


@pytest.fixture
def scheduler():
    return DAGScheduler()


class TestDAGScheduler:
    def test_simple_linear_graph(self, scheduler):
        """A → B → C should produce 3 sequential batches."""
        graph = [
            {"task_id": "A", "name": "Step A", "agent": "ARCHITECT", "depends_on": [], "can_run_parallel": False},
            {"task_id": "B", "name": "Step B", "agent": "ENGINEER", "depends_on": ["A"], "can_run_parallel": False},
            {"task_id": "C", "name": "Step C", "agent": "QA", "depends_on": ["B"], "can_run_parallel": False},
        ]
        schedule = scheduler.build_schedule(graph)
        assert len(schedule) == 3
        assert schedule[0][0].task_id == "A"
        assert schedule[1][0].task_id == "B"
        assert schedule[2][0].task_id == "C"

    def test_parallel_tasks(self, scheduler):
        """A → (B, C parallel) → D should produce 3 batches with B,C in one."""
        graph = [
            {"task_id": "A", "name": "Step A", "agent": "ARCHITECT", "depends_on": [], "can_run_parallel": False},
            {"task_id": "B", "name": "Step B", "agent": "ENGINEER", "depends_on": ["A"], "can_run_parallel": True},
            {"task_id": "C", "name": "Step C", "agent": "QA", "depends_on": ["A"], "can_run_parallel": True},
            {"task_id": "D", "name": "Step D", "agent": "DOCS", "depends_on": ["B", "C"], "can_run_parallel": False},
        ]
        schedule = scheduler.build_schedule(graph)
        # A (sequential) → B,C (parallel batch) → D (sequential)
        assert len(schedule) == 3
        # The parallel batch should have 2 tasks
        parallel_batch = [b for b in schedule if len(b) > 1]
        assert len(parallel_batch) == 1
        assert len(parallel_batch[0]) == 2

    def test_single_task(self, scheduler):
        graph = [
            {"task_id": "A", "name": "Only task", "agent": "ARCHITECT", "depends_on": []},
        ]
        schedule = scheduler.build_schedule(graph)
        assert len(schedule) == 1
        assert schedule[0][0].task_id == "A"

    def test_empty_graph_raises(self, scheduler):
        with pytest.raises(SchedulerError, match="Empty task graph"):
            scheduler.build_schedule([])

    def test_cycle_detection(self, scheduler):
        graph = [
            {"task_id": "A", "name": "A", "agent": "ARCHITECT", "depends_on": ["B"]},
            {"task_id": "B", "name": "B", "agent": "ENGINEER", "depends_on": ["A"]},
        ]
        with pytest.raises(SchedulerError, match="Cycle detected"):
            scheduler.build_schedule(graph)

    def test_missing_dependency_raises(self, scheduler):
        graph = [
            {"task_id": "A", "name": "A", "agent": "ARCHITECT", "depends_on": ["NONEXISTENT"]},
        ]
        with pytest.raises(SchedulerError, match="unknown task"):
            scheduler.build_schedule(graph)

    def test_duplicate_task_id_raises(self, scheduler):
        graph = [
            {"task_id": "A", "name": "A1", "agent": "ARCHITECT", "depends_on": []},
            {"task_id": "A", "name": "A2", "agent": "ENGINEER", "depends_on": []},
        ]
        with pytest.raises(SchedulerError, match="Duplicate task_id"):
            scheduler.build_schedule(graph)

    def test_complex_diamond_graph(self, scheduler):
        """
        Diamond: A → B, A → C, B → D, C → D
        Should produce: [A], [B,C], [D]
        """
        graph = [
            {"task_id": "A", "name": "A", "agent": "ARCHITECT", "depends_on": [], "can_run_parallel": False},
            {"task_id": "B", "name": "B", "agent": "ENGINEER", "depends_on": ["A"], "can_run_parallel": True},
            {"task_id": "C", "name": "C", "agent": "QA", "depends_on": ["A"], "can_run_parallel": True},
            {"task_id": "D", "name": "D", "agent": "DOCS", "depends_on": ["B", "C"], "can_run_parallel": False},
        ]
        schedule = scheduler.build_schedule(graph)
        # Verify D comes after both B and C
        flat_order = [task.task_id for batch in schedule for task in batch]
        assert flat_order.index("D") > flat_order.index("B")
        assert flat_order.index("D") > flat_order.index("C")

    def test_multiple_roots(self, scheduler):
        """Multiple independent root tasks."""
        graph = [
            {"task_id": "A", "name": "A", "agent": "ARCHITECT", "depends_on": [], "can_run_parallel": True},
            {"task_id": "B", "name": "B", "agent": "ENGINEER", "depends_on": [], "can_run_parallel": True},
            {"task_id": "C", "name": "C", "agent": "QA", "depends_on": ["A", "B"], "can_run_parallel": False},
        ]
        schedule = scheduler.build_schedule(graph)
        # A and B should be in the first batch (parallel)
        assert len(schedule[0]) == 2
