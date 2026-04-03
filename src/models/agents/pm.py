"""
Project Manager Agent output models.

The PM agent parses client requirements and produces an execution DAG
(task_graph) that the orchestrator uses to dispatch agents.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class TaskNode(BaseModel):
    """A single node in the execution task graph.

    Attributes:
        task_id: Unique identifier for this task within the graph.
        name: Human-readable task description.
        agent: Which agent type is responsible for this task.
        depends_on: List of task_ids that must complete before this task runs.
        can_run_parallel: Whether this task can run concurrently with siblings.
    """

    task_id: str
    name: str
    agent: Literal["ARCHITECT", "ENGINEER", "QA", "DEVOPS", "DOCS"]
    depends_on: list[str] = Field(default_factory=list)
    can_run_parallel: bool = False


class PMOutput(BaseModel):
    """Structured output from the Project Manager Agent.

    Attributes:
        task_graph: The execution DAG — source of truth for orchestrator ordering.
        execution_plan: Prose summary of the plan for human review.
        assumptions: Explicit assumptions made to fill ambiguities. Every gap
            in the client requirements MUST produce an entry here.
        clarifications_needed: Questions that must be answered by the caller
            before the orchestrator proceeds. If non-empty, the run halts.
    """

    task_graph: list[TaskNode] = Field(min_length=1)
    execution_plan: str
    assumptions: list[str] = Field(default_factory=list)
    clarifications_needed: list[str] = Field(default_factory=list)
