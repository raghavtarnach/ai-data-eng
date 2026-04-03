"""
Project-level models: input schema, state model, and stage definitions.

These models represent the top-level data flow:
    ProjectInput (client request) → Orchestrator → ProjectState (tracked throughout)
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

from src.models.errors import ErrorType


class StageEnum(str, Enum):
    """Pipeline stage identifiers.

    These define the possible stages a project run can move through.
    The actual execution order is determined by the PM agent's task_graph,
    NOT by the order of this enum.
    """

    REQUIREMENT_ANALYSIS = "REQUIREMENT_ANALYSIS"
    ARCHITECTURE_DESIGN = "ARCHITECTURE_DESIGN"
    IMPLEMENTATION = "IMPLEMENTATION"
    VALIDATION = "VALIDATION"
    DEPLOYMENT = "DEPLOYMENT"
    DOCUMENTATION = "DOCUMENTATION"


class RunStatus(str, Enum):
    """Overall run lifecycle status."""

    IN_PROGRESS = "IN_PROGRESS"
    WAITING_FOR_CLARIFICATION = "WAITING_FOR_CLARIFICATION"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# ─── Input Models ───────────────────────────────────────────────────────────


class DataSource(BaseModel):
    """A data source referenced by the project.

    Attributes:
        name: Human-readable source name.
        type: Source type classification.
        connection_ref: Environment variable name holding the connection string.
            NEVER the actual connection string.
    """

    name: str
    type: Literal["blob", "sql", "api", "stream"]
    connection_ref: str = Field(
        description="Env var name for the connection string — never the value itself"
    )


class ProjectConstraints(BaseModel):
    """Constraints governing the project execution.

    Attributes:
        performance: Expected performance characteristics (e.g., "<5 min for 1M rows").
        cost: Budget ceiling (e.g., "$50/month").
        tools: Comma-separated list of allowed tools/frameworks.
    """

    performance: str = ""
    cost: str = ""
    tools: str = ""


class ProjectInput(BaseModel):
    """Top-level project input — the client request that kicks off a run.

    This schema is the single entry point for the orchestrator. Every field
    is validated before the first agent is dispatched.
    """

    project_id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str = Field(default_factory=lambda: str(uuid4()))
    project_name: str
    client_requirements: str
    data_sources: list[DataSource] = Field(default_factory=list)
    target_system: str = ""
    expected_output: str = ""
    constraints: ProjectConstraints = Field(default_factory=ProjectConstraints)


# ─── State Models ───────────────────────────────────────────────────────────


class ErrorRecord(BaseModel):
    """A single error event recorded during a run."""

    error_type: ErrorType
    stage: StageEnum
    message: str
    recovery_action: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class StageArtifact(BaseModel):
    """Captured output of a completed stage."""

    output: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ProjectState(BaseModel):
    """Full state of a project run — persisted after every stage transition.

    This is the source of truth for the orchestrator. On restart, the
    orchestrator loads this state and resumes from current_stage.
    """

    run_id: str
    project_id: str
    status: RunStatus = RunStatus.IN_PROGRESS
    current_stage: Optional[StageEnum] = None
    completed_stages: list[StageEnum] = Field(default_factory=list)
    pending_stages: list[StageEnum] = Field(default_factory=list)
    retry_counts: dict[str, int] = Field(default_factory=dict)
    artifacts: dict[str, StageArtifact] = Field(default_factory=dict)
    errors: list[ErrorRecord] = Field(default_factory=list)
    # Additional context carried forward between agents
    context: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def record_stage_completion(self, stage: StageEnum, output: dict[str, Any]) -> None:
        """Mark a stage as completed and store its artifact."""
        self.completed_stages.append(stage)
        if stage in self.pending_stages:
            self.pending_stages.remove(stage)
        self.artifacts[stage.value] = StageArtifact(output=output)
        self.updated_at = datetime.now(timezone.utc)

    def record_error(self, error: ErrorRecord) -> None:
        """Append an error to the run's error log."""
        self.errors.append(error)
        self.updated_at = datetime.now(timezone.utc)

    def increment_retry(self, stage: StageEnum) -> int:
        """Increment and return the retry counter for a given stage."""
        key = stage.value
        self.retry_counts[key] = self.retry_counts.get(key, 0) + 1
        self.updated_at = datetime.now(timezone.utc)
        return self.retry_counts[key]
