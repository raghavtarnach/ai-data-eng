"""
Base agent contracts.

Every agent in the system implements the Agent ABC with a typed run() method.
AgentInput and AgentOutput are the universal message envelope for inter-agent
communication, routed through the orchestrator.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class AgentInput(BaseModel):
    """Universal input envelope for all agents.

    Attributes:
        run_id: Unique execution run identifier.
        stage: The pipeline stage this agent is executing.
        payload: Stage-specific input data. Structure varies by agent.
    """

    run_id: str
    stage: str
    payload: dict[str, Any] = Field(default_factory=dict)


class AgentOutput(BaseModel):
    """Universal output envelope for all agents.

    Attributes:
        run_id: Echoed from input for traceability.
        stage: Echoed from input.
        status: Whether the agent completed successfully.
        error_type: Classification of failure (None if success).
        error_message: Human-readable error detail (None if success).
        data: Stage-specific output fields — see per-agent models.
    """

    run_id: str
    stage: str
    status: Literal["success", "failure"]
    error_type: Optional[Literal["RETRYABLE_ERROR", "VALIDATION_FAILURE", "FATAL_ERROR"]] = None
    error_message: Optional[str] = None
    data: dict[str, Any] = Field(default_factory=dict)


class Agent(ABC):
    """Abstract base class for all agents.

    Every agent declares its type (LLM-backed or deterministic) and
    implements a synchronous or async run() method that conforms to
    the AgentInput → AgentOutput contract.
    """

    agent_type: Literal["LLM_BACKED", "DETERMINISTIC"]

    @abstractmethod
    async def run(self, input: AgentInput) -> AgentOutput:
        """Execute the agent's core logic.

        Args:
            input: Typed input envelope with run context and payload.

        Returns:
            AgentOutput with status, optional error info, and stage-specific data.
        """
        ...
