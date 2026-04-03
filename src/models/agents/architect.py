"""
Solution Architect Agent output models.

The architect designs the end-to-end system architecture, selects tools,
and documents design decisions with rationale.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class TechnologyChoice(BaseModel):
    """A single technology selection with justification."""

    component: str
    tool: str
    rationale: str


class DesignDecision(BaseModel):
    """A design decision with alternatives considered."""

    decision: str
    alternatives_considered: list[str] = Field(default_factory=list)
    rationale: str


class ArchitectOutput(BaseModel):
    """Structured output from the Solution Architect Agent.

    Attributes:
        architecture_design: Full architecture description in markdown.
        technology_stack: List of technology choices with rationale.
        design_decisions: Explicit decisions with alternatives and justification.
        cost_estimate: Estimated cost breakdown.
        performance_notes: Performance characteristics and trade-offs.
    """

    architecture_design: str
    technology_stack: list[TechnologyChoice] = Field(default_factory=list)
    design_decisions: list[DesignDecision] = Field(default_factory=list)
    cost_estimate: str = ""
    performance_notes: str = ""
