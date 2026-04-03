"""
QA Agent output models.

The QA agent runs deterministic validation checks and LLM-generated test cases
against the pipeline output. Results are structured for the orchestrator to
decide pass/fail and trigger fix cycles if needed.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class TestCase(BaseModel):
    """A single test case result."""

    test_id: str
    description: str
    result: Literal["pass", "fail"]
    message: str = ""


class Issue(BaseModel):
    """A validation issue found during QA."""

    severity: Literal["critical", "warning"]
    description: str
    suggested_fix: str = ""


class QAMetrics(BaseModel):
    """Execution metrics captured during QA validation."""

    rows_processed: int = 0
    execution_time_ms: int = 0
    null_rate: float = 0.0
    duplicate_rate: float = 0.0


class QAOutput(BaseModel):
    """Structured output from the QA Agent.

    Attributes:
        test_cases: All executed test cases with pass/fail results.
        validation_status: Overall validation verdict.
        issues_found: List of issues with severity and suggested fixes.
        metrics: Execution metrics from the validation run.
    """

    test_cases: list[TestCase] = Field(default_factory=list)
    validation_status: Literal["pass", "fail"] = "fail"
    issues_found: list[Issue] = Field(default_factory=list)
    metrics: QAMetrics = Field(default_factory=QAMetrics)
