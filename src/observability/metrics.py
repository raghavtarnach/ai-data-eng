"""
Metrics collection for the AI Data Engineering System.

Collects per-run metrics as defined in spec §10.3. Metrics are stored
in-memory per run and can be exported to monitoring systems.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunMetrics:
    """Metrics collected for a single run.

    All timing values are in milliseconds.
    """

    run_id: str
    project_id: str

    # Timing
    run_start_time: float = field(default_factory=time.monotonic)
    run_duration_ms: int = 0
    stage_durations_ms: dict[str, int] = field(default_factory=dict)

    # Counters
    retry_counts: dict[str, int] = field(default_factory=dict)
    token_usage: dict[str, int] = field(default_factory=dict)  # agent_name → tokens

    # Gauges
    validation_pass_rate: float = 0.0
    sandbox_execution_times_ms: list[int] = field(default_factory=list)

    # Status
    run_status: str = "in_progress"  # success | failure | in_progress

    def record_stage_duration(self, stage: str, duration_ms: int) -> None:
        """Record the duration of a completed stage."""
        self.stage_durations_ms[stage] = duration_ms

    def record_retry(self, stage: str) -> None:
        """Increment the retry counter for a stage."""
        self.retry_counts[stage] = self.retry_counts.get(stage, 0) + 1

    def record_token_usage(self, agent_name: str, tokens: int) -> None:
        """Record token consumption for an agent call."""
        self.token_usage[agent_name] = self.token_usage.get(agent_name, 0) + tokens

    def record_sandbox_execution(self, execution_time_ms: int) -> None:
        """Record a sandbox execution time."""
        self.sandbox_execution_times_ms.append(execution_time_ms)

    def record_validation_result(self, passed: int, total: int) -> None:
        """Update the validation pass rate."""
        self.validation_pass_rate = passed / max(total, 1)

    def finalize(self, status: str) -> None:
        """Finalize the run metrics with total duration and status."""
        self.run_duration_ms = int((time.monotonic() - self.run_start_time) * 1000)
        self.run_status = status

    def to_dict(self) -> dict[str, Any]:
        """Export metrics as a dictionary for logging/export."""
        return {
            "run_id": self.run_id,
            "project_id": self.project_id,
            "run_duration_ms": self.run_duration_ms,
            "stage_durations_ms": self.stage_durations_ms,
            "retry_counts": self.retry_counts,
            "token_usage": self.token_usage,
            "total_tokens": sum(self.token_usage.values()),
            "validation_pass_rate": self.validation_pass_rate,
            "sandbox_execution_times_ms": self.sandbox_execution_times_ms,
            "avg_sandbox_time_ms": (
                sum(self.sandbox_execution_times_ms) // max(len(self.sandbox_execution_times_ms), 1)
            ),
            "run_status": self.run_status,
        }


class MetricsCollector:
    """In-memory metrics collector.

    Tracks metrics across multiple runs. Can be extended to export
    to Prometheus, Azure Monitor, etc.
    """

    def __init__(self):
        self._runs: dict[str, RunMetrics] = {}

    def create_run_metrics(self, run_id: str, project_id: str) -> RunMetrics:
        """Create and register metrics for a new run."""
        metrics = RunMetrics(run_id=run_id, project_id=project_id)
        self._runs[run_id] = metrics
        return metrics

    def get_run_metrics(self, run_id: str) -> RunMetrics | None:
        """Get metrics for a specific run."""
        return self._runs.get(run_id)

    def get_all_metrics(self) -> list[dict[str, Any]]:
        """Export all run metrics as dictionaries."""
        return [m.to_dict() for m in self._runs.values()]


# Singleton
metrics_collector = MetricsCollector()
