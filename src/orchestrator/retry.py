"""
Retry logic with exponential backoff and escalation.

Implements the error taxonomy routing from spec §5:
    - RETRYABLE_ERROR: Backoff retry, max 3 attempts
    - VALIDATION_FAILURE: Fix cycle via upstream agent, max 3 cycles
    - FATAL_ERROR: Halt immediately

Backoff formula: wait = 2^attempt seconds (2s, 4s, 8s).
"""

from __future__ import annotations

import asyncio
from typing import Callable, Optional

from src.config import settings
from src.models.errors import ErrorType
from src.models.project import ErrorRecord, ProjectState, StageEnum
from src.observability.logger import get_logger

logger = get_logger(__name__)


class RetryExhaustedError(Exception):
    """Raised when all retries are exhausted for a stage."""

    def __init__(self, stage: str, attempts: int, last_error: str):
        self.stage = stage
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"Retry exhausted for stage {stage} after {attempts} attempts: {last_error}"
        )


class RetryHandler:
    """Handles retry logic and escalation for the orchestrator.

    Each stage maintains an independent retry counter. When the ceiling
    is reached, the handler escalates to FATAL_ERROR.
    """

    def __init__(
        self,
        max_retries: int = settings.max_retries,
        max_fix_cycles: int = settings.max_fix_cycles,
    ):
        self._max_retries = max_retries
        self._max_fix_cycles = max_fix_cycles

    def get_backoff_seconds(self, attempt: int) -> float:
        """Calculate exponential backoff wait time.

        Formula: wait = 2^attempt seconds.
            attempt 1 → 2s
            attempt 2 → 4s
            attempt 3 → 8s

        Args:
            attempt: Current attempt number (1-indexed).

        Returns:
            Wait time in seconds.
        """
        return float(2 ** attempt)

    def can_retry(self, state: ProjectState, stage: StageEnum) -> bool:
        """Check if a stage has retries remaining.

        Args:
            state: Current project state.
            stage: Stage to check.

        Returns:
            True if retry is allowed.
        """
        current_count = state.retry_counts.get(stage.value, 0)
        return current_count < self._max_retries

    def can_fix_cycle(self, state: ProjectState, stage: StageEnum) -> bool:
        """Check if a stage has fix cycles remaining (for VALIDATION_FAILURE).

        Args:
            state: Current project state.
            stage: Stage to check.

        Returns:
            True if another fix cycle is allowed.
        """
        current_count = state.retry_counts.get(stage.value, 0)
        return current_count < self._max_fix_cycles

    async def execute_with_retry(
        self,
        state: ProjectState,
        stage: StageEnum,
        execute_fn: Callable,
        error_type: ErrorType,
    ) -> None:
        """Execute a function with retry logic.

        This is used for RETRYABLE_ERROR handling. The function is called
        with exponential backoff until it succeeds or the retry ceiling
        is reached.

        Args:
            state: Current project state (retry counters are updated).
            stage: Stage being retried.
            execute_fn: Async callable to execute.
            error_type: Classification of the error triggering retry.

        Raises:
            RetryExhaustedError: When all retries are exhausted.
        """
        while self.can_retry(state, stage):
            attempt = state.increment_retry(stage)
            backoff = self.get_backoff_seconds(attempt)

            logger.info(
                f"Retrying stage {stage.value}",
                extra={
                    "run_id": state.run_id,
                    "stage": stage.value,
                    "attempt": attempt,
                    "backoff_seconds": backoff,
                    "max_retries": self._max_retries,
                },
            )

            await asyncio.sleep(backoff)

            try:
                await execute_fn()
                return  # Success — exit retry loop
            except Exception as e:
                state.record_error(
                    ErrorRecord(
                        error_type=ErrorType.RETRYABLE_ERROR,
                        stage=stage,
                        message=str(e),
                        recovery_action=f"Retry attempt {attempt}/{self._max_retries}",
                    )
                )

        # All retries exhausted — escalate
        raise RetryExhaustedError(
            stage=stage.value,
            attempts=state.retry_counts.get(stage.value, 0),
            last_error=state.errors[-1].message if state.errors else "Unknown",
        )

    def should_escalate(
        self, state: ProjectState, stage: StageEnum, error_type: ErrorType
    ) -> bool:
        """Determine if an error should be escalated to FATAL_ERROR.

        Returns True if:
            - Error is already FATAL_ERROR
            - RETRYABLE_ERROR and retry ceiling reached
            - VALIDATION_FAILURE and fix cycle ceiling reached

        Args:
            state: Current project state.
            stage: Stage with the error.
            error_type: Classification of the error.

        Returns:
            True if escalation to FATAL is required.
        """
        if error_type == ErrorType.FATAL_ERROR:
            return True

        if error_type == ErrorType.RETRYABLE_ERROR:
            return not self.can_retry(state, stage)

        if error_type == ErrorType.VALIDATION_FAILURE:
            return not self.can_fix_cycle(state, stage)

        return False
