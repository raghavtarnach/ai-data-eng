"""
Unit tests for the retry handler.
"""

from __future__ import annotations

import pytest

from src.models.errors import ErrorType
from src.models.project import ProjectState, StageEnum
from src.orchestrator.retry import RetryHandler


@pytest.fixture
def handler():
    return RetryHandler(max_retries=3, max_fix_cycles=3)


@pytest.fixture
def state():
    return ProjectState(run_id="test-run", project_id="test-project")


class TestRetryHandler:
    def test_backoff_formula(self, handler):
        """Backoff should be 2^attempt seconds."""
        assert handler.get_backoff_seconds(1) == 2.0
        assert handler.get_backoff_seconds(2) == 4.0
        assert handler.get_backoff_seconds(3) == 8.0

    def test_can_retry_initial(self, handler, state):
        """Fresh state should allow retries."""
        assert handler.can_retry(state, StageEnum.IMPLEMENTATION)

    def test_can_retry_after_max(self, handler, state):
        """After max retries, can_retry should return False."""
        for _ in range(3):
            state.increment_retry(StageEnum.IMPLEMENTATION)
        assert not handler.can_retry(state, StageEnum.IMPLEMENTATION)

    def test_can_fix_cycle_initial(self, handler, state):
        assert handler.can_fix_cycle(state, StageEnum.VALIDATION)

    def test_can_fix_cycle_after_max(self, handler, state):
        for _ in range(3):
            state.increment_retry(StageEnum.VALIDATION)
        assert not handler.can_fix_cycle(state, StageEnum.VALIDATION)

    def test_should_escalate_fatal(self, handler, state):
        """FATAL_ERROR always escalates."""
        assert handler.should_escalate(state, StageEnum.IMPLEMENTATION, ErrorType.FATAL_ERROR)

    def test_should_escalate_retryable_under_limit(self, handler, state):
        """RETRYABLE_ERROR under limit should NOT escalate."""
        assert not handler.should_escalate(
            state, StageEnum.IMPLEMENTATION, ErrorType.RETRYABLE_ERROR
        )

    def test_should_escalate_retryable_at_limit(self, handler, state):
        """RETRYABLE_ERROR at limit should escalate."""
        for _ in range(3):
            state.increment_retry(StageEnum.IMPLEMENTATION)
        assert handler.should_escalate(
            state, StageEnum.IMPLEMENTATION, ErrorType.RETRYABLE_ERROR
        )

    def test_should_escalate_validation_under_limit(self, handler, state):
        assert not handler.should_escalate(
            state, StageEnum.VALIDATION, ErrorType.VALIDATION_FAILURE
        )

    def test_independent_stage_counters(self, handler, state):
        """Retries for different stages should be independent."""
        state.increment_retry(StageEnum.IMPLEMENTATION)
        state.increment_retry(StageEnum.IMPLEMENTATION)
        state.increment_retry(StageEnum.IMPLEMENTATION)

        # IMPLEMENTATION exhausted
        assert not handler.can_retry(state, StageEnum.IMPLEMENTATION)
        # VALIDATION still fresh
        assert handler.can_retry(state, StageEnum.VALIDATION)
