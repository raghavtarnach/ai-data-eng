"""
Error taxonomy for the AI Data Engineering System.

All errors across all components are classified into exactly one of three types.
The orchestrator uses this classification to decide retry, fix-cycle, or halt behavior.
"""

from __future__ import annotations

from enum import Enum


class ErrorType(str, Enum):
    """Error classification enum.

    RETRYABLE_ERROR: Transient failure (network timeout, LLM rate limit).
        Orchestrator retries with exponential backoff, max 3 attempts.

    VALIDATION_FAILURE: Output did not pass QA checks (schema mismatch, null check).
        Orchestrator sends failure detail back to upstream agent for a fix attempt.
        Max 3 fix cycles.

    FATAL_ERROR: Unrecoverable failure (sandbox crash, missing credentials).
        Orchestrator halts the run, persists final state, emits alert. No retry.
    """

    RETRYABLE_ERROR = "RETRYABLE_ERROR"
    VALIDATION_FAILURE = "VALIDATION_FAILURE"
    FATAL_ERROR = "FATAL_ERROR"
