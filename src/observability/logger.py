"""
Structured JSON logging for the AI Data Engineering System.

Every log entry includes: run_id, project_id, stage, agent, event_type,
duration_ms, status, timestamp. Sensitive fields (connection_ref values,
secrets) are never logged — only env var names appear.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from src.config import settings


class StructuredFormatter(logging.Formatter):
    """JSON formatter that outputs one JSON object per log line.

    Fields:
        timestamp, level, logger, message, + any extra fields passed
        via the `extra` dict on the log call.
    """

    # Fields that should never appear in logs
    REDACTED_PATTERNS: set[str] = {
        "password",
        "secret",
        "api_key",
        "token",
        "credential",
        "connection_string",
    }

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Merge extra fields (passed via logging extra={...})
        if hasattr(record, "__dict__"):
            for key, value in record.__dict__.items():
                if key not in logging.LogRecord.__dict__ and not key.startswith("_"):
                    # Redact sensitive fields
                    if any(p in key.lower() for p in self.REDACTED_PATTERNS):
                        log_entry[key] = "***REDACTED***"
                    else:
                        log_entry[key] = value

        # Remove standard LogRecord noise
        for noise_key in (
            "msg", "args", "created", "filename", "funcName", "levelno",
            "lineno", "module", "msecs", "pathname", "process", "processName",
            "relativeCreated", "stack_info", "thread", "threadName",
            "exc_info", "exc_text", "name", "levelname", "message",
            "taskName",
        ):
            log_entry.pop(noise_key, None)

        return json.dumps(log_entry, default=str)


def get_logger(name: str) -> logging.Logger:
    """Get a structured JSON logger.

    Args:
        name: Logger name (typically __name__).

    Returns:
        Configured logger instance with JSON output to stderr.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, settings.observability.log_level.upper(), logging.INFO))

    return logger


class RunLogger:
    """Context-aware logger that automatically includes run_id and project_id.

    Use this in the orchestrator and agents to avoid repeating context fields.
    """

    def __init__(self, logger: logging.Logger, run_id: str, project_id: str):
        self._logger = logger
        self._run_id = run_id
        self._project_id = project_id

    def _enrich(self, extra: dict[str, Any] | None) -> dict[str, Any]:
        enriched = {"run_id": self._run_id, "project_id": self._project_id}
        if extra:
            enriched.update(extra)
        return enriched

    def info(self, msg: str, extra: dict[str, Any] | None = None) -> None:
        self._logger.info(msg, extra=self._enrich(extra))

    def warning(self, msg: str, extra: dict[str, Any] | None = None) -> None:
        self._logger.warning(msg, extra=self._enrich(extra))

    def error(self, msg: str, extra: dict[str, Any] | None = None) -> None:
        self._logger.error(msg, extra=self._enrich(extra))

    def debug(self, msg: str, extra: dict[str, Any] | None = None) -> None:
        self._logger.debug(msg, extra=self._enrich(extra))
