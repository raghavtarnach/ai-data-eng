"""
Docker sandbox executor.

All generated code (Python and SQL) executes inside an isolated Docker
container with hard resource limits, no network access, and pinned
dependencies. This module manages the container lifecycle and captures
stdout/stderr/exit codes.
"""

from __future__ import annotations

import time
from typing import Any, Literal, Optional

import docker
from docker.errors import ContainerError, ImageNotFound
from pydantic import BaseModel, Field

from src.config import settings
from src.observability.logger import get_logger

logger = get_logger(__name__)


# ─── Models ─────────────────────────────────────────────────────────────────


class ExecutionRequest(BaseModel):
    """Request to execute code in the sandbox.

    Attributes:
        code: Source code to execute.
        language: Execution language — determines the interpreter.
        context: Runtime variables injected as environment variables.
        timeout_seconds: Max execution time before the container is killed.
    """

    code: str
    language: Literal["python", "sql"]
    context: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: int = 300


class ExecutionResult(BaseModel):
    """Result from a sandbox execution.

    Attributes:
        status: Outcome classification.
        stdout: Captured standard output.
        stderr: Captured standard error.
        output_ref: Pointer to output artifact in storage (if any).
        execution_time_ms: Wall-clock execution time.
        exit_code: Container exit code.
    """

    status: Literal["success", "timeout", "error", "oom"]
    stdout: str = ""
    stderr: str = ""
    output_ref: Optional[str] = None
    execution_time_ms: int = 0
    exit_code: int = -1


# ─── Executor ───────────────────────────────────────────────────────────────


class SandboxExecutor:
    """Docker-based isolated code executor.

    Creates a fresh container per execution with:
        - 2 vCPU, 4 GB RAM limits
        - No network access
        - No host filesystem mounts
        - Pinned dependency set (baked into the image)
        - Configurable timeout (60s SQL, 300s Python)
    """

    def __init__(self):
        self._client = docker.from_env()
        self._image = settings.sandbox.image

    async def execute(self, request: ExecutionRequest) -> ExecutionResult:
        """Execute code in an isolated Docker container.

        Args:
            request: Execution request with code, language, context, timeout.

        Returns:
            ExecutionResult with status, output, timing, and exit code.
        """
        # Build the command based on language
        if request.language == "python":
            cmd = ["python", "-c", request.code]
            timeout = request.timeout_seconds or settings.sandbox.python_timeout
        elif request.language == "sql":
            # SQL is executed via a Python wrapper script inside the container
            wrapper = f"""
import sqlite3, sys, os
conn = sqlite3.connect(':memory:')
try:
    result = conn.executescript('''{request.code}''')
    conn.commit()
    cursor = conn.execute("SELECT * FROM sqlite_master WHERE type='table'")
    for row in cursor:
        print(row)
except Exception as e:
    print(str(e), file=sys.stderr)
    sys.exit(1)
finally:
    conn.close()
"""
            cmd = ["python", "-c", wrapper]
            timeout = request.timeout_seconds or settings.sandbox.sql_timeout
        else:
            return ExecutionResult(
                status="error",
                stderr=f"Unsupported language: {request.language}",
                exit_code=1,
            )

        # Prepare environment variables from context
        env_vars = {k: str(v) for k, v in request.context.items()}

        start_time = time.monotonic()

        try:
            container = self._client.containers.run(
                image=self._image,
                command=cmd,
                environment=env_vars,
                # Resource limits
                nano_cpus=int(settings.sandbox.cpu_limit * 1e9),
                mem_limit=settings.sandbox.memory_limit,
                # Security: no network, no host mounts
                network_disabled=settings.sandbox.network_disabled,
                # Timeout and cleanup
                detach=True,
                remove=False,  # We need to inspect before removing
            )

            # Wait for completion with timeout
            result = container.wait(timeout=timeout)
            exit_code = result.get("StatusCode", -1)

            # Capture output
            stdout = container.logs(stdout=True, stderr=False).decode("utf-8", errors="replace")
            stderr = container.logs(stdout=False, stderr=True).decode("utf-8", errors="replace")

            elapsed_ms = int((time.monotonic() - start_time) * 1000)

            # Determine status
            oom_killed = result.get("Error", "") or ""
            if "OOMKilled" in str(container.attrs.get("State", {})):
                status = "oom"
            elif exit_code == 0:
                status = "success"
            else:
                status = "error"

            # Cleanup container
            try:
                container.remove(force=True)
            except Exception:
                pass

            execution_result = ExecutionResult(
                status=status,
                stdout=stdout[:1_000_000],  # Cap at 1MB for safety
                stderr=stderr[:500_000],
                execution_time_ms=elapsed_ms,
                exit_code=exit_code,
            )

            logger.info(
                "Sandbox execution completed",
                extra={
                    "language": request.language,
                    "status": status,
                    "exit_code": exit_code,
                    "execution_time_ms": elapsed_ms,
                },
            )

            return execution_result

        except docker.errors.ContainerError as e:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            return ExecutionResult(
                status="error",
                stderr=str(e),
                execution_time_ms=elapsed_ms,
                exit_code=e.exit_status,
            )

        except Exception as e:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            error_msg = str(e)

            # Check if timeout
            if "timed out" in error_msg.lower() or "timeout" in error_msg.lower():
                return ExecutionResult(
                    status="timeout",
                    stderr=f"Execution timed out after {timeout}s",
                    execution_time_ms=elapsed_ms,
                    exit_code=124,
                )

            return ExecutionResult(
                status="error",
                stderr=error_msg,
                execution_time_ms=elapsed_ms,
                exit_code=1,
            )
