"""
Core Orchestrator Engine.

The central brain of the AI Data Engineering System. Reads the task_graph
from PM agent output, dispatches agents in dependency order, runs parallel
tasks concurrently, applies error taxonomy to all failures, and persists
state after every stage transition.

This is NOT a sequential script runner — it's a stateful DAG executor
with dynamic scheduling, retry logic, and failure propagation.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

from src.agents.architect_agent import ArchitectAgent
from src.agents.engineer_agent import EngineerAgent
from src.agents.pm_agent import PMAgent
from src.agents.qa_agent import QAValidationAgent
from src.agents.qa_test_gen import QATestGenAgent
from src.models.base import Agent, AgentInput, AgentOutput
from src.models.errors import ErrorType
from src.models.project import (
    ErrorRecord,
    ProjectInput,
    ProjectState,
    RunStatus,
    StageEnum,
)
from src.observability.logger import RunLogger, get_logger
from src.observability.metrics import RunMetrics, metrics_collector
from src.observability.tracing import trace_agent_call
from src.orchestrator.retry import RetryExhaustedError, RetryHandler
from src.orchestrator.scheduler import DAGScheduler, ScheduledTask, SchedulerError
from src.state.artifacts import ArtifactStore
from src.state.backend import StateBackend

logger = get_logger(__name__)

# Agent type → Stage mapping
AGENT_STAGE_MAP: dict[str, StageEnum] = {
    "ARCHITECT": StageEnum.ARCHITECTURE_DESIGN,
    "ENGINEER": StageEnum.IMPLEMENTATION,
    "QA": StageEnum.VALIDATION,
    "DEVOPS": StageEnum.DEPLOYMENT,
    "DOCS": StageEnum.DOCUMENTATION,
}


class OrchestratorError(Exception):
    """Fatal orchestrator error — run cannot continue."""

    pass


class Orchestrator:
    """Stateful multi-agent orchestrator.

    Lifecycle:
        1. Load or initialize state
        2. Dispatch PM Agent → get task_graph
        3. Build schedule from task_graph (DAG → batches)
        4. Execute batches: parallel tasks via asyncio.gather
        5. Handle failures via error taxonomy
        6. Persist state after every transition

    Supports:
        - Resume from crash (reads last persisted state)
        - Clarification halt (WAITING_FOR_CLARIFICATION status)
        - Parallel dispatch with FATAL cancellation propagation
        - Fix cycles: VALIDATION_FAILURE → upstream agent → re-validate
    """

    def __init__(
        self,
        state_backend: StateBackend,
        artifact_store: ArtifactStore,
    ):
        self._state_backend = state_backend
        self._artifact_store = artifact_store
        self._scheduler = DAGScheduler()
        self._retry_handler = RetryHandler()

        # Initialize agents
        self._pm_agent = PMAgent()
        self._agents: dict[str, Agent] = {
            "ARCHITECT": ArchitectAgent(),
            "ENGINEER": EngineerAgent(),
            "QA": QAValidationAgent(),
        }
        self._qa_test_gen = QATestGenAgent()

    async def run(self, project_input: ProjectInput) -> ProjectState:
        """Execute the full pipeline for a project.

        Args:
            project_input: Validated client request.

        Returns:
            Final ProjectState with all artifacts and results.

        Raises:
            OrchestratorError: On unrecoverable failures.
        """
        # Initialize metrics
        run_metrics = metrics_collector.create_run_metrics(
            run_id=project_input.run_id,
            project_id=project_input.project_id,
        )
        run_log = RunLogger(logger, project_input.run_id, project_input.project_id)

        # Load or initialize state
        state = await self._load_or_init_state(project_input)
        run_log.info("Orchestrator started", extra={"status": state.status.value})

        try:
            # ── Stage 1: Requirement Analysis ───────────────────────────
            if StageEnum.REQUIREMENT_ANALYSIS not in state.completed_stages:
                state.current_stage = StageEnum.REQUIREMENT_ANALYSIS
                await self._persist_state(state)

                stage_start = time.monotonic()

                pm_input = AgentInput(
                    run_id=state.run_id,
                    stage=StageEnum.REQUIREMENT_ANALYSIS.value,
                    payload={**project_input.model_dump(), **state.context},
                )

                pm_output = await self._dispatch_with_retry(
                    self._pm_agent, pm_input, state, StageEnum.REQUIREMENT_ANALYSIS
                )

                if pm_output.status == "failure":
                    return await self._halt_fatal(state, pm_output, run_metrics)

                # Check for clarifications
                clarifications = pm_output.data.get("clarifications_needed", [])
                if clarifications:
                    state.status = RunStatus.WAITING_FOR_CLARIFICATION
                    state.context["clarifications_needed"] = clarifications
                    state.record_stage_completion(
                        StageEnum.REQUIREMENT_ANALYSIS, pm_output.data
                    )
                    await self._persist_state(state)
                    run_log.info(
                        "Halting for clarification",
                        extra={"clarifications": clarifications},
                    )
                    return state

                state.record_stage_completion(
                    StageEnum.REQUIREMENT_ANALYSIS, pm_output.data
                )
                run_metrics.record_stage_duration(
                    StageEnum.REQUIREMENT_ANALYSIS.value,
                    int((time.monotonic() - stage_start) * 1000),
                )
                await self._persist_state(state)

            # ── Build execution schedule from task_graph ────────────────
            pm_data = state.artifacts.get(StageEnum.REQUIREMENT_ANALYSIS.value)
            if not pm_data:
                raise OrchestratorError("PM output missing from state — cannot build schedule")

            task_graph = pm_data.output.get("task_graph", [])

            try:
                schedule = self._scheduler.build_schedule(task_graph)
            except SchedulerError as e:
                state.record_error(
                    ErrorRecord(
                        error_type=ErrorType.FATAL_ERROR,
                        stage=StageEnum.REQUIREMENT_ANALYSIS,
                        message=f"Invalid task graph: {e}",
                    )
                )
                return await self._halt_fatal_with_message(
                    state, f"Invalid task graph: {e}", run_metrics
                )

            # ── Execute remaining stages per schedule ───────────────────
            # Build cumulative context for downstream agents
            accumulated_context: dict[str, Any] = {
                "project_input": project_input.model_dump(),
                "pm_output": pm_data.output,
            }

            for batch_idx, batch in enumerate(schedule):
                # Skip already-completed stages
                batch = [
                    task
                    for task in batch
                    if AGENT_STAGE_MAP.get(task.agent) not in state.completed_stages
                ]
                if not batch:
                    continue

                run_log.info(
                    f"Executing batch {batch_idx + 1}/{len(schedule)}",
                    extra={
                        "tasks": [t.task_id for t in batch],
                        "parallel": len(batch) > 1,
                    },
                )

                if len(batch) == 1:
                    # Single task — execute directly
                    result = await self._execute_task(
                        batch[0], state, accumulated_context, run_metrics, run_log
                    )
                    if result and result.status == "success":
                        accumulated_context[batch[0].agent.lower() + "_output"] = result.data
                else:
                    # Parallel batch — asyncio.gather with cancellation
                    results = await self._execute_parallel_batch(
                        batch, state, accumulated_context, run_metrics, run_log
                    )
                    for task, result in zip(batch, results):
                        if result and result.status == "success":
                            accumulated_context[task.agent.lower() + "_output"] = result.data

            # ── Finalize ────────────────────────────────────────────────
            state.status = RunStatus.COMPLETED
            await self._persist_state(state)
            await self._state_backend.update_latest_run(state.project_id, state.run_id)
            run_metrics.finalize("success")
            run_log.info("Run completed successfully", extra=run_metrics.to_dict())

            return state

        except RetryExhaustedError as e:
            return await self._halt_fatal_with_message(
                state, str(e), run_metrics
            )

        except OrchestratorError as e:
            return await self._halt_fatal_with_message(
                state, str(e), run_metrics
            )

        except Exception as e:
            logger.error(f"Unexpected orchestrator error: {e}", extra={"run_id": state.run_id})
            return await self._halt_fatal_with_message(
                state, f"Unexpected error: {e}", run_metrics
            )

    async def _execute_task(
        self,
        task: ScheduledTask,
        state: ProjectState,
        context: dict[str, Any],
        metrics: RunMetrics,
        run_log: RunLogger,
    ) -> Optional[AgentOutput]:
        """Execute a single scheduled task.

        Handles the full lifecycle: dispatch → validate → handle failure.
        For QA failures, triggers fix cycles with the upstream agent.
        """
        stage = AGENT_STAGE_MAP.get(task.agent)
        if not stage:
            run_log.warning(f"Unknown agent type: {task.agent}, skipping")
            return None

        agent = self._agents.get(task.agent)
        if not agent:
            run_log.warning(f"Agent {task.agent} not implemented (v2 scope), skipping")
            return None

        state.current_stage = stage
        await self._persist_state(state)

        stage_start = time.monotonic()

        agent_input = AgentInput(
            run_id=state.run_id,
            stage=stage.value,
            payload={**context},
        )

        output = await self._dispatch_with_retry(agent, agent_input, state, stage)

        if output.status == "success":
            state.record_stage_completion(stage, output.data)
            metrics.record_stage_duration(
                stage.value, int((time.monotonic() - stage_start) * 1000)
            )
            await self._persist_state(state)

            # Save artifacts
            await self._artifact_store.save_artifact(
                state.project_id,
                state.run_id,
                stage.value,
                f"{task.agent.lower()}_output.json",
                output.data,
            )

            return output

        # Handle failure
        if output.error_type == "VALIDATION_FAILURE" and task.agent == "QA":
            # Fix cycle: send failure back to Engineer, then re-validate
            return await self._run_fix_cycle(
                state, context, output, metrics, run_log
            )

        if output.error_type == "FATAL_ERROR":
            state.record_error(
                ErrorRecord(
                    error_type=ErrorType.FATAL_ERROR,
                    stage=stage,
                    message=output.error_message or "Fatal error",
                )
            )
            state.status = RunStatus.FAILED
            await self._persist_state(state)
            metrics.finalize("failure")
            raise OrchestratorError(output.error_message or "Fatal error")

        return output

    async def _run_fix_cycle(
        self,
        state: ProjectState,
        context: dict[str, Any],
        qa_output: AgentOutput,
        metrics: RunMetrics,
        run_log: RunLogger,
    ) -> Optional[AgentOutput]:
        """Run a fix cycle: VALIDATION_FAILURE → Engineer fix → QA re-validate.

        Max fix cycles controlled by retry handler.
        """
        engineer_agent = self._agents.get("ENGINEER")
        qa_agent = self._agents.get("QA")

        if not engineer_agent or not qa_agent:
            raise OrchestratorError("Cannot run fix cycle: missing ENGINEER or QA agent")

        while self._retry_handler.can_fix_cycle(state, StageEnum.VALIDATION):
            attempt = state.increment_retry(StageEnum.VALIDATION)

            run_log.info(
                f"Fix cycle {attempt}",
                extra={
                    "issues": qa_output.data.get("issues_found", []),
                },
            )
            metrics.record_retry(StageEnum.VALIDATION.value)

            # Send failure feedback to Engineer
            fix_context = {
                **context,
                "validation_feedback": qa_output.data,
            }
            fix_input = AgentInput(
                run_id=state.run_id,
                stage=StageEnum.IMPLEMENTATION.value,
                payload=fix_context,
            )

            fix_output = await engineer_agent.run(fix_input)
            if fix_output.status == "failure":
                run_log.warning(f"Engineer fix attempt failed: {fix_output.error_message}")
                continue

            # Re-validate
            qa_input = AgentInput(
                run_id=state.run_id,
                stage=StageEnum.VALIDATION.value,
                payload={**context, **fix_output.data},
            )
            qa_output = await qa_agent.run(qa_input)

            if qa_output.status == "success":
                # Fix succeeded
                state.record_stage_completion(StageEnum.IMPLEMENTATION, fix_output.data)
                state.record_stage_completion(StageEnum.VALIDATION, qa_output.data)
                await self._persist_state(state)
                run_log.info(f"Fix cycle {attempt} succeeded")
                return qa_output

        # Fix cycles exhausted — escalate
        raise RetryExhaustedError(
            stage=StageEnum.VALIDATION.value,
            attempts=state.retry_counts.get(StageEnum.VALIDATION.value, 0),
            last_error="All fix cycles exhausted",
        )

    async def _execute_parallel_batch(
        self,
        batch: list[ScheduledTask],
        state: ProjectState,
        context: dict[str, Any],
        metrics: RunMetrics,
        run_log: RunLogger,
    ) -> list[Optional[AgentOutput]]:
        """Execute a batch of tasks in parallel via asyncio.gather.

        One task's FATAL_ERROR cancels all siblings in the batch.
        """
        tasks = [
            self._execute_task(task, state, context, metrics, run_log)
            for task in batch
        ]

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            run_log.error(f"Parallel batch failed: {e}")
            raise

        # Check for FATAL among results
        outputs: list[Optional[AgentOutput]] = []
        fatal_error: Optional[Exception] = None

        for result in results:
            if isinstance(result, Exception):
                if isinstance(result, (OrchestratorError, RetryExhaustedError)):
                    fatal_error = result
                outputs.append(None)
            else:
                outputs.append(result)

        if fatal_error:
            run_log.error(
                f"FATAL_ERROR in parallel batch — cancelling siblings",
                extra={"error": str(fatal_error)},
            )
            raise fatal_error

        return outputs

    async def _dispatch_with_retry(
        self,
        agent: Agent,
        input: AgentInput,
        state: ProjectState,
        stage: StageEnum,
    ) -> AgentOutput:
        """Dispatch an agent with retry logic for RETRYABLE_ERROR.

        Retries with exponential backoff up to MAX_RETRIES. Escalates to
        FATAL_ERROR when ceiling is reached.
        """
        last_output: Optional[AgentOutput] = None

        while True:
            output = await agent.run(input)

            if output.status == "success":
                return output

            if output.error_type == "RETRYABLE_ERROR":
                if self._retry_handler.can_retry(state, stage):
                    attempt = state.increment_retry(stage)
                    backoff = self._retry_handler.get_backoff_seconds(attempt)

                    logger.info(
                        f"Retrying {stage.value}",
                        extra={
                            "run_id": state.run_id,
                            "attempt": attempt,
                            "backoff": backoff,
                        },
                    )

                    state.record_error(
                        ErrorRecord(
                            error_type=ErrorType.RETRYABLE_ERROR,
                            stage=stage,
                            message=output.error_message or "Retryable error",
                            recovery_action=f"Retry {attempt} with {backoff}s backoff",
                        )
                    )
                    await self._persist_state(state)
                    await asyncio.sleep(backoff)
                    continue
                else:
                    # Escalate
                    output.error_type = "FATAL_ERROR"
                    output.error_message = (
                        f"Retry ceiling reached for {stage.value}: "
                        f"{output.error_message}"
                    )

            # VALIDATION_FAILURE or FATAL_ERROR — return for caller to handle
            return output

    async def _load_or_init_state(self, project_input: ProjectInput) -> ProjectState:
        """Load existing state or initialize a new one."""
        existing = await self._state_backend.load_state(project_input.run_id)
        if existing:
            logger.info(
                "Resuming from persisted state",
                extra={
                    "run_id": existing.run_id,
                    "current_stage": existing.current_stage,
                    "completed": [s.value for s in existing.completed_stages],
                },
            )
            return existing

        state = ProjectState(
            run_id=project_input.run_id,
            project_id=project_input.project_id,
            context={"project_input": project_input.model_dump()},
        )
        await self._persist_state(state)
        return state

    async def _persist_state(self, state: ProjectState) -> None:
        """Persist state to the backend — called after every transition."""
        await self._state_backend.save_state(state)

    async def _halt_fatal(
        self,
        state: ProjectState,
        output: AgentOutput,
        metrics: RunMetrics,
    ) -> ProjectState:
        """Halt the run on fatal error."""
        state.status = RunStatus.FAILED
        state.record_error(
            ErrorRecord(
                error_type=ErrorType.FATAL_ERROR,
                stage=state.current_stage or StageEnum.REQUIREMENT_ANALYSIS,
                message=output.error_message or "Fatal error",
            )
        )
        await self._persist_state(state)
        metrics.finalize("failure")
        return state

    async def _halt_fatal_with_message(
        self,
        state: ProjectState,
        message: str,
        metrics: RunMetrics,
    ) -> ProjectState:
        """Halt with a custom fatal message."""
        state.status = RunStatus.FAILED
        state.record_error(
            ErrorRecord(
                error_type=ErrorType.FATAL_ERROR,
                stage=state.current_stage or StageEnum.REQUIREMENT_ANALYSIS,
                message=message,
            )
        )
        await self._persist_state(state)
        metrics.finalize("failure")
        logger.error(f"Run FATAL: {message}", extra={"run_id": state.run_id})
        return state
