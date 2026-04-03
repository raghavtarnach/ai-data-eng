"""
Project Manager Agent.

Type: LLM_BACKED
Stage: REQUIREMENT_ANALYSIS

Parses client requirements, identifies ambiguities, fills them with explicit
assumptions, and produces an execution DAG (task_graph) that the orchestrator
uses to dispatch all downstream agents.

If clarifications_needed is non-empty, the orchestrator halts and surfaces
them to the caller before proceeding.
"""

from __future__ import annotations

import json
from typing import Any

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

from src.config import settings
from src.models.agents.pm import PMOutput
from src.models.base import Agent, AgentInput, AgentOutput
from src.observability.logger import get_logger
from src.observability.tracing import trace_agent_call

logger = get_logger(__name__)

PM_SYSTEM_PROMPT = """You are a senior Project Manager AI agent for a data engineering platform.

Your job is to:
1. Parse the client requirements carefully and thoroughly
2. Identify any ambiguities or missing information
3. Fill ambiguities with explicit, reasonable assumptions — log each one
4. Generate an execution task_graph (a DAG) with explicit dependencies and parallelism hints
5. If there are critical questions that CANNOT be assumed, add them to clarifications_needed

RULES:
- Every task_graph node must have a valid agent type: ARCHITECT, ENGINEER, QA, DEVOPS, or DOCS
- The ARCHITECT task should depend on nothing (it's the first real work after PM)
- The ENGINEER task should depend on ARCHITECT
- QA should depend on ENGINEER
- DEVOPS and DOCS are v2 scope — include them only if explicitly requested
- Set can_run_parallel=true for tasks that genuinely have no data dependency on each other
- Assumptions must be specific and actionable, not vague

You MUST respond with a valid JSON object matching this schema:
{
  "task_graph": [
    {
      "task_id": "string",
      "name": "string (descriptive task name)",
      "agent": "ARCHITECT | ENGINEER | QA | DEVOPS | DOCS",
      "depends_on": ["task_id"],
      "can_run_parallel": bool
    }
  ],
  "execution_plan": "string (prose summary of the plan)",
  "assumptions": ["string (each assumption made)"],
  "clarifications_needed": ["string (critical questions that cannot be assumed)"]
}

Respond with ONLY the JSON object. No markdown, no code fences, no explanation."""


class PMAgent(Agent):
    """Project Manager Agent — requirement analysis and DAG generation.

    Parses client requirements, generates assumptions for ambiguities,
    and produces a task_graph that the orchestrator uses for execution scheduling.
    """

    agent_type = "LLM_BACKED"

    def __init__(self):
        self._llm = ChatAnthropic(
            model=settings.llm.model_name,
            api_key=settings.llm.api_key,
            temperature=settings.llm.temperature,
            max_tokens=settings.llm.max_tokens,
        )

    async def run(self, input: AgentInput) -> AgentOutput:
        """Parse requirements and generate execution DAG.

        Args:
            input: Contains project_input in payload with client_requirements,
                   data_sources, constraints, etc.

        Returns:
            AgentOutput with PMOutput in the data field.
        """
        with trace_agent_call(input.run_id, "pm_agent", input.stage):
            try:
                # Build the prompt with full project context
                project_context = json.dumps(input.payload, indent=2, default=str)
                user_prompt = f"""Analyze the following project requirements and produce an execution plan.

PROJECT INPUT:
{project_context}

Generate the task_graph, execution_plan, assumptions, and clarifications_needed."""

                messages = [
                    SystemMessage(content=PM_SYSTEM_PROMPT),
                    HumanMessage(content=user_prompt),
                ]

                response = await self._llm.ainvoke(messages)
                response_text = response.content

                # Parse the LLM response
                if isinstance(response_text, list):
                    response_text = response_text[0].get("text", "") if response_text else ""

                # Clean potential markdown fences
                cleaned = response_text.strip()
                if cleaned.startswith("```"):
                    cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned
                    if cleaned.endswith("```"):
                        cleaned = cleaned[:-3]
                    cleaned = cleaned.strip()

                parsed_data = json.loads(cleaned)
                pm_output = PMOutput.model_validate(parsed_data)

                logger.info(
                    "PM Agent completed",
                    extra={
                        "run_id": input.run_id,
                        "task_count": len(pm_output.task_graph),
                        "assumption_count": len(pm_output.assumptions),
                        "clarifications": len(pm_output.clarifications_needed),
                    },
                )

                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="success",
                    data=pm_output.model_dump(),
                )

            except json.JSONDecodeError as e:
                logger.error(
                    "PM Agent failed to parse LLM response",
                    extra={"run_id": input.run_id, "error": str(e)},
                )
                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="failure",
                    error_type="RETRYABLE_ERROR",
                    error_message=f"Failed to parse LLM response as JSON: {e}",
                )

            except Exception as e:
                logger.error(
                    "PM Agent error",
                    extra={"run_id": input.run_id, "error": str(e)},
                )
                error_type = "RETRYABLE_ERROR" if "rate" in str(e).lower() else "FATAL_ERROR"
                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="failure",
                    error_type=error_type,
                    error_message=str(e),
                )
