"""
Solution Architect Agent.

Type: LLM_BACKED
Stage: ARCHITECTURE_DESIGN

Designs end-to-end system architecture from PM output, selects tools from
the allowed list, documents design decisions with rationale, and flags
cost/performance trade-offs explicitly.
"""

from __future__ import annotations

import json
from typing import Any

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

from src.config import settings
from src.memory.store import memory_store
from src.models.agents.architect import ArchitectOutput
from src.models.base import Agent, AgentInput, AgentOutput
from src.observability.logger import get_logger
from src.observability.tracing import trace_agent_call

logger = get_logger(__name__)

ARCHITECT_SYSTEM_PROMPT = """You are a senior Solution Architect AI agent for a data engineering platform.

Your job is to:
1. Design an end-to-end system architecture based on the requirements and execution plan
2. Select specific tools and services — ONLY from the allowed tools list in project constraints
3. Document every design decision with rationale AND alternatives considered
4. Flag cost and performance trade-offs explicitly
5. If estimated cost exceeds the budget constraint, propose a cheaper alternative

RULES:
- Default to Parquet or Delta for storage format. Justify any deviation.
- Partition by date or high-cardinality column when data > 1 GB.
- Start with the smallest viable compute. Document scale-up conditions.
- If prior architecture patterns are provided in context, consider them. If a pattern conflicts with current requirements, explicitly log the conflict and justify the override.
- Be specific: name exact services, versions, and configurations.

You MUST respond with a valid JSON object matching this schema:
{
  "architecture_design": "string (detailed markdown architecture document)",
  "technology_stack": [
    {"component": "string", "tool": "string", "rationale": "string"}
  ],
  "design_decisions": [
    {"decision": "string", "alternatives_considered": ["string"], "rationale": "string"}
  ],
  "cost_estimate": "string (monthly cost breakdown)",
  "performance_notes": "string (expected performance characteristics)"
}

Respond with ONLY the JSON object. No markdown fences, no explanation."""


class ArchitectAgent(Agent):
    """Solution Architect Agent — system design and tech stack selection.

    Queries memory for prior architecture patterns before designing.
    Documents all decisions with rationale and alternatives.
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
        """Design system architecture from PM output.

        Args:
            input: Contains PM output (task_graph, assumptions) and project
                   constraints in payload.

        Returns:
            AgentOutput with ArchitectOutput in the data field.
        """
        with trace_agent_call(input.run_id, "architect_agent", input.stage):
            try:
                # Memory retrieval: query prior architecture patterns
                tools_list = input.payload.get("constraints", {}).get("tools", "")
                tags = [t.strip() for t in tools_list.split(",") if t.strip()]
                prior_patterns = await memory_store.query_patterns(
                    category="architecture_pattern", tags=tags
                )

                # Build context
                context_parts = [
                    f"PROJECT CONTEXT:\n{json.dumps(input.payload, indent=2, default=str)}"
                ]

                if prior_patterns:
                    context_parts.append(
                        f"\nPRIOR ARCHITECTURE PATTERNS (consider but don't blindly copy):\n"
                        f"{json.dumps(prior_patterns, indent=2)}"
                    )

                user_prompt = "\n\n".join(context_parts)
                user_prompt += "\n\nDesign the complete system architecture."

                messages = [
                    SystemMessage(content=ARCHITECT_SYSTEM_PROMPT),
                    HumanMessage(content=user_prompt),
                ]

                response = await self._llm.ainvoke(messages)
                response_text = response.content

                if isinstance(response_text, list):
                    response_text = response_text[0].get("text", "") if response_text else ""

                # Clean markdown fences
                cleaned = response_text.strip()
                if cleaned.startswith("```"):
                    cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned
                    if cleaned.endswith("```"):
                        cleaned = cleaned[:-3]
                    cleaned = cleaned.strip()

                parsed_data = json.loads(cleaned)
                architect_output = ArchitectOutput.model_validate(parsed_data)

                logger.info(
                    "Architect Agent completed",
                    extra={
                        "run_id": input.run_id,
                        "tech_stack_count": len(architect_output.technology_stack),
                        "decisions_count": len(architect_output.design_decisions),
                    },
                )

                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="success",
                    data=architect_output.model_dump(),
                )

            except json.JSONDecodeError as e:
                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="failure",
                    error_type="RETRYABLE_ERROR",
                    error_message=f"Failed to parse architecture response as JSON: {e}",
                )

            except Exception as e:
                error_type = "RETRYABLE_ERROR" if "rate" in str(e).lower() else "FATAL_ERROR"
                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="failure",
                    error_type=error_type,
                    error_message=str(e),
                )
