"""
QA Test Generation Agent (LLM_BACKED).

Type: LLM_BACKED
Stage: VALIDATION (supplementary)

Uses LLM to generate additional edge-case test scenarios beyond the 6
mandatory checks. Generated tests are executed through the sandbox by
the QAValidationAgent.
"""

from __future__ import annotations

import json
from typing import Any

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

from src.config import settings
from src.models.base import Agent, AgentInput, AgentOutput
from src.observability.logger import get_logger
from src.observability.tracing import trace_agent_call

logger = get_logger(__name__)

TEST_GEN_SYSTEM_PROMPT = """You are a QA Test Generation AI agent for a data engineering platform.

Your job is to generate additional edge-case test scenarios for data pipeline code.
You will receive the pipeline code and its expected behavior, and you must generate
Python test scripts that validate edge cases.

Focus on:
1. Empty input handling
2. Single-row input
3. Maximum volume input (simulate with sample)
4. Malformed data (wrong types, extra columns, missing columns)
5. Boundary values (nulls, empty strings, max int, negative numbers)
6. Unicode and special characters in data
7. Timezone and date format edge cases

Each test should be a standalone Python script that:
- Prints a JSON result: {"test_id": "...", "description": "...", "result": "pass|fail", "message": "..."}
- Uses only: pandas, numpy, pyarrow, sqlalchemy, json, sys, os (env vars only)
- Does NOT make network calls or write to host filesystem

Respond with a JSON object:
{
  "test_scripts": [
    {
      "test_id": "string",
      "description": "string",
      "code": "string (complete Python script)"
    }
  ]
}

Respond with ONLY the JSON object."""


class QATestGenAgent(Agent):
    """QA Test Generation Agent — LLM-powered edge-case test creation.

    Generates supplementary test scripts that the QAValidationAgent executes
    via the sandbox. Focuses on edge cases the deterministic checks don't cover.
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
        """Generate edge-case test scripts.

        Args:
            input: Contains pipeline_code and expected_schema in payload.

        Returns:
            AgentOutput with test_scripts list in data field.
        """
        with trace_agent_call(input.run_id, "qa_test_gen", input.stage):
            try:
                context = json.dumps(input.payload, indent=2, default=str)
                user_prompt = f"""Generate edge-case test scripts for the following pipeline:

{context}

Focus on the edge cases listed in your instructions. Generate 3-5 focused test scripts."""

                messages = [
                    SystemMessage(content=TEST_GEN_SYSTEM_PROMPT),
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

                logger.info(
                    "QA Test Gen completed",
                    extra={
                        "run_id": input.run_id,
                        "tests_generated": len(parsed_data.get("test_scripts", [])),
                    },
                )

                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="success",
                    data=parsed_data,
                )

            except json.JSONDecodeError as e:
                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="failure",
                    error_type="RETRYABLE_ERROR",
                    error_message=f"Failed to parse test gen response: {e}",
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
