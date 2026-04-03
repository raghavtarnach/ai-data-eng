"""
Pipeline Engineer Agent.

Type: LLM_BACKED
Stage: IMPLEMENTATION

Generates modular, runnable pipeline code from the architecture design.
Each file includes docstrings. Secrets are referenced by env var name only.
All generated code is security-scanned before being returned.
"""

from __future__ import annotations

import json
from typing import Any

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

from src.config import settings
from src.memory.store import memory_store
from src.models.agents.engineer import EngineerOutput
from src.models.base import Agent, AgentInput, AgentOutput
from src.observability.logger import get_logger
from src.observability.tracing import trace_agent_call
from src.sandbox.security import SecurityScanner

logger = get_logger(__name__)

ENGINEER_SYSTEM_PROMPT = """You are a senior Pipeline Engineer AI agent for a data engineering platform.

Your job is to:
1. Generate modular, runnable pipeline code from the architecture design
2. Each file MUST include a docstring stating its purpose, inputs, and outputs
3. Code must be production-quality: error handling, logging, clear variable names
4. NEVER hardcode secrets — reference them via environment variable names only
5. Include a clear execution_order for the generated files

RULES:
- Generate Python and/or SQL files as appropriate for the architecture
- Each file should be self-contained and focused on a single responsibility
- Include proper error handling with try/except blocks
- Use environment variables for all connection strings and credentials
- Dependencies must be explicitly listed per file
- If prior pipeline templates are provided, use them as reference but adapt to current requirements
- Code must be compatible with the sandbox environment (pandas, numpy, pyarrow, sqlalchemy available)
- VERY IMPORTANT: Be extremely concise. Generate ONLY STUB FILES (use `pass` in functions) and minimal scaffolding. DO NOT write full implementations. Our immediate goal is to test the orchestration flow and avoid JSON string truncation limits.
- Do NOT generate a `requirements.txt` file or any other configuration files. Only generate Python and SQL files.

You MUST respond with a valid JSON object matching this schema:
{
  "pipeline_code": [
    {
      "filename": "string",
      "language": "python | sql",
      "content": "string (full file content)",
      "description": "string (purpose, inputs, outputs)",
      "dependencies": ["string (pip package name)"]
    }
  ],
  "configurations": [
    {"key": "string", "value": "string (env var name if secret)", "is_secret": bool}
  ],
  "execution_order": ["filename1.py", "filename2.py"]
}

Respond with ONLY the JSON object. No markdown fences."""


class EngineerAgent(Agent):
    """Pipeline Engineer Agent — code generation with security scanning.

    Generates modular pipeline code from architecture design. All generated
    code is security-scanned before being returned — code with disallowed
    patterns (subprocess, os.system, etc.) causes a FATAL_ERROR.
    """

    agent_type = "LLM_BACKED"

    def __init__(self):
        self._llm = ChatAnthropic(
            model=settings.llm.model_name,
            api_key=settings.llm.api_key,
            temperature=settings.llm.temperature,
            max_tokens=settings.llm.max_tokens,
        )
        self._scanner = SecurityScanner()

    async def run(self, input: AgentInput) -> AgentOutput:
        """Generate pipeline code from architecture design.

        Args:
            input: Contains architecture_design and project context in payload.

        Returns:
            AgentOutput with EngineerOutput in data field. Code is
            security-scanned before return.
        """
        with trace_agent_call(input.run_id, "engineer_agent", input.stage):
            try:
                # Memory retrieval: query prior pipeline templates
                tags = input.payload.get("tags", [])
                prior_templates = await memory_store.query_patterns(
                    category="pipeline_template", tags=tags
                )

                # Build context
                context_parts = [
                    f"ARCHITECTURE & PROJECT CONTEXT:\n{json.dumps(input.payload, indent=2, default=str)}"
                ]

                if prior_templates:
                    context_parts.append(
                        f"\nPRIOR PIPELINE TEMPLATES (adapt, don't copy blindly):\n"
                        f"{json.dumps(prior_templates, indent=2)}"
                    )

                # Check if this is a fix cycle (QA failure feedback)
                if "validation_feedback" in input.payload:
                    context_parts.append(
                        f"\nVALIDATION FAILURE FEEDBACK (fix these issues):\n"
                        f"{json.dumps(input.payload['validation_feedback'], indent=2)}"
                    )

                user_prompt = "\n\n".join(context_parts)
                user_prompt += "\n\nGenerate the complete pipeline code."

                messages = [
                    SystemMessage(content=ENGINEER_SYSTEM_PROMPT),
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
                engineer_output = EngineerOutput.model_validate(parsed_data)

                # Security scan all generated code
                for file in engineer_output.pipeline_code:
                    if file.language == "python":
                        scan_result = self._scanner.scan(file.content, file.filename)
                    elif file.language == "sql":
                        scan_result = self._scanner.scan_sql(file.content, file.filename)
                    else:
                        continue

                    if not scan_result.is_safe:
                        logger.error(
                            "Security scan failed for generated code",
                            extra={
                                "run_id": input.run_id,
                                "file_name": file.filename,
                                "violations": scan_result.violations,
                            },
                        )
                        return AgentOutput(
                            run_id=input.run_id,
                            stage=input.stage,
                            status="failure",
                            error_type="FATAL_ERROR",
                            error_message=(
                                f"Security scan failed for {file.filename}: "
                                f"{'; '.join(scan_result.violations)}"
                            ),
                        )

                logger.info(
                    "Engineer Agent completed",
                    extra={
                        "run_id": input.run_id,
                        "file_count": len(engineer_output.pipeline_code),
                        "languages": list({f.language for f in engineer_output.pipeline_code}),
                    },
                )

                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="success",
                    data=engineer_output.model_dump(),
                )

            except json.JSONDecodeError as e:
                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="failure",
                    error_type="RETRYABLE_ERROR",
                    error_message=f"Failed to parse engineer response as JSON: {e}",
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
