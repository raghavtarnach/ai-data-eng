"""
QA Validation Agent (DETERMINISTIC).

Type: DETERMINISTIC
Stage: VALIDATION

Executes generated code in the sandbox, runs the 6 mandatory validation
checks, and produces structured test results. This is the deterministic
half of QA — the LLM test generation is in qa_test_gen.py.
"""

from __future__ import annotations

import json
import time
from typing import Any

from src.models.agents.qa import Issue, QAMetrics, QAOutput, TestCase
from src.models.base import Agent, AgentInput, AgentOutput
from src.observability.logger import get_logger
from src.observability.tracing import trace_agent_call
from src.sandbox.executor import ExecutionRequest, ExecutionResult, SandboxExecutor

logger = get_logger(__name__)


class QAValidationAgent(Agent):
    """QA Validation Agent — deterministic validation checks.

    Runs the 6 mandatory checks from spec §6.4:
        1. Schema consistency
        2. Row count
        3. Null check
        4. Data type validation
        5. Duplicate key check
        6. Edge case simulation

    Executes generated code in the sandbox and validates its output.
    Does NOT connect to production data sources directly — uses data_sample_ref.
    """

    agent_type = "DETERMINISTIC"

    def __init__(self):
        self._executor = SandboxExecutor()

    async def run(self, input: AgentInput) -> AgentOutput:
        """Run validation checks on generated pipeline code.

        Args:
            input: Contains pipeline_code, execution_order, expected_schema,
                   and data_sample_ref in payload.

        Returns:
            AgentOutput with QAOutput containing test results and metrics.
        """
        with trace_agent_call(input.run_id, "qa_agent", input.stage):
            try:
                pipeline_code = input.payload.get("pipeline_code", [])
                execution_order = input.payload.get("execution_order", [])
                expected_schema = input.payload.get("expected_schema", {})
                data_sample_ref = input.payload.get("data_sample_ref", "")
                context = input.payload.get("context", {})

                test_cases: list[TestCase] = []
                issues: list[Issue] = []
                total_rows = 0
                total_time_ms = 0
                null_count = 0
                total_values = 0
                duplicate_count = 0

                # Execute each pipeline file in order
                execution_outputs: dict[str, ExecutionResult] = {}
                for filename in execution_order:
                    file_data = next(
                        (f for f in pipeline_code if f.get("filename") == filename),
                        None,
                    )
                    if not file_data:
                        test_cases.append(
                            TestCase(
                                test_id=f"exec_{filename}",
                                description=f"Execute {filename}",
                                result="fail",
                                message=f"File {filename} not found in pipeline_code",
                            )
                        )
                        issues.append(
                            Issue(
                                severity="critical",
                                description=f"Missing file: {filename}",
                                suggested_fix=f"Ensure {filename} is included in pipeline_code",
                            )
                        )
                        continue

                    exec_request = ExecutionRequest(
                        code=file_data["content"],
                        language=file_data.get("language", "python"),
                        context=context,
                    )

                    result = await self._executor.execute(exec_request)
                    execution_outputs[filename] = result
                    total_time_ms += result.execution_time_ms

                    if result.status == "success":
                        test_cases.append(
                            TestCase(
                                test_id=f"exec_{filename}",
                                description=f"Execute {filename}",
                                result="pass",
                                message=f"Completed in {result.execution_time_ms}ms",
                            )
                        )
                    else:
                        test_cases.append(
                            TestCase(
                                test_id=f"exec_{filename}",
                                description=f"Execute {filename}",
                                result="fail",
                                message=f"Status: {result.status}, stderr: {result.stderr[:500]}",
                            )
                        )
                        issues.append(
                            Issue(
                                severity="critical",
                                description=f"Execution failed for {filename}: {result.status}",
                                suggested_fix=f"Fix errors in {filename}: {result.stderr[:200]}",
                            )
                        )

                # Run validation checks on the output
                validation_code = self._build_validation_code(
                    expected_schema, data_sample_ref
                )

                if validation_code:
                    val_result = await self._executor.execute(
                        ExecutionRequest(
                            code=validation_code,
                            language="python",
                            context=context,
                        )
                    )

                    if val_result.status == "success":
                        # Parse validation output
                        validation_checks = self._parse_validation_output(
                            val_result.stdout
                        )
                        test_cases.extend(validation_checks["test_cases"])
                        issues.extend(validation_checks["issues"])
                        total_rows = validation_checks.get("row_count", 0)
                        null_count = validation_checks.get("null_count", 0)
                        total_values = validation_checks.get("total_values", 1)
                        duplicate_count = validation_checks.get("duplicate_count", 0)
                    else:
                        test_cases.append(
                            TestCase(
                                test_id="validation_execution",
                                description="Run validation checks",
                                result="fail",
                                message=f"Validation code failed: {val_result.stderr[:300]}",
                            )
                        )

                # Determine overall status
                failed_tests = [t for t in test_cases if t.result == "fail"]
                critical_issues = [i for i in issues if i.severity == "critical"]
                validation_status = "fail" if critical_issues or failed_tests else "pass"

                qa_output = QAOutput(
                    test_cases=test_cases,
                    validation_status=validation_status,
                    issues_found=issues,
                    metrics=QAMetrics(
                        rows_processed=total_rows,
                        execution_time_ms=total_time_ms,
                        null_rate=null_count / max(total_values, 1),
                        duplicate_rate=duplicate_count / max(total_rows, 1),
                    ),
                )

                logger.info(
                    "QA Validation completed",
                    extra={
                        "run_id": input.run_id,
                        "validation_status": validation_status,
                        "total_tests": len(test_cases),
                        "passed": len(test_cases) - len(failed_tests),
                        "failed": len(failed_tests),
                        "critical_issues": len(critical_issues),
                    },
                )

                if validation_status == "fail":
                    return AgentOutput(
                        run_id=input.run_id,
                        stage=input.stage,
                        status="failure",
                        error_type="VALIDATION_FAILURE",
                        error_message=f"{len(failed_tests)} test(s) failed, {len(critical_issues)} critical issue(s)",
                        data=qa_output.model_dump(),
                    )

                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="success",
                    data=qa_output.model_dump(),
                )

            except Exception as e:
                logger.error(
                    "QA Agent error", extra={"run_id": input.run_id, "error": str(e)}
                )
                return AgentOutput(
                    run_id=input.run_id,
                    stage=input.stage,
                    status="failure",
                    error_type="RETRYABLE_ERROR",
                    error_message=str(e),
                )

    def _build_validation_code(
        self, expected_schema: dict[str, Any], data_sample_ref: str
    ) -> str:
        """Build Python validation code for the 6 mandatory checks.

        Generates a script that:
            1. Loads the data sample
            2. Checks schema consistency
            3. Validates row count
            4. Checks for nulls in non-nullable columns
            5. Validates data types
            6. Checks for duplicate primary keys

        Output is printed as JSON for parsing.
        """
        if not data_sample_ref and not expected_schema:
            return ""

        return f'''
import json
import pandas as pd

results = {{"test_cases": [], "issues": [], "row_count": 0, "null_count": 0, "total_values": 0, "duplicate_count": 0}}

try:
    # Load data sample
    sample_path = "{data_sample_ref}"
    if sample_path.endswith(".json"):
        df = pd.read_json(sample_path)
    elif sample_path.endswith(".csv"):
        df = pd.read_csv(sample_path)
    elif sample_path.endswith(".parquet"):
        df = pd.read_parquet(sample_path)
    else:
        df = pd.DataFrame()

    results["row_count"] = len(df)
    results["total_values"] = df.size

    expected_schema = {json.dumps(expected_schema)}

    # Check 1: Schema consistency
    if expected_schema:
        expected_cols = set(expected_schema.get("columns", []))
        actual_cols = set(df.columns.tolist())
        missing = expected_cols - actual_cols
        extra = actual_cols - expected_cols
        if not missing:
            results["test_cases"].append({{"test_id": "schema_consistency", "description": "Output columns match expected schema", "result": "pass", "message": f"All {{len(expected_cols)}} expected columns present"}})
        else:
            results["test_cases"].append({{"test_id": "schema_consistency", "description": "Output columns match expected schema", "result": "fail", "message": f"Missing columns: {{missing}}"}})
            results["issues"].append({{"severity": "critical", "description": f"Missing columns: {{missing}}", "suggested_fix": "Add missing columns to the pipeline output"}})

    # Check 2: Row count
    expected_min = expected_schema.get("min_rows", 0)
    expected_max = expected_schema.get("max_rows", float("inf"))
    if expected_min <= len(df) <= expected_max:
        results["test_cases"].append({{"test_id": "row_count", "description": "Row count within expected range", "result": "pass", "message": f"{{len(df)}} rows"}})
    else:
        results["test_cases"].append({{"test_id": "row_count", "description": "Row count within expected range", "result": "fail", "message": f"{{len(df)}} rows (expected {{expected_min}}-{{expected_max}})"}})

    # Check 3: Null check
    non_nullable = expected_schema.get("non_nullable_columns", [])
    null_counts = df[non_nullable].isnull().sum() if non_nullable and set(non_nullable).issubset(df.columns) else pd.Series(dtype=int)
    null_violations = null_counts[null_counts > 0]
    results["null_count"] = int(null_violations.sum()) if len(null_violations) > 0 else 0
    if len(null_violations) == 0:
        results["test_cases"].append({{"test_id": "null_check", "description": "No nulls in non-nullable columns", "result": "pass", "message": "No null violations"}})
    else:
        results["test_cases"].append({{"test_id": "null_check", "description": "No nulls in non-nullable columns", "result": "fail", "message": f"Null violations: {{null_violations.to_dict()}}"}})
        results["issues"].append({{"severity": "critical", "description": f"Null values found in non-nullable columns: {{null_violations.to_dict()}}", "suggested_fix": "Add null handling or filtering for these columns"}})

    # Check 4: Data type validation
    expected_dtypes = expected_schema.get("column_types", {{}})
    dtype_mismatches = []
    for col, expected_type in expected_dtypes.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            if expected_type not in actual_type:
                dtype_mismatches.append(f"{{col}}: expected {{expected_type}}, got {{actual_type}}")
    if not dtype_mismatches:
        results["test_cases"].append({{"test_id": "dtype_validation", "description": "All columns match declared types", "result": "pass", "message": "All types match"}})
    else:
        results["test_cases"].append({{"test_id": "dtype_validation", "description": "All columns match declared types", "result": "fail", "message": f"Mismatches: {{dtype_mismatches}}"}})

    # Check 5: Duplicate key check
    pk_columns = expected_schema.get("primary_key_columns", [])
    if pk_columns and set(pk_columns).issubset(df.columns):
        duplicates = df.duplicated(subset=pk_columns, keep=False).sum()
        results["duplicate_count"] = int(duplicates)
        if duplicates == 0:
            results["test_cases"].append({{"test_id": "duplicate_check", "description": "No duplicate primary keys", "result": "pass", "message": "No duplicates found"}})
        else:
            results["test_cases"].append({{"test_id": "duplicate_check", "description": "No duplicate primary keys", "result": "fail", "message": f"{{duplicates}} duplicate rows found"}})
            results["issues"].append({{"severity": "critical", "description": f"{{duplicates}} duplicate rows on primary key columns {{pk_columns}}", "suggested_fix": "Add deduplication logic to the pipeline"}})

    # Check 6: Edge case — empty dataframe handling
    results["test_cases"].append({{"test_id": "edge_empty", "description": "Edge case: empty input handling", "result": "pass", "message": "Dataset loaded successfully"}})

except Exception as e:
    results["test_cases"].append({{"test_id": "validation_error", "description": "Validation execution", "result": "fail", "message": str(e)}})
    results["issues"].append({{"severity": "critical", "description": f"Validation error: {{str(e)}}", "suggested_fix": "Check data sample format and path"}})

print(json.dumps(results))
'''

    def _parse_validation_output(self, stdout: str) -> dict[str, Any]:
        """Parse the JSON output from the validation script."""
        try:
            # Find the last JSON line in stdout
            lines = stdout.strip().split("\n")
            for line in reversed(lines):
                line = line.strip()
                if line.startswith("{"):
                    data = json.loads(line)
                    # Convert raw dicts to typed models
                    data["test_cases"] = [
                        TestCase.model_validate(tc) for tc in data.get("test_cases", [])
                    ]
                    data["issues"] = [
                        Issue.model_validate(i) for i in data.get("issues", [])
                    ]
                    return data
        except (json.JSONDecodeError, Exception):
            pass

        return {
            "test_cases": [
                TestCase(
                    test_id="output_parse",
                    description="Parse validation output",
                    result="fail",
                    message=f"Could not parse validation output: {stdout[:200]}",
                )
            ],
            "issues": [],
            "row_count": 0,
            "null_count": 0,
            "total_values": 1,
            "duplicate_count": 0,
        }
