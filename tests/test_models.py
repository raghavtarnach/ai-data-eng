"""
Unit tests for Pydantic models — serialization, validation, and round-trips.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from src.models.agents.architect import ArchitectOutput, DesignDecision, TechnologyChoice
from src.models.agents.engineer import ConfigEntry, EngineerOutput, PipelineFile
from src.models.agents.pm import PMOutput, TaskNode
from src.models.agents.qa import Issue, QAMetrics, QAOutput, TestCase
from src.models.base import AgentInput, AgentOutput
from src.models.errors import ErrorType
from src.models.project import (
    DataSource,
    ErrorRecord,
    ProjectConstraints,
    ProjectInput,
    ProjectState,
    RunStatus,
    StageEnum,
)


class TestAgentInput:
    def test_create_minimal(self):
        inp = AgentInput(run_id="r1", stage="TEST")
        assert inp.run_id == "r1"
        assert inp.stage == "TEST"
        assert inp.payload == {}

    def test_create_with_payload(self):
        inp = AgentInput(run_id="r1", stage="TEST", payload={"key": "value"})
        assert inp.payload["key"] == "value"

    def test_serialization_roundtrip(self):
        inp = AgentInput(run_id="r1", stage="TEST", payload={"nested": {"a": 1}})
        json_str = inp.model_dump_json()
        restored = AgentInput.model_validate_json(json_str)
        assert restored == inp


class TestAgentOutput:
    def test_success_output(self):
        out = AgentOutput(run_id="r1", stage="TEST", status="success", data={"result": 42})
        assert out.status == "success"
        assert out.error_type is None

    def test_failure_output(self):
        out = AgentOutput(
            run_id="r1",
            stage="TEST",
            status="failure",
            error_type="RETRYABLE_ERROR",
            error_message="Rate limited",
        )
        assert out.status == "failure"
        assert out.error_type == "RETRYABLE_ERROR"

    def test_serialization_roundtrip(self):
        out = AgentOutput(run_id="r1", stage="TEST", status="success", data={"x": [1, 2, 3]})
        restored = AgentOutput.model_validate_json(out.model_dump_json())
        assert restored == out


class TestProjectInput:
    def test_defaults(self):
        pi = ProjectInput(project_name="Test", client_requirements="Build something")
        assert pi.project_id  # UUID generated
        assert pi.run_id  # UUID generated
        assert pi.data_sources == []

    def test_full_input(self):
        pi = ProjectInput(
            project_name="ETL Pipeline",
            client_requirements="Build an ETL",
            data_sources=[
                DataSource(name="src1", type="blob", connection_ref="BLOB_CONN"),
            ],
            target_system="Snowflake",
            constraints=ProjectConstraints(performance="fast", cost="$100", tools="pandas"),
        )
        assert pi.data_sources[0].type == "blob"
        assert pi.constraints.tools == "pandas"

    def test_data_source_types(self):
        for src_type in ["blob", "sql", "api", "stream"]:
            ds = DataSource(name="test", type=src_type, connection_ref="REF")
            assert ds.type == src_type


class TestProjectState:
    def test_initial_state(self):
        state = ProjectState(run_id="r1", project_id="p1")
        assert state.status == RunStatus.IN_PROGRESS
        assert state.completed_stages == []
        assert state.errors == []

    def test_record_stage_completion(self):
        state = ProjectState(run_id="r1", project_id="p1")
        state.pending_stages = [StageEnum.REQUIREMENT_ANALYSIS]
        state.record_stage_completion(
            StageEnum.REQUIREMENT_ANALYSIS, {"task_graph": []}
        )
        assert StageEnum.REQUIREMENT_ANALYSIS in state.completed_stages
        assert StageEnum.REQUIREMENT_ANALYSIS not in state.pending_stages

    def test_record_error(self):
        state = ProjectState(run_id="r1", project_id="p1")
        state.record_error(
            ErrorRecord(
                error_type=ErrorType.RETRYABLE_ERROR,
                stage=StageEnum.IMPLEMENTATION,
                message="Timeout",
            )
        )
        assert len(state.errors) == 1
        assert state.errors[0].error_type == ErrorType.RETRYABLE_ERROR

    def test_increment_retry(self):
        state = ProjectState(run_id="r1", project_id="p1")
        count1 = state.increment_retry(StageEnum.VALIDATION)
        count2 = state.increment_retry(StageEnum.VALIDATION)
        assert count1 == 1
        assert count2 == 2
        assert state.retry_counts[StageEnum.VALIDATION.value] == 2

    def test_serialization_roundtrip(self):
        state = ProjectState(run_id="r1", project_id="p1")
        state.record_stage_completion(StageEnum.REQUIREMENT_ANALYSIS, {"data": "test"})
        state.record_error(
            ErrorRecord(
                error_type=ErrorType.VALIDATION_FAILURE,
                stage=StageEnum.VALIDATION,
                message="Schema mismatch",
            )
        )
        json_str = state.model_dump_json()
        restored = ProjectState.model_validate_json(json_str)
        assert restored.run_id == state.run_id
        assert len(restored.completed_stages) == 1
        assert len(restored.errors) == 1


class TestPMOutput:
    def test_valid_output(self):
        output = PMOutput(
            task_graph=[
                TaskNode(task_id="t1", name="Design", agent="ARCHITECT"),
                TaskNode(task_id="t2", name="Build", agent="ENGINEER", depends_on=["t1"]),
            ],
            execution_plan="Design then build",
            assumptions=["Using Python 3.11"],
        )
        assert len(output.task_graph) == 2
        assert output.task_graph[1].depends_on == ["t1"]

    def test_empty_task_graph_rejected(self):
        with pytest.raises(Exception):
            PMOutput(task_graph=[], execution_plan="Nothing")


class TestArchitectOutput:
    def test_valid_output(self):
        output = ArchitectOutput(
            architecture_design="# Architecture\nData flows from A to B",
            technology_stack=[
                TechnologyChoice(component="ETL", tool="pandas", rationale="Lightweight")
            ],
            design_decisions=[
                DesignDecision(
                    decision="Use Parquet",
                    alternatives_considered=["CSV", "ORC"],
                    rationale="Best compression ratio",
                )
            ],
            cost_estimate="$50/month",
            performance_notes="Handles 10M rows in 3 min",
        )
        assert len(output.technology_stack) == 1


class TestEngineerOutput:
    def test_valid_output(self):
        output = EngineerOutput(
            pipeline_code=[
                PipelineFile(
                    filename="extract.py",
                    language="python",
                    content="import pandas as pd\ndf = pd.read_csv('data.csv')",
                    description="Extract data from CSV",
                    dependencies=["pandas"],
                )
            ],
            configurations=[
                ConfigEntry(key="DB_HOST", value="DB_HOST_ENV", is_secret=True),
            ],
            execution_order=["extract.py"],
        )
        assert output.pipeline_code[0].language == "python"
        assert output.configurations[0].is_secret is True


class TestQAOutput:
    def test_pass_output(self):
        output = QAOutput(
            test_cases=[
                TestCase(test_id="t1", description="Schema check", result="pass"),
            ],
            validation_status="pass",
            metrics=QAMetrics(rows_processed=1000, execution_time_ms=500),
        )
        assert output.validation_status == "pass"

    def test_fail_with_issues(self):
        output = QAOutput(
            test_cases=[
                TestCase(test_id="t1", description="Null check", result="fail", message="5 nulls"),
            ],
            validation_status="fail",
            issues_found=[
                Issue(severity="critical", description="Nulls found", suggested_fix="Filter nulls"),
            ],
        )
        assert output.validation_status == "fail"
        assert len(output.issues_found) == 1


class TestErrorType:
    def test_values(self):
        assert ErrorType.RETRYABLE_ERROR.value == "RETRYABLE_ERROR"
        assert ErrorType.VALIDATION_FAILURE.value == "VALIDATION_FAILURE"
        assert ErrorType.FATAL_ERROR.value == "FATAL_ERROR"

    def test_string_comparison(self):
        assert ErrorType.RETRYABLE_ERROR == "RETRYABLE_ERROR"


class TestStageEnum:
    def test_all_stages(self):
        stages = list(StageEnum)
        assert len(stages) == 6
        assert StageEnum.REQUIREMENT_ANALYSIS in stages
        assert StageEnum.DOCUMENTATION in stages
