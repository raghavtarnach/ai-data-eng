"""
Pipeline Engineer Agent output models.

The engineer generates modular, runnable pipeline code from the architecture
design. Each file includes docstrings, and secrets are referenced by env var
name only — never hardcoded.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class PipelineFile(BaseModel):
    """A single generated code file.

    Attributes:
        filename: Name of the file (e.g., 'extract.py', 'transform.sql').
        language: Programming language.
        content: Full file content. Must NOT contain raw secrets.
        description: Purpose of this file, its inputs and outputs.
        dependencies: Required pip packages or system dependencies.
    """

    filename: str
    language: str  # "python" | "sql"
    content: str
    description: str = ""
    dependencies: list[str] = Field(default_factory=list)


class ConfigEntry(BaseModel):
    """A configuration key-value pair.

    Attributes:
        key: Configuration key name.
        value: Configuration value. If is_secret is True, this is an env var name.
        is_secret: Whether this value is a secret. Secret values are masked in
            logs and state storage.
    """

    key: str
    value: str
    is_secret: bool = False


class EngineerOutput(BaseModel):
    """Structured output from the Pipeline Engineer Agent.

    Attributes:
        pipeline_code: List of generated code files.
        configurations: Runtime configuration entries.
        execution_order: Ordered list of filenames defining run sequence.
    """

    pipeline_code: list[PipelineFile] = Field(default_factory=list)
    configurations: list[ConfigEntry] = Field(default_factory=list)
    execution_order: list[str] = Field(default_factory=list)
