"""
Central configuration for the AI Data Engineering System.

All configuration is loaded from environment variables with sensible defaults
for local development. Secrets are never hardcoded.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class LLMConfig:
    """LLM provider configuration — Anthropic Claude."""

    provider: str = "anthropic"
    model_name: str = os.getenv("LLM_MODEL", "claude-sonnet-4-20250514")
    api_key: str = os.getenv("ANTHROPIC_API_KEY", "")
    # Generation params
    temperature: float = float(os.getenv("LLM_TEMPERATURE", "0.1"))
    max_tokens: int = int(os.getenv("LLM_MAX_TOKENS", "4096"))


@dataclass(frozen=True)
class DatabaseConfig:
    """PostgreSQL state persistence configuration."""

    url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@localhost:5432/ai_data_eng",
    )
    pool_size: int = int(os.getenv("DB_POOL_SIZE", "5"))
    max_overflow: int = int(os.getenv("DB_MAX_OVERFLOW", "10"))


@dataclass(frozen=True)
class SandboxConfig:
    """Docker sandbox configuration."""

    image: str = os.getenv("SANDBOX_IMAGE", "ai-data-eng-sandbox:latest")
    cpu_limit: float = float(os.getenv("SANDBOX_CPU_LIMIT", "2.0"))
    memory_limit: str = os.getenv("SANDBOX_MEMORY_LIMIT", "4g")
    python_timeout: int = int(os.getenv("SANDBOX_PYTHON_TIMEOUT", "300"))
    sql_timeout: int = int(os.getenv("SANDBOX_SQL_TIMEOUT", "60"))
    output_size_cap_mb: int = int(os.getenv("SANDBOX_OUTPUT_CAP_MB", "500"))
    network_disabled: bool = True  # Non-negotiable per spec


@dataclass(frozen=True)
class StorageConfig:
    """Artifact storage configuration."""

    root: Path = field(
        default_factory=lambda: Path(
            os.getenv("STORAGE_ROOT", "./artifacts")
        )
    )


@dataclass(frozen=True)
class ObservabilityConfig:
    """Logging, tracing, and metrics configuration."""

    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    otel_endpoint: str = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    service_name: str = "ai-data-eng"


@dataclass(frozen=True)
class Settings:
    """Root configuration container."""

    llm: LLMConfig = field(default_factory=LLMConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    sandbox: SandboxConfig = field(default_factory=SandboxConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)

    # Orchestrator
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    max_fix_cycles: int = int(os.getenv("MAX_FIX_CYCLES", "3"))


# Singleton instance
settings = Settings()
