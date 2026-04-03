"""
OpenTelemetry tracing integration.

Each run_id is the distributed trace root. Every agent call and sandbox
execution emits a child span with parent_run_id + span_id. Compatible
with OpenTelemetry exporters (console for local dev, OTLP for prod).
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator, Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)

from src.config import settings


def _init_tracer_provider() -> TracerProvider:
    """Initialize the OpenTelemetry tracer provider."""
    resource = Resource.create(
        {
            "service.name": settings.observability.service_name,
            "service.version": "1.0.0",
        }
    )

    provider = TracerProvider(resource=resource)

    if settings.observability.otel_endpoint:
        # Production: export via OTLP
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )

            exporter = OTLPSpanExporter(endpoint=settings.observability.otel_endpoint)
            provider.add_span_processor(BatchSpanProcessor(exporter))
        except ImportError:
            # Fallback to console if OTLP exporter not available
            provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    else:
        # Local dev: console exporter
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)
    return provider


# Initialize on import
_provider = _init_tracer_provider()


def get_tracer(name: str) -> trace.Tracer:
    """Get a named tracer instance.

    Args:
        name: Tracer name (typically module __name__).

    Returns:
        OpenTelemetry tracer.
    """
    return trace.get_tracer(name)


@contextmanager
def trace_agent_call(
    run_id: str,
    agent_name: str,
    stage: str,
    attributes: Optional[dict[str, Any]] = None,
) -> Generator[trace.Span, None, None]:
    """Context manager for tracing an agent invocation.

    Creates a span with standard attributes for the agent call.

    Args:
        run_id: Current run identifier (trace root).
        agent_name: Name of the agent being invoked.
        stage: Pipeline stage.
        attributes: Additional span attributes.

    Yields:
        The active span for adding events or setting status.
    """
    tracer = get_tracer("ai-data-eng.agents")
    with tracer.start_as_current_span(
        name=f"agent.{agent_name}",
        attributes={
            "run_id": run_id,
            "agent.name": agent_name,
            "agent.stage": stage,
            **(attributes or {}),
        },
    ) as span:
        yield span


@contextmanager
def trace_sandbox_execution(
    run_id: str,
    language: str,
    attributes: Optional[dict[str, Any]] = None,
) -> Generator[trace.Span, None, None]:
    """Context manager for tracing a sandbox code execution.

    Args:
        run_id: Current run identifier.
        language: Execution language (python/sql).
        attributes: Additional span attributes.

    Yields:
        The active span.
    """
    tracer = get_tracer("ai-data-eng.sandbox")
    with tracer.start_as_current_span(
        name=f"sandbox.execute.{language}",
        attributes={
            "run_id": run_id,
            "sandbox.language": language,
            **(attributes or {}),
        },
    ) as span:
        yield span
