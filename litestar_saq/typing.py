# ruff: noqa: A005
# pyright: reportUnknownVariableType=false
"""Public type exports for litestar-saq OpenTelemetry support.

This module provides the public API for OpenTelemetry types and utilities.
All application code should import from this module, NEVER from _typing.py.

Example usage::

    from litestar_saq.typing import OPENTELEMETRY_INSTALLED, Span, Tracer, trace

    if OPENTELEMETRY_INSTALLED:
        tracer = trace.get_tracer("my_app")
        with tracer.start_as_current_span("operation") as span:
            span.set_attribute("key", "value")
"""

from litestar_saq._typing import (
    OPENTELEMETRY_INSTALLED,
    STRUCTLOG_INSTALLED,
    Context,
    Span,
    SpanKind,
    Status,
    StatusCode,
    Tracer,
    propagate,
    trace,
)

__all__ = (
    "OPENTELEMETRY_INSTALLED",
    "STRUCTLOG_INSTALLED",
    "Context",
    "Span",
    "SpanKind",
    "Status",
    "StatusCode",
    "Tracer",
    "propagate",
    "trace",
)
