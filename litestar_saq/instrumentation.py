# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportInvalidTypeForm=false, reportUnnecessaryComparison=false, reportAssignmentType=false
# mypy: disable-error-code="unreachable,arg-type,assignment"
"""OpenTelemetry instrumentation utilities for litestar-saq.

This module provides instrumentation helpers for SAQ workers and queues,
following OpenTelemetry semantic conventions for messaging systems.

All OTEL type imports come from litestar_saq.typing (the public API).
"""

from typing import TYPE_CHECKING, Any, Optional

from litestar_saq.typing import (
    OPENTELEMETRY_INSTALLED,
    Span,
    SpanKind,
    Status,
    StatusCode,
    Tracer,
    propagate,
    trace,
)

if TYPE_CHECKING:
    from saq.job import Job
    from saq.queue.base import Queue
    from saq.types import Context

__all__ = [
    "InstrumentedQueue",
    "create_process_span",
    "end_process_span",
    "extract_trace_context",
    "get_tracer",
    "inject_trace_context",
]

_tracer: Optional[Tracer] = None


def get_tracer(
    name: str = "litestar_saq",
    version: Optional[str] = None,
) -> Tracer:
    """Get or create the tracer instance.

    Args:
        name: Instrumenting module name.
        version: Instrumenting module version.

    Returns:
        Tracer instance (real or stub depending on OPENTELEMETRY_INSTALLED).
    """
    global _tracer  # noqa: PLW0603
    if _tracer is None:
        _tracer = trace.get_tracer(name, version)
    return _tracer


def inject_trace_context(job: "Job") -> None:
    """Inject current trace context into job metadata.

    This enables distributed tracing across process boundaries by
    storing the W3C trace context in job.meta["_otel_context"].

    Uses the standard opentelemetry.propagate API for context injection,
    which supports W3C Trace Context, Baggage, and future standards.

    Args:
        job: SAQ Job to inject context into.
    """
    if not OPENTELEMETRY_INSTALLED:
        return

    carrier: dict[str, str] = {}
    propagate.inject(carrier)

    if job.meta is None:
        job.meta = {}
    job.meta["_otel_context"] = carrier


def extract_trace_context(job: "Job") -> Optional[Any]:
    """Extract trace context from job metadata.

    Retrieves the stored W3C trace context from job.meta["_otel_context"]
    and returns an OpenTelemetry Context that can be used as a parent.

    Uses the standard opentelemetry.propagate API for context extraction.

    Args:
        job: SAQ Job to extract context from.

    Returns:
        OpenTelemetry Context or None if no context found.
    """
    if not OPENTELEMETRY_INSTALLED:
        return None

    carrier = (job.meta or {}).get("_otel_context", {})
    if not carrier:
        return None

    return propagate.extract(carrier)


def create_process_span(
    ctx: "Context",
    tracer: Tracer,
    queue_name: str,
) -> Optional[Span]:
    """Create a span for job processing (CONSUMER).

    Following OTEL messaging semantic conventions:
    - Span name: "{queue_name} process"
    - SpanKind: CONSUMER
    - Attributes follow messaging.* conventions

    Args:
        ctx: SAQ context containing job information.
        tracer: Tracer instance to use.
        queue_name: Name of the queue being processed.

    Returns:
        Created span or None if job not in context.
    """
    job: Optional[Job] = ctx.get("job")
    if job is None:
        return None

    parent_context = extract_trace_context(job)
    attributes: dict[str, Any] = {
        "messaging.system": "saq",
        "messaging.operation.name": "process",
        "messaging.destination.name": queue_name,
    }

    if job.id:
        attributes["messaging.message.id"] = job.id
    if job.function:
        attributes["saq.job.function"] = job.function
    if job.attempts is not None:
        attributes["saq.job.attempts"] = job.attempts

    span_kind = SpanKind.CONSUMER if OPENTELEMETRY_INSTALLED else None
    return tracer.start_span(
        name=f"{queue_name} process",
        context=parent_context,
        kind=span_kind,
        attributes=attributes,
    )


def end_process_span(
    ctx: "Context",
    span: Optional[Span],
    error: Optional[BaseException] = None,
) -> None:
    """End a job processing span with status and error information.

    Should be called in a finally block to ensure span is always ended.

    Args:
        ctx: SAQ context containing job information.
        span: Span to end (may be None).
        error: Optional exception that occurred during processing.
    """
    if span is None:
        return

    try:
        job: Optional[Job] = ctx.get("job")
        if job and job.status is not None:
            span.set_attribute("saq.job.status", str(job.status))

        if error is not None:
            if OPENTELEMETRY_INSTALLED:
                span.record_exception(error)
                span.set_status(Status(StatusCode.ERROR, str(error)))
            else:
                span.set_status(None, str(error))
        elif OPENTELEMETRY_INSTALLED:
            span.set_status(Status(StatusCode.OK))
    finally:
        span.end()


class InstrumentedQueue:
    """Queue wrapper that adds OTEL instrumentation to enqueue operations.

    This class wraps queue operations to create PRODUCER spans for
    enqueue operations, enabling end-to-end distributed tracing.

    Uses __getattr__ to pass through methods not explicitly wrapped
    (stats(), info(), sweep(), etc.).
    """

    def __init__(self, queue: "Queue", tracer: Tracer) -> None:
        """Initialize the instrumented queue wrapper.

        Args:
            queue: The underlying SAQ Queue to wrap.
            tracer: Tracer instance to use for creating spans.
        """
        self._queue = queue
        self._tracer = tracer

    @property
    def name(self) -> str:
        """Get the queue name."""
        return self._queue.name

    async def enqueue(
        self,
        job_or_func: str,
        *args: Any,
        **kwargs: Any,
    ) -> "Job":
        """Enqueue a job with OTEL instrumentation.

        Creates a PRODUCER span and injects trace context into
        the job for distributed tracing.

        Args:
            job_or_func: Job function name or identifier.
            *args: Positional arguments for the job.
            **kwargs: Keyword arguments for the job.

        Returns:
            The enqueued Job instance.
        """
        attributes: dict[str, Any] = {
            "messaging.system": "saq",
            "messaging.operation.name": "publish",
            "messaging.destination.name": self._queue.name,
            "saq.job.function": job_or_func,
        }

        span_kind = SpanKind.PRODUCER if OPENTELEMETRY_INSTALLED else None

        with self._tracer.start_as_current_span(
            name=f"{self._queue.name} publish",
            kind=span_kind,
            attributes=attributes,
        ) as span:
            job: Job = await self._queue.enqueue(job_or_func, *args, **kwargs)
            if job is not None:
                inject_trace_context(job)
                if job.id:
                    span.set_attribute("messaging.message.id", job.id)
            return job

    async def apply(
        self,
        job_or_func: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Apply a job with OTEL instrumentation (enqueue and wait for result).

        Creates a PRODUCER span and injects trace context into
        the job for distributed tracing.

        Args:
            job_or_func: Job function name or identifier.
            *args: Positional arguments for the job.
            **kwargs: Keyword arguments for the job.

        Returns:
            The job result.
        """
        attributes: dict[str, Any] = {
            "messaging.system": "saq",
            "messaging.operation.name": "publish",
            "messaging.destination.name": self._queue.name,
            "saq.job.function": job_or_func,
        }

        span_kind = SpanKind.PRODUCER if OPENTELEMETRY_INSTALLED else None

        with self._tracer.start_as_current_span(
            name=f"{self._queue.name} apply",
            kind=span_kind,
            attributes=attributes,
        ):
            return await self._queue.apply(job_or_func, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        """Pass through all other methods to the wrapped queue.

        This ensures methods like stats(), info(), sweep(), etc.
        work transparently through the wrapper.

        Args:
            name: Attribute name to retrieve.

        Returns:
            The attribute from the wrapped queue.
        """
        return getattr(self._queue, name)
