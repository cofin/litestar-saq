"""Default lifecycle hooks for SAQ workers.

This module provides ready-to-use hooks for common operations like logging
and timing. These hooks reduce boilerplate and demonstrate best practices
for implementing custom hooks.

Usage:
    >>> from litestar_saq import QueueConfig
    >>> from litestar_saq.hooks import startup_logger, shutdown_logger
    >>>
    >>> config = QueueConfig(
    ...     dsn="redis://localhost:6379/0",
    ...     startup=[startup_logger],
    ...     shutdown=[shutdown_logger],
    ... )

All hooks follow SAQ's standard signature: async def hook(ctx: Context) -> None
"""

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from saq.types import Context

__all__ = (
    "after_process_logger",
    "before_process_logger",
    "shutdown_logger",
    "startup_logger",
    "timing_after_process",
    "timing_before_process",
)

logger = logging.getLogger(__name__)


async def startup_logger(ctx: "Context") -> None:
    """Log worker startup information.

    Logs structured information when a SAQ worker starts, including
    worker ID, queue name, and timestamp. Uses INFO level logging.

    Args:
        ctx: SAQ context dictionary. Optional keys:
            - worker_id: Unique identifier for this worker
            - queue: Queue instance with name attribute

    Example:
        >>> from litestar_saq import QueueConfig
        >>> from litestar_saq.hooks import startup_logger
        >>>
        >>> config = QueueConfig(
        ...     dsn="redis://localhost:6379/0",
        ...     startup=[startup_logger],
        ... )

    Notes:
        Safe to use with missing context keys - will use sensible defaults.
        Can be combined with custom startup hooks in a list.
    """
    worker_id = ctx.get("worker_id", "unknown")  # pyright: ignore[reportUnknownMemberType]
    queue = ctx.get("queue")  # pyright: ignore[reportUnknownMemberType]
    queue_name = getattr(queue, "name", "unknown") if queue else "unknown"

    logger.info(
        "SAQ worker starting",
        extra={
            "worker_id": worker_id,
            "queue_name": queue_name,
            "event": "worker_startup",
        },
    )


async def shutdown_logger(ctx: "Context") -> None:
    """Log worker shutdown information.

    Logs structured information when a SAQ worker shuts down, including
    worker ID and queue name. Uses INFO level logging.

    Args:
        ctx: SAQ context dictionary. Optional keys:
            - worker_id: Unique identifier for this worker
            - queue: Queue instance with name attribute

    Example:
        >>> from litestar_saq import QueueConfig
        >>> from litestar_saq.hooks import shutdown_logger
        >>>
        >>> config = QueueConfig(
        ...     dsn="redis://localhost:6379/0",
        ...     shutdown=[shutdown_logger],
        ... )

    Notes:
        Safe to use with missing context keys - will use sensible defaults.
        Can be combined with custom shutdown hooks in a list.
    """
    worker_id = ctx.get("worker_id", "unknown")  # pyright: ignore[reportUnknownMemberType]
    queue = ctx.get("queue")  # pyright: ignore[reportUnknownMemberType]
    queue_name = getattr(queue, "name", "unknown") if queue else "unknown"

    logger.info(
        "SAQ worker shutting down",
        extra={
            "worker_id": worker_id,
            "queue_name": queue_name,
            "event": "worker_shutdown",
        },
    )


async def before_process_logger(ctx: "Context") -> None:
    """Log job processing start.

    Logs information when a job begins processing, including job ID
    and function name. Uses DEBUG level logging to avoid noise.

    Args:
        ctx: SAQ context dictionary. Expected keys:
            - job: Current job being processed (with id and function attributes)

    Example:
        >>> from litestar_saq import QueueConfig
        >>> from litestar_saq.hooks import before_process_logger
        >>>
        >>> config = QueueConfig(
        ...     dsn="redis://localhost:6379/0",
        ...     before_process=[before_process_logger],
        ... )

    Notes:
        Uses DEBUG level to avoid excessive logging in production.
        Does not log job arguments to avoid exposing sensitive data.
    """
    job = ctx.get("job")  # pyright: ignore[reportUnknownMemberType]
    job_id = getattr(job, "id", "unknown") if job else "unknown"
    function = getattr(job, "function", "unknown") if job else "unknown"

    logger.debug(
        "Processing job",
        extra={
            "job_id": job_id,
            "function": function,
            "event": "job_start",
        },
    )


async def after_process_logger(ctx: "Context") -> None:
    """Log job processing completion.

    Logs information when a job finishes processing, including job ID,
    function name, and status. Uses DEBUG level logging.

    Args:
        ctx: SAQ context dictionary. Expected keys:
            - job: Current job being processed (with id, function, status attributes)

    Example:
        >>> from litestar_saq import QueueConfig
        >>> from litestar_saq.hooks import after_process_logger
        >>>
        >>> config = QueueConfig(
        ...     dsn="redis://localhost:6379/0",
        ...     after_process=[after_process_logger],
        ... )

    Notes:
        Uses DEBUG level to avoid excessive logging in production.
        Status is extracted from the job object if available.
    """
    job = ctx.get("job")  # pyright: ignore[reportUnknownMemberType]
    job_id = getattr(job, "id", "unknown") if job else "unknown"
    function = getattr(job, "function", "unknown") if job else "unknown"
    status = getattr(job, "status", "unknown") if job else "unknown"

    logger.debug(
        "Job completed",
        extra={
            "job_id": job_id,
            "function": function,
            "status": status,
            "event": "job_complete",
        },
    )


async def timing_before_process(ctx: "Context") -> None:
    """Record job start time for duration calculation.

    Stores the current timestamp in the context under '_timing_start' key.
    Used in conjunction with timing_after_process to calculate job duration.

    Args:
        ctx: SAQ context dictionary. Modified to include:
            - _timing_start: Current time in seconds since epoch

    Example:
        >>> from litestar_saq import QueueConfig
        >>> from litestar_saq.hooks import timing_before_process, timing_after_process
        >>>
        >>> config = QueueConfig(
        ...     dsn="redis://localhost:6379/0",
        ...     before_process=[timing_before_process],
        ...     after_process=[timing_after_process],
        ... )

    Notes:
        - Does not log anything (silent operation)
        - Must be used before timing_after_process
        - Uses internal key (_timing_start) to avoid conflicts with user data
    """
    ctx["_timing_start"] = time.time()  # type: ignore[typeddict-unknown-key]  # pyright: ignore[reportGeneralTypeIssues]


async def timing_after_process(ctx: "Context") -> None:
    """Calculate and log job execution duration.

    Calculates job duration using the timestamp from timing_before_process
    and logs it at INFO level with job metadata.

    Args:
        ctx: SAQ context dictionary. Expected keys:
            - _timing_start: Start timestamp from timing_before_process
            - job: Current job being processed

    Example:
        >>> from litestar_saq import QueueConfig
        >>> from litestar_saq.hooks import timing_before_process, timing_after_process
        >>>
        >>> config = QueueConfig(
        ...     dsn="redis://localhost:6379/0",
        ...     before_process=[timing_before_process],
        ...     after_process=[timing_after_process],
        ... )

    Notes:
        - Requires timing_before_process to be called first
        - Safely handles missing _timing_start key with a warning
        - Logs duration in milliseconds for readability
    """
    start_time = ctx.get("_timing_start")  # pyright: ignore[reportUnknownMemberType]
    if start_time is None:
        logger.warning("timing_after_process called without timing_before_process")
        return

    duration_ms = (time.time() - start_time) * 1000  # type: ignore[operator]
    job = ctx.get("job")  # pyright: ignore[reportUnknownMemberType]
    job_id = getattr(job, "id", "unknown") if job else "unknown"
    function = getattr(job, "function", "unknown") if job else "unknown"

    logger.info(
        "Job completed in %.2fms",
        duration_ms,
        extra={
            "job_id": job_id,
            "function": function,
            "duration_ms": duration_ms,
            "event": "job_timing",
        },
    )
