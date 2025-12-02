"""Decorators for SAQ job functions.

This module provides decorators that add cross-cutting functionality to SAQ jobs,
such as automatic heartbeat monitoring.

Usage:
    >>> from litestar_saq import monitored_job
    >>>
    >>> @monitored_job(interval=10.0)
    >>> async def long_running_task(ctx):
    ...     # Long-running work with automatic heartbeats
    ...     await process_large_dataset()
    ...     return {"status": "complete"}
"""

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import TYPE_CHECKING, Optional, TypeVar, cast

from typing_extensions import Concatenate, ParamSpec

if TYPE_CHECKING:
    from saq.job import Job
    from saq.types import Context

__all__ = ("monitored_job",)

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def monitored_job(
    interval: float = 5.0,
) -> Callable[
    [Callable[Concatenate["Context", P], Awaitable[R]]],
    Callable[Concatenate["Context", P], Awaitable[R]],
]:
    """Decorator that adds automatic heartbeat monitoring to SAQ jobs.

    This decorator starts a background task that periodically calls job.update()
    to send heartbeats, preventing long-running jobs from timing out. The heartbeat
    task is automatically cleaned up when the job completes, fails, or is cancelled.

    Args:
        interval: Seconds between heartbeat updates. Defaults to 5.0 seconds.
                 Recommended: Set to ~10% of expected job duration.
                 Minimum: Should be > 0 to avoid excessive overhead.

    Returns:
        Decorated function with automatic heartbeat monitoring.

    Raises:
        ValueError: If interval is <= 0.

    Example:
        Basic usage with default interval::

            from litestar_saq import monitored_job

            @monitored_job()
            async def process_data(ctx):
                # Long-running work with automatic heartbeats
                await process_large_dataset()
                return {"status": "complete"}

        Custom interval::

            @monitored_job(interval=30.0)
            async def train_model(ctx, model_id: str):
                # Very long job - heartbeat every 30 seconds
                model = await load_model(model_id)
                for epoch in range(100):
                    await train_epoch(model)
                return {"model_id": model_id}

    Note:
        - If the job context doesn't contain a job object, monitoring is silently skipped
        - Heartbeat failures are logged as warnings but don't fail the job
        - Works with regular jobs, cron jobs, and scheduled jobs
        - Compatible with worker before_process and after_process hooks
    """
    if interval <= 0:
        msg = f"Heartbeat interval must be positive, got {interval}"
        raise ValueError(msg)

    def decorator(
        func: Callable[Concatenate["Context", P], Awaitable[R]],
    ) -> Callable[Concatenate["Context", P], Awaitable[R]]:
        @wraps(func)
        async def wrapper(ctx: "Context", *args: P.args, **kwargs: P.kwargs) -> R:
            job: Optional[Job] = ctx.get("job")  # pyright: ignore[reportUnknownMemberType]

            # Start background heartbeat monitoring
            heartbeat_task = asyncio.create_task(_heartbeat_loop(job, interval))

            try:
                # Execute the actual job function
                return await func(ctx, *args, **kwargs)
            finally:
                # Always clean up the heartbeat task
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task

        return cast("Callable[Concatenate[Context, P], Awaitable[R]]", wrapper)

    return decorator


async def _heartbeat_loop(job: "Optional[Job]", interval: float) -> None:
    """Run periodic heartbeat updates for a job.

    This is an internal function that runs in a background task, sending
    heartbeats at the specified interval until cancelled.

    Args:
        job: The SAQ job to monitor. If None, returns immediately.
        interval: Seconds between heartbeat updates.

    Note:
        - Catches and logs exceptions from job.update()
        - Continues on failure (doesn't stop monitoring)
        - Cancellation is expected and not logged as error
    """
    if job is None:
        logger.debug("No job in context, heartbeat monitoring disabled")
        return

    job_id = getattr(job, "id", "unknown")

    try:
        while True:
            await asyncio.sleep(interval)
            try:
                await job.update()
                logger.debug(
                    "Heartbeat sent for job %s",
                    job_id,
                    extra={
                        "job_id": job_id,
                        "event": "heartbeat_sent",
                    },
                )
            except Exception as e:  # noqa: BLE001
                # Log but continue - heartbeat failures shouldn't kill jobs
                logger.warning(
                    "Failed to send heartbeat for job %s: %s",
                    job_id,
                    e,
                    exc_info=True,
                    extra={
                        "job_id": job_id,
                        "event": "heartbeat_failed",
                        "error": str(e),
                    },
                )
    except asyncio.CancelledError:
        # Expected when job completes - don't re-raise
        logger.debug(
            "Heartbeat monitoring stopped for job %s",
            job_id,
            extra={
                "job_id": job_id,
                "event": "heartbeat_stopped",
            },
        )
