"""Example demonstrating default lifecycle hooks.

This example shows how to use litestar-saq's default hooks for
logging and timing without writing custom hook functions.

Run with: litestar --app examples.hooks:app run
Start worker with: litestar --app examples.hooks:app saq run-worker
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from litestar import Controller, Litestar, get

from litestar_saq import (
    CronJob,
    QueueConfig,
    SAQConfig,
    SAQPlugin,
    # Default hooks for easy observability
    shutdown_logger,
    startup_logger,
    timing_after_process,
    timing_before_process,
)

if TYPE_CHECKING:
    from saq.types import Context, QueueInfo

    from litestar_saq.config import TaskQueues


async def example_task(_ctx: Context, *, name: str) -> dict[str, str]:
    """Example background task that greets a user.

    This task demonstrates a simple async operation with timing hooks
    that will automatically log execution duration.

    Returns:
        Dictionary with greeting message.
    """
    await asyncio.sleep(0.5)  # Simulate work
    return {"message": f"Hello, {name}!"}


async def slow_task(_ctx: Context, *, duration: float = 2.0) -> dict[str, float]:
    """Example slow task for timing demonstration.

    Args:
        _ctx: SAQ context.
        duration: How long to sleep (default 2 seconds).

    Returns:
        Dictionary with the sleep duration.
    """
    await asyncio.sleep(duration)
    return {"slept_for": duration}


async def scheduled_task(_ctx: Context) -> dict[str, str]:
    """Example scheduled task that runs on cron.

    Returns:
        Dictionary with execution status.
    """
    return {"status": "scheduled task executed"}


class TaskController(Controller):
    """Controller for viewing task queue info."""

    @get(path="/queue-info")
    async def get_queue_info(self, task_queues: TaskQueues) -> QueueInfo:
        """Get information about the task queue.

        Returns:
            Queue information including pending jobs and workers.
        """
        queue = task_queues.get("default")
        return await queue.info()


# Configure SAQ with default hooks for observability
saq = SAQPlugin(
    config=SAQConfig(
        web_enabled=True,
        use_server_lifespan=True,
        queue_configs=[
            QueueConfig(
                dsn="redis://localhost:6379/0",
                name="default",
                tasks=[example_task, slow_task, scheduled_task],
                scheduled_tasks=[
                    CronJob(
                        function=scheduled_task,
                        cron="*/5 * * * *",  # Every 5 minutes
                        timeout=60,
                    ),
                ],
                # Default hooks provide easy observability
                startup=[startup_logger],
                shutdown=[shutdown_logger],
                before_process=[timing_before_process],
                after_process=[timing_after_process],
            ),
        ],
    ),
)

app = Litestar(plugins=[saq], route_handlers=[TaskController])
