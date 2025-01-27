from __future__ import annotations

APP_DEFAULT_CONFIG_FILE_CONTENT = """
from __future__ import annotations

import asyncio
from logging import getLogger
from typing import TYPE_CHECKING

from examples import tasks
from litestar import Controller, Litestar, get

from litestar_saq import CronJob, QueueConfig, SAQConfig, SAQPlugin

if TYPE_CHECKING:
    from saq.types import Context, QueueInfo

    from litestar_saq.config import TaskQueues

logger = getLogger(__name__)


async def system_upkeep(_: Context) -> None:
    logger.info("Performing system upkeep operations.")
    logger.info("Simulating a long running operation.  Sleeping for 60 seconds.")
    await asyncio.sleep(3)
    logger.info("Simulating an even longer running operation.  Sleeping for 120 seconds.")
    await asyncio.sleep(3)
    logger.info("Long running process complete.")
    logger.info("Performing system upkeep operations.")


async def background_worker_task(_: Context) -> None:
    logger.info("Performing background worker task.")
    await asyncio.sleep(1)
    logger.info("Performing system upkeep operations.")


async def system_task(_: Context) -> None:
    logger.info("Performing simple system task")
    await asyncio.sleep(2)
    logger.info("System task complete.")


class SampleController(Controller):
    @get(path="/samples")
    async def samples_queue_info(self, task_queues: TaskQueues) -> QueueInfo:
        queue = task_queues.get("samples")
        return await queue.info()


saq = SAQPlugin(
    config=SAQConfig(
        web_enabled=True,
        use_server_lifespan=True,
        queue_configs=[
            QueueConfig(
                dsn="redis://localhost:6397/0",
                name="samples",
                tasks=[tasks.background_worker_task, tasks.system_task, tasks.system_upkeep],
                scheduled_tasks=[CronJob(function=tasks.system_upkeep, cron="* * * * *", timeout=600, ttl=2000)],
            ),
        ],
    ),
)
app = Litestar(plugins=[saq], route_handlers=[SampleController])

"""
