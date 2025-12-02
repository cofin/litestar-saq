from __future__ import annotations

from typing import TYPE_CHECKING

from litestar import Controller, Litestar, get

from examples import tasks
from litestar_saq import CronJob, QueueConfig, SAQConfig, SAQPlugin

if TYPE_CHECKING:
    from saq.types import QueueInfo

    from litestar_saq.config import TaskQueues


class SampleController(Controller):
    @get(path="/samples")
    async def samples_queue_info(self, task_queues: TaskQueues) -> QueueInfo:
        """Get information about the samples queue.

        Returns:
            Queue information including pending jobs and workers.
        """
        queue = task_queues.get("samples")
        return await queue.info()


saq = SAQPlugin(
    config=SAQConfig(
        web_enabled=True,
        use_server_lifespan=True,
        queue_configs=[
            QueueConfig(
                dsn="redis://localhost:6379/0",
                name="samples",
                tasks=[tasks.background_worker_task, tasks.system_task, tasks.system_upkeep],
                scheduled_tasks=[CronJob(function=tasks.system_upkeep, cron="* * * * *", timeout=600, ttl=2000)],
            ),
        ],
    ),
)
app = Litestar(plugins=[saq], route_handlers=[SampleController])
