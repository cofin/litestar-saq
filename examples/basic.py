from __future__ import annotations

from typing import TYPE_CHECKING

from litestar import Controller, Litestar, get

from examples import tasks
from litestar_saq import QueueConfig, SAQConfig, SAQPlugin
from litestar_saq.base import CronJob

if TYPE_CHECKING:
    from saq.types import QueueInfo

    from litestar_saq.config import TaskQueues


class SampleController(Controller):
    @get(path="/samples")
    async def samples_queue_info(self, task_queues: TaskQueues) -> QueueInfo:
        """Check database available and returns app config info."""
        queue = task_queues.get("samples")
        return await queue.info()


saq = SAQPlugin(
    config=SAQConfig(
        redis_url="redis://localhost:6397/0",
        web_enabled=True,
        use_server_lifespan=True,
        queue_configs=[
            QueueConfig(
                name="samples",
                tasks=[tasks.background_worker_task, tasks.system_task, tasks.system_upkeep],
                scheduled_tasks=[CronJob(function=tasks.system_upkeep, cron="* * * * *", timeout=600, ttl=2000)],
            ),
        ],
    ),
)
app = Litestar(plugins=[saq], route_handlers=[SampleController])
