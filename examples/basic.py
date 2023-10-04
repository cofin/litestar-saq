from __future__ import annotations

from typing import TYPE_CHECKING

from litestar import Controller, Litestar, get

from litestar_saq import QueueConfig, SAQConfig, SAQPlugin

if TYPE_CHECKING:
    from saq.types import QueueInfo

    from litestar_saq.base import Queue


class SampleController(Controller):
    @get(path="/samples")
    async def samples_queue_info(self, task_queues: dict[str, Queue]) -> QueueInfo:
        """Check database available and returns app config info."""
        queue = task_queues.get("samples")
        return await queue.info()  # type: ignore[union-attr]


saq = SAQPlugin(config=SAQConfig(redis_url="redis://localhost:6397/0", queue_configs=[QueueConfig(name="samples")]))
app = Litestar(plugins=[saq], route_handlers=[SampleController])
