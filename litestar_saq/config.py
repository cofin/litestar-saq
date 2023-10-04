from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar, cast

from saq.types import QueueInfo

from litestar_saq.base import Job, Queue, Worker

if TYPE_CHECKING:
    from typing import Any

    from litestar import Litestar
    from litestar.datastructures.state import State

T = TypeVar("T")


@dataclass
class SAQConfig:
    """SAQ Configuration."""

    queues_dependency_key: str = field(default="task_queues")
    """Key to use for storing dependency information in litestar."""
    worker_concurrency: int = 10
    """The number of concurrent jobs allowed to execute per worker.

    Default is set to 10.
    """
    worker_processes: int = 1
    """The number of worker processes to spawn.

    Default is set to 1.
    """
    web_enabled: bool = False
    """If true, the worker admin UI is launched on worker startup.."""
    _queue_instances: dict[str, Queue] | None = None

    @property
    def signature_namespace(self) -> dict[str, Any]:
        """Return the plugin's signature namespace.

        Returns:
            A string keyed dict of names to be added to the namespace for signature forward reference resolution.
        """
        return {"Queue": Queue, "Worker": Worker, "QueueInfo": QueueInfo, "Job": Job}

    async def on_shutdown(self, app: Litestar) -> None:
        """Disposes of the SAQ Workers.

        Args:
            app: The ``Litestar`` instance.

        Returns:
            None
        """

    def provide_queues(self, state: State) -> dict[str, Queue]:
        """Create an engine instance.

        Args:
            state: The ``Litestar.state`` instance.

        Returns:
            An engine instance.
        """
        return cast("dict[str, Queue]", state.get(self.queues_dependency_key))


@dataclass
class QueueConfig:
    """SAQ Queue Configuration"""
