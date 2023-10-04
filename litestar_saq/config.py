from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar, cast

from litestar.exceptions import ImproperlyConfiguredException
from litestar.serialization import decode_json, encode_json
from redis.asyncio import ConnectionPool, Redis
from saq.types import DumpType, LoadType, PartialTimersDict, QueueInfo, QueueStats, ReceivesContext

from litestar_saq.base import CronJob, Job, Queue, Worker

if TYPE_CHECKING:
    from typing import Any

    from litestar import Litestar
    from litestar.datastructures.state import State

T = TypeVar("T")


def serializer(value: Any) -> str:
    """Serialize JSON field values.

    Args:
        value: Any json serializable value.

    Returns:
        JSON string.
    """
    return encode_json(value).decode("utf-8")


@dataclass
class SAQConfig:
    """SAQ Configuration."""

    queue_configs: list[QueueConfig] = field(default_factory=lambda: [QueueConfig()])
    """Configuration for Queues"""
    redis: Redis | None = None
    """Redis URL to connect with."""
    redis_url: str | None = None
    """Redis URL to connect with."""
    namespace: str = "saq"
    """Namespace to use for Redis"""
    queue_instances: dict[str, Queue] | None = None
    """Current configured queue instances.  When None, queues will be auto-created on startup"""
    queues_dependency_key: str = field(default="task_queues")
    """Key to use for storing dependency information in litestar."""
    worker_processes: int = 1
    """The number of worker processes to spawn.

    Default is set to 1.
    """
    web_enabled: bool = False
    """If true, the worker admin UI is launched on worker startup.."""
    json_deserializer: LoadType = decode_json
    """This is a Python callable that will
    convert a JSON string to a Python object. By default, this is set to Litestar's
    :attr:`decode_json() <.serialization.decode_json>` function."""
    json_serializer: DumpType = serializer
    """This is a Python callable that will render a given object as JSON.
    By default, Litestar's :attr:`encode_json() <.serialization.encode_json>` is used."""

    def __post_init__(self) -> None:
        if self.redis is not None and self.redis_url is not None:
            msg = "Only one of 'redis' or 'redis_url' can be provided."
            raise ImproperlyConfiguredException(msg)

    @property
    def signature_namespace(self) -> dict[str, Any]:
        """Return the plugin's signature namespace.

        Returns:
            A string keyed dict of names to be added to the namespace for signature forward reference resolution.
        """
        return {"Queue": Queue, "Worker": Worker, "QueueInfo": QueueInfo, "Job": Job, "QueueStats": QueueStats}

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

    def get_redis(self) -> Redis:
        """Get the configured Redis connection.

        Returns:
            Dictionary of queues.
        """
        if self.redis is not None:
            return self.redis
        pool = ConnectionPool.from_url(
            url=cast("str", self.redis_url),
            decode_responses=False,
            socket_connect_timeout=2,
            socket_keepalive=5,
            health_check_interval=5,
        )
        self.redis = Redis(connection_pool=pool)
        return self.redis

    def get_queues(self) -> dict[str, Queue]:
        """Get the configured SAQ queues.

        Returns:
            Dictionary of queues.
        """
        if self.queue_instances is not None:
            return self.queue_instances
        self.queue_instances = {}
        for queue_config in self.queue_configs:
            self.queue_instances[queue_config.name] = Queue(
                queue_namespace=self.namespace,
                redis=self.get_redis(),
                name=queue_config.name,
                dump=self.json_serializer,
                load=self.json_deserializer,
                max_concurrent_ops=queue_config.max_concurrent_ops,
            )
        return self.queue_instances


@dataclass
class QueueConfig:
    """SAQ Queue Configuration"""

    redis: Redis | None = None
    name: str = "default"
    concurrency: int = 10
    max_concurrent_ops: int = 20
    tasks: list[ReceivesContext] = field(default_factory=list)
    """Allowed list of functions to execute in this queue"""
    scheduled_tasks: list[CronJob] = field(default_factory=list)
    """Scheduled cron jobs to execute in this queue."""
    startup: ReceivesContext | None = None
    shutdown: ReceivesContext | None = None
    before_process: ReceivesContext | None = None
    after_process: ReceivesContext | None = None
    timers: PartialTimersDict | None = None
    dequeue_timeout: float = 0
