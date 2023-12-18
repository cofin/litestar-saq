from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Collection, Dict, Literal, Mapping, TypeVar, Union, cast

from litestar.exceptions import ImproperlyConfiguredException
from litestar.serialization import decode_json, encode_json
from redis.asyncio import ConnectionPool, Redis
from saq.queue import Queue as SaqQueue
from saq.types import DumpType as SaqDumpType
from saq.types import LoadType, PartialTimersDict, QueueInfo, QueueStats, ReceivesContext

from litestar_saq._util import import_string, module_to_os_path
from litestar_saq.base import CronJob, Job, Queue, Worker

if TYPE_CHECKING:
    from typing import Any

    from litestar import Litestar
    from litestar.datastructures.state import State
    from litestar.types.callable_types import Guard
    from saq.types import Function

T = TypeVar("T")
TaskQueue = Union[Queue, SaqQueue]
DumpType = Union[SaqDumpType, Callable[[Dict], bytes]]


def serializer(value: Any) -> str:
    """Serialize JSON field values.

    Args:
        value: Any json serializable value.

    Returns:
        JSON string.
    """
    return encode_json(value).decode("utf-8")


def _get_static_files() -> Path:
    return Path(module_to_os_path("saq") / "web" / "static")


@dataclass
class TaskQueues:
    queues: Mapping[str, Queue] = field(default_factory=dict)

    def get(self, name: str) -> Queue:
        queue = self.queues.get(name)
        if queue is not None:
            return queue
        msg = "Could not find the specified queue.  Please check your configuration."
        raise ImproperlyConfiguredException(msg)


@dataclass
class SAQConfig:
    """SAQ Configuration."""

    queue_configs: Collection[QueueConfig] = field(default_factory=lambda: [QueueConfig()])
    """Configuration for Queues"""
    redis: Redis | None = None
    """Pre-configured Redis instance to use."""
    redis_url: str | None = None
    """Redis URL to connect with."""
    redis_kwargs: dict[str, Any] = field(
        default_factory=lambda: {"socket_connect_timeout": 2, "socket_keepalive": 5, "health_check_interval": 5},
    )
    """Redis kwargs to pass into a redis instance."""
    namespace: str = "saq"
    """Namespace to use for Redis"""
    queue_instances: Mapping[str, Queue] | None = None
    """Current configured queue instances.  When None, queues will be auto-created on startup"""
    queues_dependency_key: str = field(default="task_queues")
    """Key to use for storing dependency information in litestar."""
    worker_processes: int = 1
    """The number of worker processes to spawn.

    Default is set to 1.
    """

    json_deserializer: LoadType = decode_json
    """This is a Python callable that will
    convert a JSON string to a Python object. By default, this is set to Litestar's
    :attr:`decode_json() <.serialization.decode_json>` function."""
    json_serializer: DumpType = serializer
    """This is a Python callable that will render a given object as JSON.
    By default, Litestar's :attr:`encode_json() <.serialization.encode_json>` is used."""
    static_files: Path = field(default_factory=_get_static_files)
    """Location of the static files to serve for the SAQ UI"""
    web_enabled: bool = False
    """If true, the worker admin UI is launched on worker startup.."""
    web_path = "/saq"
    """Base path to serve the SAQ web UI"""
    web_guards: list[Guard] | None = field(default=None)
    """Guards to apply to web endpoints."""
    web_include_in_schema: bool = True
    """Include Queue API endpoints in generated OpenAPI schema"""
    use_server_lifespan: bool = False
    """Utilize the server lifespan hook to run SAQ."""

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
        return {
            "Queue": Queue,
            "Worker": Worker,
            "QueueInfo": QueueInfo,
            "Job": Job,
            "QueueStats": QueueStats,
            "TaskQueues": TaskQueues,
            "TaskQueue": TaskQueue,
        }

    def provide_queues(self, state: State) -> TaskQueues:
        """Provide the configured job queues.

        Args:
            state: The ``Litestar.state`` instance.

        Returns:
            a ``TaskQueues`` instance.
        """
        return cast("TaskQueues", state.get(self.queues_dependency_key, TaskQueues()))

    def get_redis(self) -> Redis:
        """Get the configured Redis connection.

        Returns:
            Dictionary of queues.
        """
        if self.redis is not None:
            return self.redis
        pool = ConnectionPool.from_url(
            url=cast("str", self.redis_url),
        )
        self.redis = Redis(connection_pool=pool, **self.redis_kwargs)
        return self.redis

    def get_queues(self) -> TaskQueues:
        """Get the configured SAQ queues.

        Returns:
            Dictionary of queues.
        """
        if self.queue_instances is not None:
            return TaskQueues(queues=self.queue_instances)
        self.queue_instances = {}
        for queue_config in self.queue_configs:
            self.queue_instances[queue_config.name] = Queue(
                queue_namespace=self.namespace,
                redis=self.get_redis(),
                name=queue_config.name,
                dump=cast("SaqDumpType", self.json_serializer),
                load=self.json_deserializer,
                max_concurrent_ops=queue_config.max_concurrent_ops,
            )
        return TaskQueues(queues=self.queue_instances)

    def create_app_state_items(self) -> dict[str, Any]:
        """Key/value pairs to be stored in application state."""
        return {
            self.queues_dependency_key: self.get_queues(),
        }

    def update_app_state(self, app: Litestar) -> None:
        """Set the app state with worker queues.

        Args:
            app: The ``Litestar`` instance.
        """
        app.state.update(self.create_app_state_items())


@dataclass
class QueueConfig:
    """SAQ Queue Configuration"""

    redis: Redis | None = None
    """Pre-configured Redis instance to use."""
    name: str = "default"
    """The name of the queue to create."""
    concurrency: int = 10
    """Number of jobs to process concurrently"""
    max_concurrent_ops: int = 15
    """Maximum concurrent operations. (default 15)
            This throttles calls to `enqueue`, `job`, and `abort` to prevent the Queue
            from consuming too many Redis connections."""
    tasks: Collection[ReceivesContext | tuple[str, Function] | str] = field(default_factory=list)
    """Allowed list of functions to execute in this queue"""
    scheduled_tasks: Collection[CronJob] = field(default_factory=list)
    """Scheduled cron jobs to execute in this queue."""
    startup: ReceivesContext | None = None
    """Async callable to call on startup"""
    shutdown: ReceivesContext | None = None
    """Async callable to call on shutdown"""
    before_process: ReceivesContext | None = None
    """Async callable to call before a job processes"""
    after_process: ReceivesContext | None = None
    """Async callable to call after a job processes"""
    timers: PartialTimersDict | None = None
    """Dict with various timer overrides in seconds
            schedule: how often we poll to schedule jobs
            stats: how often to update stats
            sweep: how often to clean up stuck jobs
            abort: how often to check if a job is aborted"""
    dequeue_timeout: float = 0
    """How long it will wait to dequeue"""
    multiprocessing_mode: Literal["multiprocessing", "threading"] = "multiprocessing"
    """Executes with the multiprocessing or threading backend.
            Set it threading for workloads that aren't CPU bound."""
    separate_process: bool = True
    """Executes as a separate event loop when True.
            Set it False to execute within the Litestar application."""

    def __post_init__(self) -> None:
        self.tasks = [self._get_or_import_task(task) for task in self.tasks]

    @staticmethod
    def _get_or_import_task(task_or_import_string: str | tuple[str, Function] | ReceivesContext) -> ReceivesContext:
        if isinstance(task_or_import_string, str):
            return cast("ReceivesContext", import_string(task_or_import_string))
        if isinstance(task_or_import_string, tuple):
            return task_or_import_string[1]
        return task_or_import_string
