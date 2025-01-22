from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypedDict, TypeVar, cast

from litestar.exceptions import ImproperlyConfiguredException
from litestar.serialization import decode_json, encode_json
from litestar.utils.module_loader import import_string, module_to_os_path
from saq.queue.base import Queue
from saq.types import DumpType, LoadType, PartialTimersDict, QueueInfo, ReceivesContext, WorkerInfo

from litestar_saq.base import CronJob, Job, Worker

if TYPE_CHECKING:
    from typing import Any

    from litestar import Litestar
    from litestar.datastructures.state import State
    from litestar.types.callable_types import Guard  # pyright: ignore[reportUnknownVariableType]
    from saq.types import Function

T = TypeVar("T")


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

    dsn: str
    """DSN for connecting to backend. e.g. 'redis://...' or 'postgres://...'.
    """
    queue_configs: Collection[QueueConfig] = field(default_factory=lambda: [QueueConfig()])
    """Configuration for Queues"""

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
    web_path: str = "/saq"
    """Base path to serve the SAQ web UI"""
    web_guards: list[Guard] | None = field(default=None)
    """Guards to apply to web endpoints."""
    web_include_in_schema: bool = False
    """Include Queue API endpoints in generated OpenAPI schema"""
    use_server_lifespan: bool = False
    """Utilize the server lifespan hook to run SAQ."""

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
            "WorkerInfo": WorkerInfo,
            "Job": Job,
            "TaskQueues": TaskQueues,
        }

    def provide_queues(self, state: State) -> TaskQueues:
        """Provide the configured job queues.

        Args:
            state: The ``Litestar.state`` instance.

        Returns:
            a ``TaskQueues`` instance.
        """
        return cast("TaskQueues", state.get(self.queues_dependency_key, TaskQueues()))

    def filter_delete_queues(self, queues: list[str]) -> None:
        """Remove all queues except the ones in the given list."""
        new_config = [queue_config for queue_config in self.queue_configs if queue_config.name in queues]
        self.queue_configs = new_config
        if self.queue_instances is not None:
            for queue_name in dict(self.queue_instances):
                if queue_name not in queues:
                    del self.queue_instances[queue_name]  # type: ignore  # noqa: PGH003

    def get_queues(self) -> TaskQueues:
        """Get the configured SAQ queues."""
        if self.queue_instances is not None:
            return TaskQueues(queues=self.queue_instances)
        self.queue_instances = {}
        for queue_config in self.queue_configs:
            self.queue_instances[queue_config.name] = Queue.from_url(
                url=self.dsn,
                dump=self.json_serializer,
                load=self.json_deserializer,
                **queue_config.broker_options,
            )
        return TaskQueues(queues=self.queue_instances)

    def create_app_state_items(self) -> dict[str, Any]:
        """Key/value pairs to be stored in application state."""
        return {self.queues_dependency_key: self.get_queues()}

    def update_app_state(self, app: Litestar) -> None:
        """Set the app state with worker queues."""
        app.state.update(self.create_app_state_items())


class RedisQueueOptions(TypedDict, total=False):
    """Options for the Redis backend."""

    max_concurrent_ops: int
    """Maximum concurrent operations. (default 15)
        This throttles calls to `enqueue`, `job`, and `abort` to prevent the Queue
        from consuming too many Redis connections."""


class PostgresQueueOptions(TypedDict, total=False):
    """Options for the Postgres backend."""

    versions_table: str
    jobs_table: str
    stats_table: str
    min_size: int
    max_size: int
    poll_interval: float
    saq_lock_keyspace: int
    job_lock_keyspace: int
    priorities: tuple[int, int]


@dataclass
class QueueConfig:
    """SAQ Queue Configuration"""

    name: str = "default"
    """The name of the queue to create."""
    concurrency: int = 10
    """Number of jobs to process concurrently."""
    broker_options: RedisQueueOptions | PostgresQueueOptions | dict[str, Any] = field(default_factory=dict)
    """Broker-specific options. For Redis or Postgres backends."""
    tasks: Collection[ReceivesContext | tuple[str, Function] | str] = field(default_factory=list)
    """Allowed list of functions to execute in this queue."""
    scheduled_tasks: Collection[CronJob] = field(default_factory=list)
    """Scheduled cron jobs to execute in this queue."""
    startup: ReceivesContext | str | Collection[ReceivesContext | str] | None = None
    """Async callable to call on startup."""
    shutdown: ReceivesContext | str | Collection[ReceivesContext | str] | None = None
    """Async callable to call on shutdown."""
    before_process: ReceivesContext | str | Collection[ReceivesContext | str] | None = None
    """Async callable to call before a job processes."""
    after_process: ReceivesContext | str | Collection[ReceivesContext | str] | None = None
    """Async callable to call after a job processes."""
    timers: PartialTimersDict | None = None
    """Dict with various timer overrides in seconds
            schedule: how often we poll to schedule jobs
            stats: how often to update stats
            sweep: how often to clean up stuck jobs
            abort: how often to check if a job is aborted"""
    dequeue_timeout: float = 0
    """How long to wait to dequeue."""
    multiprocessing_mode: Literal["multiprocessing", "threading"] = "multiprocessing"
    """Executes with the multiprocessing or threading backend.  Multi-processing is recommended and how SAQ is designed to work."""
    separate_process: bool = True
    """Executes as a separate event loop when True.
            Set it False to execute within the Litestar application."""

    def __post_init__(self) -> None:
        self.tasks = [self._get_or_import_task(task) for task in self.tasks]
        if self.startup is not None and not isinstance(self.startup, Collection):
            self.startup = [self.startup]
        if self.shutdown is not None and not isinstance(self.shutdown, Collection):
            self.shutdown = [self.shutdown]
        if self.before_process is not None and not isinstance(self.before_process, Collection):
            self.before_process = [self.before_process]
        if self.after_process is not None and not isinstance(self.after_process, Collection):
            self.after_process = [self.after_process]
        self.startup = [self._get_or_import_task(task) for task in self.startup or []]
        self.shutdown = [self._get_or_import_task(task) for task in self.shutdown or []]
        self.before_process = [self._get_or_import_task(task) for task in self.before_process or []]
        self.after_process = [self._get_or_import_task(task) for task in self.after_process or []]

    @staticmethod
    def _get_or_import_task(task_or_import_string: str | tuple[str, Function] | ReceivesContext) -> ReceivesContext:
        if isinstance(task_or_import_string, str):
            return cast("ReceivesContext", import_string(task_or_import_string))
        if isinstance(task_or_import_string, tuple):
            return task_or_import_string[1]
        return task_or_import_string
