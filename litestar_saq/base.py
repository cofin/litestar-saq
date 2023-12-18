from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, cast

from saq import Job as SaqJob
from saq import Worker as SaqWorker
from saq.job import CronJob as SaqCronJob
from saq.queue import Queue as SaqQueue

from litestar_saq._util import import_string

if TYPE_CHECKING:
    from collections.abc import Collection

    from redis.asyncio.client import Redis
    from saq.types import DumpType as SaqDumpType
    from saq.types import Function, LoadType, PartialTimersDict, ReceivesContext

    from litestar_saq.config import DumpType


@dataclass
class Job(SaqJob):
    """Job Details"""


@dataclass
class CronJob(SaqCronJob):
    """Cron Job Details"""

    function: Function | str  # type: ignore[assignment]
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.function = self._get_or_import_function(self.function)

    @staticmethod
    def _get_or_import_function(function_or_import_string: str | Function) -> Function:
        if isinstance(function_or_import_string, str):
            return cast("Function", import_string(function_or_import_string))
        return function_or_import_string


class Queue(SaqQueue):
    """[SAQ Queue](https://github.com/tobymao/saq/blob/master/saq/queue.py).

    Configures `msgspec` for msgpack serialization/deserialization if not otherwise configured.

    Parameters
    ----------
    *args : Any
        Passed through to `saq.Queue.__init__()`
    **kwargs : Any
        Passed through to `saq.Queue.__init__()`
    """

    def __init__(
        self,
        redis: Redis[bytes],
        name: str = "default",
        dump: DumpType | None = None,
        load: LoadType | None = None,
        max_concurrent_ops: int = 20,
        queue_namespace: str | None = None,
    ) -> None:
        self._namespace = queue_namespace if queue_namespace is not None else "saq"
        super().__init__(redis, name, cast("SaqDumpType", dump), load, max_concurrent_ops)

    def namespace(self, key: str) -> str:
        """Make the namespace unique per app."""
        return f"{self._namespace}:{self.name}:{key}"

    def job_id(self, job_key: str) -> str:
        """Job ID.

        Args:
            job_key (str): Sets the job ID for the given key

        Returns:
            str: Job ID for the specified key
        """
        return f"{self._namespace}:{self.name}:job:{job_key}"


class Worker(SaqWorker):
    """Worker."""

    """
    # same issue: https://github.com/samuelcolvin/arq/issues/182
    SIGNALS: list[Signals] = []
    """

    def __init__(
        self,
        queue: Queue,
        functions: Collection[Function | tuple[str, Function]],
        *,
        concurrency: int = 10,
        cron_jobs: Collection[CronJob] | None = None,
        startup: ReceivesContext | None = None,
        shutdown: ReceivesContext | None = None,
        before_process: ReceivesContext | None = None,
        after_process: ReceivesContext | None = None,
        timers: PartialTimersDict | None = None,
        dequeue_timeout: float = 0,
        separate_process: bool = True,
        multiprocessing_mode: Literal["multiprocessing", "threading"] = "multiprocessing",
    ) -> None:
        self.separate_process = separate_process
        self.multiprocessing_mode = multiprocessing_mode
        super().__init__(
            cast("SaqQueue", queue),
            functions,
            concurrency=concurrency,
            cron_jobs=cron_jobs,
            startup=startup,
            shutdown=shutdown,
            before_process=before_process,
            after_process=after_process,
            timers=timers,
            dequeue_timeout=dequeue_timeout,
        )

    async def on_app_startup(self) -> None:
        """Attach the worker to the running event loop."""
        if not self.separate_process:
            self.SIGNALS = []
            loop = asyncio.get_running_loop()
            _ = loop.create_task(self.start())

    async def on_app_shutdown(self) -> None:
        """Attach the worker to the running event loop."""
        if not self.separate_process:
            loop = asyncio.get_running_loop()
            _ = loop.create_task(self.stop())
