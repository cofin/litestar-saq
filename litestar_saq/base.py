from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from saq import Job as SaqJob
from saq import Worker as SaqWorker
from saq.job import CronJob as SaqCronJob
from saq.queue import Queue as SaqQueue

if TYPE_CHECKING:
    from collections.abc import Collection
    from signal import Signals

    from redis.asyncio.client import Redis
    from saq.types import DumpType, Function, LoadType, PartialTimersDict, ReceivesContext


@dataclass
class Job(SaqJob):
    """Job Details"""

    job_name: str | None = None
    job_description: str | None = None


@dataclass
class CronJob(SaqCronJob):
    """Cron Job Details"""

    job_name: str | None = None
    job_description: str | None = None


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
        super().__init__(redis, name, dump, load, max_concurrent_ops)

    def temp(self, *args: Any, **kwargs: Any) -> None:
        """Initialize a new queue."""
        self._namespace = kwargs.pop("queue_namespace", "saq")
        super().__init__(*args, **kwargs)

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

    # same issue: https://github.com/samuelcolvin/arq/issues/182
    SIGNALS: list[Signals] = []

    def __init__(
        self,
        queue: Queue | SaqQueue,
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
    ) -> None:
        super().__init__(
            queue,
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
        loop = asyncio.get_running_loop()
        _ = loop.create_task(self.start())
