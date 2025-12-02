from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timezone, tzinfo
from typing import TYPE_CHECKING, Any, Literal, cast

from litestar.utils.module_loader import import_string
from saq import Job as SaqJob
from saq.job import CronJob as SaqCronJob
from saq.types import Context
from saq.worker import Worker as SaqWorker

if TYPE_CHECKING:
    from collections.abc import Collection

    from saq.queue.base import Queue
    from saq.types import Function, PartialTimersDict, ReceivesContext

JsonDict = dict[str, Any]


@dataclass
class Job(SaqJob):
    """Job Details"""


@dataclass
class CronJob(SaqCronJob[Context]):
    """Cron Job Details"""

    function: Function[Context] | str  # type: ignore[assignment]
    meta: dict[str, Any] = field(default_factory=dict)  # pyright: ignore

    def __post_init__(self) -> None:
        self.function = self._get_or_import_function(self.function)  # pyright: ignore[reportIncompatibleMethodOverride]

    @staticmethod
    def _get_or_import_function(
        function_or_import_string: str | Function[Context],
    ) -> Function[Context]:
        if isinstance(function_or_import_string, str):
            return cast("Function[Context]", import_string(function_or_import_string))
        return function_or_import_string


class Worker(SaqWorker[Context]):
    """Worker.

    Extends SAQ's Worker with Litestar lifecycle integration.

    Args:
        queue: SAQ Queue instance.
        functions: Task functions to register.
        id: Optional worker identifier.
        concurrency: Number of concurrent tasks (default: 10).
        cron_jobs: Scheduled cron jobs.
        cron_tz: Timezone for cron jobs (default: UTC).
        startup: Async callable(s) to run on worker startup.
        shutdown: Async callable(s) to run on worker shutdown.
        before_process: Async callable(s) to run before each job.
        after_process: Async callable(s) to run after each job.
        timers: Dict with timer overrides (schedule, stats, sweep, abort).
        dequeue_timeout: How long to wait for dequeue (default: 0).
        burst: If True, process jobs in burst mode.
        max_burst_jobs: Maximum jobs in burst mode.
        metadata: Arbitrary metadata to register with SAQ.
        separate_process: Execute as separate event loop (default: True).
        multiprocessing_mode: Backend for multiprocessing (default: "multiprocessing").
        shutdown_grace_period_s: Time in seconds to allow jobs to complete
            gracefully before forced shutdown. Defaults to SAQ's internal default.
        cancellation_hard_deadline_s: Absolute deadline for task cancellation.
            Prevents zombie tasks from blocking shutdown. Defaults to SAQ's default (1.0s).
        poll_interval: Queue polling interval in seconds. Lower values reduce
            latency but increase CPU/database load. Defaults to backend-specific value.
    """

    def __init__(
        self,
        queue: Queue,
        functions: Collection[Function[Context] | tuple[str, Function[Context]]],
        *,
        id: str | None = None,  # noqa: A002
        concurrency: int = 10,
        cron_jobs: Collection[CronJob] | None = None,
        cron_tz: tzinfo = timezone.utc,
        startup: ReceivesContext[Context] | Collection[ReceivesContext[Context]] | None = None,
        shutdown: ReceivesContext[Context] | Collection[ReceivesContext[Context]] | None = None,
        before_process: ReceivesContext[Context] | Collection[ReceivesContext[Context]] | None = None,
        after_process: ReceivesContext[Context] | Collection[ReceivesContext[Context]] | None = None,
        timers: PartialTimersDict | None = None,
        dequeue_timeout: float = 0,
        burst: bool = False,
        max_burst_jobs: "Optional[int]" = None,
        metadata: "Optional[JsonDict]" = None,
        separate_process: bool = True,
        multiprocessing_mode: Literal["multiprocessing", "threading"] = "multiprocessing",
        shutdown_grace_period_s: "Optional[int]" = None,
        cancellation_hard_deadline_s: float = 1.0,
        poll_interval: float | None = None,
    ) -> None:
        self.separate_process = separate_process
        self.multiprocessing_mode = multiprocessing_mode

        # Build kwargs for super().__init__, only including new params if provided
        kwargs: dict[str, Any] = {
            "id": id,
            "concurrency": concurrency,
            "cron_jobs": cron_jobs,
            "cron_tz": cron_tz,
            "startup": startup,
            "shutdown": shutdown,
            "before_process": before_process,
            "after_process": after_process,
            "timers": timers,
            "dequeue_timeout": dequeue_timeout,
            "burst": burst,
            "max_burst_jobs": max_burst_jobs,
            "metadata": metadata,
        }

        # Add SAQ 0.26+ parameters if provided
        if shutdown_grace_period_s is not None:
            kwargs["shutdown_grace_period_s"] = shutdown_grace_period_s
        if cancellation_hard_deadline_s is not None:
            kwargs["cancellation_hard_deadline_s"] = cancellation_hard_deadline_s
        if poll_interval is not None:
            kwargs["poll_interval"] = poll_interval

        super().__init__(queue, functions, **kwargs)

    async def on_app_startup(self) -> None:
        """Attach the worker to the running event loop."""
        if not self.separate_process:
            self.SIGNALS = []
            loop = asyncio.get_running_loop()
            self._saq_asyncio_tasks = loop.create_task(self.start())

    async def on_app_shutdown(self) -> None:
        """Attach the worker to the running event loop."""
        if not self.separate_process:
            loop = asyncio.get_running_loop()
            self._saq_asyncio_tasks = loop.create_task(self.stop())
