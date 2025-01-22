from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, cast

from litestar.utils.module_loader import import_string
from saq import Job as SaqJob
from saq import Worker as SaqWorker
from saq.job import CronJob as SaqCronJob

if TYPE_CHECKING:
    from collections.abc import Collection

    from saq.queue.base import Queue
    from saq.types import Function, PartialTimersDict, ReceivesContext


@dataclass
class Job(SaqJob):
    """Job Details"""


@dataclass
class CronJob(SaqCronJob):
    """Cron Job Details"""

    function: Function | str  # type: ignore[assignment]
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.function = self._get_or_import_function(self.function)  # pyright: ignore[reportIncompatibleMethodOverride]

    @staticmethod
    def _get_or_import_function(function_or_import_string: str | Function) -> Function:
        if isinstance(function_or_import_string, str):
            return cast("Function", import_string(function_or_import_string))
        return function_or_import_string


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
        startup: ReceivesContext | Collection[ReceivesContext] | None = None,
        shutdown: ReceivesContext | Collection[ReceivesContext] | None = None,
        before_process: ReceivesContext | Collection[ReceivesContext] | None = None,
        after_process: ReceivesContext | Collection[ReceivesContext] | None = None,
        timers: PartialTimersDict | None = None,
        dequeue_timeout: float = 0,
        separate_process: bool = True,
        multiprocessing_mode: Literal["multiprocessing", "threading"] = "multiprocessing",
    ) -> None:
        self.separate_process = separate_process
        self.multiprocessing_mode = multiprocessing_mode
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
        if not self.separate_process:
            self.SIGNALS = []
            loop = asyncio.get_running_loop()
            _ = loop.create_task(self.start())  # noqa: RUF006

    async def on_app_shutdown(self) -> None:
        """Attach the worker to the running event loop."""
        if not self.separate_process:
            loop = asyncio.get_running_loop()
            _ = loop.create_task(self.stop())  # noqa: RUF006
