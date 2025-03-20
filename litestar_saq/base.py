import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Optional, Union, cast

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

    function: "Union[Function, str]"  # type: ignore[assignment]
    meta: "dict[str, Any]" = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.function = self._get_or_import_function(self.function)  # pyright: ignore[reportIncompatibleMethodOverride]

    @staticmethod
    def _get_or_import_function(function_or_import_string: "Union[str, Function]") -> "Function":
        if isinstance(function_or_import_string, str):
            return cast("Function", import_string(function_or_import_string))
        return function_or_import_string


class Worker(SaqWorker):
    """Worker."""

    def __init__(
        self,
        queue: "Queue",
        functions: "Collection[Union[Function, tuple[str, Function]]]",
        *,
        concurrency: int = 10,
        cron_jobs: "Optional[Collection[CronJob]]" = None,
        startup: "Optional[Union[ReceivesContext, Collection[ReceivesContext]]]" = None,
        shutdown: "Optional[Union[ReceivesContext, Collection[ReceivesContext]]]" = None,
        before_process: "Optional[Union[ReceivesContext, Collection[ReceivesContext]]]" = None,
        after_process: "Optional[Union[ReceivesContext, Collection[ReceivesContext]]]" = None,
        timers: "Optional[PartialTimersDict]" = None,
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
            self._saq_asyncio_tasks = loop.create_task(self.start())

    async def on_app_shutdown(self) -> None:
        """Attach the worker to the running event loop."""
        if not self.separate_process:
            loop = asyncio.get_running_loop()
            self._saq_asyncio_tasks = loop.create_task(self.stop())
