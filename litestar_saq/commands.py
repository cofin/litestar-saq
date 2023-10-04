from __future__ import annotations

from typing import TYPE_CHECKING, Any

from litestar_saq.base import Queue, Worker

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Collection

    import saq
    from saq.types import Context

DEFAULT_CONCURRENCY = 10


def create_worker_instance(
    queue: Queue,
    tasks: Collection[Callable[..., Any] | tuple[str, Callable]],
    scheduled_tasks: Collection[saq.CronJob] | None = None,
    startup: Callable[[Context], Awaitable[Any]] | None = None,
    shutdown: Callable[[Context], Awaitable[Any]] | None = None,
    before_process: Callable[[Context], Awaitable[Any]] | None = None,
    after_process: Callable[[Context], Awaitable[Any]] | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> Worker:
    """Create worker instance.

    Args:
        queue: Queue: The queue instance to use for the worker
        tasks: Collection[Callable[..., Any] | tuple[str, Callable]]: Functions to be called via the async workers
        scheduled_tasks (Collection[saq.CronJob] | None, optional): Scheduled functions to be called via the async workers. Defaults to None.
        startup (Callable[[Context]], Awaitable[Any]] | None, optional): Async function called on startup. Defaults to None.
        shutdown (Callable[[Context]], Awaitable[Any]] | None, optional): Async functions called on startup. Defaults to None.
        before_process (Callable[[Context]], Awaitable[Any]] | None, optional): Async function called before a job processes.. Defaults to None.
        after_process (Callable[[Context]], Awaitable[Any]] | None, optional): _Async function called after a job processes. Defaults to None.
        concurrency (int, optional):  Defaults to DEFAULT_CONCURRENCY.

    Returns:
        Worker: The worker instance
    """

    return Worker(
        queue,
        functions=tasks,
        cron_jobs=scheduled_tasks,
        startup=startup,
        shutdown=shutdown,
        before_process=before_process,
        after_process=after_process,
        concurrency=concurrency,
    )
