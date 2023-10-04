from __future__ import annotations

import asyncio
import multiprocessing
from contextlib import suppress
from typing import TYPE_CHECKING

from click import IntRange, group, option
from litestar.cli._utils import LitestarGroup, console

from litestar_saq.exceptions import ImproperConfigurationError
from litestar_saq.plugin import SAQPlugin

if TYPE_CHECKING:
    from litestar import Litestar

    from litestar_saq.base import Worker


@group(cls=LitestarGroup, name="tasks")
def background_worker_group() -> None:
    """Manage background task workers."""


@background_worker_group.command(
    name="run-worker",
    help="Run background worker processes.",
)
@option(
    "--workers",
    help="The number of worker processes to start.",
    type=IntRange(min=1),
    default=1,
    required=False,
    show_default=True,
)
@option("-v", "--verbose", help="Enable verbose logging.", is_flag=True, default=None, type=bool, required=False)
@option("-d", "--debug", help="Enable debugging.", is_flag=True, default=None, type=bool, required=False)
def run_worker(
    app: Litestar,
    workers: int,
    verbose: bool | None,  # noqa: ARG001
    debug: bool | None,  # noqa: ARG001
) -> None:
    """Run the API server."""
    console.rule("[yellow]Starting SAQ Workers[/]", align="left")

    plugin = get_saq_plugin(app)
    if workers > 1:
        for _ in range(workers - 1):
            p = multiprocessing.Process(target=run_worker_process, args=(plugin.get_workers(),))
            p.start()

    with suppress(KeyboardInterrupt):
        run_worker_process(workers=plugin.get_workers())


def get_saq_plugin(app: Litestar) -> SAQPlugin:
    """Retrieve a SAQ plugin from the Litestar application's plugins.

    This function attempts to find a SAQ plugin instance.
    If plugin is not found, it raises an ImproperlyConfiguredException.
    """

    with suppress(KeyError):
        return app.plugins.get(SAQPlugin)
    msg = "Failed to initialize SAQ. The required plugin (SAQPlugin) is missing."
    raise ImproperConfigurationError(
        msg,
    )


def run_worker_process(workers: list[Worker]) -> None:
    """Run a worker."""
    loop = asyncio.get_event_loop()

    try:
        for i, worker_instance in enumerate(workers):
            if i < len(workers) - 1:
                loop.create_task(worker_instance.start())
            else:
                loop.run_until_complete(worker_instance.start())
    except KeyboardInterrupt:
        for worker in workers:
            loop.run_until_complete(worker.stop())
