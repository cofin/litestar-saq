from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from click import Group
    from litestar import Litestar
    from litestar.logging.config import BaseLoggingConfig

    from litestar_saq.base import Worker
    from litestar_saq.plugin import SAQPlugin


def build_cli_app() -> "Group":  # noqa: C901
    import asyncio
    import multiprocessing
    import platform
    from typing import cast

    from click import IntRange, group, option
    from litestar.cli._utils import LitestarGroup, console  # pyright: ignore

    @group(cls=LitestarGroup, name="workers", no_args_is_help=True)
    def background_worker_group() -> None:
        """Manage background task workers."""

    @background_worker_group.command(
        name="run",
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
    @option(
        "--queues",
        help="List of queue names to process.",
        type=str,
        multiple=True,
        required=False,
        show_default=False,
    )
    @option("-v", "--verbose", help="Enable verbose logging.", is_flag=True, default=None, type=bool, required=False)
    @option("-d", "--debug", help="Enable debugging.", is_flag=True, default=None, type=bool, required=False)
    def run_worker(  # pyright: ignore[reportUnusedFunction]
        app: "Litestar",
        workers: int,
        queues: "Optional[tuple[str, ...]]",
        verbose: "Optional[bool]",
        debug: "Optional[bool]",
    ) -> None:
        """Run the API server."""
        console.rule("[yellow]Starting SAQ Workers[/]", align="left")
        if platform.system() == "Darwin":
            multiprocessing.set_start_method("fork", force=True)

        if app.logging_config is not None:
            app.logging_config.configure()
        if debug is not None or verbose is not None:
            app.debug = True
        plugin = get_saq_plugin(app)
        if queues:
            queue_list = list(queues)
            limited_start_up(plugin, queue_list)
        show_saq_info(app, workers, plugin)
        managed_workers = list(plugin.get_workers().values())
        processes: list[multiprocessing.Process] = []
        if workers > 1:
            for _ in range(workers - 1):
                for worker in managed_workers:
                    p = multiprocessing.Process(
                        target=run_saq_worker,
                        args=(
                            worker,
                            app.logging_config,
                        ),
                    )
                    p.start()
                    processes.append(p)

        if len(managed_workers) > 1:
            for j in range(len(managed_workers) - 1):
                p = multiprocessing.Process(target=run_saq_worker, args=(managed_workers[j + 1], app.logging_config))
                p.start()
                processes.append(p)

        try:
            run_saq_worker(
                worker=managed_workers[0],
                logging_config=cast("BaseLoggingConfig", app.logging_config),
            )
        except KeyboardInterrupt:
            loop = asyncio.get_event_loop()
            for w in managed_workers:
                loop.run_until_complete(w.stop())
        console.print("[yellow]SAQ workers stopped.[/]")

    @background_worker_group.command(
        name="status",
        help="Check the status of currently configured workers and queues.",
    )
    @option("-v", "--verbose", help="Enable verbose logging.", is_flag=True, default=None, type=bool, required=False)
    @option("-d", "--debug", help="Enable debugging.", is_flag=True, default=None, type=bool, required=False)
    def worker_status(  # pyright: ignore[reportUnusedFunction]
        app: "Litestar",
        verbose: "Optional[bool]",
        debug: "Optional[bool]",
    ) -> None:
        """Check the status of currently configured workers and queues."""
        console.rule("[yellow]Checking SAQ worker status[/]", align="left")
        if app.logging_config is not None:
            app.logging_config.configure()
        if debug is not None or verbose is not None:
            app.debug = True
        plugin = get_saq_plugin(app)
        show_saq_info(app, plugin.config.worker_processes, plugin)

    return background_worker_group


def limited_start_up(plugin: "SAQPlugin", queues: "list[str]") -> None:
    """Reset the workers and include only the specified queues."""
    plugin.remove_workers()
    plugin.config.filter_delete_queues(queues)


def get_saq_plugin(app: "Litestar") -> "SAQPlugin":
    """Retrieve a SAQ plugin from the Litestar application's plugins.

    This function attempts to find a SAQ plugin instance.
    If plugin is not found, it raises an ImproperlyConfiguredException.

    Args:
        app: The Litestar application instance.

    Returns:
        The SAQ plugin instance.

    Raises:
        ImproperConfigurationError: If the SAQ plugin is not found.
    """
    from contextlib import suppress

    from litestar_saq.exceptions import ImproperConfigurationError
    from litestar_saq.plugin import SAQPlugin

    with suppress(KeyError):
        return app.plugins.get(SAQPlugin)
    msg = "Failed to initialize SAQ. The required plugin (SAQPlugin) is missing."
    raise ImproperConfigurationError(
        msg,
    )


def show_saq_info(app: "Litestar", workers: int, plugin: "SAQPlugin") -> None:  # pragma: no cover
    """Display basic information about the application and its configuration."""

    from litestar.cli._utils import _format_is_enabled, console  # pyright: ignore
    from rich.table import Table
    from saq import __version__ as saq_version

    table = Table(show_header=False)
    table.add_column("title", style="cyan")
    table.add_column("value", style="bright_blue")

    table.add_row("SAQ version", saq_version)
    table.add_row("Debug mode", _format_is_enabled(app.debug))
    table.add_row("Number of Processes", str(workers))
    table.add_row("Queues", str(len(plugin.config.queue_configs)))

    console.print(table)


def run_saq_worker(worker: "Worker", logging_config: "Optional[BaseLoggingConfig]") -> None:
    """Run a worker."""
    import asyncio

    loop = asyncio.get_event_loop()
    if logging_config is not None:
        logging_config.configure()

    async def worker_start(w: "Worker") -> None:
        try:
            await w.queue.connect()
            await w.start()
        finally:
            await w.queue.disconnect()

    try:
        if worker.separate_process:
            loop.run_until_complete(loop.create_task(worker_start(worker)))
    except KeyboardInterrupt:
        loop.run_until_complete(loop.create_task(worker.stop()))
