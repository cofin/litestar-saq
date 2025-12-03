import signal
import sys
import time
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    import multiprocessing

    from click import Group
    from litestar import Litestar
    from litestar.logging.config import BaseLoggingConfig

    from litestar_saq.base import Worker
    from litestar_saq.plugin import SAQPlugin


def _terminate_worker_processes(
    processes: "list[multiprocessing.Process]",
    timeout: float = 5.0,
) -> None:
    """Gracefully terminate worker processes with timeout.

    Args:
        processes: List of worker processes to terminate
        timeout: Maximum time to wait for graceful shutdown in seconds
    """
    from litestar.cli._utils import console  # pyright: ignore

    # Send SIGTERM to all processes
    for p in processes:
        if p.is_alive():
            p.terminate()

    # Wait for processes to terminate gracefully
    termination_start = time.time()
    while time.time() - termination_start < timeout:
        if not any(p.is_alive() for p in processes):
            break
        time.sleep(0.1)

    # Force kill any remaining processes
    for p in processes:
        if p.is_alive():
            try:
                p.kill()  # Send SIGKILL
                p.join(timeout=1.0)
            except Exception:  # noqa: BLE001
                console.print(f"[red]Error killing worker process: {p.name}[/]")


def build_cli_app() -> "Group":  # noqa: C901, PLR0915
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

        def handle_shutdown_signal(signum: int, _frame: Any) -> None:
            """Handle shutdown signals (SIGTERM/SIGINT) for graceful shutdown."""
            sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
            console.print(f"[yellow]Received {sig_name}, stopping workers...[/]")
            _terminate_worker_processes(processes)
            loop = asyncio.get_event_loop()
            for w in managed_workers:
                loop.run_until_complete(w.stop())
            console.print("[yellow]SAQ workers stopped.[/]")
            sys.exit(0)

        signal.signal(signal.SIGTERM, handle_shutdown_signal)
        signal.signal(signal.SIGINT, handle_shutdown_signal)

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

        run_saq_worker(
            worker=managed_workers[0],
            logging_config=cast("BaseLoggingConfig", app.logging_config),
        )

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
    """Run a worker.

    Args:
        worker: The worker instance to run.
        logging_config: Optional logging configuration to apply.
    """
    import asyncio

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if logging_config is not None:
        logging_config.configure()

    # Configure structlog context for separate process workers
    # (In-process workers configure in on_app_startup)
    if worker.separate_process:
        worker.configure_structlog_context()

    def handle_sigterm(_signum: int, _frame: Any) -> None:
        """Handle SIGTERM in worker process."""
        loop.run_until_complete(loop.create_task(worker.stop()))
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

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
