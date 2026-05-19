import contextlib
import signal
import sys
import time
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    import asyncio
    import multiprocessing
    from collections.abc import Collection

    from click import Group
    from litestar import Litestar
    from litestar.logging.config import BaseLoggingConfig

    from litestar_saq.base import Worker
    from litestar_saq.config import SAQConfig
    from litestar_saq.plugin import SAQPlugin

# Default timeout for graceful shutdown when no grace period is configured
DEFAULT_SHUTDOWN_TIMEOUT = 5.0
# Extra buffer time to allow for signal propagation and cleanup
SHUTDOWN_BUFFER = 2.0


def get_max_shutdown_timeout(workers: "Collection[Worker]") -> float:
    """Calculate the maximum shutdown timeout from worker configurations.

    The timeout is the maximum of all workers' shutdown_grace_period_s + cancellation_hard_deadline_s plus
    a buffer for signal propagation. Falls back to DEFAULT_SHUTDOWN_TIMEOUT
    if no grace periods are configured.

    Args:
        workers: Collection of worker instances.

    Returns:
        Maximum shutdown timeout in seconds.
    """
    grace_periods: list[float] = []
    for worker in workers:
        shutdown_grace_period = getattr(worker, "_shutdown_grace_period_s", 0) or 0
        cancellation_hard_deadline = getattr(worker, "_cancellation_hard_deadline_s", 0)
        grace_period = shutdown_grace_period + cancellation_hard_deadline
        grace_periods.append(grace_period)
    if grace_periods:
        # This ensures that the overall shutdown timeout is always at least 5s
        return max(max(grace_periods) + SHUTDOWN_BUFFER, DEFAULT_SHUTDOWN_TIMEOUT)
    return DEFAULT_SHUTDOWN_TIMEOUT


def _terminate_worker_processes(
    processes: "list[multiprocessing.Process]",
    timeout: float = DEFAULT_SHUTDOWN_TIMEOUT,
) -> None:
    """Gracefully terminate worker processes with timeout.

    Args:
        processes: List of worker processes to terminate
        timeout: Maximum time to wait for graceful shutdown in seconds.
            Should be at least as long as the worker's shutdown_grace_period_s
            plus buffer time for signal propagation.
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


def _get_event_loop() -> "asyncio.AbstractEventLoop":
    import asyncio

    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def _register_shutdown_handlers(
    loop: "asyncio.AbstractEventLoop",
    shutdown_event: "asyncio.Event",
) -> Callable[[], None]:
    signal_handlers_registered = False
    fallback_handlers_registered = False
    original_sigterm: Any = None
    original_sigint: Any = None

    def request_shutdown() -> None:
        shutdown_event.set()

    try:
        loop.add_signal_handler(signal.SIGTERM, request_shutdown)
        loop.add_signal_handler(signal.SIGINT, request_shutdown)
        signal_handlers_registered = True
    except (NotImplementedError, RuntimeError):
        original_sigterm = signal.getsignal(signal.SIGTERM)
        original_sigint = signal.getsignal(signal.SIGINT)

        def fallback_handler(_signum: int, _frame: Any) -> None:
            loop.call_soon_threadsafe(shutdown_event.set)

        try:
            signal.signal(signal.SIGTERM, fallback_handler)
            signal.signal(signal.SIGINT, fallback_handler)
            fallback_handlers_registered = True
        except ValueError:
            fallback_handlers_registered = False

    def cleanup() -> None:
        if signal_handlers_registered:
            loop.remove_signal_handler(signal.SIGTERM)
            loop.remove_signal_handler(signal.SIGINT)
            return
        if not fallback_handlers_registered:
            return
        if original_sigterm is not None:
            with contextlib.suppress(ValueError):
                signal.signal(signal.SIGTERM, original_sigterm)
        if original_sigint is not None:
            with contextlib.suppress(ValueError):
                signal.signal(signal.SIGINT, original_sigint)

    return cleanup


async def _run_worker_with_shutdown(worker: "Worker") -> None:
    import asyncio

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()
    cleanup_handlers = _register_shutdown_handlers(loop, shutdown_event)

    try:
        await worker.queue.connect()
        worker_task = asyncio.create_task(worker.start())
        shutdown_task = asyncio.create_task(shutdown_event.wait())

        done, pending = await asyncio.wait(
            [worker_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if worker_task in done:
            worker_task.result()

        if shutdown_event.is_set():
            await worker.stop()
            if not worker_task.done():
                worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            pending.discard(worker_task)

        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    finally:
        await worker.queue.disconnect()
        cleanup_handlers()


def _prepare_config_for_spawn(config: "SAQConfig") -> "SAQConfig":
    """Return a deep-copied ``SAQConfig`` safe to pickle into a child process.

    Each :class:`QueueConfig` in the copy has its live ``broker_instance``
    (and cached broker-type/queue-class state) nulled out so the child can
    rebuild fresh broker clients lazily from ``dsn``. This is required for
    Python 3.14+ where ``multiprocessing`` defaults to ``forkserver`` /
    ``spawn`` start methods, which pickle the target and its args (the live
    ``redis.asyncio.Redis`` client is not picklable due to lambda-based
    response callbacks).

    Args:
        config: The application's :class:`SAQConfig`.

    Returns:
        A deep-copied, pickle-safe :class:`SAQConfig`.

    Raises:
        ImproperConfigurationError: If any :class:`QueueConfig` was constructed
            with only ``broker_instance`` and no ``dsn``. Such queues cannot
            be rebuilt inside the child process.
    """
    import copy

    from litestar_saq.exceptions import ImproperConfigurationError

    for qc in config.queue_configs:
        if qc.broker_instance is not None and not qc.dsn:
            msg = (
                f"QueueConfig(name={qc.name!r}) was constructed with "
                "`broker_instance` and no `dsn`. Multi-process worker spawning "
                "requires a `dsn` so the broker can be rebuilt inside the "
                "child process. Provide a `dsn` or run with "
                "`separate_process=False`."
            )
            raise ImproperConfigurationError(msg)

    prepared = copy.deepcopy(config)
    # Null out the live queue instances on the config itself — these hold live
    # broker clients that are not picklable under forkserver/spawn.
    prepared.queue_instances = None
    for qc in prepared.queue_configs:
        qc.broker_instance = None
        qc._broker_type = None  # noqa: SLF001
        qc._queue_class = None  # noqa: SLF001
    return prepared


def _run_worker_in_child(
    queue_name: str,
    config: "SAQConfig",
    logging_config: "Optional[BaseLoggingConfig]",
) -> None:
    """Reconstruct the ``Worker`` inside the child process and run it.

    This is the multiprocessing ``target`` for spawned workers. It must be
    a top-level function so ``forkserver`` / ``spawn`` can pickle it. The
    ``config`` argument must have been passed through
    :func:`_prepare_config_for_spawn` first.

    Args:
        queue_name: Name of the queue this child should run.
        config: Pickle-safe :class:`SAQConfig` (no live ``broker_instance``).
        logging_config: Optional logging configuration to apply in the child.
    """
    from litestar_saq.plugin import SAQPlugin

    plugin = SAQPlugin(config=config)
    worker = plugin.get_workers()[queue_name]
    run_saq_worker(worker, logging_config)


def build_cli_app() -> "Group":  # noqa: C901, PLR0915
    import asyncio
    import multiprocessing
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
        managed_queue_names = list(plugin.get_workers().keys())
        processes: list[multiprocessing.Process] = []
        shutdown_timeout = get_max_shutdown_timeout(managed_workers)

        def handle_shutdown_signal(signum: int, _frame: Any) -> None:
            """Handle shutdown signals (SIGTERM/SIGINT) for graceful shutdown."""
            sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
            console.print(f"[yellow]Received {sig_name}, stopping workers (timeout: {shutdown_timeout:.1f}s)...[/]")
            _terminate_worker_processes(processes, timeout=shutdown_timeout)
            loop = asyncio.get_event_loop()
            for w in managed_workers:
                loop.run_until_complete(w.stop())
            console.print("[yellow]SAQ workers stopped.[/]")
            sys.exit(0)

        signal.signal(signal.SIGTERM, handle_shutdown_signal)
        signal.signal(signal.SIGINT, handle_shutdown_signal)

        spawn_config = _prepare_config_for_spawn(plugin.config)

        if workers > 1:
            for _ in range(workers - 1):
                for queue_name in managed_queue_names:
                    p = multiprocessing.Process(
                        target=_run_worker_in_child,
                        args=(queue_name, spawn_config, app.logging_config),
                    )
                    p.start()
                    processes.append(p)

        if len(managed_queue_names) > 1:
            for j in range(len(managed_queue_names) - 1):
                p = multiprocessing.Process(
                    target=_run_worker_in_child,
                    args=(managed_queue_names[j + 1], spawn_config, app.logging_config),
                )
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
    from litestar.logging.config import StructLoggingConfig

    loop = _get_event_loop()

    if logging_config is not None:
        logging_config.configure()

    # Configure structlog context for separate process workers
    # (In-process workers configure in on_app_startup)
    if worker.separate_process:
        worker.configure_structlog_context()
        if isinstance(logging_config, StructLoggingConfig) and logging_config.standard_lib_logging_config is not None:
            logging_config.standard_lib_logging_config.configure()

    try:
        if worker.separate_process:
            loop.run_until_complete(_run_worker_with_shutdown(worker))
    except KeyboardInterrupt:
        loop.run_until_complete(loop.create_task(worker.stop()))
