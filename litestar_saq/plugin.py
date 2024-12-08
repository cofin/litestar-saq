from __future__ import annotations

import signal
import sys
import time
from contextlib import contextmanager
from importlib.util import find_spec
from multiprocessing import Process
from typing import TYPE_CHECKING, Any, Collection, Iterator, TypeVar, cast

from litestar.plugins import CLIPlugin, InitPluginProtocol
from saq.types import ReceivesContext

from litestar_saq.base import Worker

if TYPE_CHECKING:
    from click import Group
    from litestar import Litestar
    from litestar.config.app import AppConfig
    from saq.types import Function

    from litestar_saq.base import Queue
    from litestar_saq.config import SAQConfig, TaskQueues

T = TypeVar("T")

STRUCTLOG_INSTALLED = find_spec("structlog") is not None


class SAQPlugin(InitPluginProtocol, CLIPlugin):
    """SAQ plugin."""

    __slots__ = ("_config", "_worker_instances")

    WORKER_SHUTDOWN_TIMEOUT = 5.0  # seconds
    WORKER_JOIN_TIMEOUT = 1.0  # seconds

    def __init__(self, config: SAQConfig) -> None:
        """Initialize ``SAQPlugin``.

        Args:
            config: configure and start SAQ.
        """
        self._config = config
        self._worker_instances: list[Worker] | None = None

    @property
    def config(self) -> SAQConfig:
        return self._config

    def on_cli_init(self, cli: Group) -> None:
        from litestar_saq.cli import build_cli_app

        cli.add_command(build_cli_app())
        return super().on_cli_init(cli)  # type: ignore[safe-super]

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        """Configure application for use with SQLAlchemy.

        Args:
            app_config: The :class:`AppConfig <.config.app.AppConfig>` instance.
        """

        from litestar.di import Provide
        from litestar.static_files import create_static_files_router

        from litestar_saq.controllers import build_controller

        app_config.dependencies.update(
            {
                self._config.queues_dependency_key: Provide(
                    dependency=self._config.provide_queues,
                    sync_to_thread=False,
                ),
            },
        )
        if self._config.web_enabled:
            app_config.route_handlers.append(
                create_static_files_router(
                    directories=[self._config.static_files],
                    path=f"{self._config.web_path}/static",
                    name="saq",
                    html_mode=False,
                    opt={"exclude_from_auth": True},
                    include_in_schema=False,
                ),
            )
            app_config.route_handlers.append(
                build_controller(self._config.web_path, self._config.web_guards, self._config.web_include_in_schema),  # type: ignore[arg-type]
            )
        app_config.on_startup.append(self._config.update_app_state)
        app_config.signature_namespace.update(self._config.signature_namespace)
        workers = self.get_workers()
        for worker in workers:
            if not worker.separate_process:
                app_config.on_startup.append(worker.on_app_startup)
                app_config.on_shutdown.append(worker.on_app_shutdown)
        return app_config

    def get_workers(self) -> list[Worker]:
        """Return workers"""
        if self._worker_instances is not None:
            return self._worker_instances
        self._worker_instances = []
        self._worker_instances.extend(
            Worker(
                queue=self.get_queue(queue_config.name),
                functions=cast("Collection[Function]", queue_config.tasks),
                cron_jobs=queue_config.scheduled_tasks,
                concurrency=queue_config.concurrency,
                startup=cast(Collection[ReceivesContext], queue_config.startup),
                shutdown=cast(Collection[ReceivesContext], queue_config.shutdown),
                before_process=cast(Collection[ReceivesContext], queue_config.before_process),
                after_process=cast(Collection[ReceivesContext], queue_config.after_process),
                timers=queue_config.timers,
                dequeue_timeout=queue_config.dequeue_timeout,
                separate_process=queue_config.separate_process,
            )
            for queue_config in self._config.queue_configs
        )
        return self._worker_instances

    def remove_workers(self) -> None:
        self._worker_instances = None

    def get_queues(self) -> TaskQueues:
        return self._config.get_queues()

    def get_queue(self, name: str) -> Queue:
        return self.get_queues().get(name)

    @contextmanager
    def server_lifespan(self, app: Litestar) -> Iterator[None]:
        import multiprocessing
        import platform

        from litestar.cli._utils import console

        from litestar_saq.cli import run_saq_worker

        if platform.system() == "Darwin":
            multiprocessing.set_start_method("fork", force=True)

        if not self._config.use_server_lifespan:
            yield
            return

        console.rule("[yellow]Starting SAQ Workers[/]", align="left")
        processes: list[Process] = []

        def handle_shutdown(_signum: Any, _frame: Any) -> None:
            """Handle shutdown signals gracefully."""
            console.print("[yellow]Received shutdown signal, stopping workers...[/]")
            self._terminate_workers(processes)
            sys.exit(0)

        # Register signal handlers
        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)

        try:
            processes.extend(
                Process(target=run_saq_worker, args=(self.get_workers(), app.logging_config), name=f"saq-worker-{i}")
                for i in range(self._config.worker_processes)
            )

            for p in processes:
                p.start()

            yield

        except Exception as e:
            console.print(f"[red]Error in worker processes: {e}[/]")
            raise
        finally:
            console.print("[yellow]Shutting down SAQ workers...[/]")
            self._terminate_workers(processes)
            console.print("[yellow]SAQ workers stopped.[/]")

    def _terminate_workers(self, processes: list[Process], timeout: float = 5.0) -> None:
        """Gracefully terminate worker processes with timeout.

        Args:
            processes: List of worker processes to terminate
            timeout: Maximum time to wait for graceful shutdown in seconds
        """
        # Send SIGTERM to all processes
        from litestar.cli._utils import console

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
                except Exception as e:  # noqa: BLE001
                    console.print(f"[red]Error killing worker process: {e}[/]")
