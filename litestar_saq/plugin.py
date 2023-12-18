from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Collection, Iterator, TypeVar, cast

from litestar.plugins import CLIPlugin, InitPluginProtocol

from litestar_saq.base import Worker

if TYPE_CHECKING:
    from click import Group
    from litestar import Litestar
    from litestar.config.app import AppConfig
    from saq.types import Function

    from litestar_saq.base import Queue
    from litestar_saq.config import SAQConfig, TaskQueues

T = TypeVar("T")


class SAQPlugin(InitPluginProtocol, CLIPlugin):
    """SAQ plugin."""

    __slots__ = ("_config", "_worker_instances")

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
        return super().on_cli_init(cli)

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        """Configure application for use with SQLAlchemy.

        Args:
            app_config: The :class:`AppConfig <.config.app.AppConfig>` instance.
        """

        from litestar.di import Provide
        from litestar.static_files import StaticFilesConfig

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
            app_config.static_files_config.append(
                StaticFilesConfig(
                    directories=[self._config.static_files],
                    path=f"{self._config.web_path}/static",
                    name="saq",
                    html_mode=False,
                    opt={"exclude_from_auth": True},
                ),
            )
            app_config.route_handlers.append(build_controller(self._config.web_path, self._config.web_guards))  # type: ignore[arg-type]
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
                startup=queue_config.startup,
                shutdown=queue_config.shutdown,
                before_process=queue_config.before_process,
                after_process=queue_config.after_process,
                timers=queue_config.timers,
                dequeue_timeout=queue_config.dequeue_timeout,
                separate_process=queue_config.separate_process,
            )
            for queue_config in self._config.queue_configs
        )
        return self._worker_instances

    def get_queues(self) -> TaskQueues:
        return self._config.get_queues()

    def get_queue(self, name: str) -> Queue:
        return self.get_queues().get(name)

    @contextmanager
    def server_lifespan(self, app: Litestar) -> Iterator[None]:
        import multiprocessing

        from litestar_saq.cli import run_saq_worker

        if self._config.use_server_lifespan:
            processes = [
                multiprocessing.Process(target=run_saq_worker, args=(self.get_workers(), app.logging_config))
                for _ in range(self._config.worker_processes)
            ]

            try:
                for p in processes:
                    p.start()
                yield
            finally:
                for p in processes:
                    if p.is_alive():
                        p.terminate()
                        p.join()
        else:
            yield
