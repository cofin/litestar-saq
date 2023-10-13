from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from litestar.di import Provide
from litestar.exceptions import ImproperlyConfiguredException
from litestar.plugins import CLIPluginProtocol, InitPluginProtocol
from litestar.static_files import StaticFilesConfig

from litestar_saq.base import Worker
from litestar_saq.controllers import build_controller

if TYPE_CHECKING:
    from click import Group
    from litestar.config.app import AppConfig

    from litestar_saq.config import SAQConfig, TaskQueue, TaskQueues


T = TypeVar("T")


class SAQPlugin(InitPluginProtocol, CLIPluginProtocol):
    """SAQ plugin."""

    __slots__ = ("_config",)

    def __init__(self, config: SAQConfig) -> None:
        """Initialize ``SAQPlugin``.

        Args:
            config: configure and start SAQ.
        """
        self._config = config
        self._worker_instances: list[Worker] | None = None

    def on_cli_init(self, cli: Group) -> None:
        from litestar_saq.cli import build_cli_app

        cli.add_command(build_cli_app())
        return super().on_cli_init(cli)

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        """Configure application for use with SQLAlchemy.

        Args:
            app_config: The :class:`AppConfig <.config.app.AppConfig>` instance.
        """
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
            app_config.route_handlers.append(build_controller(self._config.web_path))
        app_config.on_startup.append(self._config.update_app_state)
        app_config.on_shutdown.append(self._config.on_shutdown)
        app_config.signature_namespace.update(self._config.signature_namespace)
        return app_config

    def get_workers(self) -> list[Worker]:
        """Return workers"""
        if self._worker_instances is not None:
            return self._worker_instances
        queues = self._config.get_queues()
        self._worker_instances = []
        self._worker_instances.extend(
            Worker(
                queue=self._get_queue(queue_config.name, queues.queues),
                functions=queue_config.tasks,
                cron_jobs=queue_config.scheduled_tasks,
                concurrency=queue_config.concurrency,
                startup=queue_config.startup,
                shutdown=queue_config.shutdown,
                before_process=queue_config.before_process,
                after_process=queue_config.after_process,
                timers=queue_config.timers,
                dequeue_timeout=queue_config.dequeue_timeout,
            )
            for queue_config in self._config.queue_configs
        )
        return self._worker_instances

    def get_queues(self) -> TaskQueues:
        return self._config.get_queues()

    @staticmethod
    def _get_queue(name: str, queues: dict[str, TaskQueue]) -> TaskQueue:
        queue = queues.get(name)
        if queue is not None:
            return queue
        msg = "Could not find the specified queue.  Please check your configuration."
        raise ImproperlyConfiguredException(msg)
