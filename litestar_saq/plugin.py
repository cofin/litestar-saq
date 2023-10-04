from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from litestar.di import Provide
from litestar.plugins import CLIPluginProtocol, InitPluginProtocol

from litestar_saq.config import SAQConfig

__all__ = ["SAQConfig", "SAQPlugin"]


if TYPE_CHECKING:
    from click import Group
    from litestar.config.app import AppConfig

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

    def on_cli_init(self, cli: Group) -> None:
        from litestar_saq.cli import background_worker_group

        cli.add_command(background_worker_group)
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
        app_config.on_shutdown.append(self._config.on_shutdown)
        app_config.signature_namespace.update(self._config.signature_namespace)
        return app_config
