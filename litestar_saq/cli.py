from __future__ import annotations

from typing import TYPE_CHECKING

from click import group, option
from litestar.cli._utils import LitestarGroup, console

if TYPE_CHECKING:
    from litestar import Litestar


@group(cls=LitestarGroup, name="database")
def background_worker_group() -> None:
    """Manage background task workers."""


@background_worker_group.command(
    name="show-current-revision",
    help="Shows the current revision for the database.",
)
@option("--verbose", type=bool, help="Enable verbose output.", default=False, is_flag=True)
def show_database_revision(app: Litestar, verbose: bool) -> None:  # noqa: ARG001
    """Show current database revision."""
    console.rule("[yellow]Starting SAQ Workers[/]", align="left")
