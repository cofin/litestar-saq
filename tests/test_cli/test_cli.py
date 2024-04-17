import pytest
from click.testing import CliRunner
from litestar.cli._utils import LitestarGroup
from redis.asyncio import Redis

from tests.test_cli import APP_DEFAULT_CONFIG_FILE_CONTENT
from tests.test_cli.conftest import CreateAppFileFixture

pytestmark = pytest.mark.anyio


async def test_basic_command(
    runner: CliRunner,
    create_app_file: CreateAppFileFixture,
    root_command: LitestarGroup,
    redis_service: None,
    redis: Redis,
) -> None:
    app_file = create_app_file("command_test_app.py", content=APP_DEFAULT_CONFIG_FILE_CONTENT)
    result = runner.invoke(root_command, ["--app", f"{app_file.stem}:app", "workers"])

    assert not result.exception
    assert "Manage background task workers." in result.output
