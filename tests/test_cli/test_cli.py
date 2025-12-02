import signal
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner
from litestar.cli._utils import LitestarGroup
from redis.asyncio import Redis

from litestar_saq.cli import _terminate_worker_processes
from tests.test_cli.conftest import CreateAppFileFixture

if TYPE_CHECKING:
    from pytest_databases.docker.redis import RedisService

pytestmark = pytest.mark.anyio


def get_app_config_content(redis_port: int) -> str:
    """Generate app config content with dynamic Redis port."""
    return f"""
from __future__ import annotations

import asyncio
from logging import getLogger
from typing import TYPE_CHECKING

from examples import tasks
from litestar import Controller, Litestar, get

from litestar_saq import CronJob, QueueConfig, SAQConfig, SAQPlugin

if TYPE_CHECKING:
    from saq.types import Context, QueueInfo

    from litestar_saq.config import TaskQueues

logger = getLogger(__name__)


async def system_upkeep(_: Context) -> None:
    logger.info("Performing system upkeep operations.")
    await asyncio.sleep(1)
    logger.info("System upkeep complete.")


async def background_worker_task(_: Context) -> None:
    logger.info("Performing background worker task.")
    await asyncio.sleep(1)
    logger.info("Background worker task complete.")


async def system_task(_: Context) -> None:
    logger.info("Performing simple system task")
    await asyncio.sleep(1)
    logger.info("System task complete.")


class SampleController(Controller):
    @get(path="/samples")
    async def samples_queue_info(self, task_queues: TaskQueues) -> QueueInfo:
        queue = task_queues.get("samples")
        return await queue.info()


saq = SAQPlugin(
    config=SAQConfig(
        web_enabled=True,
        use_server_lifespan=True,
        queue_configs=[
            QueueConfig(
                dsn="redis://localhost:{redis_port}/0",
                name="samples",
                tasks=[tasks.background_worker_task, tasks.system_task, tasks.system_upkeep],
                scheduled_tasks=[CronJob(function=tasks.system_upkeep, cron="* * * * *", timeout=600, ttl=2000)],
            ),
        ],
    ),
)
app = Litestar(plugins=[saq], route_handlers=[SampleController])
"""


async def test_basic_command(
    runner: CliRunner,
    create_app_file: CreateAppFileFixture,
    root_command: LitestarGroup,
    redis_service: "RedisService",
    redis: Redis,
) -> None:
    app_content = get_app_config_content(redis_service.port)
    app_file = create_app_file("command_test_app.py", content=app_content)
    result = runner.invoke(root_command, ["--app", f"{app_file.stem}:app", "workers", "status"])

    assert result.exit_code == 0
    assert "Checking SAQ worker status" in result.output


def test_terminate_worker_processes_graceful_shutdown() -> None:
    """Test that _terminate_worker_processes terminates processes gracefully."""
    mock_process = Mock()
    # is_alive called: 1) terminate check, 2) while loop check, 3) force kill check
    mock_process.is_alive.side_effect = [True, False, False]
    mock_process.name = "test-worker"

    with patch("litestar_saq.cli.time") as mock_time:
        # Simulate time passing quickly
        mock_time.time.side_effect = [0.0, 0.1]
        mock_time.sleep = Mock()

        _terminate_worker_processes([mock_process], timeout=5.0)

    mock_process.terminate.assert_called_once()
    mock_process.kill.assert_not_called()  # Should not force kill if graceful shutdown works


def test_terminate_worker_processes_force_kill_on_timeout() -> None:
    """Test that _terminate_worker_processes force kills processes that don't terminate."""
    mock_process = Mock()
    mock_process.is_alive.return_value = True  # Always alive - won't terminate gracefully
    mock_process.name = "stuck-worker"

    with patch("litestar_saq.cli.time") as mock_time:
        # Simulate timeout by advancing time past the timeout
        mock_time.time.side_effect = [0.0, 6.0]  # Start at 0, then jump past timeout
        mock_time.sleep = Mock()

        _terminate_worker_processes([mock_process], timeout=5.0)

    mock_process.terminate.assert_called_once()
    mock_process.kill.assert_called_once()  # Should force kill after timeout
    mock_process.join.assert_called_once_with(timeout=1.0)


def test_terminate_worker_processes_handles_kill_exception() -> None:
    """Test that _terminate_worker_processes handles exceptions when killing processes."""
    mock_process = Mock()
    mock_process.is_alive.return_value = True
    mock_process.name = "error-worker"
    mock_process.kill.side_effect = Exception("Kill failed")

    with patch("litestar_saq.cli.time") as mock_time:
        mock_time.time.side_effect = [0.0, 6.0]
        mock_time.sleep = Mock()

        # Should not raise exception
        _terminate_worker_processes([mock_process], timeout=5.0)

    mock_process.terminate.assert_called_once()
    mock_process.kill.assert_called_once()


def test_terminate_worker_processes_skips_dead_processes() -> None:
    """Test that _terminate_worker_processes skips already dead processes."""
    mock_process = Mock()
    mock_process.is_alive.return_value = False  # Already dead
    mock_process.name = "dead-worker"

    _terminate_worker_processes([mock_process], timeout=5.0)

    mock_process.terminate.assert_not_called()
    mock_process.kill.assert_not_called()


def test_terminate_worker_processes_multiple_processes() -> None:
    """Test that _terminate_worker_processes handles multiple processes."""
    mock_process1 = Mock()
    # is_alive called: 1) terminate check, 2) while loop any() check, 3) force kill check
    mock_process1.is_alive.side_effect = [True, False, False]
    mock_process1.name = "worker-1"

    mock_process2 = Mock()
    mock_process2.is_alive.side_effect = [True, False, False]
    mock_process2.name = "worker-2"

    with patch("litestar_saq.cli.time") as mock_time:
        mock_time.time.side_effect = [0.0, 0.1]
        mock_time.sleep = Mock()

        _terminate_worker_processes([mock_process1, mock_process2], timeout=5.0)

    mock_process1.terminate.assert_called_once()
    mock_process2.terminate.assert_called_once()


def test_signal_handlers_registered_in_run_saq_worker() -> None:
    """Test that SIGTERM handler is registered in run_saq_worker."""
    import asyncio

    from litestar_saq.cli import run_saq_worker

    mock_worker = Mock()
    mock_worker.separate_process = False  # Don't actually run worker

    original_handler = signal.getsignal(signal.SIGTERM)

    try:
        # Patch asyncio.get_event_loop at the module level where it's imported
        with patch.object(asyncio, "get_event_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop

            run_saq_worker(mock_worker, None)

            # Verify SIGTERM handler was registered
            current_handler = signal.getsignal(signal.SIGTERM)
            assert current_handler != original_handler
            assert callable(current_handler)
    finally:
        # Restore original handler
        signal.signal(signal.SIGTERM, original_handler)
