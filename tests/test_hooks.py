"""Tests for default lifecycle hooks."""

import logging
from typing import cast
from unittest.mock import Mock

import pytest
from saq.types import Context

from litestar_saq.hooks import (
    after_process_logger,
    before_process_logger,
    shutdown_logger,
    startup_logger,
    timing_after_process,
    timing_before_process,
)

pytestmark = pytest.mark.anyio


# =============================================================================
# Startup Logger Tests
# =============================================================================


async def test_startup_logger_logs_worker_info(caplog: pytest.LogCaptureFixture) -> None:
    """Test startup_logger logs worker ID and queue name."""
    queue_mock = Mock()
    queue_mock.name = "test-queue"
    ctx = cast(
        Context,
        {
            "worker_id": "test-worker-1",
            "queue": queue_mock,
        },
    )

    with caplog.at_level(logging.INFO, logger="litestar_saq.hooks"):
        await startup_logger(ctx)

    assert "SAQ worker starting" in caplog.text
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.worker_id == "test-worker-1"
    assert record.queue_name == "test-queue"
    assert record.event == "worker_startup"


async def test_startup_logger_handles_missing_keys(caplog: pytest.LogCaptureFixture) -> None:
    """Test startup_logger works with empty context."""
    ctx = cast(Context, {})

    with caplog.at_level(logging.INFO, logger="litestar_saq.hooks"):
        await startup_logger(ctx)

    assert "SAQ worker starting" in caplog.text
    record = caplog.records[0]
    assert record.worker_id == "unknown"
    assert record.queue_name == "unknown"


async def test_startup_logger_handles_queue_without_name(caplog: pytest.LogCaptureFixture) -> None:
    """Test startup_logger handles queue object without name attribute."""
    ctx = cast(
        Context,
        {
            "worker_id": "worker-123",
            "queue": object(),  # No 'name' attribute
        },
    )

    with caplog.at_level(logging.INFO, logger="litestar_saq.hooks"):
        await startup_logger(ctx)

    assert "SAQ worker starting" in caplog.text
    record = caplog.records[0]
    assert record.queue_name == "unknown"


# =============================================================================
# Shutdown Logger Tests
# =============================================================================


async def test_shutdown_logger_logs_worker_info(caplog: pytest.LogCaptureFixture) -> None:
    """Test shutdown_logger logs worker ID and queue name."""
    queue_mock = Mock()
    queue_mock.name = "default"
    ctx = cast(
        Context,
        {
            "worker_id": "worker-456",
            "queue": queue_mock,
        },
    )

    with caplog.at_level(logging.INFO, logger="litestar_saq.hooks"):
        await shutdown_logger(ctx)

    assert "SAQ worker shutting down" in caplog.text
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.worker_id == "worker-456"
    assert record.queue_name == "default"
    assert record.event == "worker_shutdown"


async def test_shutdown_logger_handles_missing_keys(caplog: pytest.LogCaptureFixture) -> None:
    """Test shutdown_logger works with empty context."""
    ctx = cast(Context, {})

    with caplog.at_level(logging.INFO, logger="litestar_saq.hooks"):
        await shutdown_logger(ctx)

    assert "SAQ worker shutting down" in caplog.text
    record = caplog.records[0]
    assert record.worker_id == "unknown"
    assert record.queue_name == "unknown"


# =============================================================================
# Before Process Logger Tests
# =============================================================================


async def test_before_process_logger_logs_job_info(caplog: pytest.LogCaptureFixture) -> None:
    """Test before_process_logger logs job ID and function."""
    job_mock = Mock()
    job_mock.id = "job-123"
    job_mock.function = "my_task"
    ctx = cast(Context, {"job": job_mock})

    with caplog.at_level(logging.DEBUG, logger="litestar_saq.hooks"):
        await before_process_logger(ctx)

    assert "Processing job" in caplog.text
    record = caplog.records[0]
    assert record.job_id == "job-123"
    assert record.function == "my_task"
    assert record.event == "job_start"


async def test_before_process_logger_handles_missing_job(caplog: pytest.LogCaptureFixture) -> None:
    """Test before_process_logger works without job in context."""
    ctx = cast(Context, {})

    with caplog.at_level(logging.DEBUG, logger="litestar_saq.hooks"):
        await before_process_logger(ctx)

    assert "Processing job" in caplog.text
    record = caplog.records[0]
    assert record.job_id == "unknown"
    assert record.function == "unknown"


# =============================================================================
# After Process Logger Tests
# =============================================================================


async def test_after_process_logger_logs_job_info(caplog: pytest.LogCaptureFixture) -> None:
    """Test after_process_logger logs job ID, function, and status."""
    job_mock = Mock()
    job_mock.id = "job-456"
    job_mock.function = "another_task"
    job_mock.status = "complete"
    ctx = cast(Context, {"job": job_mock})

    with caplog.at_level(logging.DEBUG, logger="litestar_saq.hooks"):
        await after_process_logger(ctx)

    assert "Job completed" in caplog.text
    record = caplog.records[0]
    assert record.job_id == "job-456"
    assert record.function == "another_task"
    assert record.status == "complete"
    assert record.event == "job_complete"


async def test_after_process_logger_handles_missing_job(caplog: pytest.LogCaptureFixture) -> None:
    """Test after_process_logger works without job in context."""
    ctx = cast(Context, {})

    with caplog.at_level(logging.DEBUG, logger="litestar_saq.hooks"):
        await after_process_logger(ctx)

    assert "Job completed" in caplog.text
    record = caplog.records[0]
    assert record.job_id == "unknown"
    assert record.function == "unknown"
    assert record.status == "unknown"


# =============================================================================
# Timing Hooks Tests
# =============================================================================


async def test_timing_before_process_sets_start_time() -> None:
    """Test timing_before_process records start time in context."""
    ctx = cast(Context, {})

    await timing_before_process(ctx)

    assert "_timing_start" in ctx
    assert isinstance(ctx["_timing_start"], float)  # type: ignore[typeddict-item]
    assert ctx["_timing_start"] > 0  # type: ignore[typeddict-item]


async def test_timing_hooks_calculate_duration(caplog: pytest.LogCaptureFixture) -> None:
    """Test timing hooks correctly calculate job duration."""
    import asyncio

    job_mock = Mock()
    job_mock.id = "job-789"
    job_mock.function = "slow_task"
    ctx = cast(Context, {"job": job_mock})

    await timing_before_process(ctx)
    await asyncio.sleep(0.05)  # 50ms

    with caplog.at_level(logging.INFO, logger="litestar_saq.hooks"):
        await timing_after_process(ctx)

    assert "Job completed in" in caplog.text
    assert "ms" in caplog.text
    record = caplog.records[0]
    assert record.job_id == "job-789"
    assert record.function == "slow_task"
    assert record.duration_ms >= 50  # At least 50ms
    assert record.event == "job_timing"


async def test_timing_after_without_before_warns(caplog: pytest.LogCaptureFixture) -> None:
    """Test timing_after_process warns if timing_before_process not called."""
    ctx = cast(Context, {"job": Mock(id="job-000")})

    with caplog.at_level(logging.WARNING, logger="litestar_saq.hooks"):
        await timing_after_process(ctx)

    assert "timing_after_process called without timing_before_process" in caplog.text


async def test_timing_after_handles_missing_job(caplog: pytest.LogCaptureFixture) -> None:
    """Test timing_after_process works without job in context."""
    ctx = cast(Context, {})

    await timing_before_process(ctx)

    with caplog.at_level(logging.INFO, logger="litestar_saq.hooks"):
        await timing_after_process(ctx)

    record = caplog.records[0]
    assert record.job_id == "unknown"
    assert record.function == "unknown"


# =============================================================================
# Integration Tests
# =============================================================================


async def test_hooks_compose_with_custom_hooks(caplog: pytest.LogCaptureFixture) -> None:
    """Test default hooks can be composed with custom hooks."""
    custom_startup_called = False

    async def custom_startup(ctx: Context) -> None:
        nonlocal custom_startup_called
        custom_startup_called = True
        ctx["custom_data"] = "initialized"  # type: ignore[typeddict-unknown-key]

    queue_mock = Mock()
    queue_mock.name = "test"
    ctx = cast(Context, {"worker_id": "w1", "queue": queue_mock})

    # Simulate hook composition
    with caplog.at_level(logging.INFO, logger="litestar_saq.hooks"):
        await startup_logger(ctx)
        await custom_startup(ctx)

    assert "SAQ worker starting" in caplog.text
    assert custom_startup_called
    assert ctx["custom_data"] == "initialized"  # type: ignore[typeddict-item]


async def test_hooks_share_context(caplog: pytest.LogCaptureFixture) -> None:
    """Test hooks can share context data."""
    job_mock = Mock()
    job_mock.id = "job-shared"
    job_mock.function = "task"
    ctx = cast(Context, {"job": job_mock})

    # timing_before_process sets context data
    await timing_before_process(ctx)

    # timing_after_process reads that data
    with caplog.at_level(logging.INFO, logger="litestar_saq.hooks"):
        await timing_after_process(ctx)

    assert "_timing_start" in ctx
    assert "Job completed in" in caplog.text


def test_hooks_importable_from_top_level() -> None:
    """Test hooks can be imported from litestar_saq package."""
    from litestar_saq import (
        after_process_logger,
        before_process_logger,
        shutdown_logger,
        startup_logger,
        timing_after_process,
        timing_before_process,
    )

    assert callable(startup_logger)
    assert callable(shutdown_logger)
    assert callable(before_process_logger)
    assert callable(after_process_logger)
    assert callable(timing_before_process)
    assert callable(timing_after_process)


def test_hooks_importable_from_hooks_module() -> None:
    """Test hooks can be imported from litestar_saq.hooks."""
    from litestar_saq.hooks import (
        after_process_logger,
        before_process_logger,
        shutdown_logger,
        startup_logger,
        timing_after_process,
        timing_before_process,
    )

    assert callable(startup_logger)
    assert callable(shutdown_logger)
    assert callable(before_process_logger)
    assert callable(after_process_logger)
    assert callable(timing_before_process)
    assert callable(timing_after_process)


def test_hooks_all_exported() -> None:
    """Test all hooks are in __all__."""
    from litestar_saq import hooks

    expected = {
        "startup_logger",
        "shutdown_logger",
        "before_process_logger",
        "after_process_logger",
        "timing_before_process",
        "timing_after_process",
    }
    assert set(hooks.__all__) == expected


async def test_hooks_with_queue_config_startup() -> None:
    """Test hooks can be configured via QueueConfig startup parameter."""
    from unittest.mock import MagicMock

    from litestar_saq import QueueConfig

    # Create a queue config with startup hooks
    broker = MagicMock()
    config = QueueConfig(
        broker_instance=broker,
        name="test-queue",
        startup=[startup_logger, before_process_logger],
    )

    # Verify hooks are properly set
    assert config.startup is not None
    assert isinstance(config.startup, list)
    assert len(config.startup) == 2
    assert startup_logger in config.startup
    assert before_process_logger in config.startup


async def test_hooks_with_queue_config_shutdown() -> None:
    """Test hooks can be configured via QueueConfig shutdown parameter."""
    from unittest.mock import MagicMock

    from litestar_saq import QueueConfig

    broker = MagicMock()
    config = QueueConfig(
        broker_instance=broker,
        name="test-queue",
        shutdown=[shutdown_logger],
    )

    assert config.shutdown is not None
    assert isinstance(config.shutdown, list)
    assert len(config.shutdown) == 1
    assert shutdown_logger in config.shutdown


async def test_hooks_with_queue_config_process_hooks() -> None:
    """Test hooks can be configured via QueueConfig before/after_process parameters."""
    from unittest.mock import MagicMock

    from litestar_saq import QueueConfig

    broker = MagicMock()
    config = QueueConfig(
        broker_instance=broker,
        name="test-queue",
        before_process=[timing_before_process, before_process_logger],
        after_process=[timing_after_process, after_process_logger],
    )

    assert config.before_process is not None
    assert isinstance(config.before_process, list)
    assert len(config.before_process) == 2
    assert timing_before_process in config.before_process
    assert before_process_logger in config.before_process

    assert config.after_process is not None
    assert isinstance(config.after_process, list)
    assert len(config.after_process) == 2
    assert timing_after_process in config.after_process
    assert after_process_logger in config.after_process


async def test_hooks_with_queue_config_single_hook() -> None:
    """Test single hook (not in list) is properly normalized by QueueConfig."""
    from unittest.mock import MagicMock

    from litestar_saq import QueueConfig

    broker = MagicMock()
    config = QueueConfig(
        broker_instance=broker,
        name="test-queue",
        startup=startup_logger,  # Single hook, not a list
        shutdown=shutdown_logger,  # Single hook, not a list
    )

    # QueueConfig should normalize single hooks to lists
    assert config.startup is not None
    assert isinstance(config.startup, list)
    assert len(config.startup) == 1
    assert startup_logger in config.startup

    assert config.shutdown is not None
    assert isinstance(config.shutdown, list)
    assert len(config.shutdown) == 1
    assert shutdown_logger in config.shutdown


async def test_hooks_with_queue_config_mixed_hooks() -> None:
    """Test mixing default hooks with custom hooks via QueueConfig."""
    from unittest.mock import MagicMock

    from litestar_saq import QueueConfig

    custom_called = False

    async def custom_hook(_ctx: Context) -> None:
        nonlocal custom_called
        custom_called = True

    broker = MagicMock()
    config = QueueConfig(
        broker_instance=broker,
        name="test-queue",
        startup=[startup_logger, custom_hook],
        before_process=[timing_before_process, before_process_logger],
        after_process=[after_process_logger, timing_after_process],
    )

    # Verify all hooks are present
    assert config.startup is not None
    assert isinstance(config.startup, list)
    assert len(config.startup) == 2
    assert startup_logger in config.startup
    assert custom_hook in config.startup

    # Verify custom hook is callable
    assert callable(custom_hook)
    await custom_hook(cast(Context, {}))
    assert custom_called
