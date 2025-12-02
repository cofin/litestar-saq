import asyncio
from typing import Any, Optional
from unittest.mock import Mock, patch

import pytest

from litestar_saq.base import Worker

pytestmark = pytest.mark.anyio


async def _async_noop() -> None:
    """Async no-op helper for mock methods."""
    pass


@pytest.fixture
def mock_queue() -> Mock:
    queue = Mock(spec=["name", "connect", "disconnect", "sweep", "schedule", "stats", "info"])
    queue.name = "test-queue"
    # Make async methods return fresh coroutines each time
    queue.connect = Mock(side_effect=lambda: _async_noop())
    queue.disconnect = Mock(side_effect=lambda: _async_noop())
    queue.sweep = Mock(side_effect=lambda *_args, **_kwargs: _async_noop())
    queue.schedule = Mock(side_effect=lambda *_args, **_kwargs: _async_noop())
    queue.stats = Mock(side_effect=lambda *_args, **_kwargs: _async_noop())
    queue.info = Mock(side_effect=lambda *_args, **_kwargs: _async_noop())
    return queue


@pytest.fixture
def create_worker(mock_queue: Mock) -> Any:
    def _create_worker(
        *,
        worker_id: "Optional[str]" = "test-worker-123",
        concurrency: int = 10,
        separate_process: bool = False,
        metadata: "Optional[dict[str, Any]]" = None,
    ) -> Worker:
        async def dummy_task(_ctx: "dict[str, Any]") -> None:
            pass

        return Worker(
            queue=mock_queue,
            functions=[("dummy", dummy_task)],  # type: ignore[list-item]
            id=worker_id,
            concurrency=concurrency,
            separate_process=separate_process,
            metadata=metadata,
        )

    return _create_worker


# ============================================================================
# Test get_structlog_context()
# ============================================================================


def test_get_structlog_context_with_all_fields(create_worker: Any) -> None:
    """Test get_structlog_context returns all expected fields."""
    # Arrange
    worker = create_worker(
        worker_id="worker-abc",
        concurrency=5,
        separate_process=True,
        metadata={"env": "production", "region": "us-west"},
    )

    # Act
    context = worker.get_structlog_context()

    # Assert
    assert context["worker_id"] == "worker-abc"
    assert context["queue_name"] == "test-queue"
    assert context["concurrency"] == 5
    assert context["separate_process"] is True
    assert context["worker_meta_env"] == "production"
    assert context["worker_meta_region"] == "us-west"


def test_get_structlog_context_defaults_missing_worker_id(create_worker: Any) -> None:
    """Test get_structlog_context handles None worker_id (SAQ generates UUID)."""
    # Arrange
    worker = create_worker(worker_id=None)

    # Act
    context = worker.get_structlog_context()

    # Assert
    # SAQ's Worker parent class generates a UUID when id=None is passed
    # Our get_structlog_context uses self.id which will be the generated UUID
    # So we just verify it's set and is a string
    assert "worker_id" in context
    assert isinstance(context["worker_id"], str)
    assert context["worker_id"] != ""  # Not empty


def test_get_structlog_context_without_metadata(create_worker: Any) -> None:
    """Test get_structlog_context handles empty metadata correctly."""
    # Arrange
    worker = create_worker(metadata=None)

    # Act
    context = worker.get_structlog_context()

    # Assert
    assert "worker_id" in context
    assert "queue_name" in context
    assert "concurrency" in context
    assert "separate_process" in context
    # No worker_meta_* keys should be present
    assert not any(key.startswith("worker_meta_") for key in context.keys())


def test_get_structlog_context_prefixes_metadata_keys(create_worker: Any) -> None:
    """Test get_structlog_context prefixes custom metadata with worker_meta_."""
    # Arrange
    metadata = {
        "deployment_id": "deploy-123",
        "version": "1.2.3",
        "custom_tag": "value",
    }
    worker = create_worker(metadata=metadata)

    # Act
    context = worker.get_structlog_context()

    # Assert
    assert context["worker_meta_deployment_id"] == "deploy-123"
    assert context["worker_meta_version"] == "1.2.3"
    assert context["worker_meta_custom_tag"] == "value"


# ============================================================================
# Test configure_structlog_context()
# ============================================================================


def test_configure_structlog_context_when_not_installed(
    create_worker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test configure_structlog_context does nothing when structlog is not installed."""
    # Arrange
    monkeypatch.setattr("litestar_saq.typing.STRUCTLOG_INSTALLED", False)
    worker = create_worker()

    # Act - should not raise any errors
    worker.configure_structlog_context()

    # Assert - passes if no exception raised


def test_configure_structlog_context_binds_when_installed(
    create_worker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test configure_structlog_context binds context when structlog is available."""
    # Arrange
    monkeypatch.setattr("litestar_saq.typing.STRUCTLOG_INSTALLED", True)

    mock_structlog = Mock()
    mock_bind = Mock()
    mock_structlog.contextvars.bind_contextvars = mock_bind

    worker = create_worker(
        worker_id="test-worker",
        concurrency=3,
        metadata={"env": "test"},
    )

    # Act
    with patch.dict("sys.modules", {"structlog": mock_structlog}):
        worker.configure_structlog_context()

    # Assert
    mock_bind.assert_called_once()
    call_kwargs = mock_bind.call_args[1]
    assert call_kwargs["worker_id"] == "test-worker"
    assert call_kwargs["queue_name"] == "test-queue"
    assert call_kwargs["concurrency"] == 3
    assert call_kwargs["worker_meta_env"] == "test"


def test_configure_structlog_context_handles_import_error(
    create_worker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test configure_structlog_context handles ImportError gracefully when structlog import fails."""
    # Arrange
    monkeypatch.setattr("litestar_saq.typing.STRUCTLOG_INSTALLED", True)
    worker = create_worker()

    # Simulate structlog not being importable by removing it from sys.modules
    # and making it raise ImportError
    with patch.dict("sys.modules", {"structlog": None}, clear=False):
        # Act - should handle ImportError gracefully
        worker.configure_structlog_context()

    # Assert - no exception raised (method catches and logs the error)


def test_configure_structlog_context_handles_bind_error(
    create_worker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test configure_structlog_context handles bind_contextvars errors gracefully."""
    # Arrange
    monkeypatch.setattr("litestar_saq.typing.STRUCTLOG_INSTALLED", True)

    mock_structlog = Mock()
    mock_bind = Mock(side_effect=RuntimeError("Context binding failed"))
    mock_structlog.contextvars.bind_contextvars = mock_bind

    worker = create_worker()

    # Act - should catch exception and not re-raise
    with patch.dict("sys.modules", {"structlog": mock_structlog}):
        worker.configure_structlog_context()

    # Assert - passes if no exception propagated


# ============================================================================
# Test on_app_startup integration
# ============================================================================


async def test_on_app_startup_calls_configure_structlog(
    create_worker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test on_app_startup calls configure_structlog_context before starting."""
    # Arrange
    monkeypatch.setattr("litestar_saq.typing.STRUCTLOG_INSTALLED", True)

    mock_structlog = Mock()
    mock_bind = Mock()
    mock_structlog.contextvars.bind_contextvars = mock_bind

    worker = create_worker(separate_process=False)

    # Act
    with patch.dict("sys.modules", {"structlog": mock_structlog}):
        await worker.on_app_startup()

    # Assert - configure_structlog_context should have been called
    mock_bind.assert_called_once()


async def test_on_app_startup_for_in_process_worker(create_worker: Any) -> None:
    """Test on_app_startup works correctly for in-process workers."""
    # Arrange
    worker = create_worker(separate_process=False)

    # Act
    await worker.on_app_startup()

    # Assert - should create asyncio task
    assert hasattr(worker, "_saq_asyncio_tasks")

    # Cleanup - cancel the task to avoid "Task exception never retrieved" warning
    if hasattr(worker, "_saq_asyncio_tasks"):
        worker._saq_asyncio_tasks.cancel()
        try:
            await worker._saq_asyncio_tasks
        except asyncio.CancelledError:
            pass


async def test_on_app_startup_for_separate_process_worker(create_worker: Any) -> None:
    """Test on_app_startup for separate_process=True workers."""
    # Arrange
    worker = create_worker(separate_process=True)

    # Act
    await worker.on_app_startup()

    # Assert - should NOT create asyncio task for separate process
    assert not hasattr(worker, "_saq_asyncio_tasks")


# ============================================================================
# Test run_saq_worker integration (from cli.py)
# ============================================================================


def test_run_saq_worker_calls_configure_structlog_for_separate_process(
    create_worker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test run_saq_worker calls configure_structlog_context for separate_process workers."""
    from litestar_saq.cli import run_saq_worker

    # Arrange
    monkeypatch.setattr("litestar_saq.typing.STRUCTLOG_INSTALLED", True)

    mock_structlog = Mock()
    mock_bind = Mock()
    mock_structlog.contextvars.bind_contextvars = mock_bind

    worker = create_worker(separate_process=True)

    # Mock the asyncio event loop to prevent actual worker execution
    # We need to properly close coroutines to avoid warnings
    mock_loop = Mock()

    def run_until_complete_side_effect(coro_or_task: Any) -> None:
        # Close any coroutines to prevent "never awaited" warnings
        if hasattr(coro_or_task, "close"):
            coro_or_task.close()
        raise KeyboardInterrupt

    mock_loop.run_until_complete = Mock(side_effect=run_until_complete_side_effect)
    mock_loop.create_task = Mock(side_effect=lambda coro: coro)  # Return coroutine as-is

    # Act
    with (
        patch.dict("sys.modules", {"structlog": mock_structlog}),
        patch("asyncio.get_event_loop", return_value=mock_loop),
    ):
        try:
            run_saq_worker(worker, logging_config=None)
        except KeyboardInterrupt:
            pass

    # Assert
    mock_bind.assert_called_once()


def test_run_saq_worker_does_not_configure_for_in_process(
    create_worker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test run_saq_worker does NOT call configure_structlog_context for in-process workers."""
    from litestar_saq.cli import run_saq_worker

    # Arrange
    monkeypatch.setattr("litestar_saq.typing.STRUCTLOG_INSTALLED", True)

    mock_structlog = Mock()
    mock_bind = Mock()
    mock_structlog.contextvars.bind_contextvars = mock_bind

    worker = create_worker(separate_process=False)

    # Mock the asyncio event loop to prevent actual worker execution
    # We need to properly close coroutines to avoid warnings
    mock_loop = Mock()

    def run_until_complete_side_effect(coro_or_task: Any) -> None:
        # Close any coroutines to prevent "never awaited" warnings
        if hasattr(coro_or_task, "close"):
            coro_or_task.close()

    mock_loop.run_until_complete = Mock(side_effect=run_until_complete_side_effect)
    mock_loop.create_task = Mock(side_effect=lambda coro: coro)  # Return coroutine as-is

    # Act
    with (
        patch.dict("sys.modules", {"structlog": mock_structlog}),
        patch("asyncio.get_event_loop", return_value=mock_loop),
    ):
        run_saq_worker(worker, logging_config=None)

    # Assert - configure should NOT be called for in-process workers
    # (they configure during on_app_startup instead)
    mock_bind.assert_not_called()


# ============================================================================
# Integration test with real structlog (skip if not installed)
# ============================================================================


async def test_real_structlog_integration(create_worker: Any) -> None:
    """Integration test with real structlog library."""
    try:
        import structlog  # pyright: ignore
    except ImportError:
        pytest.skip("structlog not available")

    # Arrange
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.JSONRenderer(),
        ],
    )

    worker = create_worker(
        worker_id="integration-test",
        metadata={"test_run": "real_structlog"},
    )

    # Act
    worker.configure_structlog_context()

    # Assert - verify context was bound
    # Get current context
    context_dict = structlog.contextvars.get_contextvars()

    assert context_dict.get("worker_id") == "integration-test"
    assert context_dict.get("queue_name") == "test-queue"
    assert context_dict.get("worker_meta_test_run") == "real_structlog"

    # Cleanup
    structlog.contextvars.clear_contextvars()


# ============================================================================
# Edge cases and error scenarios
# ============================================================================


def test_get_structlog_context_with_empty_metadata_dict(create_worker: Any) -> None:
    """Test get_structlog_context with explicitly empty metadata dict."""
    # Arrange
    worker = create_worker(metadata={})

    # Act
    context = worker.get_structlog_context()

    # Assert
    assert not any(key.startswith("worker_meta_") for key in context.keys())


def test_configure_structlog_context_with_malformed_structlog_module(
    create_worker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test configure_structlog_context handles malformed structlog module."""
    # Arrange
    monkeypatch.setattr("litestar_saq.typing.STRUCTLOG_INSTALLED", True)

    # Mock structlog module missing contextvars attribute
    mock_structlog = Mock()
    del mock_structlog.contextvars  # Remove the attribute

    worker = create_worker()

    # Act - should handle AttributeError gracefully
    with patch.dict("sys.modules", {"structlog": mock_structlog}):
        worker.configure_structlog_context()

    # Assert - passes if no exception raised


def test_get_structlog_context_with_complex_metadata_types(create_worker: Any) -> None:
    """Test get_structlog_context handles various metadata value types."""
    # Arrange
    metadata = {
        "string_val": "test",
        "int_val": 42,
        "bool_val": True,
        "list_val": [1, 2, 3],
        "dict_val": {"nested": "value"},
        "none_val": None,
    }
    worker = create_worker(metadata=metadata)

    # Act
    context = worker.get_structlog_context()

    # Assert - all types should be preserved
    assert context["worker_meta_string_val"] == "test"
    assert context["worker_meta_int_val"] == 42
    assert context["worker_meta_bool_val"] is True
    assert context["worker_meta_list_val"] == [1, 2, 3]
    assert context["worker_meta_dict_val"] == {"nested": "value"}
    assert context["worker_meta_none_val"] is None
