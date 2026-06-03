import importlib
import types
from typing import cast

import pytest

from litestar_saq.config import QueueConfig


def test_postgres_pool_defaults_sets_autocommit(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyPool:
        def __init__(self) -> None:
            self.kwargs = None

    dummy_module = types.SimpleNamespace(AsyncConnectionPool=DummyPool)

    def fake_import_module(path: str) -> types.ModuleType:
        if path == "psycopg_pool":
            return dummy_module  # type: ignore[return-value]
        return importlib.import_module(path)

    monkeypatch.setattr("litestar_saq.config.import_module", fake_import_module)

    config = QueueConfig(dsn="postgresql://user:pass@localhost/db")
    config.broker_instance = DummyPool()  # type: ignore[assignment]

    config._ensure_postgres_pool_defaults()

    pool = cast(DummyPool, config.broker_instance)
    assert pool.kwargs is not None
    assert pool.kwargs["autocommit"] is True


def test_broker_type_detection_with_async_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that redis.asyncio.Redis instances are correctly detected."""

    class DummyAsyncRedis:
        pass

    dummy_module = types.SimpleNamespace(Redis=DummyAsyncRedis)

    def fake_import_module(path: str) -> types.ModuleType:
        if path == "redis.asyncio":
            return dummy_module  # type: ignore[return-value]
        return importlib.import_module(path)

    monkeypatch.setattr("litestar_saq.config.import_module", fake_import_module)

    config = QueueConfig(dsn="redis://localhost:6379/0")
    config.broker_instance = DummyAsyncRedis()  # type: ignore[assignment]

    assert config.broker_type == "redis"


def test_postgres_queue_options_does_not_contain_deprecated_options() -> None:
    """Verify that deprecated postgres lock options are not in PostgresQueueOptions."""
    from litestar_saq.config import PostgresQueueOptions

    assert "job_lock_keyspace" not in PostgresQueueOptions.__annotations__
    assert "job_lock_sweep" not in PostgresQueueOptions.__annotations__


def test_background_task_error_not_importable() -> None:
    """Verify that BackgroundTaskError has been removed and cannot be imported."""
    import litestar_saq.exceptions

    assert not hasattr(litestar_saq.exceptions, "BackgroundTaskError")
