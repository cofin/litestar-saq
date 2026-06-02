import io
from multiprocessing.reduction import ForkingPickler
from typing import Any

import pytest
from litestar import Litestar
from redis.asyncio import from_url as redis_from_url

from litestar_saq import QueueConfig, SAQConfig
from litestar_saq.cli import (
    prepare_config_for_spawn,
    prepare_logging_config_for_spawn,
    requires_multiprocessing_safe_args,
    run_worker_in_child,
)
from litestar_saq.exceptions import ImproperConfigurationError


async def _noop(_ctx: dict) -> None:
    return None


def test_prepare_config_for_spawn_nulls_broker_instance_when_dsn_present() -> None:
    cfg = SAQConfig(
        queue_configs=[
            QueueConfig(name="q1", dsn="redis://localhost:6379/0", tasks=[_noop]),
            QueueConfig(name="q2", dsn="redis://localhost:6379/0", tasks=[_noop]),
        ],
    )
    # Force broker construction in the parent so broker_instance is a live Redis.
    for qc in cfg.queue_configs:
        qc.get_broker()
        assert qc.broker_instance is not None
    cfg.get_queues()
    assert cfg.queue_instances is not None

    prepared = prepare_config_for_spawn(cfg)

    # Original is untouched.
    assert cfg.queue_instances is not None
    for qc in cfg.queue_configs:
        assert qc.broker_instance is not None

    # Prepared copy has no live brokers.
    for qc in prepared.queue_configs:
        assert qc.broker_instance is None
        assert qc.dsn is not None

    # Prepared copy is picklable (the whole point).
    ForkingPickler.dumps(prepared)


def test_prepare_config_for_spawn_rejects_broker_instance_without_dsn() -> None:
    live_client = redis_from_url("redis://localhost:6379/0")
    cfg = SAQConfig(
        queue_configs=[
            QueueConfig(name="q1", broker_instance=live_client, tasks=[_noop]),
        ],
    )

    with pytest.raises(ImproperConfigurationError, match="dsn"):
        prepare_config_for_spawn(cfg)


def test_run_worker_in_child_is_picklable() -> None:
    # The function itself must be picklable so multiprocessing.Process can
    # ship it as `target` under forkserver/spawn.
    ForkingPickler.dumps(run_worker_in_child)


@pytest.mark.parametrize(
    ("start_method", "expected"),
    [
        ("fork", False),
        ("spawn", True),
        ("forkserver", True),
    ],
)
def test_requires_multiprocessing_safe_args(
    start_method: str,
    expected: bool,
    mocker: Any,
) -> None:
    mocker.patch("litestar_saq.cli.multiprocessing.get_start_method", return_value=start_method)

    assert requires_multiprocessing_safe_args() is expected


def test_spawn_args_are_picklable_with_forking_pickler() -> None:
    """The exact (target, args) tuple multiprocessing.Process would pickle
    under forkserver/spawn must round-trip through ForkingPickler - the same
    pickler multiprocessing uses internally. This is the unit-level guarantee
    that prevents the #104 PicklingError without spawning real worker loops.
    """
    cfg = SAQConfig(
        queue_configs=[
            QueueConfig(name="queue-a", dsn="redis://localhost:6379/0", tasks=[_noop]),
            QueueConfig(name="queue-b", dsn="redis://localhost:6379/0", tasks=[_noop]),
        ],
    )
    # Force live broker construction in the parent (mirrors what the CLI does
    # before calling prepare_config_for_spawn).
    cfg.get_queues()

    spawn_config = prepare_config_for_spawn(cfg)
    spawn_args = ("queue-a", spawn_config, None)

    buf = io.BytesIO()
    ForkingPickler(buf).dump((run_worker_in_child, spawn_args))

    target, args = ForkingPickler.loads(buf.getvalue())
    assert target is run_worker_in_child
    queue_name, restored_config, restored_logging = args
    assert queue_name == "queue-a"
    assert restored_logging is None
    # The restored config has no live broker references; children rebuild
    # them lazily from `dsn`.
    for qc in restored_config.queue_configs:
        assert qc.broker_instance is None
        assert qc.dsn == "redis://localhost:6379/0"


def test_spawn_args_are_picklable_with_default_litestar_logging_config() -> None:
    cfg = SAQConfig(
        queue_configs=[
            QueueConfig(name="queue-a", dsn="redis://localhost:6379/0", tasks=[_noop]),
            QueueConfig(name="queue-b", dsn="redis://localhost:6379/0", tasks=[_noop]),
        ],
    )
    cfg.get_queues()
    spawn_config = prepare_config_for_spawn(cfg)
    spawn_logging_config = prepare_logging_config_for_spawn(Litestar(route_handlers=[]).logging_config)
    spawn_args = ("queue-a", spawn_config, spawn_logging_config)

    buf = io.BytesIO()
    ForkingPickler(buf).dump((run_worker_in_child, spawn_args))

    _target, args = ForkingPickler.loads(buf.getvalue())
    _queue_name, _restored_config, restored_logging = args
    assert restored_logging is not None


def test_prepare_logging_config_for_spawn_rewrites_default_queue_handler() -> None:
    prepared = prepare_logging_config_for_spawn(Litestar(route_handlers=[]).logging_config)

    assert prepared is not None
    handler = prepared.logging_config["handlers"]["queue_listener"]
    assert handler["class"] == "logging.StreamHandler"
    assert "listener" not in handler
    ForkingPickler.dumps(prepared)
