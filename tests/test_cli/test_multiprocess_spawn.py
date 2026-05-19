from __future__ import annotations

import pickle
from multiprocessing.reduction import ForkingPickler

import pytest
from redis.asyncio import from_url as redis_from_url

from litestar_saq import QueueConfig, SAQConfig
from litestar_saq.cli import _prepare_config_for_spawn, _run_worker_in_child
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

    prepared = _prepare_config_for_spawn(cfg)

    # Original is untouched.
    for qc in cfg.queue_configs:
        assert qc.broker_instance is not None

    # Prepared copy has no live brokers.
    for qc in prepared.queue_configs:
        assert qc.broker_instance is None
        assert qc.dsn is not None

    # Prepared copy is picklable (the whole point).
    pickle.dumps(prepared)


def test_prepare_config_for_spawn_rejects_broker_instance_without_dsn() -> None:
    live_client = redis_from_url("redis://localhost:6379/0")
    cfg = SAQConfig(
        queue_configs=[
            QueueConfig(name="q1", broker_instance=live_client, tasks=[_noop]),
        ],
    )

    with pytest.raises(ImproperConfigurationError, match="dsn"):
        _prepare_config_for_spawn(cfg)


def test_run_worker_in_child_is_picklable() -> None:
    # The function itself must be picklable so multiprocessing.Process can
    # ship it as `target` under forkserver/spawn.
    pickle.dumps(_run_worker_in_child)


def test_spawn_args_are_picklable_with_forking_pickler() -> None:
    """The exact (target, args) tuple multiprocessing.Process would pickle
    under forkserver/spawn must round-trip through ForkingPickler — the same
    pickler multiprocessing uses internally. This is the unit-level guarantee
    that prevents the #104 PicklingError without spawning real worker loops.
    """
    import io

    cfg = SAQConfig(
        queue_configs=[
            QueueConfig(name="queue-a", dsn="redis://localhost:6379/0", tasks=[_noop]),
            QueueConfig(name="queue-b", dsn="redis://localhost:6379/0", tasks=[_noop]),
        ],
    )
    # Force live broker construction in the parent (mirrors what the CLI does
    # before calling _prepare_config_for_spawn).
    cfg.get_queues()

    spawn_config = _prepare_config_for_spawn(cfg)
    spawn_args = ("queue-a", spawn_config, None)

    buf = io.BytesIO()
    ForkingPickler(buf).dump((_run_worker_in_child, spawn_args))

    target, args = pickle.loads(buf.getvalue())
    assert target is _run_worker_in_child
    queue_name, restored_config, restored_logging = args
    assert queue_name == "queue-a"
    assert restored_logging is None
    # The restored config has no live broker references — children rebuild
    # them lazily from `dsn`.
    for qc in restored_config.queue_configs:
        assert qc.broker_instance is None
        assert qc.dsn == "redis://localhost:6379/0"
