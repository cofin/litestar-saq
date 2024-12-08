from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from redis.asyncio import Redis

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


pytestmark = pytest.mark.anyio
pytest_plugins = [
    "pytest_databases.docker",
    "pytest_databases.docker.redis",
]


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(name="redis")
async def fx_redis(redis_docker_ip: str, redis_service: None, redis_port: int) -> AsyncGenerator[Redis, None]:
    """Redis instance for testing.

    Returns:
        Redis client instance, function scoped.
    """
    yield Redis(host=redis_docker_ip, port=redis_port)
