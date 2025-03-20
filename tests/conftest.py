from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from redis.asyncio import Redis

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from pytest_databases.docker.redis import RedisService

pytestmark = pytest.mark.anyio
pytest_plugins = [
    "pytest_databases.docker",
    "pytest_databases.docker.redis",
]


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(name="redis", autouse=True)
async def fx_redis(redis_service: RedisService) -> AsyncGenerator[Redis, None]:
    """Redis instance for testing.

    Returns:
        Redis client instance, function scoped.
    """
    yield Redis(host=redis_service.host, port=redis_service.port)
