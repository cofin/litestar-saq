from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import pytest
from redis.asyncio import Redis

# Suppress rich-click pending deprecations triggered by upstream Litestar CLI defaults
warnings.filterwarnings("ignore", category=PendingDeprecationWarning)

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


@pytest.fixture(name="redis")
async def fx_redis(redis_service: RedisService) -> AsyncGenerator[Redis, None]:
    yield Redis(host=redis_service.host, port=redis_service.port)
