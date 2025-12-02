from __future__ import annotations

from typing import Any

import pytest
from litestar import Litestar
from litestar.di import Provide
from litestar.exceptions import ImproperlyConfiguredException
from litestar.testing import TestClient
from saq.types import QueueInfo

from litestar_saq.config import TaskQueues
from litestar_saq.controllers import build_controller


class DummyQueue:
    def __init__(self, name: str) -> None:
        self.name = name

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> dict[str, Any]:
        return {"name": self.name, "jobs": [] if jobs else None}


def test_saq_ui_serves_without_trailing_slash() -> None:
    queues = TaskQueues(queues={"default": DummyQueue("default")})
    controller = build_controller("/saq")

    app = Litestar(
        route_handlers=[controller],
        dependencies={"task_queues": Provide(lambda: queues, sync_to_thread=False)},
        signature_namespace={"TaskQueues": TaskQueues, "QueueInfo": QueueInfo},
    )

    with TestClient(app) as client:
        resp = client.get("/saq")
        assert resp.status_code == 200
        assert 'root_path = "/saq/"' in resp.text

        resp_nested = client.get("/saq/queues/default")
        assert resp_nested.status_code == 200


def test_taskqueues_get_returns_configured_queue() -> None:
    queues = TaskQueues(queues={"alpha": DummyQueue("alpha")})
    assert queues.get("alpha").name == "alpha"

    with pytest.raises(ImproperlyConfiguredException):
        queues.get("missing")


def test_health_endpoint_returns_ok_when_queues_accessible() -> None:
    queues = TaskQueues(queues={"default": DummyQueue("default")})
    controller = build_controller("/saq")

    app = Litestar(
        route_handlers=[controller],
        dependencies={"task_queues": Provide(lambda: queues, sync_to_thread=False)},
        signature_namespace={"TaskQueues": TaskQueues, "QueueInfo": QueueInfo},
    )

    with TestClient(app) as client:
        resp = client.get("/saq/health")
        assert resp.status_code == 200
        assert resp.text == "OK"


class FailingQueue:
    def __init__(self, name: str) -> None:
        self.name = name

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> dict[str, Any]:
        msg = "Queue connection failed"
        raise RuntimeError(msg)


def test_health_endpoint_returns_500_when_queues_fail() -> None:
    queues = TaskQueues(queues={"default": FailingQueue("default")})
    controller = build_controller("/saq")

    app = Litestar(
        route_handlers=[controller],
        dependencies={"task_queues": Provide(lambda: queues, sync_to_thread=False)},
        signature_namespace={"TaskQueues": TaskQueues, "QueueInfo": QueueInfo},
    )

    with TestClient(app) as client:
        resp = client.get("/saq/health")
        assert resp.status_code == 500
        assert resp.text == "Service Unavailable"


def test_health_endpoint_returns_500_when_no_queues() -> None:
    queues = TaskQueues(queues={})
    controller = build_controller("/saq")

    app = Litestar(
        route_handlers=[controller],
        dependencies={"task_queues": Provide(lambda: queues, sync_to_thread=False)},
        signature_namespace={"TaskQueues": TaskQueues, "QueueInfo": QueueInfo},
    )

    with TestClient(app) as client:
        resp = client.get("/saq/health")
        assert resp.status_code == 500
        assert resp.text == "Service Unavailable"
