from __future__ import annotations

from typing import TYPE_CHECKING

from litestar import Controller, MediaType, get, post
from litestar.exceptions import NotFoundException

from litestar_saq import info, urls

if TYPE_CHECKING:
    from saq.types import QueueInfo

    from litestar_saq.base import Queue


class SAQController(Controller):
    tags = ["SAQ"]

    @get(
        operation_id="WorkerQueueList",
        name="worker:queue-list",
        path=urls.WORKER_QUEUE_LIST,
        media_type=MediaType.JSON,
        cache=False,
        summary="Queue List",
        description="List configured worker queues.",
    )
    async def queue_list(self, task_queues: dict[str, Queue]) -> dict[str, list[QueueInfo]]:
        """Get Worker queues."""
        return {"queues": [await queue.info() for queue in task_queues.values()]}

    @get(
        operation_id="WorkerQueueDetail",
        name="worker:queue-detail",
        path=urls.WORKER_QUEUE_DETAIL,
        media_type=MediaType.JSON,
        cache=False,
        summary="Queue Detail",
        description="List queue details.",
    )
    async def queue_detail(self, task_queues: dict[str, Queue], queue_id: str) -> dict[str, QueueInfo]:
        """Get queue information."""
        queue = task_queues.get(queue_id)
        if not queue:
            msg = f"Could not find the {queue_id} queue"
            raise NotFoundException(msg)
        return {"queue": await queue.info()}

    @get(
        operation_id="WorkerJobDetail",
        name="worker:job-detail",
        path=urls.WORKER_JOB_DETAIL,
        media_type=MediaType.JSON,
        cache=False,
        summary="Job Details",
        description="List job details.",
    )
    async def job_detail(self, task_queues: dict[str, Queue], queue_id: str, job_id: str) -> dict:
        """Get job information."""
        queue = task_queues.get(queue_id)
        if not queue:
            msg = f"Could not find the {queue_id} queue"
            raise NotFoundException(msg)
        job = await info.job(queue, job_id)
        job_dict = job.to_dict()
        if "kwargs" in job_dict:
            job_dict["kwargs"] = repr(job_dict["kwargs"])
        if "result" in job_dict:
            job_dict["result"] = repr(job_dict["result"])
        return {"job": job_dict}

    @post(
        operation_id="WorkerJobRetry",
        name="worker:job-retry",
        path=urls.WORKER_JOB_RETRY,
        media_type=MediaType.JSON,
        cache=False,
        summary="Job Retry",
        description="Retry a failed job..",
    )
    async def job_retry(self, task_queues: dict[str, Queue], queue_id: str, job_id: str) -> dict:
        """Retry job."""
        queue = task_queues.get(queue_id)
        if not queue:
            msg = f"Could not find the {queue_id} queue"
            raise NotFoundException(msg)
        job = await info.job(queue, job_id)
        await job.retry("retried from ui")
        return {}

    @post(
        operation_id="WorkerJobAbort",
        name="worker:job-abort",
        path=urls.WORKER_JOB_ABORT,
        media_type=MediaType.JSON,
        cache=False,
        summary="Job Abort",
        description="Abort active job.",
    )
    async def job_abort(self, task_queues: dict[str, Queue], queue_id: str, job_id: str) -> dict:
        """Abort job."""
        queue = task_queues.get(queue_id)
        if not queue:
            msg = f"Could not find the {queue_id} queue"
            raise NotFoundException(msg)
        job = await info.job(queue, job_id)
        await job.abort("aborted from ui")
        return {}

    # static site
    @get(
        [urls.WORKER_ROOT, urls.WORKER_QUEUE_ROOT, urls.WORKER_JOB_ROOT],
        operation_id="WorkerIndex",
        name="worker:index",
        media_type=MediaType.HTML,
        cache=False,
        include_in_schema=False,
    )
    async def index(self) -> str:
        """Serve site root."""
        return SITE_BODY


SITE_BODY = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="stylesheet" type="text/css" href="/saq/static/pico.min.css.gz">
        <title>SAQ</title>
    </head>
    <body>
        <div id="app"></div>
        <script>const root_path = "/saq";</script>
        <script src="/saq/static/snabbdom.js.gz"></script>
        <script src="/saq/static/app.js"></script>
    </body>
</html>
""".strip()
