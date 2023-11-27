from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, cast

from litestar.exceptions import NotFoundException

if TYPE_CHECKING:
    from litestar import Controller
    from litestar.types.callable_types import Guard
    from saq.types import QueueInfo

    from litestar_saq.base import Job
    from litestar_saq.config import TaskQueue, TaskQueues


async def job_info(queue: TaskQueue, job_id: str) -> Job:
    job = await queue._get_job_by_id(job_id)  # noqa: SLF001
    if not job:
        msg = f"Could not find job ID {job_id}"
        raise NotFoundException(msg)
    return cast("Job", job)


@lru_cache(typed=True)
def build_controller(  # noqa: C901
    url_base: str = "/saq",
    controller_guards: list[Guard] | None = None,
) -> type[Controller]:
    from litestar import Controller, MediaType, get, post
    from litestar.exceptions import NotFoundException
    from litestar.status_codes import HTTP_202_ACCEPTED

    class SAQController(Controller):
        tags = ["SAQ"]
        guards = controller_guards

        @get(
            operation_id="WorkerQueueList",
            name="worker:queue-list",
            path=f"{url_base}/api/queues/",
            media_type=MediaType.JSON,
            cache=False,
            summary="Queue List",
            description="List configured worker queues.",
        )
        async def queue_list(self, task_queues: TaskQueues) -> dict[str, list[QueueInfo]]:
            """Get Worker queues."""
            return {"queues": [await queue.info() for queue in task_queues.queues.values()]}

        @get(
            operation_id="WorkerQueueDetail",
            name="worker:queue-detail",
            path=f"{url_base}/api/queues/{{queue_id:str}}",
            media_type=MediaType.JSON,
            cache=False,
            summary="Queue Detail",
            description="List queue details.",
        )
        async def queue_detail(self, task_queues: TaskQueues, queue_id: str) -> dict[str, QueueInfo]:
            """Get queue information."""
            queue = task_queues.get(queue_id)
            if not queue:
                msg = f"Could not find the {queue_id} queue"
                raise NotFoundException(msg)
            return {"queue": await queue.info()}

        @get(
            operation_id="WorkerJobDetail",
            name="worker:job-detail",
            path=f"{url_base}/api/queues/{{queue_id:str}}/jobs/{{job_id:str}}",
            media_type=MediaType.JSON,
            cache=False,
            summary="Job Details",
            description="List job details.",
        )
        async def job_detail(self, task_queues: TaskQueues, queue_id: str, job_id: str) -> dict:
            """Get job information."""
            queue = task_queues.get(queue_id)
            if not queue:
                msg = f"Could not find the {queue_id} queue"
                raise NotFoundException(msg)
            job = await job_info(queue, job_id)
            job_dict = job.to_dict()
            if "kwargs" in job_dict:
                job_dict["kwargs"] = repr(job_dict["kwargs"])
            if "result" in job_dict:
                job_dict["result"] = repr(job_dict["result"])
            return {"job": job_dict}

        @post(
            operation_id="WorkerJobRetry",
            name="worker:job-retry",
            path=f"{url_base}/api/queues/{{queue_id:str}}/jobs/{{job_id:str}}/retry",
            media_type=MediaType.JSON,
            cache=False,
            summary="Job Retry",
            description="Retry a failed job..",
            status_code=HTTP_202_ACCEPTED,
        )
        async def job_retry(self, task_queues: TaskQueues, queue_id: str, job_id: str) -> dict:
            """Retry job."""
            queue = task_queues.get(queue_id)
            if not queue:
                msg = f"Could not find the {queue_id} queue"
                raise NotFoundException(msg)
            job = await job_info(queue, job_id)
            await job.retry("retried from ui")
            return {}

        @post(
            operation_id="WorkerJobAbort",
            name="worker:job-abort",
            path=f"{url_base}/api/queues/{{queue_id:str}}/jobs/{{job_id:str}}/abort",
            media_type=MediaType.JSON,
            cache=False,
            summary="Job Abort",
            description="Abort active job.",
            status_code=HTTP_202_ACCEPTED,
        )
        async def job_abort(self, task_queues: TaskQueues, queue_id: str, job_id: str) -> dict:
            """Abort job."""
            queue = task_queues.get(queue_id)
            if not queue:
                msg = f"Could not find the {queue_id} queue"
                raise NotFoundException(msg)
            job = await job_info(queue, job_id)
            await job.abort("aborted from ui")
            return {}

        # static site
        @get(
            [
                url_base,
                f"{url_base}/queues/{{queue_id:str}}",
                f"{url_base}/queues/{{queue_id:str}}/jobs/{{job_id:str}}",
            ],
            operation_id="WorkerIndex",
            name="worker:index",
            media_type=MediaType.HTML,
            include_in_schema=False,
        )
        async def index(self) -> str:
            """Serve site root."""
            return f"""
            <!DOCTYPE html>
            <html>
                <head>
                    <meta charset="utf-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1">
                    <link rel="stylesheet" type="text/css" href="{url_base}/static/pico.min.css.gz">
                    <title>SAQ</title>
                </head>
                <body>
                    <div id="app"></div>
                    <script>const root_path = "{url_base}";</script>
                    <script src="{url_base}/static/snabbdom.js.gz"></script>
                    <script src="{url_base}/static/app.js"></script>
                </body>
            </html>""".strip()

    return SAQController
