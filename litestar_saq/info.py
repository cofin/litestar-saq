from __future__ import annotations

from typing import TYPE_CHECKING, cast

from .exceptions import LitestarSaqError

if TYPE_CHECKING:
    from .base import Job, Queue


async def job(queue: Queue, job_id: str) -> Job:
    job = await queue._get_job_by_id(job_id)  # noqa: SLF001
    if not job:
        msg = "Could not find job ID %s"
        raise LitestarSaqError(msg, job_id)
    return cast("Job", job)
