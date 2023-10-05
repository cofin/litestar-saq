from __future__ import annotations

import asyncio
from logging import getLogger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from saq.types import Context

logger = getLogger(__name__)


async def system_upkeep(_: Context) -> None:
    logger.info("Performing system upkeep operations.")
    logger.info("Simulating a long running operation.  Sleeping for 60 seconds.")
    await asyncio.sleep(60)
    logger.info("Simulating an even longer running operation.  Sleeping for 120 seconds.")
    await asyncio.sleep(120)
    logger.info("Long running process complete.")
    logger.info("Performing system upkeep operations.")


async def background_worker_task(_: Context) -> None:
    logger.info("Performing background worker task.")
    await asyncio.sleep(20)
    logger.info("Performing system upkeep operations.")


async def system_task(_: Context) -> None:
    logger.info("Performing simple system task")
    await asyncio.sleep(2)
    logger.info("System task complete.")
