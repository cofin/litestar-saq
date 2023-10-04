from __future__ import annotations

from litestar import Litestar

from litestar_saq import SAQConfig, SAQPlugin

saq = SAQPlugin(config=SAQConfig(redis_url="redis://cache:6379/0"))
app = Litestar(plugins=[saq])
