# Litestar SAQ

> [!IMPORTANT]
> This plugin currently contains minimal features and is a work-in-progress

## Installation

```shell
pip install litestar-saq
```

## Usage

Here is a basic application that demonstrates how to use the plugin.

```python
from __future__ import annotations

from litestar import Litestar

from litestar_saq import QueueConfig, SAQConfig, SAQPlugin

saq = SAQPlugin(config=SAQConfig(redis_url="redis://localhost:6397/0", queue_configs=[QueueConfig(name="samples")]))
app = Litestar(plugins=[saq])


```

You can start a background worker with the following command now:

```shell
litestar --app-dir=examples/ --app basic:app workers run
Using Litestar app from env: 'basic:app'
Starting SAQ Workers ──────────────────────────────────────────────────────────────────
INFO - 2023-10-04 17:39:03,255 - saq - worker - Worker starting: Queue<redis=Redis<ConnectionPool<Connection<host=localhost,port=6397,db=0>>>, name='samples'>
INFO - 2023-10-04 17:39:06,545 - saq - worker - Worker shutting down
```
