# Litestar SAQ

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

saq = SAQPlugin(config=SAQConfig(dsn="redis://localhost:6397/0", queue_configs=[QueueConfig(name="samples")]))
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

You can also start the process for only specific queues. This is helpful if you want separated processes working on different queues instead of combining them.

```shell
litestar --app-dir=examples/ --app basic:app workers run --queues sample
Using Litestar app from env: 'basic:app'
Starting SAQ Workers ──────────────────────────────────────────────────────────────────
INFO - 2023-10-04 17:39:03,255 - saq - worker - Worker starting: Queue<redis=Redis<ConnectionPool<Connection<host=localhost,port=6397,db=0>>>, name='samples'>
INFO - 2023-10-04 17:39:06,545 - saq - worker - Worker shutting down
```

If you are starting the process for only specific queues and still want to read from the other queues or enqueue a task into another queue that was not initialized in your worker or is found somewhere else, you can do so like here

```python
import os
from saq import Queue


def get_queue_directly(queue_name: str, redis_url: str) -> Queue:
    return Queue.from_url(redis_url, name=queue_name)

redis_url = os.getenv("REDIS_URL")
queue = get_queue_directly("queue-in-other-process", redis_url)
# Get queue info
info = await queue.info(jobs=True)
# Enqueue new task
queue.enqueue(
    ....
)
```
