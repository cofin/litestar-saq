# Litestar SAQ

## Installation

```shell
pip install litestar-saq
```

For OpenTelemetry support:

```shell
pip install litestar-saq[otel]
```

## Usage

Here is a basic application that demonstrates how to use the plugin.

```python
from litestar import Litestar
from litestar_saq import QueueConfig, SAQConfig, SAQPlugin

saq = SAQPlugin(
    config=SAQConfig(
        use_server_lifespan=True,
        queue_configs=[
            QueueConfig(name="samples", dsn="redis://localhost:6379/0")
        ],
    )
)
app = Litestar(plugins=[saq])
```

You can start a background worker with the following command now:

```shell
litestar --app-dir=examples/ --app basic:app workers run
Using Litestar app from env: 'basic:app'
Starting SAQ Workers ──────────────────────────────────────────────────────────────────
INFO - 2023-10-04 17:39:03,255 - saq - worker - Worker starting: Queue<redis=Redis<ConnectionPool<Connection<host=localhost,port=6379,db=0>>>, name='samples'>
INFO - 2023-10-04 17:39:06,545 - saq - worker - Worker shutting down
```

You can also start the process for only specific queues. This is helpful if you want separated processes working on different queues instead of combining them.

```shell
litestar --app-dir=examples/ --app basic:app workers run --queues sample
Using Litestar app from env: 'basic:app'
Starting SAQ Workers ──────────────────────────────────────────────────────────────────
INFO - 2023-10-04 17:39:03,255 - saq - worker - Worker starting: Queue<redis=Redis<ConnectionPool<Connection<host=localhost,port=6379,db=0>>>, name='samples'>
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
await queue.enqueue("task_name", arg1="value1")
```

## Monitored Jobs

For long-running tasks, use the `monitored_job` decorator to automatically send heartbeats and prevent SAQ from marking jobs as stuck:

```python
from litestar_saq import monitored_job

@monitored_job()  # Auto-calculates interval from job.heartbeat
async def long_running_task(ctx):
    await process_large_dataset()
    return {"status": "complete"}

@monitored_job(interval=30.0)  # Explicit 30-second interval
async def train_model(ctx, model_id: str):
    for epoch in range(100):
        await train_epoch(model)
    return {"model_id": model_id}
```

## OpenTelemetry Integration

litestar-saq supports optional OpenTelemetry instrumentation for distributed tracing.

### Configuration

```python
from litestar_saq import SAQConfig, QueueConfig

config = SAQConfig(
    queue_configs=[QueueConfig(dsn="redis://localhost:6379/0")],
    enable_otel=None,  # Auto-detect (default) - enabled if OTEL installed AND Litestar OpenTelemetryPlugin is active
    # enable_otel=True,  # Force enable (raises error if not installed)
    # enable_otel=False,  # Force disable
)
```

When enabled, the plugin creates:

- **CONSUMER spans** for job processing
- **PRODUCER spans** for job enqueue operations
- **Automatic context propagation** across process boundaries

Spans follow [OpenTelemetry messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/) with `messaging.system = "saq"`.
