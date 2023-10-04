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
from litestar_saq import SaqPlugin, SaqConfig

saq = SaqPlugin(config=SaqConfig())
app = Litestar(plugins=[saq])

```
