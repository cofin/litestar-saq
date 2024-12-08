"""Metadata for the Project."""
from __future__ import annotations

import importlib.metadata

__all__ = ["__project__", "__version__"]

__version__ = importlib.metadata.version("litestar_saq")
"""Version of the project."""
__project__ = importlib.metadata.metadata("litestar_saq")["Name"]
"""Name of the project."""
