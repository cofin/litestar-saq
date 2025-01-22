"""Metadata for the Project."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, metadata, version

__all__ = ("__project__", "__version__")

try:
    __version__ = version("litestar_saq")
    """Version of the project."""
    __project__ = metadata("litestar_saq")["Name"]
    """Name of the project."""
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"
    __project__ = "Litestar SAQ"
finally:
    del version, PackageNotFoundError, metadata
