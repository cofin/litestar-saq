class LitestarSaqError(Exception):
    """Base exception type for the Litestar Saq."""


class ImproperConfigurationError(LitestarSaqError):
    """Improper Configuration error.

    This exception is raised only when a module depends on a dependency that has not been installed.
    """


class BackgroundTaskError(Exception):
    """Base class for `Task` related exceptions."""
