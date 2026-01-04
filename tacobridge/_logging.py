"""Logging configuration for tacobridge."""

import logging

DEFAULT_FORMAT = "%(levelname)s [%(name)s] %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Get logger for tacobridge module."""
    if not name.startswith("tacobridge"):
        name = "tacobridge" if name == "__main__" else f"tacobridge.{name}"
    return logging.getLogger(name)


def setup_basic_logging(level: int = logging.INFO, fmt: str | None = None) -> None:
    """Setup basic logging configuration for tacobridge."""
    if fmt is None:
        fmt = DEFAULT_FORMAT

    logger = logging.getLogger("tacobridge")
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)

    logger.propagate = False


def disable_logging() -> None:
    """Disable all tacobridge logging."""
    logger = logging.getLogger("tacobridge")
    logger.setLevel(logging.CRITICAL + 1)
