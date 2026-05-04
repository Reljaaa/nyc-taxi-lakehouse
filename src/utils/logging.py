"""Shared logging configuration."""

import logging
import sys


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging once for notebooks and jobs."""
    root_logger = logging.getLogger()

    if root_logger.handlers:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )

    root_logger.addHandler(handler)
    root_logger.setLevel(level)
