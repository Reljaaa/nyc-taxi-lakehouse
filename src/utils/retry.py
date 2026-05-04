"""Retry helpers for transient external failures."""

import logging
import time
from functools import wraps
from typing import Any, Callable


logger = logging.getLogger(__name__)


class TransientHTTPError(Exception):
    """Raised for HTTP responses that should be retried (5xx, 429)."""


def raise_for_transient_status(response: Any) -> None:
    """Raise retryable errors for 5xx/429 and normal HTTP errors for 4xx."""
    if response.status_code == 429 or response.status_code >= 500:
        raise TransientHTTPError(
            f"Transient HTTP {response.status_code} for {response.url}"
        )

    response.raise_for_status()


def retry_with_backoff(
    max_attempts: int,
    initial_wait_s: float,
    max_wait_s: float,
    retry_on: tuple[type[BaseException], ...],
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Retry a function for selected exception types with exponential backoff."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    def decorator(function: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(function)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            wait_s = initial_wait_s

            for attempt in range(1, max_attempts + 1):
                try:
                    return function(*args, **kwargs)
                except retry_on as error:
                    if attempt == max_attempts:
                        logger.error(
                            "final attempt failed function=%s attempt=%s "
                            "total_attempts=%s planned_wait_s=%.2f error=%s: %s",
                            function.__name__,
                            attempt,
                            max_attempts,
                            0.0,
                            error.__class__.__name__,
                            error,
                        )
                        raise

                    planned_wait_s = wait_s
                    logger.warning(
                        "retrying function=%s attempt=%s total_attempts=%s "
                        "planned_wait_s=%.2f error=%s: %s",
                        function.__name__,
                        attempt,
                        max_attempts,
                        planned_wait_s,
                        error.__class__.__name__,
                        error,
                    )
                    time.sleep(planned_wait_s)
                    # Could add jitter (random.uniform offset) here for multi-client scenarios; not needed for our single-client TLC download.
                    wait_s = min(wait_s * 2, max_wait_s)

        return wrapper

    return decorator
