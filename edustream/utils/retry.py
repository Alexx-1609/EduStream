# =============================================================================
# utils/retry.py  —  Automatic retry with exponential backoff
#
# What is exponential backoff?
#   If something fails, wait 1s, retry. Fail again? Wait 2s. Again? Wait 4s.
#   This prevents hammering a server that is temporarily down.
# =============================================================================

import time
import functools
from edustream.utils.logging_cfg import get_logger

log = get_logger("retry")


def with_retry(max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 30.0):
    """
    Decorator: automatically retries the wrapped function on failure.

    Usage:
        @with_retry(max_retries=3, base_delay=1.0)
        def connect_to_kafka():
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if attempt == max_retries:
                        log.error(
                            f"[{func.__name__}] Failed after {max_retries} attempts. "
                            f"Last error: {exc}"
                        )
                        raise  # give up — let the caller decide what to do
                    log.warning(
                        f"[{func.__name__}] Attempt {attempt}/{max_retries} failed: {exc}. "
                        f"Retrying in {delay:.1f}s …"
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)   # double the wait, cap at max
        return wrapper
    return decorator
