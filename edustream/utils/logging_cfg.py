# =============================================================================
# utils/logging_cfg.py  —  Sets up colored, readable log output
# =============================================================================

import logging
import sys

try:
    import colorlog
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger that:
      - Prints to the terminal with colors (if colorlog is installed)
      - Shows  TIME  |  LEVEL  |  module  |  message
    """
    logger = logging.getLogger(name)

    # Only add handler once (avoid duplicate lines if called multiple times)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)

    if HAS_COLOR:
        fmt = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s%(reset)s │ %(log_color)s%(levelname)-8s%(reset)s │ %(name)-20s │ %(message)s",
            datefmt="%H:%M:%S",
            log_colors={
                "DEBUG":    "cyan",
                "INFO":     "green",
                "WARNING":  "yellow",
                "ERROR":    "red",
                "CRITICAL": "red,bg_white",
            },
        )
    else:
        fmt = logging.Formatter(
            "%(asctime)s │ %(levelname)-8s │ %(name)-20s │ %(message)s",
            datefmt="%H:%M:%S",
        )

    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger
