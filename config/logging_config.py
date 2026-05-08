"""
config/logging_config.py
========================
Centralized logging configuration for all services.
"""

import logging
import sys
from pathlib import Path


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Create and configure a logger with both console and file handlers.

    Args:
        name: Logger name (usually __name__ of the calling module)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(log_dir / f"{name.split('.')[-1]}.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
