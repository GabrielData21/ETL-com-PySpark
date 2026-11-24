"""
Logging configuration for the ETL pipeline.
"""
import logging
import sys


def get_logger(name: str = "ETL-ENEM") -> logging.Logger:
    """
    Returns a configured logger instance with standard formatting.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
