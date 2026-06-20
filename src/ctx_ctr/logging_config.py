"""Shared logging configuration."""

from __future__ import annotations

import logging
import sys

LOG_FORMAT = "%(levelname)s %(name)s: %(message)s"

LOG_COLORS = {
    "black": "\033[30m",
    "red": "\033[31m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "blue": "\033[34m",
    "magenta": "\033[35m",
    "cyan": "\033[36m",
    "white": "\033[37m",
}
RESET_COLOR = "\033[0m"
SUCCESS_COLOR = LOG_COLORS["green"]
ERROR_COLOR = LOG_COLORS["red"]
NOISY_LOGGERS = ["kafka"]


class ColorFormatter(logging.Formatter):
    """Format log records with a configured terminal color."""

    def __init__(self, color_name: str) -> None:
        super().__init__(LOG_FORMAT)
        self._color = LOG_COLORS.get(color_name.lower(), LOG_COLORS["cyan"])

    def format(self, record: logging.LogRecord) -> str:
        """Return the formatted log line wrapped in the configured color."""

        message = super().format(record)
        return f"{self._color}{message}{RESET_COLOR}"


def configure_logging(log_level: str, log_color: str) -> None:
    """Configure the project logger from runtime settings."""

    level = logging.getLevelName(log_level.upper())
    if not isinstance(level, int):
        level = logging.INFO

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(ColorFormatter(log_color))

    project_logger = logging.getLogger("ctx_ctr")
    project_logger.handlers.clear()
    project_logger.addHandler(handler)
    project_logger.setLevel(level)
    project_logger.propagate = False

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.WARNING)

    for logger_name in NOISY_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a named project logger."""

    return logging.getLogger(name)


def success_line(message: str) -> str:
    """Return a green terminal summary line."""

    return f"{SUCCESS_COLOR}{message}{RESET_COLOR}"


def error_line(message: str) -> str:
    """Return a red terminal summary line."""

    return f"{ERROR_COLOR}{message}{RESET_COLOR}"
