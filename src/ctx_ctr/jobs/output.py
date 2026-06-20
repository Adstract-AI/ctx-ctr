"""Human-readable job output helpers."""

from __future__ import annotations

from collections.abc import Iterable

from ctx_ctr.logging_config import error_line, success_line


def print_success(title: str, lines: Iterable[str]) -> None:
    """Print a green success summary."""

    print(success_line(f"OK {title}"))
    for line in lines:
        print(success_line(f"  {line}"))


def print_failure(title: str, error: BaseException) -> None:
    """Print a red failure summary."""

    print(error_line(f"FAILED {title}"))
    print(error_line(f"  {error}"))
