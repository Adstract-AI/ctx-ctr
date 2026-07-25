"""Human-readable job output helpers."""

from __future__ import annotations

from collections.abc import Iterable

from ctx_ctr.logging_config import error_line, success_line

MIN_SUMMARY_WIDTH = 78
SUMMARY_PADDING = 2


def print_success(title: str, lines: Iterable[str]) -> None:
    """Print a green boxed success summary."""

    for line in _summary_box(
        heading="SUCCESS JOB SUMMARY",
        title=title,
        status="SUCCESS",
        lines=lines,
    ):
        print(success_line(line))


def print_failure(title: str, error: BaseException) -> None:
    """Print a red boxed failure summary."""

    for line in _summary_box(
        heading="FAILED JOB SUMMARY",
        title=title,
        status="FAILED",
        lines=[f"failure reason: {error}"],
    ):
        print(error_line(line))


def format_summary_box(
    *,
    heading: str,
    title: str,
    status: str,
    lines: Iterable[str],
) -> str:
    """Return a plain boxed summary string for logging."""

    return "\n".join(
        _summary_box(
            heading=heading,
            title=title,
            status=status,
            lines=lines,
        )
    )


def _summary_box(
    *,
    heading: str,
    title: str,
    status: str,
    lines: Iterable[str],
) -> list[str]:
    rows = [("Job", title), ("Status", status), *[_parse_summary_line(line) for line in lines]]
    label_width = max(len(label) for label, _value in rows)
    value_width = max(len(str(value)) for _label, value in rows)
    content_width = SUMMARY_PADDING + label_width + 3 + value_width + SUMMARY_PADDING
    width = max(MIN_SUMMARY_WIDTH, content_width)
    return [
        _summary_border(heading, width),
        *[_summary_row(label, str(value), label_width, width) for label, value in rows],
        _summary_border("", width),
    ]


def _parse_summary_line(line: str) -> tuple[str, str]:
    label, separator, value = line.partition(":")
    if not separator:
        return "Info", line.strip()
    return label.strip().title(), value.strip()


def _summary_border(heading: str, width: int) -> str:
    if not heading:
        return f"+{'=' * (width - 2)}+"
    title = f" {heading} "
    remaining = width - len(title) - 2
    left = remaining // 2
    right = remaining - left
    return f"+{'=' * left}{title}{'=' * right}+"


def _summary_row(label: str, value: str, label_width: int, width: int) -> str:
    prefix = f"|  {label:<{label_width}} : {value}"
    return f"{prefix:<{width - 1}}|"
