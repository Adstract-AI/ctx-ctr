from __future__ import annotations

from ctx_ctr.jobs.output import print_failure, print_success


def test_print_success_renders_boxed_summary(capsys) -> None:  # type: ignore[no-untyped-def]
    print_success("Example job", ["items: 3", "config: configs/example.yaml"])

    output = capsys.readouterr().out

    assert "SUCCESS JOB SUMMARY" in output
    assert "Job" in output
    assert "Example job" in output
    assert "Status" in output
    assert "SUCCESS" in output
    assert "Items" in output
    assert "3" in output
    assert "Config" in output


def test_print_failure_renders_boxed_summary(capsys) -> None:  # type: ignore[no-untyped-def]
    print_failure("Example job", RuntimeError("boom"))

    output = capsys.readouterr().out

    assert "FAILED JOB SUMMARY" in output
    assert "Example job" in output
    assert "FAILED" in output
    assert "Failure Reason" in output
    assert "boom" in output
