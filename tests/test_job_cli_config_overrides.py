from __future__ import annotations

from pathlib import Path

from ctx_ctr.jobs.produce_events import main as produce_events_main


def test_produce_events_cli_overrides_yaml_config(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    config_path = tmp_path / "produce_events.yaml"
    config_path.write_text(
        "\n".join(
            [
                "impressions: 7",
                "events_per_second: 0",
                "random_seed: 42",
                "log_every: 0",
                "also_unified: false",
                "dry_run: true",
                "impression_topic: ctr.impressions",
                "click_topic: ctr.clicks",
                "event_topic: ctr.events",
            ]
        ),
        encoding="utf-8",
    )

    produce_events_main(
        [
            "--config",
            str(config_path),
            "--impressions",
            "3",
            "--dry-run",
            "--log-every",
            "0",
        ]
    )

    output = capsys.readouterr().out

    assert "Impressions  : 3" in output
    assert f"Config       : {config_path}" in output
