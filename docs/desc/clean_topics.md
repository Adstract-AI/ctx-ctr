# clean_topics

Deletes and recreates Kafka topics.

## Command

Clean all project topics:

```bash
python -m ctx_ctr.jobs.clean_topics
```

Short command:

```bash
clean-topics
```

Clean only selected topics:

```bash
python -m ctx_ctr.jobs.clean_topics --only ctr.impressions ctr.clicks
```

Dry-run:

```bash
python -m ctx_ctr.jobs.clean_topics --dry-run
```

Default config:

```text
configs/clean_topics.yaml
```

## Purpose

Use this job when Kafka contains old messages and you want a clean stream for a
new local run.

## Default Topics

Without `--only`, the job cleans all project topics:

- `ctr.impressions`
- `ctr.clicks`
- `ctr.events`
- `ctr.dead-letter`

## Flags

- `--config <path>`: YAML config path. Defaults to `configs/clean_topics.yaml`.
- `--only <topic...>`: Cleans only the listed topic names.
- `--dry-run`: Prints selected topics and does not connect to Kafka.
- `--bootstrap-servers <host:port>`: Kafka bootstrap servers.
- `--impression-topic <topic>`: Default impression topic used when `only` is null.
- `--click-topic <topic>`: Default click topic used when `only` is null.
- `--event-topic <topic>`: Default unified event topic used when `only` is null.
- `--dead-letter-topic <topic>`: Default dead-letter topic used when `only` is null.

CLI flags override values from the YAML config.

## Config Fields

- `only`: List of topic names to clean, or `null` for all project topics.
- `dry_run`: Same behavior as `--dry-run`.
- `bootstrap_servers`: Kafka bootstrap servers.
- `impression_topic`, `click_topic`, `event_topic`, `dead_letter_topic`:
  Default project topics used when `only` is `null`.

## How It Works

The job deletes the selected topics, waits briefly, and recreates them with:

- `num_partitions=1`
- `replication_factor=1`

This matches the local Docker Kafka setup.

## Writes

Kafka:

- deletes selected topics
- recreates selected topics

## Notes

This job is intentionally separate from `seed_values` and `reset_values`.

Cleaning topics removes Kafka messages. It does not affect PostgreSQL or Redis.
