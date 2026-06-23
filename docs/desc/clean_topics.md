# clean_topics

Deletes and recreates Kafka topics.

## Command

Clean all project topics:

```bash
python -m ctx_ctr.jobs.clean_topics
```

Clean only selected topics:

```bash
python -m ctx_ctr.jobs.clean_topics --only ctr.impressions ctr.clicks
```

Dry-run:

```bash
python -m ctx_ctr.jobs.clean_topics --dry-run
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

- `--only <topic...>`: Cleans only the listed topic names.
- `--dry-run`: Prints selected topics and does not connect to Kafka.

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

