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
- `--impression-topic <topic>`: Default impression topic used when `only` is null.
- `--click-topic <topic>`: Default click topic used when `only` is null.
- `--event-topic <topic>`: Default unified event topic used when `only` is null.
- `--dead-letter-topic <topic>`: Default dead-letter topic used when `only` is null.
- `--topic-partitions <int>`: Partition count used for impression, click,
  unified-event, and other selected topics. Defaults to `6`.
- `--dead-letter-topic-partitions <int>`: Dead-letter partition count. Defaults
  to `3`.

CLI flags override values from the YAML config.

## Config Fields

- `only`: List of topic names to clean, or `null` for all project topics.
- `dry_run`: Same behavior as `--dry-run`.
- `impression_topic`, `click_topic`, `event_topic`, `dead_letter_topic`:
  Default project topics used when `only` is `null`.
- `topic_partitions`: Partition count for regular CTR topics and unknown topics
  selected through `only`.
- `dead_letter_topic_partitions`: Partition count for the configured dead-letter
  topic.

Kafka bootstrap servers come from `KAFKA_BOOTSTRAP_SERVERS` in the environment.

## How It Works

The job deletes the selected topics, waits briefly, and recreates them with:

- `topic_partitions` for impression, click, unified-event, and explicitly
  selected topics not otherwise configured
- `dead_letter_topic_partitions` for the configured dead-letter topic
- `replication_factor=1`

The supplied YAML uses six regular partitions and three dead-letter partitions,
matching the local Docker Kafka setup.

## Writes

Kafka:

- deletes selected topics
- recreates selected topics

## Notes

Kafka topic lifecycle is separate from Redis and PostgreSQL state
initialization.

Cleaning topics removes Kafka messages. It does not affect PostgreSQL or Redis.
