# run_streaming_weight_update

Runs the Flink-native streaming weight-update job.

## Command

```bash
python -m ctx_ctr.jobs.run_streaming_weight_update
```

Short command:

```bash
streaming-weight-update
```

Default config:

```text
configs/run_streaming_weight_update.yaml
```

## Purpose

Use this job when you want the weight updater itself to be a real long-running
Flink streaming job.

Unlike `weight-update`, this job does not scan Redis `ctr:*` buckets. It
consumes Kafka impression/click events directly, keeps cumulative evidence in
Flink keyed state, and recalibrates weights on a processing-time timer.

## Required Services

- Kafka
- Redis
- PostgreSQL, unless `dry_run` is enabled

Redis must already contain:

```text
weights:current
```

Run `seed-values` first when you need the starting model.

## How It Works

The job:

1. Starts a local PyFlink streaming environment.
2. Reads `weights:current` from Redis on operator startup.
3. Consumes impression and click events from Kafka.
4. Keys all records to one logical learner.
5. Maintains cumulative triplet counters in Flink `ValueState`.
6. Builds single-feature and global evidence from those counters.
7. Every `interval_seconds`, runs the existing weight-update math.
8. Writes accepted models to Redis `weights:current`.
9. Inserts accepted model snapshots into PostgreSQL.
10. Stores the accepted model back into Flink state for the next update.

## Timer Semantics

This job uses a processing-time timer, not an event-time tumbling window.

That means the update cadence is:

```text
first event arrives -> register timer -> every interval_seconds update weights
```

Evidence is cumulative from the running Flink state. It is not limited to only
the last hour unless we later add decay or explicit window expiration.

## Flags

- `--config <path>`: YAML config path. Defaults to
  `configs/run_streaming_weight_update.yaml`.
- `--impression-topic <topic>`: Impression input topic.
- `--click-topic <topic>`: Click input topic.
- `--consumer-group <name>`: Kafka consumer group.
- `--parallelism <int>`: PyFlink parallelism. Defaults to `1`.
- `--checkpoint-interval-ms <int>`: Checkpoint interval. Use `0` to disable.
- `--kafka-connector-jar <path>`: Local Flink Kafka connector jar.
- `--interval-seconds <int>`: Weight-update timer interval.
- `--learning-rate <float>`: Feature-weight learning rate.
- `--evidence-smoothing <float>`: Evidence smoothing for learning rate.
- `--ridge <float>`: Ridge regularization.
- `--max-delta <float>`: Maximum per-run feature delta.
- `--min-feature-impressions <int>`: Feature guard minimum impressions.
- `--max-feature-ci-width <float>`: Feature guard maximum CI width.
- `--snapshot-name-prefix <name>`: Snapshot name prefix.
- `--baseline-update` / `--no-baseline-update`: Enable or disable `w0` update.
- `--baseline-max-ci-width <float>`: Baseline guard maximum CI width.
- `--dry-run` / `--no-dry-run`: Compute without writing Redis/PostgreSQL.

CLI flags override values from the YAML config.

Kafka bootstrap servers come from `KAFKA_BOOTSTRAP_SERVERS`.
Redis comes from `REDIS_URL`.
PostgreSQL comes from `POSTGRES_DSN`.

## Config Fields

- `impression_topic`, `click_topic`: Kafka input topics.
- `consumer_group`: Kafka consumer group.
- `parallelism`: Flink parallelism.
- `checkpoint_interval_ms`: Flink checkpoint interval.
- `kafka_connector_jar`: Local Flink Kafka connector jar path.
- `interval_seconds`: Processing-time update cadence.
- `learning_rate`, `evidence_smoothing`, `ridge`, `max_delta`,
  `min_feature_impressions`, `max_feature_ci_width`,
  `snapshot_name_prefix`: Weight-learning controls.
- `baseline_update`: Enable global baseline updates.
- `baseline_max_ci_width`: Baseline CI guard. The default streaming config is
  intentionally looser than `weight-update` so short local timer intervals can
  produce accepted updates during development.
- `dry_run`: Compute without Redis/PostgreSQL writes.

## Notes

This job can run alongside `realtime-ctr`, but both consume Kafka independently.

This job updates only `weights:current` and model snapshots. It does not write
Redis `ctr:*` bucket keys.

For local debugging, set `interval_seconds` to a small value such as `60`.
