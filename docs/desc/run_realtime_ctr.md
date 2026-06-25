# run_realtime_ctr

Runs the local PyFlink realtime CTR updater.

## Command

```bash
python -m ctx_ctr.jobs.run_realtime_ctr
```

Short command:

```bash
realtime-ctr
```

Example with slower progress logs:

```bash
python -m ctx_ctr.jobs.run_realtime_ctr --log-every 500
```

Default config:

```text
configs/run_realtime_ctr.yaml
```

## Purpose

Use this job after `seed_values` and before producing simulated traffic.

The job consumes impression and click events from Kafka, updates Bayesian CTR
statistics per contextual bucket, writes valid bucket updates to Redis, and
sends invalid click events to the dead-letter topic.

## Required Services

The must-have Docker services should be running:

- Kafka
- Redis

Redis must already contain:

- `weights:current`

The easiest way to create the starting state is:

```bash
python -m ctx_ctr.jobs.seed_values
```

## Flags

- `--config <path>`: YAML config path. Defaults to
  `configs/run_realtime_ctr.yaml`.
- `--impression-topic <topic>`: Impression input topic. Defaults to
  `ctr.impressions`.
- `--click-topic <topic>`: Click input topic. Defaults to `ctr.clicks`.
- `--dead-letter-topic <topic>`: Output topic for rejected records. Defaults to
  `ctr.dead-letter`.
- `--bootstrap-servers <host:port>`: Kafka bootstrap servers. Defaults to the
  runtime setting.
- `--redis-url <url>`: Redis URL. Defaults to the runtime setting.
- `--consumer-group <name>`: Kafka consumer group. Defaults to
  `ctx-ctr-flink-realtime`.
- `--parallelism <int>`: PyFlink parallelism. Defaults to `1`.
- `--checkpoint-interval-ms <int>`: Checkpoint interval. Defaults to `10000`.
  Use `0` to disable checkpointing.
- `--kafka-connector-jar <path>`: Local path to the Flink Kafka connector jar.
  Defaults to the project `jars/` folder through the YAML config.
- `--log-every <int>`: Log progress every N valid events. Use `0` to disable
  progress logs.
- `--trust-z-score <float>`: Z-score used for bucket confidence intervals.
- `--trust-min-impressions <int>`: Minimum bucket impressions required for the
  bucket `trusted` flag.
- `--trust-max-variance <float>`: Maximum bucket posterior variance allowed for
  the bucket `trusted` flag.
- `--trust-max-ci-width <float>`: Maximum confidence-interval width allowed for
  the bucket `trusted` flag.

CLI flags override values from the YAML config.

## Config Fields

- `impression_topic`, `click_topic`, `dead_letter_topic`: Kafka topics.
- `bootstrap_servers`: Kafka bootstrap servers.
- `redis_url`: Redis URL.
- `consumer_group`: Kafka consumer group.
- `parallelism`: PyFlink parallelism.
- `checkpoint_interval_ms`: Flink checkpoint interval. Use `0` to disable.
- `kafka_connector_jar`: Local Flink Kafka connector jar path. Relative paths
  are resolved from the project root. Defaults to
  `jars/flink-sql-connector-kafka-3.2.0-1.19.jar`.
- `log_every`: Progress logging interval.
- `trust_z_score`: Z-score used for bucket confidence intervals.
- `trust_min_impressions`: Minimum bucket impressions required for `trusted`.
- `trust_max_variance`: Maximum bucket posterior variance allowed for
  `trusted`.
- `trust_max_ci_width`: Maximum bucket confidence-interval width allowed for
  `trusted`.

## Kafka Input

The job consumes the producer default topics:

- `ctr.impressions`
- `ctr.clicks`

Events must match the `CtrEvent` payload shape used by `produce_events`.

## Redis Input

At startup, each operator loads:

```text
weights:current
```

For each bucket, the job first tries to load existing state from:

```text
ctr:{ad_category}:{publisher_domain}:{conversation_category}
```

If the bucket is missing, it initializes an empty bucket from the current model
weights.

## Redis Output

Every valid event writes an updated bucket payload to:

```text
ctr:{ad_category}:{publisher_domain}:{conversation_category}
```

The payload is compatible with the seeded bucket-statistic shape.

## Dead-Letter Output

Clicks are rejected when accepting the click would make:

```text
clicks > impressions
```

Rejected records are written to:

```text
ctr.dead-letter
```

Invalid JSON or invalid event payloads are also dead-lettered.

## How It Works

The job:

1. Starts a local PyFlink streaming environment.
2. Reads impression and click events from Kafka.
3. Keys events by contextual CTR bucket.
4. Keeps per-bucket state in Flink keyed state.
5. Loads initial bucket state from Redis when needed.
6. Applies the Bayesian CTR update.
7. Writes accepted bucket updates to Redis.
8. Emits invalid events to the dead-letter Kafka topic.

The trust guardrails only control the bucket `trusted` flag. They do not reject
valid impression/click events and do not block Redis writes.

## Notes

This job does not write to PostgreSQL.

This job does not update model weights.

Local PyFlink needs a Flink Kafka connector jar. The default config expects it
at:

```text
jars/flink-sql-connector-kafka-3.2.0-1.19.jar
```

See `docs/development_setup.md` for the download command. Use
`--kafka-connector-jar` only when using a different connector location.

For local development, start this job before running `produce_events` so the
Kafka consumer begins from the latest offsets and receives newly produced
events.
