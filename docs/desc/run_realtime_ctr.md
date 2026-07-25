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
- `--consumer-group <name>`: Kafka consumer group. Defaults to
  `ctx-ctr-flink-realtime`.
- `--starting-offsets <latest|earliest>`: Offset policy used when starting the
  source. Defaults to `latest`; backlog experiments use `earliest`.
- `--parallelism <int>`: PyFlink parallelism. Defaults to `2` in the supplied
  YAML config.
- `--checkpoint-interval-ms <int>`: Checkpoint interval. Defaults to `10000`.
  Use `0` to disable checkpointing.
- `--kafka-connector-jar <path>`: Local path to the Flink Kafka connector jar.
  Defaults to the project `jars/` folder through the YAML config.
- `--log-every <int>`: Log progress every N valid events. Use `0` to disable
  progress logs.
- `--metrics-flush-interval-ms <int>`: Flush a partial processor metrics window
  after activity. Defaults to `1000`; use `0` to disable.
- `--redis-flush-mode <periodic|per_event>`: Redis persistence strategy.
  Defaults to `periodic`. Use `per_event` to synchronously write every accepted
  bucket update when immediate external visibility is required or when measuring
  the cost of synchronous writes.
- `--redis-flush-interval-ms <int>`: Maximum processing-time delay before a
  dirty bucket is written to Redis in `periodic` mode. Defaults to `500`.
- `--redis-flush-max-updates <int>`: Flush a bucket after this many updates even
  if its timer has not fired in `periodic` mode. Defaults to `100`.
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
- `consumer_group`: Kafka consumer group.
- `starting_offsets`: `latest` for newly arriving traffic or `earliest` to
  consume records already present in Kafka.
- `parallelism`: PyFlink parallelism.
- `checkpoint_interval_ms`: Flink checkpoint interval. Use `0` to disable.
- `kafka_connector_jar`: Local Flink Kafka connector jar path. Relative paths
  are resolved from the project root. Defaults to
  `jars/flink-sql-connector-kafka-3.2.0-1.19.jar`.
- `log_every`: Progress logging interval.
- `metrics_flush_interval_ms`: Processing-time interval used to record partial
  metrics batches that do not reach `log_every`.
- `redis_flush_mode`: `periodic` coalesces dirty updates using Flink timers;
  `per_event` writes each accepted bucket update to Redis synchronously.
- `redis_flush_interval_ms`: Maximum healthy-runtime Redis staleness for a dirty
  bucket in `periodic` mode.
- `redis_flush_max_updates`: Maximum bucket updates coalesced before an early
  Redis flush in `periodic` mode.
- `trust_z_score`: Z-score used for bucket confidence intervals.
- `trust_min_impressions`: Minimum bucket impressions required for `trusted`.
- `trust_max_variance`: Maximum bucket posterior variance allowed for
  `trusted`.
- `trust_max_ci_width`: Maximum bucket confidence-interval width allowed for
  `trusted`.

Kafka bootstrap servers come from `KAFKA_BOOTSTRAP_SERVERS` in the environment.
Redis comes from `REDIS_URL` in the environment.

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

Updated bucket state is written to:

```text
ctr:{ad_category}:{publisher_domain}:{conversation_category}
```

The payload is compatible with the seeded bucket-statistic shape. Flink keyed
state is updated for every valid event. In `periodic` mode, Redis receives the
latest dirty snapshot after `redis_flush_interval_ms` or
`redis_flush_max_updates`, whichever happens first. In `per_event` mode, the
accepted update is written synchronously before the next event is handled by
that operator subtask.

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
7. Persists accepted updates according to `redis_flush_mode`: immediately for
   `per_event`, or by marking the bucket dirty for `periodic`.
8. In `periodic` mode, flushes the latest dirty bucket snapshot to Redis on a
   keyed timer or update threshold.
9. Emits invalid events to the dead-letter Kafka topic.

The trust guardrails only control the bucket `trusted` flag. They do not reject
valid impression/click events and do not block Redis flushes.

Processor metrics include `redis_flushes`, `redis_updates_flushed`, and
`redis_updates_coalesced`. For a completed backlog,
`redis_updates_flushed == valid_events`; fewer `redis_flushes` means more
per-event writes were removed from the hot path. In `per_event` mode,
`redis_flushes == redis_updates_flushed` and `redis_updates_coalesced == 0`.

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

For normal local streaming, start this job before `produce-events`. The default
`latest` offset policy consumes events published after the Kafka source starts.
Use `earliest` only when the job must consume an existing backlog.

## Flink Dashboard

The short command runs the job in a local PyFlink runtime:

```bash
realtime-ctr
```

To inspect the topology in the Docker Flink dashboard, start the processing
cluster and submit the same Python job through the cluster's Flink CLI:

```bash
docker compose \
  -f docker-compose.yml \
  -f compose.processing-clusters.yml \
  up -d --build

docker compose \
  -f docker-compose.yml \
  -f compose.processing-clusters.yml \
  exec flink-jobmanager \
  flink run -d \
  --python /opt/ctx-ctr/src/ctx_ctr/jobs/run_realtime_ctr.py \
  --config /opt/ctx-ctr/configs/run_realtime_ctr.yaml \
  --kafka-connector-jar /opt/ctx-ctr/jars/flink-sql-connector-kafka-3.2.0-1.19.jar \
  --parallelism 2
```

Open `http://localhost:8081` and select the running
`ctx-ctr-realtime-ctr` job. The graph should report parallelism `2` for the
parallel source and keyed processing operators. Operator chaining can combine
the keyed process and dead-letter sink into one displayed vertex.

See the captured [realtime Flink topology](../flink_realtime_ctr_topology.md)
for an annotated example of this operator graph.
