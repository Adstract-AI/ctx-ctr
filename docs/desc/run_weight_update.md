# run_weight_update

Periodically recalibrates Task 2 feature-family weights from Redis CTR bucket
statistics.

## Command

One-shot dry-run:

```bash
python -m ctx_ctr.jobs.run_weight_update --once --dry-run
```

Short command:

```bash
weight-update --once --dry-run
```

One real recalibration:

```bash
python -m ctx_ctr.jobs.run_weight_update --once
```

Periodic mode:

```bash
python -m ctx_ctr.jobs.run_weight_update --interval-seconds 3600
```

Default config:

```text
configs/run_weight_update.yaml
```

## Purpose

Use this job after `seed_values` or after the Task 1 CTR updater has populated
Redis bucket statistics.

It reads the current model from Redis, learns only from trusted bucket
statistics, updates `w_ad`, `w_dom`, and `w_ctx`, keeps `w0` unchanged, writes
the updated current model back to Redis, and stores a model snapshot in
PostgreSQL.

## Required Services

- Redis
- PostgreSQL for non-dry-run mode

Kafka, Spark, and Task 1 runtime processing are not used directly by this job.

## Redis Inputs

- `weights:current`
- `ctr:*`

Bucket payloads must stay compatible with
`SeedBucketStatistic.redis_payload()`.

## Redis Output

- overwrites `weights:current`

The Redis model payload keeps:

- `w0` unchanged
- `metrics.baseline_ctr` unchanged
- `metrics.prior_strength` unchanged

## PostgreSQL Output

- inserts one row into `ctr_model_snapshots` for each successful non-dry-run
  recalibration

The inserted row stores:

- unchanged `w0`
- updated `w_ad`, `w_dom`, `w_ctx`
- run metrics in the JSONB `metrics` column

## Learning Rules

- use only buckets where `trusted == true`
- clip bucket CTR before `logit`
- compute residual targets per family value
- aggregate targets using bucket impressions as evidence weight
- apply evidence-scaled learning rate, ridge regularization, and max-delta
  clipping
- re-center each family so its mean stays zero

## Why `w0` Is Not Updated

Task 2 updates only feature-family weights.

Global baseline learning for `w0` is intentionally out of scope here and
belongs to Task 3.

## Flags

- `--config <path>`: YAML config path. Defaults to
  `configs/run_weight_update.yaml`.
- `--redis-url`: Redis connection URL
- `--postgres-dsn`: PostgreSQL DSN for snapshot history
- `--interval-seconds`: periodic sleep interval, default `3600`
- `--once`: run one recalibration and exit
- `--learning-rate`: default `0.25`
- `--evidence-smoothing`: default `1000`
- `--ridge`: default `0.01`
- `--max-delta`: default `0.25`
- `--min-trusted-buckets`: default `1`
- `--snapshot-name-prefix`: default `flink_weight_update`
- `--dry-run`: compute updates without writing Redis or PostgreSQL

CLI flags override values from the YAML config.

## Config Fields

- `redis_url`, `postgres_dsn`: Runtime storage connections.
- `interval_seconds`: Periodic sleep interval.
- `once`: Run one recalibration and exit.
- `learning_rate`, `evidence_smoothing`, `ridge`, `max_delta`,
  `min_trusted_buckets`, `snapshot_name_prefix`: Weight-learning controls.
- `dry_run`: Compute without writing Redis or PostgreSQL.

## Example Verification

Read the current Redis model:

```bash
docker compose -f docker-compose.yml exec redis redis-cli GET weights:current
```

Inspect recent PostgreSQL snapshots:

```bash
docker compose -f docker-compose.yml exec postgres \
  psql -U ctx_ctr -d ctx_ctr \
  -c "SELECT id, snapshot_name, created_at FROM ctr_model_snapshots ORDER BY id DESC LIMIT 5;"
```

## Safety Notes

- dry-run mode still reads Redis but does not write Redis or PostgreSQL
- if fewer than `--min-trusted-buckets` are available, the run is skipped
- this job does not update Redis bucket priors
- this job does not update `w0`
- this job does not use Spark
