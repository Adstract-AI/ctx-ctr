# run_weight_update

Periodically recalibrates feature-family weights and, by default, the global
baseline `w0` from Redis CTR bucket statistics.

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

It reads the current model from Redis, learns feature-family weights from
trusted bucket statistics, updates `w_ad`, `w_dom`, and `w_ctx`, and updates the
global baseline `w0` when baseline evidence guards pass. Accepted runs write one
coherent current model back to Redis and store one model snapshot in PostgreSQL.

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

The accepted Redis model payload includes:

- updated `w0`
- updated `w_ad`, `w_dom`, and `w_ctx`
- updated `metrics.baseline_ctr`
- `metrics.prior_strength` unchanged

## PostgreSQL Output

- inserts one row into `ctr_model_snapshots` for each successful non-dry-run
  recalibration

The inserted row stores:

- updated `w0`
- updated `w_ad`, `w_dom`, `w_ctx`
- run metrics in the JSONB `metrics` column

## Learning Rules

Feature-weight learning:

- use only buckets where `trusted == true`
- clip bucket CTR before `logit`
- compute residual targets per family value
- aggregate targets using bucket impressions as evidence weight
- apply evidence-scaled learning rate, ridge regularization, and max-delta
  clipping
- re-center each family so its mean stays zero

Baseline learning:

- use all valid buckets where `impressions > 0` and `clicks <= impressions`
- aggregate global impressions and clicks
- combine the aggregate evidence with the current baseline prior
- update `w0` only when minimum-impression and confidence-interval guards pass
- skip the whole model write when baseline update is enabled but guards fail

## Disabling Baseline Updates

Baseline updates are enabled by default. To keep Task 2 behavior and update only
feature-family weights, disable baseline updates:

```bash
python -m ctx_ctr.jobs.run_weight_update --once --no-baseline-update
```

When disabled, `w0` and `metrics.baseline_ctr` remain unchanged, while accepted
feature-family updates may still write Redis and PostgreSQL snapshots.

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
- `--baseline-update` / `--no-baseline-update`: enable or disable global
  baseline updates. Default `true`.
- `--baseline-learning-rate`: default `0.10`
- `--baseline-evidence-smoothing`: default `5000`
- `--baseline-max-delta`: default `0.10`
- `--baseline-min-impressions`: default `1000`
- `--baseline-max-ci-width`: default `0.02`
- `--dry-run`: compute updates without writing Redis or PostgreSQL

CLI flags override values from the YAML config.

## Config Fields

- `redis_url`, `postgres_dsn`: Runtime storage connections.
- `interval_seconds`: Periodic sleep interval.
- `once`: Run one recalibration and exit.
- `learning_rate`, `evidence_smoothing`, `ridge`, `max_delta`,
  `min_trusted_buckets`, `snapshot_name_prefix`: Weight-learning controls.
- `baseline_update`: Enable global baseline updates.
- `baseline_learning_rate`, `baseline_evidence_smoothing`,
  `baseline_max_delta`, `baseline_min_impressions`,
  `baseline_max_ci_width`: Baseline-learning controls and guards.
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
- if baseline guards fail while baseline updates are enabled, the whole model
  write is skipped so no partial feature-only snapshot is published
- this job does not update Redis bucket priors
- this job does not use Spark
