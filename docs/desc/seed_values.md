# seed_values

Seeds deterministic starting CTR values into PostgreSQL and Redis.

## Command

```bash
python -m ctx_ctr.jobs.seed_values
```

Short command:

```bash
seed-values
```

Dry-run:

```bash
python -m ctx_ctr.jobs.seed_values --dry-run
```

Default config:

```text
configs/seed_values.yaml
```

## Purpose

Use this job after the infrastructure is running and after `reset_values` if
you want a clean starting point. It creates the initial state that later Flink
jobs and API-style readers can consume.

## What It Seeds

- CTR bucket statistics for all seeded bucket combinations.
- One current model snapshot with baseline and family weights.
- One seed run summary.
- Redis feature-cache entries for all bucket statistics.
- Redis current-weight entry.

The buckets are generated from:

- ad categories: `finance`, `travel`, `education`, `health`, `gaming`
- publisher domains: `news.example`, `tech.example`, `lifestyle.example`, `sports.example`
- conversation categories: `personal_finance`, `trip_planning`, `online_learning`, `wellness`, `gaming`

This produces 100 bucket combinations.

## Flags

- `--config <path>`: YAML config path. Defaults to `configs/seed_values.yaml`.
- `--dry-run`: Builds and validates the seed dataset, prints the summary, and
  does not connect to PostgreSQL or Redis.

CLI flags override values from the YAML config.

## Config Fields

- `dry_run`: Same behavior as `--dry-run`.

## How It Works

The job builds a deterministic seed dataset in Python using Pydantic models.
For every bucket it computes:

- impressions and clicks
- Beta prior values
- Beta posterior values
- CTR
- variance
- confidence interval
- trusted flag

The model weights use a baseline CTR of `0.02` plus centered family weights.

## Writes

PostgreSQL:

- `ctr_bucket_statistics`
- `ctr_model_snapshots`
- `ctr_experiment_results`

Redis:

- `ctr:{ad_category}:{publisher_domain}:{conversation_category}`
- `weights:current`

## Notes

This job does not reset existing values first. Run `reset_values` before this
job when you need a clean state.

This job does not touch Kafka.
