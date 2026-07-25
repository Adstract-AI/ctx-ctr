# reset_values

Clears seeded PostgreSQL and Redis state.

## Command

```bash
python -m ctx_ctr.jobs.reset_values
```

Short command:

```bash
reset-values
```

Dry-run:

```bash
python -m ctx_ctr.jobs.reset_values --dry-run
```

Default config:

```text
configs/reset_values.yaml
```

## Purpose

Use this job when you want to return the project to a clean seedable state
without recreating Docker volumes.

Typical flow:

```bash
python -m ctx_ctr.jobs.reset_values
python -m ctx_ctr.jobs.seed_values
```

## Flags

- `--config <path>`: YAML config path. Defaults to `configs/reset_values.yaml`.
- `--dry-run`: Shows what would be reset and does not connect to PostgreSQL or
  Redis.

CLI flags override values from the YAML config.

## Config Fields

- `dry_run`: Same behavior as `--dry-run`.

## How It Works

The job calls the seed reset service, which resets only seed-owned state.

PostgreSQL tables are truncated with identity reset:

- `ctr_bucket_statistics`
- `ctr_model_snapshots`
- `ctr_experiment_results`

Redis keys are removed by project-owned prefixes:

- `ctr:*`
- `weights:*`

## Writes

PostgreSQL:

- deletes rows from the seed-owned tables listed above

Redis:

- deletes project-owned CTR and weight keys

## Notes

This job does not touch Kafka topics.

This job does not delete Docker volumes or database schema.

Raw event history in `ctr_events` is not reset by this job.
