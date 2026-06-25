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
- `--baseline-prior-mean`: Override cold-start global CTR prior mean.
- `--triplet-prior-strength`: Override realtime triplet-bucket prior strength.
- `--global-prior-strength`: Override global baseline prior strength.
- `--ad-prior-strength`: Override ad-category family prior strength.
- `--domain-prior-strength`: Override publisher-domain family prior strength.
- `--context-prior-strength`: Override conversation-context family prior
  strength.

CLI flags override values from the YAML config.

## Config Fields

- `dry_run`: Same behavior as `--dry-run`.
- `baseline_prior_mean`: Cold-start global CTR prior mean `m0`.
- `triplet_prior_strength`: Prior strength for realtime triplet CTR buckets.
- `global_prior_strength`: Prior strength for global baseline `w0` updates.
- `ad_prior_strength`: Prior strength for ad-category single-feature buckets.
- `domain_prior_strength`: Prior strength for publisher-domain single-feature
  buckets.
- `context_prior_strength`: Prior strength for conversation-context
  single-feature buckets.

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

The model weights use `baseline_prior_mean` plus centered family weights.
The current model snapshot also stores the global, triplet, and family-specific
prior strengths used later by realtime CTR and weight-update jobs.

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
