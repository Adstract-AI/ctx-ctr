# Task 002: Flink Periodic Weight Update Job

## Summary

Implement a local PyFlink job that periodically recalibrates feature-family
weights from Redis CTR bucket statistics.

This task updates only the feature weights:

- `w_ad`
- `w_dom`
- `w_ctx`

The global baseline `w0` must stay unchanged.

The job writes the updated current model to Redis and inserts a model snapshot
into PostgreSQL for history and auditability.

## Goal

Create a runnable job:

```bash
python -m ctx_ctr.jobs.run_weight_update
```

The job should:

- read current bucket statistics from Redis `ctr:*` keys
- read the current model from Redis `weights:current`
- aggregate triplet CTR buckets into single-feature buckets
- update feature-family weights without changing `w0`
- re-center each weight family
- write updated weights back to Redis
- insert a PostgreSQL `ctr_model_snapshots` row
- support one-shot mode for local debugging
- support hourly periodic recalibration by default

## Inputs

Read current model weights from Redis:

```text
weights:current
```

Expected model shape:

```json
{
  "snapshot_name": "seed_values_v1",
  "w0": -3.8918202981106265,
  "weights": {
    "w_ad": {},
    "w_dom": {},
    "w_ctx": {}
  },
  "metrics": {
    "baseline_ctr": 0.02,
    "prior_strength": 100.0
  }
}
```

Read bucket statistics from Redis keys matching:

```text
ctr:*
```

Expected bucket payload shape is the same as
`SeedBucketStatistic.redis_payload()`.

The triplet `trusted` flag is not used for feature-weight learning. The job
derives single-feature buckets for ad category, publisher domain, and
conversation category by summing valid triplet impressions/clicks.

## Weight Update Rules

For each single-feature bucket:

```text
m0 = sigmoid(w0 + current_weight)
alpha0 = m0 * prior_strength
beta0 = (1 - m0) * prior_strength
alpha_post = alpha0 + clicks
beta_post = beta0 + impressions - clicks
posterior_ctr = alpha_post / (alpha_post + beta_post)
z_target = logit(posterior_ctr)
delta_star = z_target - w0
eta = learning_rate * impressions / (impressions + evidence_smoothing)
raw_delta = eta * (delta_star - current_weight)
regularized_delta = raw_delta - ridge * current_weight
delta = clip(regularized_delta, -max_delta, max_delta)
new_weight = current_weight + delta
```

Only update single-feature buckets that pass minimum-impression and CI-width
guards.

After every family is updated, re-center each family independently:

```text
weight = weight - mean(family_weights)
```

This keeps `w0` interpretable as the fixed global baseline.

## Unknown Feature Values

If a Redis bucket contains an ad category, publisher domain, or conversation
category missing from `weights:current`, add that feature value to the relevant
family with starting weight:

```text
0.0
```

Then include it in the update and final family centering.

## Outputs

Redis:

- overwrite `weights:current` with the updated model payload
- keep `w0` exactly unchanged
- keep the existing `metrics.baseline_ctr` and `metrics.prior_strength`
- update `snapshot_name` with the new generated snapshot name

PostgreSQL:

- insert one row into `ctr_model_snapshots` for each successful recalibration
- store unchanged `w0`
- store updated `w_ad`, `w_dom`, and `w_ctx`
- store run metrics in the `metrics` JSONB column

Suggested snapshot name format:

```text
{snapshot_name_prefix}_{YYYYMMDD_HHMMSS}
```

Metrics should include:

- input bucket count
- trusted bucket count
- skipped bucket count
- unknown feature count
- max absolute weight delta before centering
- learning rate
- evidence smoothing
- ridge
- max delta
- dry-run flag

Do not update Redis bucket priors in this task.

## Job Flags

Add these flags to `ctx_ctr.jobs.run_weight_update`:

- `--redis-url`, default from runtime settings
- `--postgres-dsn`, default from runtime settings
- `--interval-seconds`, default `3600`
- `--once`, run one recalibration and exit
- `--learning-rate`, default `0.25`
- `--evidence-smoothing`, default `1000`
- `--ridge`, default `0.01`
- `--max-delta`, default `0.25`
- `--min-trusted-buckets`, default `1`
- `--snapshot-name-prefix`, default `flink_weight_update`
- `--dry-run`, compute and log updates without writing Redis or PostgreSQL

## Runtime Behavior

Default behavior:

- run recalibration immediately
- sleep for `--interval-seconds`
- repeat until stopped

One-shot mode:

```bash
python -m ctx_ctr.jobs.run_weight_update --once
```

Dry-run mode:

```bash
python -m ctx_ctr.jobs.run_weight_update --once --dry-run
```

If fewer than `--min-trusted-buckets` are available:

- log a clear warning
- do not write Redis
- do not insert a PostgreSQL snapshot
- continue sleeping in periodic mode
- exit successfully in `--once` mode with a clear summary

## Architecture Notes

Keep the existing service pattern.

The job module should stay thin:

- parse CLI flags
- load settings
- configure logging
- construct dependencies
- invoke the weight-update service
- handle periodic scheduling
- print a clear green/red final summary for one-shot runs

Weight-learning logic should live in a reusable service/helper module.

Redis and PostgreSQL integration should stay behind adapter-style boundaries.

Do not use Spark for this task.

Do not update `w0` in this task.

## Logging

Use the project logger.

Log important business steps at `INFO`:

- job configuration at startup
- bucket scan summary
- trusted/skipped bucket counts
- whether the job writes or dry-runs
- snapshot name written
- max absolute weight delta

Keep per-bucket and per-feature details at `DEBUG`.

## Documentation

Add:

```text
docs/desc/run_weight_update.md
```

Update:

```text
docs/desc/README.md
```

The docs page should explain:

- what the job does
- when to use it
- all flags
- required services
- Redis inputs and outputs
- PostgreSQL snapshot output
- why `w0` is not updated
- example commands
- safety notes

## Manual Verification

Do not add automated tests unless explicitly requested.

Manual acceptance flow:

1. Start must-have Docker services.
2. Run:

   ```bash
   python -m ctx_ctr.jobs.seed_values
   ```

3. Confirm Redis has:

   ```text
   weights:current
   ctr:* bucket keys
   ```

4. Run dry-run mode:

   ```bash
   python -m ctx_ctr.jobs.run_weight_update --once --dry-run
   ```

5. Confirm the summary reports:

   - input bucket count
   - trusted bucket count
   - skipped bucket count
   - proposed max absolute weight delta

6. Run one real recalibration:

   ```bash
   python -m ctx_ctr.jobs.run_weight_update --once
   ```

7. Confirm Redis `weights:current` changed.
8. Confirm `w0` stayed exactly the same.
9. Confirm each family remains centered.
10. Confirm one `ctr_model_snapshots` row was inserted.
11. Run:

   ```bash
   python -m ruff check src
   python -m mypy src
   ```

## Assumptions

- This is the Flink-based replacement for the earlier Spark weight
  recalibration idea.
- Spark stays in the project but is not used by this task.
- Redis bucket statistics are the v1 source of learning data.
- PostgreSQL is used only for model snapshot history in this task.
- Baseline update for `w0` is intentionally out of scope.
- Redis bucket prior refresh/reset semantics are intentionally out of scope.
