# Task 003: Add Global Baseline `w0` Update

## Summary

Upgrade the planned Flink weight-update flow so `run_weight_update`
recalibrates both feature-family weights and the global baseline `w0` in one
coherent model snapshot.

This task extends Task 002. Do not create a separate baseline-update job.

## Goal

Extend the runnable job:

```bash
python -m ctx_ctr.jobs.run_weight_update
```

After this task, the job should:

- read current bucket statistics from Redis `ctr:*` keys
- read the current model from Redis `weights:current`
- update feature-family weights using Task 002 behavior
- update the global baseline `w0` when evidence guards pass
- write one coherent model payload to Redis
- insert one coherent model snapshot into PostgreSQL
- support disabling baseline updates from the CLI

## Inputs

Read current model weights from Redis:

```text
weights:current
```

Read bucket statistics from Redis keys matching:

```text
ctr:*
```

For feature-family updates, keep Task 002 behavior:

- aggregate triplet buckets into single-feature buckets
- use single-feature minimum-impression and CI-width guards
- update `w_ad`, `w_dom`, and `w_ctx`
- re-center each family

For global baseline updates, use all valid buckets.

A bucket is valid for the baseline when:

```text
impressions > 0
clicks <= impressions
```

Invalid buckets are skipped and counted in run metrics.

## Baseline Update Rules

Aggregate valid bucket evidence:

```text
global_impressions = sum(bucket.impressions)
global_clicks = sum(bucket.clicks)
```

Use the current model metrics:

```text
prior_strength = weights:current.metrics.global_prior_strength
old_baseline_ctr = sigmoid(old_w0)
```

Build the global posterior:

```text
alpha_prior = old_baseline_ctr * prior_strength
beta_prior = (1 - old_baseline_ctr) * prior_strength
alpha_posterior = alpha_prior + global_clicks
beta_posterior = beta_prior + global_impressions - global_clicks
posterior_ctr = alpha_posterior / (alpha_posterior + beta_posterior)
```

Compute uncertainty:

```text
variance = beta_variance(alpha_posterior, beta_posterior)
ci_low, ci_high = clipped_confidence_interval(posterior_ctr, variance, 1.645)
ci_width = ci_high - ci_low
```

Baseline guards:

```text
ci_width <= baseline_max_ci_width
```

If guards fail:

- skip the baseline update
- do not write Redis
- do not insert a PostgreSQL snapshot for that run
- log the skipped reason clearly

If guards pass:

```text
new_w0 = logit(clipped_posterior_ctr)
delta = new_w0 - old_w0
```

Use a safe logit clip before calling `logit`.

## Combined Model Update

The accepted model update should include:

- updated `w0`
- updated `w_ad`
- updated `w_dom`
- updated `w_ctx`

Feature-family centering must still happen after family updates.

Updating `w0` must not break family centering.

The output model should update:

```text
metrics.baseline_ctr = sigmoid(new_w0)
```

Keep:

```text
metrics.triplet_prior_strength
metrics.global_prior_strength
metrics.family_prior_strengths
```

from the previous model.

## Outputs

Redis:

- overwrite `weights:current` with the accepted combined model
- include updated `w0`
- include updated centered family weights
- update `snapshot_name`
- update `metrics.baseline_ctr`

PostgreSQL:

- insert one row into `ctr_model_snapshots` only when a model update is accepted
- store updated `w0`
- store updated family weights
- store run metrics in the `metrics` JSONB column

Metrics should include:

- old `w0`
- new `w0`
- baseline delta
- baseline update applied flag
- aggregate impressions
- aggregate clicks
- aggregate observed CTR
- posterior baseline CTR
- confidence interval low/high/width
- baseline guard pass/fail reason
- valid baseline bucket count
- invalid baseline bucket count
- Task 002 feature-weight metrics

## Job Flags

Add these flags to `ctx_ctr.jobs.run_weight_update`:

- `--update-baseline`, default enabled after this task
- `--disable-baseline-update`, opt out and keep Task 002 baseline behavior
- `--baseline-learning-rate`, default `0.10`
- `--baseline-evidence-smoothing`, default `5000`
- `--baseline-max-delta`, default `0.10`
- `--baseline-min-impressions`, default `1000`
- `--baseline-max-ci-width`, default `0.02`

When `--disable-baseline-update` is passed:

- keep `w0` unchanged
- keep `metrics.baseline_ctr` unchanged
- still allow Task 002 feature-family weight updates
- record in metrics that baseline update was disabled

## Runtime Behavior

Default behavior after this task:

- run feature-family update
- run baseline update
- write one combined model snapshot if the update is accepted

Dry-run behavior:

- compute feature-family and baseline updates
- log proposed `w0` delta
- log whether baseline guards pass
- do not write Redis
- do not insert PostgreSQL snapshot

If the baseline guards fail and baseline update is enabled:

- skip the whole model write for that run
- do not publish a partial family-only model
- continue sleeping in periodic mode
- exit successfully in `--once` mode with a clear skipped summary

If baseline update is disabled:

- fall back to Task 002 acceptance behavior
- feature-family updates may still write a model snapshot

## Architecture Notes

Keep the existing service pattern.

The job module should stay thin:

- parse new flags
- load settings
- configure logging
- construct dependencies
- invoke the weight-update service
- handle periodic scheduling
- print a clear green/red final summary for one-shot runs

Baseline learning should live in the same service/helper area as Task 002
weight learning.

Redis and PostgreSQL integration should stay behind adapter-style boundaries.

Do not use Spark for this task.

## Logging

Use the project logger.

Log important business steps at `INFO`:

- whether baseline update is enabled
- aggregate impressions and clicks
- aggregate observed CTR
- baseline confidence interval width
- proposed and accepted `w0` delta
- skipped guard reason
- snapshot name written

Keep per-bucket details at `DEBUG`.

## Documentation

Update:

```text
docs/desc/run_weight_update.md
```

If the docs page does not exist yet, create it.

The docs page should explain:

- that the job updates both family weights and `w0`
- how to disable baseline updates
- all baseline-related flags
- baseline evidence requirements
- Redis and PostgreSQL outputs
- why weak baseline evidence skips the model write
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

3. Run dry-run mode:

   ```bash
   python -m ctx_ctr.jobs.run_weight_update --once --dry-run
   ```

4. Confirm the summary reports:

   - baseline aggregate impressions
   - baseline aggregate clicks
   - confidence interval width
   - proposed `w0` delta
   - whether guards passed

5. Run one real update:

   ```bash
   python -m ctx_ctr.jobs.run_weight_update --once
   ```

6. Confirm Redis `weights:current.w0` changes only if guards pass.
7. Confirm Redis `weights:current.metrics.baseline_ctr` equals
   `sigmoid(weights:current.w0)`.
8. Confirm feature families remain centered.
9. Confirm one PostgreSQL snapshot is inserted only for accepted updates.
10. Run with baseline disabled:

   ```bash
   python -m ctx_ctr.jobs.run_weight_update --once --disable-baseline-update
   ```

11. Confirm `w0` stays unchanged in disabled mode.
12. Run:

   ```bash
   python -m ruff check src
   python -m mypy src
   ```

## Assumptions

- This is the final model-learning task after Tasks 001 and 002.
- Redis bucket statistics are the v1 source of global baseline evidence.
- Baseline and feature weights should be written in the same model snapshot.
- Weak aggregate baseline evidence should skip the run instead of writing a
  noisy model.
- Spark stays in the project but is not used by this task.
