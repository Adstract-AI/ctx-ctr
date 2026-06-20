# Task 001: Flink Real-Time CTR Update Job

## Summary

Implement a local PyFlink streaming job that consumes simulated CTR impression
and click events from Kafka, updates Bayesian CTR bucket statistics in real
time, writes updated bucket payloads to Redis, and sends invalid click events to
the dead-letter topic.

The first version targets local development from the `big-data` Conda
environment. Docker Flink cluster submission and PostgreSQL persistence are out
of scope for this task.

## Goal

Create a runnable job:

```bash
python -m ctx_ctr.jobs.run_realtime_ctr
```

The job should:

- consume impressions from `ctr.impressions`
- consume clicks from `ctr.clicks`
- key events by CTR bucket
- maintain per-bucket CTR state
- update Redis after each valid event
- dead-letter invalid clicks
- use the existing project logger and final job-summary style

## Input Streams

Use the current producer default topic contract.

Kafka topics:

- `ctr.impressions`
- `ctr.clicks`

Event payloads match `CtrEvent.json_payload()`:

```json
{
  "event_id": "imp-...",
  "event_type": "impression",
  "ad_category": "finance",
  "publisher_domain": "news.example",
  "conversation_category": "personal_finance",
  "occurred_at": "2026-01-01T00:00:00+00:00",
  "impression_event_id": null
}
```

Clicks have:

```json
{
  "event_type": "click",
  "impression_event_id": "imp-..."
}
```

The bucket key is:

```text
ad_category:publisher_domain:conversation_category
```

## State And Redis Bootstrap

Maintain keyed Flink state per CTR bucket:

- `impressions`
- `clicks`
- `alpha_prior`
- `beta_prior`
- `alpha_posterior`
- `beta_posterior`
- `ctr`
- `variance`
- `ci_low`
- `ci_high`
- `trusted`

Bootstrap each bucket in this order:

1. Read existing bucket state from Redis:

   ```text
   ctr:{ad_category}:{publisher_domain}:{conversation_category}
   ```

2. If the bucket key does not exist, read current model weights from:

   ```text
   weights:current
   ```

3. Build an empty bucket from the weights using:

   ```text
   prior_mean = sigmoid(w0 + w_ad + w_dom + w_ctx)
   alpha_prior = prior_mean * prior_strength
   beta_prior = (1 - prior_mean) * prior_strength
   impressions = 0
   clicks = 0
   ```

`prior_strength` comes from `weights:current.metrics.prior_strength`.

## CTR Update Rules

For an impression:

```text
impressions += 1
```

For a click:

```text
if clicks + 1 <= impressions:
    clicks += 1
else:
    send event to ctr.dead-letter
    do not update bucket state
```

After every valid event:

```text
alpha_posterior = alpha_prior + clicks
beta_posterior = beta_prior + impressions - clicks
ctr = alpha_posterior / (alpha_posterior + beta_posterior)
variance = beta_variance(alpha_posterior, beta_posterior)
ci_low, ci_high = clipped_confidence_interval(ctr, variance, 1.96)
trusted = (
    impressions >= 100
    and variance <= 0.0005
    and (ci_high - ci_low) <= 0.05
)
```

Reuse the existing math helpers from `ctx_ctr.services.ctr_math`.

## Redis Output

Write every valid update to Redis using the same shape as seeded bucket
statistics.

Key:

```text
ctr:{ad_category}:{publisher_domain}:{conversation_category}
```

Payload fields:

- `ad_category`
- `publisher_domain`
- `conversation_category`
- `impressions`
- `clicks`
- `alpha_prior`
- `beta_prior`
- `alpha_posterior`
- `beta_posterior`
- `ctr`
- `variance`
- `ci_low`
- `ci_high`
- `trusted`

The Redis payload must remain compatible with the current seeded
`SeedBucketStatistic.redis_payload()` format.

## Dead-Letter Output

Invalid click events go to:

```text
ctr.dead-letter
```

A click is invalid in v1 when accepting it would make:

```text
clicks > impressions
```

The dead-letter payload should include the original event fields and a simple
reason, for example:

```json
{
  "reason": "clicks_would_exceed_impressions",
  "event": {
    "...": "..."
  }
}
```

## Job Flags

Add these flags to `ctx_ctr.jobs.run_realtime_ctr`:

- `--impression-topic`, default `ctr.impressions`
- `--click-topic`, default `ctr.clicks`
- `--dead-letter-topic`, default `ctr.dead-letter`
- `--bootstrap-servers`, default from runtime settings
- `--redis-url`, default from runtime settings
- `--consumer-group`, default `ctx-ctr-flink-realtime`
- `--parallelism`, default `1`
- `--checkpoint-interval-ms`, default `10000`
- `--log-every`, default `100`

`--log-every 0` disables progress logs.

## Architecture Notes

Keep the existing service pattern.

The job module should stay thin:

- parse CLI flags
- load settings
- configure logging
- construct Flink runtime dependencies
- start execution
- print a clear final failure message if startup fails

CTR update logic should live outside the job entrypoint in a reusable service or
helper module.

Redis and Kafka integration details should stay behind adapter-style boundaries
where practical.

Do not use Spark for this task.

## Logging

Use the project logger.

Log important business steps at `INFO`:

- job configuration at startup
- source topics
- Redis bootstrap success
- progress every `--log-every` processed valid events
- invalid click/dead-letter counts

Keep low-level per-event details at `DEBUG`.

Use the existing green/red final summary style where possible.

## Documentation

Add:

```text
docs/desc/run_realtime_ctr.md
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
- Kafka inputs and dead-letter output
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
   ```

4. Start the Flink job:

   ```bash
   python -m ctx_ctr.jobs.run_realtime_ctr
   ```

5. In another terminal, produce events:

   ```bash
   python -m ctx_ctr.jobs.produce_events --impressions 100 --events-per-second 10
   ```

6. Confirm Redis `ctr:*` keys update as events arrive.
7. Confirm no Redis bucket has `clicks > impressions`.
8. Inject or produce an invalid click and confirm it appears in
   `ctr.dead-letter`.
9. Run:

   ```bash
   python -m ruff check src
   python -m mypy src
   ```

## Assumptions

- Local PyFlink in the `big-data` Conda env is the v1 runtime target.
- Redis is the only real-time output store for v1.
- PostgreSQL persistence for streaming updates is a later task.
- Docker Flink cluster submission is a later task.
- The two-topic producer contract remains canonical for v1.
- Spark remains in the project but is not used by this task.
