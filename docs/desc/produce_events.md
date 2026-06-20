# produce_events

Produces simulated CTR impression and click events to Kafka.

## Command

```bash
python -m ctx_ctr.jobs.produce_events
```

Example with a slower rate:

```bash
python -m ctx_ctr.jobs.produce_events --events-per-second 0.5 --impressions 10
```

Dry-run:

```bash
python -m ctx_ctr.jobs.produce_events --dry-run --impressions 100
```

## Purpose

Use this job to simulate real traffic for local Flink/Kafka development.

The job creates impression events and probabilistically creates click events
from the same seeded CTR model weights used by the seed dataset.

## Flags

- `--impressions <int>`: Number of impression events to simulate. Clicks are
  added on top of this count.
- `--events-per-second <float>`: Send rate. Use `0` to publish as fast as
  possible.
- `--random-seed <int>`: Seed for deterministic traffic generation.
- `--log-every <int>`: Log progress every N produced events. Use `0` to
  disable progress logs.
- `--also-unified`: Also publish every event to `ctr.events`.
- `--dry-run`: Generate and summarize events without publishing to Kafka.

## Event Counts

`--impressions` controls only the number of impression events.

Total events are:

```text
impressions + sampled clicks
```

For example, `--impressions 10` may produce:

```text
impressions: 10
clicks: 1
total events: 11
```

## Click Sampling

Each impression independently samples a click using:

```text
sigmoid(
  logit(0.02)
  + w_ad[ad_category]
  + w_dom[publisher_domain]
  + w_ctx[conversation_category]
)
```

So the baseline click probability is around 2%, adjusted by the selected
category, domain, and conversation context.

## Kafka Topics

By default:

- impressions go to `ctr.impressions`
- clicks go to `ctr.clicks`

With `--also-unified`, all events are also mirrored to:

- `ctr.events`

## How It Works

The simulator:

1. Chooses weighted traffic buckets.
2. Emits one impression per requested impression.
3. Samples a click for each impression using the bucket-specific probability.
4. Sends events through the Kafka producer adapter.
5. Logs progress every `--log-every` produced events.
6. Prints a final green summary.

## Notes

Dry-run mode does not connect to Kafka.

This job does not write to PostgreSQL or Redis.

