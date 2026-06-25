# persist_redis_buckets

Persists current Redis CTR bucket values into PostgreSQL.

## Command

```bash
python -m ctx_ctr.jobs.persist_redis_buckets
```

Short command:

```bash
persist-redis-buckets
```

Dry-run:

```bash
python -m ctx_ctr.jobs.persist_redis_buckets --dry-run
```

Default config:

```text
configs/persist_redis_buckets.yaml
```

## Purpose

Use this job when realtime CTR updates have been written to Redis and you want
to persist the current bucket state into PostgreSQL.

The job scans project-owned Redis bucket keys, validates their payloads, and
upserts valid rows into `ctr_bucket_statistics`.

## Required Services

- Redis
- PostgreSQL

Run `seed-values` first when you need a known starting state. Run
`realtime-ctr` and `produce-events` when you want Redis buckets to change before
persistence.

## Flags

- `--config <path>`: YAML config path. Defaults to
  `configs/persist_redis_buckets.yaml`.
- `--redis-url <url>`: Redis URL. Defaults to the runtime setting.
- `--postgres-dsn <dsn>`: PostgreSQL DSN. Defaults to the runtime setting.
- `--dry-run` / `--no-dry-run`: Scan and validate Redis buckets without writing
  PostgreSQL.

CLI flags override values from the YAML config.

## Config Fields

- `redis_url`: Redis URL.
- `postgres_dsn`: PostgreSQL DSN.
- `dry_run`: Whether to scan only and skip PostgreSQL writes.

## Redis Input

The job scans:

```text
ctr:*
```

Each value must match the CTR bucket-statistic payload shape:

- ad category
- publisher domain
- conversation category
- impressions/clicks
- Beta prior and posterior values
- CTR, variance, confidence interval
- trusted flag

Invalid or unreadable bucket payloads are skipped and counted in the summary.

## PostgreSQL Output

Valid buckets are upserted into:

```text
ctr_bucket_statistics
```

The conflict key is:

```text
ad_category, publisher_domain, conversation_category
```

Existing rows are updated with the latest Redis values and `updated_at = NOW()`.

## Notes

This job does not update Redis.

This job does not write model snapshots.

This job does not touch Kafka.
