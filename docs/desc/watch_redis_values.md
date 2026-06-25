# watch_redis_values

Prints Redis keys and values for local debugging.

## Command

```bash
python -m ctx_ctr.jobs.watch_redis_values
```

Short command:

```bash
watch-redis-values
```

Default config:

```text
configs/watch_redis_values.yaml
```

## Purpose

Use this job while `realtime-ctr` is running to watch Redis bucket statistics
change. The job is read-only and does not modify Redis.

## Required Services

Redis must be running:

```bash
docker compose -f docker-compose.yml up -d redis
```

Run `seed-values` first when you want the normal starting keys:

```bash
seed-values
```

## Flags

- `--config <path>`: YAML config path. Defaults to
  `configs/watch_redis_values.yaml`.
- `--pattern <pattern>`: Redis scan pattern. Defaults to `*`.
- `--limit <int>`: Maximum number of matching keys to print. Defaults to `200`.
- `--watch` / `--no-watch`: Keep printing snapshots until interrupted.
- `--interval-seconds <float>`: Delay between snapshots in watch mode. Defaults
  to `2.0`.
- `--pretty-json` / `--no-pretty-json`: Pretty-print JSON values. Enabled by
  default.
- `--bucket <ad_category> <publisher_domain> <conversation_category>`: Print
  only one focused CTR bucket.

CLI flags override values from the YAML config.

## Config Fields

- `pattern`: Redis key scan pattern.
- `limit`: Maximum number of scanned keys to print.
- `watch`: Whether to keep printing snapshots.
- `interval_seconds`: Watch-mode delay.
- `pretty_json`: Whether JSON values should be indented.
- `ad_category`, `publisher_domain`, `conversation_category`: Optional focused
  bucket fields. When all three are set, only that bucket is printed.

Redis comes from `REDIS_URL` in the environment.

## Examples

Print all Redis keys once:

```bash
watch-redis-values
```

Print only project CTR buckets:

```bash
watch-redis-values --pattern 'ctr:*'
```

Watch all project CTR buckets every second:

```bash
watch-redis-values --pattern 'ctr:*' --watch --interval-seconds 1
```

Watch one bucket while events are being produced:

```bash
watch-redis-values \
  --watch \
  --interval-seconds 1 \
  --bucket finance news.example personal_finance
```

Print one bucket once:

```bash
watch-redis-values --bucket finance news.example personal_finance
```

## Output

Each snapshot prints:

- snapshot timestamp
- scan pattern
- number of scanned keys printed
- every matching key/value pair, up to `limit`
- focused bucket value when `--bucket` is supplied

Missing focused buckets are printed as `<missing>`.
