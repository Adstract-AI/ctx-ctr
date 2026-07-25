# Job Reference

Each runnable job has a default YAML file under `configs/`. Values supplied as
CLI flags take precedence over YAML values. Service connection details such as
Kafka bootstrap servers, the Redis URL, and the PostgreSQL DSN come from
environment variables.

| Command | Purpose | Documentation |
| --- | --- | --- |
| `seed-values` | Initialize deterministic model and bucket state | [Seed values](seed_values.md) |
| `reset-values` | Clear project-owned Redis and PostgreSQL state | [Reset values](reset_values.md) |
| `clean-topics` | Delete and recreate Kafka topics | [Clean topics](clean_topics.md) |
| `produce-events` | Generate simulated impression and click traffic | [Produce events](produce_events.md) |
| `realtime-ctr` | Update contextual CTR buckets with PyFlink | [Realtime CTR](run_realtime_ctr.md) |
| `streaming-weight-update` | Learn model weights continuously with PyFlink | [Streaming weight update](run_streaming_weight_update.md) |
| `weight-update` | Recompute model weights from Redis bucket state | [Weight update](run_weight_update.md) |
| `persist-redis-buckets` | Copy current Redis CTR buckets to PostgreSQL | [Persist Redis buckets](persist_redis_buckets.md) |
| `watch-redis-values` | Inspect changing Redis model and bucket values | [Watch Redis values](watch_redis_values.md) |
| `run-experiment` | Run reproducible system and performance experiments | [Experiments](run_experiment.md) |

`reset-values`, `clean-topics`, and destructive experiment definitions remove
local state. Review their documentation and configuration before running them.
