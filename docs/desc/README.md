# Job Descriptions

This folder documents every runnable job in the project.

When a new job is added under `src/ctx_ctr/jobs`, add one Markdown file here
with:

- what the job does
- when to use it
- all flags
- default YAML config path under `configs/`
- short command name from `pyproject.toml`
- what it reads and writes
- example commands
- important safety notes

Every job loads defaults from its YAML config file. CLI flags override config
values when the same option is supplied in both places.

Current jobs:

- [seed_values](seed_values.md)
- [reset_values](reset_values.md)
- [clean_topics](clean_topics.md)
- [persist_redis_buckets](persist_redis_buckets.md)
- [produce_events](produce_events.md)
- [run_experiment](run_experiment.md)
- [run_realtime_ctr](run_realtime_ctr.md)
- [run_streaming_weight_update](run_streaming_weight_update.md)
- [run_weight_update](run_weight_update.md)
- [watch_redis_values](watch_redis_values.md)
