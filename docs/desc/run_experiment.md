# run_experiment

Short command:

```bash
run-experiment
```

Module command:

```bash
python -m ctx_ctr.jobs.run_experiment
```

## What It Does

Runs a configured CTR experiment and records measurable system state, timing,
throughput, and validation results.

Included experiment definitions:

- `local_smoke` produces a small deterministic traffic batch and measures an
  already-running local system.
- `full_system_local` resets local state, starts the realtime CTR and streaming
  weight processors, produces phased traffic, evaluates success gates, and
  stops the processors.
- `realtime_ctr_performance_baseline` preloads a large Kafka backlog before
  starting the realtime CTR processor so sustained consumer throughput can be
  measured independently of producer pacing.

## When To Use It

Use this when you want a repeatable run that answers:

- did the simulated traffic get produced
- did Redis bucket totals move
- did model snapshot or experiment-result counts change
- where is the artifact for this run

Processor lifecycle is controlled by the selected experiment definition.
`local_smoke` expects the required processors to be running externally.
Full-system and performance definitions can start, monitor, and stop processor
subprocesses as part of the run.

Traffic configs support two shapes:

- single-phase experiments define `impressions` and `events_per_second`
- phased experiments define `phases`; the single-phase fallback fields should be
  omitted
- performance experiments can set `preload_before_processors: true` to publish
  all configured phases before processor subprocesses start

## Default Config

Job config:

```bash
configs/run_experiment.yaml
```

Default experiment:

```bash
local_smoke
```

The runner resolves experiment names to this file pattern:

```bash
experiments/configs/{experiment_name}.yaml
```

Experiment artifacts:

```bash
experiments/results/
```

Example full-system experiment definition:

```bash
experiments/configs/full_system_local.yaml
```

## Flags

- `--config`: path to the job YAML config
- `--experiment`: experiment name from `experiments/configs/{name}.yaml`
- `--output-dir`: directory for JSON and Markdown artifacts
- `--dry-run` / `--no-dry-run`: simulate without writing Kafka events or
  Postgres experiment rows

CLI flags override the YAML config values.

## Reads And Writes

Reads:

- Redis `ctr:*`
- Redis `weights:current`
- Postgres `ctr_model_snapshots`
- Postgres `ctr_experiment_results`
- YAML job config
- YAML experiment definition

Writes:

- Kafka impression/click topics when not in dry-run mode
- local JSON/Markdown artifacts under `experiments/results/`
- Postgres `ctr_experiment_results` when not in dry-run mode
- processor log files under the experiment artifact directory
- timing metrics inside the `timing` section of the result artifact

`full_system_local` is intentionally destructive for local CTR state: it cleans
CTR Kafka topics and resets seed-owned Redis/Postgres values before seeding.

## Examples

Dry-run the configured experiment:

```bash
run-experiment --dry-run
```

Run the local smoke experiment:

```bash
seed-values
realtime-ctr
run-experiment
```

Run the full-system local experiment:

```bash
run-experiment --experiment full_system_local
```

Run the realtime CTR throughput baseline:

```bash
run-experiment --experiment realtime_ctr_performance_baseline
```

This experiment is destructive for local CTR state and Kafka topics. Its large
backlog keeps the processor under sustained load long enough to compare
parallelism and Redis persistence strategies.

Use another experiment definition:

```bash
run-experiment --experiment my_experiment
```

## Output

The terminal prints a final boxed summary with:

- total measured runtime
- traffic production time
- observed producer events per second
- realtime CTR processed events per second from processor logs
- processor teardown time
- produced impressions, clicks, and total events
- Redis bucket counts before and after
- Redis impression/click deltas
- Postgres model snapshot delta
- Postgres experiment row id
- success gate status and failures
- local artifact path

For `full_system_local`, success gates fail the command after artifacts and the
Postgres experiment result are written. Check the artifact directory for
`result.json`, `result.md`, and processor logs.

The JSON artifact stores Redis/Postgres `before`, `after`, and `delta` values
under `metrics.statistics`. Detailed phase timings live with each entry under
`metrics.traffic.phases`; orchestration timings remain under `metrics.timing`.

The producer throughput metrics measure how fast the experiment sent events to
Kafka. Realtime CTR consumer throughput is parsed separately from
`CTR_PROCESSOR_METRICS` records emitted by the `realtime-ctr` subprocess and is
stored under `metrics.processor_metrics.realtime_ctr`. That object contains all
parsed records, the average of each numeric metric, and aggregate throughput and
final counters across Flink subtasks. Processor commands, PIDs, timestamps, and
stop metadata are not included in experiment metrics.
