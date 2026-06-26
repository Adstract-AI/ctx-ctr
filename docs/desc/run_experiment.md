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

Runs one configured CTR experiment and records measurable before/after metrics.

The first included experiment is `local_smoke`. It produces a small deterministic
traffic batch, waits briefly for the realtime processors to catch up, scans Redis
CTR bucket state, reads Postgres row counts, writes local JSON/Markdown artifacts,
and inserts one row into `ctr_experiment_results`.

## When To Use It

Use this when you want a repeatable run that answers:

- did the simulated traffic get produced
- did Redis bucket totals move
- did model snapshot or experiment-result counts change
- where is the artifact for this run

The job does not start or stop Flink jobs. Start `realtime-ctr` or
`streaming-weight-update` separately when the experiment should measure those
processors.

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

It does not reset Redis, Postgres, or Kafka.

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

Use another experiment definition:

```bash
run-experiment --experiment my_experiment
```

## Output

The terminal prints a final boxed summary with:

- produced impressions, clicks, and total events
- Redis bucket counts before and after
- Redis impression/click deltas
- Postgres model snapshot delta
- Postgres experiment row id
- local artifact path
