# Contextual CTR Streaming System

A research implementation of contextual click-through rate (CTR) estimation and
model recalibration with Apache Kafka, Apache Flink, Redis, and PostgreSQL.

The system consumes impression and click events, maintains Bayesian CTR
statistics for `(ad category, publisher domain, conversation context)` buckets,
and periodically recalibrates a logistic model containing a global baseline and
centered feature-family weights.

## Goal

The project studies two questions:

1. Can a stateful Flink pipeline update contextual CTR estimates correctly under
   streaming traffic?
2. How do Flink parallelism and Redis persistence strategy affect throughput,
   scalability, and state correctness?

The domain formulas and model assumptions are based on
[CTR Modeling](metadata/CTR%20Modeling.pdf).

## System Overview

```text
Simulated events
      |
      v
Apache Kafka
      |
      +--> Flink realtime CTR job --> keyed Bayesian state --> Redis ctr:* keys
      |
      +--> Flink weight job -------> Redis weights:current
                                      + PostgreSQL model snapshots

Redis bucket state --> persistence job --> PostgreSQL bucket statistics
Experiments --------> metrics, processor logs, JSON/Markdown results
```

Kafka provides the event stream, Flink owns stateful processing, Redis exposes
current bucket and model state, and PostgreSQL stores durable snapshots and
experiment summaries.

## Quick Start

Complete the [development setup](docs/development_setup.md) once, then start the
required services and seed deterministic state:

```bash
conda activate big-data
docker compose -f docker-compose.yml up -d
reset-values
seed-values
```

Start the realtime processor:

```bash
realtime-ctr
```

In another terminal, produce traffic and inspect Redis:

```bash
produce-events --impressions 1000 --events-per-second 100
watch-redis-values --pattern 'ctr:*'
```

Run a configured experiment:

```bash
run-experiment --experiment full_system_local
```

Every job has a YAML file under `configs/`; explicit CLI flags override the YAML
values. Service connection settings come from `.env`.

## Project Structure

| Path | Purpose |
|---|---|
| `src/ctx_ctr/jobs/` | Runnable command entrypoints and Flink topology wiring. |
| `src/ctx_ctr/services/` | CTR mathematics, simulation, learning, seeding, persistence, and experiment orchestration. |
| `src/ctx_ctr/adapters/` | Kafka, Redis, and PostgreSQL integration boundaries. |
| `src/ctx_ctr/models/` | Pydantic domain, configuration, and result models. |
| `configs/` | Default YAML configuration for each runnable job. |
| `experiments/configs/` | Reproducible experiment definitions. |
| `experiments/results/` | Experiment metrics, summaries, and processor logs. |
| `notebooks/` | Performance and statistical-validity analyses. |
| `docs/` | Installation guide and detailed job reference pages. |
| `metadata/` | CTR modeling specification and captured Flink topology. |
| `docker/`, `docker-compose.yml` | Kafka, Redis, PostgreSQL, and container setup. |
| `compose.processing-clusters.yml` | Optional Dockerized Flink and Spark clusters. |
| `tests/` | Automated service, configuration, adapter, and job tests. |

## Documentation

- [Documentation index](docs/README.md)
- [Complete development setup](docs/development_setup.md)
- [Job reference](docs/desc/README.md)
- [Realtime CTR job](docs/desc/run_realtime_ctr.md)
- [Streaming weight-update job](docs/desc/run_streaming_weight_update.md)
- [Experiment runner](docs/desc/run_experiment.md)
- [CTR modeling specification](metadata/CTR%20Modeling.pdf)
- [Realtime Flink topology](metadata/flink_realtime_ctr_topology.md)

## Analysis Notebooks

- [CTR update performance analysis](notebooks/1_ctr_update_performance_analysis.ipynb)
- [CTR statistical validity analysis](notebooks/2_ctr_update_domain_analysis.ipynb)

The performance notebook compares controlled 200,000-impression runs across
parallelism and Redis persistence strategies. The domain notebook validates
count reconciliation, posterior accuracy, uncertainty, calibration, and the
assumptions required for production use.

## Credits

Created by **Andrea Stevanoska**, **Viktor Kostadinoski**, and
**Darko Petruushevski** for the course
[Mining Massive Data Sets](https://finki.ukim.mk/sites/default/files/mining_massive_data_sets.pdf).

The project was completed under the supervision and guidance of
**Prof. Dr. Gjorgji Madzarov** and teaching assistant **Stefan Andonov** at the
[Faculty of Computer Science and Engineering (FCSE / FINKI)](https://finki.ukim.mk/en),
Ss. Cyril and Methodius University in Skopje.

## License

This project is available under the [MIT License](LICENSE).
