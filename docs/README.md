# Documentation

This directory contains operational and job-level documentation for the
contextual CTR streaming system.

## Setup

- [Complete development setup](development_setup.md): Conda, Java, Python
  dependencies, the PyFlink Kafka connector, Docker services, and verification.

## Jobs

- [Job reference index](desc/README.md): commands, configuration files, inputs,
  outputs, behavior, and safety notes for every runnable job.

The main processing pages are:

- [Realtime CTR updates](desc/run_realtime_ctr.md)
- [Streaming model-weight updates](desc/run_streaming_weight_update.md)
- [Redis-based model-weight updates](desc/run_weight_update.md)
- [Experiment runner](desc/run_experiment.md)

## Model And Results

- [CTR modeling specification](../metadata/CTR%20Modeling.pdf)
- [Realtime Flink topology](flink_realtime_ctr_topology.md)
- [CTR update performance analysis](../notebooks/1_ctr_update_performance_analysis.ipynb)
- [CTR statistical validity analysis](../notebooks/2_ctr_update_domain_analysis.ipynb)

Generated experiment artifacts are written under `experiments/results/`.
