# CTR Real-Time Processing Research Project

## Overview

This project is a standalone implementation of the CTR estimation and updating pipeline proposed in the Adstract CTR Modeling framework.

The goal of the project is not to build a complete advertising platform, but rather to design, implement, and evaluate the real-time data processing architecture responsible for:

- CTR estimation
- CTR updating
- Bayesian smoothing
- Streaming aggregation
- Weight recalibration
- Feature storage

The project serves as a research and experimentation environment where the CTR algorithms can be implemented and validated independently from the production backend.

---

## Objectives

The system must implement:

### Real-Time CTR Updating

Maintain continuously updated CTR estimates for contextual advertising buckets defined by:

text (ad_category, publisher_domain, conversation_category) 

using the Bayesian Beta-Binomial model described in the CTR documentation.

The CTR estimates must be updated continuously as impression and click events arrive.

---

### Periodic Weight Recalibration

Maintain the following model parameters:

text w0 w_ad[ad_category] w_dom[publisher_domain] w_ctx[conversation_category] 

using the weight update procedure described in the CTR framework.

Weight updates occur periodically (hourly) and are separated from the real-time CTR updates.

---

## Architecture

The architecture is intentionally divided into two processing layers:

### Layer 1: Streaming Layer (Apache Flink)

Responsible for:

- ingesting events
- maintaining impression and click counts
- updating Bayesian posterior parameters
- computing CTR estimates
- computing confidence intervals
- computing stability metrics

Outputs are written to Redis for low-latency access.

Detailed specification:

See:

text flink_ctr_logic.md 

---

### Layer 2: Batch Learning Layer (Apache Spark)

Responsible for:

- reading aggregated CTR statistics
- updating feature-family weights
- updating the global baseline
- applying clipping
- applying centering
- applying ridge regularization

Outputs are written back to Redis and persistent storage.

Detailed specification:

See:

text spark_weight_logic.md 

---

## Technology Stack

### Apache Kafka

Event streaming backbone.

Kafka transports:

- impression events
- click events

to downstream processing components.

---

### Apache Flink

Real-time stream processor.

Consumes Kafka events and maintains continuously updated CTR estimates.

---

### Apache Spark

Periodic batch processor.

Responsible for learning and recalibrating model weights.

---

### Redis

Online feature store.

Stores:

- CTR estimates
- posterior parameters
- confidence intervals
- stability indicators
- learned weights

Used by downstream services for low-latency access.

---

### PostgreSQL

Persistent storage.

Stores:

- historical events
- bucket statistics
- model snapshots
- experiment results

Redis should be considered a cache and feature store, while PostgreSQL remains the source of truth.

---

## Event Flow

text Impression Event                     \                      Kafka                     / Click Event          ↓        Flink          ↓  CTR Estimates Posterior Parameters Confidence Metrics          ↓        Redis          ↓        Spark (Hourly Recalibration)          ↓  Updated Weights          ↓  Redis + PostgreSQL 

---

## Project Deliverables

The project should provide:

- Complete Flink implementation of CTR updating
- Complete Spark implementation of weight learning
- Kafka event simulation
- Redis feature storage
- PostgreSQL persistence
- Metrics and monitoring
- Reproducible experiments
- Documentation of results