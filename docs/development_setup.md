# Complete Installation Guide

This guide sets up the local Python development environment and the Docker
services used by the project.

The project has two ways to run Flink and Spark:

- **Local mode:** PyFlink and PySpark run directly from the `big-data` Conda
  environment.
- **Cluster mode:** Flink and Spark run as Docker services.

Kafka, Redis, and PostgreSQL always run as Docker services.

## 1. Prerequisites

Install these tools before starting:

- Git
- Conda or Miniconda
- Docker Desktop with Docker Compose

On macOS, start Docker Desktop before running any Docker commands.

Verify the tools:

```bash
git --version
conda --version
docker --version
docker compose version
```

Run all remaining commands from the project root.

## 2. Create the Conda Environment

Create or update the `big-data` environment from `environment.yml`:

```bash
conda env update -f environment.yml
conda activate big-data
```

This installs Python 3.11, Java 17, pip, and the core Python requirements.

If the environment already existed without Java, install Java 17 directly:

```bash
conda install -n big-data -c conda-forge "openjdk=17"
conda activate big-data
```

Verify that the correct Python and Java installations are active:

```bash
python --version
java -version
echo "$JAVA_HOME"
```

Python should report version 3.11, Java should report version 17, and
`JAVA_HOME` should point inside the `big-data` environment, usually:

```text
.../miniconda3/envs/big-data/lib/jvm
```

On macOS, `command -v java` may still print `/usr/bin/java`. This is Apple's
Java launcher and is normal when `JAVA_HOME` points to the Conda JDK and
`java -version` reports version 17.

## 3. Install or Refresh Core Python Requirements

The previous `conda env update` command installs `requirements.txt`
automatically. If the `big-data` environment already existed and only the
Python dependencies need to be installed or refreshed, run:

```bash
python -m pip install -r requirements.txt
```

This includes the Python clients for Kafka, Redis, and PostgreSQL, together
with the data-processing, configuration, testing, linting, and type-checking
libraries.

## 4. Install the Project in Editable Mode

Install the `ctx_ctr` package:

```bash
python -m pip install -e .
```

Editable mode makes `src/ctx_ctr` importable as `ctx_ctr`. Changes made to the
source code become available immediately without reinstalling the package.

Verify the installation:

```bash
python -c "import ctx_ctr; print('ctx_ctr import OK')"
```

## 5. Install Local PyFlink and PySpark

This step is required only when running Flink or Spark directly from the Conda
environment. It is not required when using only the Docker processing
clusters.

PyFlink 1.19.1 depends on Apache Beam 2.48.0. On Apple Silicon with Python
3.11, Beam must be built with Cython 0.29 because it is incompatible with
Cython 3:

```bash
python -m pip install "setuptools<81" wheel "Cython==0.29.36"
python -m pip install --no-build-isolation -r requirements-processing-local.txt
```

The installation can take several minutes because pip builds Apache Beam,
PySpark, and some supporting packages locally.

Verify both processing engines:

```bash
python -c "from pyflink.datastream import StreamExecutionEnvironment; print('PyFlink import OK')"
python -c "from pyspark.sql import SparkSession; print('PySpark import OK')"
python -m pip check
```

The `pkg_resources is deprecated` message produced by Apache Beam is a warning,
not an installation failure. Keep `setuptools` below version 81 while using
this PyFlink and Beam combination.

## 6. Create the Environment File

Create the local `.env` file:

```bash
cp .env.example .env
```

The default values work with the supplied Docker configuration. Change the
ports or credentials in `.env` only when they conflict with services already
running on the machine.

## 7. Start the Required Docker Services

Start Kafka, Redis, and PostgreSQL:

```bash
docker compose -f docker-compose.yml up -d
```

Docker also runs the one-time `kafka-init` service, which creates these topics:

- `ctr.impressions`
- `ctr.clicks`
- `ctr.events`
- `ctr.dead-letter`

Check service status:

```bash
docker compose -f docker-compose.yml ps
```

Local connection values:

```text
Kafka:     localhost:9092
Redis:     redis://localhost:6379/0
Postgres:  postgresql://ctx_ctr:ctx_ctr@localhost:5432/ctx_ctr
```

Default PostgreSQL values:

```text
Database:  ctx_ctr
User:      ctx_ctr
Password:  ctx_ctr
```

## 8. Start Kafka UI (Optional)

Kafka UI is in the optional `tools` profile:

```bash
docker compose -f docker-compose.yml --profile tools up -d
```

Open `http://localhost:8088` to inspect brokers, topics, messages, and consumer
groups.

## 9. Start the Flink and Spark Clusters (Optional)

Use cluster mode when jobs should execute through Dockerized Flink and Spark
workers:

```bash
docker compose \
  -f docker-compose.yml \
  -f compose.processing-clusters.yml \
  up -d --build
```

Processing endpoints:

```text
Flink dashboard:  http://localhost:8081
Spark master UI:  http://localhost:8080
Spark master URL: spark://localhost:7077
Spark worker UI:  http://localhost:8082
```

To start the processing clusters and Kafka UI together:

```bash
docker compose \
  -f docker-compose.yml \
  -f compose.processing-clusters.yml \
  --profile tools \
  up -d --build
```

## 10. Connection Addresses

Python programs running in the local Conda environment use the exposed
localhost addresses:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_URL=redis://localhost:6379/0
POSTGRES_DSN=postgresql://ctx_ctr:ctx_ctr@localhost:5432/ctx_ctr
SPARK_MASTER_URL=spark://localhost:7077
FLINK_REST_URL=http://localhost:8081
IMPRESSION_TOPIC=ctr.impressions
CLICK_TOPIC=ctr.clicks
EVENT_TOPIC=ctr.events
DEAD_LETTER_TOPIC=ctr.dead-letter
LOG_LEVEL=INFO
LOG_COLOR=cyan
```

Supported log colors are `black`, `red`, `green`, `yellow`, `blue`, `magenta`,
`cyan`, and `white`.

Programs running inside Docker use Docker service names:

```text
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
REDIS_URL=redis://redis:6379/0
POSTGRES_DSN=postgresql://ctx_ctr:ctx_ctr@postgres:5432/ctx_ctr
SPARK_MASTER_URL=spark://spark-master:7077
FLINK_REST_URL=http://flink-jobmanager:8081
IMPRESSION_TOPIC=ctr.impressions
CLICK_TOPIC=ctr.clicks
EVENT_TOPIC=ctr.events
DEAD_LETTER_TOPIC=ctr.dead-letter
```

## 11. Verify the Complete Installation

Verify the Python environment:

```bash
python -c "import ctx_ctr, kafka, redis, psycopg; print('Core Python imports OK')"
python -m pip check
```

Verify Docker services:

```bash
docker compose -f docker-compose.yml ps
docker compose -f docker-compose.yml exec redis redis-cli ping
docker compose -f docker-compose.yml exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --list
```

When cluster mode is running, verify all containers:

```bash
docker compose \
  -f docker-compose.yml \
  -f compose.processing-clusters.yml \
  ps
```

## 12. Seed Demo Data

Validate the deterministic research-demo seed dataset without writing anything:

```bash
python -m ctx_ctr.jobs.seed_values --dry-run
```

Reset Postgres and Redis seed-owned state:

```bash
python -m ctx_ctr.jobs.reset_values
```

Seed bucket statistics, model weights, and seed metadata:

```bash
python -m ctx_ctr.jobs.seed_values
```

Kafka topics are handled separately. To delete and recreate all known project
topics, use:

```bash
python -m ctx_ctr.jobs.clean_topics
```

To clean only selected topics:

```bash
python -m ctx_ctr.jobs.clean_topics --only ctr.impressions ctr.clicks
```

Seeded Redis keys use:

```text
ctr:{ad_category}:{publisher_domain}:{conversation_category}
weights:current
```

## 13. Produce Simulated Events

Validate event generation without publishing to Kafka:

```bash
python -m ctx_ctr.jobs.produce_events --dry-run --impressions 100
```

Produce simulated impression and click events to Kafka:

```bash
python -m ctx_ctr.jobs.produce_events --impressions 1000 --events-per-second 20
```

Log progress every N produced events:

```bash
python -m ctx_ctr.jobs.produce_events --impressions 1000 --events-per-second 20 --log-every 100
```

Use `--log-every 0` to disable progress logs.

By default, impressions are published to `ctr.impressions` and clicks are
published to `ctr.clicks`. To also mirror every event into `ctr.events`, add:

```bash
python -m ctx_ctr.jobs.produce_events --impressions 1000 --also-unified
```

## 14. Stop the Services

Stop the required services:

```bash
docker compose -f docker-compose.yml down
```

Stop the required services and processing clusters:

```bash
docker compose \
  -f docker-compose.yml \
  -f compose.processing-clusters.yml \
  down
```

To also delete persisted Kafka, Redis, and PostgreSQL data:

```bash
docker compose -f docker-compose.yml down -v
```

The `-v` command permanently removes the local Docker volumes and their data.

## Quick Installation

For a fresh, complete local-mode development setup:

```bash
conda env update -f environment.yml
conda activate big-data
python -m pip install -e .
python -m pip install "setuptools<81" wheel "Cython==0.29.36"
python -m pip install --no-build-isolation -r requirements-processing-local.txt
cp .env.example .env
docker compose -f docker-compose.yml up -d
python -m pip check
```

For Docker cluster mode, add:

```bash
docker compose \
  -f docker-compose.yml \
  -f compose.processing-clusters.yml \
  up -d --build
```
