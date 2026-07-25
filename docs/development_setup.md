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

The environment definition installs `requirements.txt` automatically. To
install or refresh only the Python dependencies in an existing `big-data`
environment, run:

```bash
python -m pip install -r requirements.txt
```

This includes the Python clients for Kafka, Redis, and PostgreSQL, together
with the data-processing, configuration, testing, linting, and type-checking
libraries. It also installs JupyterLab and Matplotlib for the analysis notebooks
under `notebooks/`.

Start JupyterLab from the project root with:

```bash
jupyter lab
```

## 4. Install the Project in Editable Mode

Install the `ctx_ctr` package:

```bash
python -m pip install -e .
```

Editable mode makes `src/ctx_ctr` importable as `ctx_ctr` and installs the
short job commands defined in `pyproject.toml`. Changes made to the source code
become available immediately without reinstalling the package.

Verify the installation:

```bash
python -c "import ctx_ctr; print('ctx_ctr import OK')"
```

Each job loads defaults from `configs/*.yaml`. CLI flags override values from
the YAML config.

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

## 6. Install the Local PyFlink Kafka Connector

This step is required when running the local realtime CTR Flink job from the
Conda environment. PyFlink does not ship the Kafka connector jar with the Python
package, so the project keeps runtime connector jars in `jars/`.

Download the Flink 1.19 Kafka connector:

```bash
mkdir -p jars
curl -L -o jars/flink-sql-connector-kafka-3.2.0-1.19.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
```

The default realtime CTR config already points to this project-relative path:

```yaml
kafka_connector_jar: jars/flink-sql-connector-kafka-3.2.0-1.19.jar
```

Run the job normally from the project root:

```bash
realtime-ctr
```

You only need `--kafka-connector-jar` when using a different connector jar or
location.

## 7. Create the Environment File

Create the local `.env` file:

```bash
cp .env.example .env
```

The default values work with the supplied Docker configuration. Change the
ports or credentials in `.env` only when they conflict with services already
running on the machine.

## 8. Start the Required Docker Services

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

## 9. Start Kafka UI (Optional)

Kafka UI is in the optional `tools` profile:

```bash
docker compose -f docker-compose.yml --profile tools up -d
```

Open `http://localhost:8088` to inspect brokers, topics, messages, and consumer
groups.

## 10. Start the Flink and Spark Clusters (Optional)

Use cluster mode when jobs should execute through Dockerized Flink and Spark
workers:

```bash
docker compose \
  -f docker-compose.yml \
  -f compose.processing-clusters.yml \
  up -d --build
```

Rebuild the Flink services after changes to `docker/flink/Dockerfile`:

```bash
docker compose \
  -f docker-compose.yml \
  -f compose.processing-clusters.yml \
  build --no-cache flink-jobmanager flink-taskmanager
```

Processing endpoints:

```text
Flink dashboard:  http://localhost:8081
Spark master UI:  http://localhost:8080
Spark master URL: spark://localhost:7077
Spark worker UI:  http://localhost:8082
```

The dashboard displays only jobs submitted to this Docker Flink cluster.
Running `realtime-ctr` directly from the Conda environment uses a separate local
Flink runtime and does not register the job in the Docker dashboard.

Submit the realtime CTR job to the Docker cluster:

```bash
docker compose \
  -f docker-compose.yml \
  -f compose.processing-clusters.yml \
  exec flink-jobmanager \
  flink run -d \
  --python /opt/ctx-ctr/src/ctx_ctr/jobs/run_realtime_ctr.py \
  --config /opt/ctx-ctr/configs/run_realtime_ctr.yaml \
  --kafka-connector-jar /opt/ctx-ctr/jars/flink-sql-connector-kafka-3.2.0-1.19.jar \
  --parallelism 2
```

The detached submission prints a Flink job ID. Open
`http://localhost:8081`, select **Jobs**, and open
`ctx-ctr-realtime-ctr` to inspect its operator graph, subtasks, checkpoints,
throughput, busy time, and backpressure.

The supplied Kafka topics have six partitions and the Flink TaskManager has
four task slots. With `--parallelism 2`, the keyed CTR operators run as two
subtasks and process different bucket keys concurrently.

To start the processing clusters and Kafka UI together:

```bash
docker compose \
  -f docker-compose.yml \
  -f compose.processing-clusters.yml \
  --profile tools \
  up -d --build
```

## 11. Connection Addresses

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

## 12. Verify the Complete Installation

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

## 13. Initialize Deterministic CTR State

Validate the deterministic seed dataset without writing anything:

```bash
seed-values --dry-run
```

Reset Postgres and Redis seed-owned state:

```bash
reset-values
```

Seed bucket statistics, model weights, and the seed run summary:

```bash
seed-values
```

Kafka topics are handled separately. To delete and recreate all known project
topics, use:

```bash
clean-topics
```

To clean only selected topics:

```bash
clean-topics --only ctr.impressions ctr.clicks
```

Seeded Redis keys use:

```text
ctr:{ad_category}:{publisher_domain}:{conversation_category}
weights:current
```

## 14. Produce Simulated Events

Validate event generation without publishing to Kafka:

```bash
produce-events --dry-run --impressions 100
```

Produce simulated impression and click events to Kafka:

```bash
produce-events --impressions 1000 --events-per-second 20
```

Log progress every N produced events:

```bash
produce-events --impressions 1000 --events-per-second 20 --log-every 100
```

Use `--log-every 0` to disable progress logs.

By default, impressions are published to `ctr.impressions` and clicks are
published to `ctr.clicks`. To also mirror every event into `ctr.events`, add:

```bash
produce-events --impressions 1000 --also-unified
```

## 15. Stop the Services

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
mkdir -p jars
curl -L -o jars/flink-sql-connector-kafka-3.2.0-1.19.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
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
