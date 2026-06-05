# Development Setup

This project runs Kafka, Flink, Spark, Redis, and PostgreSQL in Docker. Python code is developed from the local `big-data` conda environment and connects to those services through exposed localhost ports.

## Local Python Environment

Create or update the environment:

```bash
conda env update -f environment.yml
conda activate big-data
python -m pip install -r requirements.txt -e .
```

If the environment already exists with Python 3.11, the `pip install` command is enough.

## Start Services

```bash
cp .env.example .env
docker compose up -d --build
```

Useful endpoints:

- Kafka broker: `localhost:9092`
- Flink dashboard: `http://localhost:8081`
- Spark master UI: `http://localhost:8080`
- Spark master URL: `spark://localhost:7077`
- Spark worker UI: `http://localhost:8082`
- Redis: `localhost:6379`
- PostgreSQL: `localhost:5432`
- Kafka UI: `http://localhost:8088`

Default PostgreSQL credentials:

- database: `ctx_ctr`
- user: `ctx_ctr`
- password: `ctx_ctr`

## Kafka Topics

The stack creates:

- `ctr.impressions`
- `ctr.clicks`
- `ctr.events`
- `ctr.dead-letter`

## Python Connection Defaults

Use these host values from the local conda environment:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_URL=redis://localhost:6379/0
POSTGRES_DSN=postgresql://ctx_ctr:ctx_ctr@localhost:5432/ctx_ctr
SPARK_MASTER_URL=spark://localhost:7077
FLINK_REST_URL=http://localhost:8081
```

Inside Docker services, use container hostnames:

```text
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
REDIS_URL=redis://redis:6379/0
POSTGRES_DSN=postgresql://ctx_ctr:ctx_ctr@postgres:5432/ctx_ctr
SPARK_MASTER_URL=spark://spark-master:7077
FLINK_REST_URL=http://flink-jobmanager:8081
```

## Stop Services

```bash
docker compose down
```

To remove persisted data:

```bash
docker compose down -v
```
