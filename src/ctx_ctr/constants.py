"""Shared constants for CTR processing services and jobs."""

DEFAULT_IMPRESSION_TOPIC = "ctr.impressions"
DEFAULT_CLICK_TOPIC = "ctr.clicks"
DEFAULT_EVENT_TOPIC = "ctr.events"
DEFAULT_DEAD_LETTER_TOPIC = "ctr.dead-letter"

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_POSTGRES_DSN = "postgresql://ctx_ctr:ctx_ctr@localhost:5432/ctx_ctr"
DEFAULT_SPARK_MASTER_URL = "spark://localhost:7077"
DEFAULT_FLINK_REST_URL = "http://localhost:8081"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_COLOR = "cyan"

REDIS_CTR_KEY_PREFIX = "ctr"
REDIS_WEIGHTS_KEY_PREFIX = "weights"
REDIS_CURRENT_WEIGHTS_KEY = "weights:current"
