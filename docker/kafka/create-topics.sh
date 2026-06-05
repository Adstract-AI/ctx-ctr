#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"

create_topic() {
  local topic_name="$1"
  local partitions="$2"
  local retention_ms="$3"

  kafka-topics \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic_name}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "retention.ms=${retention_ms}"
}

create_topic "ctr.impressions" 6 604800000
create_topic "ctr.clicks" 6 604800000
create_topic "ctr.events" 6 604800000
create_topic "ctr.dead-letter" 3 1209600000

kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --list
