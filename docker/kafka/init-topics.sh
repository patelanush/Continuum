#!/usr/bin/env bash
set -euo pipefail

KAFKA_BIN=/opt/kafka/bin/kafka-topics.sh
BOOTSTRAP=kafka:9092

retry() {
  for attempt in {1..12}; do
    if "$@"; then
      return 0
    fi
    if [[ "$attempt" -eq 12 ]]; then
      echo "Kafka topic initialization failed after $attempt attempts" >&2
      return 1
    fi
    sleep 1
  done
}

retry "$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic continuum.step.ready.v1 --partitions 3 --replication-factor 1
retry "$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic continuum.dead-letter.v1 --partitions 3 --replication-factor 1
retry "$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --describe --topic continuum.step.ready.v1
retry "$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --describe --topic continuum.dead-letter.v1
