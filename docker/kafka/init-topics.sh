#!/usr/bin/env bash
set -euo pipefail

KAFKA_BIN=/opt/kafka/bin/kafka-topics.sh
BOOTSTRAP=kafka:9092

"$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic continuum.step.ready.v1 --partitions 3 --replication-factor 1
"$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic continuum.dead-letter.v1 --partitions 3 --replication-factor 1
"$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --describe --topic continuum.step.ready.v1
"$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --describe --topic continuum.dead-letter.v1
