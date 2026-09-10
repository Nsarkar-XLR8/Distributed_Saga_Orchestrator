#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Kafka Topics Initialization Script
# Auto-provisions domain event topics and dead-letter topics (DLT)
# -----------------------------------------------------------------------------

set -e

BOOTSTRAP_SERVER="localhost:9092"
KAFKA_CONTAINER="distributed-kafka"

echo "=========================================================="
echo "⚡ Initializing Kafka Topics in ${KAFKA_CONTAINER}..."
echo "=========================================================="

# Helper function to create topics inside kafka container
create_topic() {
  local topic_name=$1
  local partitions=$2
  local replication_factor=$3

  echo "Creating topic: ${topic_name} (Partitions: ${partitions}, Replication: ${replication_factor})..."
  docker exec "${KAFKA_CONTAINER}" kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "${topic_name}" \
    --partitions "${partitions}" \
    --replication-factor "${replication_factor}"
}

# 1. Main Domain Event Topics (3 Partitions for concurrent ordering by aggregateId)
create_topic "order-events" 3 1
create_topic "inventory-events" 3 1
create_topic "payment-events" 3 1

# 2. Dead Letter Topics (DLT)
create_topic "order-events.DLT" 1 1
create_topic "inventory-events.DLT" 1 1
create_topic "payment-events.DLT" 1 1

echo "=========================================================="
echo "✅ All Kafka topics successfully provisioned!"
echo "=========================================================="
docker exec "${KAFKA_CONTAINER}" kafka-topics --bootstrap-server localhost:9092 --list
