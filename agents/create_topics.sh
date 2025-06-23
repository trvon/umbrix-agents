#!/bin/bash
# Script to create required Kafka topics

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVERS="localhost:9092"

TOPICS=(
    "raw.intel"
    "coordination.tasks" 
    "raw.intel.taxii"
    "raw.intel.misp"
    "normalized.intel"
    "agent.errors"
    "graph.events"
)

echo "Creating required Kafka topics..."

for topic in "${TOPICS[@]}"; do
    echo "Creating topic: $topic"
    docker exec -it $KAFKA_CONTAINER kafka-topics.sh \
        --create \
        --topic "$topic" \
        --partitions 1 \
        --replication-factor 1 \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --if-not-exists
done

echo "✓ All topics created successfully"