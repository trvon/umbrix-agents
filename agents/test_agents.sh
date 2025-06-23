#!/bin/bash
# Test script to verify agents are working

echo "Testing CTI Agents..."
echo "===================="

# Check if Kafka is running
echo "1. Checking Kafka connectivity..."
docker-compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✓ Kafka is running"
else
    echo "✗ Kafka is not accessible"
    exit 1
fi

# Check required topics
echo -e "\n2. Checking required Kafka topics..."
TOPICS=("feeds.discovered" "raw.intel" "enriched.intel" "coordination.tasks")
for topic in "${TOPICS[@]}"; do
    docker-compose exec kafka kafka-topics.sh --describe --topic "$topic" --bootstrap-server localhost:9092 2>/dev/null > /dev/null
    if [ $? -eq 0 ]; then
        echo "✓ Topic $topic exists"
    else
        echo "✗ Topic $topic missing - creating..."
        docker-compose exec kafka kafka-topics.sh --create --topic "$topic" --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
    fi
done

# Send a test message to coordination.tasks
echo -e "\n3. Sending test task to Master Coordinator..."
TEST_TASK='{
  "task_id": "test-001",
  "task_type": "feed_discovery",
  "correlation_id": "test-correlation-001",
  "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'",
  "payload": {
    "target_agent": "RssFeedDiscoverer",
    "action": "discover_feeds",
    "priority": "medium"
  }
}'

echo "$TEST_TASK" | docker-compose exec -T kafka kafka-console-producer.sh \
    --broker-list localhost:9092 \
    --topic coordination.tasks

echo "✓ Test task sent to coordination.tasks"

# Check for messages in feeds.discovered topic
echo -e "\n4. Checking for discovered feeds..."
timeout 10 docker-compose exec kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic feeds.discovered \
    --from-beginning \
    --max-messages 5 2>/dev/null

# Check agent logs
echo -e "\n5. Recent agent logs:"
docker-compose logs --tail=20 master-coordinator 2>/dev/null | grep -E "(Started|Error|task_received|task_completed)"

echo -e "\nAgent test complete!"