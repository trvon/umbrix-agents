#!/usr/bin/env python3
"""
Test script to send tasks to the master coordinator.
"""

import json
import os
import sys
from datetime import datetime, timezone
from kafka import KafkaProducer
import uuid

def send_test_task():
    """Send a test task to the coordination topic."""
    
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    
    # Create producer with timeout
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        request_timeout_ms=10000,
        api_version=(2, 0, 0)
    )
    
    # Create test task
    test_task = {
        "task_id": f"test-{uuid.uuid4()}",
        "correlation_id": f"corr-{uuid.uuid4()}",
        "task_type": "feed_discovery",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "target_agent": "RssFeedDiscoverer",
            "action": "discover_feeds",
            "priority": "medium",
            "search_terms": ["cybersecurity", "threat intelligence"]
        }
    }
    
    print(f"Sending test task: {test_task['task_id']}")
    
    # Send task
    future = producer.send("coordination.tasks", test_task)
    record_metadata = future.get(timeout=10)
    
    print(f"✓ Task sent to topic: {record_metadata.topic}")
    print(f"✓ Partition: {record_metadata.partition}")
    print(f"✓ Offset: {record_metadata.offset}")
    
    producer.close()

if __name__ == "__main__":
    send_test_task()