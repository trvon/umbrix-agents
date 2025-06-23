#!/usr/bin/env python3
"""
Script to ensure required Kafka topics exist for agents.
"""

import os
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_topics(bootstrap_servers: str = "kafka:9092"):
    """Ensure required Kafka topics exist."""
    
    required_topics = [
        "feeds.discovered",
        "raw.intel", 
        "enriched.intel",
        "coordination.tasks",
        "raw.intel.taxii",
        "raw.intel.misp",
        "normalized.intel",
        "agent.errors",
        "graph.events"
    ]
    
    # Wait for Kafka to be available
    max_retries = 30
    for i in range(max_retries):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="topic_creator"
            )
            break
        except Exception as e:
            if i == max_retries - 1:
                logger.error(f"Failed to connect to Kafka after {max_retries} attempts: {e}")
                return False
            logger.info(f"Waiting for Kafka... attempt {i+1}/{max_retries}")
            time.sleep(2)
    
    # Get existing topics
    try:
        existing_topics = set(admin_client.list_topics())
        logger.info(f"Existing topics: {existing_topics}")
    except Exception as e:
        logger.error(f"Failed to list topics: {e}")
        return False
    
    # Create missing topics
    topics_to_create = []
    for topic_name in required_topics:
        if topic_name not in existing_topics:
            topic = NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1
            )
            topics_to_create.append(topic)
            logger.info(f"Will create topic: {topic_name}")
    
    if topics_to_create:
        try:
            result = admin_client.create_topics(topics_to_create)
            # Handle different Kafka client versions
            if hasattr(result, 'items'):
                # Newer kafka-python versions
                for topic_name, future in result.items():
                    try:
                        future.result()  # Will raise exception if creation failed
                        logger.info(f"✓ Created topic: {topic_name}")
                    except TopicAlreadyExistsError:
                        logger.info(f"✓ Topic already exists: {topic_name}")
                    except Exception as e:
                        logger.error(f"✗ Failed to create topic {topic_name}: {e}")
            else:
                # Older kafka-python versions - result is a dict-like object
                for topic in topics_to_create:
                    topic_name = topic.name
                    try:
                        if hasattr(result, topic_name):
                            future = getattr(result, topic_name)
                            future.result()
                        logger.info(f"✓ Created topic: {topic_name}")
                    except TopicAlreadyExistsError:
                        logger.info(f"✓ Topic already exists: {topic_name}")
                    except Exception as e:
                        logger.error(f"✗ Failed to create topic {topic_name}: {e}")
        except Exception as e:
            logger.error(f"Failed to create topics: {e}")
            return False
    else:
        logger.info("All required topics already exist")
    
    admin_client.close()
    return True

if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    success = ensure_topics(bootstrap_servers)
    exit(0 if success else 1)