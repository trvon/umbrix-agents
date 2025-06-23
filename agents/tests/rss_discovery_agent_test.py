#!/usr/bin/env python3
"""
Test-friendly version of RssDiscoveryAgent that doesn't exit on errors
and uses a unique metrics registry.
"""

import sys
import os
from prometheus_client import CollectorRegistry
from agents.collector_agent.rss_discovery_agent import RssDiscoveryAgent, SITES_CRAWLED_COUNTER, FEEDS_FOUND_COUNTER, CONFIG_UPDATES_COUNTER, DISCOVERY_ERROR_COUNTER
from kafka import KafkaProducer
import json
from datetime import datetime
from agents.common_tools.models import FeedRecord

# Create a unique metrics registry for tests
TEST_METRICS_REGISTRY = CollectorRegistry()

class RssDiscoveryAgentForTest(RssDiscoveryAgent):
    """A test-friendly version of RssDiscoveryAgent that doesn't exit on errors
    and uses a unique metrics registry.
    """
    
    def __init__(self, kafka_bootstrap_servers: str = None, topic: str = 'feeds.discovered', config_path=None,
               name: str = 'rss_discovery', description: str = 'Discovers RSS/Atom feeds and publishes URLs', **kwargs):
        # Prevent sys.exit when Kafka connection fails
        original_exit = sys.exit
        sys.exit = lambda x: None
        
        try:
            # Call the original constructor but with a mock producer
            super().__init__(
                kafka_bootstrap_servers=kafka_bootstrap_servers,
                topic=topic,
                config_path=config_path,
                name=name,
                description=description,
                **kwargs
            )
            
            # Replace the producer with a mock
            self.producer = MockProducer(value_serializer=lambda v: v.model_dump_json().encode('utf-8') if isinstance(v, FeedRecord) else json.dumps(v).encode('utf-8'))
            
        finally:
            # Restore original exit function
            sys.exit = original_exit


# Mock Kafka producer for testing
class MockProducer:
    """A mock KafkaProducer that doesn't connect to Kafka"""
    
    def __init__(self, **kwargs):
        self.sent_messages = []
        self.value_serializer = kwargs.get('value_serializer', lambda x: x)
        
    def send(self, topic, value=None, **kwargs):
        """Mock the send method"""
        self.sent_messages.append((topic, value))
        
    def flush(self):
        """Mock the flush method"""
        pass
