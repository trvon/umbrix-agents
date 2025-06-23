"""Test utilities for mocking agent dependencies"""
import os
import sys
from unittest.mock import MagicMock, patch
from prometheus_client import CollectorRegistry
from prometheus_client import Counter

from agents.collector_agent.rss_discovery_agent import RssDiscoveryAgent
from agents.collector_agent.shodan_stream_agent import ShodanStreamAgent

# Create separate registry instances for tests to avoid conflicts
RSS_DISCOVERY_TEST_REGISTRY = CollectorRegistry()
SHODAN_STREAM_TEST_REGISTRY = CollectorRegistry()

def create_test_rss_discovery_agent(*args, **kwargs):
    """Factory function to create a test-friendly RssDiscoveryAgent instance"""
    # Clear the test registry to prevent duplicate metrics
    global RSS_DISCOVERY_TEST_REGISTRY
    RSS_DISCOVERY_TEST_REGISTRY = CollectorRegistry()
    
    # Create a custom validation error counter for testing to avoid conflicts
    test_validation_error_counter = Counter('test_rss_validation_errors_total', 
                                           'Test counter for RSS validation errors', 
                                           registry=RSS_DISCOVERY_TEST_REGISTRY)
    
    # Mock the KafkaProducer creation
    with patch('kafka.KafkaProducer', return_value=MagicMock()):
        # Mock the Prometheus metrics registration
        with patch('agents.collector_agent.rss_discovery_agent.METRICS_REGISTRY', RSS_DISCOVERY_TEST_REGISTRY):
            # Also patch the counter initialization in the agent's __init__ method
            with patch.object(Counter, '__init__', return_value=None):
                # Mock sys.exit to prevent agent from exiting on Kafka connection error
                original_exit = sys.exit
                sys.exit = lambda *args: None
                try:
                    agent = RssDiscoveryAgent(*args, **kwargs)
                    # Replace the counter with our test version
                    agent.validation_error_counter = test_validation_error_counter
                finally:
                    sys.exit = original_exit
    return agent

def create_test_shodan_stream_agent(*args, **kwargs):
    """Factory function to create a test-friendly ShodanStreamAgent instance"""
    # Clear the test registry to prevent duplicate metrics
    global SHODAN_STREAM_TEST_REGISTRY
    SHODAN_STREAM_TEST_REGISTRY = CollectorRegistry()
    
    # Create a test validation error counter
    test_validation_error_counter = Counter('test_shodan_validation_errors_total', 
                                           'Test counter for Shodan validation errors', 
                                           registry=SHODAN_STREAM_TEST_REGISTRY)
    
    # Create a mock producer that will be used for tests
    mock_producer = MagicMock()
    mock_producer.send = MagicMock()
    
    # Mock the KafkaProducer creation
    with patch('kafka.KafkaProducer', return_value=mock_producer):
        # Mock the Prometheus metrics registration
        with patch('agents.collector_agent.shodan_stream_agent.METRICS_REGISTRY', SHODAN_STREAM_TEST_REGISTRY):
            # Also patch the counter initialization to avoid conflicts
            with patch.object(Counter, '__init__', return_value=None):
                # Mock other counters in shodan_stream_agent.py
                with patch('agents.collector_agent.shodan_stream_agent.SHODAN_EVENTS_COUNTER', MagicMock()):
                    with patch('agents.collector_agent.shodan_stream_agent.SHODAN_ERROR_COUNTER', MagicMock()):
                        # Mock sys.exit to prevent agent from exiting on Kafka connection error
                        original_exit = sys.exit
                        sys.exit = lambda *args: None
                        try:
                            agent = ShodanStreamAgent(*args, **kwargs)
                            # Replace the counter with our test version
                            agent.validation_error_counter = test_validation_error_counter
                            # Ensure the producer is properly set for tests
                            agent.producer = mock_producer
                        finally:
                            sys.exit = original_exit
    return agent
