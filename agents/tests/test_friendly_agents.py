"""
This module contains test-friendly versions of agents that disable sys.exit and other behavior
that might cause issues in unit tests.
"""
import sys
from unittest.mock import MagicMock, patch
import os
from prometheus_client import CollectorRegistry
from agents.collector_agent.rss_discovery_agent import RssDiscoveryAgent, DEDUP_TTL_SECONDS
from agents.collector_agent.shodan_stream_agent import ShodanStreamAgent, SHODAN_EVENTS_COUNTER

class MockRssDiscoveryAgent(RssDiscoveryAgent):
    """A test-friendly version of RssDiscoveryAgent that doesn't exit on errors."""
    
    def __init__(self, *args, **kwargs):
        # Backup original exit function
        original_exit = sys.exit
        # Replace exit with a function that does nothing
        sys.exit = lambda *args: None
        
        # Create a mock KafkaProducer for testing
        mock_producer = MagicMock()
        
        try:
            # Mock KafkaProducer before calling super().__init__
            with patch('kafka.KafkaProducer', return_value=mock_producer):
                # Mock the CollectorRegistry and metrics to avoid duplicate registrations
                with patch('prometheus_client.CollectorRegistry', return_value=CollectorRegistry()):
                    with patch('prometheus_client.Counter', return_value=MagicMock()):
                        super().__init__(*args, **kwargs)
        finally:
            # Restore original exit function
            sys.exit = original_exit

class MockShodanStreamAgent(ShodanStreamAgent):
    """A test-friendly version of ShodanStreamAgent that doesn't exit on errors
    and doesn't start a Prometheus HTTP server.
    """
    
    def __init__(self, *args, **kwargs):
        # Backup original exit function
        original_exit = sys.exit
        # Replace exit with a function that does nothing
        sys.exit = lambda *args: None
        
        # Create a mock KafkaProducer for testing
        mock_producer = MagicMock()
        
        try:
            # Mock KafkaProducer before calling super().__init__
            with patch('kafka.KafkaProducer', return_value=mock_producer):
                # Call the parent constructor with our mocked dependencies
                super().__init__(*args, **kwargs)
        finally:
            # Restore original exit function
            sys.exit = original_exit
    
    def run(self):
        """Override the run method to avoid starting an HTTP server for Prometheus."""
        # Skip starting the Prometheus HTTP server
        print("[Shodan] Starting Shodan stream (test-friendly mode)...", file=sys.stderr)
        
        for shodan_event in self.tool.stream():
            # Durable dedupe: skip if this event was seen
            ip = shodan_event.get('ip_str')
            port = shodan_event.get('port')
            ts = shodan_event.get('timestamp')
            key = f"dedupe:shodan:{ip}:{port}:{ts}"
            if not self.dedupe_store.set_if_not_exists(key, DEDUP_TTL_SECONDS):
                continue
                
            # Process the event (same as original)
            try:
                SHODAN_EVENTS_COUNTER.inc()
                
                # The rest of the processing would normally be here
                # For testing, we'll just send the event directly to Kafka
                self.producer.send(self.raw_intel_topic, shodan_event)
                
            except Exception as e:
                print(f"[Shodan Test] Error processing event: {e}", file=sys.stderr)
