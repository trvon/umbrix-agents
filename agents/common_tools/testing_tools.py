"""
Testing utilities for Umbrix agents.

This module provides mock services, test fixtures, and utilities
for testing agents in isolation and integration scenarios.
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional, Callable, AsyncGenerator
from dataclasses import dataclass
from datetime import datetime
from unittest.mock import Mock, AsyncMock, MagicMock
import tempfile
import shutil
import os
from contextlib import asynccontextmanager, contextmanager

try:
    import pytest
    PYTEST_AVAILABLE = True
except ImportError:
    PYTEST_AVAILABLE = False

try:
    import fakeredis
    FAKEREDIS_AVAILABLE = True
except ImportError:
    FAKEREDIS_AVAILABLE = False

from .logging import setup_logging


@dataclass
class MockMessage:
    """Mock Kafka message for testing."""
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: Dict[str, Any]
    timestamp: int
    headers: Optional[Dict[str, bytes]] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = int(time.time() * 1000)
        if self.headers is None:
            self.headers = {}


class MockKafkaConsumer:
    """
    Mock Kafka consumer for testing.
    
    Simulates consuming messages from predefined topics with
    configurable message sequences and timing.
    """
    
    def __init__(self, topics: List[str], group_id: str, **kwargs):
        self.topics = topics
        self.group_id = group_id
        self.kwargs = kwargs
        
        self.logger = setup_logging(f"mock_kafka_consumer_{group_id}")
        
        # Message queue for each topic
        self.message_queues = {topic: [] for topic in topics}
        self.consumed_messages = []
        
        # Configuration
        self.auto_offset_reset = kwargs.get('auto_offset_reset', 'earliest')
        self.enable_auto_commit = kwargs.get('enable_auto_commit', True)
        
        # State
        self.closed = False
        self.current_offsets = {}
        
        # Statistics
        self.stats = {
            'messages_consumed': 0,
            'commits_made': 0,
            'errors_simulated': 0
        }
    
    def add_message(self, topic: str, message: Dict[str, Any], key: Optional[str] = None):
        """Add a message to the consumer's queue."""
        if topic not in self.message_queues:
            raise ValueError(f"Topic {topic} not subscribed")
        
        mock_message = MockMessage(
            topic=topic,
            partition=0,
            offset=len(self.message_queues[topic]),
            key=key,
            value=message,
            timestamp=int(time.time() * 1000)
        )
        
        self.message_queues[topic].append(mock_message)
        self.logger.debug(f"Added message to {topic}: {message}")
    
    def add_messages(self, topic: str, messages: List[Dict[str, Any]]):
        """Add multiple messages to the consumer's queue."""
        for msg in messages:
            self.add_message(topic, msg)
    
    async def __aiter__(self):
        """Async iterator for consuming messages."""
        while not self.closed:
            # Check all topics for messages
            message_found = False
            
            for topic in self.topics:
                if self.message_queues[topic]:
                    message = self.message_queues[topic].pop(0)
                    self.consumed_messages.append(message)
                    self.stats['messages_consumed'] += 1
                    
                    self.logger.debug(f"Consumed message from {topic}: {message.value}")
                    yield message
                    message_found = True
                    break
            
            if not message_found:
                # No messages available, yield control
                await asyncio.sleep(0.1)
    
    def commit(self):
        """Mock commit operation."""
        self.stats['commits_made'] += 1
        self.logger.debug("Committed offsets")
    
    def close(self):
        """Close the consumer."""
        self.closed = True
        self.logger.debug("Consumer closed")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics."""
        return self.stats.copy()


class MockKafkaProducer:
    """
    Mock Kafka producer for testing.
    
    Captures produced messages and simulates producer behavior
    including failures and delivery confirmations.
    """
    
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.logger = setup_logging("mock_kafka_producer")
        
        # Produced messages storage
        self.produced_messages = []
        
        # Configuration
        self.simulate_failures = kwargs.get('simulate_failures', False)
        self.failure_rate = kwargs.get('failure_rate', 0.1)
        
        # Statistics
        self.stats = {
            'messages_produced': 0,
            'messages_failed': 0,
            'bytes_sent': 0
        }
    
    async def send(self, topic: str, value: Dict[str, Any], key: Optional[str] = None, headers: Optional[Dict[str, bytes]] = None):
        """Mock send operation."""
        import random
        
        # Simulate failures if configured
        if self.simulate_failures and random.random() < self.failure_rate:
            self.stats['messages_failed'] += 1
            raise Exception(f"Simulated producer failure for topic {topic}")
        
        message = {
            'topic': topic,
            'key': key,
            'value': value,
            'headers': headers or {},
            'timestamp': int(time.time() * 1000)
        }
        
        self.produced_messages.append(message)
        self.stats['messages_produced'] += 1
        self.stats['bytes_sent'] += len(json.dumps(value))
        
        self.logger.debug(f"Produced message to {topic}: {value}")
        
        # Return a mock future that resolves immediately
        future = asyncio.Future()
        record_metadata = type('RecordMetadata', (), {
            'topic': topic,
            'partition': 0,
            'offset': len(self.produced_messages) - 1
        })()
        future.set_result(record_metadata)
        return future
    
    def get_produced_messages(self, topic: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get produced messages, optionally filtered by topic."""
        if topic:
            return [msg for msg in self.produced_messages if msg['topic'] == topic]
        return self.produced_messages.copy()
    
    def clear_messages(self):
        """Clear produced messages."""
        self.produced_messages.clear()
    
    def close(self):
        """Close the producer."""
        self.logger.debug("Producer closed")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics."""
        return self.stats.copy()


class MockRedisClient:
    """
    Mock Redis client for testing.
    
    Provides in-memory Redis-like operations for testing
    without requiring a real Redis instance.
    """
    
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.logger = setup_logging("mock_redis")
        
        # In-memory storage
        self.data = {}
        self.expiry_times = {}
        
        # Configuration
        self.simulate_failures = kwargs.get('simulate_failures', False)
        self.failure_rate = kwargs.get('failure_rate', 0.05)
        
        # Statistics
        self.stats = {
            'operations_performed': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'keys_expired': 0
        }
    
    def _check_expiry(self, key: str) -> bool:
        """Check if a key has expired and remove it if so."""
        if key in self.expiry_times:
            if time.time() > self.expiry_times[key]:
                del self.data[key]
                del self.expiry_times[key]
                self.stats['keys_expired'] += 1
                return True
        return False
    
    def _simulate_failure(self):
        """Simulate Redis operation failure."""
        if self.simulate_failures:
            import random
            if random.random() < self.failure_rate:
                raise Exception("Simulated Redis operation failure")
    
    async def get(self, key: str) -> Optional[Any]:
        """Mock Redis GET operation."""
        self._simulate_failure()
        self.stats['operations_performed'] += 1
        
        if self._check_expiry(key):
            self.stats['cache_misses'] += 1
            return None
        
        if key in self.data:
            self.stats['cache_hits'] += 1
            return self.data[key]
        else:
            self.stats['cache_misses'] += 1
            return None
    
    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """Mock Redis SET operation."""
        self._simulate_failure()
        self.stats['operations_performed'] += 1
        
        self.data[key] = value
        
        if ex:
            self.expiry_times[key] = time.time() + ex
        
        return True
    
    async def delete(self, key: str) -> int:
        """Mock Redis DELETE operation."""
        self._simulate_failure()
        self.stats['operations_performed'] += 1
        
        if key in self.data:
            del self.data[key]
            if key in self.expiry_times:
                del self.expiry_times[key]
            return 1
        return 0
    
    async def exists(self, key: str) -> bool:
        """Mock Redis EXISTS operation."""
        self._simulate_failure()
        self.stats['operations_performed'] += 1
        
        if self._check_expiry(key):
            return False
        
        return key in self.data
    
    async def expire(self, key: str, seconds: int) -> bool:
        """Mock Redis EXPIRE operation."""
        self._simulate_failure()
        self.stats['operations_performed'] += 1
        
        if key in self.data:
            self.expiry_times[key] = time.time() + seconds
            return True
        return False
    
    def clear(self):
        """Clear all data."""
        self.data.clear()
        self.expiry_times.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get Redis client statistics."""
        return {
            **self.stats,
            'total_keys': len(self.data),
            'keys_with_expiry': len(self.expiry_times)
        }


class MockHttpServer:
    """
    Mock HTTP server for testing external API interactions.
    
    Provides configurable responses, request logging, and
    failure simulation for testing HTTP clients.
    """
    
    def __init__(self, port: int = 8888):
        self.port = port
        self.logger = setup_logging(f"mock_http_server_{port}")
        
        # Request/response configuration
        self.response_mappings = {}
        self.default_response = {'status': 404, 'body': 'Not Found'}
        
        # Request logging
        self.received_requests = []
        
        # Failure simulation
        self.simulate_failures = False
        self.failure_rate = 0.1
        
        # Statistics
        self.stats = {
            'requests_received': 0,
            'responses_sent': 0,
            'errors_simulated': 0
        }
    
    def add_response(self, method: str, path: str, response: Dict[str, Any]):
        """Add a response mapping for a specific method and path."""
        key = f"{method.upper()}:{path}"
        self.response_mappings[key] = response
        self.logger.debug(f"Added response mapping: {key} -> {response}")
    
    def set_default_response(self, response: Dict[str, Any]):
        """Set the default response for unmapped requests."""
        self.default_response = response
    
    async def handle_request(self, method: str, path: str, headers: Dict[str, str], body: str) -> Dict[str, Any]:
        """Handle an incoming request and return configured response."""
        import random
        
        # Log the request
        request = {
            'method': method,
            'path': path,
            'headers': headers,
            'body': body,
            'timestamp': time.time()
        }
        self.received_requests.append(request)
        self.stats['requests_received'] += 1
        
        # Simulate failures if configured
        if self.simulate_failures and random.random() < self.failure_rate:
            self.stats['errors_simulated'] += 1
            raise Exception("Simulated HTTP server error")
        
        # Find matching response
        key = f"{method.upper()}:{path}"
        response = self.response_mappings.get(key, self.default_response)
        
        self.stats['responses_sent'] += 1
        return response
    
    def get_received_requests(self, method: Optional[str] = None, path: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get received requests, optionally filtered."""
        requests = self.received_requests
        
        if method:
            requests = [r for r in requests if r['method'].upper() == method.upper()]
        
        if path:
            requests = [r for r in requests if r['path'] == path]
        
        return requests
    
    def clear_requests(self):
        """Clear received requests log."""
        self.received_requests.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        return self.stats.copy()


class AgentTestFixture:
    """
    Comprehensive test fixture for agent testing.
    
    Provides all mock services and utilities needed to test
    agents in isolation with configurable behaviors.
    """
    
    def __init__(self, agent_name: str = "test_agent"):
        self.agent_name = agent_name
        self.logger = setup_logging(f"test_fixture_{agent_name}")
        
        # Mock services
        self.kafka_consumer = None
        self.kafka_producer = None
        self.redis_client = None
        self.http_server = None
        
        # Temporary directories
        self.temp_dirs = []
        
        # Test configuration
        self.config = {
            'kafka': {'enabled': True, 'bootstrap_servers': 'localhost:9092'},
            'redis': {'enabled': True, 'host': 'localhost', 'port': 6379},
            'metrics': {'enabled': False},  # Disable metrics in tests
            'logging': {'level': 'DEBUG'}
        }
    
    async def setup(self):
        """Set up all mock services."""
        self.logger.info(f"Setting up test fixture for {self.agent_name}")
        
        # Create mock Kafka services
        self.kafka_consumer = MockKafkaConsumer(['test_topic'], 'test_group')
        self.kafka_producer = MockKafkaProducer()
        
        # Create mock Redis client
        self.redis_client = MockRedisClient()
        
        # Create mock HTTP server
        self.http_server = MockHttpServer()
        
        self.logger.info("Test fixture setup complete")
    
    async def teardown(self):
        """Clean up all mock services and temporary resources."""
        self.logger.info(f"Tearing down test fixture for {self.agent_name}")
        
        # Close mock services
        if self.kafka_consumer:
            self.kafka_consumer.close()
        
        if self.kafka_producer:
            self.kafka_producer.close()
        
        if self.redis_client:
            self.redis_client.clear()
        
        # Clean up temporary directories
        for temp_dir in self.temp_dirs:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        
        self.logger.info("Test fixture teardown complete")
    
    def create_temp_dir(self) -> str:
        """Create a temporary directory for testing."""
        temp_dir = tempfile.mkdtemp(prefix=f"{self.agent_name}_test_")
        self.temp_dirs.append(temp_dir)
        return temp_dir
    
    def add_kafka_messages(self, topic: str, messages: List[Dict[str, Any]]):
        """Add messages to Kafka consumer queue."""
        if not self.kafka_consumer:
            raise RuntimeError("Kafka consumer not initialized")
        
        self.kafka_consumer.add_messages(topic, messages)
    
    def get_produced_messages(self, topic: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get messages produced by the agent."""
        if not self.kafka_producer:
            raise RuntimeError("Kafka producer not initialized")
        
        return self.kafka_producer.get_produced_messages(topic)
    
    def add_http_response(self, method: str, path: str, response: Dict[str, Any]):
        """Add HTTP response mapping."""
        if not self.http_server:
            raise RuntimeError("HTTP server not initialized")
        
        self.http_server.add_response(method, path, response)
    
    def get_http_requests(self, method: Optional[str] = None, path: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get HTTP requests made by the agent."""
        if not self.http_server:
            raise RuntimeError("HTTP server not initialized")
        
        return self.http_server.get_received_requests(method, path)
    
    async def wait_for_messages(self, topic: str, count: int, timeout: float = 10.0) -> List[Dict[str, Any]]:
        """Wait for a specific number of messages to be produced."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            messages = self.get_produced_messages(topic)
            if len(messages) >= count:
                return messages[:count]
            await asyncio.sleep(0.1)
        
        raise TimeoutError(f"Timeout waiting for {count} messages on topic {topic}")
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics from all mock services."""
        stats = {}
        
        if self.kafka_consumer:
            stats['kafka_consumer'] = self.kafka_consumer.get_stats()
        
        if self.kafka_producer:
            stats['kafka_producer'] = self.kafka_producer.get_stats()
        
        if self.redis_client:
            stats['redis_client'] = self.redis_client.get_stats()
        
        if self.http_server:
            stats['http_server'] = self.http_server.get_stats()
        
        return stats


@asynccontextmanager
async def agent_test_context(agent_name: str = "test_agent"):
    """
    Async context manager for agent testing.
    
    Automatically sets up and tears down test fixtures.
    """
    fixture = AgentTestFixture(agent_name)
    
    try:
        await fixture.setup()
        yield fixture
    finally:
        await fixture.teardown()


@contextmanager
def temp_config_file(config: Dict[str, Any]) -> str:
    """
    Context manager for creating temporary configuration files.
    
    Returns the path to the temporary config file.
    """
    import yaml
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f)
        temp_path = f.name
    
    try:
        yield temp_path
    finally:
        # Clean up
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def create_mock_agent_config(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create a mock agent configuration for testing."""
    default_config = {
        'kafka': {
            'enabled': False,  # Disabled by default for testing
            'bootstrap_servers': 'localhost:9092',
            'topics': {
                'input': ['test_input_topic'],
                'output': ['test_output_topic']
            }
        },
        'redis': {
            'enabled': False,  # Disabled by default for testing
            'host': 'localhost',
            'port': 6379,
            'db': 0
        },
        'metrics': {
            'enabled': False,  # Disabled by default for testing
            'port': 8000
        },
        'logging': {
            'level': 'DEBUG',
            'format': 'simple'
        },
        'agent': {
            'poll_interval': 1,
            'batch_size': 10,
            'max_retries': 3
        }
    }
    
    if overrides:
        # Deep merge overrides
        def deep_merge(base: Dict, updates: Dict) -> Dict:
            for key, value in updates.items():
                if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                    deep_merge(base[key], value)
                else:
                    base[key] = value
            return base
        
        deep_merge(default_config, overrides)
    
    return default_config


# Pytest fixtures (if pytest is available)
if PYTEST_AVAILABLE:
    @pytest.fixture
    async def agent_fixture():
        """Pytest fixture for agent testing."""
        async with agent_test_context() as fixture:
            yield fixture
    
    @pytest.fixture
    def mock_config():
        """Pytest fixture for mock agent configuration."""
        return create_mock_agent_config()
    
    @pytest.fixture
    def temp_dir():
        """Pytest fixture for temporary directory."""
        temp_dir = tempfile.mkdtemp(prefix="umbrix_test_")
        try:
            yield temp_dir
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)