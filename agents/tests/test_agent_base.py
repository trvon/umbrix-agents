"""
Fixed tests for agent_base.py with proper Prometheus metrics isolation.

This file replaces the problematic tests that were causing Prometheus registry conflicts.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, MagicMock
from collections.abc import Callable

# Import the conftest prometheus stubs
from conftest import _clear_metrics, _Counter, _Histogram, _Gauge, _CollectorRegistry

# Now import the module under test with mocked prometheus
with patch('prometheus_client.Counter', _Counter), \
     patch('prometheus_client.Histogram', _Histogram), \
     patch('prometheus_client.Gauge', _Gauge), \
     patch('prometheus_client.CollectorRegistry', _CollectorRegistry):
    from common_tools.agent_base import BaseAgent


class MockAgentImplementation(BaseAgent):
    """Test implementation of BaseAgent for testing."""
    
    def __init__(self, agent_name: str, config: dict):
        super().__init__(agent_name, config)
    
    async def process_message(self, message):
        """Mock implementation of process_message."""
        return True
    
    def get_input_topics(self):
        """Mock implementation of get_input_topics."""
        return ["test.input"]
    
    def get_output_topics(self):
        """Mock implementation of get_output_topics."""
        return ["test.output"]


@pytest.fixture(autouse=True)
def clear_prometheus_metrics():
    """Clear prometheus metrics before each test to avoid conflicts."""
    _clear_metrics()
    yield
    _clear_metrics()


class TestSetupLogging:
    """Test logging setup functionality."""
    
    def test_setup_logging_creates_logger(self):
        """Test that setup_logging creates a logger with the correct name."""
        from common_tools.logging import setup_logging
        
        logger = setup_logging("test_agent")
        assert logger.name == "test_agent"
    
    def test_setup_logging_idempotent(self):
        """Test that calling setup_logging multiple times returns the same logger."""
        from common_tools.logging import setup_logging
        
        logger1 = setup_logging("test_agent")
        logger2 = setup_logging("test_agent")
        assert logger1 is logger2


class TestBaseAgentInitialization:
    """Test BaseAgent initialization with proper metrics isolation."""
    
    def test_basic_initialization(self):
        """Test basic BaseAgent initialization."""
        config = {
            "kafka": {"enabled": True, "bootstrap_servers": "localhost:9092"},
            "redis": {"enabled": True, "host": "localhost", "port": 6379},
            "metrics": {"enabled": True}
        }
        
        with patch('common_tools.agent_base.setup_logging') as mock_logging, \
             patch('common_tools.agent_base.RedisDedupeStore') as mock_redis, \
             patch('common_tools.agent_base.UmbrixMetrics') as mock_metrics:
            
            mock_logging.return_value = Mock()
            mock_redis.return_value = Mock()
            mock_metrics.return_value = Mock()
            
            agent = MockAgentImplementation("test_agent", config)
            
            # Basic attributes should be set
            assert agent.agent_name == "test_agent"
            assert agent.config == config
            assert agent.running is False
            assert agent.start_time is None
            
            # Metrics should be created
            assert hasattr(agent, 'messages_processed')
            assert hasattr(agent, 'processing_duration')
            assert hasattr(agent, 'active_tasks')
    
    def test_initialization_with_minimal_config(self):
        """Test BaseAgent initialization with minimal configuration."""
        config = {}
        
        with patch('common_tools.agent_base.setup_logging') as mock_logging, \
             patch('common_tools.agent_base.RedisDedupeStore') as mock_redis, \
             patch('common_tools.agent_base.UmbrixMetrics') as mock_metrics:
            
            mock_logging.return_value = Mock()
            mock_redis.return_value = Mock() 
            mock_metrics.return_value = Mock()
            
            agent = MockAgentImplementation("minimal_agent", config)
            
            assert agent.agent_name == "minimal_agent"
            assert agent.config == config
    
    def test_initialization_component_setup(self):
        """Test that components are properly initialized."""
        config = {
            "kafka": {"enabled": True, "bootstrap_servers": "test:9092"},
            "redis": {"enabled": True, "host": "test-redis", "port": 6380, "db": 1},
            "metrics": {"enabled": True}
        }
        
        with patch('common_tools.agent_base.setup_logging') as mock_logging, \
             patch('common_tools.agent_base.RedisDedupeStore') as mock_redis, \
             patch('common_tools.agent_base.UmbrixMetrics') as mock_metrics:
            
            mock_logger = Mock()
            mock_logging.return_value = mock_logger
            mock_redis_instance = Mock()
            mock_redis.return_value = mock_redis_instance
            mock_metrics_instance = Mock()
            mock_metrics.return_value = mock_metrics_instance
            
            agent = MockAgentImplementation("test_agent", config)
            
            # Check Kafka config
            assert agent.kafka['bootstrap_servers'] == "test:9092"
            assert "test_agent" in agent.kafka['client_id']
            
            # Check Redis setup
            mock_redis.assert_called_once()
            call_args = mock_redis.call_args
            assert "test-redis" in call_args[1]['redis_url']
            assert "6380" in call_args[1]['redis_url']
            assert "test_agent_dedupe" == call_args[1]['namespace']
            
            # Check metrics setup
            mock_metrics.assert_called_once_with(agent_name="test_agent")
            
            # Check logger calls
            assert mock_logger.info.call_count >= 3  # At least one for each component
    
    def test_initialization_disabled_components(self):
        """Test initialization with disabled components."""
        config = {
            "kafka": {"enabled": False},
            "redis": {"enabled": False},
            "metrics": {"enabled": False}
        }
        
        with patch('common_tools.agent_base.setup_logging') as mock_logging, \
             patch('common_tools.agent_base.RedisDedupeStore') as mock_redis, \
             patch('common_tools.agent_base.UmbrixMetrics') as mock_metrics:
            
            mock_logging.return_value = Mock()
            
            agent = MockAgentImplementation("test_agent", config)
            
            # Components should not be initialized
            mock_redis.assert_not_called()
            mock_metrics.assert_not_called()
            
            # Properties should return None
            assert agent.kafka is None
            assert agent.dedupe is None
            assert agent.metrics is None
    
    def test_initialization_error_handling(self):
        """Test error handling during component initialization."""
        config = {
            "redis": {"enabled": True}
        }
        
        with patch('common_tools.agent_base.setup_logging') as mock_logging, \
             patch('common_tools.agent_base.RedisDedupeStore') as mock_redis:
            
            mock_logger = Mock()
            mock_logging.return_value = mock_logger
            mock_redis.side_effect = Exception("Redis connection failed")
            
            with pytest.raises(Exception, match="Redis connection failed"):
                MockAgentImplementation("test_agent", config)
            
            # Should have logged the error
            mock_logger.error.assert_called_once()
            assert "Failed to initialize components" in mock_logger.error.call_args[0][0]


class TestBaseAgentProperties:
    """Test BaseAgent property access."""
    
    def test_kafka_property_access(self):
        """Test Kafka property access."""
        config = {"kafka": {"enabled": True, "bootstrap_servers": "test:9092"}}
        
        with patch('common_tools.agent_base.setup_logging'), \
             patch('common_tools.agent_base.RedisDedupeStore'), \
             patch('common_tools.agent_base.UmbrixMetrics'):
            
            agent = MockAgentImplementation("test_agent", config)
            
            kafka_config = agent.kafka
            assert kafka_config is not None
            assert kafka_config['bootstrap_servers'] == "test:9092"
            assert "test_agent" in kafka_config['client_id']
    
    def test_dedupe_property_access(self):
        """Test Redis dedupe property access."""
        config = {"redis": {"enabled": True}}
        
        with patch('common_tools.agent_base.setup_logging'), \
             patch('common_tools.agent_base.RedisDedupeStore') as mock_redis, \
             patch('common_tools.agent_base.UmbrixMetrics'):
            
            mock_redis_instance = Mock()
            mock_redis.return_value = mock_redis_instance
            
            agent = MockAgentImplementation("test_agent", config)
            
            assert agent.dedupe is mock_redis_instance
    
    def test_metrics_property_access(self):
        """Test metrics property access."""
        config = {"metrics": {"enabled": True}}
        
        with patch('common_tools.agent_base.setup_logging'), \
             patch('common_tools.agent_base.RedisDedupeStore'), \
             patch('common_tools.agent_base.UmbrixMetrics') as mock_metrics:
            
            mock_metrics_instance = Mock()
            mock_metrics.return_value = mock_metrics_instance
            
            agent = MockAgentImplementation("test_agent", config)
            
            assert agent.metrics is mock_metrics_instance


class TestBaseAgentAbstractMethods:
    """Test BaseAgent abstract method requirements."""
    
    def test_process_message_is_abstract(self):
        """Test that process_message must be implemented by subclasses."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseAgent("test", {})
    
    def test_mock_implementation_works(self):
        """Test that our mock implementation properly implements the abstract method."""
        config = {}
        
        with patch('common_tools.agent_base.setup_logging'), \
             patch('common_tools.agent_base.RedisDedupeStore'), \
             patch('common_tools.agent_base.UmbrixMetrics'):
            
            agent = MockAgentImplementation("test_agent", config)
            
            # Should be able to call the implemented method
            result = asyncio.run(agent.process_message({"test": "message"}))
            assert result is True


class TestBaseAgentMetrics:
    """Test BaseAgent metrics functionality."""
    
    def test_metrics_creation(self):
        """Test that metrics are properly created."""
        config = {}
        
        with patch('common_tools.agent_base.setup_logging'), \
             patch('common_tools.agent_base.RedisDedupeStore'), \
             patch('common_tools.agent_base.UmbrixMetrics'):
            
            agent = MockAgentImplementation("test_agent", config)
            
            # Check that metrics exist and have correct properties
            assert hasattr(agent.messages_processed, 'name')
            assert agent.messages_processed.name == 'agent_messages_processed_total'
            
            assert hasattr(agent.processing_duration, 'name')
            assert agent.processing_duration.name == 'agent_processing_duration_seconds'
            
            assert hasattr(agent.active_tasks, 'name')
            assert agent.active_tasks.name == 'agent_active_tasks'
    
    def test_metrics_have_correct_labelnames(self):
        """Test that metrics have the expected label names."""
        config = {}
        
        with patch('common_tools.agent_base.setup_logging'), \
             patch('common_tools.agent_base.RedisDedupeStore'), \
             patch('common_tools.agent_base.UmbrixMetrics'):
            
            agent = MockAgentImplementation("test_agent", config)
            
            # Check labelnames
            assert agent.messages_processed.labelnames == ['agent_name', 'message_type', 'status']
            assert agent.processing_duration.labelnames == ['agent_name', 'message_type']
            assert agent.active_tasks.labelnames == ['agent_name']
    
    def test_metrics_can_be_used(self):
        """Test that metrics can be incremented/used without errors."""
        config = {}
        
        with patch('common_tools.agent_base.setup_logging'), \
             patch('common_tools.agent_base.RedisDedupeStore'), \
             patch('common_tools.agent_base.UmbrixMetrics'):
            
            agent = MockAgentImplementation("test_agent", config)
            
            # Test using the metrics
            agent.messages_processed.labels("test_agent", "test_message", "success").inc()
            agent.processing_duration.labels("test_agent", "test_message").observe(0.5)
            agent.active_tasks.labels("test_agent").inc()
            
            # Should not raise any exceptions
            assert True  # If we get here, metrics worked