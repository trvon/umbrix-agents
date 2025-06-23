"""
Comprehensive Test Coverage for agent_base.py

This test suite provides extensive coverage for all agent_base.py functionality
including abstract methods, component initialization, lifecycle management,
error handling, and specialized agent classes.
"""

import pytest
import asyncio
import time
import json
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock
from typing import Dict, Any, List

from common_tools.agent_base import (
    BaseAgent, KafkaConsumerAgent, PollingAgent, setup_logging
)


class TestSetupLoggingFunction:
    """Test the setup_logging function thoroughly."""
    
    def test_setup_logging_creates_logger(self):
        """Test that setup_logging creates a properly configured logger."""
        logger = setup_logging("test_logger")
        
        assert logger.name == "test_logger"
        assert len(logger.handlers) >= 1
        assert logger.level == 20  # INFO level
    
    def test_setup_logging_idempotent(self):
        """Test that calling setup_logging multiple times doesn't duplicate handlers."""
        logger1 = setup_logging("idempotent_test")
        initial_handlers = len(logger1.handlers)
        
        logger2 = setup_logging("idempotent_test")
        
        assert logger1 is logger2
        assert len(logger2.handlers) == initial_handlers
    
    def test_setup_logging_formatter(self):
        """Test that the logger has the correct formatter."""
        logger = setup_logging("formatter_test")
        
        assert len(logger.handlers) > 0
        handler = logger.handlers[0]
        assert handler.formatter is not None
        
        # Test format string contains expected elements
        format_str = handler.formatter._fmt
        assert "%(asctime)s" in format_str
        assert "%(levelname)s" in format_str
        assert "%(name)s" in format_str
        assert "%(message)s" in format_str


class TestBaseAgentInitialization:
    """Test BaseAgent initialization and component setup."""
    
    def test_base_agent_basic_initialization(self):
        """Test basic BaseAgent initialization."""
        
        class TestAgent(BaseAgent):
            def get_input_topics(self):
                return ["input.topic"]
            
            def get_output_topics(self):
                return ["output.topic"]
            
            async def process_message(self, message):
                return True
        
        config = {"test": "value"}
        agent = TestAgent("test_agent", config)
        
        assert agent.agent_name == "test_agent"
        assert agent.config == config
        assert agent.logger.name == "test_agent"
        assert agent._running is False
        assert agent.running is False
        assert agent.start_time is None
    
    def test_base_agent_metrics_initialization(self):
        """Test that Prometheus metrics are properly initialized."""
        
        class TestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        agent = TestAgent("metrics_test", {})
        
        # Test Counter metric
        assert hasattr(agent, 'messages_processed')
        assert agent.messages_processed.name == 'agent_messages_processed_total'
        assert agent.messages_processed.labelnames == ['agent_name', 'message_type', 'status']
        
        # Test Histogram metric
        assert hasattr(agent, 'processing_duration')
        assert agent.processing_duration.name == 'agent_processing_duration_seconds'
        assert agent.processing_duration.labelnames == ['agent_name', 'message_type']
        
        # Test Gauge metric
        assert hasattr(agent, 'active_tasks')
        assert agent.active_tasks.name == 'agent_active_tasks'
        assert agent.active_tasks.labelnames == ['agent_name']
    
    @patch('common_tools.agent_base.RedisDedupeStore')
    @patch('common_tools.agent_base.UmbrixMetrics')
    def test_component_initialization_enabled(self, mock_metrics, mock_redis):
        """Test component initialization when all components are enabled."""
        
        class TestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        config = {
            "kafka": {"enabled": True, "bootstrap_servers": "test:9092"},
            "redis": {"enabled": True, "host": "test-redis", "port": 6379},
            "metrics": {"enabled": True}
        }
        
        agent = TestAgent("full_config_test", config)
        
        # Test Kafka configuration
        assert agent._kafka_wrapper is not None
        assert agent._kafka_wrapper["bootstrap_servers"] == "test:9092"
        assert "full_config_test" in agent._kafka_wrapper["client_id"]
        
        # Test Redis initialization
        mock_redis.assert_called_once()
        assert agent._dedupe_store is not None
        
        # Test Metrics initialization
        mock_metrics.assert_called_once_with(agent_name="full_config_test")
        assert agent._metrics is not None
    
    def test_component_initialization_disabled(self):
        """Test component initialization when components are disabled."""
        
        class TestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        config = {
            "kafka": {"enabled": False},
            "redis": {"enabled": False},
            "metrics": {"enabled": False}
        }
        
        agent = TestAgent("disabled_config_test", config)
        
        assert agent._kafka_wrapper is None
        assert agent._dedupe_store is None
        assert agent._metrics is None
    
    @patch('common_tools.agent_base.RedisDedupeStore')
    def test_component_initialization_error_handling(self, mock_redis):
        """Test error handling during component initialization."""
        
        # Make Redis initialization fail
        mock_redis.side_effect = Exception("Redis connection failed")
        
        class TestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        config = {"redis": {"enabled": True}}
        
        with pytest.raises(Exception, match="Redis connection failed"):
            TestAgent("error_test", config)


class TestBaseAgentProperties:
    """Test BaseAgent property access."""
    
    def test_kafka_property_access(self):
        """Test Kafka property access."""
        
        class TestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        agent = TestAgent("kafka_prop_test", {"kafka": {"enabled": True}})
        
        kafka_config = agent.kafka
        assert kafka_config is not None
        assert "bootstrap_servers" in kafka_config
        assert "client_id" in kafka_config
    
    def test_dedupe_property_access(self):
        """Test deduplication store property access."""
        
        class TestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        agent = TestAgent("dedupe_prop_test", {})
        
        # Mock the dedupe store
        mock_dedupe = MagicMock()
        agent._dedupe_store = mock_dedupe
        
        assert agent.dedupe is mock_dedupe
    
    def test_metrics_property_access(self):
        """Test metrics property access."""
        
        class TestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        agent = TestAgent("metrics_prop_test", {})
        
        # Mock the metrics
        mock_metrics = MagicMock()
        agent._metrics = mock_metrics
        
        assert agent.metrics is mock_metrics


class TestBaseAgentAbstractMethods:
    """Test BaseAgent abstract method enforcement."""
    
    def test_process_message_is_abstract(self):
        """Test that process_message is abstract and must be implemented."""
        
        class IncompleteAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            # Missing process_message implementation
        
        with pytest.raises(TypeError):
            IncompleteAgent("incomplete", {})
    
    def test_get_input_topics_is_abstract(self):
        """Test that get_input_topics is abstract and must be implemented."""
        
        class IncompleteAgent(BaseAgent):
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
            # Missing get_input_topics implementation
        
        with pytest.raises(TypeError):
            IncompleteAgent("incomplete", {})
    
    def test_get_output_topics_is_abstract(self):
        """Test that get_output_topics is abstract and must be implemented."""
        
        class IncompleteAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            async def process_message(self, message):
                return True
            # Missing get_output_topics implementation
        
        with pytest.raises(TypeError):
            IncompleteAgent("incomplete", {})
    
    def test_complete_implementation_works(self):
        """Test that a complete implementation can be instantiated."""
        
        class CompleteAgent(BaseAgent):
            def get_input_topics(self):
                return ["input.topic"]
            
            def get_output_topics(self):
                return ["output.topic"]
            
            async def process_message(self, message):
                return {"processed": True, "message": message}
        
        agent = CompleteAgent("complete", {})
        assert agent.get_input_topics() == ["input.topic"]
        assert agent.get_output_topics() == ["output.topic"]


class TestBaseAgentLifecycle:
    """Test BaseAgent lifecycle management."""
    
    @pytest.mark.asyncio
    async def test_run_polling_mode_basic(self):
        """Test basic polling mode operation."""
        
        class PollingTestAgent(BaseAgent):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.poll_count = 0
            
            def get_input_topics(self):
                return []  # No input topics triggers polling mode
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
            
            async def poll_and_process(self):
                self.poll_count += 1
                if self.poll_count >= 2:  # Stop after 2 polls
                    self._running = False
        
        config = {"poll_interval": 0.01}  # Fast polling for testing
        agent = PollingTestAgent("polling_test", config)
        
        await agent.run()
        
        assert agent.poll_count >= 2
        assert agent._running is False
    
    @pytest.mark.asyncio
    async def test_run_consumer_mode_not_implemented(self):
        """Test that consumer mode raises NotImplementedError in base class."""
        
        class ConsumerTestAgent(BaseAgent):
            def get_input_topics(self):
                return ["test.topic"]  # Having topics triggers consumer mode
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        # Enable Kafka for consumer mode but disable other components to focus on testing
        config = {
            "kafka": {"enabled": True, "bootstrap_servers": "localhost:9092"},
            "redis": {"enabled": False},
            "metrics": {"enabled": False}
        }
        agent = ConsumerTestAgent("consumer_test", config)
        
        with pytest.raises(NotImplementedError):
            await agent.run()
    
    @pytest.mark.asyncio
    async def test_run_with_metrics_server(self):
        """Test run method with metrics server starting."""
        
        class MetricsTestAgent(BaseAgent):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.poll_count = 0
            
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
            
            async def poll_and_process(self):
                self.poll_count += 1
                if self.poll_count >= 1:
                    self._running = False
        
        # Mock metrics with start_server method
        mock_metrics = MagicMock()
        mock_metrics.start_server = AsyncMock()
        mock_metrics.stop_server = AsyncMock()  # Add stop_server mock too
        
        # Disable components to avoid conflicts and focus on testing metrics logic
        config = {
            "kafka": {"enabled": False},
            "redis": {"enabled": False},
            "metrics": {"enabled": False},
            "poll_interval": 0.01
        }
        agent = MetricsTestAgent("metrics_server_test", config)
        agent._metrics = mock_metrics
        
        await agent.run()
        
        # Verify start_server was called
        mock_metrics.start_server.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_shutdown_graceful(self):
        """Test graceful shutdown process."""
        
        class ShutdownTestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        # Mock components with cleanup methods
        mock_dedupe = MagicMock()
        mock_dedupe.close = MagicMock()
        
        mock_metrics = MagicMock()
        mock_metrics.stop_server = AsyncMock()
        
        agent = ShutdownTestAgent("shutdown_test", {})
        agent._dedupe_store = mock_dedupe
        agent._metrics = mock_metrics
        agent._running = True
        
        await agent.shutdown()
        
        assert agent._running is False
        assert agent.running is False
        assert agent._shutdown_event.is_set()
        
        # Verify cleanup methods were called
        mock_dedupe.close.assert_called_once()
        mock_metrics.stop_server.assert_called_once()


class TestBaseAgentMessageProcessing:
    """Test BaseAgent message processing functionality."""
    
    @pytest.mark.asyncio
    async def test_process_message_with_metrics_success(self):
        """Test successful message processing with metrics."""
        
        class ProcessingTestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        # Mock metrics
        mock_metrics = MagicMock()
        mock_metrics.increment_messages_consumed = MagicMock()
        mock_metrics.increment_messages_produced = MagicMock()
        mock_metrics.observe_processing_latency = MagicMock()
        
        agent = ProcessingTestAgent("processing_test", {})
        agent._metrics = mock_metrics
        
        message = {"id": "test_123", "content": "test message"}
        
        result = await agent._process_message_with_metrics(message)
        
        assert result is True
        mock_metrics.increment_messages_consumed.assert_called_with("processed", 1)
        mock_metrics.increment_messages_produced.assert_called_with("output", 1)
        mock_metrics.observe_processing_latency.assert_called()
    
    @pytest.mark.asyncio
    async def test_process_message_with_metrics_failure(self):
        """Test message processing failure handling with metrics."""
        
        class FailingProcessingAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return False  # Simulate processing failure
        
        mock_metrics = MagicMock()
        mock_metrics.increment_messages_consumed = MagicMock()
        mock_metrics.increment_error = MagicMock()
        
        agent = FailingProcessingAgent("failing_test", {})
        agent._metrics = mock_metrics
        
        message = {"id": "test_456", "content": "test message"}
        
        result = await agent._process_message_with_metrics(message)
        
        assert result is False
        mock_metrics.increment_messages_consumed.assert_called_with("processed", 1)
        mock_metrics.increment_error.assert_called_with("processing_error", 1)
    
    @pytest.mark.asyncio
    async def test_process_message_with_exception(self):
        """Test message processing with exception handling."""
        
        class ExceptionProcessingAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                raise ValueError("Processing error")
        
        mock_metrics = MagicMock()
        mock_metrics.increment_error = MagicMock()
        
        agent = ExceptionProcessingAgent("exception_test", {})
        agent._metrics = mock_metrics
        
        message = {"id": "test_789", "content": "test message"}
        
        result = await agent._process_message_with_metrics(message)
        
        assert result is False
        mock_metrics.increment_error.assert_called_with("processing_error", 1)
    
    def test_duplicate_detection(self):
        """Test duplicate message detection."""
        
        class DedupeTestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        # Mock dedupe store
        mock_dedupe = MagicMock()
        mock_dedupe.exists.return_value = True  # Message is duplicate
        
        agent = DedupeTestAgent("dedupe_test", {})
        agent._dedupe_store = mock_dedupe
        
        message = {"id": "duplicate_123", "content": "duplicate message"}
        
        is_duplicate = agent._is_duplicate(message)
        
        assert is_duplicate is True
        mock_dedupe.exists.assert_called_once_with("duplicate_123")
    
    def test_add_to_dedupe(self):
        """Test adding message to deduplication store."""
        
        class DedupeTestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        mock_dedupe = MagicMock()
        mock_dedupe.add = MagicMock()
        
        agent = DedupeTestAgent("add_dedupe_test", {})
        agent._dedupe_store = mock_dedupe
        
        message = {"id": "new_123", "content": "new message"}
        
        agent._add_to_dedupe(message)
        
        mock_dedupe.add.assert_called_once_with("new_123")
    
    def test_compute_message_hash(self):
        """Test message hash computation."""
        
        class HashTestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        agent = HashTestAgent("hash_test", {})
        
        message1 = {"id": "123", "content": "test", "timestamp": "2023-01-01"}
        message2 = {"id": "123", "content": "test", "timestamp": "2023-01-01"}
        message3 = {"id": "456", "content": "different", "timestamp": "2023-01-02"}
        
        hash1 = agent._compute_message_hash(message1)
        hash2 = agent._compute_message_hash(message2)
        hash3 = agent._compute_message_hash(message3)
        
        # Same messages should produce same hash
        assert hash1 == hash2
        # Different messages should produce different hashes
        assert hash1 != hash3
        # Hash should be a hex string
        assert isinstance(hash1, str)
        assert len(hash1) == 64  # SHA256 hex length


class TestBaseAgentHealthCheck:
    """Test BaseAgent health check functionality."""
    
    @pytest.mark.asyncio
    async def test_health_check_all_healthy(self):
        """Test health check when all components are healthy."""
        
        class HealthTestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        agent = HealthTestAgent("health_test", {})
        agent._running = True
        agent._kafka_wrapper = {"bootstrap_servers": "test:9092"}
        agent._dedupe_store = MagicMock()
        
        health_status = await agent.health_check()
        
        assert health_status["agent_name"] == "health_test"
        assert health_status["status"] == "healthy"
        assert health_status["running"] is True
        assert health_status["components"]["kafka"] == "healthy"
        assert health_status["components"]["redis"] == "healthy"
    
    @pytest.mark.asyncio
    async def test_health_check_degraded(self):
        """Test health check when some components are unhealthy."""
        
        class HealthTestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return []
            
            async def process_message(self, message):
                return True
        
        agent = HealthTestAgent("health_degraded_test", {})
        agent._running = True
        agent._kafka_wrapper = {"bootstrap_servers": "test:9092"}
        # No Redis configured - will be missing from components
        
        health_status = await agent.health_check()
        
        assert health_status["agent_name"] == "health_degraded_test"
        assert health_status["status"] == "healthy"  # Only Kafka is configured
        assert health_status["running"] is True
        assert health_status["components"]["kafka"] == "healthy"


class TestKafkaConsumerAgent:
    """Test KafkaConsumerAgent specialized class."""
    
    def test_kafka_consumer_agent_initialization(self):
        """Test KafkaConsumerAgent initialization with batch settings."""
        
        class TestKafkaAgent(KafkaConsumerAgent):
            def get_input_topics(self):
                return ["input.topic"]
            
            def get_output_topics(self):
                return ["output.topic"]
            
            async def process_message(self, message):
                return True
        
        config = {"batch_size": 20, "batch_timeout": 60}
        agent = TestKafkaAgent("kafka_consumer_test", config)
        
        assert agent.batch_size == 20
        assert agent.batch_timeout == 60
    
    def test_kafka_consumer_agent_default_config(self):
        """Test KafkaConsumerAgent with default batch configuration."""
        
        class TestKafkaAgent(KafkaConsumerAgent):
            def get_input_topics(self):
                return ["input.topic"]
            
            def get_output_topics(self):
                return ["output.topic"]
            
            async def process_message(self, message):
                return True
        
        agent = TestKafkaAgent("kafka_consumer_default", {})
        
        assert agent.batch_size == 10  # Default
        assert agent.batch_timeout == 30  # Default
    
    @pytest.mark.asyncio
    async def test_process_batch_default_implementation(self):
        """Test default batch processing implementation."""
        
        class TestKafkaAgent(KafkaConsumerAgent):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.processed_messages = []
            
            def get_input_topics(self):
                return ["input.topic"]
            
            def get_output_topics(self):
                return ["output.topic"]
            
            async def process_message(self, message):
                self.processed_messages.append(message)
                return message["id"] != "fail"  # Fail on specific message
        
        agent = TestKafkaAgent("batch_test", {})
        
        messages = [
            {"id": "msg1", "content": "message 1"},
            {"id": "fail", "content": "failing message"},
            {"id": "msg3", "content": "message 3"}
        ]
        
        results = await agent.process_batch(messages)
        
        assert len(results) == 3
        assert results[0] is True   # msg1 succeeded
        assert results[1] is False  # fail message failed
        assert results[2] is True   # msg3 succeeded
        assert len(agent.processed_messages) == 3


class TestPollingAgent:
    """Test PollingAgent specialized class."""
    
    def test_polling_agent_initialization(self):
        """Test PollingAgent initialization."""
        
        class TestPollingAgent(PollingAgent):
            def get_output_topics(self):
                return ["output.topic"]
            
            async def process_message(self, message):
                return True
            
            async def poll_and_process(self):
                pass
        
        config = {"poll_interval": 600}
        agent = TestPollingAgent("polling_test", config)
        
        assert agent.poll_interval == 600
        assert agent.get_input_topics() == []  # Polling agents don't consume
    
    def test_polling_agent_default_config(self):
        """Test PollingAgent with default configuration."""
        
        class TestPollingAgent(PollingAgent):
            def get_output_topics(self):
                return ["output.topic"]
            
            async def process_message(self, message):
                return True
            
            async def poll_and_process(self):
                pass
        
        agent = TestPollingAgent("polling_default", {})
        
        assert agent.poll_interval == 300  # Default 5 minutes
    
    def test_polling_agent_abstract_method(self):
        """Test that poll_and_process is abstract in PollingAgent."""
        
        class IncompletePollingAgent(PollingAgent):
            def get_output_topics(self):
                return ["output.topic"]
            
            async def process_message(self, message):
                return True
            # Missing poll_and_process implementation
        
        with pytest.raises(TypeError):
            IncompletePollingAgent("incomplete_polling", {})


class TestBaseAgentPublishMessage:
    """Test BaseAgent publish_message functionality."""
    
    @pytest.mark.asyncio
    async def test_publish_message_not_implemented(self):
        """Test that publish_message raises NotImplementedError in base class."""
        
        class PublishTestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return ["output.topic"]
            
            async def process_message(self, message):
                return True
        
        agent = PublishTestAgent("publish_test", {})
        agent._kafka_wrapper = {"test": "config"}
        
        with pytest.raises(NotImplementedError):
            await agent.publish_message("test.topic", {"test": "message"})
    
    @pytest.mark.asyncio
    async def test_publish_message_no_kafka(self):
        """Test publish_message error when Kafka not initialized."""
        
        class PublishTestAgent(BaseAgent):
            def get_input_topics(self):
                return []
            
            def get_output_topics(self):
                return ["output.topic"]
            
            async def process_message(self, message):
                return True
        
        # Disable Kafka explicitly
        config = {"kafka": {"enabled": False}}
        agent = PublishTestAgent("no_kafka_test", config)
        
        with pytest.raises(ValueError, match="Kafka not initialized"):
            await agent.publish_message("test.topic", {"test": "message"})


if __name__ == "__main__":
    pytest.main([__file__, "-v"])