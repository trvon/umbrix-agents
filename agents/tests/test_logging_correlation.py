"""
Unit tests for structured logging and correlation ID propagation.

Tests cover:
- JSON log format validation
- Correlation ID extraction and injection
- Context propagation through Kafka messages
- Cross-agent tracing
"""

import json
import logging
import io
import sys
import pytest
from datetime import datetime, timezone
from typing import Dict, Any
from unittest.mock import Mock, patch, MagicMock

from agents.common_tools.logging import (
    configure_logging,
    get_logger,
    setup_agent_logging,
    log_function_entry,
    log_function_exit,
    log_kafka_message_received,
    log_kafka_message_sent,
    log_processing_error,
    log_performance_metric,
    create_child_logger
)

from agents.common_tools.context import (
    CorrelationContext,
    extract_correlation_context_from_kafka_message,
    inject_correlation_context_into_kafka_message,
    CorrelationAwareKafkaProducer,
    CorrelationAwareKafkaConsumer,
    create_child_correlation_context,
    with_correlation_context
)

from agents.common_tools.structured_logging import (
    StructuredLogger,
    StructuredJSONFormatter,
    LogContext,
    LogContextManager,
    create_correlation_id,
    create_task_id
)


class TestStructuredJSONLogging:
    """Test cases for structured JSON logging functionality."""
    
    def setup_method(self):
        """Setup for each test method."""
        # Capture log output
        self.log_stream = io.StringIO()
        self.handler = logging.StreamHandler(self.log_stream)
        self.handler.setFormatter(StructuredJSONFormatter())
        
        # Create test logger
        self.logger = StructuredLogger("test_agent", "test_agent")
        self.logger.logger.handlers = [self.handler]
        self.logger.logger.setLevel(logging.DEBUG)
    
    def teardown_method(self):
        """Cleanup after each test method."""
        self.log_stream.close()
    
    def get_log_output(self) -> str:
        """Get the captured log output."""
        return self.log_stream.getvalue().strip()
    
    def parse_log_json(self) -> Dict[str, Any]:
        """Parse the log output as JSON."""
        log_output = self.get_log_output()
        if not log_output:
            return {}
        return json.loads(log_output)
    
    def test_json_formatter_basic_fields(self):
        """Test that JSON formatter includes all required fields."""
        self.logger.info("Test message")
        
        log_data = self.parse_log_json()
        
        # Check required fields are present
        assert "timestamp" in log_data
        assert "level" in log_data
        assert "logger" in log_data
        assert "module" in log_data
        assert "message" in log_data
        assert "hostname" in log_data
        assert "thread_id" in log_data
        assert "process_id" in log_data
        
        # Check field values
        assert log_data["level"] == "INFO"
        assert log_data["message"] == "Test message"
        assert log_data["agent_name"] == "test_agent"
    
    def test_json_formatter_with_correlation_id(self):
        """Test JSON formatter with correlation ID."""
        correlation_id = "test_corr_123"
        self.logger.info("Test message", correlation_id=correlation_id)
        
        log_data = self.parse_log_json()
        
        assert log_data["correlation_id"] == correlation_id
        assert log_data["agent_name"] == "test_agent"
    
    def test_json_formatter_with_context(self):
        """Test JSON formatter with full log context."""
        context = LogContext(
            agent_name="test_agent",
            correlation_id="test_corr_456",
            module="test_module",
            task_id="test_task_789",
            feed_url="https://example.com/feed",
            extra_fields={"priority": "high", "retry_count": 3}
        )
        
        with LogContextManager(self.logger, context):
            self.logger.info("Test message with context")
        
        log_data = self.parse_log_json()
        
        assert log_data["correlation_id"] == "test_corr_456"
        assert log_data["task_id"] == "test_task_789"
        assert log_data["module"] == "test_module"
        assert log_data["feed_url"] == "https://example.com/feed"
        assert log_data["extra_fields"]["priority"] == "high"
        assert log_data["extra_fields"]["retry_count"] == 3
    
    def test_json_formatter_with_exception(self):
        """Test JSON formatter with exception information."""
        try:
            raise ValueError("Test exception")
        except ValueError:
            self.logger.error("Error occurred", exc_info=True)
        
        log_data = self.parse_log_json()
        
        assert "exception" in log_data
        assert log_data["exception"]["type"] == "ValueError"
        assert log_data["exception"]["message"] == "Test exception"
        assert "traceback" in log_data["exception"]
    
    def test_timestamp_format(self):
        """Test that timestamp is in ISO format."""
        self.logger.info("Test timestamp")
        
        log_data = self.parse_log_json()
        
        # Should be parseable as ISO datetime
        timestamp = datetime.fromisoformat(log_data["timestamp"].replace('Z', '+00:00'))
        assert isinstance(timestamp, datetime)
        assert timestamp.tzinfo is not None


class TestCorrelationContext:
    """Test cases for correlation context functionality."""
    
    def test_correlation_context_creation(self):
        """Test creating correlation context."""
        context = CorrelationContext(
            correlation_id="test_corr_123",
            task_id="test_task_456",
            agent_name="test_agent"
        )
        
        assert context.correlation_id == "test_corr_123"
        assert context.task_id == "test_task_456"
        assert context.agent_name == "test_agent"
        assert context.trace_id == "test_corr_123"  # Defaults to correlation_id
        assert isinstance(context.created_at, datetime)
    
    def test_correlation_context_auto_generation(self):
        """Test auto-generation of correlation IDs."""
        context = CorrelationContext()
        
        assert context.correlation_id is not None
        assert context.correlation_id.startswith("corr_")
        assert context.trace_id == context.correlation_id
    
    def test_correlation_context_headers(self):
        """Test conversion to/from headers."""
        original_context = CorrelationContext(
            correlation_id="test_corr_123",
            task_id="test_task_456",
            trace_id="test_trace_789",
            agent_name="test_agent",
            extra_context={"priority": "high"}
        )
        
        # Convert to headers
        headers = original_context.to_headers()
        
        assert headers["X-Correlation-ID"] == b"test_corr_123"
        assert headers["X-Task-ID"] == b"test_task_456"
        assert headers["X-Trace-ID"] == b"test_trace_789"
        assert headers["X-Agent-Name"] == b"test_agent"
        
        # Convert back from headers
        reconstructed = CorrelationContext.from_headers(headers)
        
        assert reconstructed.correlation_id == original_context.correlation_id
        assert reconstructed.task_id == original_context.task_id
        assert reconstructed.trace_id == original_context.trace_id
        assert reconstructed.agent_name == original_context.agent_name
        assert reconstructed.extra_context == original_context.extra_context
    
    def test_correlation_context_payload_injection(self):
        """Test injecting context into message payload."""
        context = CorrelationContext(
            correlation_id="test_corr_123",
            task_id="test_task_456",
            agent_name="test_agent"
        )
        
        payload = {"url": "https://example.com", "title": "Test Article"}
        enhanced_payload = context.inject_into_payload(payload)
        
        assert enhanced_payload["correlation_id"] == "test_corr_123"
        assert enhanced_payload["task_id"] == "test_task_456"
        assert enhanced_payload["agent_name"] == "test_agent"
        assert enhanced_payload["url"] == "https://example.com"  # Original data preserved
    
    def test_create_child_correlation_context(self):
        """Test creating child correlation context."""
        parent_context = CorrelationContext(
            correlation_id="parent_corr_123",
            task_id="parent_task_456",
            agent_name="parent_agent",
            extra_context={"priority": "high"}
        )
        
        child_context = create_child_correlation_context(
            parent_context,
            "child_agent",
            "child_task_789"
        )
        
        # Should preserve correlation and trace IDs
        assert child_context.correlation_id == "parent_corr_123"
        assert child_context.trace_id == "parent_corr_123"
        
        # Should have new task ID and agent name
        assert child_context.task_id == "child_task_789"
        assert child_context.agent_name == "child_agent"
        
        # Should preserve extra context
        assert child_context.extra_context == {"priority": "high"}


class TestKafkaCorrelationIntegration:
    """Test cases for Kafka correlation ID integration."""
    
    def test_extract_correlation_from_kafka_headers(self):
        """Test extracting correlation context from Kafka message headers."""
        # Mock Kafka message
        message = Mock()
        message.headers = [
            ("X-Correlation-ID", b"test_corr_123"),
            ("X-Task-ID", b"test_task_456"),
            ("X-Agent-Name", b"test_agent")
        ]
        message.value = {"url": "https://example.com"}
        
        context = extract_correlation_context_from_kafka_message(message)
        
        assert context.correlation_id == "test_corr_123"
        assert context.task_id == "test_task_456"
        assert context.agent_name == "test_agent"
    
    def test_extract_correlation_from_kafka_payload(self):
        """Test extracting correlation context from Kafka message payload."""
        # Mock Kafka message without headers
        message = Mock()
        message.headers = []
        message.value = {
            "url": "https://example.com",
            "correlation_id": "payload_corr_789",
            "task_id": "payload_task_123"
        }
        
        context = extract_correlation_context_from_kafka_message(message)
        
        assert context.correlation_id == "payload_corr_789"
        assert context.task_id == "payload_task_123"
    
    def test_inject_correlation_into_kafka_message(self):
        """Test injecting correlation context into Kafka message."""
        context = CorrelationContext(
            correlation_id="inject_corr_456",
            task_id="inject_task_789",
            agent_name="inject_agent"
        )
        
        payload = {"url": "https://example.com"}
        headers = {"existing-header": b"existing-value"}
        
        enhanced_payload, enhanced_headers = inject_correlation_context_into_kafka_message(
            context, payload, headers
        )
        
        # Check payload injection
        assert enhanced_payload["correlation_id"] == "inject_corr_456"
        assert enhanced_payload["task_id"] == "inject_task_789"
        assert enhanced_payload["url"] == "https://example.com"
        
        # Check header injection
        assert enhanced_headers["X-Correlation-ID"] == b"inject_corr_456"
        assert enhanced_headers["X-Task-ID"] == b"inject_task_789"
        assert enhanced_headers["existing-header"] == b"existing-value"
    
    @patch('kafka.KafkaProducer')
    def test_correlation_aware_kafka_producer(self, mock_producer_class):
        """Test correlation-aware Kafka producer."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        # Create logger mock
        logger = Mock()
        
        producer = CorrelationAwareKafkaProducer(mock_producer, "test_agent", logger)
        
        context = CorrelationContext(
            correlation_id="producer_corr_123",
            task_id="producer_task_456"
        )
        
        payload = {"url": "https://example.com"}
        
        # Send message with context
        producer.send_with_context("test.topic", payload, context)
        
        # Verify producer.send was called with enhanced payload and headers
        mock_producer.send.assert_called_once()
        call_args = mock_producer.send.call_args
        
        assert call_args[0][0] == "test.topic"  # topic
        sent_payload = call_args[1]["value"]
        sent_headers = dict(call_args[1]["headers"])
        
        assert sent_payload["correlation_id"] == "producer_corr_123"
        assert sent_headers["X-Correlation-ID"] == b"producer_corr_123"


class TestLoggingFunctionIntegration:
    """Test cases for logging function integration."""
    
    @patch('agents.common_tools.logging.get_logger')
    def test_log_function_entry_exit(self, mock_get_logger):
        """Test function entry/exit logging."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        correlation_id = "func_corr_123"
        
        # Test function entry
        with log_function_entry(mock_logger, "test_function", {"arg1": "value1"}, correlation_id):
            pass
        
        # Verify entry logging
        assert mock_logger.info.call_count >= 1
        entry_call = mock_logger.info.call_args_list[0]
        assert "Entering function: test_function" in entry_call[0][0]
        
        # Test function exit
        log_function_exit(mock_logger, "test_function", "test_result", 150, correlation_id)
        
        # Verify exit logging
        assert mock_logger.info.call_count >= 2
    
    @patch('agents.common_tools.logging.get_logger')
    def test_log_kafka_messages(self, mock_get_logger):
        """Test Kafka message logging functions."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        correlation_id = "kafka_corr_456"
        
        # Test message received logging
        log_kafka_message_received(
            mock_logger,
            topic="test.topic",
            partition=0,
            offset=12345,
            key="test_key",
            correlation_id=correlation_id,
            message_size=1024
        )
        
        # Verify received logging
        mock_logger.info.assert_called()
        call_args = mock_logger.info.call_args
        assert "Kafka message received" in call_args[0][0]
        assert call_args[1]["correlation_id"] == correlation_id
        
        # Reset mock
        mock_logger.reset_mock()
        
        # Test message sent logging
        log_kafka_message_sent(
            mock_logger,
            topic="test.topic",
            key="test_key",
            correlation_id=correlation_id,
            message_size=1024
        )
        
        # Verify sent logging
        mock_logger.info.assert_called()
        call_args = mock_logger.info.call_args
        assert "Kafka message sent" in call_args[0][0]
        assert call_args[1]["correlation_id"] == correlation_id
    
    @patch('agents.common_tools.logging.get_logger')
    def test_log_processing_error(self, mock_get_logger):
        """Test processing error logging."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        correlation_id = "error_corr_789"
        error = ValueError("Test error message")
        
        log_processing_error(
            mock_logger,
            error=error,
            context="test_context",
            correlation_id=correlation_id,
            recovery_action="retry_operation",
            extra_data={"attempt": 2, "max_retries": 3}
        )
        
        # Verify error logging
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        
        assert "Processing error in test_context" in call_args[0][0]
        assert call_args[1]["correlation_id"] == correlation_id
        
        extra_fields = call_args[1]["extra_fields"]
        assert extra_fields["error_type"] == "ValueError"
        assert extra_fields["error_message"] == "Test error message"
        assert extra_fields["recovery_action"] == "retry_operation"
        assert extra_fields["attempt"] == 2
    
    @patch('agents.common_tools.logging.get_logger')
    def test_log_performance_metric(self, mock_get_logger):
        """Test performance metric logging."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        correlation_id = "metric_corr_101"
        
        log_performance_metric(
            mock_logger,
            metric_name="processing_time",
            value=150.5,
            unit="ms",
            correlation_id=correlation_id,
            tags={"operation": "feed_processing", "agent": "rss_collector"}
        )
        
        # Verify metric logging
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        
        assert "Performance metric: processing_time=150.5ms" in call_args[0][0]
        assert call_args[1]["correlation_id"] == correlation_id
        
        extra_fields = call_args[1]["extra_fields"]
        assert extra_fields["metric_name"] == "processing_time"
        assert extra_fields["value"] == 150.5
        assert extra_fields["unit"] == "ms"
        assert extra_fields["tags"]["operation"] == "feed_processing"


class TestWithCorrelationContextDecorator:
    """Test cases for the correlation context decorator."""
    
    def test_decorator_with_existing_correlation_id(self):
        """Test decorator with existing correlation ID."""
        @with_correlation_context()
        def test_function(message, correlation_id=None, correlation_context=None):
            return {
                "message": message,
                "correlation_id": correlation_id,
                "context": correlation_context
            }
        
        result = test_function("test message", correlation_id="existing_corr_123")
        
        assert result["correlation_id"] == "existing_corr_123"
        assert result["context"].correlation_id == "existing_corr_123"
        assert result["message"] == "test message"
    
    def test_decorator_auto_generates_correlation_id(self):
        """Test decorator auto-generates correlation ID when none provided."""
        @with_correlation_context()
        def test_function(message, correlation_context=None):
            return {
                "message": message,
                "context": correlation_context
            }
        
        result = test_function("test message")
        
        assert result["context"] is not None
        assert result["context"].correlation_id.startswith("corr_")
        assert result["message"] == "test message"


class TestHelperFunctions:
    """Test cases for helper functions."""
    
    def test_create_correlation_id(self):
        """Test correlation ID generation."""
        id1 = create_correlation_id()
        id2 = create_correlation_id()
        
        assert id1.startswith("corr_")
        assert id2.startswith("corr_")
        assert id1 != id2
        assert len(id1) > 10  # Should have reasonable length
    
    def test_create_task_id(self):
        """Test task ID generation."""
        id1 = create_task_id()
        id2 = create_task_id()
        
        assert id1.startswith("task_")
        assert id2.startswith("task_")
        assert id1 != id2
        assert len(id1) > 15  # Should include timestamp and random part
    
    def test_create_child_logger(self):
        """Test creating child logger."""
        parent_logger = StructuredLogger("parent", "parent_agent")
        
        # Set up parent context
        parent_context = LogContext(
            agent_name="parent_agent",
            correlation_id="parent_corr_123",
            module="parent_module",
            task_id="parent_task_456"
        )
        parent_logger.push_context(parent_context)
        
        # Create child logger
        child_logger = create_child_logger(parent_logger, "child")
        
        # Check child inherits context
        child_context = child_logger.get_current_context()
        assert child_context is not None
        assert child_context.correlation_id == "parent_corr_123"
        assert child_context.task_id == "parent_task_456"
        assert child_logger.agent_name == "parent_agent.child"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])