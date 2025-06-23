"""
Comprehensive tests for context.py to improve coverage to 90%.

This test file adds coverage for areas not covered by test_logging_correlation.py,
specifically focusing on:
- CorrelationContext.to_dict() method
- Error handling in from_headers with JSON decode errors
- Fallback paths in extract_correlation_context_from_kafka_message
- CorrelationAwareKafkaProducer.send_auto_context method
- CorrelationAwareKafkaConsumer full functionality
- propagate_context_to_feedrecord and extract_context_from_feedrecord
- Edge cases in with_correlation_context decorator
"""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, Any

from agents.common_tools.context import (
    CorrelationContext,
    extract_correlation_context_from_kafka_message,
    inject_correlation_context_into_kafka_message,
    CorrelationAwareKafkaProducer,
    CorrelationAwareKafkaConsumer,
    create_child_correlation_context,
    propagate_context_to_feedrecord,
    extract_context_from_feedrecord,
    with_correlation_context,
    CORRELATION_ID_HEADER,
    CORRELATION_ID_FIELD,
    TASK_ID_HEADER,
    TASK_ID_FIELD,
    TRACE_ID_HEADER,
    AGENT_NAME_HEADER
)


class TestCorrelationContextMethods:
    """Test CorrelationContext methods not covered in other tests."""
    
    def test_to_dict_method(self):
        """Test the to_dict method of CorrelationContext."""
        context = CorrelationContext(
            correlation_id="test-corr-123",
            task_id="task-456",
            trace_id="trace-789",
            agent_name="test_agent",
            parent_span="parent_agent",
            extra_context={"key": "value", "nested": {"data": 123}}
        )
        
        result = context.to_dict()
        
        assert result["correlation_id"] == "test-corr-123"
        assert result["task_id"] == "task-456"
        assert result["trace_id"] == "trace-789"
        assert result["agent_name"] == "test_agent"
        assert result["parent_span"] == "parent_agent"
        assert result["extra_context"] == {"key": "value", "nested": {"data": 123}}
        assert "created_at" in result
        
        # Verify created_at is ISO format
        created_at = datetime.fromisoformat(result["created_at"])
        assert created_at.tzinfo is not None  # Should have timezone info
    
    def test_from_headers_json_decode_error(self):
        """Test from_headers handling of JSON decode errors."""
        headers = {
            CORRELATION_ID_HEADER: b"test-corr-123",
            "X-Extra-Context": b"invalid json {not valid"  # Invalid JSON
        }
        
        context = CorrelationContext.from_headers(headers)
        
        # Should handle JSON decode error gracefully
        assert context.correlation_id == "test-corr-123"
        assert context.extra_context == {}  # Should default to empty dict
    
    def test_from_headers_unicode_decode_error(self):
        """Test from_headers handling of Unicode decode errors."""
        headers = {
            CORRELATION_ID_HEADER: b"test-corr-123",
            "X-Extra-Context": b"\xff\xfe invalid unicode"  # Invalid UTF-8
        }
        
        context = CorrelationContext.from_headers(headers)
        
        # Should handle Unicode decode error gracefully
        assert context.correlation_id == "test-corr-123"
        assert context.extra_context == {}  # Should default to empty dict


class TestCorrelationContextExtraction:
    """Test edge cases in correlation context extraction."""
    
    def test_extract_correlation_context_fallback_to_empty(self):
        """Test fallback to empty context when nothing is found."""
        # Mock message with no headers and no correlation ID in value
        mock_message = Mock()
        mock_message.headers = None
        mock_message.value = {"data": "test", "no_correlation": True}
        
        context = extract_correlation_context_from_kafka_message(mock_message)
        
        # Should return a valid context with generated correlation ID
        assert context is not None
        assert context.correlation_id is not None
        assert len(context.correlation_id) > 0
    
    def test_extract_correlation_context_no_headers_attr(self):
        """Test extraction when message has no headers attribute."""
        mock_message = Mock(spec=[])  # No headers attribute
        mock_message.value = {"data": "test"}
        
        context = extract_correlation_context_from_kafka_message(mock_message)
        
        # Should handle gracefully and return context
        assert context is not None
        assert context.correlation_id is not None
    
    def test_extract_correlation_context_non_dict_value(self):
        """Test extraction when message value is not a dict."""
        mock_message = Mock()
        mock_message.headers = []
        mock_message.value = "string value, not dict"
        
        context = extract_correlation_context_from_kafka_message(mock_message)
        
        # Should handle gracefully and return context
        assert context is not None
        assert context.correlation_id is not None


class TestCorrelationAwareKafkaProducer:
    """Test CorrelationAwareKafkaProducer functionality."""
    
    def test_send_auto_context(self):
        """Test send_auto_context method."""
        mock_producer = Mock()
        mock_producer.send.return_value = Mock()  # Future mock
        
        producer = CorrelationAwareKafkaProducer(
            producer=mock_producer,
            agent_name="test_agent"
        )
        
        # Test with auto-generated correlation ID
        result = producer.send_auto_context(
            topic="test.topic",
            value={"data": "test"},
            key="test-key"
        )
        
        # Verify send was called
        mock_producer.send.assert_called_once()
        call_args = mock_producer.send.call_args
        
        # Check topic
        assert call_args[0][0] == "test.topic"
        
        # Check value has correlation context injected
        sent_value = call_args[1]["value"]
        assert CORRELATION_ID_FIELD in sent_value
        assert sent_value["data"] == "test"
        
        # Check headers were added
        headers = call_args[1]["headers"]
        header_dict = dict(headers)
        assert CORRELATION_ID_HEADER in header_dict
        assert AGENT_NAME_HEADER in header_dict
        assert header_dict[AGENT_NAME_HEADER] == b"test_agent"
    
    def test_send_auto_context_with_provided_ids(self):
        """Test send_auto_context with provided correlation and task IDs."""
        mock_producer = Mock()
        mock_producer.send.return_value = Mock()
        
        producer = CorrelationAwareKafkaProducer(
            producer=mock_producer,
            agent_name="test_agent"
        )
        
        # Test with provided IDs
        result = producer.send_auto_context(
            topic="test.topic",
            value={"data": "test"},
            correlation_id="provided-corr-123",
            task_id="provided-task-456",
            partition=2
        )
        
        # Verify send was called with provided IDs
        call_args = mock_producer.send.call_args
        sent_value = call_args[1]["value"]
        
        assert sent_value[CORRELATION_ID_FIELD] == "provided-corr-123"
        assert sent_value[TASK_ID_FIELD] == "provided-task-456"
        
        # Check headers
        headers = dict(call_args[1]["headers"])
        assert headers[CORRELATION_ID_HEADER] == b"provided-corr-123"
        assert headers[TASK_ID_HEADER] == b"provided-task-456"


class TestCorrelationAwareKafkaConsumer:
    """Test CorrelationAwareKafkaConsumer functionality."""
    
    def test_consumer_initialization(self):
        """Test CorrelationAwareKafkaConsumer initialization."""
        mock_consumer = Mock()
        mock_logger = Mock()
        
        consumer = CorrelationAwareKafkaConsumer(
            consumer=mock_consumer,
            agent_name="test_agent",
            logger=mock_logger
        )
        
        assert consumer.consumer == mock_consumer
        assert consumer.agent_name == "test_agent"
        assert consumer.logger == mock_logger
    
    def test_poll_with_context(self):
        """Test poll_with_context method."""
        mock_consumer = Mock()
        mock_logger = Mock()
        
        # Create mock messages
        mock_message1 = Mock()
        mock_message1.headers = [(CORRELATION_ID_HEADER, b"corr-123")]
        mock_message1.value = {"data": "test1"}
        mock_message1.topic = "test.topic"
        mock_message1.partition = 0
        mock_message1.offset = 100
        mock_message1.key = b"key1"
        
        mock_message2 = Mock()
        mock_message2.headers = []
        mock_message2.value = {"data": "test2", CORRELATION_ID_FIELD: "corr-456"}
        mock_message2.topic = "test.topic"
        mock_message2.partition = 1
        mock_message2.offset = 200
        mock_message2.key = None
        
        # Mock poll to return messages
        mock_consumer.poll.return_value = {
            Mock(): [mock_message1, mock_message2]
        }
        
        consumer = CorrelationAwareKafkaConsumer(
            consumer=mock_consumer,
            agent_name="test_agent",
            logger=mock_logger
        )
        
        # Poll and collect results
        results = list(consumer.poll_with_context(timeout_ms=1000, max_records=10))
        
        # Verify poll was called correctly
        mock_consumer.poll.assert_called_once_with(timeout_ms=1000, max_records=10)
        
        # Verify we got both messages with contexts
        assert len(results) == 2
        
        # Check first message
        message1, context1 = results[0]
        assert message1 == mock_message1
        assert context1.correlation_id == "corr-123"
        
        # Check second message
        message2, context2 = results[1]
        assert message2 == mock_message2
        assert context2.correlation_id == "corr-456"
        
        # Verify logging imports were attempted (actual logging happens through imported module)
        # The consumer should work even if logging isn't directly called on the mock
    
    def test_poll_with_context_no_logger(self):
        """Test poll_with_context without logger."""
        mock_consumer = Mock()
        
        # Mock empty poll result
        mock_consumer.poll.return_value = {}
        
        consumer = CorrelationAwareKafkaConsumer(
            consumer=mock_consumer,
            agent_name="test_agent",
            logger=None  # No logger
        )
        
        results = list(consumer.poll_with_context())
        
        # Should work without logger
        assert results == []
        mock_consumer.poll.assert_called_once()


class TestFeedRecordIntegration:
    """Test FeedRecord integration functions."""
    
    def test_propagate_context_to_feedrecord(self):
        """Test propagating context to a FeedRecord."""
        # Create mock FeedRecord with metadata
        mock_feedrecord = Mock()
        mock_metadata = Mock()
        mock_metadata.additional = {"existing": "data"}
        mock_feedrecord.metadata = mock_metadata
        
        context = CorrelationContext(
            correlation_id="test-corr-123",
            task_id="task-456",
            trace_id="trace-789"
        )
        
        propagate_context_to_feedrecord(context, mock_feedrecord)
        
        # Verify correlation ID was set
        assert mock_metadata.correlation_id == "test-corr-123"
        
        # Verify task_id and trace_id were added to additional
        assert mock_metadata.additional["task_id"] == "task-456"
        assert mock_metadata.additional["trace_id"] == "trace-789"
        assert mock_metadata.additional["existing"] == "data"  # Preserved
    
    def test_propagate_context_to_feedrecord_no_additional(self):
        """Test propagating context when metadata has no additional field."""
        mock_feedrecord = Mock()
        mock_metadata = Mock(spec=["correlation_id"])  # No additional attribute
        mock_feedrecord.metadata = mock_metadata
        
        context = CorrelationContext(
            correlation_id="test-corr-123",
            task_id="task-456"
        )
        
        propagate_context_to_feedrecord(context, mock_feedrecord)
        
        # Should only set correlation_id
        assert mock_metadata.correlation_id == "test-corr-123"
    
    def test_propagate_context_to_feedrecord_no_metadata(self):
        """Test propagating context when feedrecord has no metadata."""
        mock_feedrecord = Mock(spec=[])  # No metadata attribute
        
        context = CorrelationContext(correlation_id="test-corr-123")
        
        # Should handle gracefully (no exception)
        propagate_context_to_feedrecord(context, mock_feedrecord)
    
    def test_extract_context_from_feedrecord(self):
        """Test extracting context from a FeedRecord."""
        # Create mock FeedRecord with full metadata
        mock_feedrecord = Mock()
        mock_metadata = Mock()
        mock_metadata.correlation_id = "test-corr-123"
        mock_metadata.additional = {
            "task_id": "task-456",
            "trace_id": "trace-789"
        }
        mock_feedrecord.metadata = mock_metadata
        
        context = extract_context_from_feedrecord(mock_feedrecord)
        
        assert context is not None
        assert context.correlation_id == "test-corr-123"
        assert context.task_id == "task-456"
        assert context.trace_id == "trace-789"
    
    def test_extract_context_from_feedrecord_no_metadata(self):
        """Test extracting context when feedrecord has no metadata."""
        mock_feedrecord = Mock(spec=[])  # No metadata attribute
        
        context = extract_context_from_feedrecord(mock_feedrecord)
        
        assert context is None
    
    def test_extract_context_from_feedrecord_no_correlation_id(self):
        """Test extracting context when metadata has no correlation_id."""
        mock_feedrecord = Mock()
        mock_metadata = Mock()
        mock_metadata.correlation_id = None
        mock_feedrecord.metadata = mock_metadata
        
        context = extract_context_from_feedrecord(mock_feedrecord)
        
        assert context is None
    
    def test_extract_context_from_feedrecord_no_additional(self):
        """Test extracting context when metadata has no additional field."""
        mock_feedrecord = Mock()
        mock_metadata = Mock(spec=["correlation_id"])
        mock_metadata.correlation_id = "test-corr-123"
        mock_feedrecord.metadata = mock_metadata
        
        context = extract_context_from_feedrecord(mock_feedrecord)
        
        assert context is not None
        assert context.correlation_id == "test-corr-123"
        assert context.task_id is None
        # trace_id defaults to correlation_id when not provided
        assert context.trace_id == "test-corr-123"


class TestWithCorrelationContextDecorator:
    """Test edge cases in with_correlation_context decorator."""
    
    def test_decorator_with_correlation_id_in_first_arg(self):
        """Test decorator extracting correlation_id from first argument."""
        mock_logger = Mock()
        
        @with_correlation_context(logger=mock_logger)
        def test_function(obj, value, **kwargs):
            # Check that correlation_context was added to kwargs
            assert "correlation_context" in kwargs
            context = kwargs["correlation_context"]
            assert context.correlation_id == "arg-corr-123"
            return value * 2
        
        # Create object with correlation_id attribute
        mock_obj = Mock()
        mock_obj.correlation_id = "arg-corr-123"
        
        result = test_function(mock_obj, 5)
        
        assert result == 10
        
        # Verify logging was called (decorator uses logger.info for entry/exit)
        assert mock_logger.info.call_count >= 2  # Entry and exit
    
    def test_decorator_with_exception(self):
        """Test decorator handling exceptions."""
        mock_logger = Mock()
        
        @with_correlation_context(logger=mock_logger)
        def failing_function(value, **kwargs):
            raise ValueError("Test error")
        
        with pytest.raises(ValueError, match="Test error"):
            failing_function(42)
        
        # Verify error logging was called (decorator uses logger.error for exceptions)
        assert mock_logger.error.call_count >= 1
    
    def test_decorator_without_logger(self):
        """Test decorator without logger."""
        @with_correlation_context(logger=None)
        def test_function(value, **kwargs):
            return value * 2
        
        result = test_function(42)
        
        # Should work without logger
        assert result == 84
    
    def test_decorator_auto_generates_context(self):
        """Test decorator auto-generating context when none found."""
        @with_correlation_context()
        def test_function(**kwargs):
            # Should have correlation_context in kwargs
            assert "correlation_context" in kwargs
            context = kwargs["correlation_context"]
            assert context is not None
            assert context.correlation_id is not None
            return context.correlation_id
        
        result = test_function()
        
        # Should return a valid correlation ID
        assert result is not None
        assert len(result) > 0


if __name__ == "__main__":
    pytest.main([__file__])