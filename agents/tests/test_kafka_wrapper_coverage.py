"""
Additional tests for kafka_wrapper.py to increase test coverage.

This file focuses on testing previously uncovered code paths in kafka_wrapper.py
to improve production readiness.
"""

import json
import os
import tempfile
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from jsonschema import ValidationError

from common_tools.kafka_wrapper import SchemaRegistry, ValidatingKafkaProducer, ValidatingKafkaConsumer


class TestSchemaRegistryEdgeCases:
    """Test edge cases and error handling in SchemaRegistry."""
    
    def test_schema_registry_default_path(self):
        """Test SchemaRegistry initialization with default schema directory."""
        # This tests the lines around 145-150 that calculate the default path
        registry = SchemaRegistry()
        assert registry.schema_dir is not None
        assert isinstance(registry.schema_dir, Path)
    
    def test_schema_registry_nonexistent_directory(self, caplog):
        """Test SchemaRegistry with non-existent schema directory."""
        # This tests lines 155-157 (warning when directory doesn't exist)
        with tempfile.TemporaryDirectory() as tmpdir:
            non_existent = Path(tmpdir) / "nonexistent"
            
            registry = SchemaRegistry(str(non_existent))
            
            # Check that warning was logged
            assert "does not exist" in caplog.text
            assert len(registry._schemas) == 0
    
    def test_schema_registry_invalid_json_file(self, caplog):
        """Test SchemaRegistry handling of invalid JSON files."""
        # This tests the exception handling in _load_schemas (around lines 160-170)
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_dir = Path(tmpdir)
            
            # Create an invalid JSON file
            invalid_file = schema_dir / "invalid.json"
            invalid_file.write_text("{ invalid json }")
            
            registry = SchemaRegistry(str(schema_dir))
            
            # Should handle the JSON decode error gracefully
            assert "invalid" not in registry._schemas

    def test_get_schema_pattern_matching(self):
        """Test schema pattern matching logic."""
        # This tests lines 184-195 (pattern matching logic)
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_dir = Path(tmpdir)

            # Create test schemas
            schemas = {
                "raw.json": {"type": "object"},
                "raw_intel.json": {"type": "object", "properties": {"id": {"type": "string"}}},
                "agent.delegate.json": {"type": "object", "properties": {"name": {"type": "string"}}}
            }

            for filename, schema in schemas.items():
                (schema_dir / filename).write_text(json.dumps(schema))

            registry = SchemaRegistry(str(schema_dir))

            # Test exact match
            assert registry.get_schema("raw") is not None

            # Test base topic match (first 2 parts: "agent.delegate.something" -> "agent.delegate")
            assert registry.get_schema("agent.delegate.specific") is not None

            # Test underscore replacement (raw_intel -> raw.intel mapping)
            assert registry.get_schema("raw.intel") is not None  # Should match raw_intel.json
            
            # Test no match
            assert registry.get_schema("nonexistent.topic") is None
    
    def test_get_validator_pattern_matching(self):
        """Test validator pattern matching logic."""
        # This tests lines 200-210 (validator pattern matching)
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_dir = Path(tmpdir)
            
            # Create test schema
            schema = {"type": "object", "properties": {"test": {"type": "string"}}}
            (schema_dir / "test_topic.json").write_text(json.dumps(schema))
            
            registry = SchemaRegistry(str(schema_dir))
            
            # Test exact match
            validator = registry.get_validator("test_topic")
            assert validator is not None
            
            # Test underscore replacement
            validator = registry.get_validator("test.topic")
            assert validator is not None
            
            # Test no match
            assert registry.get_validator("nonexistent") is None


class TestValidatingKafkaProducerCoverage:
    """Test uncovered code paths in ValidatingKafkaProducer."""
    
    @patch('common_tools.kafka_wrapper.KafkaProducer')
    def test_producer_initialization_with_custom_dlq(self, mock_kafka_producer):
        """Test producer initialization with custom DLQ producer."""
        # This tests lines around 225-240 (initialization paths)
        mock_producer = MagicMock()
        mock_dlq_producer = MagicMock()
        mock_kafka_producer.side_effect = [mock_producer, mock_dlq_producer]
        
        custom_dlq = MagicMock()
        producer = ValidatingKafkaProducer(
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            dlq_producer=custom_dlq
        )
        
        assert producer.dlq_producer == custom_dlq
        assert producer.agent_name == "test_agent"
    
    @patch('common_tools.kafka_wrapper.KafkaProducer')
    def test_producer_without_schema_registry(self, mock_kafka_producer):
        """Test producer behavior when no schema registry is provided."""
        # Tests default schema registry creation
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer
        
        producer = ValidatingKafkaProducer(
            bootstrap_servers="localhost:9092",
            agent_name="test_agent"
        )
        
        assert producer.schema_registry is not None
    
    @patch('common_tools.kafka_wrapper.KafkaProducer')
    def test_send_without_validator(self, mock_kafka_producer):
        """Test sending message when no validator is available."""
        # This tests lines around 285-291 (no validator path)
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer
        
        mock_registry = MagicMock()
        mock_registry.get_validator.return_value = None
        
        producer = ValidatingKafkaProducer(
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            schema_registry=mock_registry
        )
        
        # Should send successfully even without validator
        result = producer.send("test.topic", {"data": "test"})
        
        # Should have called the underlying producer
        mock_producer.send.assert_called_once()

    @patch('common_tools.kafka_wrapper.KafkaProducer')
    def test_send_with_correlation_id(self, mock_kafka_producer):
        """Test sending message with correlation ID."""
        # Tests correlation ID handling in send method
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer

        mock_registry = MagicMock()
        mock_registry.get_validator.return_value = None

        producer = ValidatingKafkaProducer(
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            schema_registry=mock_registry
        )

        # Create a message with correlation_id
        test_message = {"data": "test", "correlation_id": "test-123"}
        result = producer.send("test.topic", test_message)

        # Check that send was called with the message
        mock_producer.send.assert_called_once()
        call_args = mock_producer.send.call_args
        
        # Verify the message was passed correctly
        assert call_args[0][0] == "test.topic"  # topic
        assert call_args[1]['value'] == test_message  # message value
    
    @patch('common_tools.kafka_wrapper.KafkaProducer')
    def test_flush_and_close_methods(self, mock_kafka_producer):
        """Test flush and close methods."""
        # Tests lines around 358-368 (flush/close methods)
        mock_producer = MagicMock()
        mock_dlq_producer = MagicMock()
        mock_kafka_producer.side_effect = [mock_producer, mock_dlq_producer]
        
        producer = ValidatingKafkaProducer(
            bootstrap_servers="localhost:9092",
            agent_name="test_agent"
        )
        
        # Test flush
        producer.flush(timeout=10)
        mock_producer.flush.assert_called_with(10)
        mock_dlq_producer.flush.assert_called_with(10)
        
        # Test close
        producer.close(timeout=5)
        mock_producer.close.assert_called_with(5)
        mock_dlq_producer.close.assert_called_with(5)


class TestValidatingKafkaConsumerCoverage:
    """Test uncovered code paths in ValidatingKafkaConsumer."""
    
    @patch('common_tools.kafka_wrapper.KafkaConsumer')
    @patch('common_tools.kafka_wrapper.KafkaProducer')
    def test_consumer_initialization_options(self, mock_producer, mock_consumer):
        """Test consumer initialization with various options."""
        # Tests initialization paths around lines 400-430
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance
        
        mock_dlq_producer = MagicMock()
        mock_producer.return_value = mock_dlq_producer
        
        # Test with custom error handler
        error_handler = MagicMock()
        
        consumer = ValidatingKafkaConsumer(
            topics=["test.topic"],
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            group_id="test_group",
            error_handler=error_handler,
            auto_offset_reset="earliest"
        )
        
        assert consumer.error_handler == error_handler
        assert consumer.agent_name == "test_agent"
        
        # Verify consumer was created with correct parameters
        mock_consumer.assert_called_once()
        call_kwargs = mock_consumer.call_args[1]
        assert call_kwargs['auto_offset_reset'] == 'earliest'
    
    @patch('common_tools.kafka_wrapper.KafkaConsumer')
    @patch('common_tools.kafka_wrapper.KafkaProducer')
    def test_consumer_iteration_without_validator(self, mock_producer, mock_consumer):
        """Test consuming messages when no validator is available."""
        # Tests the path around lines 460-470 (no validator)
        mock_message = MagicMock()
        mock_message.topic = "test.topic"
        mock_message.value = {"data": "test"}
        
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.__iter__.return_value = iter([mock_message])
        mock_consumer.return_value = mock_consumer_instance
        
        mock_dlq_producer = MagicMock()
        mock_producer.return_value = mock_dlq_producer
        
        mock_registry = MagicMock()
        mock_registry.get_validator.return_value = None
        
        consumer = ValidatingKafkaConsumer(
            topics=["test.topic"],
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            group_id="test_group",
            schema_registry=mock_registry
        )
        
        messages = list(consumer)
        
        # Should return the message even without validation
        assert len(messages) == 1
        assert messages[0] == mock_message
    
    @patch('common_tools.kafka_wrapper.KafkaConsumer')
    @patch('common_tools.kafka_wrapper.KafkaProducer')
    def test_close_method(self, mock_producer, mock_consumer):
        """Test consumer close method."""
        # Tests lines around 515-520 (close method)
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance
        
        mock_dlq_producer = MagicMock()
        mock_producer.return_value = mock_dlq_producer
        
        consumer = ValidatingKafkaConsumer(
            topics=["test.topic"],
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            group_id="test_group"
        )
        
        consumer.close()
        
        mock_consumer_instance.close.assert_called_once()
        mock_dlq_producer.close.assert_called_once()


class TestDLQFunctionality:
    """Test Dead Letter Queue functionality."""
    
    @patch('common_tools.kafka_wrapper.KafkaProducer')
    def test_dlq_message_structure(self, mock_kafka_producer):
        """Test the structure of DLQ messages."""
        # Tests DLQ message creation around lines 320-350
        mock_producer = MagicMock()
        mock_dlq_producer = MagicMock()
        mock_kafka_producer.side_effect = [mock_producer, mock_dlq_producer]
        
        # Create a proper ValidationError
        validation_error = ValidationError("Test validation error")
        
        mock_validator = MagicMock()
        mock_validator.validate.side_effect = validation_error
        
        mock_registry = MagicMock()
        mock_registry.get_validator.return_value = mock_validator
        
        producer = ValidatingKafkaProducer(
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            schema_registry=mock_registry
        )
        
        test_message = {"invalid": "data"}
        # Should return None when validation fails and message goes to DLQ
        result = producer.send("test.topic", test_message)
        
        # Should return None for validation failures
        assert result is None
        
        # Should have sent to DLQ
        mock_dlq_producer.send.assert_called_once()
        
        # Check DLQ message structure
        dlq_call = mock_dlq_producer.send.call_args
        dlq_topic = dlq_call[0][0]
        dlq_message = dlq_call[1]['value']
        
        assert dlq_topic == "test.topic.dlq"
        assert 'original_topic' in dlq_message
        assert 'original_payload' in dlq_message  # Based on actual implementation
        assert 'error' in dlq_message
        assert 'metadata' in dlq_message
        assert 'timestamp' in dlq_message
        assert dlq_message['original_topic'] == "test.topic"


class TestErrorHandling:
    """Test error handling scenarios."""
    
    @patch('common_tools.kafka_wrapper.KafkaProducer')
    def test_producer_kafka_error_handling(self, mock_kafka_producer):
        """Test handling of Kafka errors in producer."""
        # Tests error handling paths - Kafka errors should propagate up
        mock_producer = MagicMock()
        mock_producer.send.side_effect = Exception("Kafka connection error")
        mock_kafka_producer.return_value = mock_producer
        
        producer = ValidatingKafkaProducer(
            bootstrap_servers="localhost:9092",
            agent_name="test_agent"
        )
        
        # Kafka connection errors should propagate up (not handled gracefully)
        with pytest.raises(Exception, match="Kafka connection error"):
            producer.send("test.topic", {"data": "test"})
    
    @patch('common_tools.kafka_wrapper.KafkaConsumer')
    @patch('common_tools.kafka_wrapper.KafkaProducer')
    def test_consumer_error_handler_callback(self, mock_producer, mock_consumer):
        """Test consumer error handler callback functionality."""
        # Tests error handler callback path
        mock_message = MagicMock()
        mock_message.topic = "test.topic"
        mock_message.value = {"invalid": "data"}
        
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.__iter__.return_value = iter([mock_message])
        mock_consumer.return_value = mock_consumer_instance
        
        mock_dlq_producer = MagicMock()
        mock_producer.return_value = mock_dlq_producer
        
        # Mock failing validator with ValidationError (not generic Exception)
        mock_validator = MagicMock()
        validation_error = ValidationError("Validation error")
        mock_validator.validate.side_effect = validation_error
        
        mock_registry = MagicMock()
        mock_registry.get_validator.return_value = mock_validator
        
        # Custom error handler
        error_handler = MagicMock()
        
        consumer = ValidatingKafkaConsumer(
            topics=["test.topic"],
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            group_id="test_group",
            schema_registry=mock_registry,
            error_handler=error_handler
        )
        
        # Consume messages - should handle ValidationError gracefully
        messages = list(consumer)
        
        # Should return empty list (no valid messages)
        assert messages == []
        
        # Error handler should have been called
        error_handler.assert_called_once_with(mock_message.value, validation_error)
        
        # DLQ producer should have been called
        mock_dlq_producer.send.assert_called_once()
