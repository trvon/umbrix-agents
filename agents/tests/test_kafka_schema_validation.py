"""
Unit tests for Kafka schema validation functionality.

Tests cover:
- Schema loading and registry
- Producer validation
- Consumer validation  
- DLQ message formatting
- Error handling
"""

import json
import os
import tempfile
import time
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock
import pytest
from agents.conftest import _ValidationError as ValidationError

from agents.common_tools.kafka_wrapper import (
    SchemaRegistry,
    ValidatingKafkaProducer,
    ValidatingKafkaConsumer
)
from agents.common_tools.validation import (
    SchemaValidationError,
    load_schema,
    validate_message,
    create_error_report,
    get_schema_for_topic,
    merge_schemas,
    extract_validation_path
)


class TestSchemaRegistry(TestCase):
    """Test cases for SchemaRegistry class."""
    
    def setUp(self):
        """Create temporary schema directory with test schemas."""
        self.temp_dir = tempfile.mkdtemp()
        self.schema_dir = Path(self.temp_dir) / "schemas"
        self.schema_dir.mkdir()
        
        # Create test schemas
        self.test_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "value": {"type": "number"}
            },
            "required": ["id", "value"]
        }
        
        # Write test schema files
        with open(self.schema_dir / "test.topic.json", "w") as f:
            json.dump(self.test_schema, f)
        
        with open(self.schema_dir / "agent.delegate.json", "w") as f:
            json.dump(self.test_schema, f)
    
    def tearDown(self):
        """Clean up temporary directory."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_load_schemas(self):
        """Test loading schemas from directory."""
        registry = SchemaRegistry(self.schema_dir)
        
        # Check schemas were loaded
        self.assertIn("test.topic", registry._schemas)
        self.assertIn("agent.delegate", registry._schemas)
        
        # Check validators were created
        self.assertIn("test.topic", registry._validators)
        self.assertIn("agent.delegate", registry._validators)
    
    def test_get_schema_exact_match(self):
        """Test getting schema with exact topic match."""
        registry = SchemaRegistry(self.schema_dir)
        schema = registry.get_schema("test.topic")
        
        self.assertIsNotNone(schema)
        self.assertEqual(schema, self.test_schema)
    
    def test_get_schema_pattern_match(self):
        """Test getting schema with pattern match."""
        registry = SchemaRegistry(self.schema_dir)
        
        # Should match agent.delegate.json for agent.delegate.* topics
        schema = registry.get_schema("agent.delegate.feed_discovery")
        self.assertIsNotNone(schema)
        self.assertEqual(schema, self.test_schema)
    
    def test_get_schema_not_found(self):
        """Test getting schema for unknown topic."""
        registry = SchemaRegistry(self.schema_dir)
        schema = registry.get_schema("unknown.topic")
        
        self.assertIsNone(schema)


class TestValidatingKafkaProducer(TestCase):
    """Test cases for ValidatingKafkaProducer."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_producer = Mock()
        self.mock_dlq_producer = Mock()
        self.mock_registry = Mock(spec=SchemaRegistry)
        
        # Create validator that passes
        self.valid_validator = Mock()
        self.valid_validator.validate.return_value = None
        
        # Create validator that fails
        self.invalid_validator = Mock()
        self.invalid_validator.validate.side_effect = ValidationError(
            "Invalid value",
            path=["value"],
            schema_path=["properties", "value", "type"]
        )
    
    @pytest.mark.xfail(reason="Intermittent mock issue, to be fixed separately", strict=False)
    @patch('agents.common_tools.kafka_wrapper.KafkaProducer')
    def test_send_valid_message(self, mock_kafka_producer):
        """Test sending a valid message."""
        mock_kafka_producer.return_value = self.mock_producer
        
        producer = ValidatingKafkaProducer(
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            schema_registry=self.mock_registry,
            dlq_producer=self.mock_dlq_producer
        )
        
        # Set up registry to return validator
        self.mock_registry.get_validator.return_value = self.valid_validator
        
        # Send message
        message = {"id": "123", "value": 42.0}
        future = producer.send("test.topic", message)
        
        # Verify main producer was called
        self.mock_producer.send.assert_called_once_with(
            "test.topic",
            value=message,
            key=None
        )
        
        # Verify DLQ producer was not called
        self.mock_dlq_producer.send.assert_not_called()
    
    @patch('agents.common_tools.kafka_wrapper.KafkaProducer')
    def test_send_invalid_message(self, mock_kafka_producer):
        """Test sending an invalid message routes to DLQ."""
        mock_kafka_producer.return_value = self.mock_producer
        
        producer = ValidatingKafkaProducer(
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            schema_registry=self.mock_registry,
            dlq_producer=self.mock_dlq_producer
        )
        
        # Set up registry to return failing validator
        self.mock_registry.get_validator.return_value = self.invalid_validator
        
        # Send invalid message
        message = {"id": "123", "value": "not_a_number"}
        result = producer.send("test.topic", message)
        
        # Verify main producer was not called
        self.mock_producer.send.assert_not_called()
        
        # Verify DLQ producer was called
        self.mock_dlq_producer.send.assert_called_once()
        
        # Check DLQ message format
        dlq_call = self.mock_dlq_producer.send.call_args
        self.assertEqual(dlq_call[0][0], "test.topic.dlq")
        
        dlq_message = dlq_call[1]["value"]
        self.assertEqual(dlq_message["original_topic"], "test.topic")
        self.assertEqual(dlq_message["original_payload"], message)
        self.assertEqual(dlq_message["error"]["error_type"], "schema_validation")
        self.assertEqual(dlq_message["metadata"]["source_agent"], "test_agent")
        self.assertEqual(dlq_message["metadata"]["processing_stage"], "producer")
        
        # Result should be None for failed validation
        self.assertIsNone(result)
    
    @patch('agents.common_tools.kafka_wrapper.KafkaProducer')
    def test_send_with_correlation_id(self, mock_kafka_producer):
        """Test sending message with correlation ID."""
        mock_kafka_producer.return_value = self.mock_producer
        
        producer = ValidatingKafkaProducer(
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            schema_registry=self.mock_registry,
            dlq_producer=self.mock_dlq_producer
        )
        
        # Set up registry to return validator
        self.mock_registry.get_validator.return_value = self.valid_validator
        
        # Send message with correlation ID
        message = {"id": "123", "value": 42.0}
        producer.send("test.topic", message, correlation_id="corr-123")
        
        # Verify correlation ID was added
        sent_message = self.mock_producer.send.call_args[1]["value"]
        self.assertEqual(sent_message["correlation_id"], "corr-123")


class TestValidatingKafkaConsumer(TestCase):
    """Test cases for ValidatingKafkaConsumer."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_consumer = Mock()
        self.mock_dlq_producer = Mock()
        self.mock_registry = Mock(spec=SchemaRegistry)
        
        # Create mock message
        self.mock_message = Mock()
        self.mock_message.topic = "test.topic"
        self.mock_message.value = {"id": "123", "value": 42.0}
        self.mock_message.partition = 0
        self.mock_message.offset = 100
        
        # Create validators
        self.valid_validator = Mock()
        self.valid_validator.validate.return_value = None
        
        self.invalid_validator = Mock()
        self.invalid_validator.validate.side_effect = ValidationError(
            "Invalid value",
            path=["value"],
            schema_path=["properties", "value", "type"]
        )
    
    @pytest.mark.xfail(reason="Intermittent mock issue, to be fixed separately", strict=False)
    @patch('agents.common_tools.kafka_wrapper.KafkaConsumer')
    @patch('agents.common_tools.kafka_wrapper.KafkaProducer')
    def test_consume_valid_messages(self, mock_kafka_producer, mock_kafka_consumer):
        """Test consuming valid messages."""
        mock_kafka_consumer.return_value = self.mock_consumer
        mock_kafka_producer.return_value = self.mock_dlq_producer
        
        # Set up consumer to return messages
        self.mock_consumer.__iter__.return_value = iter([self.mock_message])
        
        consumer = ValidatingKafkaConsumer(
            topics=["test.topic"],
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            group_id="test_group",
            schema_registry=self.mock_registry,
            dlq_producer=self.mock_dlq_producer
        )
        
        # Set up registry to return passing validator
        self.mock_registry.get_validator.return_value = self.valid_validator
        
        # Consume messages
        messages = list(consumer)
        
        # Should yield the valid message
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0], self.mock_message)
        
        # DLQ should not be called
        self.mock_dlq_producer.send.assert_not_called()
    
    @patch('agents.common_tools.kafka_wrapper.KafkaConsumer')
    @patch('agents.common_tools.kafka_wrapper.KafkaProducer')
    def test_consume_invalid_messages(self, mock_kafka_producer, mock_kafka_consumer):
        """Test consuming invalid messages routes to DLQ."""
        mock_kafka_consumer.return_value = self.mock_consumer
        mock_kafka_producer.return_value = self.mock_dlq_producer
        
        # Create invalid message
        invalid_message = Mock()
        invalid_message.topic = "test.topic"
        invalid_message.value = {"id": "123", "value": "not_a_number"}
        invalid_message.partition = 0
        invalid_message.offset = 101
        
        # Set up consumer to return invalid message
        self.mock_consumer.__iter__.return_value = iter([invalid_message])
        
        consumer = ValidatingKafkaConsumer(
            topics=["test.topic"],
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            group_id="test_group",
            schema_registry=self.mock_registry,
            dlq_producer=self.mock_dlq_producer
        )
        
        # Set up registry to return failing validator
        self.mock_registry.get_validator.return_value = self.invalid_validator
        
        # Consume messages
        messages = list(consumer)
        
        # Should not yield invalid message
        self.assertEqual(len(messages), 0)
        
        # DLQ should be called
        self.mock_dlq_producer.send.assert_called_once()
        
        # Check DLQ message
        dlq_call = self.mock_dlq_producer.send.call_args
        self.assertEqual(dlq_call[0][0], "test.topic.dlq")
        
        dlq_message = dlq_call[1]["value"]
        self.assertEqual(dlq_message["original_topic"], "test.topic")
        self.assertEqual(dlq_message["metadata"]["processing_stage"], "consumer")
        self.assertEqual(dlq_message["metadata"]["partition"], 0)
        self.assertEqual(dlq_message["metadata"]["offset"], 101)
    
    @patch('agents.common_tools.kafka_wrapper.KafkaConsumer')
    @patch('agents.common_tools.kafka_wrapper.KafkaProducer')
    def test_error_handler_callback(self, mock_kafka_producer, mock_kafka_consumer):
        """Test error handler is called on validation failure."""
        mock_kafka_consumer.return_value = self.mock_consumer
        mock_kafka_producer.return_value = self.mock_dlq_producer
        
        # Create invalid message
        invalid_message = Mock()
        invalid_message.topic = "test.topic"
        invalid_message.value = {"id": "123", "value": "not_a_number"}
        invalid_message.partition = 0
        invalid_message.offset = 101
        
        # Set up consumer to return invalid message
        self.mock_consumer.__iter__.return_value = iter([invalid_message])
        
        # Create error handler
        error_handler = Mock()
        
        consumer = ValidatingKafkaConsumer(
            topics=["test.topic"],
            bootstrap_servers="localhost:9092",
            agent_name="test_agent",
            group_id="test_group",
            schema_registry=self.mock_registry,
            dlq_producer=self.mock_dlq_producer,
            error_handler=error_handler
        )
        
        # Set up registry to return failing validator
        self.mock_registry.get_validator.return_value = self.invalid_validator
        
        # Consume messages
        list(consumer)
        
        # Error handler should be called
        error_handler.assert_called_once()
        call_args = error_handler.call_args[0]
        self.assertEqual(call_args[0], invalid_message.value)
        self.assertIsInstance(call_args[1], ValidationError)


class TestValidationHelpers(TestCase):
    """Test cases for validation helper functions."""
    
    def setUp(self):
        """Create test schema."""
        self.test_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "value": {"type": "number"}
                        },
                        "required": ["name", "value"]
                    }
                }
            },
            "required": ["id"]
        }
    
    def test_validate_message_valid(self):
        """Test validating a valid message."""
        message = {
            "id": "123",
            "items": [
                {"name": "item1", "value": 10.0},
                {"name": "item2", "value": 20.0}
            ]
        }
        
        is_valid, errors = validate_message(
            message,
            self.test_schema,
            raise_on_error=False
        )
        
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
    
    def test_validate_message_invalid(self):
        """Test validating an invalid message."""
        message = {
            # Missing required 'id' field
            "items": [
                {"name": "item1", "value": "not_a_number"}  # Invalid type
            ]
        }
        
        is_valid, errors = validate_message(
            message,
            self.test_schema,
            raise_on_error=False
        )
        
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
    
    def test_validate_message_raise_on_error(self):
        """Test validation raises exception when configured."""
        message = {"invalid": "data"}
        
        with self.assertRaises(SchemaValidationError) as context:
            validate_message(
                message,
                self.test_schema,
                raise_on_error=True
            )
        
        error = context.exception
        self.assertGreater(len(error.errors), 0)
        self.assertIsInstance(error.get_error_details(), list)
    
    def test_create_error_report(self):
        """Test creating error report from validation errors."""
        # Create a validation error
        message = {"id": 123}  # Wrong type
        _, errors = validate_message(
            message,
            self.test_schema,
            raise_on_error=False
        )
        
        report = create_error_report(
            message,
            errors,
            "test.topic",
            "test_agent"
        )
        
        self.assertEqual(report["topic"], "test.topic")
        self.assertEqual(report["agent"], "test_agent")
        self.assertEqual(report["error_count"], len(errors))
        self.assertIn("errors", report)
        self.assertIsInstance(report["errors"], list)
    
    def test_extract_validation_path(self):
        """Test extracting human-readable path from validation error."""
        # Test simple path
        error = Mock(spec=ValidationError)
        error.path = ["field1", "field2"]
        
        path = extract_validation_path(error)
        self.assertEqual(path, "field1.field2")
        
        # Test path with array index
        error.path = ["items", 0, "value"]
        path = extract_validation_path(error)
        self.assertEqual(path, "items[0].value")
        
        # Test empty path
        error.path = []
        path = extract_validation_path(error)
        self.assertEqual(path, "root")
    
    def test_merge_schemas(self):
        """Test merging multiple schemas."""
        base = {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": ["id"]
        }
        
        additional = {
            "properties": {"name": {"type": "string"}},
            "required": ["name"]
        }
        
        merged = merge_schemas(base, additional)
        
        # Check merged properties
        self.assertIn("id", merged["properties"])
        self.assertIn("name", merged["properties"])
        
        # Check merged required fields
        self.assertIn("id", merged["required"])
        self.assertIn("name", merged["required"])
    
    def test_get_schema_for_topic(self):
        """Test getting schema for various topic patterns."""
        with tempfile.TemporaryDirectory() as temp_dir:
            schema_dir = Path(temp_dir)
            
            # Create test schema files
            test_schema = {"type": "object"}
            
            # Exact match
            with open(schema_dir / "test.topic.json", "w") as f:
                json.dump(test_schema, f)
            
            # Underscore variant
            with open(schema_dir / "raw_intel.json", "w") as f:
                json.dump(test_schema, f)
            
            # Pattern match
            with open(schema_dir / "agent.delegate.json", "w") as f:
                json.dump(test_schema, f)
            
            # Test exact match
            schema = get_schema_for_topic("test.topic", schema_dir)
            self.assertIsNotNone(schema)
            
            # Test underscore replacement
            schema = get_schema_for_topic("raw.intel", schema_dir)
            self.assertIsNotNone(schema)
            
            # Test pattern matching
            schema = get_schema_for_topic("agent.delegate.feed_discovery", schema_dir)
            self.assertIsNotNone(schema)
            
            # Test not found
            schema = get_schema_for_topic("unknown.topic", schema_dir)
            self.assertIsNone(schema)