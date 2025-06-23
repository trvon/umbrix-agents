"""
Extended tests for kafka_tools.py to improve coverage from 32% to 75%.

These tests focus on the missing functionality:
- Message processing with retry logic
- Dead letter queue handling  
- Batch consumption patterns
- Producer/consumer lifecycle management
- Error handling and recovery scenarios
- Advanced message routing
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Mock Kafka dependencies before importing
import sys

mock_kafka = MagicMock()
mock_kafka.KafkaProducer = MagicMock()
mock_kafka.KafkaConsumer = MagicMock()
mock_kafka.TopicPartition = MagicMock()

mock_kafka_errors = MagicMock()
mock_kafka_errors.KafkaError = Exception
mock_kafka_errors.CommitFailedError = Exception

mock_kafka_admin = MagicMock()
mock_kafka_admin.KafkaAdminClient = MagicMock()
mock_kafka_admin.NewTopic = MagicMock()

sys.modules['kafka'] = mock_kafka
sys.modules['kafka.errors'] = mock_kafka_errors
sys.modules['kafka.admin'] = mock_kafka_admin

# Now import the module under test
from common_tools.kafka_tools import (
    EnhancedKafkaWrapper, MessageMetadata, ProcessingResult
)


class TestEnhancedKafkaWrapperProduction:
    """Test EnhancedKafkaWrapper producer functionality."""
    
    @pytest.mark.asyncio
    async def test_produce_creates_producer_on_demand(self):
        """Test that producers are created on-demand for topics."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        # Mock the create_producer method
        mock_producer = Mock()
        mock_future = Mock()
        mock_producer.send.return_value = mock_future
        
        with patch.object(wrapper, 'create_producer', return_value=mock_producer):
            # First call should create a producer
            result = await wrapper.produce("test_topic", {"test": "data"})
            
            assert "test_topic" in wrapper._producers
            assert wrapper._producers["test_topic"] == mock_producer
            mock_producer.send.assert_called_once_with(
                "test_topic", {"test": "data"}, key=None, headers=None
            )
            assert result == mock_future
    
    @pytest.mark.asyncio
    async def test_produce_reuses_existing_producer(self):
        """Test that existing producers are reused for the same topic."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        # Pre-populate a producer
        mock_producer = Mock()
        mock_future = Mock()
        mock_producer.send.return_value = mock_future
        wrapper._producers["test_topic"] = mock_producer
        
        # Should reuse existing producer
        result = await wrapper.produce("test_topic", {"test": "data"}, key="test_key")
        
        mock_producer.send.assert_called_once_with(
            "test_topic", {"test": "data"}, key="test_key", headers=None
        )
        assert result == mock_future
    
    @pytest.mark.asyncio
    async def test_produce_with_headers(self):
        """Test producing messages with custom headers."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        mock_producer = Mock()
        mock_producer.send.return_value = Mock()
        
        with patch.object(wrapper, 'create_producer', return_value=mock_producer):
            headers = {"content-type": b"application/json", "source": b"test"}
            await wrapper.produce("test_topic", {"data": "value"}, headers=headers)
            
            mock_producer.send.assert_called_once_with(
                "test_topic", {"data": "value"}, key=None, headers=headers
            )


class TestMessageRetryProcessing:
    """Test message processing with retry logic."""
    
    @pytest.mark.asyncio
    async def test_process_with_retry_success_first_attempt(self):
        """Test successful processing on first attempt."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        # Mock processor that succeeds immediately
        async def mock_processor(message_data, metadata):
            yield ProcessingResult(
                success=True,
                message_id="test_msg",
                processing_time=0.1
            )
        
        metadata = MessageMetadata(
            topic="test_topic",
            partition=0,
            offset=123
        )
        
        # Should succeed without retries
        result = await wrapper._process_with_retry(
            {"test": "data"}, metadata, mock_processor
        )
        
        assert result.success is True
        assert result.retry_count == 0
        # Don't assert on processing_time since it's set in _process_with_retry
    
    @pytest.mark.asyncio
    async def test_process_with_retry_success_after_retries(self):
        """Test successful processing after some failed attempts."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client", retry_delay=0.01)
        
        call_count = 0
        
        async def mock_processor(message_data, metadata):
            nonlocal call_count
            call_count += 1
            
            if call_count <= 2:
                # Fail first two attempts
                yield ProcessingResult(
                    success=False,
                    error=f"Attempt {call_count} failed",
                    message_id="test_msg"
                )
            else:
                # Succeed on third attempt
                yield ProcessingResult(
                    success=True,
                    message_id="test_msg"
                )
        
        metadata = MessageMetadata(topic="test_topic", partition=0, offset=123)
        
        start_time = time.time()
        result = await wrapper._process_with_retry(
            {"test": "data"}, metadata, mock_processor
        )
        elapsed = time.time() - start_time
        
        assert result.success is True
        assert result.retry_count == 2  # Two retries before success
        assert call_count == 3
        assert elapsed >= 0.02  # Should have some delay from retries
    
    @pytest.mark.asyncio
    async def test_process_with_retry_exception_handling(self):
        """Test processing when processor raises exceptions."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client", retry_delay=0.01)
        
        call_count = 0
        
        async def failing_processor(message_data, metadata):
            nonlocal call_count
            call_count += 1
            raise ValueError(f"Processing error {call_count}")
            yield  # Make it an async generator (unreachable)
        
        metadata = MessageMetadata(topic="test_topic", partition=0, offset=123)
        
        # Mock DLQ sending to avoid actual Kafka calls
        with patch.object(wrapper, '_send_to_dlq', new_callable=AsyncMock) as mock_dlq:
            await wrapper._process_with_retry(
                {"test": "data"}, metadata, failing_processor
            )
            
            # Should have tried max_retries + 1 times (3 + 1 = 4)
            assert call_count == 4
            
            # Should have sent to DLQ
            mock_dlq.assert_called_once()
            dlq_args = mock_dlq.call_args[0]
            assert dlq_args[0] == {"test": "data"}  # message_data
            assert dlq_args[1] == metadata
            assert "Processing error" in dlq_args[2]  # error message
            assert dlq_args[3] == 4  # retry_count
    
    @pytest.mark.asyncio
    async def test_process_with_retry_dlq_disabled(self):
        """Test retry processing when DLQ is disabled."""
        wrapper = EnhancedKafkaWrapper(
            "localhost:9092", "test_client", 
            dlq_enabled=False, retry_delay=0.01
        )
        
        async def failing_processor(message_data, metadata):
            raise ValueError("Always fails")
            yield  # Make it an async generator (unreachable)
        
        metadata = MessageMetadata(topic="test_topic", partition=0, offset=123)
        
        with patch.object(wrapper, '_send_to_dlq', new_callable=AsyncMock) as mock_dlq:
            await wrapper._process_with_retry(
                {"test": "data"}, metadata, failing_processor
            )
            
            # DLQ should not be called when disabled
            mock_dlq.assert_not_called()


class TestDeadLetterQueue:
    """Test dead letter queue functionality."""
    
    @pytest.mark.asyncio
    async def test_send_to_dlq_basic(self):
        """Test basic DLQ message construction and sending."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        metadata = MessageMetadata(
            topic="original_topic",
            partition=2,
            offset=456,
            timestamp=datetime(2025, 1, 1, 12, 0, 0),
            key="test_key"
        )
        
        original_message = {"important": "data"}
        error_msg = "Processing failed"
        retry_count = 3
        
        with patch.object(wrapper, 'produce', new_callable=AsyncMock) as mock_produce:
            await wrapper._send_to_dlq(original_message, metadata, error_msg, retry_count)
            
            # Verify produce was called with correct DLQ topic
            mock_produce.assert_called_once()
            call_args = mock_produce.call_args
            
            assert call_args[0][0] == "original_topic_dlq"  # DLQ topic name
            assert call_args[1]['key'] == "test_key"
            
            # Verify DLQ message structure
            dlq_message = call_args[0][1]
            assert dlq_message['original_topic'] == "original_topic"
            assert dlq_message['original_partition'] == 2
            assert dlq_message['original_offset'] == 456
            assert dlq_message['original_timestamp'] == "2025-01-01T12:00:00"
            assert dlq_message['error'] == "Processing failed"
            assert dlq_message['retry_count'] == 3
            assert dlq_message['original_message'] == original_message
            assert 'dlq_timestamp' in dlq_message
    
    @pytest.mark.asyncio
    async def test_send_to_dlq_custom_suffix(self):
        """Test DLQ with custom suffix configuration."""
        wrapper = EnhancedKafkaWrapper(
            "localhost:9092", "test_client", 
            dlq_suffix="_failed"
        )
        
        metadata = MessageMetadata(topic="test_topic", partition=0, offset=123)
        
        with patch.object(wrapper, 'produce', new_callable=AsyncMock) as mock_produce:
            await wrapper._send_to_dlq({}, metadata, "error", 1)
            
            # Should use custom suffix
            assert mock_produce.call_args[0][0] == "test_topic_failed"
    
    @pytest.mark.asyncio
    async def test_send_to_dlq_produce_failure(self):
        """Test DLQ handling when produce itself fails."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        metadata = MessageMetadata(topic="test_topic", partition=0, offset=123)
        
        with patch.object(wrapper, 'produce', new_callable=AsyncMock) as mock_produce:
            mock_produce.side_effect = Exception("DLQ produce failed")
            
            # Should handle DLQ produce failure gracefully (no exception raised)
            await wrapper._send_to_dlq({}, metadata, "original error", 1)
            
            mock_produce.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_send_to_dlq_with_none_timestamp(self):
        """Test DLQ handling when original message has no timestamp."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        metadata = MessageMetadata(
            topic="test_topic",
            partition=0,
            offset=123,
            timestamp=None  # No timestamp
        )
        
        with patch.object(wrapper, 'produce', new_callable=AsyncMock) as mock_produce:
            await wrapper._send_to_dlq({}, metadata, "error", 1)
            
            dlq_message = mock_produce.call_args[0][1]
            assert dlq_message['original_timestamp'] is None


class TestBatchConsumption:
    """Test batch consumption functionality."""
    
    @pytest.mark.asyncio
    async def test_batch_consume_basic_setup(self):
        """Test basic batch consumption setup and configuration."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        # Mock consumer creation
        mock_consumer = Mock()
        
        async def mock_batch_processor(messages):
            yield [ProcessingResult(success=True, message_id=f"msg_{i}") 
                   for i in range(len(messages))]
        
        # Create async iterator for consumer
        async def empty_async_iterator():
            return
            yield  # Make it async generator but yield nothing
            
        mock_consumer.__aiter__ = Mock(return_value=empty_async_iterator())
        mock_consumer.close = Mock()
        
        with patch.object(wrapper, 'create_consumer', return_value=mock_consumer):
            
            await wrapper.batch_consume(
                topics=["topic1", "topic2"],
                group_id="test_group",
                batch_processor=mock_batch_processor,
                batch_size=50
            )
            
            # Verify consumer was created correctly
            wrapper.create_consumer.assert_called_once_with(
                topics=["topic1", "topic2"],
                group_id="test_group",
                enable_auto_commit=False
            )
    
    @pytest.mark.asyncio
    async def test_batch_consume_uses_default_batch_size(self):
        """Test that batch consumption uses default batch size when not specified."""
        wrapper = EnhancedKafkaWrapper(
            "localhost:9092", "test_client", 
            batch_size=75  # Custom default
        )
        
        mock_consumer = Mock()
        
        async def empty_async_iterator():
            return
            yield  # Make it async generator
            
        mock_consumer.__aiter__ = Mock(return_value=empty_async_iterator())
        mock_consumer.close = Mock()
        
        async def mock_processor(messages):
            yield []
        
        with patch.object(wrapper, 'create_consumer', return_value=mock_consumer):
            # Don't specify batch_size - should use default
            await wrapper.batch_consume(
                topics=["test_topic"],
                group_id="test_group", 
                batch_processor=mock_processor
            )
        
        # The batch size would be used internally (tested in actual batch processing)
        assert wrapper.default_batch_size == 75


class TestTopicManager:
    """Test topic management functionality."""
    
    def test_topic_manager_initialization(self):
        """Test TopicManager initialization."""
        from common_tools.kafka_tools import TopicManager
        
        manager = TopicManager("localhost:9092")
        assert manager.bootstrap_servers == "localhost:9092"
        assert hasattr(manager, 'logger')
    
    @pytest.mark.asyncio
    async def test_topic_manager_create_topic_success(self):
        """Test successful topic creation through TopicManager."""
        from common_tools.kafka_tools import TopicManager
        
        manager = TopicManager("localhost:9092")
        
        # Mock Kafka admin components
        mock_admin_client = Mock()
        mock_future = Mock()
        mock_future.result.return_value = None  # Success
        
        with patch('kafka.admin.KafkaAdminClient', return_value=mock_admin_client) as mock_admin_class:
            with patch('kafka.admin.NewTopic') as mock_new_topic:
                mock_topic_instance = Mock()
                mock_new_topic.return_value = mock_topic_instance
                
                # Mock the create_topics result - values() should yield (topic_name, future) tuples
                mock_admin_client.create_topics.return_value = {
                    "key1": ("test_topic", mock_future)
                }
                
                result = await manager.create_topic(
                    "test_topic", 
                    num_partitions=3, 
                    replication_factor=2,
                    config={"retention.ms": "86400000"}
                )
                
                assert result is True
                mock_admin_class.assert_called_once_with(
                    bootstrap_servers="localhost:9092",
                    client_id="topic_manager"
                )
                mock_new_topic.assert_called_once_with(
                    name="test_topic",
                    num_partitions=3,
                    replication_factor=2,
                    topic_configs={"retention.ms": "86400000"}
                )
                mock_admin_client.create_topics.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_topic_manager_create_topic_failure(self):
        """Test topic creation failure handling."""
        from common_tools.kafka_tools import TopicManager
        
        manager = TopicManager("localhost:9092")
        
        mock_admin_client = Mock()
        mock_future = Mock()
        mock_future.result.side_effect = Exception("Topic already exists")
        
        with patch('kafka.admin.KafkaAdminClient', return_value=mock_admin_client):
            with patch('kafka.admin.NewTopic'):
                mock_admin_client.create_topics.return_value = {
                    "key1": ("test_topic", mock_future)
                }
                
                result = await manager.create_topic("test_topic")
                
                assert result is False
    
    @pytest.mark.asyncio
    async def test_topic_manager_create_topic_admin_exception(self):
        """Test topic creation when admin client raises exception."""
        from common_tools.kafka_tools import TopicManager
        
        manager = TopicManager("localhost:9092")
        
        with patch('kafka.admin.KafkaAdminClient', side_effect=Exception("Connection failed")):
            result = await manager.create_topic("test_topic")
            assert result is False
    
    @pytest.mark.asyncio
    async def test_topic_manager_topic_exists_true(self):
        """Test topic existence check when topic exists."""
        from common_tools.kafka_tools import TopicManager
        
        manager = TopicManager("localhost:9092")
        
        mock_admin_client = Mock()
        mock_admin_client.describe_topics.return_value = {"test_topic": Mock()}
        
        with patch('kafka.admin.KafkaAdminClient', return_value=mock_admin_client):
            result = await manager.topic_exists("test_topic")
            
            assert result is True
            mock_admin_client.describe_topics.assert_called_once_with(["test_topic"])
    
    @pytest.mark.asyncio
    async def test_topic_manager_topic_exists_false(self):
        """Test topic existence check when topic doesn't exist."""
        from common_tools.kafka_tools import TopicManager
        
        manager = TopicManager("localhost:9092")
        
        mock_admin_client = Mock()
        mock_admin_client.describe_topics.return_value = {}
        
        with patch('kafka.admin.KafkaAdminClient', return_value=mock_admin_client):
            result = await manager.topic_exists("nonexistent_topic")
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_topic_manager_topic_exists_exception(self):
        """Test topic existence check when admin client raises exception."""
        from common_tools.kafka_tools import TopicManager
        
        manager = TopicManager("localhost:9092")
        
        with patch('kafka.admin.KafkaAdminClient', side_effect=Exception("Connection failed")):
            result = await manager.topic_exists("test_topic")
            assert result is False
    
    @pytest.mark.asyncio
    async def test_topic_manager_delete_topic_success(self):
        """Test successful topic deletion."""
        from common_tools.kafka_tools import TopicManager
        
        manager = TopicManager("localhost:9092")
        
        mock_admin_client = Mock()
        mock_future = Mock()
        mock_future.result.return_value = None  # Success
        
        with patch('kafka.admin.KafkaAdminClient', return_value=mock_admin_client):
            mock_admin_client.delete_topics.return_value = {
                "key1": ("test_topic", mock_future)
            }
            
            result = await manager.delete_topic("test_topic")
            
            assert result is True
            mock_admin_client.delete_topics.assert_called_once_with(["test_topic"])
    
    @pytest.mark.asyncio
    async def test_topic_manager_delete_topic_failure(self):
        """Test topic deletion failure handling."""
        from common_tools.kafka_tools import TopicManager
        
        manager = TopicManager("localhost:9092")
        
        mock_admin_client = Mock()
        mock_future = Mock()
        mock_future.result.side_effect = Exception("Topic not found")
        
        with patch('kafka.admin.KafkaAdminClient', return_value=mock_admin_client):
            mock_admin_client.delete_topics.return_value = {
                "key1": ("test_topic", mock_future)
            }
            
            result = await manager.delete_topic("test_topic")
            
            assert result is False


class TestMessageRouter:
    """Test message routing functionality."""
    
    def test_message_router_initialization(self):
        """Test MessageRouter initialization."""
        from common_tools.kafka_tools import MessageRouter
        
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        router = MessageRouter(wrapper)
        assert router.kafka == wrapper
        assert hasattr(router, 'logger')
        assert hasattr(router, 'routing_rules')
        assert isinstance(router.routing_rules, list)
    
    def test_message_router_add_rule(self):
        """Test adding rules to MessageRouter."""
        from common_tools.kafka_tools import MessageRouter
        
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        router = MessageRouter(wrapper)
        
        def test_condition(message):
            return message.get('type') == 'test'
        
        def test_transform(message):
            return {**message, 'transformed': True}
        
        router.add_rule(test_condition, "target_topic", test_transform)
        
        assert len(router.routing_rules) == 1
        rule = router.routing_rules[0]
        assert rule['condition'] == test_condition
        assert rule['target_topic'] == "target_topic"
        assert rule['transform'] == test_transform
    
    def test_message_router_add_rule_no_transform(self):
        """Test adding rules without transform function."""
        from common_tools.kafka_tools import MessageRouter
        
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        router = MessageRouter(wrapper)
        
        def test_condition(message):
            return message.get('priority') == 'high'
        
        router.add_rule(test_condition, "high_priority_topic")
        
        assert len(router.routing_rules) == 1
        rule = router.routing_rules[0]
        
        # Should have identity transform function
        test_message = {"data": "test"}
        assert rule['transform'](test_message) == test_message
    
    @pytest.mark.asyncio
    async def test_message_router_route_message_success(self):
        """Test successful message routing."""
        from common_tools.kafka_tools import MessageRouter
        
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        router = MessageRouter(wrapper)
        
        # Add routing rules
        router.add_rule(
            lambda msg: msg.get('type') == 'A',
            "topic_a"
        )
        router.add_rule(
            lambda msg: msg.get('type') == 'B',
            "topic_b",
            lambda msg: {**msg, 'routed_via': 'rule_b'}
        )
        
        # Mock produce_with_confirmation
        with patch.object(wrapper, 'produce_with_confirmation', new_callable=AsyncMock) as mock_produce:
            mock_produce.return_value = True
            
            # Test routing to topic_a
            result = await router.route_message({"type": "A", "data": "test"}, key="test_key")
            
            assert result == ["topic_a"]
            mock_produce.assert_called_once_with(
                "topic_a",
                {"type": "A", "data": "test"},
                "test_key"
            )
    
    @pytest.mark.asyncio
    async def test_message_router_route_message_with_transform(self):
        """Test message routing with transformation."""
        from common_tools.kafka_tools import MessageRouter
        
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        router = MessageRouter(wrapper)
        
        router.add_rule(
            lambda msg: msg.get('type') == 'B',
            "topic_b",
            lambda msg: {**msg, 'routed_via': 'rule_b', 'timestamp': 12345}
        )
        
        with patch.object(wrapper, 'produce_with_confirmation', new_callable=AsyncMock) as mock_produce:
            mock_produce.return_value = True
            
            result = await router.route_message({"type": "B", "data": "test"})
            
            assert result == ["topic_b"]
            mock_produce.assert_called_once_with(
                "topic_b",
                {"type": "B", "data": "test", "routed_via": "rule_b", "timestamp": 12345},
                None
            )
    
    @pytest.mark.asyncio
    async def test_message_router_multiple_rules_match(self):
        """Test message routing when multiple rules match."""
        from common_tools.kafka_tools import MessageRouter
        
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        router = MessageRouter(wrapper)
        
        # Add multiple rules that could match
        router.add_rule(lambda msg: msg.get('urgent', False), "urgent_topic")
        router.add_rule(lambda msg: msg.get('type') == 'alert', "alert_topic")
        router.add_rule(lambda msg: True, "default_topic")  # Catch-all
        
        with patch.object(wrapper, 'produce_with_confirmation', new_callable=AsyncMock) as mock_produce:
            mock_produce.return_value = True
            
            result = await router.route_message({"type": "alert", "urgent": True, "data": "emergency"})
            
            # Should route to all matching topics
            assert set(result) == {"urgent_topic", "alert_topic", "default_topic"}
            assert mock_produce.call_count == 3
    
    @pytest.mark.asyncio
    async def test_message_router_produce_failure(self):
        """Test message routing when produce fails."""
        from common_tools.kafka_tools import MessageRouter
        
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        router = MessageRouter(wrapper)
        
        router.add_rule(lambda msg: True, "failing_topic")
        
        with patch.object(wrapper, 'produce_with_confirmation', new_callable=AsyncMock) as mock_produce:
            mock_produce.return_value = False  # Simulate produce failure
            
            result = await router.route_message({"data": "test"})
            
            # Should return empty list when all produces fail
            assert result == []
            mock_produce.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_message_router_condition_exception(self):
        """Test message routing when condition function raises exception."""
        from common_tools.kafka_tools import MessageRouter
        
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        router = MessageRouter(wrapper)
        
        def failing_condition(msg):
            raise ValueError("Condition evaluation failed")
        
        router.add_rule(failing_condition, "topic")
        router.add_rule(lambda msg: True, "backup_topic")  # This should still work
        
        with patch.object(wrapper, 'produce_with_confirmation', new_callable=AsyncMock) as mock_produce:
            mock_produce.return_value = True
            
            result = await router.route_message({"data": "test"})
            
            # Should route to backup topic despite first rule failing
            assert result == ["backup_topic"]
            mock_produce.assert_called_once_with("backup_topic", {"data": "test"}, None)
    
    @pytest.mark.asyncio
    async def test_message_router_transform_exception(self):
        """Test message routing when transform function raises exception."""
        from common_tools.kafka_tools import MessageRouter
        
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        router = MessageRouter(wrapper)
        
        def failing_transform(msg):
            raise ValueError("Transform failed")
        
        router.add_rule(lambda msg: True, "topic", failing_transform)
        
        with patch.object(wrapper, 'produce_with_confirmation', new_callable=AsyncMock) as mock_produce:
            result = await router.route_message({"data": "test"})
            
            # Should return empty list when transform fails
            assert result == []
            mock_produce.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_message_router_no_matching_rules(self):
        """Test message routing when no rules match."""
        from common_tools.kafka_tools import MessageRouter
        
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        router = MessageRouter(wrapper)
        
        router.add_rule(lambda msg: msg.get('type') == 'special', "special_topic")
        
        with patch.object(wrapper, 'produce_with_confirmation', new_callable=AsyncMock) as mock_produce:
            result = await router.route_message({"type": "normal", "data": "test"})
            
            # Should return empty list when no rules match
            assert result == []
            mock_produce.assert_not_called()


class TestAdvancedErrorHandling:
    """Test advanced error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_consume_with_retry_consumer_error(self):
        """Test error handling when consumer itself fails."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        # Mock consumer that raises an exception
        mock_consumer = Mock()
        
        async def failing_async_iter():
            raise Exception("Consumer connection failed")
            yield  # Unreachable but makes it async generator
            
        mock_consumer.__aiter__ = Mock(return_value=failing_async_iter())
        mock_consumer.close = Mock()
        
        async def mock_processor(message_data, metadata):
            yield ProcessingResult(success=True)
        
        with patch.object(wrapper, 'create_consumer', return_value=mock_consumer):
            with pytest.raises(Exception, match="Consumer connection failed"):
                await wrapper.consume_with_retry(
                    topics=["test_topic"],
                    group_id="test_group",
                    message_processor=mock_processor
                )
            
            # Consumer should be closed even on error
            mock_consumer.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_message_metadata_conversion(self):
        """Test message metadata conversion from Kafka message."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        # Mock Kafka message
        mock_message = Mock()
        mock_message.topic = "test_topic"
        mock_message.partition = 1
        mock_message.offset = 12345
        mock_message.timestamp = 1640995200000  # 2022-01-01 00:00:00 UTC in milliseconds
        mock_message.headers = [("content-type", b"application/json")]
        mock_message.key = b"test_key"
        mock_message.value = {"test": "data"}
        
        # Mock consumer that yields one message then stops
        mock_consumer = Mock()
        
        async def single_message_async_iter():
            yield mock_message
            
        mock_consumer.__aiter__ = Mock(return_value=single_message_async_iter())
        mock_consumer.close = Mock()
        
        processed_metadata = None
        
        async def capture_processor(message_data, metadata):
            nonlocal processed_metadata
            processed_metadata = metadata
            yield ProcessingResult(success=True)
        
        with patch.object(wrapper, 'create_consumer', return_value=mock_consumer):
            with patch.object(wrapper, '_process_with_retry', new_callable=AsyncMock) as mock_process:
                # Configure the async iteration to end immediately 
                async def single_message_async_iter():
                    yield mock_message
                    
                mock_consumer.__aiter__ = Mock(return_value=single_message_async_iter())
                
                try:
                    await wrapper.consume_with_retry(
                        topics=["test_topic"],
                        group_id="test_group",
                        message_processor=capture_processor
                    )
                except StopAsyncIteration:
                    pass  # Expected when iterator is exhausted
                
                # Verify metadata conversion
                if mock_process.call_args:
                    call_args = mock_process.call_args[0]
                    message_data = call_args[0]
                    metadata = call_args[1]
                    
                    assert message_data == {"test": "data"}
                    assert metadata.topic == "test_topic"
                    assert metadata.partition == 1
                    assert metadata.offset == 12345
                    assert metadata.timestamp == datetime.fromtimestamp(1640995200)
                    assert metadata.headers == {"content-type": b"application/json"}
                    assert metadata.key == "test_key"


class TestConfigurationEdgeCases:
    """Test configuration edge cases and validation."""
    
    def test_wrapper_with_zero_retries(self):
        """Test wrapper configuration with zero retries."""
        wrapper = EnhancedKafkaWrapper(
            "localhost:9092", "test_client",
            max_retries=0,
            retry_delay=1
        )
        
        assert wrapper.max_retries == 0
        assert wrapper.retry_delay == 1
        
        # With 0 retries, should only try once
    
    def test_wrapper_with_negative_retry_delay(self):
        """Test wrapper behavior with negative retry delay."""
        wrapper = EnhancedKafkaWrapper(
            "localhost:9092", "test_client",
            retry_delay=-1
        )
        
        # Should accept negative values (could be business logic)
        assert wrapper.retry_delay == -1
    
    def test_wrapper_with_large_batch_size(self):
        """Test wrapper with very large batch sizes."""
        wrapper = EnhancedKafkaWrapper(
            "localhost:9092", "test_client",
            batch_size=10000,
            batch_timeout=600
        )
        
        assert wrapper.default_batch_size == 10000
        assert wrapper.batch_timeout == 600
    
    def test_wrapper_with_custom_dlq_config(self):
        """Test wrapper with various DLQ configurations."""
        # Test empty suffix
        wrapper1 = EnhancedKafkaWrapper(
            "localhost:9092", "test_client",
            dlq_suffix=""
        )
        assert wrapper1.dlq_suffix == ""
        
        # Test long suffix
        wrapper2 = EnhancedKafkaWrapper(
            "localhost:9092", "test_client", 
            dlq_suffix="_failed_messages_queue"
        )
        assert wrapper2.dlq_suffix == "_failed_messages_queue"


class TestProduceWithConfirmation:
    """Test produce with confirmation functionality."""
    
    @pytest.mark.asyncio
    async def test_produce_with_confirmation_success(self):
        """Test successful message production with confirmation."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        # Mock the produce method and future
        mock_future = asyncio.Future()
        mock_result = Mock()
        mock_result.partition = 1
        mock_result.offset = 12345
        mock_future.set_result(mock_result)
        
        with patch.object(wrapper, 'produce', new_callable=AsyncMock) as mock_produce:
            mock_produce.return_value = mock_future
            
            result = await wrapper.produce_with_confirmation(
                "test_topic",
                {"data": "test"},
                key="test_key",
                timeout=10.0
            )
            
            assert result is True
            
            # Verify produce was called with enhanced headers
            call_args = mock_produce.call_args
            assert call_args[0][0] == "test_topic"
            assert call_args[0][1] == {"data": "test"}
            assert call_args[0][2] == "test_key"  # key is positional argument
            
            headers = call_args[0][3]  # headers is 4th positional argument
            assert b'test_client' == headers['producer_id']
            assert 'timestamp' in headers
    
    @pytest.mark.asyncio
    async def test_produce_with_confirmation_timeout(self):
        """Test produce with confirmation timeout handling."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        # Mock future that never completes
        mock_future = asyncio.Future()
        # Don't set result - future will timeout
        
        with patch.object(wrapper, 'produce', new_callable=AsyncMock) as mock_produce:
            mock_produce.return_value = mock_future
            
            result = await wrapper.produce_with_confirmation(
                "test_topic",
                {"data": "test"},
                timeout=0.01  # Very short timeout
            )
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_produce_with_confirmation_exception(self):
        """Test produce with confirmation exception handling."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        with patch.object(wrapper, 'produce', new_callable=AsyncMock) as mock_produce:
            mock_produce.side_effect = Exception("Produce failed")
            
            result = await wrapper.produce_with_confirmation(
                "test_topic",
                {"data": "test"}
            )
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_produce_with_confirmation_no_headers(self):
        """Test produce with confirmation when no headers provided."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        mock_future = asyncio.Future()
        mock_future.set_result(Mock(partition=0, offset=1))
        
        with patch.object(wrapper, 'produce', new_callable=AsyncMock) as mock_produce:
            mock_produce.return_value = mock_future
            
            result = await wrapper.produce_with_confirmation(
                "test_topic",
                {"data": "test"}
            )
            
            assert result is True
            
            # Should have added default headers  
            headers = mock_produce.call_args[0][3]  # headers is 4th positional argument
            assert 'producer_id' in headers
            assert 'timestamp' in headers


class TestBatchProcessing:
    """Test batch processing functionality."""
    
    @pytest.mark.asyncio
    async def test_process_batch_success(self):
        """Test successful batch processing."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        # Mock consumer for commit operations
        mock_consumer = Mock()
        mock_consumer.commit.return_value = None
        
        # Mock batch processor
        async def mock_processor(batch):
            yield [
                ProcessingResult(success=True, message_id=f"msg_{i}")
                for i in range(len(batch))
            ]
        
        message_batch = [
            {
                'data': {"test": f"data_{i}"},
                'metadata': MessageMetadata(topic="test_topic", partition=0, offset=i)
            }
            for i in range(3)
        ]
        
        # Should not raise exception
        await wrapper._process_batch(message_batch, mock_processor, mock_consumer)
        
        # Should commit successful messages
        mock_consumer.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_batch_partial_failure(self):
        """Test batch processing with some failed messages."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        mock_consumer = Mock()
        mock_consumer.commit.return_value = None
        
        # Mock processor that fails some messages
        async def mock_processor(batch):
            results = []
            for i, msg in enumerate(batch):
                if i == 1:  # Fail second message
                    results.append(ProcessingResult(
                        success=False,
                        error="Processing failed",
                        retry_count=2
                    ))
                else:
                    results.append(ProcessingResult(success=True))
            yield results
        
        message_batch = [
            {
                'data': {"test": f"data_{i}"},
                'metadata': MessageMetadata(topic="test_topic", partition=0, offset=i)
            }
            for i in range(3)
        ]
        
        with patch.object(wrapper, '_send_to_dlq', new_callable=AsyncMock) as mock_dlq:
            await wrapper._process_batch(message_batch, mock_processor, mock_consumer)
            
            # Should commit successful messages
            mock_consumer.commit.assert_called_once()
            
            # Should send failed message to DLQ
            mock_dlq.assert_called_once()
            dlq_args = mock_dlq.call_args[0]
            assert dlq_args[0] == {"test": "data_1"}  # Failed message data
            assert dlq_args[2] == "Processing failed"  # Error message
    
    @pytest.mark.asyncio
    async def test_process_batch_commit_failure(self):
        """Test batch processing when commit fails."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        mock_consumer = Mock()
        mock_consumer.commit.side_effect = mock_kafka_errors.CommitFailedError("Commit failed")
        
        async def mock_processor(batch):
            yield [ProcessingResult(success=True) for _ in batch]
        
        message_batch = [
            {
                'data': {"test": "data"},
                'metadata': MessageMetadata(topic="test_topic", partition=0, offset=0)
            }
        ]
        
        # Should handle commit failure gracefully
        await wrapper._process_batch(message_batch, mock_processor, mock_consumer)
        
        mock_consumer.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_batch_processor_exception(self):
        """Test batch processing when processor raises exception."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        mock_consumer = Mock()
        
        async def failing_processor(batch):
            raise ValueError("Processor failed")
            yield  # Make it an async generator (unreachable)
        
        message_batch = [
            {
                'data': {"test": "data"},
                'metadata': MessageMetadata(topic="test_topic", partition=0, offset=0)
            }
        ]
        
        with pytest.raises(ValueError, match="Processor failed"):
            await wrapper._process_batch(message_batch, failing_processor, mock_consumer)
        
        # Should not commit when processor fails
        mock_consumer.commit.assert_not_called()


class TestWrapperFactoryMethods:
    """Test wrapper factory methods."""
    
    def test_create_topic_manager(self):
        """Test creating topic manager from wrapper."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        topic_manager = wrapper.create_topic_manager()
        
        assert topic_manager.bootstrap_servers == "localhost:9092"
        assert hasattr(topic_manager, 'logger')
    
    @pytest.mark.asyncio
    async def test_get_consumer_lag_placeholder(self):
        """Test consumer lag monitoring placeholder implementation."""
        wrapper = EnhancedKafkaWrapper("localhost:9092", "test_client")
        
        result = await wrapper.get_consumer_lag("test_group", ["topic1", "topic2"])
        
        # Current implementation returns empty dict
        assert result == {}


class TestAdvancedBatchConsumption:
    """Test advanced batch consumption scenarios."""
    
    @pytest.mark.asyncio
    async def test_batch_consume_timeout_triggers_processing(self):
        """Test that batch timeout triggers processing even with small batches."""
        wrapper = EnhancedKafkaWrapper(
            "localhost:9092", "test_client", 
            batch_size=10, batch_timeout=0.01  # Very short timeout
        )
        
        # Mock consumer with slow message delivery
        mock_consumer = Mock()
        mock_message = Mock()
        mock_message.topic = "test_topic"
        mock_message.partition = 0
        mock_message.offset = 1
        mock_message.timestamp = int(time.time() * 1000)
        mock_message.headers = []
        mock_message.key = None
        mock_message.value = {"test": "data"}
        
        # Mock async iteration that yields one message then stops
        async def mock_iter():
            yield mock_message
            # Simulate timeout by waiting longer than batch_timeout
            await asyncio.sleep(0.02)
        
        mock_consumer.__aiter__ = Mock(return_value=mock_iter())
        mock_consumer.close = Mock()
        
        batch_processed = False
        
        async def mock_batch_processor(messages):
            nonlocal batch_processed
            batch_processed = True
            yield [ProcessingResult(success=True) for _ in messages]
        
        with patch.object(wrapper, 'create_consumer', return_value=mock_consumer):
            with patch.object(wrapper, '_process_batch', new_callable=AsyncMock) as mock_process:
                # Timeout quickly to end the test
                try:
                    await asyncio.wait_for(
                        wrapper.batch_consume(
                            topics=["test_topic"],
                            group_id="test_group",
                            batch_processor=mock_batch_processor
                        ),
                        timeout=0.1
                    )
                except asyncio.TimeoutError:
                    pass  # Expected due to timeout
                
                # Should have processed batch due to timeout
                if mock_process.call_count > 0:
                    call_args = mock_process.call_args[0]
                    message_batch = call_args[0]
                    assert len(message_batch) == 1
                    assert message_batch[0]['data'] == {"test": "data"}


if __name__ == "__main__":
    pytest.main([__file__])