"""
Enhanced Kafka utilities for Umbrix agents.

This module provides higher-level Kafka utilities and patterns commonly used
across agents, extending the base KafkaWrapper with additional functionality.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Callable, AsyncGenerator
from dataclasses import dataclass
from datetime import datetime, timedelta
import time

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, CommitFailedError

from .kafka_wrapper import ValidatingKafkaProducer, ValidatingKafkaConsumer

def setup_logging(name: str):
    """Simple logging setup."""
    import logging
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


@dataclass
class MessageMetadata:
    """Metadata for a Kafka message."""
    topic: str
    partition: int
    offset: int
    timestamp: Optional[datetime] = None
    headers: Optional[Dict[str, bytes]] = None
    key: Optional[str] = None


@dataclass
class ProcessingResult:
    """Result of processing a message."""
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None
    retry_count: int = 0
    processing_time: float = 0.0


class EnhancedKafkaWrapper:
    """
    Enhanced Kafka wrapper with additional utilities for agent development.
    
    Provides batch processing, dead letter queues, retry mechanisms,
    and advanced message routing capabilities.
    """
    
    def __init__(self, bootstrap_servers: str, client_id: str, **kwargs):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.logger = setup_logging(f"kafka_{client_id}")
        
        # Dead letter queue configuration
        self.dlq_enabled = kwargs.get('dlq_enabled', True)
        self.dlq_suffix = kwargs.get('dlq_suffix', '_dlq')
        
        # Retry configuration
        self.max_retries = kwargs.get('max_retries', 3)
        self.retry_delay = kwargs.get('retry_delay', 5)
        
        # Batch processing configuration
        self.default_batch_size = kwargs.get('batch_size', 100)
        self.batch_timeout = kwargs.get('batch_timeout', 30)
        
        # Initialize producers and consumers
        self._producers = {}
        self._consumers = {}
    
    def create_producer(self, **kwargs):
        """Create a Kafka producer."""
        return ValidatingKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            **kwargs
        )
    
    def create_consumer(self, topics: List[str], group_id: str, **kwargs):
        """Create a Kafka consumer."""
        return ValidatingKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            group_id=group_id,
            **kwargs
        )
    
    async def produce(self, topic: str, message: Dict[str, Any], key: Optional[str] = None, headers: Optional[Dict[str, bytes]] = None):
        """Produce a message to a topic."""
        if topic not in self._producers:
            self._producers[topic] = self.create_producer()
        
        producer = self._producers[topic]
        
        # Use the ValidatingKafkaProducer's send method
        future = producer.send(topic, message, key=key, headers=headers)
        return future
    
    async def consume_with_retry(
        self,
        topics: List[str],
        group_id: str,
        message_processor: Callable[[Dict[str, Any], MessageMetadata], AsyncGenerator[ProcessingResult, None]],
        **consumer_kwargs
    ):
        """
        Consume messages with automatic retry and dead letter queue handling.
        
        Args:
            topics: List of topics to consume from
            group_id: Consumer group ID
            message_processor: Async function to process each message
            **consumer_kwargs: Additional consumer configuration
        """
        consumer = self.create_consumer(
            topics=topics,
            group_id=group_id,
            **consumer_kwargs
        )
        
        try:
            async for message in consumer:
                metadata = MessageMetadata(
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                    timestamp=datetime.fromtimestamp(message.timestamp / 1000) if message.timestamp else None,
                    headers=dict(message.headers or []),
                    key=message.key.decode('utf-8') if message.key else None
                )
                
                await self._process_with_retry(message.value, metadata, message_processor)
                
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            raise
        finally:
            consumer.close()
    
    async def _process_with_retry(
        self,
        message_data: Dict[str, Any],
        metadata: MessageMetadata,
        processor: Callable[[Dict[str, Any], MessageMetadata], AsyncGenerator[ProcessingResult, None]]
    ):
        """Process a message with retry logic."""
        retry_count = 0
        last_error = None
        
        while retry_count <= self.max_retries:
            try:
                start_time = time.time()
                
                async for result in processor(message_data, metadata):
                    result.retry_count = retry_count
                    result.processing_time = time.time() - start_time
                    
                    if result.success:
                        self.logger.debug(
                            f"Successfully processed message from {metadata.topic} "
                            f"(retries: {retry_count})"
                        )
                        return result
                    else:
                        last_error = result.error
                        break
                
            except Exception as e:
                last_error = str(e)
                self.logger.warning(
                    f"Processing failed for message from {metadata.topic} "
                    f"(attempt {retry_count + 1}/{self.max_retries + 1}): {e}"
                )
            
            retry_count += 1
            if retry_count <= self.max_retries:
                await asyncio.sleep(self.retry_delay * retry_count)  # Exponential backoff
        
        # Send to DLQ if all retries failed
        if self.dlq_enabled:
            await self._send_to_dlq(message_data, metadata, last_error, retry_count)
        
        self.logger.error(
            f"Failed to process message from {metadata.topic} after {retry_count} attempts: {last_error}"
        )
    
    async def _send_to_dlq(
        self,
        message_data: Dict[str, Any],
        metadata: MessageMetadata,
        error: str,
        retry_count: int
    ):
        """Send failed message to dead letter queue."""
        dlq_topic = f"{metadata.topic}{self.dlq_suffix}"
        
        dlq_message = {
            'original_topic': metadata.topic,
            'original_partition': metadata.partition,
            'original_offset': metadata.offset,
            'original_timestamp': metadata.timestamp.isoformat() if metadata.timestamp else None,
            'error': error,
            'retry_count': retry_count,
            'dlq_timestamp': datetime.utcnow().isoformat(),
            'original_message': message_data
        }
        
        try:
            await self.produce(dlq_topic, dlq_message, key=metadata.key)
            self.logger.info(f"Sent message to DLQ: {dlq_topic}")
        except Exception as e:
            self.logger.error(f"Failed to send message to DLQ {dlq_topic}: {e}")
    
    async def batch_consume(
        self,
        topics: List[str],
        group_id: str,
        batch_processor: Callable[[List[Dict[str, Any]]], AsyncGenerator[List[ProcessingResult], None]],
        batch_size: Optional[int] = None,
        **consumer_kwargs
    ):
        """
        Consume messages in batches for improved throughput.
        
        Args:
            topics: List of topics to consume from
            group_id: Consumer group ID
            batch_processor: Function to process batches of messages
            batch_size: Number of messages per batch
            **consumer_kwargs: Additional consumer configuration
        """
        consumer = self.create_consumer(
            topics=topics,
            group_id=group_id,
            enable_auto_commit=False,  # Manual commit for batch processing
            **consumer_kwargs
        )
        
        batch_size = batch_size or self.default_batch_size
        
        try:
            message_batch = []
            batch_start_time = time.time()
            
            async for message in consumer:
                message_batch.append({
                    'data': message.value,
                    'metadata': MessageMetadata(
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        timestamp=datetime.fromtimestamp(message.timestamp / 1000) if message.timestamp else None,
                        headers=dict(message.headers or []),
                        key=message.key.decode('utf-8') if message.key else None
                    )
                })
                
                # Process batch when size reached or timeout exceeded
                if (len(message_batch) >= batch_size or 
                    time.time() - batch_start_time > self.batch_timeout):
                    
                    await self._process_batch(message_batch, batch_processor, consumer)
                    message_batch = []
                    batch_start_time = time.time()
            
            # Process remaining messages
            if message_batch:
                await self._process_batch(message_batch, batch_processor, consumer)
                
        except Exception as e:
            self.logger.error(f"Batch consumer error: {e}")
            raise
        finally:
            consumer.close()
    
    async def _process_batch(
        self,
        message_batch: List[Dict[str, Any]],
        processor: Callable[[List[Dict[str, Any]]], AsyncGenerator[List[ProcessingResult], None]],
        consumer
    ):
        """Process a batch of messages."""
        try:
            async for results in processor(message_batch):
                # Commit offsets for successfully processed messages
                successful_messages = [
                    msg for msg, result in zip(message_batch, results)
                    if result.success
                ]
                
                if successful_messages:
                    try:
                        consumer.commit()
                        self.logger.debug(f"Committed batch of {len(successful_messages)} messages")
                    except Exception as e:
                        # Handle any commit failures gracefully
                        self.logger.error(f"Failed to commit batch: {e}")
                
                # Handle failed messages
                failed_messages = [
                    (msg, result) for msg, result in zip(message_batch, results)
                    if not result.success
                ]
                
                for msg, result in failed_messages:
                    if self.dlq_enabled:
                        await self._send_to_dlq(
                            msg['data'],
                            msg['metadata'],
                            result.error or "Batch processing failed",
                            result.retry_count
                        )
                
        except Exception as e:
            self.logger.error(f"Batch processing error: {e}")
            # Don't commit on error - messages will be reprocessed
            raise
    
    async def produce_with_confirmation(
        self,
        topic: str,
        message: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[Dict[str, bytes]] = None,
        timeout: float = 30.0
    ) -> bool:
        """
        Produce a message with delivery confirmation.
        
        Args:
            topic: Target topic
            message: Message payload
            key: Optional message key
            headers: Optional message headers
            timeout: Timeout for delivery confirmation
            
        Returns:
            bool: True if message was successfully delivered
        """
        try:
            # Add correlation headers
            if headers is None:
                headers = {}
            
            headers.update({
                'producer_id': self.client_id.encode('utf-8'),
                'timestamp': str(int(time.time() * 1000)).encode('utf-8')
            })
            
            future = await self.produce(topic, message, key, headers)
            
            # Wait for confirmation with timeout
            result = await asyncio.wait_for(future, timeout=timeout)
            
            self.logger.debug(
                f"Message delivered to {topic} partition {result.partition} "
                f"offset {result.offset}"
            )
            return True
            
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout waiting for delivery confirmation to {topic}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to produce message to {topic}: {e}")
            return False
    
    def create_topic_manager(self) -> 'TopicManager':
        """Create a topic manager for administrative operations."""
        return TopicManager(self.bootstrap_servers)
    
    async def get_consumer_lag(self, group_id: str, topics: List[str]) -> Dict[str, Dict[int, int]]:
        """
        Get consumer lag information for a consumer group.
        
        Args:
            group_id: Consumer group ID
            topics: List of topics to check
            
        Returns:
            Dict mapping topic -> partition -> lag
        """
        # This would require additional Kafka admin client setup
        # Placeholder implementation
        self.logger.warning("Consumer lag monitoring not yet implemented")
        return {}


class TopicManager:
    """
    Utility class for Kafka topic administration.
    
    Provides methods for creating, deleting, and managing Kafka topics.
    """
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.logger = setup_logging("topic_manager")
    
    async def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        config: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Create a Kafka topic.
        
        Args:
            topic_name: Name of the topic to create
            num_partitions: Number of partitions
            replication_factor: Replication factor
            config: Additional topic configuration
            
        Returns:
            bool: True if topic was created successfully
        """
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="topic_manager"
            )
            
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=config or {}
            )
            
            result = admin_client.create_topics([topic], validate_only=False)
            
            # Wait for creation to complete
            for topic_name, future in result.values():
                try:
                    future.result()  # This will raise an exception if creation failed
                    self.logger.info(f"Topic {topic_name} created successfully")
                    return True
                except Exception as e:
                    self.logger.error(f"Failed to create topic {topic_name}: {e}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error creating topic {topic_name}: {e}")
            return False
    
    async def topic_exists(self, topic_name: str) -> bool:
        """
        Check if a topic exists.
        
        Args:
            topic_name: Name of the topic to check
            
        Returns:
            bool: True if topic exists
        """
        try:
            from kafka.admin import KafkaAdminClient
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="topic_manager"
            )
            
            metadata = admin_client.describe_topics([topic_name])
            return topic_name in metadata
            
        except Exception as e:
            self.logger.error(f"Error checking topic existence {topic_name}: {e}")
            return False
    
    async def delete_topic(self, topic_name: str) -> bool:
        """
        Delete a Kafka topic.
        
        Args:
            topic_name: Name of the topic to delete
            
        Returns:
            bool: True if topic was deleted successfully
        """
        try:
            from kafka.admin import KafkaAdminClient
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="topic_manager"
            )
            
            result = admin_client.delete_topics([topic_name])
            
            # Wait for deletion to complete
            for topic_name, future in result.values():
                try:
                    future.result()
                    self.logger.info(f"Topic {topic_name} deleted successfully")
                    return True
                except Exception as e:
                    self.logger.error(f"Failed to delete topic {topic_name}: {e}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error deleting topic {topic_name}: {e}")
            return False


class MessageRouter:
    """
    Utility for routing messages to different topics based on content or rules.
    
    Useful for implementing complex message routing logic.
    """
    
    def __init__(self, kafka_wrapper: EnhancedKafkaWrapper):
        self.kafka = kafka_wrapper
        self.logger = setup_logging("message_router")
        self.routing_rules = []
    
    def add_rule(
        self,
        condition: Callable[[Dict[str, Any]], bool],
        target_topic: str,
        transform: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    ):
        """
        Add a routing rule.
        
        Args:
            condition: Function that returns True if message should be routed
            target_topic: Topic to route the message to
            transform: Optional function to transform the message before routing
        """
        self.routing_rules.append({
            'condition': condition,
            'target_topic': target_topic,
            'transform': transform or (lambda x: x)
        })
    
    async def route_message(self, message: Dict[str, Any], key: Optional[str] = None) -> List[str]:
        """
        Route a message based on configured rules.
        
        Args:
            message: Message to route
            key: Optional message key
            
        Returns:
            List of topics the message was sent to
        """
        routed_topics = []
        
        for rule in self.routing_rules:
            try:
                if rule['condition'](message):
                    transformed_message = rule['transform'](message)
                    
                    success = await self.kafka.produce_with_confirmation(
                        rule['target_topic'],
                        transformed_message,
                        key
                    )
                    
                    if success:
                        routed_topics.append(rule['target_topic'])
                        self.logger.debug(f"Routed message to {rule['target_topic']}")
                    else:
                        self.logger.error(f"Failed to route message to {rule['target_topic']}")
                        
            except Exception as e:
                self.logger.error(f"Error applying routing rule for {rule['target_topic']}: {e}")
        
        return routed_topics