"""
Context propagation utilities for correlation IDs and distributed tracing.

This module provides utilities for extracting, injecting, and propagating
correlation IDs through Kafka messages, HTTP headers, and agent workflows.
"""

import json
import uuid
from typing import Dict, Any, Optional, Union
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

from .structured_logging import create_correlation_id, StructuredLogger


# Headers and field names for correlation ID propagation
CORRELATION_ID_HEADER = "X-Correlation-ID"
CORRELATION_ID_FIELD = "correlation_id"
TASK_ID_HEADER = "X-Task-ID"
TASK_ID_FIELD = "task_id"
TRACE_ID_HEADER = "X-Trace-ID"
AGENT_NAME_HEADER = "X-Agent-Name"


class CorrelationContext:
    """Container for correlation and tracing context."""
    
    def __init__(
        self,
        correlation_id: str = None,
        task_id: str = None,
        trace_id: str = None,
        agent_name: str = None,
        parent_span: str = None,
        extra_context: Dict[str, Any] = None
    ):
        # Preserve provided correlation ID. Only generate a new one if *None*
        # or empty string is supplied.  This prevents accidental mutation when
        # parsing messages that legitimately omit a correlation identifier –
        # the caller can decide how to handle the absence.
        self.correlation_id = correlation_id if correlation_id else create_correlation_id()
        self.task_id = task_id
        self.trace_id = trace_id or self.correlation_id
        self.agent_name = agent_name
        self.parent_span = parent_span
        self.extra_context = extra_context or {}
        self.created_at = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary."""
        return {
            "correlation_id": self.correlation_id,
            "task_id": self.task_id,
            "trace_id": self.trace_id,
            "agent_name": self.agent_name,
            "parent_span": self.parent_span,
            "created_at": self.created_at.isoformat(),
            "extra_context": self.extra_context
        }
    
    def to_headers(self) -> Dict[str, bytes]:
        """Convert context to Kafka headers format."""
        headers = {}
        
        if self.correlation_id:
            headers[CORRELATION_ID_HEADER] = self.correlation_id.encode('utf-8')
        
        if self.task_id:
            headers[TASK_ID_HEADER] = self.task_id.encode('utf-8')
        
        if self.trace_id:
            headers[TRACE_ID_HEADER] = self.trace_id.encode('utf-8')
        
        if self.agent_name:
            headers[AGENT_NAME_HEADER] = self.agent_name.encode('utf-8')
        
        # Add extra context as JSON
        if self.extra_context:
            headers["X-Extra-Context"] = json.dumps(self.extra_context).encode('utf-8')
        
        return headers
    
    @classmethod
    def from_headers(cls, headers: Dict[str, bytes]) -> 'CorrelationContext':
        """Create context from Kafka headers."""
        correlation_id = None
        task_id = None
        trace_id = None
        agent_name = None
        extra_context = {}
        
        if headers:
            # Extract correlation ID
            if CORRELATION_ID_HEADER in headers:
                correlation_id = headers[CORRELATION_ID_HEADER].decode('utf-8')
            
            # Extract task ID
            if TASK_ID_HEADER in headers:
                task_id = headers[TASK_ID_HEADER].decode('utf-8')
            
            # Extract trace ID
            if TRACE_ID_HEADER in headers:
                trace_id = headers[TRACE_ID_HEADER].decode('utf-8')
            
            # Extract agent name
            if AGENT_NAME_HEADER in headers:
                agent_name = headers[AGENT_NAME_HEADER].decode('utf-8')
            
            # Extract extra context
            if "X-Extra-Context" in headers:
                try:
                    extra_context = json.loads(headers["X-Extra-Context"].decode('utf-8'))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    extra_context = {}
        
        return cls(
            correlation_id=correlation_id,
            task_id=task_id,
            trace_id=trace_id,
            agent_name=agent_name,
            extra_context=extra_context
        )
    
    @classmethod
    def from_message_payload(cls, payload: Dict[str, Any]) -> 'CorrelationContext':
        """Create context from message payload fields."""
        return cls(
            correlation_id=payload.get(CORRELATION_ID_FIELD),
            task_id=payload.get(TASK_ID_FIELD),
            trace_id=payload.get("trace_id"),
            agent_name=payload.get("agent_name"),
            extra_context=payload.get("extra_context", {})
        )
    
    def inject_into_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Inject context fields into message payload."""
        if self.correlation_id:
            payload[CORRELATION_ID_FIELD] = self.correlation_id
        
        if self.task_id:
            payload[TASK_ID_FIELD] = self.task_id
        
        if self.trace_id:
            payload["trace_id"] = self.trace_id
        
        if self.agent_name:
            payload["agent_name"] = self.agent_name
        
        return payload


def extract_correlation_context_from_kafka_message(message) -> CorrelationContext:
    """
    Extract correlation context from a Kafka message.
    
    Args:
        message: Kafka message with headers and value
    
    Returns:
        CorrelationContext extracted from message
    """
    # Build headers dict (Kafka headers are list of tuples in Python client)
    headers_dict = dict(message.headers) if getattr(message, "headers", None) else {}

    # Parse context from headers (may be empty)
    header_context = CorrelationContext.from_headers(headers_dict) if headers_dict else None

    # Determine if we obtained a *real* correlation id from headers
    if header_context and header_context.correlation_id:
        return header_context

    # Fallback to payload fields
    if hasattr(message, "value") and isinstance(message.value, dict):
        payload_context = CorrelationContext.from_message_payload(message.value)
        if payload_context.correlation_id:
            return payload_context

    # Nothing found – return empty context to avoid None handling downstream
    return CorrelationContext(correlation_id=None)


def inject_correlation_context_into_kafka_message(
    context: CorrelationContext,
    payload: Dict[str, Any],
    headers: Dict[str, bytes] = None
) -> tuple[Dict[str, Any], Dict[str, bytes]]:
    """
    Inject correlation context into Kafka message payload and headers.
    
    Args:
        context: CorrelationContext to inject
        payload: Message payload dictionary
        headers: Existing headers dictionary
    
    Returns:
        Tuple of (enhanced_payload, enhanced_headers)
    """
    # Inject into payload
    enhanced_payload = context.inject_into_payload(payload.copy())
    
    # Inject into headers
    enhanced_headers = headers.copy() if headers else {}
    enhanced_headers.update(context.to_headers())
    
    return enhanced_payload, enhanced_headers


class CorrelationAwareKafkaProducer:
    """Kafka producer wrapper that automatically injects correlation context."""
    
    def __init__(self, producer: KafkaProducer, agent_name: str, logger: StructuredLogger = None):
        self.producer = producer
        self.agent_name = agent_name
        self.logger = logger
    
    def send_with_context(
        self,
        topic: str,
        value: Dict[str, Any],
        context: CorrelationContext,
        key: str = None,
        partition: int = None,
        **kwargs
    ):
        """
        Send message with correlation context injected.
        
        Args:
            topic: Kafka topic
            value: Message payload
            context: CorrelationContext to inject
            key: Message key
            partition: Target partition
            **kwargs: Additional producer.send() arguments
        
        Returns:
            Future from producer.send()
        """
        # Ensure agent name is set in context
        if not context.agent_name:
            context.agent_name = self.agent_name
        
        # Inject context into message
        enhanced_payload, headers = inject_correlation_context_into_kafka_message(
            context, value, kwargs.get('headers', {})
        )
        
        # Log message send
        if self.logger:
            from .logging import log_kafka_message_sent
            log_kafka_message_sent(
                self.logger,
                topic=topic,
                key=key,
                correlation_id=context.correlation_id,
                message_size=len(json.dumps(enhanced_payload).encode('utf-8'))
            )
        
        # Send message with enhanced payload and headers
        return self.producer.send(
            topic,
            value=enhanced_payload,
            key=key,
            partition=partition,
            headers=list(headers.items()),
            **{k: v for k, v in kwargs.items() if k != 'headers'}
        )
    
    def send_auto_context(
        self,
        topic: str,
        value: Dict[str, Any],
        correlation_id: str = None,
        task_id: str = None,
        **kwargs
    ):
        """
        Send message with automatically created correlation context.
        
        Args:
            topic: Kafka topic
            value: Message payload
            correlation_id: Optional correlation ID (generated if not provided)
            task_id: Optional task ID
            **kwargs: Additional producer.send() arguments
        
        Returns:
            Future from producer.send()
        """
        context = CorrelationContext(
            correlation_id=correlation_id,
            task_id=task_id,
            agent_name=self.agent_name
        )
        
        return self.send_with_context(topic, value, context, **kwargs)


class CorrelationAwareKafkaConsumer:
    """Kafka consumer wrapper that automatically extracts correlation context."""
    
    def __init__(self, consumer: KafkaConsumer, agent_name: str, logger: StructuredLogger = None):
        self.consumer = consumer
        self.agent_name = agent_name
        self.logger = logger
    
    def poll_with_context(self, timeout_ms: int = 1000, max_records: int = None):
        """
        Poll for messages and extract correlation context.
        
        Args:
            timeout_ms: Poll timeout in milliseconds
            max_records: Maximum number of records to return
        
        Yields:
            Tuple of (message, CorrelationContext)
        """
        message_batch = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        
        for topic_partition, messages in message_batch.items():
            for message in messages:
                # Extract correlation context
                context = extract_correlation_context_from_kafka_message(message)
                
                # Log message received
                if self.logger:
                    from .logging import log_kafka_message_received
                    log_kafka_message_received(
                        self.logger,
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        key=message.key.decode('utf-8') if message.key else None,
                        correlation_id=context.correlation_id,
                        message_size=len(str(message.value).encode('utf-8'))
                    )
                
                yield message, context


def create_child_correlation_context(
    parent_context: CorrelationContext,
    child_agent_name: str,
    child_task_id: str = None
) -> CorrelationContext:
    """
    Create a child correlation context that maintains tracing lineage.
    
    Args:
        parent_context: Parent correlation context
        child_agent_name: Name of the child agent
        child_task_id: Optional child task ID
    
    Returns:
        Child CorrelationContext
    """
    return CorrelationContext(
        correlation_id=parent_context.correlation_id,  # Preserve correlation ID
        task_id=child_task_id,
        trace_id=parent_context.trace_id,  # Preserve trace ID
        agent_name=child_agent_name,
        parent_span=parent_context.agent_name,  # Set parent for span tracking
        extra_context=parent_context.extra_context.copy()
    )


def propagate_context_to_feedrecord(
    context: CorrelationContext,
    feed_record
) -> None:
    """
    Propagate correlation context to a FeedRecord's metadata.
    
    Args:
        context: CorrelationContext to propagate
        feed_record: FeedRecord instance to update
    """
    if hasattr(feed_record, 'metadata'):
        if context.correlation_id:
            feed_record.metadata.correlation_id = context.correlation_id
        
        if context.task_id:
            if hasattr(feed_record.metadata, 'additional'):
                feed_record.metadata.additional['task_id'] = context.task_id
        
        if context.trace_id:
            if hasattr(feed_record.metadata, 'additional'):
                feed_record.metadata.additional['trace_id'] = context.trace_id


def extract_context_from_feedrecord(feed_record) -> Optional[CorrelationContext]:
    """
    Extract correlation context from a FeedRecord's metadata.
    
    Args:
        feed_record: FeedRecord instance to extract from
    
    Returns:
        CorrelationContext if found, None otherwise
    """
    if not hasattr(feed_record, 'metadata'):
        return None
    
    correlation_id = getattr(feed_record.metadata, 'correlation_id', None)
    
    if not correlation_id:
        return None
    
    task_id = None
    trace_id = None
    
    if hasattr(feed_record.metadata, 'additional'):
        task_id = feed_record.metadata.additional.get('task_id')
        trace_id = feed_record.metadata.additional.get('trace_id')
    
    return CorrelationContext(
        correlation_id=correlation_id,
        task_id=task_id,
        trace_id=trace_id
    )


# Decorator for automatic context propagation
def with_correlation_context(logger: StructuredLogger = None):
    """
    Decorator that automatically extracts and propagates correlation context.
    
    Args:
        logger: Optional StructuredLogger for logging
    
    Returns:
        Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Try to extract context from various sources
            context = None
            
            # Check if first argument has correlation context
            if args and hasattr(args[0], 'correlation_id'):
                context = CorrelationContext(correlation_id=args[0].correlation_id)
            
            # Check kwargs for context
            elif 'correlation_id' in kwargs:
                context = CorrelationContext(correlation_id=kwargs['correlation_id'])
            
            # Create new context if none found
            if not context:
                context = CorrelationContext()
            
            # Add context to kwargs
            kwargs['correlation_context'] = context
            
            # Log function entry if logger provided
            if logger:
                from .logging import log_function_entry
                log_function_entry(logger, func.__name__, correlation_id=context.correlation_id)
            
            try:
                result = func(*args, **kwargs)
                
                # Log function exit if logger provided
                if logger:
                    from .logging import log_function_exit
                    log_function_exit(logger, func.__name__, correlation_id=context.correlation_id)
                
                return result
            except Exception as e:
                # Log error if logger provided
                if logger:
                    from .logging import log_processing_error
                    log_processing_error(logger, e, func.__name__, correlation_id=context.correlation_id)
                raise
        
        return wrapper
    return decorator


# Export commonly used items
__all__ = [
    "CorrelationContext",
    "extract_correlation_context_from_kafka_message",
    "inject_correlation_context_into_kafka_message",
    "CorrelationAwareKafkaProducer",
    "CorrelationAwareKafkaConsumer",
    "create_child_correlation_context",
    "propagate_context_to_feedrecord",
    "extract_context_from_feedrecord",
    "with_correlation_context",
    "CORRELATION_ID_HEADER",
    "CORRELATION_ID_FIELD"
]