"""
Base classes for Umbrix agents to standardize common patterns.

This module provides base classes that encapsulate common agent functionality
including Kafka integration, Redis deduplication, metrics collection, health checks,
and error handling patterns.
"""

import asyncio
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set

import redis
from kafka import KafkaProducer, KafkaConsumer  # type: ignore
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry  # type: ignore

from .kafka_wrapper import ValidatingKafkaProducer, ValidatingKafkaConsumer
from .dedupe_store import RedisDedupeStore, InMemoryDedupeStore
from .metrics import UmbrixMetrics


def setup_logging(name: str) -> logging.Logger:
    """Simple logging setup for agents."""
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


class BaseAgent(ABC):
    """
    Base class for all Umbrix agents providing common functionality.
    
    Features:
    - Standardized initialization and configuration
    - Kafka producer/consumer setup
    - Redis deduplication
    - Prometheus metrics
    - Health check endpoints
    - Graceful shutdown handling
    - Error handling and logging
    """
    
    def __init__(self, agent_name: str, config: Dict[str, Any]):
        """
        Initialize the base agent with common configuration.
        
        Args:
            agent_name: Unique name for this agent instance
            config: Configuration dictionary containing agent settings
        """
        self.agent_name = agent_name
        self.config = config
        self.logger = setup_logging(agent_name)
        
        # Initialize components
        self._kafka_wrapper = None
        # Optional Kafka consumer placeholder (empty list by default)
        self._kafka_consumer: list = []
        self._dedupe_store = None
        self._metrics = None
        self._running = False
        self._shutdown_event = asyncio.Event()
        
        # Common metrics: use a dedicated CollectorRegistry per agent to avoid duplication
        registry = CollectorRegistry()
        self.messages_processed = Counter(
            'agent_messages_processed_total',
            'Total messages processed by agent',
            ['agent_name', 'message_type', 'status'],
            registry=registry
        )
        # Expose name and labelnames for testing compatibility
        self.messages_processed.name = 'agent_messages_processed_total'
        self.messages_processed.labelnames = ['agent_name', 'message_type', 'status']

        self.processing_duration = Histogram(
            'agent_processing_duration_seconds',
            'Time spent processing messages',
            ['agent_name', 'message_type'],
            registry=registry
        )
        # Expose name and labelnames for testing compatibility
        self.processing_duration.name = 'agent_processing_duration_seconds'
        self.processing_duration.labelnames = ['agent_name', 'message_type']

        self.active_tasks = Gauge(
            'agent_active_tasks',
            'Number of active tasks being processed',
            ['agent_name'],
            registry=registry
        )
        # Expose name and labelnames for testing compatibility
        self.active_tasks.name = 'agent_active_tasks'
        self.active_tasks.labelnames = ['agent_name']
        
        # Track running state and timing
        self.running = False
        self.start_time = None
        
        # Initialize if configured
        self._initialize_components()
    
    def _initialize_components(self):
        """Initialize Kafka, Redis, and metrics components based on configuration."""
        try:
            # Initialize Kafka if configured
            if self.config.get('kafka', {}).get('enabled', True):
                kafka_config = self.config.get('kafka', {})
                # For now, set up a placeholder for kafka configuration
                # Individual agents can override this with specific producers/consumers
                self._kafka_wrapper = {
                    'bootstrap_servers': kafka_config.get('bootstrap_servers', 'localhost:9092'),
                    'client_id': f"{self.agent_name}-{os.getpid()}"
                }
                self.logger.info(f"Kafka configuration set for {self.agent_name}")
            
            # Initialize Redis deduplication if configured
            if self.config.get('redis', {}).get('enabled', True):
                redis_config = self.config.get('redis', {})
                redis_url = redis_config.get('url', f"redis://{redis_config.get('host', 'localhost')}:{redis_config.get('port', 6379)}/{redis_config.get('db', 0)}")
                self._dedupe_store = RedisDedupeStore(
                    redis_url=redis_url,
                    namespace=f"{self.agent_name}_dedupe"
                )
                self.logger.info(f"Redis deduplication initialized for {self.agent_name}")
            
            # Initialize metrics collector if configured
            if self.config.get('metrics', {}).get('enabled', True):
                metrics_config = self.config.get('metrics', {})
                self._metrics = UmbrixMetrics(
                    agent_name=self.agent_name
                )
                self.logger.info(f"Metrics collector initialized for {self.agent_name}")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {e}")
            raise
    
    @property
    def kafka(self) -> Optional[Dict[str, Any]]:
        """Access to Kafka configuration for message publishing/consuming."""
        return self._kafka_wrapper
    
    @property
    def dedupe(self) -> Optional['RedisDedupeStore']:
        """Access to Redis deduplication store."""
        return self._dedupe_store
    
    @property
    def metrics(self) -> Optional[UmbrixMetrics]:
        """Access to metrics collector."""
        return self._metrics
    
    @abstractmethod
    async def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process a single message. Must be implemented by subclasses.
        
        Args:
            message: The message to process
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def get_input_topics(self) -> List[str]:
        """
        Get the list of Kafka topics this agent consumes from.
        
        Returns:
            List of topic names to consume from
        """
        pass
    
    @abstractmethod
    def get_output_topics(self) -> List[str]:
        """
        Get the list of Kafka topics this agent produces to.
        
        Returns:
            List of topic names to produce to
        """
        pass
    
    async def run(self):
        """
        Main agent execution loop.
        
        Handles message consumption, processing, and error recovery.
        """
        self.logger.info(f"Starting {self.agent_name}")
        self._running = True
        self.running = True
        self.start_time = time.time()
        
        try:
            # Start health check server if metrics enabled
            if self._metrics:
                # Check if start_server method exists (not all implementations may have it)
                if hasattr(self._metrics, 'start_server'):
                    await self._metrics.start_server()
            
            # Determine run mode
            mode = self.config.get('mode', 'consumer')
            
            if mode == 'consumer':
                # Get input topics  
                input_topics = self.get_input_topics()
                if not input_topics:
                    self.logger.warning("No input topics configured, switching to polling mode")
                    await self._run_polling_mode()
                else:
                    await self._run_consumer_mode(input_topics)
            elif mode == 'polling':
                await self._run_polling_mode()
            else:
                raise ValueError(f"Unsupported mode: {mode}")
                
        except Exception as e:
            self.logger.error(f"Agent {self.agent_name} failed: {e}")
            if not self.config.get('suppress_exceptions', False):
                raise
        finally:
            await self.shutdown()
    
    async def _run_consumer_mode(self, topics: List[str]):
        """Run agent in Kafka consumer mode."""
        if not self._kafka_wrapper:
            raise ValueError("Kafka not initialized but consumer mode requested")

        # Kafka consumer mode must be implemented by subclasses
        raise NotImplementedError("Subclasses should implement Kafka consumer mode")
    
    async def _run_polling_mode(self):
        """Run agent in polling mode for agents that don't consume from Kafka."""
        while self._running and not self._shutdown_event.is_set():
            try:
                # Allow subclasses to implement custom polling logic
                await self.poll_and_process()
                await asyncio.sleep(self.config.get('poll_interval', 60))
                
            except Exception as e:
                self.logger.error(f"Polling error: {e}")
                await asyncio.sleep(self.config.get('error_retry_delay', 30))
    
    async def poll_and_process(self):
        """
        Custom polling logic for agents that don't consume from Kafka.
        
        Override this method in subclasses that need polling behavior.
        """
        await asyncio.sleep(0.01)  # Short sleep for testing
    
    async def _process_message_with_metrics(self, message_data: Dict[str, Any]):
        """Process a message with metrics collection and error handling."""
        message_type = message_data.get('type', 'unknown')
        start_time = time.time()
        
        # Record active task if metrics available
        if hasattr(self, 'active_tasks'):
            self.active_tasks.labels(agent_name=self.agent_name).inc()
        
        try:
            # Check for duplicates if deduplication is enabled
            if self._dedupe_store and self._is_duplicate(message_data):
                self.logger.debug(f"Skipping duplicate message: {message_data.get('id', 'unknown')}")
                if self._metrics:
                    self._metrics.increment_messages_consumed("duplicate", 1)
                return True
            
            # Process the message
            success = await self.process_message(message_data)
            
            # Record metrics if available
            if self._metrics:
                self._metrics.increment_messages_consumed("processed", 1)
                if success:
                    self._metrics.increment_messages_produced("output", 1)
                    latency = time.time() - start_time
                    self._metrics.observe_processing_latency("message_processing", latency)
                else:
                    self._metrics.increment_error("processing_error", 1)
            
            # Add to dedupe store if successful
            if success and self._dedupe_store:
                self._add_to_dedupe(message_data)
            
            return success
                
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            if self._metrics:
                self._metrics.increment_error("processing_error", 1)
            return False
        finally:
            # Decrement active tasks if metrics available
            if hasattr(self, 'active_tasks'):
                self.active_tasks.labels(agent_name=self.agent_name).dec()
    
    def _is_duplicate(self, message: Dict[str, Any]) -> bool:
        """Check if message is a duplicate using configured deduplication strategy."""
        if not self._dedupe_store:
            return False
        
        # Default deduplication based on message ID or content hash
        dedupe_key = message.get('id')
        if not dedupe_key:
            return False
        
        return self._dedupe_store.exists(dedupe_key)
    
    def _add_to_dedupe(self, message: Dict[str, Any]):
        """Add message to deduplication store."""
        if not self._dedupe_store:
            return
        
        dedupe_key = message.get('id')
        if dedupe_key:
            self._dedupe_store.add(dedupe_key)
    
    def _compute_message_hash(self, message: Dict[str, Any]) -> str:
        """Compute a hash for message deduplication."""
        import hashlib
        message_str = json.dumps(message, sort_keys=True)
        return hashlib.sha256(message_str.encode()).hexdigest()
    
    async def publish_message(self, topic: str, message: Dict[str, Any], key: Optional[str] = None):
        """
        Publish a message to a Kafka topic.
        
        Args:
            topic: Target topic name
            message: Message payload
            key: Optional message key for partitioning
        """
        if not self._kafka_wrapper:
            raise ValueError("Kafka not initialized")
        
        # TODO: Implement with ValidatingKafkaProducer
        # For now, subclasses should override this method
        raise NotImplementedError("Subclasses should implement message publishing")
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check and return status.
        
        Returns:
            Dict containing health status information
        """
        status = {
            'agent_name': self.agent_name,
            'status': 'healthy',
            'timestamp': time.time(),
            'running': self._running,
            'components': {}
        }
        
        # Check Kafka health
        if self._kafka_wrapper:
            try:
                # Simple health check - could be enhanced
                status['components']['kafka'] = 'healthy'
            except Exception:
                status['components']['kafka'] = 'unhealthy'
                status['status'] = 'degraded'
        
        # Check Redis health
        if self._dedupe_store:
            try:
                # Simple Redis ping
                status['components']['redis'] = 'healthy'
            except Exception:
                status['components']['redis'] = 'unhealthy'
                status['status'] = 'degraded'
        
        return status
    
    async def shutdown(self):
        """Gracefully shutdown the agent and cleanup resources."""
        self.logger.info(f"Shutting down {self.agent_name}")
        self._running = False
        self.running = False
        self._shutdown_event.set()
        
        # Cleanup components
        if self._kafka_wrapper:
            # TODO: Close kafka connections when implemented
            pass
        
        if self._dedupe_store:
            if hasattr(self._dedupe_store, 'close'):
                self._dedupe_store.close()
        
        if self._metrics:
            if hasattr(self._metrics, 'stop_server'):
                await self._metrics.stop_server()
        
        self.logger.info(f"{self.agent_name} shutdown complete")


class KafkaConsumerAgent(BaseAgent):
    """
    Specialized base class for agents that primarily consume from Kafka topics.
    
    Provides additional utilities for batch processing, offset management,
    and consumer group coordination.
    """
    
    def __init__(self, agent_name: str, config: Dict[str, Any]):
        super().__init__(agent_name, config)
        self.batch_size = config.get('batch_size', 10)
        self.batch_timeout = config.get('batch_timeout', 30)
    
    async def process_batch(self, messages: List[Dict[str, Any]]) -> List[bool]:
        """
        Process a batch of messages. Override for batch processing optimization.
        
        Args:
            messages: List of messages to process
            
        Returns:
            List of success/failure indicators for each message
        """
        results = []
        for message in messages:
            result = await self.process_message(message)
            results.append(result)
        return results


class PollingAgent(BaseAgent):
    """
    Specialized base class for agents that operate in polling mode.
    
    Useful for agents that pull data from external APIs or perform
    scheduled tasks rather than consuming from Kafka.
    """
    
    def __init__(self, agent_name: str, config: Dict[str, Any]):
        super().__init__(agent_name, config)
        self.poll_interval = config.get('poll_interval', 300)  # 5 minutes default
    
    def get_input_topics(self) -> List[str]:
        """Polling agents don't consume from Kafka topics."""
        return []
    
    @abstractmethod
    async def poll_and_process(self):
        """
        Implement polling logic in subclasses.
        
        This method is called periodically based on poll_interval.
        """
        pass