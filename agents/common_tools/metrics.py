"""
Standardized Prometheus metrics framework for Umbrix agents.

This module provides a centralized metrics registry and helper functions
for consistent metric collection across all agents.
"""

import os
import time
import threading
from typing import Dict, Optional, Union, List, Any, Callable
from collections import defaultdict
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from prometheus_client import (  # type: ignore
    CollectorRegistry, 
    Counter, 
    Histogram, 
    Gauge, 
    Info,
    generate_latest,
    start_http_server as prometheus_start_http_server,
    CONTENT_TYPE_LATEST
)
from functools import wraps
import logging

# Global registry instance
_GLOBAL_REGISTRY = CollectorRegistry()

# Thread-safe access to the registry
_registry_lock = threading.Lock()

# Logger for metrics module
logger = logging.getLogger(__name__)


class UmbrixMetrics:
    """Centralized metrics collector for Umbrix agents."""
    
    def __init__(self, agent_name: str, registry: Optional[CollectorRegistry] = None):
        """
        Initialize metrics collector for an agent.
        
        Args:
            agent_name: Name of the agent (used as label value)
            registry: Prometheus registry (uses global if None)
        """
        self.agent_name = agent_name
        self.registry = registry or _GLOBAL_REGISTRY
        self._metrics: Dict[str, Any] = {}
        
        # Standard metrics for all agents
        self._init_standard_metrics()
    
    def _init_standard_metrics(self):
        """Initialize standard metrics that all agents should have."""
        # Message consumption/production counters
        self.messages_consumed = self._get_or_create_counter(
            name='messages_consumed_total',
            description='Total messages consumed by agent',
            labels=['agent', 'topic']
        )
        
        self.messages_produced = self._get_or_create_counter(
            name='messages_produced_total',
            description='Total messages produced by agent',
            labels=['agent', 'topic']
        )
        
        # Error counters
        self.errors_total = self._get_or_create_counter(
            name='errors_total',
            description='Total errors by agent and type',
            labels=['agent', 'error_type']
        )
        
        # Processing latency histogram
        self.processing_latency = self._get_or_create_histogram(
            name='processing_latency_seconds',
            description='Time spent processing by agent and stage',
            labels=['agent', 'stage'],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0]
        )
        
        # External call duration histogram
        self.external_call_duration = self._get_or_create_histogram(
            name='external_call_duration_seconds',
            description='Duration of external service calls',
            labels=['agent', 'service', 'endpoint'],
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
        )
        
        # Internal queue size gauge
        self.internal_queue_size = self._get_or_create_gauge(
            name='internal_queue_size',
            description='Current size of internal queues',
            labels=['agent', 'queue_name']
        )
        
        # Agent info
        self.agent_info = self._get_or_create_info(
            name='agent_info',
            description='Information about the agent'
        )
        
        # Set agent info
        self.agent_info.info({
            'agent': self.agent_name,
            'version': os.environ.get('AGENT_VERSION', 'unknown'),
            'python_version': os.environ.get('PYTHON_VERSION', 'unknown')
        })
    
    def _get_or_create_counter(self, name: str, description: str, 
                             labels: Optional[List[str]] = None) -> Counter:
        """Get or create a counter metric."""
        with _registry_lock:
            if name in self._metrics:
                return self._metrics[name]
            
            try:
                counter = Counter(name, description, labels or [], registry=self.registry)
                self._metrics[name] = counter
                return counter
            except ValueError:
                # Metric already exists in registry
                return self.registry._names_to_collectors[name]
    
    def _get_or_create_histogram(self, name: str, description: str,
                               labels: Optional[List[str]] = None, buckets: Optional[List[float]] = None) -> Histogram:
        """Get or create a histogram metric."""
        with _registry_lock:
            if name in self._metrics:
                return self._metrics[name]

            # Fallback to Prometheus default buckets if none provided.
            bucket_list = buckets if buckets is not None else Histogram.DEFAULT_BUCKETS

            try:
                histogram = Histogram(
                    name,
                    description,
                    labels or [],
                    registry=self.registry,
                    buckets=bucket_list,
                )
                self._metrics[name] = histogram
                return histogram
            except ValueError:
                # Metric already exists in registry
                return self.registry._names_to_collectors[name]
    
    def _get_or_create_gauge(self, name: str, description: str,
                           labels: Optional[List[str]] = None) -> Gauge:
        """Get or create a gauge metric."""
        with _registry_lock:
            if name in self._metrics:
                return self._metrics[name]
            
            try:
                gauge = Gauge(name, description, labels or [], registry=self.registry)
                self._metrics[name] = gauge
                return gauge
            except ValueError:
                # Metric already exists in registry
                return self.registry._names_to_collectors[name]
    
    def _get_or_create_info(self, name: str, description: str) -> Info:
        """Get or create an info metric."""
        with _registry_lock:
            if name in self._metrics:
                return self._metrics[name]
            
            try:
                info = Info(name, description, registry=self.registry)
                self._metrics[name] = info
                return info
            except ValueError:
                # Metric already exists in registry
                return self.registry._names_to_collectors[name]
    
    # Convenience methods for common operations
    def increment_messages_consumed(self, topic: str, count: int = 1):
        """Increment messages consumed counter."""
        self.messages_consumed.labels(agent=self.agent_name, topic=topic).inc(count)
    
    def increment_messages_produced(self, topic: str, count: int = 1):
        """Increment messages produced counter."""
        self.messages_produced.labels(agent=self.agent_name, topic=topic).inc(count)
    
    def increment_error(self, error_type: str, count: int = 1):
        """Increment error counter."""
        self.errors_total.labels(agent=self.agent_name, error_type=error_type).inc(count)
    
    def observe_processing_latency(self, stage: str, duration: float):
        """Observe processing latency."""
        self.processing_latency.labels(agent=self.agent_name, stage=stage).observe(duration)
    
    def observe_external_call(self, service: str, endpoint: str, duration: float):
        """Observe external call duration."""
        self.external_call_duration.labels(
            agent=self.agent_name, 
            service=service, 
            endpoint=endpoint
        ).observe(duration)
    
    def set_queue_size(self, queue_name: str, size: int):
        """Set internal queue size."""
        self.internal_queue_size.labels(agent=self.agent_name, queue_name=queue_name).set(size)
    
    async def start_server(self, host: str = '0.0.0.0', port: int = 8000):
        """
        Start the Prometheus HTTP server for metrics exposure.
        """
        # Start HTTP server asynchronously
        prometheus_start_http_server(port, addr=host, registry=self.registry)
    
    async def stop_server(self):
        """
        Stop the Prometheus HTTP server. No-op as built-in server cannot be stopped programmatically.
        """
        pass
    
    def create_custom_counter(self, name: str, description: str, 
                            labels: Optional[List[str]] = None) -> Counter:
        """Create a custom counter for agent-specific metrics."""
        full_name = f"{self.agent_name}_{name}"
        return self._get_or_create_counter(full_name, description, labels)
    
    def create_custom_histogram(self, name: str, description: str,
                              labels: Optional[List[str]] = None, buckets: Optional[List[float]] = None) -> Histogram:
        """Create a custom histogram for agent-specific metrics."""
        full_name = f"{self.agent_name}_{name}"
        return self._get_or_create_histogram(full_name, description, labels, buckets)
    
    def create_custom_gauge(self, name: str, description: str,
                          labels: Optional[List[str]] = None) -> Gauge:
        """Create a custom gauge for agent-specific metrics."""
        full_name = f"{self.agent_name}_{name}"
        return self._get_or_create_gauge(full_name, description, labels)


class MetricsTimer:
    """Context manager for timing operations."""
    
    def __init__(self, metrics: UmbrixMetrics, stage: str):
        """
        Initialize timer for a processing stage.
        
        Args:
            metrics: UmbrixMetrics instance
            stage: Name of the processing stage
        """
        self.metrics = metrics
        self.stage = stage
        self.start_time = None
    
    def __enter__(self):
        """Start timing."""
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timing and record metric."""
        if self.start_time is not None:
            duration = time.time() - self.start_time
            self.metrics.observe_processing_latency(self.stage, duration)


class ExternalCallTimer:
    """Context manager for timing external calls."""
    
    def __init__(self, metrics: UmbrixMetrics, service: str, endpoint: str = "default"):
        """
        Initialize timer for external calls.
        
        Args:
            metrics: UmbrixMetrics instance
            service: Name of external service
            endpoint: Specific endpoint or operation
        """
        self.metrics = metrics
        self.service = service
        self.endpoint = endpoint
        self.start_time = None
    
    def __enter__(self):
        """Start timing."""
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timing and record metric."""
        if self.start_time is not None:
            duration = time.time() - self.start_time
            self.metrics.observe_external_call(self.service, self.endpoint, duration)


# Decorator for automatic method timing
def timed_method(stage: Optional[str] = None):
    """
    Decorator to automatically time method execution.
    
    Args:
        stage: Stage name (defaults to method name)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Assume the class has a 'metrics' attribute
            if hasattr(self, 'metrics') and isinstance(self.metrics, UmbrixMetrics):
                method_stage = stage or func.__name__
                with MetricsTimer(self.metrics, method_stage):
                    return func(self, *args, **kwargs)
            else:
                return func(self, *args, **kwargs)
        return wrapper
    return decorator


# Decorator for external call timing
def timed_external_call(service: str, endpoint: Optional[str] = None):
    """
    Decorator to automatically time external calls.
    
    Args:
        service: External service name
        endpoint: Endpoint name (defaults to method name)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if hasattr(self, 'metrics') and isinstance(self.metrics, UmbrixMetrics):
                call_endpoint = endpoint or func.__name__
                with ExternalCallTimer(self.metrics, service, call_endpoint):
                    return func(self, *args, **kwargs)
            else:
                return func(self, *args, **kwargs)
        return wrapper
    return decorator


class MetricsHTTPHandler(BaseHTTPRequestHandler):
    """HTTP handler for metrics endpoint."""
    
    def __init__(self, registry: CollectorRegistry, *args, **kwargs):
        self.registry = registry
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET request for metrics."""
        if self.path == '/metrics':
            # Generate Prometheus metrics
            output = generate_latest(self.registry)
            
            # Send response
            self.send_response(200)
            self.send_header('Content-Type', CONTENT_TYPE_LATEST)
            self.send_header('Content-Length', str(len(output)))
            self.end_headers()
            self.wfile.write(output)
        elif self.path == '/health' or self.path == '/healthz':
            # Basic health check
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        """Override to use our logger instead of stderr."""
        logger.info(f"Metrics HTTP: {format % args}")


def start_metrics_server(
    port: int,
    registry: Optional[CollectorRegistry] = None,
    host: str = "0.0.0.0",
    max_attempts: int = 10,
) -> HTTPServer:
    """Start a Prometheus metrics HTTP server.

    When multiple agents run inside **one container/pod** they often try to
    bind the same default port (9464).  We make the bind *opportunistic*:

    • Attempt to listen on *port*.
    • If it's busy, increment the port and retry (up to *max_attempts*).

    The first agent still gets the canonical port, subsequent agents fall back
    to 9465, 9466, … which removes the noisy *"port already in use"* warnings
    while keeping metrics for every co-located process.
    """

    registry = registry or _GLOBAL_REGISTRY

    def handler_factory(*args, **kwargs):
        return MetricsHTTPHandler(registry, *args, **kwargs)

    attempt = 0
    while attempt < max_attempts:
        try_port = port + attempt
        try:
            server = HTTPServer((host, try_port), handler_factory)
            break  # success
        except OSError as exc:
            if exc.errno != 98:  # EADDRINUSE
                raise
            attempt += 1
            continue
    else:
        raise OSError(98, f"No free metrics port starting at {port}")

    logger.info("Starting metrics server on %s:%s", host, server.server_port)

    import threading

    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def setup_agent_metrics(
    agent_name: str,
    metrics_port: Optional[int] = None,
    registry: Optional[CollectorRegistry] = None
) -> UmbrixMetrics:
    """
    Setup standardized metrics for an agent.
    
    Args:
        agent_name: Name of the agent
        metrics_port: Port for HTTP metrics server (optional)
        registry: Prometheus registry (uses global if None)
    
    Returns:
        UmbrixMetrics instance
    """
    # Create metrics instance
    metrics = UmbrixMetrics(agent_name, registry)
    
    # Start metrics server if port provided
    if metrics_port:
        try:
            start_metrics_server(metrics_port, metrics.registry)
            logger.info(f"Metrics server started for {agent_name} on port {metrics_port}")
        except Exception as e:
            logger.error(f"Failed to start metrics server for {agent_name}: {e}")
    
    return metrics


def get_global_registry() -> CollectorRegistry:
    """Get the global Prometheus registry."""
    return _GLOBAL_REGISTRY


# Utility functions for manual instrumentation
def time_function(func, metrics: UmbrixMetrics, stage: Optional[str] = None):
    """Time a function execution and record metric."""
    stage_name = stage or func.__name__
    start_time = time.time()
    try:
        result = func()
        return result
    finally:
        duration = time.time() - start_time
        metrics.observe_processing_latency(stage_name, duration)


def count_errors(func, metrics: UmbrixMetrics, error_type: Optional[str] = None):
    """Execute function and count any exceptions."""
    error_name = error_type or "unknown"  # type: ignore
    try:
        return func()
    except Exception as e:
        actual_error_type = error_type or type(e).__name__
        metrics.increment_error(actual_error_type)
        raise


# Retry metrics
retry_attempts_total = Counter(
    'agent_retry_attempts_total',
    'Total number of retry attempts',
    ['agent_type', 'operation', 'attempt_number'],
    registry=_GLOBAL_REGISTRY
)

retry_success_total = Counter(
    'agent_retry_success_total',
    'Total number of successful retries',
    ['agent_type', 'operation'],
    registry=_GLOBAL_REGISTRY
)

retry_failure_total = Counter(
    'agent_retry_failure_total',
    'Total number of failed retries',
    ['agent_type', 'operation', 'error_type'],
    registry=_GLOBAL_REGISTRY
)

retry_delay_seconds = Histogram(
    'agent_retry_delay_seconds',
    'Time spent waiting between retry attempts',
    ['agent_type', 'operation'],
    registry=_GLOBAL_REGISTRY
)

class RetryEventTracker:
    """Track retry events for monitoring and analysis."""
    def __init__(self, agent_type: str):
        self.agent_type = agent_type

    def track_retry_attempt(self, operation: str, attempt: int, delay: float):
        retry_attempts_total.labels(
            agent_type=self.agent_type,
            operation=operation,
            attempt_number=attempt
        ).inc()
        retry_delay_seconds.labels(
            agent_type=self.agent_type,
            operation=operation
        ).observe(delay)

    def track_retry_success(self, operation: str):
        retry_success_total.labels(
            agent_type=self.agent_type,
            operation=operation
        ).inc()

    def track_retry_failure(self, operation: str, error_type: str):
        retry_failure_total.labels(
            agent_type=self.agent_type,
            operation=operation,
            error_type=error_type
        ).inc()


# Export commonly used items
__all__ = [
    "UmbrixMetrics",
    "MetricsTimer", 
    "ExternalCallTimer",
    "timed_method",
    "timed_external_call",
    "start_metrics_server",
    "setup_agent_metrics",
    "get_global_registry",
    "time_function",
    "count_errors",
    "RetryEventTracker"
]