"""
Centralized logging configuration and utilities for Umbrix agents.

This module provides standardized JSON logging configuration with correlation ID 
propagation for distributed tracing across all agents.
"""

import json
import logging
import logging.config
import os
import sys
from typing import Dict, Any, Optional, TYPE_CHECKING
from datetime import datetime, timezone

if TYPE_CHECKING:
    # For type checking and forward references
    from .structured_logging import StructuredLogger, StructuredJSONFormatter, LogContext, LogContextManager
try:
    from .structured_logging import (
        StructuredLogger,
        StructuredJSONFormatter,
        LogContext,
        LogContextManager,
        setup_agent_logging as _setup_agent_logging
    )
    STRUCTURED_LOGGING_AVAILABLE = True
except ImportError:
    STRUCTURED_LOGGING_AVAILABLE = False


# Global logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
        "formatters": {
        "json": {
            # Use string path to avoid static import issues
            "()": "common_tools.structured_logging.StructuredJSONFormatter"
        },
        "console": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "json" if os.getenv("LOG_FORMAT", "json").lower() == "json" else "console",
            "stream": "ext://sys.stdout"
        },
        "error_file": {
            "class": "logging.FileHandler",
            "formatter": "json",
            "filename": "/tmp/umbrix_errors.log",
            "mode": "a"
        }
    },
    "loggers": {
        "agents": {
            "level": os.getenv("LOG_LEVEL", "INFO").upper(),
            "handlers": ["console"],
            "propagate": False
        },
        "kafka": {
            "level": "WARNING",
            "handlers": ["console"],
            "propagate": False
        },
        "urllib3": {
            "level": "WARNING", 
            "handlers": ["console"],
            "propagate": False
        }
    },
    "root": {
        "level": "WARNING",
        "handlers": ["console"]
    }
}


def configure_logging(
    agent_name: Optional[str] = None,
    log_level: Optional[str] = None,
    log_format: Optional[str] = None,
    log_file: Optional[str] = None
) -> None:
    """
    Configure logging for the entire application.
    
    Args:
        agent_name: Name of the agent (for logging context)
        log_level: Override default log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Override default log format (json, console)
        log_file: Optional log file path for persistent logging
    """
    # Override defaults with parameters
    config = LOGGING_CONFIG.copy()
    
    if log_level:
        config["loggers"]["agents"]["level"] = log_level.upper()
    
    if log_format:
        formatter = "json" if log_format.lower() == "json" else "console"
        config["handlers"]["console"]["formatter"] = formatter
    
    if log_file:
        config["handlers"]["file"] = {
            "class": "logging.FileHandler",
            "formatter": "json",
            "filename": log_file,
            "mode": "a"
        }
        config["loggers"]["agents"]["handlers"].append("file")
    
    # Apply configuration
    logging.config.dictConfig(config)
    
    # Log configuration event
    if agent_name:
        logger = get_logger(agent_name)
        logger.info(
            f"Logging configured for agent {agent_name}",
            extra_fields={
                "event_type": "logging_configured",
                "log_level": config["loggers"]["agents"]["level"],
                "log_format": config["handlers"]["console"]["formatter"]
            }
        )


def get_logger(name: str) -> 'StructuredLogger':
    """
    Get a structured logger instance.
    
    Args:
        name: Logger name (typically agent name or module name)
    
    Returns:
        StructuredLogger instance configured for the agent
    """
    return StructuredLogger(f"agents.{name}", name)


def setup_agent_logging(
    agent_name: str,
    level: str = "INFO",
    enable_kafka_headers: bool = True
) -> 'StructuredLogger':
    """
    Setup comprehensive logging for an agent with correlation ID support.
    
    Args:
        agent_name: Name of the agent
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        enable_kafka_headers: Whether to enable Kafka header correlation ID extraction
    
    Returns:
        Configured StructuredLogger instance
    """
    # Configure overall logging
    configure_logging(agent_name, level)
    
    # Get structured logger
    logger = get_logger(agent_name)
    
    # Log agent initialization
    logger.info(
        f"Agent {agent_name} initialized with structured logging",
        extra_fields={
            "event_type": "agent_initialization",
            "log_level": level,
            "kafka_headers_enabled": enable_kafka_headers,
            "pid": os.getpid(),
            "python_version": sys.version.split()[0]
        }
    )
    
    return logger


def log_function_entry(
    logger: 'StructuredLogger',
    function_name: str,
    args: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None
) -> 'LogContextManager':
    """
    Log function entry and return a context manager for the function scope.
    
    Args:
        logger: StructuredLogger instance
        function_name: Name of the function being entered
        args: Function arguments to log
        correlation_id: Correlation ID for tracing
    
    Returns:
        LogContextManager for the function scope
    """
    # Generate correlation ID if not provided
    if not correlation_id:
        from .structured_logging import create_correlation_id
        correlation_id = create_correlation_id()
    
    # Create context for this function
    context = LogContext(
        agent_name=logger.agent_name,
        correlation_id=correlation_id,
        module=function_name,
        extra_fields={
            "event_type": "function_entry",
            "function_args": args or {}
        }
    )
    
    # Log function entry
    logger.info(
        f"Entering function: {function_name}",
        correlation_id=correlation_id,
        module=function_name,
        extra_fields={"event_type": "function_entry", "args": args}
    )
    
    return LogContextManager(logger, context)


def log_function_exit(
    logger: 'StructuredLogger',
    function_name: str,
    result: Any = None,
    execution_time_ms: Optional[int] = None,
    correlation_id: Optional[str] = None
) -> None:
    """
    Log function exit with results and timing information.
    
    Args:
        logger: StructuredLogger instance
        function_name: Name of the function being exited
        result: Function result (will be serialized)
        execution_time_ms: Function execution time in milliseconds
        correlation_id: Correlation ID for tracing
    """
    extra_fields = {
        "event_type": "function_exit",
        "execution_time_ms": execution_time_ms
    }
    
    # Add serializable result if provided
    if result is not None:
        try:
            # Try to serialize result for logging
            if hasattr(result, 'dict'):  # Pydantic model
                extra_fields["result"] = result.dict()
            elif isinstance(result, (dict, list, str, int, float, bool)):
                extra_fields["result"] = result
            else:
                extra_fields["result"] = str(result)
        except Exception:
            extra_fields["result"] = f"<non-serializable: {type(result).__name__}>"
    
    logger.info(
        f"Exiting function: {function_name}",
        correlation_id=correlation_id,
        module=function_name,
        extra_fields=extra_fields
    )


def log_kafka_message_received(
    logger: 'StructuredLogger',
    topic: str,
    partition: int,
    offset: int,
    key: Optional[str] = None,
    correlation_id: Optional[str] = None,
    message_size: Optional[int] = None
) -> None:
    """
    Log Kafka message received event with tracing information.
    
    Args:
        logger: StructuredLogger instance
        topic: Kafka topic name
        partition: Message partition
        offset: Message offset
        key: Message key
        correlation_id: Correlation ID from message headers
        message_size: Size of message in bytes
    """
    logger.info(
        f"Kafka message received from {topic}",
        correlation_id=correlation_id,
        module="kafka_consumer",
        extra_fields={
            "event_type": "kafka_message_received",
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "key": key,
            "message_size_bytes": message_size
        }
    )


def log_kafka_message_sent(
    logger: 'StructuredLogger',
    topic: str,
    key: Optional[str] = None,
    correlation_id: Optional[str] = None,
    message_size: Optional[int] = None,
    partition: Optional[int] = None
) -> None:
    """
    Log Kafka message sent event with tracing information.
    
    Args:
        logger: StructuredLogger instance
        topic: Kafka topic name
        key: Message key
        correlation_id: Correlation ID in message headers
        message_size: Size of message in bytes
        partition: Target partition (if specified)
    """
    logger.info(
        f"Kafka message sent to {topic}",
        correlation_id=correlation_id,
        module="kafka_producer",
        extra_fields={
            "event_type": "kafka_message_sent",
            "topic": topic,
            "key": key,
            "message_size_bytes": message_size,
            "partition": partition
        }
    )


def log_processing_error(
    logger: 'StructuredLogger',
    error: Exception,
    context: str,
    correlation_id: Optional[str] = None,
    recovery_action: Optional[str] = None,
    extra_data: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log processing errors with full context and recovery information.
    
    Args:
        logger: StructuredLogger instance
        error: Exception that occurred
        context: Context where error occurred
        correlation_id: Correlation ID for tracing
        recovery_action: Action taken to recover from error
        extra_data: Additional context data
    """
    error_data = {
        "event_type": "processing_error",
        "error_type": type(error).__name__,
        "error_message": str(error),
        "context": context,
        "recovery_action": recovery_action
    }
    
    if extra_data:
        error_data.update(extra_data)
    
    logger.error(
        f"Processing error in {context}: {error}",
        correlation_id=correlation_id,
        extra_fields=error_data
    )


def log_performance_metric(
    logger: 'StructuredLogger',
    metric_name: str,
    value: float,
    unit: str,
    correlation_id: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None
) -> None:
    """
    Log performance metrics in a structured format.
    
    Args:
        logger: StructuredLogger instance
        metric_name: Name of the metric
        value: Metric value
        unit: Unit of measurement
        correlation_id: Correlation ID for tracing
        tags: Additional tags for metric filtering
    """
    metric_data = {
        "event_type": "performance_metric",
        "metric_name": metric_name,
        "value": value,
        "unit": unit,
        "tags": tags or {}
    }
    
    logger.info(
        f"Performance metric: {metric_name}={value}{unit}",
        correlation_id=correlation_id,
        module="metrics",
        extra_fields=metric_data
    )


def create_child_logger(
    parent_logger: StructuredLogger,
    child_name: str
) -> StructuredLogger:
    """
    Create a child logger that inherits context from parent.
    
    Args:
        parent_logger: Parent StructuredLogger instance
        child_name: Name for the child logger
    
    Returns:
        Child StructuredLogger instance
    """
    child_logger = StructuredLogger(
        f"{parent_logger.logger.name}.{child_name}",
        f"{parent_logger.agent_name}.{child_name}"
    )
    
    # Copy current context from parent
    current_context = parent_logger.get_current_context()
    if current_context:
        child_context = LogContext(
            agent_name=child_logger.agent_name,
            correlation_id=current_context.correlation_id,
            module=f"{current_context.module}.{child_name}",
            task_id=current_context.task_id,
            feed_url=current_context.feed_url,
            extra_fields=current_context.extra_fields
        )
        child_logger.push_context(child_context)
    
    return child_logger


# Convenience functions for common logging patterns
def log_agent_startup(agent_name: str, version: Optional[str] = None, config: Optional[Dict[str, Any]] = None) -> None:
    """Log agent startup event."""
    logger = get_logger(agent_name)
    startup_data = {
        "event_type": "agent_startup",
        "version": version,
        "config": config
    }
    logger.info(f"Agent {agent_name} starting up", extra_fields=startup_data)


def log_agent_shutdown(agent_name: str, reason: Optional[str] = None) -> None:
    """Log agent shutdown event."""
    logger = get_logger(agent_name)
    logger.info(
        f"Agent {agent_name} shutting down",
        extra_fields={
            "event_type": "agent_shutdown",
            "reason": reason
        }
    )


def log_health_check(agent_name: str, status: str, checks: Optional[Dict[str, Any]] = None) -> None:
    """Log health check result."""
    logger = get_logger(agent_name)
    logger.info(
        f"Health check: {status}",
        extra_fields={
            "event_type": "health_check",
            "status": status,
            "checks": checks or {}
        }
    )


# Export commonly used items for convenience
__all__ = [
    "configure_logging",
    "get_logger", 
    "setup_agent_logging",
    "log_function_entry",
    "log_function_exit",
    "log_kafka_message_received",
    "log_kafka_message_sent",
    "log_processing_error",
    "log_performance_metric",
    "create_child_logger",
    "log_agent_startup",
    "log_agent_shutdown",
    "log_health_check",
    "StructuredLogger",
    "LogContext",
    "LogContextManager",
    "setup_logging"
]


def setup_logging(name: str) -> logging.Logger:
    """
    Simple logging setup function for backward compatibility.
    
    Args:
        name: Logger name
        
    Returns:
        Configured logger instance
    """
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