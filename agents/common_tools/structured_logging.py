"""
Structured JSON logging utilities for Umbrix agents.
Provides consistent logging patterns with correlation IDs and standardized fields.
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
import threading


@dataclass
class LogContext:
    """Context information for structured logging."""
    agent_name: str
    correlation_id: str
    module: str = ""
    task_id: Optional[str] = None
    feed_url: Optional[str] = None
    extra_fields: Optional[Dict[str, Any]] = None


class StructuredJSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = "umbrix-agent"  # Can be made configurable
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        # Base log structure
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": getattr(record, 'component', getattr(record, 'module', record.name)),
            "message": record.getMessage(),
            "hostname": self.hostname,
            "thread_id": record.thread,
            "process_id": record.process
        }
        
        # Add agent-specific context if available
        if hasattr(record, 'agent_name'):
            log_entry["agent_name"] = record.agent_name
        
        if hasattr(record, 'correlation_id'):
            log_entry["correlation_id"] = record.correlation_id
        
        if hasattr(record, 'task_id'):
            log_entry["task_id"] = record.task_id
        
        if hasattr(record, 'feed_url'):
            log_entry["feed_url"] = record.feed_url
        
        # Preserve additional fields in a dedicated dictionary so callers can
        # access them without colliding with the top-level schema expected by
        # downstream log processors.
        if hasattr(record, 'extra_fields') and record.extra_fields:
            log_entry["extra_fields"] = record.extra_fields
        
        # Add exception information if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info) if record.exc_info else None
            }
        
        return json.dumps(log_entry, default=str, ensure_ascii=False)


class StructuredLogger:
    """Wrapper for structured logging with context management."""
    
    def __init__(self, name: str, agent_name: str):
        self.logger = logging.getLogger(name)
        self.agent_name = agent_name
        self._context_stack = threading.local()
        
        # Configure formatter if not already done
        if not self.logger.handlers or not isinstance(
            self.logger.handlers[0].formatter, StructuredJSONFormatter
        ):
            self._setup_structured_logging()
    
    def _setup_structured_logging(self):
        """Setup structured JSON logging for this logger."""
        # Remove existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Add new structured handler
        handler = logging.StreamHandler()
        handler.setFormatter(StructuredJSONFormatter())
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
    
    def _get_context_stack(self):
        """Get thread-local context stack."""
        if not hasattr(self._context_stack, 'stack'):
            self._context_stack.stack = []
        return self._context_stack.stack
    
    def push_context(self, context: LogContext):
        """Push a logging context onto the stack."""
        stack = self._get_context_stack()
        stack.append(context)
    
    def pop_context(self) -> Optional[LogContext]:
        """Pop the current logging context from the stack."""
        stack = self._get_context_stack()
        return stack.pop() if stack else None
    
    def get_current_context(self) -> Optional[LogContext]:
        """Get the current logging context."""
        stack = self._get_context_stack()
        return stack[-1] if stack else None
    
    def _log_with_context(
        self,
        level: int,
        message: str,
        *,
        extra_fields: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
        task_id: Optional[str] = None,
        module: Optional[str] = None,
        feed_url: Optional[str] = None,
        **log_kwargs,
    ):
        """Log with structured context."""
        # Get current context
        context = self.get_current_context()
        
        # Prepare extra information for log record
        # Note: 'module' is renamed to 'component' to avoid conflict with LogRecord's reserved field
        extra = {
            'agent_name': self.agent_name,
            'correlation_id': correlation_id or (context.correlation_id if context else None),
            'task_id': task_id or (context.task_id if context else None),
            'component': module or (context.module if context else ""),
            'feed_url': feed_url or (context.feed_url if context else None),
            'extra_fields': extra_fields or (context.extra_fields if context else {})
        }
        
        # Remove None values
        extra = {k: v for k, v in extra.items() if v is not None}
        
        self.logger.log(level, message, extra=extra, **log_kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message with context."""
        self._log_with_context(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with context."""
        self._log_with_context(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message with context."""
        self._log_with_context(logging.ERROR, message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with context."""
        self._log_with_context(logging.DEBUG, message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message with context."""
        self._log_with_context(logging.CRITICAL, message, **kwargs)
    
    def log_task_event(self, event_type: str, task_id: str, 
                      correlation_id: str, task_type: str = None,
                      agent_target: str = None, status: str = None,
                      error: str = None, execution_time_ms: int = None,
                      extra_fields: Optional[Dict[str, Any]] = None):
        """Log a structured task lifecycle event."""
        event_data = {
            "event_type": event_type,
            "task_id": task_id,
            "task_type": task_type,
            "agent_target": agent_target,
            "status": status,
            "error": error,
            "execution_time_ms": execution_time_ms
        }
        
        # Remove None values
        event_data = {k: v for k, v in event_data.items() if v is not None}
        
        # Merge with extra fields
        if extra_fields:
            event_data.update(extra_fields)
        
        message = f"Task {event_type}: {task_id}"
        if task_type:
            message += f" (type: {task_type})"
        
        level = logging.ERROR if error else logging.INFO
        self._log_with_context(
            level, message,
            extra_fields=event_data,
            correlation_id=correlation_id,
            task_id=task_id,
            module="task_orchestration"
        )


class LogContextManager:
    """Context manager for structured logging contexts."""
    
    def __init__(self, logger: StructuredLogger, context: LogContext):
        self.logger = logger
        self.context = context
    
    def __enter__(self):
        self.logger.push_context(self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.pop_context()


def create_correlation_id() -> str:
    """Generate a new correlation ID."""
    return f"corr_{uuid.uuid4().hex[:16]}"


def create_task_id() -> str:
    """Generate a new task ID."""
    timestamp = int(time.time() * 1000)  # milliseconds
    random_suffix = uuid.uuid4().hex[:8]
    return f"task_{timestamp}_{random_suffix}"


def setup_agent_logging(agent_name: str, level: str = "INFO") -> StructuredLogger:
    """Setup structured logging for an agent."""
    logger = StructuredLogger(agent_name, agent_name)
    logger.logger.setLevel(getattr(logging, level.upper()))
    
    # Log agent startup
    logger.info(
        f"Agent {agent_name} logging initialized",
        extra_fields={"event_type": "agent_startup"}
    )
    
    return logger


# Example usage and testing functions
if __name__ == "__main__":
    # Example usage
    logger = setup_agent_logging("test_agent")
    
    # Log with context
    context = LogContext(
        agent_name="test_agent",
        correlation_id="test_corr_123",
        module="test_module",
        task_id="test_task_456"
    )
    
    with LogContextManager(logger, context):
        logger.info("This is a test message with context")
        logger.error("This is an error message", extra_fields={"error_code": "TEST001"})
    
    # Log task events
    logger.log_task_event(
        event_type="task_received",
        task_id="test_task_789",
        correlation_id="test_corr_456",
        task_type="feed_discovery",
        extra_fields={"priority": "high"}
    )