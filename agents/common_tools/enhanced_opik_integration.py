"""
Enhanced OPIK Integration for DSPy Pipeline Observability

This module provides comprehensive OPIK instrumentation for DSPy threat intelligence
pipelines with advanced tracing, metrics collection, and admin UI exposure.

Features:
- Automatic DSPy module tracing with OpikCallback
- Custom trace hierarchy for threat intelligence workflows
- Performance metrics and cost tracking
- Distributed tracing across microservices
- Admin UI dashboard integration
- Quality score tracking and alerting
"""

from __future__ import annotations

import os
import sys
import json
import time
import asyncio
import functools
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import logging

# Optional OPIK imports with graceful degradation
try:
    import opik
    from opik import track, opik_context, Opik
    from opik.integrations.dspy.callback import OpikCallback
    OPIK_AVAILABLE = True
except ImportError:
    OPIK_AVAILABLE = False
    # Create mock classes for type hints
    class OpikCallback:
        def __init__(self, *args, **kwargs): pass
    
    def track(*args, **kwargs):
        def decorator(func): return func
        return decorator

import dspy
from .opik_wrapper import trace_llm_call

logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_PROJECT_NAME = "Umbrix-ThreatIntel"
DEFAULT_OPIK_URL = "http://localhost:5173"  # Local OPIK instance
TRACE_TIMEOUT_SECONDS = 30
MAX_TRACE_SIZE_BYTES = 1024 * 1024  # 1MB

@dataclass
class ThreatIntelTraceMetrics:
    """Metrics for threat intelligence pipeline tracing"""
    processing_time_ms: float
    token_count: int
    cost_usd: float
    accuracy_score: Optional[float] = None
    confidence_score: Optional[float] = None
    ioc_count: int = 0
    false_positive_rate: Optional[float] = None
    threat_level: Optional[str] = None
    data_source: Optional[str] = None

@dataclass
class OpikConfig:
    """Configuration for OPIK integration"""
    enabled: bool = True
    use_local: bool = True
    project_name: str = DEFAULT_PROJECT_NAME
    api_key: Optional[str] = None
    workspace: Optional[str] = None
    log_graph: bool = True
    auto_instrument_dspy: bool = True
    trace_sampling_rate: float = 1.0
    admin_ui_enabled: bool = True
    quality_threshold: float = 0.8

class OpikIntegrationManager:
    """Enhanced OPIK integration manager for threat intelligence pipeline"""
    
    def __init__(self, config: Optional[OpikConfig] = None):
        self.config = config or self._load_config_from_env()
        self.client: Optional[Opik] = None
        self.dspy_callback: Optional[OpikCallback] = None
        self._initialized = False
        self._session_start_time = time.time()
        
        if self.config.enabled and OPIK_AVAILABLE:
            self._initialize_opik()
    
    @classmethod
    def _load_config_from_env(cls) -> OpikConfig:
        """Load OPIK configuration from environment variables"""
        return OpikConfig(
            enabled=os.getenv("OPIK_ENABLED", "true").lower() == "true",
            use_local=os.getenv("OPIK_USE_LOCAL", "true").lower() == "true",
            project_name=os.getenv("OPIK_PROJECT_NAME", DEFAULT_PROJECT_NAME),
            api_key=os.getenv("OPIK_API_KEY"),
            workspace=os.getenv("OPIK_WORKSPACE"),
            log_graph=os.getenv("OPIK_LOG_GRAPH", "true").lower() == "true",
            auto_instrument_dspy=os.getenv("OPIK_AUTO_INSTRUMENT_DSPY", "true").lower() == "true",
            trace_sampling_rate=float(os.getenv("OPIK_TRACE_SAMPLING_RATE", "1.0")),
            admin_ui_enabled=os.getenv("OPIK_ADMIN_UI_ENABLED", "true").lower() == "true",
            quality_threshold=float(os.getenv("OPIK_QUALITY_THRESHOLD", "0.8"))
        )
    
    def _initialize_opik(self):
        """Initialize OPIK client and DSPy integration"""
        try:
            # Configure OPIK
            if self.config.use_local:
                opik.configure(use_local=True)
                logger.info(f"OPIK configured for local use. Dashboard: {DEFAULT_OPIK_URL}")
            else:
                if not self.config.api_key:
                    logger.warning("OPIK API key not provided, disabling OPIK integration")
                    self.config.enabled = False
                    return
                
                opik.configure(
                    api_key=self.config.api_key,
                    workspace=self.config.workspace,
                    use_local=False
                )
                logger.info("OPIK configured for hosted use")
            
            # Initialize client
            self.client = Opik(project_name=self.config.project_name)
            
            # Create DSPy callback if auto-instrumentation is enabled
            if self.config.auto_instrument_dspy:
                self.dspy_callback = OpikCallback(
                    project_name=self.config.project_name,
                    log_graph=self.config.log_graph
                )
                logger.info("DSPy auto-instrumentation enabled")
            
            self._initialized = True
            logger.info(f"OPIK integration initialized successfully for project: {self.config.project_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize OPIK: {e}")
            self.config.enabled = False
    
    def configure_dspy(self, lm: Any, **kwargs) -> None:
        """Configure DSPy with OPIK tracing"""
        if not self._initialized or not self.dspy_callback:
            logger.warning("OPIK not initialized or DSPy callback not available")
            return
        
        callbacks = kwargs.get('callbacks', [])
        if self.dspy_callback not in callbacks:
            callbacks.append(self.dspy_callback)
        
        dspy.configure(lm=lm, callbacks=callbacks, **kwargs)
        logger.info("DSPy configured with OPIK tracing")
    
    @contextmanager
    def trace_threat_pipeline(
        self,
        name: str,
        input_data: Dict[str, Any],
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Context manager for tracing threat intelligence pipeline execution"""
        if not self._initialized:
            yield None
            return
        
        # Apply sampling
        if self.config.trace_sampling_rate < 1.0:
            import random
            if random.random() > self.config.trace_sampling_rate:
                yield None
                return
        
        start_time = time.time()
        trace = None
        
        try:
            # Sanitize input data to prevent large traces
            sanitized_input = self._sanitize_trace_data(input_data)
            
            trace = self.client.trace(
                name=name,
                input=sanitized_input,
                tags=(tags or []) + ["threat_intel", "pipeline"],
                metadata={
                    **(metadata or {}),
                    "session_id": str(int(self._session_start_time)),
                    "pipeline_version": "v2.0",
                    "environment": os.getenv("ENVIRONMENT", "development")
                }
            )
            
            yield trace
            
        except Exception as e:
            if trace:
                trace.end(
                    output={"error": str(e), "status": "failed"},
                    metadata={"error_type": type(e).__name__}
                )
            logger.error(f"Error in trace_threat_pipeline: {e}")
            raise
            
        finally:
            if trace:
                processing_time = (time.time() - start_time) * 1000
                trace.end(
                    output={"status": "completed", "processing_time_ms": processing_time},
                    metadata={"total_processing_time_ms": processing_time}
                )
    
    def create_span(
        self,
        trace: Any,
        name: str,
        span_type: str = "processing",
        input_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        """Create a child span within a trace"""
        if not trace or not self._initialized:
            return None
        
        sanitized_input = self._sanitize_trace_data(input_data) if input_data else None
        
        return trace.span(
            name=name,
            type=span_type,
            input=sanitized_input,
            **kwargs
        )
    
    def log_metrics(
        self,
        trace: Any,
        metrics: ThreatIntelTraceMetrics,
        component: str = "pipeline"
    ):
        """Log threat intelligence metrics to current trace"""
        if not trace or not self._initialized:
            return
        
        try:
            # Convert metrics to dict and add component info
            metrics_dict = asdict(metrics)
            metrics_dict["component"] = component
            
            # Update trace with metrics
            trace.update(
                metadata={"metrics": metrics_dict},
                feedback_scores=[
                    {"name": "processing_time_ms", "value": metrics.processing_time_ms},
                    {"name": "cost_efficiency", "value": 1.0 / max(metrics.cost_usd, 0.001)},
                ]
            )
            
            # Add quality scores if available
            if metrics.accuracy_score is not None:
                trace.update(
                    feedback_scores=[
                        {"name": "accuracy", "value": metrics.accuracy_score}
                    ]
                )
            
            if metrics.confidence_score is not None:
                trace.update(
                    feedback_scores=[
                        {"name": "confidence", "value": metrics.confidence_score}
                    ]
                )
            
            # Check quality threshold and alert if needed
            if (metrics.accuracy_score is not None and 
                metrics.accuracy_score < self.config.quality_threshold):
                self._trigger_quality_alert(metrics, component)
                
        except Exception as e:
            logger.error(f"Failed to log metrics: {e}")
    
    def _sanitize_trace_data(self, data: Any, max_size: int = MAX_TRACE_SIZE_BYTES) -> Any:
        """Sanitize data for tracing to prevent oversized traces"""
        if not isinstance(data, (dict, list, str)):
            return str(data)
        
        try:
            serialized = json.dumps(data)
            if len(serialized) <= max_size:
                return data
            
            # Truncate large content
            if isinstance(data, str):
                return data[:max_size//2] + "...[truncated]"
            elif isinstance(data, dict):
                sanitized = {}
                for k, v in data.items():
                    if k.lower() in ['content', 'raw_content', 'html', 'text']:
                        # Truncate large text fields
                        if isinstance(v, str) and len(v) > 1000:
                            sanitized[k] = v[:500] + "...[truncated]"
                        else:
                            sanitized[k] = v
                    else:
                        sanitized[k] = v
                return sanitized
            elif isinstance(data, list):
                # Limit list length
                if len(data) > 100:
                    return data[:50] + ["...[truncated]"]
                return data
                
        except Exception as e:
            logger.warning(f"Failed to sanitize trace data: {e}")
            return {"error": "sanitization_failed", "type": str(type(data))}
        
        return data
    
    def _trigger_quality_alert(self, metrics: ThreatIntelTraceMetrics, component: str):
        """Trigger quality alert for poor performance"""
        alert_data = {
            "component": component,
            "accuracy": metrics.accuracy_score,
            "threshold": self.config.quality_threshold,
            "timestamp": time.time(),
            "threat_level": metrics.threat_level,
            "data_source": metrics.data_source
        }
        
        logger.warning(f"Quality threshold breach detected: {alert_data}")
        
        # Additional alerting logic could be added here
        # (e.g., sending to monitoring systems, Slack, etc.)
    
    def get_dashboard_url(self) -> Optional[str]:
        """Get the URL for the OPIK dashboard"""
        if not self.config.admin_ui_enabled:
            return None
        
        if self.config.use_local:
            return DEFAULT_OPIK_URL
        else:
            workspace = self.config.workspace or "default"
            return f"https://www.comet.com/opik/{workspace}/projects/{self.config.project_name}"
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on OPIK integration"""
        health_status = {
            "opik_available": OPIK_AVAILABLE,
            "integration_enabled": self.config.enabled,
            "initialized": self._initialized,
            "project_name": self.config.project_name,
            "dashboard_url": self.get_dashboard_url(),
            "dspy_instrumented": self.dspy_callback is not None,
            "config": asdict(self.config)
        }
        
        if self._initialized and self.client:
            try:
                # Simple test trace
                test_trace = self.client.trace(
                    name="health_check",
                    input={"test": True},
                    tags=["health_check"]
                )
                test_trace.end(output={"status": "ok"})
                health_status["trace_test"] = "passed"
            except Exception as e:
                health_status["trace_test"] = f"failed: {e}"
                health_status["initialized"] = False
        
        return health_status

# Global instance
_opik_manager: Optional[OpikIntegrationManager] = None

def get_opik_manager() -> OpikIntegrationManager:
    """Get or create the global OPIK manager instance"""
    global _opik_manager
    if _opik_manager is None:
        _opik_manager = OpikIntegrationManager()
    return _opik_manager

def initialize_opik_for_dspy(lm: Any, **kwargs) -> OpikIntegrationManager:
    """Initialize OPIK integration and configure DSPy"""
    manager = get_opik_manager()
    manager.configure_dspy(lm, **kwargs)
    return manager

# Convenience decorators
def trace_threat_analysis(
    name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    quality_threshold: Optional[float] = None
):
    """Decorator for tracing threat analysis functions"""
    def decorator(func: Callable) -> Callable:
        if not OPIK_AVAILABLE:
            return func
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            manager = get_opik_manager()
            if not manager.config.enabled:
                return func(*args, **kwargs)
            
            func_name = name or f"{func.__module__}.{func.__name__}"
            input_data = {"args_count": len(args), "kwargs_keys": list(kwargs.keys())}
            
            with manager.trace_threat_pipeline(
                name=func_name,
                input_data=input_data,
                tags=(tags or []) + ["threat_analysis"]
            ) as trace:
                start_time = time.time()
                
                try:
                    result = func(*args, **kwargs)
                    
                    # Calculate basic metrics
                    processing_time = (time.time() - start_time) * 1000
                    metrics = ThreatIntelTraceMetrics(
                        processing_time_ms=processing_time,
                        token_count=0,  # Would need to be calculated based on actual usage
                        cost_usd=0.0,   # Would need to be calculated based on actual usage
                    )
                    
                    if trace:
                        manager.log_metrics(trace, metrics, component=func.__name__)
                    
                    return result
                    
                except Exception as e:
                    if trace:
                        trace.update(
                            output={"error": str(e), "status": "failed"},
                            metadata={"error_type": type(e).__name__}
                        )
                    raise
        
        return wrapper
    return decorator

# Async version of the decorator
def trace_threat_analysis_async(
    name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    quality_threshold: Optional[float] = None
):
    """Async decorator for tracing threat analysis functions"""
    def decorator(func: Callable) -> Callable:
        if not OPIK_AVAILABLE:
            return func
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            manager = get_opik_manager()
            if not manager.config.enabled:
                return await func(*args, **kwargs)
            
            func_name = name or f"{func.__module__}.{func.__name__}"
            input_data = {"args_count": len(args), "kwargs_keys": list(kwargs.keys())}
            
            with manager.trace_threat_pipeline(
                name=func_name,
                input_data=input_data,
                tags=(tags or []) + ["threat_analysis", "async"]
            ) as trace:
                start_time = time.time()
                
                try:
                    result = await func(*args, **kwargs)
                    
                    # Calculate basic metrics
                    processing_time = (time.time() - start_time) * 1000
                    metrics = ThreatIntelTraceMetrics(
                        processing_time_ms=processing_time,
                        token_count=0,
                        cost_usd=0.0,
                    )
                    
                    if trace:
                        manager.log_metrics(trace, metrics, component=func.__name__)
                    
                    return result
                    
                except Exception as e:
                    if trace:
                        trace.update(
                            output={"error": str(e), "status": "failed"},
                            metadata={"error_type": type(e).__name__}
                        )
                    raise
        
        return wrapper
    return decorator

# Export main classes and functions
__all__ = [
    "OpikIntegrationManager",
    "OpikConfig", 
    "ThreatIntelTraceMetrics",
    "get_opik_manager",
    "initialize_opik_for_dspy",
    "trace_threat_analysis",
    "trace_threat_analysis_async",
    "OPIK_AVAILABLE"
]