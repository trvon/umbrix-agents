"""
Backend Integration for OPIK Observability

This module provides integration points between the Python agents
and the Rust backend to expose OPIK tracing and observability data.
"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from .enhanced_opik_integration import get_opik_manager, ThreatIntelTraceMetrics
from .opik_admin_ui import get_admin_interface
from .instrumented_dspy_modules import setup_instrumented_dspy_pipeline

logger = logging.getLogger(__name__)

class BackendOpikBridge:
    """Bridge between Python OPIK integration and Rust backend"""
    
    def __init__(self):
        self.opik_manager = get_opik_manager()
        self.admin_interface = get_admin_interface()
    
    def get_observability_status(self) -> Dict[str, Any]:
        """Get current observability status for backend API"""
        try:
            health_report = self.admin_interface.generate_health_report()
            
            # Format for backend consumption
            status = {
                "opik_integration": {
                    "enabled": self.opik_manager.config.enabled,
                    "initialized": self.opik_manager._initialized,
                    "health_score": health_report.get("overall_health", {}).get("score", 0),
                    "status": health_report.get("overall_health", {}).get("status", "unknown"),
                    "dashboard_url": self.opik_manager.get_dashboard_url(),
                    "project_name": self.opik_manager.config.project_name
                },
                "dspy_instrumentation": {
                    "auto_enabled": self.opik_manager.config.auto_instrument_dspy,
                    "callback_active": self.opik_manager.dspy_callback is not None,
                    "trace_sampling_rate": self.opik_manager.config.trace_sampling_rate
                },
                "recent_metrics": health_report.get("recent_activity", {}),
                "timestamp": datetime.utcnow().isoformat()
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Failed to get observability status: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def trace_intelligent_graph_query(
        self,
        query: str,
        query_type: str,
        result_count: int,
        processing_time_ms: float,
        success: bool,
        error_message: Optional[str] = None
    ) -> str:
        """Create trace for intelligent graph query execution"""
        try:
            if not self.opik_manager.config.enabled:
                return "opik_disabled"
            
            trace_name = "intelligent_graph_query"
            input_data = {
                "query": query[:200] + "..." if len(query) > 200 else query,
                "query_type": query_type,
                "query_length": len(query)
            }
            
            with self.opik_manager.trace_threat_pipeline(
                name=trace_name,
                input_data=input_data,
                tags=["graph_query", "backend", "intelligent"],
                metadata={
                    "component": "intelligent_graph_query_tool",
                    "backend": "rust"
                }
            ) as trace:
                
                if trace:
                    # Create span for query processing
                    processing_span = self.opik_manager.create_span(
                        trace,
                        "query_processing",
                        "processing",
                        {
                            "query_type": query_type,
                            "result_count": result_count
                        }
                    )
                    
                    if processing_span:
                        if success:
                            processing_span.end(
                                output={
                                    "success": True,
                                    "result_count": result_count,
                                    "processing_time_ms": processing_time_ms
                                }
                            )
                        else:
                            processing_span.end(
                                output={
                                    "success": False,
                                    "error": error_message,
                                    "processing_time_ms": processing_time_ms
                                }
                            )
                    
                    # Log metrics
                    metrics = ThreatIntelTraceMetrics(
                        processing_time_ms=processing_time_ms,
                        token_count=0,  # Graph queries don't use tokens
                        cost_usd=0.0,   # No cost for graph queries
                        data_source="graph_database"
                    )
                    
                    self.opik_manager.log_metrics(trace, metrics, "graph_query")
                    
                    return f"traced:{trace_name}"
                
            return "trace_created"
            
        except Exception as e:
            logger.error(f"Failed to trace graph query: {e}")
            return f"trace_failed:{str(e)}"
    
    def trace_llm_tool_execution(
        self,
        tool_name: str,
        input_params: Dict[str, Any],
        output_result: Dict[str, Any],
        processing_time_ms: float,
        token_usage: Optional[Dict[str, int]] = None,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> str:
        """Create trace for LLM tool execution from backend"""
        try:
            if not self.opik_manager.config.enabled:
                return "opik_disabled"
            
            trace_name = f"llm_tool_{tool_name}"
            
            # Sanitize input params for tracing
            sanitized_input = self.opik_manager._sanitize_trace_data(input_params)
            
            with self.opik_manager.trace_threat_pipeline(
                name=trace_name,
                input_data=sanitized_input,
                tags=["llm_tool", "backend", tool_name],
                metadata={
                    "tool_name": tool_name,
                    "component": "llm_tools",
                    "backend": "rust"
                }
            ) as trace:
                
                if trace:
                    # Create span for tool execution
                    tool_span = self.opik_manager.create_span(
                        trace,
                        f"{tool_name}_execution",
                        "llm",
                        {"input_size": len(str(input_params))}
                    )
                    
                    if tool_span:
                        if success:
                            tool_span.end(
                                output={
                                    "success": True,
                                    "result_size": len(str(output_result)),
                                    "processing_time_ms": processing_time_ms
                                }
                            )
                        else:
                            tool_span.end(
                                output={
                                    "success": False,
                                    "error": error_message,
                                    "processing_time_ms": processing_time_ms
                                }
                            )
                    
                    # Calculate metrics
                    token_count = 0
                    cost_usd = 0.0
                    
                    if token_usage:
                        token_count = token_usage.get("total_tokens", 0)
                        # Rough cost estimation (would need actual pricing)
                        cost_usd = token_count * 0.00002
                    
                    metrics = ThreatIntelTraceMetrics(
                        processing_time_ms=processing_time_ms,
                        token_count=token_count,
                        cost_usd=cost_usd,
                        data_source="llm_tool"
                    )
                    
                    self.opik_manager.log_metrics(trace, metrics, f"llm_tool_{tool_name}")
                    
                    return f"traced:{trace_name}"
                
            return "trace_created"
            
        except Exception as e:
            logger.error(f"Failed to trace LLM tool execution: {e}")
            return f"trace_failed:{str(e)}"
    
    def get_admin_dashboard_data(self) -> Dict[str, Any]:
        """Get data for admin dashboard rendering"""
        try:
            dashboard_info = self.admin_interface.get_dashboard_info()
            system_metrics = self.admin_interface.get_system_metrics()
            traces_summary = self.admin_interface.get_recent_traces_summary()
            
            return {
                "dashboard_info": dashboard_info,
                "system_metrics": system_metrics,
                "traces_summary": traces_summary,
                "health_score": self._calculate_health_score(dashboard_info, traces_summary),
                "recommendations": self._get_recommendations(dashboard_info, traces_summary)
            }
            
        except Exception as e:
            logger.error(f"Failed to get admin dashboard data: {e}")
            return {
                "error": str(e),
                "dashboard_info": {"available": False},
                "health_score": 0
            }
    
    def _calculate_health_score(self, dashboard_info: Dict, traces_summary: Dict) -> int:
        """Calculate overall health score for observability"""
        score = 0
        
        if dashboard_info.get("enabled"):
            score += 25
        
        if dashboard_info.get("initialized"):
            score += 25
        
        if dashboard_info.get("dspy_instrumented"):
            score += 25
        
        error_rate = traces_summary.get("error_rate", 100)
        if error_rate < 5:
            score += 25
        elif error_rate < 15:
            score += 15
        elif error_rate < 25:
            score += 5
        
        return score
    
    def _get_recommendations(self, dashboard_info: Dict, traces_summary: Dict) -> List[str]:
        """Get recommendations for improving observability"""
        recommendations = []
        
        if not dashboard_info.get("enabled"):
            recommendations.append("Enable OPIK integration for better observability")
        
        if not dashboard_info.get("dspy_instrumented"):
            recommendations.append("Enable DSPy auto-instrumentation for ML pipeline tracing")
        
        error_rate = traces_summary.get("error_rate", 0)
        if error_rate > 15:
            recommendations.append(f"High error rate ({error_rate:.1f}%) - investigate failing operations")
        
        if traces_summary.get("quality_threshold_breaches", 0) > 0:
            recommendations.append("Quality threshold breaches detected - review model performance")
        
        if not recommendations:
            recommendations.append("Observability is healthy - consider expanding coverage to more components")
        
        return recommendations
    
    def export_traces_for_analysis(self, hours: int = 24) -> Dict[str, Any]:
        """Export trace data for external analysis"""
        try:
            # In a real implementation, this would query OPIK API
            # For now, provide structure for what would be exported
            
            export_data = {
                "export_metadata": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "time_range_hours": hours,
                    "source": "umbrix_opik_integration",
                    "version": "v1.0"
                },
                "summary": self.admin_interface.get_recent_traces_summary(hours),
                "traces": [],  # Would contain actual trace data from OPIK API
                "metrics_aggregations": {
                    "by_operation": {},
                    "by_hour": {},
                    "by_component": {},
                    "error_patterns": []
                },
                "recommendations": self._get_recommendations(
                    self.admin_interface.get_dashboard_info(),
                    self.admin_interface.get_recent_traces_summary(hours)
                )
            }
            
            return export_data
            
        except Exception as e:
            logger.error(f"Failed to export traces: {e}")
            return {
                "error": str(e),
                "export_metadata": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "status": "failed"
                }
            }

# Rust FFI-compatible functions for backend integration
def get_opik_status_json() -> str:
    """Get OPIK status as JSON string for Rust backend"""
    try:
        bridge = BackendOpikBridge()
        status = bridge.get_observability_status()
        return json.dumps(status)
    except Exception as e:
        return json.dumps({"error": str(e)})

def trace_graph_query_json(
    query: str,
    query_type: str,
    result_count: int,
    processing_time_ms: float,
    success: bool,
    error_message: Optional[str] = None
) -> str:
    """Trace graph query and return result as JSON string"""
    try:
        bridge = BackendOpikBridge()
        result = bridge.trace_intelligent_graph_query(
            query, query_type, result_count, processing_time_ms, success, error_message
        )
        return json.dumps({"status": "success", "trace_id": result})
    except Exception as e:
        return json.dumps({"status": "error", "error": str(e)})

def trace_llm_tool_json(
    tool_name: str,
    input_json: str,
    output_json: str,
    processing_time_ms: float,
    token_usage_json: Optional[str] = None,
    success: bool = True,
    error_message: Optional[str] = None
) -> str:
    """Trace LLM tool execution and return result as JSON string"""
    try:
        bridge = BackendOpikBridge()
        
        input_params = json.loads(input_json)
        output_result = json.loads(output_json)
        token_usage = json.loads(token_usage_json) if token_usage_json else None
        
        result = bridge.trace_llm_tool_execution(
            tool_name, input_params, output_result, processing_time_ms,
            token_usage, success, error_message
        )
        
        return json.dumps({"status": "success", "trace_id": result})
    except Exception as e:
        return json.dumps({"status": "error", "error": str(e)})

def get_admin_dashboard_json() -> str:
    """Get admin dashboard data as JSON string"""
    try:
        bridge = BackendOpikBridge()
        data = bridge.get_admin_dashboard_data()
        return json.dumps(data)
    except Exception as e:
        return json.dumps({"error": str(e)})

# Global bridge instance
_backend_bridge: Optional[BackendOpikBridge] = None

def get_backend_bridge() -> BackendOpikBridge:
    """Get or create the global backend bridge instance"""
    global _backend_bridge
    if _backend_bridge is None:
        _backend_bridge = BackendOpikBridge()
    return _backend_bridge

# Export main classes and functions
__all__ = [
    "BackendOpikBridge",
    "get_backend_bridge",
    "get_opik_status_json",
    "trace_graph_query_json", 
    "trace_llm_tool_json",
    "get_admin_dashboard_json"
]