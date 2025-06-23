"""
OPIK Admin UI Integration

This module provides web interface components for exposing OPIK traces
and observability data through the Umbrix admin interface.
"""

import json
import time
from typing import Dict, List, Any, Optional
from dataclasses import asdict
from datetime import datetime, timedelta
import logging

from .enhanced_opik_integration import get_opik_manager, OPIK_AVAILABLE

logger = logging.getLogger(__name__)

class OpikAdminInterface:
    """Admin interface for OPIK observability data"""
    
    def __init__(self):
        self.opik_manager = get_opik_manager()
    
    def get_dashboard_info(self) -> Dict[str, Any]:
        """Get dashboard information for admin UI"""
        try:
            health_status = self.opik_manager.health_check()
            
            dashboard_info = {
                "available": OPIK_AVAILABLE,
                "enabled": health_status.get("integration_enabled", False),
                "initialized": health_status.get("initialized", False),
                "dashboard_url": health_status.get("dashboard_url"),
                "project_name": health_status.get("project_name"),
                "dspy_instrumented": health_status.get("dspy_instrumented", False),
                "last_check": datetime.utcnow().isoformat(),
                "status": "healthy" if health_status.get("initialized") else "degraded"
            }
            
            return dashboard_info
            
        except Exception as e:
            logger.error(f"Failed to get OPIK dashboard info: {e}")
            return {
                "available": False,
                "enabled": False,
                "error": str(e),
                "status": "error"
            }
    
    def get_recent_traces_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get summary of recent traces for admin dashboard"""
        try:
            # Note: This would ideally query OPIK API for actual trace data
            # For now, we provide a mock structure that shows what would be available
            
            summary = {
                "time_range_hours": hours,
                "total_traces": 0,
                "successful_traces": 0,
                "failed_traces": 0,
                "avg_processing_time_ms": 0.0,
                "total_cost_usd": 0.0,
                "top_operations": [],
                "error_rate": 0.0,
                "quality_scores": {
                    "avg_accuracy": 0.0,
                    "avg_confidence": 0.0,
                    "quality_threshold_breaches": 0
                },
                "performance_trends": {
                    "latency_trend": "stable",
                    "cost_trend": "stable",
                    "error_rate_trend": "stable"
                }
            }
            
            if not self.opik_manager.config.enabled:
                summary["message"] = "OPIK integration disabled"
                return summary
            
            # In a real implementation, this would query OPIK API
            # For demonstration, we return mock data structure
            summary.update({
                "message": "OPIK integration active (mock data)",
                "total_traces": 156,
                "successful_traces": 148,
                "failed_traces": 8,
                "avg_processing_time_ms": 1250.5,
                "total_cost_usd": 0.347,
                "error_rate": 5.1,
                "top_operations": [
                    {"name": "security_threat_analysis", "count": 89, "avg_time_ms": 1100},
                    {"name": "apt_attribution_analysis", "count": 34, "avg_time_ms": 1890},
                    {"name": "threat_intel_extraction", "count": 67, "avg_time_ms": 890}
                ],
                "quality_scores": {
                    "avg_accuracy": 0.847,
                    "avg_confidence": 0.782,
                    "quality_threshold_breaches": 3
                }
            })
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get traces summary: {e}")
            return {
                "error": str(e),
                "status": "error"
            }
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get system-level metrics for OPIK integration"""
        try:
            health_status = self.opik_manager.health_check()
            
            metrics = {
                "opik_integration": {
                    "status": "active" if health_status.get("initialized") else "inactive",
                    "project_name": health_status.get("project_name"),
                    "dashboard_url": health_status.get("dashboard_url"),
                    "dspy_auto_instrumentation": health_status.get("dspy_instrumented"),
                    "trace_sampling_rate": self.opik_manager.config.trace_sampling_rate,
                    "quality_threshold": self.opik_manager.config.quality_threshold
                },
                "configuration": {
                    "use_local": self.opik_manager.config.use_local,
                    "admin_ui_enabled": self.opik_manager.config.admin_ui_enabled,
                    "log_graph": self.opik_manager.config.log_graph,
                    "auto_instrument_dspy": self.opik_manager.config.auto_instrument_dspy
                },
                "runtime_info": {
                    "opik_available": OPIK_AVAILABLE,
                    "integration_enabled": self.opik_manager.config.enabled,
                    "session_start_time": self.opik_manager._session_start_time,
                    "uptime_seconds": time.time() - self.opik_manager._session_start_time
                }
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get system metrics: {e}")
            return {
                "error": str(e),
                "status": "error"
            }
    
    def get_trace_details(self, trace_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific trace"""
        try:
            # In a real implementation, this would query OPIK API for trace details
            # For now, return mock structure
            
            if not self.opik_manager.config.enabled:
                return {
                    "error": "OPIK integration disabled",
                    "trace_id": trace_id
                }
            
            # Mock trace details structure
            trace_details = {
                "trace_id": trace_id,
                "name": "comprehensive_threat_analysis",
                "status": "completed",
                "start_time": "2024-06-20T10:30:00Z",
                "end_time": "2024-06-20T10:30:02.5Z",
                "duration_ms": 2500,
                "input": {
                    "content_length": 1245,
                    "analysis_depth": "comprehensive"
                },
                "output": {
                    "threat_level": "medium",
                    "confidence": 0.87,
                    "ioc_count": 3
                },
                "spans": [
                    {
                        "span_id": f"{trace_id}_1",
                        "name": "threat_analysis_step",
                        "type": "processing",
                        "duration_ms": 1100,
                        "status": "completed"
                    },
                    {
                        "span_id": f"{trace_id}_2", 
                        "name": "apt_attribution_step",
                        "type": "processing",
                        "duration_ms": 890,
                        "status": "completed"
                    },
                    {
                        "span_id": f"{trace_id}_3",
                        "name": "intel_extraction_step",
                        "type": "processing", 
                        "duration_ms": 510,
                        "status": "completed"
                    }
                ],
                "metrics": {
                    "processing_time_ms": 2500,
                    "token_count": 1847,
                    "cost_usd": 0.0369,
                    "confidence_score": 0.87,
                    "ioc_count": 3
                },
                "tags": ["comprehensive_analysis", "pipeline", "orchestration"],
                "metadata": {
                    "module": "InstrumentedAdvancedThreatAnalyzer",
                    "pipeline_version": "v2.0"
                }
            }
            
            return trace_details
            
        except Exception as e:
            logger.error(f"Failed to get trace details for {trace_id}: {e}")
            return {
                "error": str(e),
                "trace_id": trace_id
            }
    
    def generate_health_report(self) -> Dict[str, Any]:
        """Generate comprehensive health report for OPIK integration"""
        try:
            dashboard_info = self.get_dashboard_info()
            system_metrics = self.get_system_metrics()
            traces_summary = self.get_recent_traces_summary(hours=1)  # Last hour
            
            # Determine overall health
            health_score = 0
            health_issues = []
            
            if dashboard_info.get("enabled"):
                health_score += 25
            else:
                health_issues.append("OPIK integration disabled")
            
            if dashboard_info.get("initialized"):
                health_score += 25
            else:
                health_issues.append("OPIK not properly initialized")
            
            if dashboard_info.get("dspy_instrumented"):
                health_score += 25
            else:
                health_issues.append("DSPy auto-instrumentation not active")
            
            if traces_summary.get("error_rate", 100) < 10:
                health_score += 25
            else:
                health_issues.append(f"High error rate: {traces_summary.get('error_rate', 0):.1f}%")
            
            health_status = "healthy" if health_score >= 75 else "degraded" if health_score >= 50 else "unhealthy"
            
            report = {
                "timestamp": datetime.utcnow().isoformat(),
                "overall_health": {
                    "status": health_status,
                    "score": health_score,
                    "issues": health_issues
                },
                "dashboard_info": dashboard_info,
                "system_metrics": system_metrics,
                "recent_activity": traces_summary,
                "recommendations": self._generate_recommendations(health_score, health_issues)
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate health report: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
                "overall_health": {
                    "status": "error",
                    "score": 0
                }
            }
    
    def _generate_recommendations(self, health_score: int, issues: List[str]) -> List[str]:
        """Generate recommendations based on health status"""
        recommendations = []
        
        if health_score < 75:
            if "OPIK integration disabled" in issues:
                recommendations.append("Enable OPIK integration by setting OPIK_ENABLED=true")
            
            if "OPIK not properly initialized" in issues:
                recommendations.append("Check OPIK configuration and API key settings")
            
            if "DSPy auto-instrumentation not active" in issues:
                recommendations.append("Verify DSPy modules are using OPIK callbacks")
            
            if any("error rate" in issue for issue in issues):
                recommendations.append("Investigate high error rates in trace logs")
        
        if health_score >= 75:
            recommendations.append("OPIK integration is healthy - consider expanding observability coverage")
        
        return recommendations

# HTML template for admin UI component
OPIK_ADMIN_HTML_TEMPLATE = """
<div id="opik-admin-panel" class="admin-panel">
    <h2>🔍 OPIK Observability</h2>
    
    <div class="status-grid">
        <div class="status-card" id="opik-status">
            <h3>Integration Status</h3>
            <div class="status-indicator" data-status="{{status}}">{{status}}</div>
            <div class="status-details">
                <p><strong>Project:</strong> {{project_name}}</p>
                <p><strong>Dashboard:</strong> 
                    {{#if dashboard_url}}
                        <a href="{{dashboard_url}}" target="_blank">Open Dashboard</a>
                    {{else}}
                        Not Available
                    {{/if}}
                </p>
            </div>
        </div>
        
        <div class="status-card" id="traces-summary">
            <h3>Recent Activity (24h)</h3>
            <div class="metrics-grid">
                <div class="metric">
                    <span class="metric-value">{{total_traces}}</span>
                    <span class="metric-label">Total Traces</span>
                </div>
                <div class="metric">
                    <span class="metric-value">{{error_rate}}%</span>
                    <span class="metric-label">Error Rate</span>
                </div>
                <div class="metric">
                    <span class="metric-value">${{total_cost_usd}}</span>
                    <span class="metric-label">Total Cost</span>
                </div>
            </div>
        </div>
        
        <div class="status-card" id="performance-metrics">
            <h3>Performance</h3>
            <div class="metrics-grid">
                <div class="metric">
                    <span class="metric-value">{{avg_processing_time_ms}}ms</span>
                    <span class="metric-label">Avg Processing Time</span>
                </div>
                <div class="metric">
                    <span class="metric-value">{{avg_accuracy}}</span>
                    <span class="metric-label">Avg Accuracy</span>
                </div>
                <div class="metric">
                    <span class="metric-value">{{quality_threshold_breaches}}</span>
                    <span class="metric-label">Quality Alerts</span>
                </div>
            </div>
        </div>
    </div>
    
    <div class="actions-section">
        <button onclick="refreshOpikData()" class="btn btn-primary">Refresh Data</button>
        <button onclick="openOpikDashboard()" class="btn btn-secondary">Open Dashboard</button>
        <button onclick="downloadHealthReport()" class="btn btn-info">Health Report</button>
    </div>
    
    <div class="recent-traces" id="recent-traces">
        <h3>Top Operations</h3>
        <table class="traces-table">
            <thead>
                <tr>
                    <th>Operation</th>
                    <th>Count</th>
                    <th>Avg Time (ms)</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                {{#each top_operations}}
                <tr>
                    <td>{{name}}</td>
                    <td>{{count}}</td>
                    <td>{{avg_time_ms}}</td>
                    <td>
                        <button onclick="viewTraces('{{name}}')" class="btn-sm">View</button>
                    </td>
                </tr>
                {{/each}}
            </tbody>
        </table>
    </div>
</div>

<style>
.admin-panel {
    padding: 20px;
    background: #f8f9fa;
    border-radius: 8px;
    margin: 20px 0;
}

.status-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 20px;
    margin: 20px 0;
}

.status-card {
    background: white;
    padding: 20px;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.status-indicator {
    padding: 8px 16px;
    border-radius: 4px;
    color: white;
    font-weight: bold;
    margin: 10px 0;
}

.status-indicator[data-status="healthy"] { background-color: #28a745; }
.status-indicator[data-status="degraded"] { background-color: #ffc107; }
.status-indicator[data-status="unhealthy"] { background-color: #dc3545; }
.status-indicator[data-status="error"] { background-color: #6c757d; }

.metrics-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
    gap: 15px;
    margin: 15px 0;
}

.metric {
    text-align: center;
}

.metric-value {
    display: block;
    font-size: 1.5em;
    font-weight: bold;
    color: #007acc;
}

.metric-label {
    display: block;
    font-size: 0.9em;
    color: #666;
    margin-top: 5px;
}

.traces-table {
    width: 100%;
    border-collapse: collapse;
    margin: 15px 0;
}

.traces-table th,
.traces-table td {
    padding: 10px;
    text-align: left;
    border-bottom: 1px solid #ddd;
}

.traces-table th {
    background-color: #f8f9fa;
    font-weight: 600;
}

.actions-section {
    margin: 20px 0;
    text-align: center;
}

.btn {
    padding: 10px 20px;
    margin: 0 5px;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-size: 14px;
}

.btn-primary { background-color: #007acc; color: white; }
.btn-secondary { background-color: #6c757d; color: white; }
.btn-info { background-color: #17a2b8; color: white; }
.btn-sm { padding: 5px 10px; font-size: 12px; }
</style>

<script>
function refreshOpikData() {
    // Refresh OPIK data from backend
    fetch('/admin/opik/refresh')
        .then(response => response.json())
        .then(data => {
            // Update UI with fresh data
            console.log('OPIK data refreshed:', data);
            location.reload(); // Simple refresh for now
        })
        .catch(error => console.error('Failed to refresh OPIK data:', error));
}

function openOpikDashboard() {
    const dashboardUrl = '{{dashboard_url}}';
    if (dashboardUrl) {
        window.open(dashboardUrl, '_blank');
    } else {
        alert('OPIK dashboard not available');
    }
}

function downloadHealthReport() {
    fetch('/admin/opik/health-report')
        .then(response => response.blob())
        .then(blob => {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'opik-health-report.json';
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        })
        .catch(error => console.error('Failed to download health report:', error));
}

function viewTraces(operationName) {
    // Navigate to traces view filtered by operation
    const dashboardUrl = '{{dashboard_url}}';
    if (dashboardUrl) {
        window.open(`${dashboardUrl}/traces?filter=${encodeURIComponent(operationName)}`, '_blank');
    } else {
        alert('OPIK dashboard not available');
    }
}
</script>
"""

# Global admin interface instance
_admin_interface: Optional[OpikAdminInterface] = None

def get_admin_interface() -> OpikAdminInterface:
    """Get or create the global admin interface instance"""
    global _admin_interface
    if _admin_interface is None:
        _admin_interface = OpikAdminInterface()
    return _admin_interface

# Export main classes and functions
__all__ = [
    "OpikAdminInterface",
    "get_admin_interface",
    "OPIK_ADMIN_HTML_TEMPLATE"
]