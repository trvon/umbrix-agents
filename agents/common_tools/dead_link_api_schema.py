"""
Dead Link Management API Schema

This module defines the expected API schema for dead link management
that should be implemented in the Rust/Axum backend.
"""

from typing import List, Dict, Any, Optional, Literal
from dataclasses import dataclass
from datetime import datetime


# Request/Response Models

@dataclass
class DeadLinkStatsResponse:
    """Response model for dead link statistics."""
    total_links: int
    by_status: Dict[str, int]  # {"healthy": 10, "suspicious": 5, "quarantined": 2, "dead": 1}
    health_distribution: Dict[str, int]  # {"healthy": 8, "degraded": 5, "poor": 3, "critical": 2}
    recent_failures: int
    scheduled_for_retry: int
    last_updated: str  # ISO timestamp
    system_health: Literal["healthy", "degraded", "critical"]


@dataclass
class DeadLinkInfo:
    """Individual dead link information."""
    url: str
    status: Literal["healthy", "suspicious", "quarantined", "dead", "manually_disabled", "manually_enabled"]
    first_seen: str  # ISO timestamp
    last_checked: str  # ISO timestamp
    last_success: Optional[str]  # ISO timestamp or null
    failure_count: int
    consecutive_failures: int
    success_count: int
    health_score: float  # 0.0 to 1.0
    next_check_time: Optional[str]  # ISO timestamp or null
    manual_override: Optional[bool]
    notes: str
    failure_history: List[Dict[str, Any]]  # Last 10 failures


@dataclass
class DeadLinkListResponse:
    """Response model for listing dead links."""
    links: List[DeadLinkInfo]
    total: int
    offset: int
    limit: int
    has_more: bool


@dataclass
class OverrideRequest:
    """Request model for manual override."""
    enabled: bool
    notes: str


@dataclass
class BulkOverrideRequest:
    """Request model for bulk override."""
    urls: List[str]
    enabled: bool
    notes: str


@dataclass
class BulkOverrideResponse:
    """Response model for bulk override."""
    results: Dict[str, bool]  # url -> success
    total: int
    successful: int
    failed: int


@dataclass
class TestUrlResponse:
    """Response model for URL connectivity test."""
    url: str
    status_code: Optional[int]
    response_time_ms: float
    success: bool
    error: Optional[str]
    timestamp: str  # ISO timestamp


@dataclass
class HealthTrendResponse:
    """Response model for health trend analysis."""
    trend_data: List[Dict[str, Any]]  # [{"date": "2025-06-09", "failures": 5, "recoveries": 2, "net_health": -3}]
    analysis_period_days: int


@dataclass
class DomainStatsResponse:
    """Response model for domain statistics."""
    domains: Dict[str, Dict[str, Any]]  # domain -> stats
    total_domains: int


@dataclass
class ExportResponse:
    """Response model for data export."""
    data: str  # CSV or JSON string
    format: Literal["json", "csv"]
    timestamp: str  # ISO timestamp


# API Endpoint Specifications

API_ENDPOINTS = {
    # Statistics and Overview
    "GET /api/admin/deadlinks/stats": {
        "description": "Get comprehensive dead link statistics",
        "response": DeadLinkStatsResponse,
        "auth_required": True
    },
    
    # List and Search
    "GET /api/admin/deadlinks/list": {
        "description": "List dead links with filtering and pagination",
        "query_params": {
            "status": "Optional[str] - Filter by status",
            "limit": "int = 100 - Number of results",
            "offset": "int = 0 - Pagination offset",
            "search": "Optional[str] - Search URLs by substring"
        },
        "response": DeadLinkListResponse,
        "auth_required": True
    },
    
    # Individual Link Management
    "GET /api/admin/deadlinks/{url}": {
        "description": "Get detailed information about a specific URL",
        "path_params": {
            "url": "str - URL-encoded URL to query"
        },
        "response": DeadLinkInfo,
        "auth_required": True
    },
    
    "POST /api/admin/deadlinks/{url}/override": {
        "description": "Manually override a URL's status",
        "path_params": {
            "url": "str - URL-encoded URL to override"
        },
        "request_body": OverrideRequest,
        "response": {"success": bool, "message": str},
        "auth_required": True
    },
    
    # Bulk Operations
    "POST /api/admin/deadlinks/bulk-override": {
        "description": "Apply manual override to multiple URLs",
        "request_body": BulkOverrideRequest,
        "response": BulkOverrideResponse,
        "auth_required": True
    },
    
    # Testing and Validation
    "POST /api/admin/deadlinks/test-url": {
        "description": "Test URL connectivity and record result",
        "request_body": {"url": str},
        "response": TestUrlResponse,
        "auth_required": True
    },
    
    # Analytics and Reporting
    "GET /api/admin/deadlinks/health-trend": {
        "description": "Get health trend analysis for dashboard",
        "query_params": {
            "days": "int = 7 - Number of days to analyze"
        },
        "response": HealthTrendResponse,
        "auth_required": True
    },
    
    "GET /api/admin/deadlinks/domain-stats": {
        "description": "Get statistics grouped by domain",
        "response": DomainStatsResponse,
        "auth_required": True
    },
    
    # Data Management
    "GET /api/admin/deadlinks/export": {
        "description": "Export dead link data",
        "query_params": {
            "format": "Literal['json', 'csv'] = 'json' - Export format",
            "status_filter": "Optional[str] - Filter by status"
        },
        "response": ExportResponse,
        "auth_required": True
    },
    
    "POST /api/admin/deadlinks/import": {
        "description": "Import dead link data (restore from backup)",
        "request_body": {"data": str, "format": Literal["json", "csv"]},
        "response": {"imported": int, "skipped": int, "errors": List[str]},
        "auth_required": True
    },
    
    # System Management
    "POST /api/admin/deadlinks/cleanup": {
        "description": "Clean up old dead link entries",
        "query_params": {
            "dry_run": "bool = true - Preview cleanup without executing"
        },
        "response": {"would_remove": int, "removed": int},
        "auth_required": True
    },
    
    "GET /api/admin/deadlinks/health-report": {
        "description": "Generate comprehensive health report",
        "response": {
            "summary": DeadLinkStatsResponse,
            "trends": Dict[str, Any],
            "recommendations": List[str],
            "generated_at": str
        },
        "auth_required": True
    }
}


# Database Schema Requirements

DATABASE_SCHEMA = {
    "dead_links": {
        "description": "Table for storing dead link information",
        "columns": {
            "id": "UUID PRIMARY KEY",
            "url": "TEXT NOT NULL UNIQUE",
            "status": "TEXT NOT NULL",  # enum: healthy, suspicious, quarantined, dead, manually_disabled, manually_enabled
            "first_seen": "TIMESTAMPTZ NOT NULL",
            "last_checked": "TIMESTAMPTZ NOT NULL",
            "last_success": "TIMESTAMPTZ",
            "failure_count": "INTEGER NOT NULL DEFAULT 0",
            "consecutive_failures": "INTEGER NOT NULL DEFAULT 0", 
            "success_count": "INTEGER NOT NULL DEFAULT 0",
            "health_score": "REAL NOT NULL DEFAULT 1.0",
            "next_check_time": "TIMESTAMPTZ",
            "manual_override": "BOOLEAN",
            "notes": "TEXT",
            "created_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
            "updated_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()"
        },
        "indexes": [
            "CREATE INDEX idx_dead_links_status ON dead_links(status)",
            "CREATE INDEX idx_dead_links_health_score ON dead_links(health_score)",
            "CREATE INDEX idx_dead_links_last_checked ON dead_links(last_checked)",
            "CREATE INDEX idx_dead_links_next_check_time ON dead_links(next_check_time)"
        ]
    },
    
    "dead_link_failures": {
        "description": "Table for storing failure history",
        "columns": {
            "id": "UUID PRIMARY KEY",
            "dead_link_id": "UUID NOT NULL REFERENCES dead_links(id) ON DELETE CASCADE",
            "timestamp": "TIMESTAMPTZ NOT NULL",
            "failure_type": "TEXT NOT NULL",  # enum: http_404, connection_timeout, etc.
            "status_code": "INTEGER",
            "error_message": "TEXT",
            "response_time_ms": "REAL",
            "created_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()"
        },
        "indexes": [
            "CREATE INDEX idx_dead_link_failures_dead_link_id ON dead_link_failures(dead_link_id)",
            "CREATE INDEX idx_dead_link_failures_timestamp ON dead_link_failures(timestamp)",
            "CREATE INDEX idx_dead_link_failures_failure_type ON dead_link_failures(failure_type)"
        ]
    },
    
    "dead_link_admin_actions": {
        "description": "Audit log for admin actions",
        "columns": {
            "id": "UUID PRIMARY KEY", 
            "user_id": "TEXT NOT NULL",
            "action": "TEXT NOT NULL",  # override, bulk_override, test_url, export, etc.
            "target_url": "TEXT",
            "details": "JSONB",  # Additional action details
            "timestamp": "TIMESTAMPTZ NOT NULL DEFAULT NOW()"
        },
        "indexes": [
            "CREATE INDEX idx_dead_link_admin_actions_user_id ON dead_link_admin_actions(user_id)",
            "CREATE INDEX idx_dead_link_admin_actions_timestamp ON dead_link_admin_actions(timestamp)",
            "CREATE INDEX idx_dead_link_admin_actions_action ON dead_link_admin_actions(action)"
        ]
    }
}


# Configuration Schema

CONFIGURATION_SCHEMA = {
    "dead_link_detection": {
        "enabled": {"type": "boolean", "default": True},
        "max_consecutive_failures": {"type": "integer", "default": 5, "min": 1, "max": 50},
        "failure_window_hours": {"type": "integer", "default": 24, "min": 1, "max": 168},
        "min_failures_for_suspicious": {"type": "integer", "default": 3, "min": 1, "max": 20},
        "min_failures_for_quarantine": {"type": "integer", "default": 5, "min": 1, "max": 30},
        "min_failures_for_dead": {"type": "integer", "default": 10, "min": 1, "max": 100},
        
        "retry_intervals": {
            "healthy_check_hours": {"type": "integer", "default": 24, "min": 1, "max": 168},
            "suspicious_check_hours": {"type": "integer", "default": 12, "min": 1, "max": 48},
            "quarantined_check_hours": {"type": "integer", "default": 168, "min": 24, "max": 720},
            "dead_check_hours": {"type": "integer", "default": 720, "min": 168, "max": 8760}
        },
        
        "health_scoring": {
            "success_increment": {"type": "number", "default": 0.1, "min": 0.01, "max": 1.0},
            "failure_decrement": {"type": "number", "default": 0.2, "min": 0.01, "max": 1.0},
            "permanent_failure_decrement": {"type": "number", "default": 0.5, "min": 0.1, "max": 1.0}
        },
        
        "cleanup": {
            "max_failure_history": {"type": "integer", "default": 50, "min": 10, "max": 200},
            "history_retention_days": {"type": "integer", "default": 90, "min": 7, "max": 365},
            "cleanup_interval_hours": {"type": "integer", "default": 24, "min": 1, "max": 168}
        }
    }
}


# Integration Requirements

INTEGRATION_REQUIREMENTS = {
    "authentication": {
        "description": "Admin endpoints require authentication",
        "implementation": "Use existing JWT/session auth from backend",
        "required_permissions": ["admin", "dead_link_manager"]
    },
    
    "rate_limiting": {
        "description": "Rate limiting for admin endpoints",
        "limits": {
            "/api/admin/deadlinks/test-url": "10 requests per minute",
            "/api/admin/deadlinks/bulk-override": "5 requests per minute", 
            "other_endpoints": "100 requests per minute"
        }
    },
    
    "caching": {
        "description": "Caching for expensive operations",
        "cache_keys": {
            "stats": "Cache for 1 minute",
            "domain_stats": "Cache for 5 minutes",
            "health_trend": "Cache for 10 minutes"
        }
    },
    
    "background_jobs": {
        "description": "Background tasks for dead link management",
        "jobs": [
            "dead_link_health_checker - Check quarantined/dead URLs on schedule",
            "dead_link_cleanup - Clean up old entries",
            "dead_link_stats_aggregator - Update cached statistics"
        ]
    },
    
    "metrics": {
        "description": "Prometheus metrics for monitoring",
        "metrics": [
            "dead_links_total{status} - Gauge of links by status",
            "dead_link_admin_actions_total{action,user} - Counter of admin actions",
            "dead_link_tests_total{result} - Counter of URL tests",
            "dead_link_api_duration_seconds{endpoint} - Histogram of API response times"
        ]
    }
}


if __name__ == "__main__":
    """Print API documentation."""
    import json
    
    print("Dead Link Management API Schema")
    print("=" * 50)
    print()
    
    print("API Endpoints:")
    for endpoint, spec in API_ENDPOINTS.items():
        print(f"\n{endpoint}")
        print(f"  Description: {spec['description']}")
        if 'query_params' in spec:
            print("  Query Parameters:")
            for param, desc in spec['query_params'].items():
                print(f"    {param}: {desc}")
        if 'request_body' in spec:
            print(f"  Request Body: {spec['request_body']}")
        print(f"  Response: {spec['response']}")
        print(f"  Auth Required: {spec['auth_required']}")
    
    print("\n\nDatabase Schema:")
    print(json.dumps(DATABASE_SCHEMA, indent=2))
    
    print("\n\nConfiguration Schema:")
    print(json.dumps(CONFIGURATION_SCHEMA, indent=2))