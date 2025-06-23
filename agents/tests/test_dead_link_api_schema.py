"""
Test coverage for common_tools/dead_link_api_schema.py

This module provides comprehensive test coverage for the dead link API schema
definitions, ensuring all dataclasses and configurations are valid.
"""

import pytest
import json
from datetime import datetime, timezone
from typing import Dict, Any, List

from common_tools.dead_link_api_schema import (
    DeadLinkStatsResponse,
    DeadLinkInfo,
    DeadLinkListResponse,
    OverrideRequest,
    BulkOverrideRequest,
    BulkOverrideResponse,
    TestUrlResponse,
    HealthTrendResponse,
    DomainStatsResponse,
    ExportResponse,
    API_ENDPOINTS,
    DATABASE_SCHEMA,
    CONFIGURATION_SCHEMA,
    INTEGRATION_REQUIREMENTS
)


class TestDeadLinkStatsResponse:
    """Test the DeadLinkStatsResponse dataclass."""
    
    def test_stats_response_creation(self):
        """Test creating a stats response with all fields."""
        stats = DeadLinkStatsResponse(
            total_links=100,
            by_status={"healthy": 80, "suspicious": 10, "quarantined": 7, "dead": 3},
            health_distribution={"healthy": 85, "degraded": 10, "poor": 3, "critical": 2},
            recent_failures=15,
            scheduled_for_retry=5,
            last_updated="2025-06-21T10:00:00Z",
            system_health="healthy"
        )
        
        assert stats.total_links == 100
        assert stats.by_status["healthy"] == 80
        assert stats.by_status["dead"] == 3
        assert stats.health_distribution["healthy"] == 85
        assert stats.recent_failures == 15
        assert stats.scheduled_for_retry == 5
        assert stats.last_updated == "2025-06-21T10:00:00Z"
        assert stats.system_health == "healthy"

    def test_stats_response_degraded_system(self):
        """Test stats response for degraded system."""
        stats = DeadLinkStatsResponse(
            total_links=50,
            by_status={"healthy": 30, "suspicious": 15, "quarantined": 3, "dead": 2},
            health_distribution={"healthy": 30, "degraded": 15, "poor": 3, "critical": 2},
            recent_failures=20,
            scheduled_for_retry=8,
            last_updated="2025-06-21T10:00:00Z",
            system_health="degraded"
        )
        
        assert stats.system_health == "degraded"
        assert stats.recent_failures == 20
        assert stats.scheduled_for_retry == 8


class TestDeadLinkInfo:
    """Test the DeadLinkInfo dataclass."""
    
    def test_dead_link_info_healthy(self):
        """Test creating healthy link info."""
        link_info = DeadLinkInfo(
            url="https://example.com/article",
            status="healthy",
            first_seen="2025-01-01T00:00:00Z",
            last_checked="2025-06-21T10:00:00Z",
            last_success="2025-06-21T10:00:00Z",
            failure_count=0,
            consecutive_failures=0,
            success_count=100,
            health_score=1.0,
            next_check_time="2025-06-22T10:00:00Z",
            manual_override=None,
            notes="",
            failure_history=[]
        )
        
        assert link_info.url == "https://example.com/article"
        assert link_info.status == "healthy"
        assert link_info.failure_count == 0
        assert link_info.consecutive_failures == 0
        assert link_info.success_count == 100
        assert link_info.health_score == 1.0
        assert link_info.manual_override is None
        assert link_info.failure_history == []

    def test_dead_link_info_dead(self):
        """Test creating dead link info."""
        failure_history = [
            {"timestamp": "2025-06-21T09:00:00Z", "error": "404 Not Found"},
            {"timestamp": "2025-06-21T08:00:00Z", "error": "404 Not Found"}
        ]
        
        link_info = DeadLinkInfo(
            url="https://deadsite.com/gone",
            status="dead",
            first_seen="2025-01-01T00:00:00Z",
            last_checked="2025-06-21T10:00:00Z",
            last_success="2025-01-15T00:00:00Z",
            failure_count=25,
            consecutive_failures=10,
            success_count=5,
            health_score=0.1,
            next_check_time="2025-06-28T10:00:00Z",
            manual_override=False,
            notes="Site appears to be permanently down",
            failure_history=failure_history
        )
        
        assert link_info.status == "dead"
        assert link_info.failure_count == 25
        assert link_info.consecutive_failures == 10
        assert link_info.health_score == 0.1
        assert link_info.manual_override is False
        assert len(link_info.failure_history) == 2
        assert link_info.notes == "Site appears to be permanently down"

    def test_dead_link_info_manually_disabled(self):
        """Test creating manually disabled link info."""
        link_info = DeadLinkInfo(
            url="https://suspicious.com/malware",
            status="manually_disabled",
            first_seen="2025-06-01T00:00:00Z",
            last_checked="2025-06-21T10:00:00Z",
            last_success="2025-06-15T00:00:00Z",
            failure_count=5,
            consecutive_failures=2,
            success_count=20,
            health_score=0.7,
            next_check_time=None,
            manual_override=True,
            notes="Disabled due to suspicious content",
            failure_history=[]
        )
        
        assert link_info.status == "manually_disabled"
        assert link_info.manual_override is True
        assert link_info.next_check_time is None
        assert "suspicious content" in link_info.notes


class TestDeadLinkListResponse:
    """Test the DeadLinkListResponse dataclass."""
    
    def test_list_response_with_data(self):
        """Test list response with link data."""
        links = [
            DeadLinkInfo(
                url="https://example1.com",
                status="healthy",
                first_seen="2025-01-01T00:00:00Z",
                last_checked="2025-06-21T10:00:00Z",
                last_success="2025-06-21T10:00:00Z",
                failure_count=0,
                consecutive_failures=0,
                success_count=50,
                health_score=1.0,
                next_check_time="2025-06-22T10:00:00Z",
                manual_override=None,
                notes="",
                failure_history=[]
            ),
            DeadLinkInfo(
                url="https://example2.com",
                status="suspicious",
                first_seen="2025-01-01T00:00:00Z",
                last_checked="2025-06-21T10:00:00Z",
                last_success="2025-06-20T10:00:00Z",
                failure_count=3,
                consecutive_failures=2,
                success_count=47,
                health_score=0.8,
                next_check_time="2025-06-21T22:00:00Z",
                manual_override=None,
                notes="",
                failure_history=[]
            )
        ]
        
        response = DeadLinkListResponse(
            links=links,
            total=150,
            offset=0,
            limit=50,
            has_more=True
        )
        
        assert len(response.links) == 2
        assert response.total == 150
        assert response.offset == 0
        assert response.limit == 50
        assert response.has_more is True
        assert response.links[0].status == "healthy"
        assert response.links[1].status == "suspicious"

    def test_list_response_empty(self):
        """Test empty list response."""
        response = DeadLinkListResponse(
            links=[],
            total=0,
            offset=0,
            limit=50,
            has_more=False
        )
        
        assert len(response.links) == 0
        assert response.total == 0
        assert response.has_more is False


class TestOverrideRequest:
    """Test the OverrideRequest dataclass."""
    
    def test_enable_override_request(self):
        """Test creating enable override request."""
        request = OverrideRequest(
            enabled=True,
            notes="Manually enabling this link after verification"
        )
        
        assert request.enabled is True
        assert "verification" in request.notes

    def test_disable_override_request(self):
        """Test creating disable override request."""
        request = OverrideRequest(
            enabled=False,
            notes="Disabling due to security concerns"
        )
        
        assert request.enabled is False
        assert "security concerns" in request.notes


class TestBulkOverrideRequest:
    """Test the BulkOverrideRequest dataclass."""
    
    def test_bulk_override_request(self):
        """Test creating bulk override request."""
        urls = [
            "https://example1.com/article1",
            "https://example2.com/article2",
            "https://example3.com/article3"
        ]
        
        request = BulkOverrideRequest(
            urls=urls,
            enabled=False,
            notes="Bulk disable for maintenance"
        )
        
        assert len(request.urls) == 3
        assert request.enabled is False
        assert "maintenance" in request.notes
        assert "https://example1.com/article1" in request.urls


class TestBulkOverrideResponse:
    """Test the BulkOverrideResponse dataclass."""
    
    def test_bulk_override_response_mixed_results(self):
        """Test bulk override response with mixed results."""
        results = {
            "https://example1.com": True,
            "https://example2.com": True,
            "https://invalid-url.com": False
        }
        
        response = BulkOverrideResponse(
            results=results,
            total=3,
            successful=2,
            failed=1
        )
        
        assert len(response.results) == 3
        assert response.total == 3
        assert response.successful == 2
        assert response.failed == 1
        assert response.results["https://example1.com"] is True
        assert response.results["https://invalid-url.com"] is False

    def test_bulk_override_response_all_success(self):
        """Test bulk override response with all successful."""
        results = {
            "https://example1.com": True,
            "https://example2.com": True
        }
        
        response = BulkOverrideResponse(
            results=results,
            total=2,
            successful=2,
            failed=0
        )
        
        assert response.total == response.successful
        assert response.failed == 0


class TestTestUrlResponse:
    """Test the TestUrlResponse dataclass."""
    
    def test_successful_url_test(self):
        """Test successful URL test response."""
        response = TestUrlResponse(
            url="https://example.com",
            status_code=200,
            response_time_ms=150.5,
            success=True,
            error=None,
            timestamp="2025-06-21T10:00:00Z"
        )
        
        assert response.url == "https://example.com"
        assert response.status_code == 200
        assert response.response_time_ms == 150.5
        assert response.success is True
        assert response.error is None
        assert response.timestamp == "2025-06-21T10:00:00Z"

    def test_failed_url_test(self):
        """Test failed URL test response."""
        response = TestUrlResponse(
            url="https://deadsite.com",
            status_code=None,
            response_time_ms=5000.0,
            success=False,
            error="Connection timeout",
            timestamp="2025-06-21T10:00:00Z"
        )
        
        assert response.success is False
        assert response.status_code is None
        assert response.error == "Connection timeout"
        assert response.response_time_ms == 5000.0

    def test_http_error_url_test(self):
        """Test URL test with HTTP error."""
        response = TestUrlResponse(
            url="https://example.com/not-found",
            status_code=404,
            response_time_ms=200.0,
            success=False,
            error="404 Not Found",
            timestamp="2025-06-21T10:00:00Z"
        )
        
        assert response.status_code == 404
        assert response.success is False
        assert "404" in response.error


class TestHealthTrendResponse:
    """Test the HealthTrendResponse dataclass."""
    
    def test_health_trend_response(self):
        """Test health trend response."""
        trend_data = [
            {"date": "2025-06-21", "failures": 5, "recoveries": 2, "net_health": -3},
            {"date": "2025-06-20", "failures": 3, "recoveries": 4, "net_health": 1},
            {"date": "2025-06-19", "failures": 8, "recoveries": 1, "net_health": -7}
        ]
        
        response = HealthTrendResponse(
            trend_data=trend_data,
            analysis_period_days=7
        )
        
        assert len(response.trend_data) == 3
        assert response.analysis_period_days == 7
        assert response.trend_data[0]["net_health"] == -3
        assert response.trend_data[1]["net_health"] == 1
        assert response.trend_data[2]["net_health"] == -7


class TestDomainStatsResponse:
    """Test the DomainStatsResponse dataclass."""
    
    def test_domain_stats_response(self):
        """Test domain statistics response."""
        domains = {
            "example.com": {
                "total_links": 50,
                "healthy": 45,
                "suspicious": 3,
                "dead": 2,
                "avg_health_score": 0.9
            },
            "testsite.org": {
                "total_links": 20,
                "healthy": 15,
                "suspicious": 3,
                "dead": 2,
                "avg_health_score": 0.75
            }
        }
        
        response = DomainStatsResponse(
            domains=domains,
            total_domains=2
        )
        
        assert len(response.domains) == 2
        assert response.total_domains == 2
        assert response.domains["example.com"]["total_links"] == 50
        assert response.domains["example.com"]["avg_health_score"] == 0.9
        assert response.domains["testsite.org"]["total_links"] == 20


class TestExportResponse:
    """Test the ExportResponse dataclass."""
    
    def test_json_export_response(self):
        """Test JSON export response."""
        data = json.dumps([
            {"url": "https://example.com", "status": "healthy", "health_score": 1.0},
            {"url": "https://deadsite.com", "status": "dead", "health_score": 0.1}
        ])
        
        response = ExportResponse(
            data=data,
            format="json",
            timestamp="2025-06-21T10:00:00Z"
        )
        
        assert response.format == "json"
        assert response.timestamp == "2025-06-21T10:00:00Z"
        
        # Verify data can be parsed back to JSON
        parsed_data = json.loads(response.data)
        assert len(parsed_data) == 2
        assert parsed_data[0]["status"] == "healthy"
        assert parsed_data[1]["status"] == "dead"

    def test_csv_export_response(self):
        """Test CSV export response."""
        csv_data = "url,status,health_score\nhttps://example.com,healthy,1.0\nhttps://deadsite.com,dead,0.1"
        
        response = ExportResponse(
            data=csv_data,
            format="csv",
            timestamp="2025-06-21T10:00:00Z"
        )
        
        assert response.format == "csv"
        assert "url,status,health_score" in response.data
        assert "https://example.com,healthy,1.0" in response.data


class TestAPIEndpoints:
    """Test the API_ENDPOINTS configuration."""
    
    def test_api_endpoints_structure(self):
        """Test that API endpoints have required structure."""
        assert isinstance(API_ENDPOINTS, dict)
        assert len(API_ENDPOINTS) > 0
        
        # Test a few key endpoints
        assert "GET /api/admin/deadlinks/stats" in API_ENDPOINTS
        assert "POST /api/admin/deadlinks/bulk-override" in API_ENDPOINTS
        assert "GET /api/admin/deadlinks/list" in API_ENDPOINTS

    def test_endpoint_specifications(self):
        """Test that endpoints have required specifications."""
        for endpoint, spec in API_ENDPOINTS.items():
            # All endpoints should have these fields
            assert "description" in spec
            assert "auth_required" in spec
            assert isinstance(spec["description"], str)
            assert isinstance(spec["auth_required"], bool)
            
            # Description should not be empty
            assert len(spec["description"]) > 0
            
            # Auth should be required for admin endpoints
            if "/api/admin/" in endpoint:
                assert spec["auth_required"] is True

    def test_stats_endpoint_spec(self):
        """Test the stats endpoint specification."""
        endpoint = "GET /api/admin/deadlinks/stats"
        spec = API_ENDPOINTS[endpoint]
        
        assert spec["description"] == "Get comprehensive dead link statistics"
        assert spec["response"] == DeadLinkStatsResponse
        assert spec["auth_required"] is True

    def test_list_endpoint_spec(self):
        """Test the list endpoint specification."""
        endpoint = "GET /api/admin/deadlinks/list"
        spec = API_ENDPOINTS[endpoint]
        
        assert "query_params" in spec
        assert "status" in spec["query_params"]
        assert "limit" in spec["query_params"]
        assert "offset" in spec["query_params"]
        assert "search" in spec["query_params"]
        assert spec["response"] == DeadLinkListResponse

    def test_bulk_override_endpoint_spec(self):
        """Test the bulk override endpoint specification."""
        endpoint = "POST /api/admin/deadlinks/bulk-override"
        spec = API_ENDPOINTS[endpoint]
        
        assert spec["request_body"] == BulkOverrideRequest
        assert spec["response"] == BulkOverrideResponse
        assert "multiple" in spec["description"].lower()


class TestDatabaseSchema:
    """Test the DATABASE_SCHEMA configuration."""
    
    def test_database_schema_structure(self):
        """Test database schema structure."""
        assert isinstance(DATABASE_SCHEMA, dict)
        assert "dead_links" in DATABASE_SCHEMA
        assert "dead_link_failures" in DATABASE_SCHEMA
        assert "dead_link_admin_actions" in DATABASE_SCHEMA

    def test_dead_links_table_schema(self):
        """Test dead_links table schema."""
        table = DATABASE_SCHEMA["dead_links"]
        
        assert "description" in table
        assert "columns" in table
        assert "indexes" in table
        
        columns = table["columns"]
        assert "id" in columns
        assert "url" in columns
        assert "status" in columns
        assert "health_score" in columns
        assert "created_at" in columns
        assert "updated_at" in columns
        
        # Check primary key
        assert "PRIMARY KEY" in columns["id"]
        assert "UNIQUE" in columns["url"]

    def test_dead_link_failures_table_schema(self):
        """Test dead_link_failures table schema."""
        table = DATABASE_SCHEMA["dead_link_failures"]
        
        columns = table["columns"]
        assert "id" in columns
        assert "dead_link_id" in columns
        assert "timestamp" in columns
        assert "failure_type" in columns
        assert "status_code" in columns
        
        # Check foreign key
        assert "REFERENCES dead_links(id)" in columns["dead_link_id"]

    def test_database_indexes(self):
        """Test database indexes are properly defined."""
        for table_name, table_schema in DATABASE_SCHEMA.items():
            assert "indexes" in table_schema
            indexes = table_schema["indexes"]
            assert isinstance(indexes, list)
            
            for index in indexes:
                assert "CREATE INDEX" in index
                assert table_name.replace("_", "") in index or table_name in index


class TestConfigurationSchema:
    """Test the CONFIGURATION_SCHEMA."""
    
    def test_configuration_schema_structure(self):
        """Test configuration schema structure."""
        assert isinstance(CONFIGURATION_SCHEMA, dict)
        assert "dead_link_detection" in CONFIGURATION_SCHEMA

    def test_dead_link_detection_config(self):
        """Test dead link detection configuration."""
        config = CONFIGURATION_SCHEMA["dead_link_detection"]
        
        # Main settings
        assert "enabled" in config
        assert "max_consecutive_failures" in config
        assert "failure_window_hours" in config
        
        # Thresholds
        assert "min_failures_for_suspicious" in config
        assert "min_failures_for_quarantine" in config
        assert "min_failures_for_dead" in config
        
        # Sub-sections
        assert "retry_intervals" in config
        assert "health_scoring" in config
        assert "cleanup" in config

    def test_retry_intervals_config(self):
        """Test retry intervals configuration."""
        retry_config = CONFIGURATION_SCHEMA["dead_link_detection"]["retry_intervals"]
        
        assert "healthy_check_hours" in retry_config
        assert "suspicious_check_hours" in retry_config
        assert "quarantined_check_hours" in retry_config
        assert "dead_check_hours" in retry_config
        
        # Check that each has proper constraints
        for key, value in retry_config.items():
            assert "type" in value
            assert "default" in value
            assert "min" in value
            assert "max" in value
            assert value["type"] == "integer"

    def test_health_scoring_config(self):
        """Test health scoring configuration."""
        health_config = CONFIGURATION_SCHEMA["dead_link_detection"]["health_scoring"]
        
        assert "success_increment" in health_config
        assert "failure_decrement" in health_config
        assert "permanent_failure_decrement" in health_config
        
        for key, value in health_config.items():
            assert value["type"] == "number"
            assert 0 < value["default"] <= 1.0
            assert 0 < value["min"] <= 1.0
            assert 0 < value["max"] <= 1.0

    def test_cleanup_config(self):
        """Test cleanup configuration."""
        cleanup_config = CONFIGURATION_SCHEMA["dead_link_detection"]["cleanup"]
        
        assert "max_failure_history" in cleanup_config
        assert "history_retention_days" in cleanup_config
        assert "cleanup_interval_hours" in cleanup_config
        
        # Check reasonable defaults
        assert cleanup_config["max_failure_history"]["default"] >= 10
        assert cleanup_config["history_retention_days"]["default"] >= 7
        assert cleanup_config["cleanup_interval_hours"]["default"] >= 1


class TestIntegrationRequirements:
    """Test the INTEGRATION_REQUIREMENTS configuration."""
    
    def test_integration_requirements_structure(self):
        """Test integration requirements structure."""
        assert isinstance(INTEGRATION_REQUIREMENTS, dict)
        assert "authentication" in INTEGRATION_REQUIREMENTS
        assert "rate_limiting" in INTEGRATION_REQUIREMENTS
        assert "caching" in INTEGRATION_REQUIREMENTS
        assert "background_jobs" in INTEGRATION_REQUIREMENTS
        assert "metrics" in INTEGRATION_REQUIREMENTS

    def test_authentication_requirements(self):
        """Test authentication requirements."""
        auth = INTEGRATION_REQUIREMENTS["authentication"]
        
        assert "description" in auth
        assert "implementation" in auth
        assert "required_permissions" in auth
        
        assert isinstance(auth["required_permissions"], list)
        assert "admin" in auth["required_permissions"]

    def test_rate_limiting_requirements(self):
        """Test rate limiting requirements."""
        rate_limit = INTEGRATION_REQUIREMENTS["rate_limiting"]
        
        assert "description" in rate_limit
        assert "limits" in rate_limit
        
        limits = rate_limit["limits"]
        assert "/api/admin/deadlinks/test-url" in limits
        assert "/api/admin/deadlinks/bulk-override" in limits
        assert "other_endpoints" in limits

    def test_caching_requirements(self):
        """Test caching requirements."""
        caching = INTEGRATION_REQUIREMENTS["caching"]
        
        assert "cache_keys" in caching
        cache_keys = caching["cache_keys"]
        
        assert "stats" in cache_keys
        assert "domain_stats" in cache_keys
        assert "health_trend" in cache_keys

    def test_background_jobs_requirements(self):
        """Test background jobs requirements."""
        jobs = INTEGRATION_REQUIREMENTS["background_jobs"]
        
        assert "jobs" in jobs
        job_list = jobs["jobs"]
        
        assert isinstance(job_list, list)
        assert len(job_list) >= 3
        
        # Check for expected job types
        job_descriptions = " ".join(job_list)
        assert "health_checker" in job_descriptions
        assert "cleanup" in job_descriptions
        assert "stats_aggregator" in job_descriptions

    def test_metrics_requirements(self):
        """Test metrics requirements."""
        metrics = INTEGRATION_REQUIREMENTS["metrics"]
        
        assert "metrics" in metrics
        metrics_list = metrics["metrics"]
        
        assert isinstance(metrics_list, list)
        assert len(metrics_list) >= 4
        
        # Check for expected metric types
        metrics_str = " ".join(metrics_list)
        assert "dead_links_total" in metrics_str
        assert "admin_actions_total" in metrics_str
        assert "tests_total" in metrics_str
        assert "api_duration_seconds" in metrics_str


class TestMainFunction:
    """Test the __main__ function behavior."""
    
    def test_main_function_execution(self, capsys):
        """Test that the main function executes without errors."""
        # Test the main execution by importing as a module and calling the main logic
        import subprocess
        import sys
        
        result = subprocess.run(
            [sys.executable, "-c", "import common_tools.dead_link_api_schema; exec(open('common_tools/dead_link_api_schema.py').read())"],
            capture_output=True,
            text=True,
            cwd="/Volumes/picaso/work/tools/umbrix/agents"
        )
        
        assert result.returncode == 0
        assert "Dead Link Management API Schema" in result.stdout
        assert "API Endpoints:" in result.stdout


class TestDataclassCompatibility:
    """Test dataclass compatibility and serialization."""
    
    def test_dataclass_serialization(self):
        """Test that dataclasses can be converted to dictionaries."""
        stats = DeadLinkStatsResponse(
            total_links=100,
            by_status={"healthy": 90, "dead": 10},
            health_distribution={"healthy": 95, "poor": 5},
            recent_failures=5,
            scheduled_for_retry=2,
            last_updated="2025-06-21T10:00:00Z",
            system_health="healthy"
        )
        
        # Convert to dictionary (should work for JSON serialization)
        stats_dict = stats.__dict__
        assert isinstance(stats_dict, dict)
        assert stats_dict["total_links"] == 100
        assert stats_dict["system_health"] == "healthy"

    def test_nested_dataclass_handling(self):
        """Test handling of nested complex data structures."""
        failure_history = [
            {"timestamp": "2025-06-21T09:00:00Z", "error": "Connection timeout"},
            {"timestamp": "2025-06-21T08:00:00Z", "error": "404 Not Found"}
        ]
        
        link_info = DeadLinkInfo(
            url="https://example.com",
            status="suspicious",
            first_seen="2025-01-01T00:00:00Z",
            last_checked="2025-06-21T10:00:00Z",
            last_success="2025-06-20T10:00:00Z",
            failure_count=2,
            consecutive_failures=2,
            success_count=48,
            health_score=0.8,
            next_check_time="2025-06-21T22:00:00Z",
            manual_override=None,
            notes="Monitoring for pattern",
            failure_history=failure_history
        )
        
        # Should handle complex nested structures
        assert len(link_info.failure_history) == 2
        assert link_info.failure_history[0]["error"] == "Connection timeout"
        assert isinstance(link_info.failure_history, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])