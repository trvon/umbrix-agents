"""
Basic tests for health.py to achieve 50% coverage.

Tests focus on critical paths and core functionality:
- HealthChecker basic operations
- Health report generation and serialization
- Core readiness check functionality
- Configuration health checks
- Basic convenience functions
"""

import pytest
import asyncio
from datetime import datetime, timezone
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any

from common_tools.health import (
    HealthStatus, HealthCheck, HealthReport, HealthChecker,
    check_config_loaded, create_basic_health_checker, create_kafka_health_checker
)


class TestHealthDataStructures:
    """Test health check data structures."""
    
    def test_health_status_enum(self):
        """Test HealthStatus enum values."""
        assert HealthStatus.HEALTHY == "healthy"
        assert HealthStatus.UNHEALTHY == "unhealthy"
        assert HealthStatus.DEGRADED == "degraded"
    
    def test_health_check_creation(self):
        """Test HealthCheck dataclass creation."""
        timestamp = datetime.now(timezone.utc)
        check = HealthCheck(
            name="test_check",
            status=HealthStatus.HEALTHY,
            message="Test passed",
            response_time_ms=10.5,
            timestamp=timestamp,
            details={"key": "value"}
        )
        
        assert check.name == "test_check"
        assert check.status == HealthStatus.HEALTHY
        assert check.message == "Test passed"
        assert check.response_time_ms == 10.5
        assert check.timestamp == timestamp
        assert check.details == {"key": "value"}
    
    def test_health_report_to_dict(self):
        """Test HealthReport to_dict conversion."""
        timestamp = datetime.now(timezone.utc)
        check = HealthCheck(
            name="test_check",
            status=HealthStatus.HEALTHY,
            message="All good",
            response_time_ms=5.0,
            timestamp=timestamp
        )
        
        report = HealthReport(
            status=HealthStatus.HEALTHY,
            checks=[check],
            overall_response_time_ms=15.0,
            timestamp=timestamp
        )
        
        result = report.to_dict()
        
        assert result["status"] == "healthy"
        assert len(result["checks"]) == 1
        assert result["checks"][0]["name"] == "test_check"
        assert result["checks"][0]["status"] == "healthy"
        assert result["overall_response_time_ms"] == 15.0
        assert "timestamp" in result
        assert "timestamp" in result["checks"][0]


class TestHealthChecker:
    """Test HealthChecker core functionality."""
    
    def test_health_checker_initialization(self):
        """Test HealthChecker initialization."""
        checker = HealthChecker("test_agent")
        
        assert checker.agent_name == "test_agent"
        assert checker.readiness_checks == {}
        assert checker.logger is not None
    
    def test_add_readiness_check(self):
        """Test adding readiness checks."""
        checker = HealthChecker("test_agent")
        
        def test_check():
            return True, "Check passed"
        
        checker.add_readiness_check("test_check", test_check)
        
        assert "test_check" in checker.readiness_checks
        assert checker.readiness_checks["test_check"] == test_check
    
    @pytest.mark.asyncio
    async def test_check_liveness(self):
        """Test liveness check."""
        checker = HealthChecker("test_agent")
        
        report = await checker.check_liveness()
        
        assert report.status == HealthStatus.HEALTHY
        assert len(report.checks) == 1
        assert report.checks[0].name == "liveness"
        assert report.checks[0].status == HealthStatus.HEALTHY
        assert report.checks[0].message == "Agent process is running"
        assert report.overall_response_time_ms >= 0
    
    @pytest.mark.asyncio
    async def test_check_readiness_no_checks(self):
        """Test readiness check with no registered checks."""
        checker = HealthChecker("test_agent")
        
        report = await checker.check_readiness()
        
        assert report.status == HealthStatus.HEALTHY
        assert len(report.checks) == 1
        assert report.checks[0].name == "no_dependencies"
        assert report.checks[0].status == HealthStatus.HEALTHY
        assert report.checks[0].message == "No dependency checks configured"
    
    @pytest.mark.asyncio
    async def test_check_readiness_with_passing_sync_check(self):
        """Test readiness check with passing synchronous check."""
        checker = HealthChecker("test_agent")
        
        def passing_check():
            return (True, "Sync check passed", {"detail": "value"})
        
        checker.add_readiness_check("sync_check", passing_check)
        
        report = await checker.check_readiness()
        
        assert report.status == HealthStatus.HEALTHY
        assert len(report.checks) == 1
        assert report.checks[0].name == "sync_check"
        assert report.checks[0].status == HealthStatus.HEALTHY
        assert report.checks[0].message == "Sync check passed"
        assert report.checks[0].details == {"detail": "value"}
    
    @pytest.mark.asyncio
    async def test_check_readiness_with_passing_async_check(self):
        """Test readiness check with passing asynchronous check."""
        checker = HealthChecker("test_agent")
        
        async def async_passing_check():
            return (True, "Async check passed")
        
        checker.add_readiness_check("async_check", async_passing_check)
        
        report = await checker.check_readiness()
        
        assert report.status == HealthStatus.HEALTHY
        assert len(report.checks) == 1
        assert report.checks[0].name == "async_check"
        assert report.checks[0].status == HealthStatus.HEALTHY
        assert report.checks[0].message == "Async check passed"
    
    @pytest.mark.asyncio
    async def test_check_readiness_with_failing_check(self):
        """Test readiness check with failing check."""
        checker = HealthChecker("test_agent")
        
        def failing_check():
            return (False, "Check failed", {"error": "timeout"})
        
        checker.add_readiness_check("failing_check", failing_check)
        
        report = await checker.check_readiness()
        
        assert report.status == HealthStatus.UNHEALTHY
        assert len(report.checks) == 1
        assert report.checks[0].name == "failing_check"
        assert report.checks[0].status == HealthStatus.UNHEALTHY
        assert report.checks[0].message == "Check failed"
        assert report.checks[0].details == {"error": "timeout"}
    
    @pytest.mark.asyncio
    async def test_check_readiness_with_exception(self):
        """Test readiness check when check function raises exception."""
        checker = HealthChecker("test_agent")
        
        def exception_check():
            raise ValueError("Test exception")
        
        checker.add_readiness_check("exception_check", exception_check)
        
        report = await checker.check_readiness()
        
        assert report.status == HealthStatus.UNHEALTHY
        assert len(report.checks) == 1
        assert report.checks[0].name == "exception_check"
        assert report.checks[0].status == HealthStatus.UNHEALTHY
        assert "Check failed: Test exception" in report.checks[0].message
        assert report.checks[0].details["error_type"] == "ValueError"
    
    @pytest.mark.asyncio
    async def test_check_readiness_boolean_return(self):
        """Test readiness check with simple boolean return."""
        checker = HealthChecker("test_agent")
        
        def simple_check():
            return True
        
        checker.add_readiness_check("simple_check", simple_check)
        
        report = await checker.check_readiness()
        
        assert report.status == HealthStatus.HEALTHY
        assert len(report.checks) == 1
        assert report.checks[0].name == "simple_check"
        assert report.checks[0].status == HealthStatus.HEALTHY
        assert report.checks[0].message == "Check completed"
        assert report.checks[0].details is None
    
    @pytest.mark.asyncio
    async def test_check_readiness_mixed_results(self):
        """Test readiness check with both passing and failing checks."""
        checker = HealthChecker("test_agent")
        
        def passing_check():
            return (True, "Passed")
        
        def failing_check():
            return (False, "Failed")
        
        checker.add_readiness_check("pass", passing_check)
        checker.add_readiness_check("fail", failing_check)
        
        report = await checker.check_readiness()
        
        # Overall status should be unhealthy if any check fails
        assert report.status == HealthStatus.UNHEALTHY
        assert len(report.checks) == 2
        
        # Find the checks (order may vary)
        pass_check = next(c for c in report.checks if c.name == "pass")
        fail_check = next(c for c in report.checks if c.name == "fail")
        
        assert pass_check.status == HealthStatus.HEALTHY
        assert fail_check.status == HealthStatus.UNHEALTHY


class TestConfigHealthCheck:
    """Test configuration health check function."""
    
    def test_check_config_loaded_empty_config(self):
        """Test config check with empty configuration."""
        result = check_config_loaded({})
        
        is_healthy, message, details = result
        assert is_healthy is False
        assert "Configuration is empty" in message
        assert details["config_keys"] == 0
    
    def test_check_config_loaded_no_required_keys(self):
        """Test config check with no required keys."""
        config = {"key1": "value1", "key2": "value2"}
        result = check_config_loaded(config)
        
        is_healthy, message, details = result
        assert is_healthy is True
        assert "Configuration loaded successfully (2 keys)" in message
        assert details["config_keys"] == 2
        assert details["required_keys"] == 0
    
    def test_check_config_loaded_required_keys_present(self):
        """Test config check with required keys present."""
        config = {"kafka": {"host": "localhost"}, "redis": {"port": 6379}}
        required_keys = ["kafka", "redis"]
        result = check_config_loaded(config, required_keys)
        
        is_healthy, message, details = result
        assert is_healthy is True
        assert "Configuration loaded successfully (2 keys)" in message
        assert details["config_keys"] == 2
        assert details["required_keys"] == 2
    
    def test_check_config_loaded_missing_required_keys(self):
        """Test config check with missing required keys."""
        config = {"kafka": {"host": "localhost"}}
        required_keys = ["kafka", "redis", "neo4j"]
        result = check_config_loaded(config, required_keys)
        
        is_healthy, message, details = result
        assert is_healthy is False
        assert "Missing required configuration keys: redis, neo4j" in message
        assert details["missing_keys"] == ["redis", "neo4j"]
        assert details["config_keys"] == 1


class TestConvenienceFunctions:
    """Test convenience functions for health checker creation."""
    
    def test_create_basic_health_checker(self):
        """Test creating basic health checker."""
        checker = create_basic_health_checker("test_agent")
        
        assert isinstance(checker, HealthChecker)
        assert checker.agent_name == "test_agent"
        assert len(checker.readiness_checks) == 0
    
    def test_create_kafka_health_checker(self):
        """Test creating Kafka health checker."""
        kafka_config = {"bootstrap_servers": "localhost:9092"}
        checker = create_kafka_health_checker("test_agent", kafka_config)
        
        assert isinstance(checker, HealthChecker)
        assert checker.agent_name == "test_agent"
        assert "kafka" in checker.readiness_checks
        assert len(checker.readiness_checks) == 1
    
    def test_create_kafka_health_checker_default_servers(self):
        """Test creating Kafka health checker with default servers."""
        kafka_config = {}  # No bootstrap_servers specified
        checker = create_kafka_health_checker("test_agent", kafka_config)
        
        assert isinstance(checker, HealthChecker)
        assert "kafka" in checker.readiness_checks


class TestHealthCheckIntegration:
    """Integration tests for health check flows."""
    
    @pytest.mark.asyncio
    async def test_full_health_check_flow(self):
        """Test complete health check flow."""
        checker = HealthChecker("integration_test_agent")
        
        # Add some mock checks
        def db_check():
            return (True, "Database connected")
        
        async def service_check():
            return (True, "Service available", {"version": "1.0"})
        
        checker.add_readiness_check("database", db_check)
        checker.add_readiness_check("service", service_check)
        
        # Test liveness
        liveness_report = await checker.check_liveness()
        assert liveness_report.status == HealthStatus.HEALTHY
        
        # Test readiness
        readiness_report = await checker.check_readiness()
        assert readiness_report.status == HealthStatus.HEALTHY
        assert len(readiness_report.checks) == 2
        
        # Test serialization
        liveness_dict = liveness_report.to_dict()
        readiness_dict = readiness_report.to_dict()
        
        assert isinstance(liveness_dict, dict)
        assert isinstance(readiness_dict, dict)
        assert liveness_dict["status"] == "healthy"
        assert readiness_dict["status"] == "healthy"


if __name__ == "__main__":
    pytest.main([__file__])