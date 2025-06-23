"""
Health check utilities for agent monitoring.

Provides standardized health and readiness checks for all agents.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum

import kafka
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None


class HealthStatus(str, Enum):
    """Health status enumeration."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"


@dataclass
class HealthCheck:
    """Individual health check result."""
    name: str
    status: HealthStatus
    message: str
    response_time_ms: float
    timestamp: datetime
    details: Optional[Dict[str, Any]] = None


@dataclass
class HealthReport:
    """Complete health report."""
    status: HealthStatus
    checks: List[HealthCheck]
    overall_response_time_ms: float
    timestamp: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'status': self.status.value,
            'checks': [
                {
                    'name': check.name,
                    'status': check.status.value,
                    'message': check.message,
                    'response_time_ms': check.response_time_ms,
                    'timestamp': check.timestamp.isoformat(),
                    'details': check.details
                }
                for check in self.checks
            ],
            'overall_response_time_ms': self.overall_response_time_ms,
            'timestamp': self.timestamp.isoformat()
        }


class HealthChecker:
    """
    Health checker for agents.
    
    Provides liveness and readiness checks with dependency monitoring.
    """
    
    def __init__(self, agent_name: str):
        """Initialize health checker."""
        self.agent_name = agent_name
        self.logger = logging.getLogger(__name__)
        self.readiness_checks: Dict[str, Callable] = {}
        
    def add_readiness_check(self, name: str, check_func: Callable) -> None:
        """
        Add a readiness check function.
        
        Args:
            name: Name of the check
            check_func: Async function that returns (bool, str, optional_details)
        """
        self.readiness_checks[name] = check_func
    
    async def check_liveness(self) -> HealthReport:
        """
        Check if the agent process is alive.
        
        Returns:
            HealthReport with liveness status
        """
        start_time = time.time()
        timestamp = datetime.now(timezone.utc)
        
        # Liveness is simple - if we can execute this function, we're alive
        liveness_check = HealthCheck(
            name="liveness",
            status=HealthStatus.HEALTHY,
            message="Agent process is running",
            response_time_ms=(time.time() - start_time) * 1000,
            timestamp=timestamp
        )
        
        return HealthReport(
            status=HealthStatus.HEALTHY,
            checks=[liveness_check],
            overall_response_time_ms=(time.time() - start_time) * 1000,
            timestamp=timestamp
        )
    
    async def check_readiness(self) -> HealthReport:
        """
        Check if the agent is ready to handle requests.
        
        Returns:
            HealthReport with readiness status
        """
        start_time = time.time()
        timestamp = datetime.now(timezone.utc)
        checks = []
        overall_status = HealthStatus.HEALTHY
        
        # Run all registered readiness checks
        for check_name, check_func in self.readiness_checks.items():
            check_start = time.time()
            
            try:
                # Call the check function
                if asyncio.iscoroutinefunction(check_func):
                    result = await check_func()
                else:
                    result = check_func()
                
                # Parse result
                if isinstance(result, tuple):
                    is_healthy = result[0]
                    message = result[1] if len(result) > 1 else "Check completed"
                    details = result[2] if len(result) > 2 else None
                else:
                    is_healthy = bool(result)
                    message = "Check completed"
                    details = None
                
                status = HealthStatus.HEALTHY if is_healthy else HealthStatus.UNHEALTHY
                
                # If any check fails, overall status becomes unhealthy
                if not is_healthy:
                    overall_status = HealthStatus.UNHEALTHY
                
            except Exception as e:
                status = HealthStatus.UNHEALTHY
                message = f"Check failed: {str(e)}"
                details = {'error_type': type(e).__name__}
                overall_status = HealthStatus.UNHEALTHY
            
            check = HealthCheck(
                name=check_name,
                status=status,
                message=message,
                response_time_ms=(time.time() - check_start) * 1000,
                timestamp=timestamp,
                details=details
            )
            checks.append(check)
        
        # If no checks are registered, consider the agent ready
        if not checks:
            checks.append(HealthCheck(
                name="no_dependencies",
                status=HealthStatus.HEALTHY,
                message="No dependency checks configured",
                response_time_ms=0.0,
                timestamp=timestamp
            ))
        
        return HealthReport(
            status=overall_status,
            checks=checks,
            overall_response_time_ms=(time.time() - start_time) * 1000,
            timestamp=timestamp
        )


# Predefined health check functions
async def check_kafka_connectivity(bootstrap_servers: str, timeout: int = 5) -> tuple:
    """
    Check Kafka connectivity.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        timeout: Connection timeout in seconds
        
    Returns:
        Tuple of (is_healthy, message, details)
    """
    try:
        # Try to create a producer to test connectivity
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=timeout * 1000,
            retries=0
        )
        
        # Get cluster metadata to verify connection
        metadata = producer.list_topics(timeout=timeout)
        producer.close()
        
        return (
            True,
            f"Connected to Kafka cluster with {len(metadata)} topics",
            {"topic_count": len(metadata), "bootstrap_servers": bootstrap_servers}
        )
        
    except KafkaError as e:
        return (
            False,
            f"Kafka connection failed: {str(e)}",
            {"error_type": type(e).__name__, "bootstrap_servers": bootstrap_servers}
        )
    except Exception as e:
        return (
            False,
            f"Unexpected error checking Kafka: {str(e)}",
            {"error_type": type(e).__name__}
        )


async def check_redis_connectivity(redis_url: str, timeout: int = 5) -> tuple:
    """
    Check Redis connectivity.
    
    Args:
        redis_url: Redis connection URL
        timeout: Connection timeout in seconds
        
    Returns:
        Tuple of (is_healthy, message, details)
    """
    if aioredis is None:
        return (
            False,
            "redis.asyncio is not available",
            {"error_type": "ImportError"}
        )
    
    redis_client = None
    try:
        redis_client = aioredis.from_url(redis_url, socket_timeout=timeout)
        
        # Test connection with PING
        start_time = time.time()
        pong = await redis_client.ping()
        ping_time = (time.time() - start_time) * 1000
        
        # Get Redis info
        info = await redis_client.info()
        
        return (
            True,
            f"Redis connection successful (ping: {ping_time:.2f}ms)",
            {
                "ping_time_ms": ping_time,
                "redis_version": info.get("redis_version"),
                "connected_clients": info.get("connected_clients"),
                "used_memory_human": info.get("used_memory_human")
            }
        )
        
    except Exception as e:
        return (
            False,
            f"Redis connection failed: {str(e)}",
            {"error_type": type(e).__name__, "redis_url": redis_url}
        )
    finally:
        if redis_client:
            try:
                await redis_client.close()
            except Exception:
                pass


async def check_neo4j_connectivity(uri: str, user: str, password: str, timeout: int = 5) -> tuple:
    """
    Check Neo4j connectivity.
    
    Args:
        uri: Neo4j URI
        user: Username
        password: Password
        timeout: Connection timeout in seconds
        
    Returns:
        Tuple of (is_healthy, message, details)
    """
    driver = None
    try:
        from neo4j import AsyncGraphDatabase
        
        driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
        
        # Test connection with a simple query
        start_time = time.time()
        async with driver.session() as session:
            result = await session.run("RETURN 1 as test")
            record = await result.single()
            assert record["test"] == 1
        
        query_time = (time.time() - start_time) * 1000
        
        return (
            True,
            f"Neo4j connection successful (query: {query_time:.2f}ms)",
            {"query_time_ms": query_time, "uri": uri}
        )
        
    except ImportError:
        return (
            False,
            "Neo4j driver not available",
            {"error_type": "ImportError"}
        )
    except Exception as e:
        return (
            False,
            f"Neo4j connection failed: {str(e)}",
            {"error_type": type(e).__name__, "uri": uri}
        )
    finally:
        if driver:
            try:
                await driver.close()
            except Exception:
                pass


def check_config_loaded(config: Dict[str, Any], required_keys: List[str] = None) -> tuple:
    """
    Check if configuration is properly loaded.
    
    Args:
        config: Configuration dictionary
        required_keys: List of required configuration keys
        
    Returns:
        Tuple of (is_healthy, message, details)
    """
    if not config:
        return (
            False,
            "Configuration is empty",
            {"config_keys": 0}
        )
    
    missing_keys = []
    if required_keys:
        missing_keys = [key for key in required_keys if key not in config]
    
    if missing_keys:
        return (
            False,
            f"Missing required configuration keys: {', '.join(missing_keys)}",
            {"missing_keys": missing_keys, "config_keys": len(config)}
        )
    
    return (
        True,
        f"Configuration loaded successfully ({len(config)} keys)",
        {"config_keys": len(config), "required_keys": len(required_keys or [])}
    )


# Convenience functions for common health check setups
def create_basic_health_checker(agent_name: str) -> HealthChecker:
    """Create a basic health checker with no dependencies."""
    return HealthChecker(agent_name)


def create_kafka_health_checker(agent_name: str, kafka_config: Dict[str, Any]) -> HealthChecker:
    """Create a health checker with Kafka dependency check."""
    checker = HealthChecker(agent_name)
    
    bootstrap_servers = kafka_config.get('bootstrap_servers', 'localhost:9092')
    checker.add_readiness_check(
        'kafka',
        lambda: check_kafka_connectivity(bootstrap_servers)
    )
    
    return checker


def create_full_health_checker(
    agent_name: str,
    kafka_config: Dict[str, Any] = None,
    redis_config: Dict[str, Any] = None,
    neo4j_config: Dict[str, Any] = None,
    agent_config: Dict[str, Any] = None,
    required_config_keys: List[str] = None
) -> HealthChecker:
    """Create a health checker with all common dependency checks."""
    checker = HealthChecker(agent_name)
    
    # Add Kafka check if configured
    if kafka_config:
        bootstrap_servers = kafka_config.get('bootstrap_servers', 'localhost:9092')
        checker.add_readiness_check(
            'kafka',
            lambda: check_kafka_connectivity(bootstrap_servers)
        )
    
    # Add Redis check if configured
    if redis_config:
        redis_url = redis_config.get('url', 'redis://localhost:6379')
        checker.add_readiness_check(
            'redis',
            lambda: check_redis_connectivity(redis_url)
        )
    
    # Add Neo4j check if configured
    if neo4j_config:
        uri = neo4j_config.get('uri', 'bolt://localhost:7687')
        user = neo4j_config.get('user', 'neo4j')
        password = neo4j_config.get('password', 'password')
        checker.add_readiness_check(
            'neo4j',
            lambda: check_neo4j_connectivity(uri, user, password)
        )
    
    # Add config check if configured
    if agent_config is not None:
        checker.add_readiness_check(
            'config',
            lambda: check_config_loaded(agent_config, required_config_keys)
        )
    
    return checker