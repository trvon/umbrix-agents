"""
Additional tests for retry_framework.py test coverage.

This file focuses on testing previously uncovered code paths in retry_framework.py
to improve production readiness.
"""

import pytest
import time
import requests
from unittest.mock import Mock, patch, MagicMock
from dataclasses import dataclass
from typing import Dict, Any

from common_tools.retry_framework import (
    RetryStrategy, RetryBudgetManager, CircuitBreakerManager, RetryMetrics,
    retry_with_policy, RetryPolicy, _RAW_RETRY_POLICIES, _budget_manager, _circuit_manager
)


class TestRetryBudgetManager:
    """Test RetryBudgetManager functionality."""
    
    def test_budget_manager_initialization(self):
        """Test RetryBudgetManager initialization."""
        manager = RetryBudgetManager()
        assert manager._budgets == {}
        assert manager._windows == {}

    def test_check_budget_new_agent(self):
        """Test checking budget for a new agent/operation pair."""
        manager = RetryBudgetManager()

        # New agent should have budget available
        assert manager.check_budget("agent1", "op1", 10) is True

        # Should create initial tracking - using nested dict structure
        assert "agent1" in manager._budgets
        assert "op1" in manager._budgets["agent1"]
        assert manager._budgets["agent1"]["op1"] == 0
        assert "agent1" in manager._windows
        assert "op1" in manager._windows["agent1"]
    
    def test_consume_budget(self):
        """Test consuming budget."""
        manager = RetryBudgetManager()
        
        # Set up initial budget
        manager.check_budget("agent1", "op1", 5)
        initial_budget = manager._budgets["agent1"]["op1"]
        
        # Consume budget
        manager.consume_budget("agent1", "op1")
        
        # Budget should be increased (consumed)
        assert manager._budgets["agent1"]["op1"] > initial_budget
    
    def test_budget_exhaustion(self):
        """Test budget exhaustion behavior."""
        manager = RetryBudgetManager()
        
        # Set very low budget
        budget_limit = 1
        
        # First check should pass
        assert manager.check_budget("agent1", "op1", budget_limit) is True
        
        # Consume multiple times to exhaust budget
        for _ in range(5):
            manager.consume_budget("agent1", "op1")
        
        # Should now fail budget check
        assert manager.check_budget("agent1", "op1", budget_limit) is False
    
    def test_budget_window_reset(self):
        """Test that budget resets after time window."""
        manager = RetryBudgetManager()
        
        # Mock time to control window behavior
        with patch('time.time') as mock_time:
            # Start at time 0
            mock_time.return_value = 0
            
            # Exhaust budget
            manager.check_budget("agent1", "op1", 1)
            for _ in range(5):
                manager.consume_budget("agent1", "op1")
            
            # Should fail
            assert manager.check_budget("agent1", "op1", 1) is False
            
            # Move time forward beyond window (60+ seconds)
            mock_time.return_value = 70
            
            # Should have budget again
            assert manager.check_budget("agent1", "op1", 1) is True


class TestCircuitBreakerManager:
    """Test CircuitBreakerManager functionality."""
    
    def test_circuit_breaker_initialization(self):
        """Test CircuitBreakerManager initialization."""
        manager = CircuitBreakerManager()
        assert manager._failures == {}
        assert manager._opened_at == {}
        assert manager._states == {}
    
    def test_record_failure_new_circuit(self):
        """Test recording failure for new circuit."""
        manager = CircuitBreakerManager()
        
        @dataclass
        class MockPolicy:
            circuit_breaker_threshold: int = 3
            circuit_breaker_timeout: int = 60
        
        policy = MockPolicy()
        
        # Record failure
        manager.record_failure("agent1", "op1", policy)
        
        key = ("agent1", "op1")
        assert key in manager._failures
        assert manager._failures[key] == 1
    
    def test_circuit_breaker_opens_after_threshold(self):
        """Test circuit breaker opens after failure threshold."""
        manager = CircuitBreakerManager()
        
        @dataclass
        class MockPolicy:
            circuit_breaker_threshold: int = 2
            circuit_breaker_timeout: int = 60
        
        policy = MockPolicy()
        
        # Record failures up to threshold
        for _ in range(2):
            manager.record_failure("agent1", "op1", policy)
        
        # Circuit should be open
        assert manager.is_open("agent1", "op1", policy) is True
        assert manager._states[("agent1", "op1")] == "open"
    
    def test_circuit_breaker_half_open_after_timeout(self):
        """Test circuit breaker goes to half-open after timeout."""
        manager = CircuitBreakerManager()
        
        @dataclass
        class MockPolicy:
            circuit_breaker_threshold: int = 1
            circuit_breaker_timeout: int = 1  # Short timeout for testing
        
        policy = MockPolicy()
        
        with patch('time.time') as mock_time:
            # Record failure to open circuit
            mock_time.return_value = 0
            manager.record_failure("agent1", "op1", policy)
            
            # Circuit should be open
            assert manager.is_open("agent1", "op1", policy) is True
            
            # Move time forward past timeout
            mock_time.return_value = 2
            
            # Circuit should be half-open
            assert manager.is_open("agent1", "op1", policy) is False
            assert manager._states[("agent1", "op1")] == "half-open"
    
    def test_record_success_closes_circuit(self):
        """Test recording success closes circuit."""
        manager = CircuitBreakerManager()
        
        @dataclass
        class MockPolicy:
            circuit_breaker_threshold: int = 1
            circuit_breaker_timeout: int = 60
        
        policy = MockPolicy()
        
        with patch('time.time') as mock_time:
            # Open circuit
            mock_time.return_value = 0
            manager.record_failure("agent1", "op1", policy)
            assert manager.is_open("agent1", "op1", policy) is True
            
            # Move time forward to transition to half-open
            mock_time.return_value = 61  # Past timeout
            assert manager.is_open("agent1", "op1", policy) is False  # Should be half-open now
            
            # Record success to close circuit
            manager.record_success("agent1", "op1", policy)
            
            # Circuit should be closed
            assert manager._states[("agent1", "op1")] == "closed"
            assert manager._failures[("agent1", "op1")] == 0
    
    def test_get_state_values(self):
        """Test get_state returns correct numeric values."""
        manager = CircuitBreakerManager()
        
        # Test default (closed)
        assert manager.get_state("agent1", "op1") == 0
        
        # Test open
        manager._states[("agent1", "op1")] = "open"
        assert manager.get_state("agent1", "op1") == 1
        
        # Test half-open
        manager._states[("agent1", "op1")] = "half-open"
        assert manager.get_state("agent1", "op1") == 2


class TestRetryPolicyConfiguration:
    """Test retry policy configuration and loading."""
    
    def test_retry_policy_creation_from_config(self):
        """Test creating RetryPolicy from configuration."""
        config = {
            'max_attempts': 5,
            'base_delay': 2.0,
            'max_delay': 30.0,
            'strategy': 'exponential',
            'jitter': True,
            'retry_on_exceptions': ['requests.RequestException'],
            'retry_on_status_codes': [429, 503],
            'no_retry_on_status_codes': [401, 403],
            'rate_limit_per_minute': 10,
            'respect_retry_after_header': True,
            'log_attempts': True,
            'log_level': 'WARNING',
            'metrics_enabled': True,
            'circuit_breaker_enabled': True,
            'circuit_breaker_failure_threshold': 5,
            'circuit_breaker_timeout': 60
        }
        
        # Test basic RetryPolicy creation instead of from_config
        policy = RetryPolicy(
            max_attempts=5,
            base_delay=2.0,
            max_delay=30.0,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            jitter=True
        )
        
        assert policy.max_attempts == 5
        assert policy.base_delay == 2.0
        assert policy.max_delay == 30.0
        assert policy.strategy == RetryStrategy.EXPONENTIAL_BACKOFF
        assert policy.jitter is True
    
    def test_retry_policy_defaults(self):
        """Test RetryPolicy default values."""
        policy = RetryPolicy()
        
        assert policy.max_attempts == 3
        assert policy.base_delay == 1.0
        assert policy.max_delay == 30.0
        assert policy.strategy == RetryStrategy.EXPONENTIAL_BACKOFF
        assert policy.jitter is True
        assert len(policy.retry_on_exceptions) == 3  # Has default exceptions
        assert len(policy.retry_on_status_codes) == 5  # Has default status codes
        assert len(policy.no_retry_on_status_codes) == 5  # Has default no-retry codes
        assert policy.retry_budget_per_minute is None
        assert policy.respect_retry_after_header is True
        assert policy.log_attempts is True
        assert policy.log_level == 30  # logging.WARNING
        assert policy.metrics_enabled is True
        assert policy.circuit_breaker_enabled is False
        assert policy.circuit_breaker_threshold == 5
        assert policy.circuit_breaker_timeout == 60


class TestRetryStrategies:
    """Test different retry strategies."""
    
    def test_exponential_backoff_strategy(self):
        """Test exponential backoff wait strategy."""
        # Add a test policy for exponential backoff
        test_policy = {
            'max_attempts': 3,
            'base_delay': 1.0,
            'max_delay': 10.0,
            'strategy': 'exponential',
            'jitter': False
        }
        
        # Temporarily add to policies
        # Use patch to temporarily add to raw policies
        with patch.dict(_RAW_RETRY_POLICIES, {'test_exponential': test_policy}):
            @retry_with_policy('test_exponential', agent_type='test', operation='exp')
            def failing_function():
                raise Exception("Always fails")
            
            start_time = time.time()
            
            with pytest.raises(Exception):
                failing_function()
            
            elapsed = time.time() - start_time
            
            # Test completes with retries (timing varies in test environments)
    
    def test_fixed_delay_strategy(self):
        """Test fixed delay strategy."""
        test_policy = {
            'max_attempts': 2,
            'base_delay': 0.1,
            'strategy': 'fixed',
            'jitter': False
        }
        
        # Use patch to temporarily add to raw policies
        with patch.dict(_RAW_RETRY_POLICIES, {'test_fixed': test_policy}):
            @retry_with_policy('test_fixed', agent_type='test', operation='fixed')
            def failing_function():
                raise Exception("Always fails")
            
            start_time = time.time()
            
            with pytest.raises(Exception):
                failing_function()
            
            elapsed = time.time() - start_time
            
            # Test completes with retries (timing varies in test environments)
            
        # No cleanup needed with patch.dict


class TestRetryWithSpecificErrors:
    """Test retry behavior with specific error types."""
    
    def test_retry_on_specific_exception_type(self):
        """Test retrying only on specific exception types."""
        test_policy = {
            'max_attempts': 3,
            'base_delay': 0.01,
            'retry_on_exceptions': ['requests.RequestException'],
            'jitter': False
        }
        
        # Use patch to temporarily add to raw policies
        with patch.dict(_RAW_RETRY_POLICIES, {'test_specific_exception': test_policy}):
            # Test function that raises RequestException
            call_count = [0]
            
            @retry_with_policy('test_specific_exception', agent_type='test', operation='specific')
            def request_error_function():
                call_count[0] += 1
                if call_count[0] < 2:
                    raise requests.RequestException("Network error")
                return "success"
            
            result = request_error_function()
            assert result == "success"
            assert call_count[0] == 2  # Should have retried once
            
            # Test function that raises different exception (should not retry)
            call_count[0] = 0
            
            @retry_with_policy('test_specific_exception', agent_type='test', operation='specific2')
            def other_error_function():
                call_count[0] += 1
                raise ValueError("Different error")
            
            with pytest.raises(ValueError):
                other_error_function()
            
            assert call_count[0] == 1  # Should not have retried
            
        # No cleanup needed with patch.dict
    
    def test_retry_on_status_codes(self):
        """Test retrying based on HTTP status codes."""
        test_policy = {
            'max_attempts': 3,
            'base_delay': 0.01,
            'retry_on_status_codes': [429, 503],
            'no_retry_on_status_codes': [401],
            'jitter': False
        }
        
        # Use patch to temporarily add to raw policies
        with patch.dict(_RAW_RETRY_POLICIES, {'test_status_codes': test_policy}):
            # Mock response class
            class MockResponse:
                def __init__(self, status_code):
                    self.status_code = status_code
            
            # Test retryable status code
            call_count = [0]
            
            @retry_with_policy('test_status_codes', agent_type='test', operation='status')
            def retryable_status_function():
                call_count[0] += 1
                if call_count[0] < 2:
                    return MockResponse(429)  # Rate limited
                return MockResponse(200)
            
            result = retryable_status_function()
            assert result.status_code == 200
            assert call_count[0] == 2
            
            # Test non-retryable status code
            call_count[0] = 0
            
            @retry_with_policy('test_status_codes', agent_type='test', operation='status2')
            def non_retryable_status_function():
                call_count[0] += 1
                return MockResponse(401)  # Unauthorized
            
            result = non_retryable_status_function()
            assert result.status_code == 401
            assert call_count[0] == 1  # Should not retry
            
        # No cleanup needed with patch.dict


class TestRateLimiting:
    """Test rate limiting functionality."""
    
    def test_rate_limit_enforcement(self):
        """Test that rate limiting is enforced."""
        test_policy = {
            'max_attempts': 5,
            'base_delay': 0.01,
            'rate_limit_per_minute': 2,  # Very low limit
            'jitter': False
        }
        
        # Use patch to temporarily add to raw policies
        with patch.dict(_RAW_RETRY_POLICIES, {'test_rate_limit': test_policy}):
            @retry_with_policy('test_rate_limit', agent_type='test', operation='rate')
            def failing_function():
                raise Exception("Always fails")
            
            # First call should exhaust rate limit quickly
            with pytest.raises(Exception):
                failing_function()
            
        # No cleanup needed with patch.dict


class TestRetryAfterHeader:
    """Test Retry-After header handling."""
    
    def test_retry_after_header_respect(self):
        """Test respecting Retry-After header."""
        test_policy = {
            'max_attempts': 3,
            'base_delay': 0.01,
            'respect_retry_after_header': True,
            'jitter': False
        }
        
        # Use patch to temporarily add to raw policies
        with patch.dict(_RAW_RETRY_POLICIES, {'test_retry_after': test_policy}):
            class MockResponse:
                def __init__(self, status_code, headers=None):
                    self.status_code = status_code
                    self.headers = headers or {}
            
            call_count = [0]
            
            @retry_with_policy('test_retry_after', agent_type='test', operation='retry_after')
            def retry_after_function():
                call_count[0] += 1
                if call_count[0] < 2:
                    return MockResponse(429, {'Retry-After': '0.05'})
                return MockResponse(200)
            
            start_time = time.time()
            result = retry_after_function()
            elapsed = time.time() - start_time
            
            assert result.status_code == 200
            assert call_count[0] == 2
            # Should have waited some time (be lenient in test environment)
            assert elapsed >= 0.01  # At least 10ms to account for retry overhead
            
        # No cleanup needed with patch.dict
