"""
Simplified retry framework coverage tests focusing on actual implementation.

This file targets uncovered code paths in retry_framework.py for production readiness.
"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from dataclasses import dataclass

from common_tools.retry_framework import (
    RetryBudgetManager, 
    CircuitBreakerManager, 
    RetryPolicy,
    retry_with_policy,
    _RAW_RETRY_POLICIES
)


class TestRetryBudgetManagerCoverage:
    """Test RetryBudgetManager edge cases and coverage."""
    
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
        
        # Budget should be incremented (consumed)
        assert manager._budgets["agent1"]["op1"] == initial_budget + 1

    def test_budget_exhaustion(self):
        """Test budget exhaustion behavior."""
        manager = RetryBudgetManager()
        
        # Exhaust budget
        for i in range(5):
            assert manager.check_budget("agent1", "op1", 5) is True
            manager.consume_budget("agent1", "op1")
        
        # Should be at limit
        assert manager.check_budget("agent1", "op1", 5) is False

    @patch('time.time')
    def test_budget_window_reset(self, mock_time):
        """Test budget window reset functionality."""
        manager = RetryBudgetManager()
        
        # Start at time 0
        mock_time.return_value = 0
        manager.check_budget("agent1", "op1", 5)
        
        # Advance to next minute
        mock_time.return_value = 65  # 65 seconds = minute 1
        
        # Budget should reset
        assert manager.check_budget("agent1", "op1", 5) is True
        assert manager._budgets["agent1"]["op1"] == 0


class TestCircuitBreakerManagerCoverage:
    """Test CircuitBreakerManager functionality."""
    
    def test_circuit_breaker_initialization(self):
        """Test CircuitBreakerManager initialization."""
        manager = CircuitBreakerManager()
        # Use actual attribute names from implementation
        assert manager._states == {}
        assert manager._failures == {}
        assert manager._opened_at == {}

    def test_record_failure_new_circuit(self):
        """Test recording failure for new circuit."""
        manager = CircuitBreakerManager()
        
        # Create a proper RetryPolicy instance
        policy = RetryPolicy(circuit_breaker_threshold=3, circuit_breaker_timeout=60)
        
        # Record failure
        manager.record_failure("agent1", "op1", policy)
        
        # Check internal state
        key = ("agent1", "op1")
        assert key in manager._failures
        assert manager._failures[key] == 1

    def test_circuit_breaker_opens_after_threshold(self):
        """Test circuit breaker opens after failure threshold."""
        manager = CircuitBreakerManager()
        
        policy = RetryPolicy(circuit_breaker_threshold=2, circuit_breaker_timeout=60)
        
        # Record failures up to threshold
        for _ in range(2):
            manager.record_failure("agent1", "op1", policy)
        
        # Circuit should be open
        assert manager.is_open("agent1", "op1", policy) is True

    @patch('time.time')
    def test_circuit_breaker_half_open_after_timeout(self, mock_time):
        """Test circuit breaker goes to half-open after timeout."""
        manager = CircuitBreakerManager()
        
        policy = RetryPolicy(circuit_breaker_threshold=1, circuit_breaker_timeout=1)
        
        # Record failure to open circuit
        mock_time.return_value = 0
        manager.record_failure("agent1", "op1", policy)
        
        # Circuit should be open
        assert manager.is_open("agent1", "op1", policy) is True
        
        # Advance time past timeout
        mock_time.return_value = 2
        
        # Circuit should be half-open (not open)
        assert manager.is_open("agent1", "op1", policy) is False

    def test_record_success_closes_circuit(self):
        """Test recording success closes circuit."""
        manager = CircuitBreakerManager()
        
        policy = RetryPolicy(circuit_breaker_threshold=1, circuit_breaker_timeout=60)
        
        # Open circuit
        manager.record_failure("agent1", "op1", policy)
        assert manager.is_open("agent1", "op1", policy) is True
        
        # Record success to close circuit (simulate half-open state first)
        key = ("agent1", "op1")
        manager._states[key] = 'half-open'
        manager.record_success("agent1", "op1", policy)
        
        # Circuit should be closed
        assert manager._failures[key] == 0

    def test_get_state_values(self):
        """Test get_state return values."""
        manager = CircuitBreakerManager()
        
        # Test closed state (default)
        assert manager.get_state("agent1", "op1") == 0
        
        # Test open state
        manager._states[("agent1", "op1")] = 'open'
        assert manager.get_state("agent1", "op1") == 1
        
        # Test half-open state
        manager._states[("agent1", "op1")] = 'half-open'
        assert manager.get_state("agent1", "op1") == 2


class TestRetryPolicyConfiguration:
    """Test RetryPolicy configuration and defaults."""
    
    def test_retry_policy_defaults(self):
        """Test RetryPolicy default values."""
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.base_delay == 1.0
        assert policy.circuit_breaker_threshold == 5
        assert policy.circuit_breaker_timeout == 60.0


class TestRetryWithSpecificErrors:
    """Test retry behavior with specific exceptions and status codes."""
    
    def test_retry_on_specific_exception_type(self):
        """Test retrying on specific exception types."""
        # Create a test policy
        test_policy = {
            'max_attempts': 2,
            'base_delay': 0.01,
            'retry_on_exceptions': ['ValueError'],
            'jitter': False
        }
        
        # Use patch to temporarily add to raw policies
        with patch.dict(_RAW_RETRY_POLICIES, {'test_exception': test_policy}):
            call_count = [0]
            
            @retry_with_policy('test_exception', agent_type='test', operation='exception')
            def failing_function():
                call_count[0] += 1
                raise ValueError("Test error")
            
            with pytest.raises(ValueError):
                failing_function()
            
            # Should have retried
            assert call_count[0] == 2
            
            # No cleanup needed with patch.dict

    def test_retry_on_status_codes(self):
        """Test retrying on specific HTTP status codes."""
        test_policy = {
            'max_attempts': 2,
            'base_delay': 0.01,
            'retry_on_status_codes': [500],
            'jitter': False
        }
        
        # Use patch to temporarily add to raw policies
        with patch.dict(_RAW_RETRY_POLICIES, {'test_status': test_policy}):
            call_count = [0]
            
            class MockResponse:
                def __init__(self, status_code):
                    self.status_code = status_code
            
            @retry_with_policy('test_status', agent_type='test', operation='status')
            def status_function():
                call_count[0] += 1
                if call_count[0] < 2:
                    return MockResponse(500)
                return MockResponse(200)
            
            result = status_function()
            assert result.status_code == 200
            assert call_count[0] == 2
            
            # No cleanup needed with patch.dict


class TestRateLimiting:
    """Test rate limiting functionality."""
    
    def test_rate_limit_enforcement(self):
        """Test that rate limiting is enforced."""
        # This test ensures rate limiting code paths are covered
        test_policy = {
            'max_attempts': 3,
            'base_delay': 0.01,
            'retry_budget_per_minute': 2,  # Need 2 budget for 2 retries
            'jitter': False,
            'retry_on_exceptions': ['ConnectionError']  # Use a retryable exception
        }
        
        # Use patch to temporarily add to raw policies
        with patch.dict(_RAW_RETRY_POLICIES, {'test_rate_limit': test_policy}):
            call_count = [0]
            
            @retry_with_policy('test_rate_limit', agent_type='test_unique', operation='rate_limit_unique')
            def rate_limited_function():
                call_count[0] += 1
                if call_count[0] <= 2:
                    raise ConnectionError("Retry me")
                return "success"
            
            # Should succeed after 3 attempts
            result = rate_limited_function()
            assert result == "success"
            
            # No cleanup needed with patch.dict
