from tenacity import (
    retry, stop_after_attempt, wait_exponential, wait_random,
    retry_if_exception_type, retry_if_result, before_sleep_log,
    after_log, RetryCallState
)
from dataclasses import dataclass, field
from typing import Dict, List, Type, Optional, Callable, Any, Union
import requests
import asyncio
import time
import uuid
import logging
from enum import Enum

try:
    from prometheus_client import Counter, Histogram, Gauge, REGISTRY, CollectorRegistry
except ImportError:
    # Fall back to conftest stubs for testing
    try:
        from ..conftest import _Counter as Counter, _Histogram as Histogram, _Gauge as Gauge, _DEFAULT_REGISTRY as REGISTRY, _CollectorRegistry as CollectorRegistry
    except ImportError:
        # Create minimal stubs if conftest is also not available
        class _DummyCollectorRegistry:
            def __init__(self):
                pass
        
        class _DummyMetric:
            def __init__(self, *args, **kwargs):
                pass
            def labels(self, *args, **kwargs):
                return self
            def inc(self):
                pass
            def observe(self, val):
                pass
            def set(self, val):
                pass
            def time(self):
                return self
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
        Counter = _DummyMetric
        Histogram = _DummyMetric
        Gauge = _DummyMetric
        CollectorRegistry = _DummyCollectorRegistry
        REGISTRY = _DummyCollectorRegistry()


class RetryStrategy(Enum):
    """Available retry strategies."""
    EXPONENTIAL_BACKOFF = "exponential"
    FIXED_DELAY = "fixed"
    LINEAR_BACKOFF = "linear"
    FIBONACCI_BACKOFF = "fibonacci"


@dataclass
class RetryPolicy:
    """Comprehensive retry policy configuration."""
    max_attempts: int = 3
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_max: float = 1.0
    retry_on_status_codes: List[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])
    retry_on_exceptions: List[Type[Exception]] = field(default_factory=lambda: [ConnectionError, TimeoutError, requests.RequestException])
    no_retry_on_status_codes: List[int] = field(default_factory=lambda: [400, 401, 403, 404, 422])
    no_retry_on_exceptions: List[Type[Exception]] = field(default_factory=lambda: [ValueError, TypeError])
    circuit_breaker_enabled: bool = False
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    respect_retry_after_header: bool = True
    retry_budget_per_minute: Optional[int] = None
    correlation_id_enabled: bool = True
    metrics_enabled: bool = True
    log_attempts: bool = True
    log_level: int = logging.WARNING


class RetryBudgetManager:
    """Manages retry budgets to prevent resource exhaustion."""

    def __init__(self):
        self._budgets: Dict[str, Dict[str, int]] = {}
        self._windows: Dict[str, Dict[str, float]] = {}

    def check_budget(self, agent_type: str, operation: str, budget_per_minute: int) -> bool:
        current_time = time.time()
        minute_window = int(current_time // 60)
        if agent_type not in self._budgets:
            self._budgets[agent_type] = {}
            self._windows[agent_type] = {}
        if operation not in self._budgets[agent_type]:
            self._budgets[agent_type][operation] = 0
            self._windows[agent_type][operation] = minute_window
        if self._windows[agent_type][operation] < minute_window:
            self._budgets[agent_type][operation] = 0
            self._windows[agent_type][operation] = minute_window
        return self._budgets[agent_type][operation] < budget_per_minute

    def consume_budget(self, agent_type: str, operation: str):
        if agent_type in self._budgets and operation in self._budgets[agent_type]:
            self._budgets[agent_type][operation] += 1


class CircuitBreakerManager:
    """Manage circuit breaker states for each agent and operation."""
    def __init__(self):
        self._states: Dict[tuple, str] = {}
        self._failures: Dict[tuple, int] = {}
        self._opened_at: Dict[tuple, float] = {}

    def is_open(self, agent_type: str, operation: str, policy: RetryPolicy) -> bool:
        key = (agent_type, operation)
        state = self._states.get(key, 'closed')
        if state == 'open':
            if time.time() - self._opened_at.get(key, 0) >= policy.circuit_breaker_timeout:
                self._states[key] = 'half-open'
                return False
            return True
        return False

    def record_failure(self, agent_type: str, operation: str, policy: RetryPolicy):
        key = (agent_type, operation)
        state = self._states.get(key, 'closed')
        if state == 'open':
            return
        count = self._failures.get(key, 0) + 1
        self._failures[key] = count
        if count >= policy.circuit_breaker_threshold:
            self._states[key] = 'open'
            self._opened_at[key] = time.time()

    def record_success(self, agent_type: str, operation: str, policy: RetryPolicy):
        key = (agent_type, operation)
        state = self._states.get(key, 'closed')
        if state == 'half-open':
            self._states[key] = 'closed'
            self._failures[key] = 0
            self._opened_at.pop(key, None)

    def get_state(self, agent_type: str, operation: str) -> int:
        key = (agent_type, operation)
        state = self._states.get(key, 'closed')
        return {'closed': 0, 'open': 1, 'half-open': 2}[state]


class RetryMetrics:
    """Centralized retry metrics collection."""

    def __init__(self):
        self.retry_attempts_total = Counter(
            'umbrix_retry_attempts_total',
            'Total number of retry attempts',
            ['agent_type', 'operation', 'attempt_number']
        )
        self.retry_success_total = Counter(
            'umbrix_retry_success_total',
            'Total number of successful operations after retries',
            ['agent_type', 'operation']
        )
        self.retry_failure_total = Counter(
            'umbrix_retry_failure_total',
            'Total number of operations that failed after all retries',
            ['agent_type', 'operation', 'final_error_type']
        )
        self.retry_delay_seconds = Histogram(
            'umbrix_retry_delay_seconds',
            'Time spent waiting between retry attempts',
            ['agent_type', 'operation'],
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
        )
        self.retry_budget_remaining = Gauge(
            'umbrix_retry_budget_remaining',
            'Remaining retry budget for current time window',
            ['agent_type', 'operation']
        )
        self.circuit_breaker_state = Gauge(
            'umbrix_circuit_breaker_state',
            'Circuit breaker state (0=closed, 1=open, 2=half-open)',
            ['agent_type', 'operation']
        )


def _get_or_create_metrics():
    """Get or create metrics with proper fallback logic.
    
    Always creates fresh metrics to handle test cleanup scenarios.
    """
    try:
        # Try creating metrics with the default registry first
        metrics = RetryMetrics()
    except Exception as e:
        class IsolatedRetryMetrics:
            def __init__(self):
                test_registry = CollectorRegistry()
                self.retry_attempts_total = Counter(
                    'umbrix_retry_attempts_total_isolated',
                    'Total number of retry attempts',
                    ['agent_type', 'operation', 'attempt_number'],
                    registry=test_registry
                )
                self.retry_success_total = Counter(
                    'umbrix_retry_success_total_isolated',
                    'Total number of successful operations after retries',
                    ['agent_type', 'operation'],
                    registry=test_registry
                )
                self.retry_failure_total = Counter(
                    'umbrix_retry_failure_total_isolated',
                    'Total number of operations that failed after all retries',
                    ['agent_type', 'operation', 'final_error_type'],
                    registry=test_registry
                )
                self.retry_delay_seconds = Histogram(
                    'umbrix_retry_delay_seconds_isolated',
                    'Time spent waiting between retry attempts',
                    ['agent_type', 'operation'],
                    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
                    registry=test_registry
                )
                self.retry_budget_remaining = Gauge(
                    'umbrix_retry_budget_remaining_isolated',
                    'Remaining retry budget for current time window',
                    ['agent_type', 'operation'],
                    registry=test_registry
                )
                self.circuit_breaker_state = Gauge(
                    'umbrix_circuit_breaker_state_isolated',
                    'Circuit breaker state (0=closed, 1=open, 2=half-open)',
                    ['agent_type', 'operation'],
                    registry=test_registry
                )
            
        metrics = IsolatedRetryMetrics()
    except Exception:
        # Fall back to dummy metrics
        class _DummyRetryMetrics:
            def __init__(self):
                class _DummyMetric:
                    def labels(self, *args, **kwargs):
                        return self
                    def inc(self):
                        pass
                    def observe(self, val):
                        pass
                    def set(self, val):
                        pass
                
                self.retry_attempts_total = _DummyMetric()
                self.retry_success_total = _DummyMetric()
                self.retry_failure_total = _DummyMetric()
                self.retry_delay_seconds = _DummyMetric()
                self.retry_budget_remaining = _DummyMetric()
                self.circuit_breaker_state = _DummyMetric()
        
        metrics = _DummyRetryMetrics()
    
    return metrics

# Initialize global retry metrics
_metrics = None
_budget_manager = RetryBudgetManager()
_circuit_manager = CircuitBreakerManager()

# Load raw retry policies from config
_RAW_RETRY_POLICIES = {}

import os
import yaml

# Load retry policies from config
CONFIG_FILE_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, 'config', 'retry_config.yaml')
)
try:
    with open(CONFIG_FILE_PATH) as f:
        data = yaml.safe_load(f) or {}
        # Support both root-level and nested 'retry_policies'
        if 'retry_policies' in data:
            _RAW_RETRY_POLICIES = data.get('retry_policies', {})
        elif isinstance(data, dict):
            _RAW_RETRY_POLICIES = data
        else:
            _RAW_RETRY_POLICIES = {}
except Exception as e:
    logging.getLogger(__name__).warning(f"Failed to load retry config: {e}")
    _RAW_RETRY_POLICIES = {}

# Initialize metrics once with proper fallback handling
_metrics = _get_or_create_metrics()

def retry_with_policy(policy_name: str, agent_type: str = 'default', operation: str = 'default') -> Callable:
    """Decorator applying full RetryPolicy for given agent_type and operation."""
    raw = _RAW_RETRY_POLICIES.get(policy_name)
    if raw is None:
        raise ValueError(f"Retry policy '{policy_name}' not found")
    # Helper function to convert string exception names to types
    def _parse_exceptions(exc_list):
        if not exc_list:
            return []
        result = []
        for exc in exc_list:
            if isinstance(exc, str):
                # Try to resolve the exception type
                if exc == 'ValueError':
                    result.append(ValueError)
                elif exc == 'TypeError':
                    result.append(TypeError)
                elif exc == 'requests.RequestException':
                    result.append(requests.RequestException)
                elif exc == 'ConnectionError':
                    result.append(ConnectionError)
                elif exc == 'TimeoutError':
                    result.append(TimeoutError)
                elif exc == 'Exception':
                    result.append(Exception)
                else:
                    # Try to resolve from builtins or requests module
                    try:
                        result.append(eval(exc))
                    except:
                        pass
            else:
                result.append(exc)
        return result
    
    # Build policy object
    policy = RetryPolicy(
        max_attempts=raw.get('max_attempts', RetryPolicy().max_attempts),
        strategy=RetryStrategy(raw.get('strategy', RetryStrategy.EXPONENTIAL_BACKOFF.value)),
        base_delay=raw.get('base_delay', RetryPolicy().base_delay),
        max_delay=raw.get('max_delay', RetryPolicy().max_delay),
        exponential_base=raw.get('exponential_base', RetryPolicy().exponential_base),
        jitter=raw.get('jitter', RetryPolicy().jitter),
        jitter_max=raw.get('jitter_max', RetryPolicy().jitter_max),
        retry_on_status_codes=raw.get('retry_on_status_codes', RetryPolicy().retry_on_status_codes),
        retry_on_exceptions=_parse_exceptions(raw.get('retry_on_exceptions', [])) or RetryPolicy().retry_on_exceptions,
        no_retry_on_status_codes=raw.get('no_retry_on_status_codes', RetryPolicy().no_retry_on_status_codes),
        no_retry_on_exceptions=_parse_exceptions(raw.get('no_retry_on_exceptions', [])) or RetryPolicy().no_retry_on_exceptions,
        retry_budget_per_minute=raw.get('retry_budget_per_minute', raw.get('rate_limit_per_minute', RetryPolicy().retry_budget_per_minute)),
        correlation_id_enabled=raw.get('correlation_id_enabled', RetryPolicy().correlation_id_enabled),
        metrics_enabled=raw.get('metrics_enabled', RetryPolicy().metrics_enabled),
        log_attempts=raw.get('log_attempts', RetryPolicy().log_attempts),
        log_level=raw.get('log_level', RetryPolicy().log_level)
    )
    # Choose wait strategy
    wait = wait_exponential(multiplier=policy.base_delay, max=policy.max_delay, exp_base=policy.exponential_base)
    if policy.jitter:
        wait = wait + wait_random(0, policy.jitter_max)
    # Retry conditions: respect no-retry lists and retry-after header
    allowed_exc = retry_if_exception_type(tuple(policy.retry_on_exceptions))
    denied_exc = retry_if_exception_type(tuple(policy.no_retry_on_exceptions))
    # Use separate conditions since tenacity doesn't support negation operator
    exc_retry = allowed_exc
    def _status_predicate(r):
        code = getattr(r, 'status_code', None)
        return code in policy.retry_on_status_codes and code not in policy.no_retry_on_status_codes
    status_retry = retry_if_result(_status_predicate)
    retry_condition = exc_retry | status_retry

    def _before_call(retry_state: RetryCallState):
        # Circuit breaker: short-circuit if open
        if policy.circuit_breaker_enabled and _circuit_manager.is_open(agent_type, operation, policy):
            metrics = _get_or_create_metrics()
            if metrics is not None:
                metrics.circuit_breaker_state.labels(agent_type, operation).set(1)
            raise Exception(f"Circuit open for {agent_type}/{operation}")
        # Check if previous outcome failed with a denied exception
        if retry_state.outcome and retry_state.outcome.failed:
            exc = retry_state.outcome.exception()
            if any(isinstance(exc, denied_type) for denied_type in policy.no_retry_on_exceptions):
                raise exc
        if policy.correlation_id_enabled:
            cid = uuid.uuid4().hex
            logging.getLogger(__name__).info(
                f"[{agent_type}/{operation}] correlation_id={cid}, attempt={retry_state.attempt_number}"
            )

    def _before_sleep(retry_state: RetryCallState):
        # honor Retry-After header if present
        if policy.respect_retry_after_header:
            try:
                result = retry_state.outcome.result()
                headers = getattr(result, 'headers', {})
                ra = headers.get('Retry-After')
                if ra:
                    retry_state.next_action.sleep = float(ra)
            except Exception:
                pass
        attempt = retry_state.attempt_number
        err = retry_state.outcome.exception() if retry_state.outcome.failed else None
        if policy.log_attempts:
            logging.getLogger(__name__).log(policy.log_level,
                f"[{agent_type}/{operation}] attempt {attempt} failed: {err}, sleeping {retry_state.next_action.sleep:.2f}s")
        if policy.metrics_enabled:
            metrics = _get_or_create_metrics()
            if metrics is not None:
                metrics.retry_attempts_total.labels(agent_type, operation, attempt).inc()
                metrics.retry_delay_seconds.labels(agent_type, operation).observe(retry_state.next_action.sleep)
        if policy.retry_budget_per_minute:
            if not _budget_manager.check_budget(agent_type, operation, policy.retry_budget_per_minute):
                metrics = _get_or_create_metrics()
                if metrics is not None:
                    metrics.circuit_breaker_state.labels(agent_type, operation).set(1)
                raise Exception(f"Circuit open for {agent_type}/{operation}")
            _budget_manager.consume_budget(agent_type, operation)
        # Record circuit breaker failure if enabled
        if policy.circuit_breaker_enabled and retry_state.outcome.failed:
            _circuit_manager.record_failure(agent_type, operation, policy)
            state_val = _circuit_manager.get_state(agent_type, operation)
            metrics = _get_or_create_metrics()
            if metrics is not None:
                metrics.circuit_breaker_state.labels(agent_type, operation).set(state_val)

    def _after_call(retry_state: RetryCallState):
        # On success, reset circuit breaker if half-open
        if policy.circuit_breaker_enabled:
            _circuit_manager.record_success(agent_type, operation, policy)
            metrics = _get_or_create_metrics()
            if metrics is not None:
                metrics.circuit_breaker_state.labels(agent_type, operation).set(
                    _circuit_manager.get_state(agent_type, operation)
                )
        if not retry_state.outcome.failed and policy.metrics_enabled:
            metrics = _get_or_create_metrics()
            if metrics is not None:
                metrics.retry_success_total.labels(agent_type, operation).inc()

    decorator = retry(
        stop=stop_after_attempt(policy.max_attempts),
        wait=wait,
        retry=retry_condition,
        before=_before_call,
        before_sleep=_before_sleep,
        after=_after_call,
        reraise=True
    )
    return decorator

__all__ = [
    'RetryStrategy', 'RetryPolicy', 'RetryBudgetManager', 'CircuitBreakerManager', 'RetryMetrics', 'retry_with_policy'
]
