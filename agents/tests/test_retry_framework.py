import pytest
import pytest
import requests
import time
from agents.common_tools import retry_framework as rf
from agents.common_tools.retry_framework import retry_with_policy, RetryPolicy, RetryStrategy


def test_exception_retry_success():
    calls = []
    def flaky():
        calls.append(1)
        if len(calls) < 2:
            raise requests.RequestException("fail")
        return "ok"
    decorated = retry_with_policy('default', agent_type='test', operation='op')(flaky)
    result = decorated()
    assert result == "ok"
    assert len(calls) == 2


def test_status_code_retry_success():
    class Resp:
        def __init__(self, code):
            self.status_code = code
    calls = []
    def f():
        calls.append(1)
        if len(calls) < 2:
            return Resp(429)
        return Resp(200)
    decorated = retry_with_policy('external_apis', agent_type='test', operation='op')(f)
    resp = decorated()
    assert hasattr(resp, 'status_code')
    assert resp.status_code == 200
    assert len(calls) == 2


def test_circuit_breaker_after_budget_exhaustion(monkeypatch):
    # Define a test policy with low budget
    rf._RAW_RETRY_POLICIES['test_budget'] = {
        'max_attempts': 5,
        'base_delay': 0,
        'max_delay': 0,
        'strategy': RetryStrategy.FIXED_DELAY.value,
        'jitter': False,
        'rate_limit_per_minute': 1,
        'retry_on_status_codes': [],
    }
    # Reset budget manager state
    rf._budget_manager._budgets.clear()
    rf._budget_manager._windows.clear()

    calls = []
    def f():
        calls.append(1)
        raise requests.RequestException("err")
    decorated = retry_with_policy('test_budget', agent_type='test', operation='op')(f)
    with pytest.raises(Exception) as exc:
        decorated()
    assert 'Circuit open for test/op' in str(exc.value)
    # First retry consumed budget, second retry triggers circuit
    assert len(calls) == 2


def test_no_retry_status_code():
    class Resp:
        def __init__(self, code):
            self.status_code = code
            self.headers = {}
    calls = []
    def f():
        calls.append(1)
        return Resp(404)
    decorated = retry_with_policy('external_apis', agent_type='test', operation='op')(f)
    resp = decorated()
    assert len(calls) == 1
    assert resp.status_code == 404


def test_retry_after_header_override(monkeypatch):
    class Resp:
        def __init__(self, code, headers):
            self.status_code = code
            self.headers = headers
    calls = []
    def f():
        calls.append(time.time())
        if len(calls) == 1:
            return Resp(429, {'Retry-After': '0.1'})
        return Resp(200, {})
    decorated = retry_with_policy('external_apis', agent_type='test', operation='op')(f)
    start = time.time()
    resp = decorated()
    elapsed = calls[1] - calls[0]
    assert resp.status_code == 200
    assert elapsed >= 0.1
