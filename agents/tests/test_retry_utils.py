import pytest
import requests
from agents.common_tools.retry_utils import load_retry_config, retry_with_policy


def test_load_retry_config_default():
    cfg = load_retry_config('default')
    assert cfg.max_attempts == 3
    assert cfg.base_delay == 1.0
    assert cfg.max_delay == 30.0
    assert cfg.exponential_base == 2.0
    assert cfg.jitter is True


def test_load_retry_config_external_apis():
    cfg = load_retry_config('external_apis')
    assert cfg.max_attempts == 3
    assert cfg.base_delay == 1.0
    assert cfg.max_delay == 15.0
    assert cfg.exponential_base == 2.0
    assert cfg.jitter is True


def test_load_retry_config_invalid():
    with pytest.raises(ValueError):
        load_retry_config('nonexistent_policy')


def test_retry_with_policy_retries_and_returns_success():
    calls = []
    def flaky():
        calls.append(1)
        if len(calls) < 2:
            raise requests.RequestException("fail")
        return "ok"
    decorated = retry_with_policy('default')(flaky)
    result = decorated()
    assert result == "ok"
    assert len(calls) == 2


def test_retry_with_policy_exhausts_retries_and_reraises():
    calls = []
    def always_fail():
        calls.append(1)
        raise requests.RequestException("error")
    decorated = retry_with_policy('default')(always_fail)
    with pytest.raises(requests.RequestException):
        decorated()
    assert len(calls) == load_retry_config('default').max_attempts
