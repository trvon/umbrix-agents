from dataclasses import dataclass, field
from typing import List, Type, Callable
import logging
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, wait_random, retry_if_exception_type, before_sleep_log
import os
import yaml

# Load retry policies from agents/config
CONFIG_FILE_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "retry_config.yaml")
)
try:
    with open(CONFIG_FILE_PATH) as f:
        _RAW_RETRY_POLICIES = yaml.safe_load(f).get("retry_policies", {})
except Exception as e:
    logging.getLogger(__name__).warning(f"Failed to load retry policy config: {e}")
    _RAW_RETRY_POLICIES = {}

@dataclass
class RetryConfig:
    """Standardized retry configuration."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_base: float = 2.0
    jitter: bool = True
    retry_on_exceptions: List[Type[Exception]] = field(default_factory=lambda: [requests.RequestException, ConnectionError])
    correlation_id_header: str = "X-Correlation-ID"


def create_retry_decorator(config: RetryConfig) -> Callable:
    """
    Create a standardized retry decorator using tenacity.
    """
    wait_strategy = wait_exponential(
        multiplier=config.base_delay,
        max=config.max_delay,
        exp_base=config.exponential_base
    )
    if config.jitter:
        wait_strategy = wait_strategy + wait_random(0, 1)

    return retry(
        stop=stop_after_attempt(config.max_attempts),
        wait=wait_strategy,
        retry=retry_if_exception_type(tuple(config.retry_on_exceptions)),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING),
        reraise=True
    )


def load_retry_config(policy_name: str) -> RetryConfig:
    """Load retry config dataclass from YAML policies."""
    raw = _RAW_RETRY_POLICIES.get(policy_name)
    if raw is None:
        raise ValueError(f"Retry policy '{policy_name}' not found in config")
    return RetryConfig(
        max_attempts=raw.get("max_attempts", RetryConfig().max_attempts),
        base_delay=raw.get("base_delay", RetryConfig().base_delay),
        max_delay=raw.get("max_delay", RetryConfig().max_delay),
        exponential_base=raw.get("exponential_base", RetryConfig().exponential_base),
        jitter=raw.get("jitter", RetryConfig().jitter)
    )


def retry_with_policy(policy_name: str) -> Callable:
    """Decorator using named retry policy from agents/config."""
    return create_retry_decorator(load_retry_config(policy_name))
