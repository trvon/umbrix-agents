"""
Pytest fixtures for test setup and teardown.
"""

import pytest
import sys
import os

# Add the parent directory to the path so we can import conftest
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from conftest import _clear_metrics

@pytest.fixture(autouse=False)  # Changed to False to prevent automatic clearing
def clear_metrics():
    """Clear metrics before each test when explicitly requested."""
    _clear_metrics()
    yield
    _clear_metrics()

@pytest.fixture(autouse=True)
def isolate_metrics():
    """Ensure metrics are properly isolated between tests."""
    # Store original module state
    original_modules = {}
    
    # Store any modules that might need cleanup
    modules_to_track = [
        'common_tools.kafka_wrapper',
        'common_tools.retry_framework'
    ]
    
    for module_name in modules_to_track:
        if module_name in sys.modules:
            original_modules[module_name] = sys.modules[module_name]
    
    yield
    
    # After test, reinitialize metrics in any tracked modules
    try:
        # Reinitialize kafka_wrapper metrics if module exists
        if 'common_tools.kafka_wrapper' in sys.modules:
            kafka_module = sys.modules['common_tools.kafka_wrapper']
            if hasattr(kafka_module, '_get_or_create_metrics'):
                # Force reinitialize
                kafka_module.VALIDATION_ERRORS, kafka_module.VALIDATION_LATENCY, kafka_module.DLQ_MESSAGES = kafka_module._get_or_create_metrics()
    
        # Reinitialize retry_framework metrics if module exists  
        if 'common_tools.retry_framework' in sys.modules:
            retry_module = sys.modules['common_tools.retry_framework']
            if hasattr(retry_module, '_get_or_create_metrics'):
                # Force reinitialize
                retry_module._metrics = retry_module._get_or_create_metrics()
    except Exception:
        # If reinitializing fails, that's ok - lazy initialization will handle it
        pass
