"""
Unit tests for Umbrix metrics framework and HTTP endpoint functionality.

Tests the agents/common_tools/metrics.py module including:
- UmbrixMetrics initialization and standard metrics
- Counter, histogram, and gauge operations  
- HTTP metrics endpoint functionality
- Thread safety and concurrent access
- Custom metrics creation
- Integration with agent workflows
"""

import pytest
import time
import threading
import requests
import sys
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor, as_completed
from prometheus_client import CollectorRegistry, generate_latest

from agents.common_tools.metrics import (
    UmbrixMetrics,
    MetricsTimer,
    ExternalCallTimer,
    setup_agent_metrics,
    start_metrics_server,
    get_global_registry,
    timed_method,
    timed_external_call,
    time_function,
    count_errors
)

# Import conftest clearing function for proper isolation
from conftest import _clear_metrics


@pytest.fixture(autouse=True)
def restore_prometheus_client():
    """Ensure prometheus_client is properly restored for metrics tests."""
    # Store original prometheus_client state
    original_prometheus = sys.modules.get('prometheus_client')
    
    # Force import real prometheus_client and patch the test module's imports
    try:
        # Remove any existing stub
        if 'prometheus_client' in sys.modules:
            del sys.modules['prometheus_client']
        
        # Import fresh prometheus_client
        import prometheus_client
        sys.modules['prometheus_client'] = prometheus_client
        
        # Also update any modules that might have cached the old prometheus_client
        if 'agents.common_tools.metrics' in sys.modules:
            metrics_module = sys.modules['agents.common_tools.metrics']
            # Force the module to re-import prometheus_client
            if hasattr(metrics_module, 'prometheus_client'):
                metrics_module.prometheus_client = prometheus_client
            # Update the specific imports too
            metrics_module.Counter = prometheus_client.Counter
            metrics_module.Histogram = prometheus_client.Histogram  
            metrics_module.Gauge = prometheus_client.Gauge
            metrics_module.CollectorRegistry = prometheus_client.CollectorRegistry
        
        # Update the test module's imports to use real prometheus_client
        current_module = sys.modules[__name__]
        current_module.CollectorRegistry = prometheus_client.CollectorRegistry
        current_module.generate_latest = prometheus_client.generate_latest
        
        # Also ensure the imported functions from metrics module use real prometheus_client
        current_module.setup_agent_metrics = sys.modules['agents.common_tools.metrics'].setup_agent_metrics
            
    except ImportError:
        # If prometheus_client is not available, restore the original
        if original_prometheus is not None:
            sys.modules['prometheus_client'] = original_prometheus
    
    yield
    
    # Restore original state if it was different
    if original_prometheus is not None and sys.modules.get('prometheus_client') is not original_prometheus:
        sys.modules['prometheus_client'] = original_prometheus


class TestUmbrixMetrics:
    """Test suite for UmbrixMetrics class functionality."""
    
    def test_metrics_initialization(self):
        """Test basic metrics initialization with default metrics."""
        metrics = UmbrixMetrics("test_agent")
        
        assert metrics.agent_name == "test_agent"
        assert metrics.registry is not None
        
        # Check that standard metrics are created
        assert metrics.messages_consumed is not None
        assert metrics.messages_produced is not None
        assert metrics.errors_total is not None
        assert metrics.processing_latency is not None
        assert metrics.external_call_duration is not None
        assert metrics.internal_queue_size is not None
        assert metrics.agent_info is not None
    
    def test_custom_registry(self):
        """Test metrics initialization with custom registry."""
        custom_registry = CollectorRegistry()
        metrics = UmbrixMetrics("test_agent", registry=custom_registry)
        
        assert metrics.registry is custom_registry
        
        # Verify metrics are registered in custom registry
        metric_families = list(custom_registry.collect())
        metric_names = [mf.name for mf in metric_families]
        
        assert "messages_consumed" in metric_names
        assert "errors" in metric_names
    
    def test_increment_operations(self):
        """Test counter increment operations."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("test_agent", registry=registry)
        
        # Test message counters
        metrics.increment_messages_consumed("test_topic", 5)
        metrics.increment_messages_produced("output_topic", 3)
        metrics.increment_error("validation_error", 2)
        
        # Verify metrics by checking registry output
        output = generate_latest(registry).decode('utf-8')
        
        assert 'messages_consumed_total{agent="test_agent",topic="test_topic"} 5.0' in output
        assert 'messages_produced_total{agent="test_agent",topic="output_topic"} 3.0' in output
        assert 'errors_total{agent="test_agent",error_type="validation_error"} 2.0' in output
    
    def test_observe_operations(self):
        """Test histogram observation operations."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("test_agent", registry=registry)
        
        # Test latency observations
        metrics.observe_processing_latency("message_processing", 0.15)
        metrics.observe_processing_latency("data_validation", 0.05)
        
        # Test external call observations
        metrics.observe_external_call("kafka", "produce", 0.08)
        metrics.observe_external_call("neo4j", "query", 0.25)
        
        # Verify histograms are recorded
        output = generate_latest(registry).decode('utf-8')
        
        assert "processing_latency_seconds" in output
        assert "external_call_duration_seconds" in output
        assert "message_processing" in output
        assert "kafka" in output
        assert "neo4j" in output
    
    def test_gauge_operations(self):
        """Test gauge set operations."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("test_agent", registry=registry)
        
        # Test queue size gauge
        metrics.set_queue_size("pending_tasks", 42)
        metrics.set_queue_size("processed_items", 123)
        
        # Verify gauge values
        output = generate_latest(registry).decode('utf-8')
        
        assert 'internal_queue_size{agent="test_agent",queue_name="pending_tasks"} 42.0' in output
        assert 'internal_queue_size{agent="test_agent",queue_name="processed_items"} 123.0' in output
    
    def test_custom_metrics_creation(self):
        """Test creation of custom metrics."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("test_agent", registry=registry)
        
        # Create custom counter
        custom_counter = metrics.create_custom_counter(
            "custom_operations", 
            "Custom operations counter", 
            ["operation_type"]
        )
        custom_counter.labels(operation_type="test_op").inc(5)
        
        # Create custom histogram  
        custom_histogram = metrics.create_custom_histogram(
            "custom_duration",
            "Custom duration histogram",
            ["stage"]
        )
        custom_histogram.labels(stage="custom_stage").observe(1.5)
        
        # Create custom gauge
        custom_gauge = metrics.create_custom_gauge(
            "custom_size",
            "Custom size gauge", 
            ["resource"]
        )
        custom_gauge.labels(resource="memory").set(2048)
        
        # Verify custom metrics
        output = generate_latest(registry).decode('utf-8')
        
        assert "test_agent_custom_operations" in output
        assert "test_agent_custom_duration" in output
        assert "test_agent_custom_size" in output
        assert "test_op" in output
        assert "custom_stage" in output
        assert "memory" in output


class TestMetricsTimers:
    """Test suite for timing utilities."""
    
    def test_metrics_timer_context_manager(self):
        """Test MetricsTimer context manager."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("test_agent", registry=registry)
        
        # Use timer context manager
        with MetricsTimer(metrics, "test_operation"):
            time.sleep(0.01)  # Simulate work
        
        # Verify timing was recorded
        output = generate_latest(registry).decode('utf-8')
        assert "processing_latency_seconds" in output
        assert "test_operation" in output
    
    def test_external_call_timer_context_manager(self):
        """Test ExternalCallTimer context manager."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("test_agent", registry=registry)
        
        # Use external call timer
        with ExternalCallTimer(metrics, "test_service", "test_endpoint"):
            time.sleep(0.01)  # Simulate external call
        
        # Verify external call timing was recorded
        output = generate_latest(registry).decode('utf-8')
        assert "external_call_duration_seconds" in output
        assert "test_service" in output
        assert "test_endpoint" in output
    
    def test_timed_method_decorator(self):
        """Test @timed_method decorator."""
        registry = CollectorRegistry()
        
        class TestClass:
            def __init__(self):
                self.metrics = UmbrixMetrics("test_agent", registry=registry)
            
            @timed_method("custom_stage")
            def test_method(self):
                time.sleep(0.01)
                return "result"
        
        test_obj = TestClass()
        result = test_obj.test_method()
        
        assert result == "result"
        
        # Verify timing was recorded
        output = generate_latest(registry).decode('utf-8')
        assert "processing_latency_seconds" in output
        assert "custom_stage" in output
    
    def test_timed_external_call_decorator(self):
        """Test @timed_external_call decorator."""
        registry = CollectorRegistry()
        
        class TestClass:
            def __init__(self):
                self.metrics = UmbrixMetrics("test_agent", registry=registry)
            
            @timed_external_call("external_api", "get_data")
            def api_call(self):
                time.sleep(0.01)
                return {"data": "value"}
        
        test_obj = TestClass()
        result = test_obj.api_call()
        
        assert result == {"data": "value"}
        
        # Verify external call timing was recorded
        output = generate_latest(registry).decode('utf-8')
        assert "external_call_duration_seconds" in output
        assert "external_api" in output
        assert "get_data" in output


class TestUtilityFunctions:
    """Test suite for utility functions."""
    
    def test_time_function_utility(self):
        """Test time_function utility."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("test_agent", registry=registry)
        
        def test_func():
            time.sleep(0.01)
            return "test_result"
        
        result = time_function(test_func, metrics, "test_stage")
        
        assert result == "test_result"
        
        # Verify timing was recorded
        output = generate_latest(registry).decode('utf-8')
        assert "processing_latency_seconds" in output
        assert "test_stage" in output
    
    def test_count_errors_utility_success(self):
        """Test count_errors utility with successful function."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("test_agent", registry=registry)
        
        def success_func():
            return "success"
        
        result = count_errors(success_func, metrics, "test_error")
        
        assert result == "success"
        
        # Verify no error was counted
        output = generate_latest(registry).decode('utf-8')
        assert 'errors_total{agent="test_agent",error_type="test_error"} 0.0' in output or \
               'errors_total{agent="test_agent",error_type="test_error"}' not in output
    
    def test_count_errors_utility_failure(self):
        """Test count_errors utility with failing function."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("test_agent", registry=registry)
        
        def failing_func():
            raise ValueError("Test error")
        
        with pytest.raises(ValueError, match="Test error"):
            count_errors(failing_func, metrics, "test_error")
        
        # Verify error was counted
        output = generate_latest(registry).decode('utf-8')
        assert 'errors_total{agent="test_agent",error_type="test_error"} 1.0' in output


class TestHTTPMetricsServer:
    """Test suite for HTTP metrics server functionality."""
    
    def test_setup_agent_metrics(self):
        """Test setup_agent_metrics function."""
        metrics = setup_agent_metrics("test_agent")
        
        assert metrics.agent_name == "test_agent"
        assert metrics.registry is not None
    
    def test_setup_agent_metrics_with_port(self):
        """Test setup_agent_metrics with HTTP server port."""
        # Use a high port number to avoid conflicts
        test_port = 19090
        
        # Mock start_metrics_server to avoid actually starting a server
        with patch('agents.common_tools.metrics.start_metrics_server') as mock_start:
            metrics = setup_agent_metrics("test_agent", metrics_port=test_port)
            
            assert metrics.agent_name == "test_agent"
            mock_start.assert_called_once_with(test_port, metrics.registry)
    
    @pytest.mark.integration
    def test_metrics_http_endpoint_integration(self):
        """Integration test for metrics HTTP endpoint (requires actual server)."""
        # Skip if we're using stubbed prometheus (detected by checking registry type)
        from prometheus_client import REGISTRY
        if hasattr(REGISTRY, '_names_to_collectors') and not REGISTRY._names_to_collectors:
            pytest.skip("Using stubbed prometheus_client - skipping integration test")
        
        # Use a high port number to avoid conflicts
        test_port = 19091
        
        try:
            # Clear any existing metrics to ensure clean state
            _clear_metrics()
            
            # Setup metrics with HTTP server using a custom registry to avoid pollution
            from prometheus_client import CollectorRegistry
            custom_registry = CollectorRegistry()
            
            # Create metrics with the custom registry
            from agents.common_tools.metrics import UmbrixMetrics
            metrics = UmbrixMetrics("integration_test", registry=custom_registry)
            
            # Start the metrics server with our custom registry
            start_metrics_server(test_port, custom_registry)
            
            # Add some sample metrics
            metrics.increment_messages_consumed("test_topic", 10)
            metrics.observe_processing_latency("test_stage", 0.1)
            
            # Give the server a moment to start
            time.sleep(0.1)
            
            # Make HTTP request to metrics endpoint
            response = requests.get(f"http://localhost:{test_port}/metrics", timeout=5)
            
            assert response.status_code == 200
            assert "text/plain" in response.headers.get("content-type", "")
            
            content = response.text
            
            # Skip test if we're getting mock output (due to stubbed prometheus_client)
            if "Mock prometheus metrics output" in content:
                pytest.skip("Test using stubbed prometheus_client - skipping integration test")
            
            assert "messages_consumed_total" in content
            assert "processing_latency_seconds" in content
            assert "integration_test" in content
            assert "test_topic" in content
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            pytest.skip("Could not connect to metrics server - may be a port conflict")
    
    @pytest.mark.integration  
    def test_health_endpoint_integration(self):
        """Integration test for health endpoint."""
        test_port = 19092
        
        try:
            # Setup metrics with HTTP server
            setup_agent_metrics("health_test", metrics_port=test_port)
            
            # Give the server a moment to start
            time.sleep(0.1)
            
            # Test health endpoint
            response = requests.get(f"http://localhost:{test_port}/health", timeout=5)
            
            assert response.status_code == 200
            assert response.text == "OK"
            
            # Test healthz endpoint (alias)
            response = requests.get(f"http://localhost:{test_port}/healthz", timeout=5)
            
            assert response.status_code == 200
            assert response.text == "OK"
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            pytest.skip("Could not connect to health endpoints - may be a port conflict")


class TestThreadSafety:
    """Test suite for thread safety and concurrent access."""
    
    def test_concurrent_counter_access(self):
        """Test concurrent access to counter metrics."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("concurrent_test", registry=registry)
        
        def increment_metrics(thread_id):
            for i in range(100):
                metrics.increment_messages_consumed(f"topic_{thread_id}", 1)
                metrics.increment_error(f"error_{thread_id}", 1)
        
        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=increment_metrics, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all metrics were recorded correctly
        output = generate_latest(registry).decode('utf-8')
        
        for i in range(5):
            assert f"topic_{i}" in output
            assert f"error_{i}" in output
    
    def test_concurrent_histogram_access(self):
        """Test concurrent access to histogram metrics."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("concurrent_test", registry=registry)
        
        def observe_metrics(thread_id):
            for i in range(50):
                metrics.observe_processing_latency(f"stage_{thread_id}", 0.01 * i)
                metrics.observe_external_call(f"service_{thread_id}", "endpoint", 0.02 * i)
        
        # Use ThreadPoolExecutor for better control
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(observe_metrics, i) for i in range(3)]
            
            # Wait for all tasks to complete
            for future in as_completed(futures):
                future.result()  # This will raise any exceptions
        
        # Verify histogram metrics were recorded
        output = generate_latest(registry).decode('utf-8')
        
        assert "processing_latency_seconds" in output
        assert "external_call_duration_seconds" in output
        
        for i in range(3):
            assert f"stage_{i}" in output
            assert f"service_{i}" in output
    
    def test_concurrent_gauge_access(self):
        """Test concurrent access to gauge metrics."""
        registry = CollectorRegistry()
        metrics = UmbrixMetrics("concurrent_test", registry=registry)
        
        def set_gauge_metrics(thread_id):
            for i in range(20):
                metrics.set_queue_size(f"queue_{thread_id}", thread_id * 100 + i)
        
        # Start multiple threads
        threads = []
        for i in range(4):
            thread = threading.Thread(target=set_gauge_metrics, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify gauge metrics were set (final values may vary due to concurrency)
        output = generate_latest(registry).decode('utf-8')
        
        for i in range(4):
            assert f"queue_{i}" in output


class TestMetricsRegistryManagement:
    """Test suite for metrics registry management."""
    
    def test_global_registry_access(self):
        """Test global registry access."""
        registry1 = get_global_registry()
        registry2 = get_global_registry()
        
        # Should return the same instance
        assert registry1 is registry2
    
    def test_metrics_isolation_with_custom_registry(self):
        """Test that metrics with custom registries are isolated."""
        registry1 = CollectorRegistry()
        registry2 = CollectorRegistry()
        
        metrics1 = UmbrixMetrics("agent1", registry=registry1)
        metrics2 = UmbrixMetrics("agent2", registry=registry2)
        
        # Add different metrics to each
        metrics1.increment_messages_consumed("topic1", 10)
        metrics2.increment_messages_consumed("topic2", 20)
        
        # Verify isolation
        output1 = generate_latest(registry1).decode('utf-8')
        output2 = generate_latest(registry2).decode('utf-8')
        
        assert "topic1" in output1
        assert "topic1" not in output2
        
        assert "topic2" in output2
        assert "topic2" not in output1
        
        assert "agent1" in output1
        assert "agent1" not in output2
        
        assert "agent2" in output2
        assert "agent2" not in output1


if __name__ == "__main__":
    # Run basic smoke tests if executed directly
    print("Running basic metrics smoke tests...")
    
    # Test basic initialization
    metrics = UmbrixMetrics("smoke_test")
    print("✓ Metrics initialization")
    
    # Test basic operations
    metrics.increment_messages_consumed("test_topic", 1)
    metrics.observe_processing_latency("test_stage", 0.1)
    metrics.set_queue_size("test_queue", 5)
    print("✓ Basic metric operations")
    
    # Test output generation
    output = generate_latest(metrics.registry).decode('utf-8')
    assert "messages_consumed_total" in output
    assert "smoke_test" in output
    print("✓ Metrics output generation")
    
    print("All smoke tests passed! ✓")