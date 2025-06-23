"""
Comprehensive test suite for OPIK integration

Tests the enhanced OPIK integration, instrumented DSPy modules,
admin UI components, and backend bridge functionality.
"""

import os
import json
import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

# Test imports
import sys
sys.path.append('.')

from common_tools.enhanced_opik_integration import (
    OpikIntegrationManager,
    OpikConfig,
    ThreatIntelTraceMetrics,
    get_opik_manager,
    trace_threat_analysis,
    OPIK_AVAILABLE
)

from common_tools.instrumented_dspy_modules import (
    InstrumentedSecurityThreatAnalyzer,
    InstrumentedAPTAttributionAnalyzer,
    InstrumentedThreatIntelExtractor,
    InstrumentedAdvancedThreatAnalyzer,
    create_instrumented_threat_analyzer,
    setup_instrumented_dspy_pipeline
)

from common_tools.opik_admin_ui import (
    OpikAdminInterface,
    get_admin_interface
)

from common_tools.backend_opik_integration import (
    BackendOpikBridge,
    get_backend_bridge,
    get_opik_status_json,
    trace_graph_query_json
)

class TestOpikConfig:
    """Test OPIK configuration management"""
    
    def test_default_config(self):
        """Test default configuration values"""
        config = OpikConfig()
        
        assert config.enabled is True
        assert config.use_local is True
        assert config.project_name == "Umbrix-ThreatIntel"
        assert config.log_graph is True
        assert config.auto_instrument_dspy is True
        assert config.trace_sampling_rate == 1.0
        assert config.admin_ui_enabled is True
        assert config.quality_threshold == 0.8
    
    def test_config_from_env(self, monkeypatch):
        """Test configuration loading from environment variables"""
        # Set environment variables
        monkeypatch.setenv("OPIK_ENABLED", "false")
        monkeypatch.setenv("OPIK_PROJECT_NAME", "test-project")
        monkeypatch.setenv("OPIK_TRACE_SAMPLING_RATE", "0.5")
        monkeypatch.setenv("OPIK_QUALITY_THRESHOLD", "0.9")
        
        # Import and test config loading
        from common_tools.enhanced_opik_integration import OpikIntegrationManager
        config = OpikIntegrationManager._load_config_from_env()
        
        assert config.enabled is False
        assert config.project_name == "test-project"
        assert config.trace_sampling_rate == 0.5
        assert config.quality_threshold == 0.9

class TestOpikIntegrationManager:
    """Test the main OPIK integration manager"""
    
    def test_initialization_disabled(self):
        """Test manager initialization when OPIK is disabled"""
        config = OpikConfig(enabled=False)
        manager = OpikIntegrationManager(config)
        
        assert manager.config.enabled is False
        assert manager._initialized is False
        assert manager.client is None
        assert manager.dspy_callback is None
    
    @patch('common_tools.enhanced_opik_integration.OPIK_AVAILABLE', False)
    def test_initialization_opik_unavailable(self):
        """Test manager initialization when OPIK package is unavailable"""
        config = OpikConfig(enabled=True)
        manager = OpikIntegrationManager(config)
        
        assert manager._initialized is False
        assert manager.client is None
    
    @patch('common_tools.enhanced_opik_integration.OPIK_AVAILABLE', True)
    @patch('common_tools.enhanced_opik_integration.opik')
    @patch('common_tools.enhanced_opik_integration.Opik')
    @patch('common_tools.enhanced_opik_integration.OpikCallback')
    def test_initialization_success(self, mock_callback, mock_opik_client, mock_opik):
        """Test successful manager initialization"""
        config = OpikConfig(enabled=True, use_local=True)
        
        # Mock the OPIK components
        mock_client_instance = Mock()
        mock_opik_client.return_value = mock_client_instance
        mock_callback_instance = Mock()
        mock_callback.return_value = mock_callback_instance
        
        manager = OpikIntegrationManager(config)
        
        # Verify initialization
        mock_opik.configure.assert_called_once_with(use_local=True)
        mock_opik_client.assert_called_once_with(project_name="Umbrix-ThreatIntel")
        assert manager._initialized is True
        assert manager.client == mock_client_instance
        assert manager.dspy_callback == mock_callback_instance
    
    def test_health_check_disabled(self):
        """Test health check when OPIK is disabled"""
        config = OpikConfig(enabled=False)
        manager = OpikIntegrationManager(config)
        
        health = manager.health_check()
        
        assert health["integration_enabled"] is False
        assert health["initialized"] is False
        assert health["dspy_instrumented"] is False
    
    def test_trace_threat_pipeline_disabled(self):
        """Test trace context manager when OPIK is disabled"""
        config = OpikConfig(enabled=False)
        manager = OpikIntegrationManager(config)
        
        with manager.trace_threat_pipeline("test", {"input": "data"}) as trace:
            assert trace is None
    
    def test_sanitize_trace_data(self):
        """Test trace data sanitization"""
        config = OpikConfig(enabled=False)  # Don't need full initialization
        manager = OpikIntegrationManager(config)
        
        # Test large string truncation
        large_string = "x" * 2000
        sanitized = manager._sanitize_trace_data(large_string)
        assert len(sanitized) < len(large_string)
        assert "truncated" in sanitized
        
        # Test large content field truncation
        large_content = {"content": "y" * 2000, "other": "data"}
        sanitized = manager._sanitize_trace_data(large_content)
        assert len(sanitized["content"]) < 2000
        assert "truncated" in sanitized["content"]
        assert sanitized["other"] == "data"
        
        # Test list truncation
        large_list = list(range(200))
        sanitized = manager._sanitize_trace_data(large_list)
        assert len(sanitized) < 200
        assert "truncated" in str(sanitized)

class TestThreatIntelTraceMetrics:
    """Test threat intelligence metrics data class"""
    
    def test_metrics_creation(self):
        """Test metrics creation and serialization"""
        metrics = ThreatIntelTraceMetrics(
            processing_time_ms=1500.5,
            token_count=1250,
            cost_usd=0.025,
            accuracy_score=0.87,
            confidence_score=0.92,
            ioc_count=5,
            threat_level="high",
            data_source="rss_feed"
        )
        
        assert metrics.processing_time_ms == 1500.5
        assert metrics.token_count == 1250
        assert metrics.cost_usd == 0.025
        assert metrics.accuracy_score == 0.87
        assert metrics.confidence_score == 0.92
        assert metrics.ioc_count == 5
        assert metrics.threat_level == "high"
        assert metrics.data_source == "rss_feed"
    
    def test_metrics_defaults(self):
        """Test metrics with default values"""
        metrics = ThreatIntelTraceMetrics(
            processing_time_ms=100.0,
            token_count=50,
            cost_usd=0.01
        )
        
        assert metrics.accuracy_score is None
        assert metrics.confidence_score is None
        assert metrics.ioc_count == 0
        assert metrics.false_positive_rate is None
        assert metrics.threat_level is None
        assert metrics.data_source is None

class TestInstrumentedDSPyModules:
    """Test instrumented DSPy modules"""
    
    @patch('common_tools.instrumented_dspy_modules.get_opik_manager')
    def test_security_threat_analyzer_creation(self, mock_get_manager):
        """Test creating instrumented security threat analyzer"""
        mock_manager = Mock()
        mock_get_manager.return_value = mock_manager
        
        analyzer = InstrumentedSecurityThreatAnalyzer()
        
        assert analyzer.opik_manager == mock_manager
        assert hasattr(analyzer, 'analyze')
    
    @patch('common_tools.instrumented_dspy_modules.get_opik_manager')
    def test_apt_attribution_analyzer_creation(self, mock_get_manager):
        """Test creating instrumented APT attribution analyzer"""
        mock_manager = Mock()
        mock_get_manager.return_value = mock_manager
        
        analyzer = InstrumentedAPTAttributionAnalyzer()
        
        assert analyzer.opik_manager == mock_manager
        assert hasattr(analyzer, 'analyze')
    
    @patch('common_tools.instrumented_dspy_modules.get_opik_manager')
    def test_threat_intel_extractor_creation(self, mock_get_manager):
        """Test creating instrumented threat intel extractor"""
        mock_manager = Mock()
        mock_get_manager.return_value = mock_manager
        
        extractor = InstrumentedThreatIntelExtractor()
        
        assert extractor.opik_manager == mock_manager
        assert hasattr(extractor, 'extract')
    
    def test_ioc_validation(self):
        """Test IOC validation logic"""
        with patch('common_tools.instrumented_dspy_modules.get_opik_manager'):
            extractor = InstrumentedThreatIntelExtractor()
            
            # Test valid IOCs
            assert extractor._is_valid_ioc("192.168.1.1") is True
            assert extractor._is_valid_ioc("example.com") is True
            assert extractor._is_valid_ioc("a1b2c3d4e5f6789012345678901234567890abcd") is True
            
            # Test invalid IOCs
            assert extractor._is_valid_ioc("not-an-ioc") is False
            assert extractor._is_valid_ioc("") is False
            assert extractor._is_valid_ioc("123") is False
    
    def test_create_instrumented_analyzer_factory(self):
        """Test the analyzer factory function"""
        with patch('common_tools.instrumented_dspy_modules.get_opik_manager'):
            # Test valid analyzer types
            security_analyzer = create_instrumented_threat_analyzer("security")
            assert isinstance(security_analyzer, InstrumentedSecurityThreatAnalyzer)
            
            apt_analyzer = create_instrumented_threat_analyzer("apt")
            assert isinstance(apt_analyzer, InstrumentedAPTAttributionAnalyzer)
            
            intel_analyzer = create_instrumented_threat_analyzer("intel")
            assert isinstance(intel_analyzer, InstrumentedThreatIntelExtractor)
            
            comprehensive_analyzer = create_instrumented_threat_analyzer("comprehensive")
            assert isinstance(comprehensive_analyzer, InstrumentedAdvancedThreatAnalyzer)
            
            # Test invalid analyzer type
            with pytest.raises(ValueError):
                create_instrumented_threat_analyzer("invalid")

class TestOpikAdminInterface:
    """Test OPIK admin interface"""
    
    @patch('common_tools.opik_admin_ui.get_opik_manager')
    def test_admin_interface_creation(self, mock_get_manager):
        """Test admin interface initialization"""
        mock_manager = Mock()
        mock_get_manager.return_value = mock_manager
        
        admin = OpikAdminInterface()
        assert admin.opik_manager == mock_manager
    
    @patch('common_tools.opik_admin_ui.get_opik_manager')
    def test_dashboard_info_healthy(self, mock_get_manager):
        """Test dashboard info when OPIK is healthy"""
        mock_manager = Mock()
        mock_manager.health_check.return_value = {
            "integration_enabled": True,
            "initialized": True,
            "dashboard_url": "http://localhost:5173",
            "project_name": "test-project",
            "dspy_instrumented": True
        }
        mock_get_manager.return_value = mock_manager
        
        admin = OpikAdminInterface()
        info = admin.get_dashboard_info()
        
        assert info["available"] is True
        assert info["enabled"] is True
        assert info["initialized"] is True
        assert info["dashboard_url"] == "http://localhost:5173"
        assert info["project_name"] == "test-project"
        assert info["status"] == "healthy"
    
    @patch('common_tools.opik_admin_ui.get_opik_manager')
    def test_dashboard_info_degraded(self, mock_get_manager):
        """Test dashboard info when OPIK is degraded"""
        mock_manager = Mock()
        mock_manager.health_check.return_value = {
            "integration_enabled": True,
            "initialized": False,
            "dashboard_url": None,
            "project_name": "test-project",
            "dspy_instrumented": False
        }
        mock_get_manager.return_value = mock_manager
        
        admin = OpikAdminInterface()
        info = admin.get_dashboard_info()
        
        assert info["enabled"] is True
        assert info["initialized"] is False
        assert info["status"] == "degraded"
    
    @patch('common_tools.opik_admin_ui.get_opik_manager')
    def test_recent_traces_summary(self, mock_get_manager):
        """Test recent traces summary generation"""
        mock_manager = Mock()
        mock_manager.config.enabled = True
        mock_get_manager.return_value = mock_manager
        
        admin = OpikAdminInterface()
        summary = admin.get_recent_traces_summary(hours=24)
        
        assert "time_range_hours" in summary
        assert summary["time_range_hours"] == 24
        assert "total_traces" in summary
        assert "successful_traces" in summary
        assert "failed_traces" in summary
        assert "avg_processing_time_ms" in summary
        assert "error_rate" in summary
        assert "top_operations" in summary
    
    @patch('common_tools.opik_admin_ui.get_opik_manager')
    def test_health_report_generation(self, mock_get_manager):
        """Test comprehensive health report generation"""
        mock_manager = Mock()
        mock_manager.config.enabled = True
        mock_manager.health_check.return_value = {
            "integration_enabled": True,
            "initialized": True,
            "dspy_instrumented": True
        }
        mock_get_manager.return_value = mock_manager
        
        admin = OpikAdminInterface()
        report = admin.generate_health_report()
        
        assert "timestamp" in report
        assert "overall_health" in report
        assert "dashboard_info" in report
        assert "system_metrics" in report
        assert "recent_activity" in report
        assert "recommendations" in report
        
        overall_health = report["overall_health"]
        assert "status" in overall_health
        assert "score" in overall_health
        assert overall_health["score"] >= 0

class TestBackendOpikBridge:
    """Test backend integration bridge"""
    
    @patch('common_tools.backend_opik_integration.get_opik_manager')
    @patch('common_tools.backend_opik_integration.get_admin_interface')
    def test_bridge_creation(self, mock_get_admin, mock_get_manager):
        """Test backend bridge initialization"""
        mock_manager = Mock()
        mock_admin = Mock()
        mock_get_manager.return_value = mock_manager
        mock_get_admin.return_value = mock_admin
        
        bridge = BackendOpikBridge()
        
        assert bridge.opik_manager == mock_manager
        assert bridge.admin_interface == mock_admin
    
    @patch('common_tools.backend_opik_integration.get_opik_manager')
    @patch('common_tools.backend_opik_integration.get_admin_interface')
    def test_observability_status(self, mock_get_admin, mock_get_manager):
        """Test getting observability status"""
        # Setup mocks
        mock_manager = Mock()
        mock_manager.config.enabled = True
        mock_manager._initialized = True
        mock_manager.config.auto_instrument_dspy = True
        mock_manager.config.trace_sampling_rate = 1.0
        mock_manager.config.project_name = "test-project"
        mock_manager.dspy_callback = Mock()
        mock_manager.get_dashboard_url.return_value = "http://localhost:5173"
        
        mock_admin = Mock()
        mock_admin.generate_health_report.return_value = {
            "overall_health": {"score": 85, "status": "healthy"},
            "recent_activity": {"error_rate": 5.2}
        }
        
        mock_get_manager.return_value = mock_manager
        mock_get_admin.return_value = mock_admin
        
        bridge = BackendOpikBridge()
        status = bridge.get_observability_status()
        
        assert "opik_integration" in status
        assert "dspy_instrumentation" in status
        assert "recent_metrics" in status
        assert "timestamp" in status
        
        opik_integration = status["opik_integration"]
        assert opik_integration["enabled"] is True
        assert opik_integration["initialized"] is True
        assert opik_integration["health_score"] == 85
        assert opik_integration["status"] == "healthy"
    
    @patch('common_tools.backend_opik_integration.get_opik_manager')
    @patch('common_tools.backend_opik_integration.get_admin_interface')
    def test_trace_graph_query(self, mock_get_admin, mock_get_manager):
        """Test tracing graph query execution"""
        # Setup mock manager
        mock_manager = Mock()
        mock_manager.config.enabled = True
        
        # Create mock trace context
        mock_trace = Mock()
        mock_span = Mock()
        mock_manager.trace_threat_pipeline.return_value.__enter__.return_value = mock_trace
        mock_manager.create_span.return_value = mock_span
        
        mock_get_manager.return_value = mock_manager
        mock_get_admin.return_value = Mock()
        
        bridge = BackendOpikBridge()
        result = bridge.trace_intelligent_graph_query(
            query="MATCH (n:Source) RETURN n",
            query_type="cypher",
            result_count=5,
            processing_time_ms=150.5,
            success=True
        )
        
        assert "traced:" in result
        mock_manager.trace_threat_pipeline.assert_called_once()
        mock_manager.create_span.assert_called_once()
        mock_span.end.assert_called_once()
    
    def test_json_interface_functions(self):
        """Test JSON interface functions for Rust backend"""
        # Test get_opik_status_json
        status_json = get_opik_status_json()
        status_data = json.loads(status_json)
        assert isinstance(status_data, dict)
        
        # Test trace_graph_query_json
        trace_json = trace_graph_query_json(
            query="MATCH (n) RETURN n",
            query_type="cypher",
            result_count=10,
            processing_time_ms=200.0,
            success=True
        )
        trace_data = json.loads(trace_json)
        assert "status" in trace_data
        
        # Test get_admin_dashboard_json
        dashboard_json = get_admin_dashboard_json()
        dashboard_data = json.loads(dashboard_json)
        assert isinstance(dashboard_data, dict)

class TestTraceDecorators:
    """Test the trace decorators"""
    
    @patch('common_tools.enhanced_opik_integration.get_opik_manager')
    def test_trace_threat_analysis_decorator(self, mock_get_manager):
        """Test the trace_threat_analysis decorator"""
        mock_manager = Mock()
        mock_manager.config.enabled = True
        mock_trace = Mock()
        mock_manager.trace_threat_pipeline.return_value.__enter__.return_value = mock_trace
        mock_get_manager.return_value = mock_manager
        
        @trace_threat_analysis(name="test_function", tags=["test"])
        def sample_function(input_data: str) -> str:
            return f"processed: {input_data}"
        
        result = sample_function("test input")
        
        assert result == "processed: test input"
        mock_manager.trace_threat_pipeline.assert_called_once()
    
    @patch('common_tools.enhanced_opik_integration.get_opik_manager')
    def test_trace_decorator_with_disabled_opik(self, mock_get_manager):
        """Test decorator behavior when OPIK is disabled"""
        mock_manager = Mock()
        mock_manager.config.enabled = False
        mock_get_manager.return_value = mock_manager
        
        @trace_threat_analysis(name="test_function")
        def sample_function(input_data: str) -> str:
            return f"processed: {input_data}"
        
        result = sample_function("test input")
        
        assert result == "processed: test input"
        # Should not call trace_threat_pipeline when disabled
        mock_manager.trace_threat_pipeline.assert_not_called()

class TestIntegrationScenarios:
    """Test integration scenarios"""
    
    @patch('common_tools.enhanced_opik_integration.OPIK_AVAILABLE', True)
    @patch('common_tools.enhanced_opik_integration.opik')
    @patch('common_tools.enhanced_opik_integration.Opik')
    @patch('common_tools.enhanced_opik_integration.OpikCallback')
    def test_full_integration_setup(self, mock_callback, mock_opik_client, mock_opik):
        """Test full integration setup with mocked OPIK"""
        # Mock OPIK components
        mock_client_instance = Mock()
        mock_opik_client.return_value = mock_client_instance
        mock_callback_instance = Mock()
        mock_callback.return_value = mock_callback_instance
        mock_lm = Mock()
        
        # Test setup function
        result = setup_instrumented_dspy_pipeline(
            lm=mock_lm,
            analyzer_types=["security", "intel"],
            opik_config={"project_name": "test-integration"}
        )
        
        assert "analyzers" in result
        assert "opik_manager" in result
        assert "dashboard_url" in result
        assert "health_status" in result
        
        analyzers = result["analyzers"]
        assert "security" in analyzers
        assert "intel" in analyzers
        assert isinstance(analyzers["security"], InstrumentedSecurityThreatAnalyzer)
        assert isinstance(analyzers["intel"], InstrumentedThreatIntelExtractor)
    
    def test_integration_without_opik(self):
        """Test integration graceful degradation without OPIK"""
        with patch('common_tools.enhanced_opik_integration.OPIK_AVAILABLE', False):
            # Should still work but with limited functionality
            manager = get_opik_manager()
            
            assert manager.config.enabled is True  # Config can still be enabled
            assert manager._initialized is False   # But won't initialize
            
            # Trace context should be no-op
            with manager.trace_threat_pipeline("test", {}) as trace:
                assert trace is None

if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "--tb=short"])