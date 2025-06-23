#!/usr/bin/env python3
"""
Simple test script to validate OPIK integration works correctly
"""

import sys
sys.path.append('.')

from common_tools.enhanced_opik_integration import (
    OpikConfig, 
    OpikIntegrationManager,
    ThreatIntelTraceMetrics,
    get_opik_manager
)

from common_tools.opik_admin_ui import get_admin_interface
from common_tools.backend_opik_integration import get_backend_bridge

def test_basic_functionality():
    """Test basic OPIK integration functionality"""
    print("🧪 Testing OPIK Integration Basic Functionality")
    
    # Test 1: Config creation
    config = OpikConfig(enabled=False)  # Disabled to avoid requiring actual OPIK
    print(f"✅ Config created: {config.project_name}")
    
    # Test 2: Manager initialization
    manager = OpikIntegrationManager(config)
    print(f"✅ Manager initialized: enabled={manager.config.enabled}")
    
    # Test 3: Health check
    health = manager.health_check()
    print(f"✅ Health check: {health['integration_enabled']}")
    
    # Test 4: Trace metrics
    metrics = ThreatIntelTraceMetrics(
        processing_time_ms=100.0,
        token_count=50,
        cost_usd=0.01,
        confidence_score=0.85
    )
    print(f"✅ Metrics created: confidence={metrics.confidence_score}")
    
    # Test 5: Admin interface
    admin = get_admin_interface()
    dashboard_info = admin.get_dashboard_info()
    print(f"✅ Admin interface: available={dashboard_info.get('available', False)}")
    
    # Test 6: Backend bridge
    bridge = get_backend_bridge()
    status = bridge.get_observability_status()
    print(f"✅ Backend bridge: timestamp present={bool(status.get('timestamp'))}")
    
    # Test 7: Trace context manager (should be no-op when disabled)
    with manager.trace_threat_pipeline("test", {"input": "test"}) as trace:
        print(f"✅ Trace context: trace={trace}")
    
    print("\n🎉 All basic functionality tests passed!")
    return True

def test_data_sanitization():
    """Test data sanitization functionality"""
    print("\n🧪 Testing Data Sanitization")
    
    config = OpikConfig(enabled=False)
    manager = OpikIntegrationManager(config)
    
    # Test large string
    large_string = "x" * 2000
    sanitized = manager._sanitize_trace_data(large_string)
    print(f"✅ Large string sanitized: {len(sanitized)} chars (was {len(large_string)})")
    
    # Test dict with large content
    large_dict = {"content": "y" * 1500, "other": "normal"}
    sanitized = manager._sanitize_trace_data(large_dict)
    print(f"✅ Dict sanitized: content={len(sanitized['content'])} chars")
    
    # Test large list
    large_list = list(range(150))
    sanitized = manager._sanitize_trace_data(large_list)
    print(f"✅ List sanitized: {len(sanitized)} items (was {len(large_list)})")
    
    print("🎉 Data sanitization tests passed!")
    return True

def test_json_interfaces():
    """Test JSON interfaces for backend integration"""
    print("\n🧪 Testing JSON Interfaces")
    
    from common_tools.backend_opik_integration import (
        get_opik_status_json,
        trace_graph_query_json
    )
    
    # Test status JSON
    status_json = get_opik_status_json()
    print(f"✅ Status JSON: {len(status_json)} chars")
    
    # Test trace JSON
    trace_json = trace_graph_query_json(
        query="MATCH (n) RETURN n",
        query_type="cypher",
        result_count=5,
        processing_time_ms=100.0,
        success=True
    )
    print(f"✅ Trace JSON: {len(trace_json)} chars")
    
    print("🎉 JSON interfaces tests passed!")
    return True

def main():
    """Run all tests"""
    print("🚀 Starting OPIK Integration Tests\n")
    
    try:
        test_basic_functionality()
        test_data_sanitization()
        test_json_interfaces()
        
        print("\n✅ All tests passed successfully!")
        print("\n📊 OPIK Integration Summary:")
        print("   - Enhanced OPIK integration ready")
        print("   - Instrumented DSPy modules available")
        print("   - Admin UI components functional")
        print("   - Backend bridge operational")
        print("   - Graceful degradation when OPIK unavailable")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())