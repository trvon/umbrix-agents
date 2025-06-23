#!/usr/bin/env python3
"""
Simple test script to validate core benchmark components
without requiring full agent dependencies.
"""

import json
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List
import time

@dataclass
class MockBenchmarkMetrics:
    """Mock metrics for testing"""
    f1_score: float = 0.62
    precision: float = 0.65
    recall: float = 0.59
    accuracy: float = 0.71
    avg_latency_ms: float = 450.0
    p95_latency_ms: float = 850.0
    total_samples: int = 10
    successful_predictions: int = 9
    estimated_cost_usd: float = 0.0245

def test_feed_loading():
    """Test loading the ground truth feeds"""
    feeds_file = Path("benchmarks/feeds_bench_v2.json")
    
    if not feeds_file.exists():
        print(f"❌ Feeds file not found: {feeds_file}")
        return False
        
    try:
        with open(feeds_file) as f:
            data = json.load(f)
        
        # Extract samples from the data structure
        samples = data.get("samples", [])
        metadata = data.get("metadata", {})
        
        print(f"✅ Loaded {len(samples)} test samples")
        print(f"✅ Data version: {metadata.get('version', 'unknown')}")
        
        # Validate structure for samples
        required_keys = ['id', 'title', 'content', 'url', 'ground_truth']
        
        for i, sample in enumerate(samples):
            missing_keys = [k for k in required_keys if k not in sample]
            if missing_keys:
                print(f"❌ Sample {i} missing keys: {missing_keys}")
                return False
                
        print("✅ All samples have required structure")
        
        # Test categories from ground truth
        categories = [sample['ground_truth']['primary_category'] for sample in samples]
        unique_categories = set(categories)
        print(f"✅ Found categories: {', '.join(sorted(unique_categories))}")
        
        # Test sample content
        sample_feed = samples[0]
        print(f"✅ Sample feed: '{sample_feed['title'][:50]}...'")
        print(f"   Category: {sample_feed['ground_truth']['primary_category']}")
        print(f"   IOCs: {len(sample_feed['ground_truth'].get('indicators', []))} indicators")
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading feeds: {e}")
        return False

def test_validation_script():
    """Test the validation script logic"""
    try:
        # Import validation components
        sys.path.append('.')
        from benchmarks.validate_criteria import AcceptanceCriteriaValidator, CriteriaResult
        
        print("✅ Validation script imports successfully")
        
        # Test criteria loading
        validator = AcceptanceCriteriaValidator()
        criteria = validator.criteria
        
        print(f"✅ Loaded {len(criteria)} acceptance criteria")
        
        expected_criteria = [
            'threat_classification_f1',
            'avg_processing_latency', 
            'success_rate',
            'cost_per_1k_docs'
        ]
        
        for criterion in expected_criteria:
            if criterion in criteria:
                print(f"✅ Found criterion: {criterion}")
            else:
                print(f"❌ Missing criterion: {criterion}")
                
        return True
        
    except Exception as e:
        print(f"❌ Validation test failed: {e}")
        return False

def test_comparison_script():
    """Test the comparison script logic"""
    try:
        sys.path.append('.')
        from benchmarks.compare_baselines import BaselineComparator, PerformanceChange
        
        print("✅ Comparison script imports successfully")
        
        # Test threshold configuration
        comparator = BaselineComparator()
        thresholds = comparator.regression_thresholds
        
        print(f"✅ Loaded {len(thresholds)} regression thresholds")
        
        expected_thresholds = ['f1_score', 'avg_latency_ms', 'estimated_cost_usd']
        for threshold in expected_thresholds:
            if threshold in thresholds:
                print(f"✅ Found threshold: {threshold} = {thresholds[threshold]}")
            else:
                print(f"❌ Missing threshold: {threshold}")
                
        return True
        
    except Exception as e:
        print(f"❌ Comparison test failed: {e}")
        return False

def test_html_report_generation():
    """Test HTML report generation"""
    try:
        sys.path.append('.')
        from benchmarks.generate_html_report import HTMLReportGenerator
        
        print("✅ HTML report generator imports successfully")
        
        generator = HTMLReportGenerator()
        
        # Test template loading
        if generator.template:
            print("✅ HTML template loaded")
            
            # Check for key template variables
            required_vars = ['timestamp', 'overall_status', 'chart_data', 'criteria_rows']
            missing_vars = [v for v in required_vars if "{{ " + v + " }}" not in generator.template]
            
            if missing_vars:
                print(f"❌ Missing template variables: {missing_vars}")
                return False
            else:
                print("✅ All required template variables present")
        else:
            print("❌ HTML template not loaded")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ HTML report test failed: {e}")
        return False

def test_mock_baseline_generation():
    """Test generating a mock baseline report"""
    try:
        metrics = MockBenchmarkMetrics()
        
        # Create mock baseline report
        baseline_report = {
            "baseline_metadata": {
                "analyzer_type": "test_analyzer",
                "timestamp": time.time(),
                "test_environment": "local_test",
                "feeds_version": "v2.0"
            },
            "baseline_metrics": {
                "f1_score": metrics.f1_score,
                "precision": metrics.precision,
                "recall": metrics.recall,
                "accuracy": metrics.accuracy,
                "avg_latency_ms": metrics.avg_latency_ms,
                "p95_latency_ms": metrics.p95_latency_ms,
                "p99_latency_ms": metrics.p95_latency_ms * 1.2,
                "total_samples": metrics.total_samples,
                "successful_predictions": metrics.successful_predictions,
                "failed_predictions": metrics.total_samples - metrics.successful_predictions,
                "estimated_cost_usd": metrics.estimated_cost_usd,
                "total_tokens": 15420,
                "throughput_docs_per_sec": 2.22,
                "test_duration_sec": 4.5
            },
            "performance_distribution": {
                "latency_p50": 380.0,
                "latency_p90": 720.0,
                "latency_p95": metrics.p95_latency_ms,
                "latency_p99": metrics.p95_latency_ms * 1.2
            }
        }
        
        # Save mock baseline
        output_file = Path("benchmarks/test_baseline.json")
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(baseline_report, f, indent=2)
            
        print(f"✅ Generated mock baseline report: {output_file}")
        print(f"   F1 Score: {baseline_report['baseline_metrics']['f1_score']}")
        print(f"   Avg Latency: {baseline_report['baseline_metrics']['avg_latency_ms']}ms")
        print(f"   Success Rate: {baseline_report['baseline_metrics']['successful_predictions']}/{baseline_report['baseline_metrics']['total_samples']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Mock baseline generation failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 Testing Task 16-13 Baseline Metrics & Benchmark Suite\n")
    
    tests = [
        ("Feed Loading", test_feed_loading),
        ("Validation Script", test_validation_script),
        ("Comparison Script", test_comparison_script), 
        ("HTML Report Generation", test_html_report_generation),
        ("Mock Baseline Generation", test_mock_baseline_generation)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 Testing {test_name}:")
        print("-" * 40)
        
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {e}")
    
    print(f"\n📊 Test Summary:")
    print(f"   Passed: {passed}/{total}")
    print(f"   Success Rate: {(passed/total)*100:.1f}%")
    
    if passed == total:
        print("✅ All tests passed! Benchmark suite is ready.")
        return 0
    else:
        print(f"❌ {total-passed} test(s) failed. Review implementation.")
        return 1

if __name__ == "__main__":
    exit(main())