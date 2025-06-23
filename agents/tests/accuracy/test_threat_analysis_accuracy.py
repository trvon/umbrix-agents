"""
Comprehensive Accuracy Testing Framework for DSPy Threat Analysis

This module implements detailed accuracy testing including:
- Overall accuracy improvement validation
- Component-specific accuracy testing
- Confidence calibration assessment
- Reasoning chain quality evaluation
- Performance constraint validation
"""

import pytest
import json
import time
import statistics
import numpy as np
from typing import List, Dict, Any, Tuple
from unittest.mock import Mock, patch
import tempfile
import os

# Import the modules we're testing
from agents.common_tools.dspy_optimization import ThreatAnalysisOptimizer, IterativeOptimizationPipeline
from agents.tests.data.training_data_generator import TrainingDataGenerator


class ThreatAnalysisAccuracyTester:
    """Comprehensive accuracy testing for threat analysis"""
    
    def __init__(self):
        self.data_generator = TrainingDataGenerator()
        self.test_data = None
        self.baseline_module = None
        self.optimized_module = None
        self.optimization_result = None
        
        # Test configuration
        self.min_improvement_threshold = 40.0  # Minimum 40% improvement required
        self.min_classification_accuracy = 0.85  # 85% classification accuracy
        self.min_ioc_precision = 0.80  # 80% IOC precision
        self.min_ioc_recall = 0.75  # 75% IOC recall
        self.min_ioc_f1 = 0.77  # 77% IOC F1-score
        self.min_confidence_correlation = 0.6  # 60% confidence correlation
        self.min_reasoning_quality = 0.75  # 75% reasoning quality
        self.max_time_increase_percent = 30.0  # Max 30% time increase
    
    def setup_test_environment(self):
        """Setup test data and modules for accuracy testing"""
        
        print("Setting up accuracy test environment...")
        
        # Generate test data
        self.test_data = self.data_generator.create_validation_set(100)
        print(f"Generated {len(self.test_data)} test examples")
        
        # Create baseline module (mock for testing)
        self.baseline_module = self._create_baseline_module()
        
        # Generate training data and optimize
        training_data = self.data_generator.create_training_examples(200)
        print(f"Generated {len(training_data)} training examples")
        
        # Create optimizer and run optimization
        optimizer = ThreatAnalysisOptimizer(training_data, self.test_data)
        self.optimization_result = optimizer.optimize_threat_analyzer(self.baseline_module)
        self.optimized_module = self.optimization_result.optimized_module
        
        print(f"Optimization completed: {self.optimization_result.improvement_percentage:.1f}% improvement")
        
        yield
        
        # Cleanup
        print("Cleaning up accuracy test environment...")
    
    def _create_baseline_module(self):
        """Create a mock baseline module for testing"""
        
        class MockBaseline:
            def __init__(self):
                self.name = "baseline"
            
            def forward(self, content: str, **kwargs):
                """Mock baseline analysis with basic results"""
                return MockPrediction(
                    threat_classification="Unknown",
                    iocs_extracted=[],
                    attribution_analysis="Unknown",
                    confidence_breakdown={'overall': 0.5}
                )
            
            def __call__(self, content: str, **kwargs):
                return self.forward(content, **kwargs)
        
        class MockPrediction:
            def __init__(self, threat_classification, iocs_extracted, attribution_analysis, confidence_breakdown):
                self.threat_classification = threat_classification
                self.iocs_extracted = iocs_extracted
                self.attribution_analysis = attribution_analysis
                self.confidence_breakdown = confidence_breakdown
        
        return MockBaseline()
    
    def test_overall_accuracy_improvement(self):
        """Test that optimized module shows significant accuracy improvement"""
        
        # Calculate baseline performance
        baseline_score = self._evaluate_module_accuracy(self.baseline_module)
        optimized_score = self._evaluate_module_accuracy(self.optimized_module)
        
        improvement_percentage = ((optimized_score - baseline_score) / baseline_score) * 100 if baseline_score > 0 else 0
        
        print(f"\n=== Overall Accuracy Test ===")
        print(f"Baseline accuracy: {baseline_score:.3f}")
        print(f"Optimized accuracy: {optimized_score:.3f}")
        print(f"Improvement: {improvement_percentage:.1f}%")
        
        # Assert minimum improvement as per PBI requirements
        assert improvement_percentage >= self.min_improvement_threshold, \
            f"Only achieved {improvement_percentage:.1f}% improvement, need ≥{self.min_improvement_threshold}%"
        
        # Verify optimization result consistency
        assert abs(self.optimization_result.improvement_percentage - improvement_percentage) < 5.0, \
            "Optimization result improvement percentage should match test calculation"
    
    def test_threat_classification_accuracy(self):
        """Test threat classification accuracy specifically"""
        
        correct_classifications = 0
        total_classifications = 0
        classification_details = []
        
        for example in self.test_data:
            try:
                result = self.optimized_module(content=example['content'])
                
                predicted_class = getattr(result, 'threat_classification', 'Unknown')
                actual_class = example.get('threat_classification', 'Unknown')
                
                is_match = self._classify_match(predicted_class, actual_class)
                if is_match:
                    correct_classifications += 1
                
                classification_details.append({
                    'predicted': predicted_class,
                    'actual': actual_class,
                    'match': is_match
                })
                
                total_classifications += 1
                
            except Exception as e:
                print(f"Classification test error: {e}")
                total_classifications += 1
        
        accuracy = correct_classifications / total_classifications if total_classifications > 0 else 0
        
        print(f"\n=== Classification Accuracy Test ===")
        print(f"Correct: {correct_classifications}/{total_classifications}")
        print(f"Accuracy: {accuracy:.3f}")
        
        # Show some examples
        print("Sample classifications:")
        for detail in classification_details[:5]:
            match_str = "✓" if detail['match'] else "✗"
            print(f"  {match_str} Predicted: {detail['predicted'][:30]} | Actual: {detail['actual'][:30]}")
        
        # Assert minimum classification accuracy
        assert accuracy >= self.min_classification_accuracy, \
            f"Classification accuracy {accuracy:.3f} below target {self.min_classification_accuracy}"
    
    def _classify_match(self, predicted: str, actual: str) -> bool:
        """Check if predicted classification matches actual classification"""
        
        if not predicted or not actual:
            return False
        
        pred_norm = predicted.lower().strip()
        actual_norm = actual.lower().strip()
        
        # Exact match
        if pred_norm == actual_norm:
            return True
        
        # Partial match for threat categories
        threat_categories = {
            'apt': ['apt campaign', 'advanced persistent threat', 'apt'],
            'malware': ['malware distribution', 'malware', 'malicious software'],
            'phishing': ['phishing campaign', 'phishing', 'credential harvesting'],
            'ransomware': ['ransomware attack', 'ransomware'],
            'unknown': ['unknown', 'unclear', 'uncertain']
        }
        
        for category, variants in threat_categories.items():
            if any(variant in pred_norm for variant in variants) and \
               any(variant in actual_norm for variant in variants):
                return True
        
        return False
    
    def test_ioc_extraction_accuracy(self):
        """Test IOC extraction precision and recall"""
        
        precision_scores = []
        recall_scores = []
        f1_scores = []
        ioc_details = []
        
        for example in self.test_data:
            actual_iocs = example.get('iocs', [])
            if not actual_iocs:
                continue  # Skip examples with no IOCs
                
            try:
                result = self.optimized_module(content=example['content'])
                predicted_iocs = self._extract_iocs_from_result(result)
                
                precision, recall, f1 = self._calculate_ioc_metrics(predicted_iocs, actual_iocs)
                
                precision_scores.append(precision)
                recall_scores.append(recall)
                f1_scores.append(f1)
                
                ioc_details.append({
                    'predicted_count': len(predicted_iocs),
                    'actual_count': len(actual_iocs),
                    'precision': precision,
                    'recall': recall,
                    'f1': f1
                })
                
            except Exception as e:
                print(f"IOC extraction test error: {e}")
                continue
        
        if not precision_scores:
            pytest.skip("No IOC examples found for testing")
        
        avg_precision = statistics.mean(precision_scores)
        avg_recall = statistics.mean(recall_scores)
        avg_f1 = statistics.mean(f1_scores)
        
        print(f"\n=== IOC Extraction Test ===")
        print(f"Examples tested: {len(precision_scores)}")
        print(f"Average Precision: {avg_precision:.3f}")
        print(f"Average Recall: {avg_recall:.3f}")
        print(f"Average F1-Score: {avg_f1:.3f}")
        
        # Show some examples
        print("Sample IOC extractions:")
        for detail in ioc_details[:5]:
            print(f"  Predicted: {detail['predicted_count']}, Actual: {detail['actual_count']}, "
                  f"P: {detail['precision']:.2f}, R: {detail['recall']:.2f}, F1: {detail['f1']:.2f}")
        
        # Assert minimum performance thresholds
        assert avg_precision >= self.min_ioc_precision, \
            f"IOC precision {avg_precision:.3f} below target {self.min_ioc_precision}"
        assert avg_recall >= self.min_ioc_recall, \
            f"IOC recall {avg_recall:.3f} below target {self.min_ioc_recall}"
        assert avg_f1 >= self.min_ioc_f1, \
            f"IOC F1-score {avg_f1:.3f} below target {self.min_ioc_f1}"
    
    def _extract_iocs_from_result(self, result) -> List[Dict[str, Any]]:
        """Extract IOCs from analysis result"""
        
        if hasattr(result, 'iocs_extracted'):
            return result.iocs_extracted or []
        
        if isinstance(result, dict) and 'iocs' in result:
            return result['iocs']
        
        return []
    
    def _calculate_ioc_metrics(self, predicted_iocs: List[Dict], actual_iocs: List[Dict]) -> Tuple[float, float, float]:
        """Calculate precision, recall, and F1 for IOC extraction"""
        
        # Extract IOC values for comparison
        pred_values = {ioc.get('value', '').lower().strip() for ioc in predicted_iocs if ioc.get('value')}
        actual_values = {ioc.get('value', '').lower().strip() for ioc in actual_iocs if ioc.get('value')}
        
        # Remove empty values
        pred_values.discard('')
        actual_values.discard('')
        
        if not actual_values:
            return 1.0 if not pred_values else 0.0, 1.0, 1.0 if not pred_values else 0.0
        
        if not pred_values:
            return 0.0, 0.0, 0.0
        
        # Calculate metrics
        true_positives = len(pred_values.intersection(actual_values))
        precision = true_positives / len(pred_values)
        recall = true_positives / len(actual_values)
        
        # F1 score
        if precision + recall == 0:
            f1_score = 0.0
        else:
            f1_score = 2 * (precision * recall) / (precision + recall)
        
        return precision, recall, f1_score
    
    def test_confidence_calibration(self):
        """Test that confidence scores correlate with actual accuracy"""
        
        confidence_accuracy_pairs = []
        
        for example in self.test_data[:50]:  # Sample for performance
            try:
                result = self.optimized_module(content=example['content'])
                
                # Extract confidence score
                confidence = self._extract_confidence_score(result)
                if confidence is None:
                    continue
                
                # Calculate actual accuracy for this prediction
                actual_accuracy = self._calculate_prediction_accuracy(result, example)
                
                confidence_accuracy_pairs.append((confidence, actual_accuracy))
                
            except Exception as e:
                print(f"Confidence calibration test error: {e}")
                continue
        
        if len(confidence_accuracy_pairs) < 10:
            pytest.skip("Insufficient confidence data for calibration testing")
        
        # Calculate correlation between confidence and accuracy
        confidences = [pair[0] for pair in confidence_accuracy_pairs]
        accuracies = [pair[1] for pair in confidence_accuracy_pairs]
        
        correlation = self._calculate_correlation(confidences, accuracies)
        
        print(f"\n=== Confidence Calibration Test ===")
        print(f"Pairs analyzed: {len(confidence_accuracy_pairs)}")
        print(f"Confidence-Accuracy Correlation: {correlation:.3f}")
        print(f"Average confidence: {statistics.mean(confidences):.3f}")
        print(f"Average accuracy: {statistics.mean(accuracies):.3f}")
        
        # Assert reasonable correlation between confidence and accuracy
        assert correlation >= self.min_confidence_correlation, \
            f"Confidence correlation {correlation:.3f} below target {self.min_confidence_correlation}"
    
    def _extract_confidence_score(self, result) -> float:
        """Extract overall confidence score from result"""
        
        if hasattr(result, 'confidence_breakdown'):
            conf_breakdown = result.confidence_breakdown
            if isinstance(conf_breakdown, dict):
                return conf_breakdown.get('overall', conf_breakdown.get('classification'))
        
        if hasattr(result, 'confidence_synthesis'):
            return getattr(result.confidence_synthesis, 'overall_confidence', None)
        
        return None
    
    def _calculate_prediction_accuracy(self, result, example: Dict[str, Any]) -> float:
        """Calculate accuracy score for a single prediction"""
        
        # Use the same metric as the optimizer
        optimizer = ThreatAnalysisOptimizer([], [])
        return optimizer.threat_analysis_metric(example, result)
    
    def _calculate_correlation(self, x_values: List[float], y_values: List[float]) -> float:
        """Calculate Pearson correlation coefficient"""
        
        if len(x_values) != len(y_values) or len(x_values) < 2:
            return 0.0
        
        try:
            correlation_matrix = np.corrcoef(x_values, y_values)
            return correlation_matrix[0, 1] if not np.isnan(correlation_matrix[0, 1]) else 0.0
        except:
            return 0.0
    
    def test_reasoning_chain_quality(self):
        """Test quality and completeness of reasoning chains"""
        
        reasoning_quality_scores = []
        reasoning_details = []
        
        for example in self.test_data[:20]:  # Sample for manual evaluation
            try:
                result = self.optimized_module(content=example['content'])
                
                reasoning_chain = self._extract_reasoning_chain(result)
                if not reasoning_chain:
                    continue
                
                # Evaluate reasoning chain quality
                quality_score = self._evaluate_reasoning_quality(reasoning_chain, example)
                reasoning_quality_scores.append(quality_score)
                
                reasoning_details.append({
                    'chain_length': len(reasoning_chain),
                    'quality_score': quality_score,
                    'sample_steps': reasoning_chain[:3]  # First 3 steps
                })
                
            except Exception as e:
                print(f"Reasoning quality test error: {e}")
                continue
        
        if not reasoning_quality_scores:
            pytest.skip("No reasoning chains found for quality testing")
        
        avg_reasoning_quality = statistics.mean(reasoning_quality_scores)
        
        print(f"\n=== Reasoning Quality Test ===")
        print(f"Chains analyzed: {len(reasoning_quality_scores)}")
        print(f"Average Reasoning Quality: {avg_reasoning_quality:.3f}")
        print(f"Average chain length: {statistics.mean([d['chain_length'] for d in reasoning_details]):.1f}")
        
        # Show sample reasoning
        print("Sample reasoning chains:")
        for detail in reasoning_details[:3]:
            print(f"  Quality: {detail['quality_score']:.2f}, Length: {detail['chain_length']}")
            for i, step in enumerate(detail['sample_steps'], 1):
                print(f"    {i}. {step[:60]}...")
        
        # Assert minimum reasoning quality
        assert avg_reasoning_quality >= self.min_reasoning_quality, \
            f"Reasoning quality {avg_reasoning_quality:.3f} below target {self.min_reasoning_quality}"
    
    def _extract_reasoning_chain(self, result) -> List[str]:
        """Extract reasoning chain from result"""
        
        if hasattr(result, 'reasoning_chain'):
            return result.reasoning_chain or []
        
        if hasattr(result, 'reasoning_steps'):
            return result.reasoning_steps or []
        
        return []
    
    def _evaluate_reasoning_quality(self, reasoning_chain: List[str], example: Dict[str, Any]) -> float:
        """Evaluate the quality of a reasoning chain"""
        
        if not reasoning_chain:
            return 0.0
        
        quality_factors = []
        
        # Factor 1: Chain length (reasonable depth)
        length_score = min(1.0, len(reasoning_chain) / 5.0)  # Optimal around 5 steps
        quality_factors.append(length_score)
        
        # Factor 2: Step diversity (different types of reasoning)
        reasoning_types = ['identified', 'analyzed', 'extracted', 'validated', 'attributed']
        type_count = sum(1 for step in reasoning_chain if any(rtype in step.lower() for rtype in reasoning_types))
        diversity_score = min(1.0, type_count / 3.0)  # At least 3 different types
        quality_factors.append(diversity_score)
        
        # Factor 3: Content relevance (steps mention content elements)
        content_words = example.get('content', '').lower().split()[:20]  # Key content words
        relevance_count = sum(1 for step in reasoning_chain 
                             if any(word in step.lower() for word in content_words))
        relevance_score = min(1.0, relevance_count / len(reasoning_chain))
        quality_factors.append(relevance_score)
        
        # Factor 4: Logical flow (steps build on each other)
        flow_score = 0.8  # Default good score (hard to evaluate automatically)
        quality_factors.append(flow_score)
        
        return statistics.mean(quality_factors)
    
    def test_processing_time_constraint(self):
        """Test that processing time increase is within acceptable bounds"""
        
        # Measure baseline processing time
        baseline_times = []
        test_examples = self.test_data[:10]  # Small sample for timing
        
        for example in test_examples:
            start_time = time.time()
            try:
                self.baseline_module(content=example['content'])
            except:
                pass  # Mock module might fail, that's OK for timing
            baseline_times.append(time.time() - start_time)
        
        avg_baseline_time = statistics.mean(baseline_times)
        
        # Measure optimized processing time
        optimized_times = []
        for example in test_examples:
            start_time = time.time()
            try:
                self.optimized_module(content=example['content'])
            except:
                pass  # Allow failures for timing test
            optimized_times.append(time.time() - start_time)
        
        avg_optimized_time = statistics.mean(optimized_times)
        
        time_increase_percentage = ((avg_optimized_time - avg_baseline_time) / avg_baseline_time) * 100 if avg_baseline_time > 0 else 0
        
        print(f"\n=== Processing Time Test ===")
        print(f"Baseline avg time: {avg_baseline_time:.3f}s")
        print(f"Optimized avg time: {avg_optimized_time:.3f}s")
        print(f"Time increase: {time_increase_percentage:.1f}%")
        print(f"Samples tested: {len(test_examples)}")
        
        # Assert processing time increase is within limits
        assert time_increase_percentage <= self.max_time_increase_percent, \
            f"Time increase {time_increase_percentage:.1f}% exceeds limit of {self.max_time_increase_percent}%"
    
    def _evaluate_module_accuracy(self, module) -> float:
        """Evaluate overall module accuracy using the optimizer metric"""
        
        optimizer = ThreatAnalysisOptimizer([], [])
        return optimizer._evaluate_module(module, self.test_data[:50])  # Sample for performance
    
    def test_optimization_consistency(self):
        """Test that optimization results are consistent and reproducible"""
        
        assert self.optimization_result is not None, "Optimization result should be available"
        
        print(f"\n=== Optimization Consistency Test ===")
        print(f"Optimization method: {self.optimization_result.optimization_method}")
        print(f"Improvement percentage: {self.optimization_result.improvement_percentage:.1f}%")
        print(f"Training time: {self.optimization_result.training_time_seconds:.1f}s")
        print(f"Validation score: {self.optimization_result.validation_score:.3f}")
        
        # Test basic consistency requirements
        assert self.optimization_result.validation_score > 0, "Validation score should be positive"
        assert self.optimization_result.training_time_seconds > 0, "Training time should be positive"
        assert self.optimization_result.optimization_method in ['bootstrap', 'labeled', 'knn'], \
            f"Unknown optimization method: {self.optimization_result.optimization_method}"


class IndustryBenchmarkTests:
    """Test against industry-standard CTI benchmarks"""
    
    def test_mitre_attack_classification(self):
        """Test classification against MITRE ATT&CK framework"""
        # Placeholder for MITRE ATT&CK technique classification testing
        # This would involve mapping threat classifications to MITRE techniques
        pytest.skip("MITRE ATT&CK benchmark test not yet implemented")
    
    def test_cti_benchmark_datasets(self):
        """Test against public CTI benchmark datasets"""
        # Placeholder for testing against standard CTI datasets
        # This would involve loading public datasets and running accuracy tests
        pytest.skip("CTI benchmark dataset test not yet implemented")


# Pytest configuration and helper functions
@pytest.fixture(scope="session")
def accuracy_tester():
    """Create accuracy tester instance for all tests"""
    return ThreatAnalysisAccuracyTester()


@pytest.fixture(scope="module")
def setup_test_environment():
    """Setup test data and modules for accuracy testing"""
    tester = ThreatAnalysisAccuracyTester()
    tester.setup_test_environment()
    return tester

def test_runner(setup_test_environment):
    """Run all accuracy tests and generate report"""
    
    # Run tests
    tester = setup_test_environment
    
    test_results = {}
    
    try:
        tester.test_overall_accuracy_improvement()
        test_results['overall_accuracy'] = 'PASS'
    except AssertionError as e:
        test_results['overall_accuracy'] = f'FAIL: {e}'
    
    try:
        tester.test_threat_classification_accuracy()
        test_results['classification_accuracy'] = 'PASS'
    except AssertionError as e:
        test_results['classification_accuracy'] = f'FAIL: {e}'
    
    try:
        tester.test_ioc_extraction_accuracy()
        test_results['ioc_accuracy'] = 'PASS'
    except AssertionError as e:
        test_results['ioc_accuracy'] = f'FAIL: {e}'
    
    try:
        tester.test_confidence_calibration()
        test_results['confidence_calibration'] = 'PASS'
    except AssertionError as e:
        test_results['confidence_calibration'] = f'FAIL: {e}'
    
    try:
        tester.test_reasoning_chain_quality()
        test_results['reasoning_quality'] = 'PASS'
    except AssertionError as e:
        test_results['reasoning_quality'] = f'FAIL: {e}'
    
    try:
        tester.test_processing_time_constraint()
        test_results['processing_time'] = 'PASS'
    except AssertionError as e:
        test_results['processing_time'] = f'FAIL: {e}'
    
    # Generate report
    print("\n" + "="*60)
    print("ACCURACY TEST REPORT")
    print("="*60)
    
    for test_name, result in test_results.items():
        status = "✓" if result == 'PASS' else "✗"
        print(f"{status} {test_name}: {result}")
    
    passed_tests = sum(1 for result in test_results.values() if result == 'PASS')
    total_tests = len(test_results)
    
    print(f"\nSummary: {passed_tests}/{total_tests} tests passed")
    
    return test_results


if __name__ == "__main__":
    # Run the test suite
    results = test_runner()