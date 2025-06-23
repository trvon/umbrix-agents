#!/usr/bin/env python3
"""
Benchmark Harness for PBI-16 Baseline Metrics & Performance Testing

This module provides comprehensive benchmarking capabilities for measuring:
- Classification accuracy (F1, Precision, Recall)
- Processing latency (p50, p95)
- Token consumption and costs
- Memory usage and throughput

Usage:
    python -m pytest benchmarks/benchmark_harness.py --benchmark-only
    python benchmarks/benchmark_harness.py --generate-baseline
"""

import json
import time
import statistics
import asyncio
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass, asdict
from pathlib import Path
import logging

import pytest
from sklearn.metrics import f1_score, precision_score, recall_score, classification_report
import tiktoken
from datetime import datetime, timezone

# Agent imports
from common_tools.dspy_extraction_tool import DSPyExtractionTool
from common_tools.intelligent_content_analyzer import IntelligentContentAnalyzer
from common_tools.advanced_dspy_modules import ChainOfThoughtAnalyzer, ReActAnalyzer
from common_tools.models.feed_record import FeedRecord

logger = logging.getLogger(__name__)

@dataclass
class BenchmarkMetrics:
    """Container for benchmark results and metrics"""
    
    # Classification Metrics
    f1_score: float
    precision: float
    recall: float
    accuracy: float
    
    # Performance Metrics  
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    
    # Resource Metrics
    avg_tokens_per_request: float
    total_tokens: int
    estimated_cost_usd: float
    memory_peak_mb: float
    
    # Processing Metrics
    total_samples: int
    successful_predictions: int
    failed_predictions: int
    throughput_docs_per_sec: float
    
    # Metadata
    timestamp: str
    model_version: str
    test_duration_sec: float

class BenchmarkHarness:
    """Main benchmark harness for baseline metrics collection"""
    
    def __init__(self, benchmark_data_path: str = "benchmarks/feeds_bench_v2.json"):
        self.benchmark_data_path = Path(benchmark_data_path)
        self.test_data = self._load_test_data()
        self.tokenizer = tiktoken.encoding_for_model("gpt-3.5-turbo")
        
        # Cost estimation (approximate, update based on actual model pricing)
        self.cost_per_1k_tokens = {
            "gpt-3.5-turbo": 0.002,
            "gpt-4": 0.03,
            "gemini-pro": 0.001,
        }
        
    def _load_test_data(self) -> Dict:
        """Load benchmark test data from JSON file"""
        try:
            with open(self.benchmark_data_path) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Benchmark data file not found: {self.benchmark_data_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in benchmark data: {e}")
            raise

    def _count_tokens(self, text: str) -> int:
        """Count tokens in text using tiktoken"""
        try:
            return len(self.tokenizer.encode(text))
        except Exception:
            # Fallback estimation if tiktoken fails
            return len(text.split()) * 1.3  # Rough approximation

    def _estimate_cost(self, total_tokens: int, model_name: str = "gpt-3.5-turbo") -> float:
        """Estimate cost based on token count and model pricing"""
        cost_per_token = self.cost_per_1k_tokens.get(model_name, 0.002) / 1000
        return total_tokens * cost_per_token

    async def _process_sample_with_timing(self, analyzer: Any, sample: Dict) -> Tuple[Dict, float, int]:
        """Process a single sample with timing and token counting"""
        start_time = time.perf_counter()
        
        try:
            # Create FeedRecord from sample
            feed_record = FeedRecord(
                title=sample["title"],
                description=sample["content"],
                url=sample["url"],
                published_date=sample.get("published_date"),
                source_name=sample.get("source_type", "unknown")
            )
            
            # Process with the analyzer
            if hasattr(analyzer, 'analyze_content'):
                result = await analyzer.analyze_content(feed_record)
            elif hasattr(analyzer, 'extract_intelligence'):
                result = await analyzer.extract_intelligence(sample["content"])
            else:
                # Fallback for basic analyzers
                result = await analyzer.analyze(sample["content"])
                
            end_time = time.perf_counter()
            latency_ms = (end_time - start_time) * 1000
            
            # Count tokens (input + estimated output)
            input_tokens = self._count_tokens(sample["content"])
            output_tokens = self._count_tokens(str(result)) if result else 0
            total_tokens = input_tokens + output_tokens
            
            return result, latency_ms, total_tokens
            
        except Exception as e:
            logger.error(f"Error processing sample {sample.get('id', 'unknown')}: {e}")
            end_time = time.perf_counter()
            latency_ms = (end_time - start_time) * 1000
            return None, latency_ms, 0

    def _extract_predicted_category(self, result: Any) -> Optional[str]:
        """Extract predicted category from analysis result"""
        if not result:
            return None
            
        # Handle different result formats
        if isinstance(result, dict):
            # Check common category fields
            for field in ["category", "primary_category", "threat_type", "classification"]:
                if field in result:
                    return str(result[field]).lower()
                    
            # Check if it's a structured analysis result
            if "analysis" in result and isinstance(result["analysis"], dict):
                return self._extract_predicted_category(result["analysis"])
                
        elif hasattr(result, "category"):
            return str(result.category).lower()
        elif hasattr(result, "primary_category"):
            return str(result.primary_category).lower()
            
        # Fallback: try to extract from string representation
        result_str = str(result).lower()
        categories = ["malware", "phishing", "vulnerability", "apt", "botnet", "fraud", "other"]
        
        for category in categories:
            if category in result_str:
                return category
                
        return "other"  # Default fallback

    async def run_classification_benchmark(self, analyzer_class, analyzer_kwargs: Dict = None) -> BenchmarkMetrics:
        """Run comprehensive classification benchmark"""
        analyzer_kwargs = analyzer_kwargs or {}
        analyzer = analyzer_class(**analyzer_kwargs)
        
        predictions = []
        ground_truths = []
        latencies = []
        token_counts = []
        failed_count = 0
        
        start_time = time.perf_counter()
        
        # Process all samples
        for sample in self.test_data["samples"]:
            result, latency_ms, tokens = await self._process_sample_with_timing(analyzer, sample)
            
            latencies.append(latency_ms)
            token_counts.append(tokens)
            
            if result is not None:
                predicted_category = self._extract_predicted_category(result)
                predictions.append(predicted_category or "other")
                ground_truths.append(sample["ground_truth"]["primary_category"])
            else:
                failed_count += 1
                predictions.append("other")  # Default for failed predictions
                ground_truths.append(sample["ground_truth"]["primary_category"])
        
        end_time = time.perf_counter()
        total_duration = end_time - start_time
        
        # Calculate classification metrics
        f1 = f1_score(ground_truths, predictions, average="weighted", zero_division=0)
        precision = precision_score(ground_truths, predictions, average="weighted", zero_division=0)
        recall = recall_score(ground_truths, predictions, average="weighted", zero_division=0)
        accuracy = sum(1 for gt, pred in zip(ground_truths, predictions) if gt == pred) / len(predictions)
        
        # Calculate performance metrics
        avg_latency = statistics.mean(latencies)
        p50_latency = statistics.median(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18]  # 95th percentile
        p99_latency = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies)
        
        # Calculate resource metrics
        total_tokens = sum(token_counts)
        avg_tokens = statistics.mean(token_counts) if token_counts else 0
        estimated_cost = self._estimate_cost(total_tokens)
        
        # Calculate throughput
        successful_predictions = len(predictions) - failed_count
        throughput = len(predictions) / total_duration if total_duration > 0 else 0
        
        return BenchmarkMetrics(
            f1_score=f1,
            precision=precision,
            recall=recall,
            accuracy=accuracy,
            avg_latency_ms=avg_latency,
            p50_latency_ms=p50_latency,
            p95_latency_ms=p95_latency,
            p99_latency_ms=p99_latency,
            avg_tokens_per_request=avg_tokens,
            total_tokens=total_tokens,
            estimated_cost_usd=estimated_cost,
            memory_peak_mb=0.0,  # TODO: Implement memory monitoring
            total_samples=len(predictions),
            successful_predictions=successful_predictions,
            failed_predictions=failed_count,
            throughput_docs_per_sec=throughput,
            timestamp=datetime.now(timezone.utc).isoformat(),
            model_version=getattr(analyzer, 'model_version', 'unknown'),
            test_duration_sec=total_duration
        )

    def generate_baseline_report(self, metrics: BenchmarkMetrics) -> Dict:
        """Generate comprehensive baseline report"""
        return {
            "baseline_metrics": asdict(metrics),
            "benchmark_data_info": {
                "total_samples": self.test_data["metadata"]["total_samples"],
                "categories": self.test_data["metadata"]["categories"],
                "source_distribution": self.test_data["statistics"]["source_distribution"]
            },
            "acceptance_criteria_validation": {
                "threat_classification_f1": {
                    "target": "0.62 ± 0.01",
                    "actual": f"{metrics.f1_score:.3f}",
                    "meets_target": abs(metrics.f1_score - 0.62) <= 0.01
                },
                "avg_processing_latency": {
                    "target": "≤ 550 ms",
                    "actual": f"{metrics.avg_latency_ms:.1f} ms",
                    "meets_target": metrics.avg_latency_ms <= 550
                },
                "cost_per_1k_docs": {
                    "target": "documented",
                    "actual": f"${(metrics.estimated_cost_usd * 1000 / metrics.total_samples):.4f}",
                    "meets_target": True
                }
            },
            "performance_analysis": {
                "latency_distribution": {
                    "p50": f"{metrics.p50_latency_ms:.1f} ms",
                    "p95": f"{metrics.p95_latency_ms:.1f} ms", 
                    "p99": f"{metrics.p99_latency_ms:.1f} ms"
                },
                "resource_efficiency": {
                    "tokens_per_doc": metrics.avg_tokens_per_request,
                    "throughput": f"{metrics.throughput_docs_per_sec:.2f} docs/sec",
                    "success_rate": f"{(metrics.successful_predictions / metrics.total_samples * 100):.1f}%"
                }
            },
            "recommendations": self._generate_recommendations(metrics)
        }

    def _generate_recommendations(self, metrics: BenchmarkMetrics) -> List[str]:
        """Generate recommendations based on benchmark results"""
        recommendations = []
        
        if metrics.f1_score < 0.62:
            recommendations.append("F1 score below target - consider model fine-tuning or prompt optimization")
        
        if metrics.avg_latency_ms > 550:
            recommendations.append("Average latency exceeds target - investigate model optimization or caching strategies")
            
        if metrics.failed_predictions > metrics.total_samples * 0.05:
            recommendations.append("High failure rate detected - improve error handling and fallback mechanisms")
            
        if metrics.estimated_cost_usd / metrics.total_samples > 0.001:  # > $1 per 1k docs
            recommendations.append("High cost per document - consider model efficiency improvements")
            
        if not recommendations:
            recommendations.append("All metrics meet targets - baseline established successfully")
            
        return recommendations

    def save_baseline_report(self, metrics: BenchmarkMetrics, output_path: str = "benchmarks/baseline_report.json"):
        """Save baseline report to file"""
        report = self.generate_baseline_report(metrics)
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
            
        logger.info(f"Baseline report saved to {output_file}")
        return output_file


# Pytest benchmarks for CI integration
class TestBenchmarkSuite:
    """Pytest test suite for benchmark execution"""
    
    @pytest.fixture
    def harness(self):
        return BenchmarkHarness()
    
    @pytest.mark.asyncio
    @pytest.mark.benchmark(group="dspy_extraction")
    async def test_dspy_extraction_baseline(self, harness, benchmark):
        """Benchmark DSPy extraction tool baseline performance"""
        
        async def run_extraction_benchmark():
            analyzer = DSPyExtractionTool()
            return await harness.run_classification_benchmark(DSPyExtractionTool)
        
        metrics = await benchmark.pedantic(run_extraction_benchmark, rounds=1)
        
        # Assertions for acceptance criteria
        assert metrics.f1_score >= 0.60, f"F1 score {metrics.f1_score:.3f} below minimum threshold"
        assert metrics.avg_latency_ms <= 1000, f"Average latency {metrics.avg_latency_ms:.1f}ms exceeds limit"
        assert metrics.successful_predictions >= metrics.total_samples * 0.9, "Success rate below 90%"
        
        # Save baseline for future comparisons
        harness.save_baseline_report(metrics, "benchmarks/dspy_extraction_baseline.json")
        
        return metrics

    @pytest.mark.asyncio
    @pytest.mark.benchmark(group="intelligent_analyzer")
    async def test_intelligent_analyzer_baseline(self, harness, benchmark):
        """Benchmark Intelligent Content Analyzer baseline performance"""
        
        async def run_analyzer_benchmark():
            return await harness.run_classification_benchmark(IntelligentContentAnalyzer)
        
        metrics = await benchmark.pedantic(run_analyzer_benchmark, rounds=1)
        
        # Enhanced assertions for PBI-16 targets
        assert metrics.f1_score >= 0.62, f"F1 score {metrics.f1_score:.3f} below PBI-16 target"
        assert metrics.avg_latency_ms <= 550, f"Average latency {metrics.avg_latency_ms:.1f}ms exceeds PBI-16 target"
        
        harness.save_baseline_report(metrics, "benchmarks/intelligent_analyzer_baseline.json")
        
        return metrics

    @pytest.mark.asyncio
    @pytest.mark.benchmark(group="chain_of_thought")
    async def test_chain_of_thought_baseline(self, harness, benchmark):
        """Benchmark Chain of Thought analyzer baseline performance"""
        
        async def run_cot_benchmark():
            return await harness.run_classification_benchmark(ChainOfThoughtAnalyzer)
        
        metrics = await benchmark.pedantic(run_cot_benchmark, rounds=1)
        
        harness.save_baseline_report(metrics, "benchmarks/chain_of_thought_baseline.json")
        
        return metrics


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run baseline benchmark suite")
    parser.add_argument("--generate-baseline", action="store_true", 
                       help="Generate baseline reports for all analyzers")
    parser.add_argument("--output-dir", default="benchmarks", 
                       help="Output directory for reports")
    parser.add_argument("--analyzer", choices=["dspy", "intelligent", "cot", "all"], 
                       default="all", help="Which analyzer to benchmark")
    
    args = parser.parse_args()
    
    if args.generate_baseline:
        async def main():
            harness = BenchmarkHarness()
            
            analyzers = {
                "dspy": DSPyExtractionTool,
                "intelligent": IntelligentContentAnalyzer,
                "cot": ChainOfThoughtAnalyzer
            }
            
            if args.analyzer == "all":
                selected_analyzers = analyzers
            else:
                selected_analyzers = {args.analyzer: analyzers[args.analyzer]}
            
            for name, analyzer_class in selected_analyzers.items():
                print(f"\nRunning baseline benchmark for {name}...")
                try:
                    metrics = await harness.run_classification_benchmark(analyzer_class)
                    output_path = f"{args.output_dir}/{name}_baseline.json"
                    harness.save_baseline_report(metrics, output_path)
                    
                    print(f"✅ {name} baseline completed:")
                    print(f"   F1 Score: {metrics.f1_score:.3f}")
                    print(f"   Avg Latency: {metrics.avg_latency_ms:.1f} ms")
                    print(f"   Cost per 1k docs: ${(metrics.estimated_cost_usd * 1000 / metrics.total_samples):.4f}")
                    print(f"   Report saved: {output_path}")
                    
                except Exception as e:
                    print(f"❌ {name} baseline failed: {e}")
        
        asyncio.run(main())
    else:
        print("Use --generate-baseline to run baseline generation or run via pytest:")
        print("pytest benchmarks/benchmark_harness.py --benchmark-only")