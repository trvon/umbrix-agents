#!/usr/bin/env python3
"""
Baseline Comparison Tool for PBI-16 Performance Tracking

Compares current benchmark results with previous baselines to detect
performance regressions and improvements.
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class PerformanceChange:
    analyzer: str
    metric: str
    current_value: float
    previous_value: float
    change: float
    change_percent: float
    is_regression: bool

class BaselineComparator:
    """Compare baseline metrics between different runs"""
    
    def __init__(self, regression_thresholds: Dict[str, float] = None):
        self.regression_thresholds = regression_thresholds or {
            "f1_score": -0.02,  # -2% regression threshold
            "avg_latency_ms": 0.15,  # +15% latency increase threshold
            "estimated_cost_usd": 0.20,  # +20% cost increase threshold
            "throughput_docs_per_sec": -0.10,  # -10% throughput decrease threshold
        }
    
    def load_baseline_report(self, path: Path) -> Optional[Dict]:
        """Load baseline report from JSON file"""
        try:
            with open(path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"Could not load baseline from {path}: {e}")
            return None
    
    def compare_metrics(self, current: Dict, previous: Dict, analyzer_name: str) -> List[PerformanceChange]:
        """Compare metrics between current and previous baselines"""
        changes = []
        
        current_metrics = current.get("baseline_metrics", {})
        previous_metrics = previous.get("baseline_metrics", {})
        
        metrics_to_compare = [
            "f1_score", "precision", "recall", "accuracy",
            "avg_latency_ms", "p95_latency_ms",
            "estimated_cost_usd", "throughput_docs_per_sec"
        ]
        
        for metric in metrics_to_compare:
            if metric in current_metrics and metric in previous_metrics:
                current_val = current_metrics[metric]
                previous_val = previous_metrics[metric]
                
                if previous_val != 0:
                    change = current_val - previous_val
                    change_percent = (change / previous_val) * 100
                    
                    # Determine if this is a regression
                    is_regression = self._is_regression(metric, change_percent / 100)
                    
                    changes.append(PerformanceChange(
                        analyzer=analyzer_name,
                        metric=metric,
                        current_value=current_val,
                        previous_value=previous_val,
                        change=change,
                        change_percent=change_percent,
                        is_regression=is_regression
                    ))
        
        return changes
    
    def _is_regression(self, metric: str, change_ratio: float) -> bool:
        """Determine if a metric change constitutes a regression"""
        threshold = self.regression_thresholds.get(metric, 0)
        
        # For metrics where higher is better (f1_score, throughput)
        if metric in ["f1_score", "precision", "recall", "accuracy", "throughput_docs_per_sec"]:
            return change_ratio < threshold
        
        # For metrics where lower is better (latency, cost)
        else:
            return change_ratio > abs(threshold)
    
    def compare_baseline_directories(self, current_dir: Path, previous_dir: Path) -> Dict:
        """Compare all baselines between two directories"""
        current_files = list(current_dir.glob("*_baseline.json"))
        comparison_results = {
            "summary": {},
            "changes": [],
            "regressions": [],
            "improvements": [],
            "missing_baselines": []
        }
        
        for current_file in current_files:
            analyzer_name = current_file.stem.replace("_baseline", "")
            previous_file = previous_dir / current_file.name
            
            current_data = self.load_baseline_report(current_file)
            previous_data = self.load_baseline_report(previous_file)
            
            if not current_data:
                continue
                
            if not previous_data:
                comparison_results["missing_baselines"].append(analyzer_name)
                continue
            
            # Compare metrics
            changes = self.compare_metrics(current_data, previous_data, analyzer_name)
            comparison_results["changes"].extend(changes)
            
            # Categorize changes
            regressions = [c for c in changes if c.is_regression]
            improvements = [c for c in changes if not c.is_regression and abs(c.change_percent) > 1]
            
            comparison_results["regressions"].extend(regressions)
            comparison_results["improvements"].extend(improvements)
            
            # Generate summary for this analyzer
            f1_change = next((c for c in changes if c.metric == "f1_score"), None)
            latency_change = next((c for c in changes if c.metric == "avg_latency_ms"), None)
            
            summary = {
                "analyzer": analyzer_name,
                "has_regressions": len(regressions) > 0,
                "improvement_count": len(improvements),
                "regression_count": len(regressions)
            }
            
            if f1_change:
                summary.update({
                    "current_f1": f1_change.current_value,
                    "previous_f1": f1_change.previous_value,
                    "f1_score_change": f1_change.change,
                    "f1_score_change_percent": f1_change.change_percent
                })
            
            if latency_change:
                summary.update({
                    "current_latency": latency_change.current_value,
                    "previous_latency": latency_change.previous_value,
                    "latency_change": latency_change.change,
                    "latency_change_percent": latency_change.change_percent
                })
            
            comparison_results["summary"][analyzer_name] = summary
        
        return comparison_results
    
    def generate_comparison_report(self, comparison_results: Dict) -> Dict:
        """Generate a comprehensive comparison report"""
        total_analyzers = len(comparison_results["summary"])
        analyzers_with_regressions = sum(1 for s in comparison_results["summary"].values() if s["has_regressions"])
        
        critical_regressions = [
            r for r in comparison_results["regressions"] 
            if abs(r.change_percent) > 10  # >10% change
        ]
        
        report = {
            "comparison_metadata": {
                "total_analyzers_compared": total_analyzers,
                "analyzers_with_regressions": analyzers_with_regressions,
                "total_regressions": len(comparison_results["regressions"]),
                "critical_regressions": len(critical_regressions),
                "total_improvements": len(comparison_results["improvements"])
            },
            "summary": comparison_results["summary"],
            "regressions": [
                {
                    "analyzer": r.analyzer,
                    "metric": r.metric,
                    "change": f"{r.change_percent:.1f}%",
                    "current": r.current_value,
                    "previous": r.previous_value,
                    "is_critical": abs(r.change_percent) > 10
                }
                for r in comparison_results["regressions"]
            ],
            "improvements": [
                {
                    "analyzer": i.analyzer,
                    "metric": i.metric,
                    "change": f"{i.change_percent:.1f}%",
                    "current": i.current_value,
                    "previous": i.previous_value
                }
                for i in comparison_results["improvements"]
            ],
            "missing_baselines": comparison_results["missing_baselines"],
            "overall_assessment": self._generate_overall_assessment(comparison_results)
        }
        
        return report
    
    def _generate_overall_assessment(self, comparison_results: Dict) -> str:
        """Generate overall assessment of the comparison"""
        regressions = comparison_results["regressions"]
        improvements = comparison_results["improvements"]
        
        critical_regressions = [r for r in regressions if abs(r.change_percent) > 10]
        
        if critical_regressions:
            return "CRITICAL_REGRESSION"
        elif len(regressions) > len(improvements):
            return "PERFORMANCE_DEGRADATION"
        elif len(improvements) > len(regressions):
            return "PERFORMANCE_IMPROVEMENT"
        else:
            return "STABLE_PERFORMANCE"


def main():
    parser = argparse.ArgumentParser(description="Compare baseline performance metrics")
    parser.add_argument("--current", required=True, help="Directory with current baseline reports")
    parser.add_argument("--previous", required=True, help="Directory with previous baseline reports")
    parser.add_argument("--output", required=True, help="Output file for comparison report")
    parser.add_argument("--fail-on-regression", action="store_true", 
                       help="Exit with error code if critical regressions detected")
    
    args = parser.parse_args()
    
    current_dir = Path(args.current)
    previous_dir = Path(args.previous)
    output_file = Path(args.output)
    
    if not current_dir.exists():
        print(f"Error: Current directory does not exist: {current_dir}")
        return 1
    
    if not previous_dir.exists():
        print(f"Warning: Previous directory does not exist: {previous_dir}")
        print("Skipping comparison - no previous baseline available")
        return 0
    
    comparator = BaselineComparator()
    comparison_results = comparator.compare_baseline_directories(current_dir, previous_dir)
    report = comparator.generate_comparison_report(comparison_results)
    
    # Save report
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Comparison report saved to: {output_file}")
    
    # Print summary
    metadata = report["comparison_metadata"]
    print(f"\n📊 Comparison Summary:")
    print(f"   Analyzers compared: {metadata['total_analyzers_compared']}")
    print(f"   Regressions found: {metadata['total_regressions']}")
    print(f"   Critical regressions: {metadata['critical_regressions']}")
    print(f"   Improvements found: {metadata['total_improvements']}")
    print(f"   Overall assessment: {report['overall_assessment']}")
    
    # Exit with error if critical regressions found and flag is set
    if args.fail_on_regression and metadata['critical_regressions'] > 0:
        print(f"\n❌ Critical regressions detected - failing build")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())