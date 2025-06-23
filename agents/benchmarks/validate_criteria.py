#!/usr/bin/env python3
"""
Acceptance Criteria Validation for PBI-16 Baseline Metrics

Validates that benchmark results meet the acceptance criteria defined
in Task 16-13 and PBI-16 overall requirements.
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class CriteriaResult:
    criterion: str
    target_value: str
    actual_value: str
    meets_target: bool
    deviation: float
    severity: str  # "critical", "warning", "info"

class AcceptanceCriteriaValidator:
    """Validates benchmark results against acceptance criteria"""
    
    def __init__(self, criteria_file: str = None):
        self.criteria = self._load_default_criteria()
        if criteria_file:
            self.criteria.update(self._load_criteria_file(criteria_file))
    
    def _load_default_criteria(self) -> Dict:
        """Load default acceptance criteria for PBI-16"""
        return {
            "threat_classification_f1": {
                "target_value": 0.62,
                "tolerance": 0.01,
                "comparison": "approximately_equal",
                "severity": "critical",
                "description": "Threat classification F1 score must be 0.62 ± 0.01"
            },
            "avg_processing_latency": {
                "target_value": 550.0,
                "tolerance": 0.0,
                "comparison": "less_than_or_equal",
                "severity": "critical", 
                "description": "Average processing latency must be ≤ 550 ms"
            },
            "p95_latency": {
                "target_value": 1000.0,
                "tolerance": 0.0,
                "comparison": "less_than_or_equal",
                "severity": "warning",
                "description": "P95 latency should be ≤ 1000 ms"
            },
            "success_rate": {
                "target_value": 0.90,
                "tolerance": 0.0,
                "comparison": "greater_than_or_equal",
                "severity": "critical",
                "description": "Processing success rate must be ≥ 90%"
            },
            "cost_per_1k_docs": {
                "target_value": 1.0,
                "tolerance": 0.0,
                "comparison": "less_than_or_equal",
                "severity": "warning",
                "description": "Cost per 1000 documents should be ≤ $1.00"
            },
            "accuracy_improvement": {
                "target_value": 0.40,
                "tolerance": 0.0,
                "comparison": "greater_than_or_equal",
                "severity": "info",
                "description": "Enhanced DSPy should achieve >40% accuracy improvement"
            },
            "latency_increase_limit": {
                "target_value": 0.30,
                "tolerance": 0.0,
                "comparison": "less_than_or_equal",
                "severity": "warning",
                "description": "Processing time increase should be <30%"
            }
        }
    
    def _load_criteria_file(self, file_path: str) -> Dict:
        """Load custom criteria from JSON file"""
        try:
            with open(file_path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"Could not load criteria file {file_path}: {e}")
            return {}
    
    def _compare_values(self, actual: float, target: float, comparison: str, tolerance: float) -> bool:
        """Compare actual value against target using specified comparison method"""
        if comparison == "approximately_equal":
            return abs(actual - target) <= tolerance
        elif comparison == "less_than_or_equal":
            return actual <= target + tolerance
        elif comparison == "greater_than_or_equal":
            return actual >= target - tolerance
        elif comparison == "less_than":
            return actual < target + tolerance
        elif comparison == "greater_than":
            return actual > target - tolerance
        else:
            logger.warning(f"Unknown comparison method: {comparison}")
            return False
    
    def _calculate_deviation(self, actual: float, target: float, comparison: str) -> float:
        """Calculate deviation from target value"""
        if comparison == "approximately_equal":
            return abs(actual - target)
        elif comparison in ["less_than_or_equal", "less_than"]:
            return max(0, actual - target)  # Positive if exceeds target
        elif comparison in ["greater_than_or_equal", "greater_than"]:
            return max(0, target - actual)  # Positive if below target
        else:
            return abs(actual - target)
    
    def validate_baseline_report(self, report_path: Path, analyzer_name: str) -> List[CriteriaResult]:
        """Validate a single baseline report against criteria"""
        try:
            with open(report_path) as f:
                report = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Could not load report {report_path}: {e}")
            return []
        
        baseline_metrics = report.get("baseline_metrics", {})
        results = []
        
        # Extract relevant metrics
        f1_score = baseline_metrics.get("f1_score", 0)
        avg_latency = baseline_metrics.get("avg_latency_ms", 0)
        p95_latency = baseline_metrics.get("p95_latency_ms", 0)
        total_samples = baseline_metrics.get("total_samples", 1)
        successful_predictions = baseline_metrics.get("successful_predictions", 0)
        estimated_cost = baseline_metrics.get("estimated_cost_usd", 0)
        
        # Calculate derived metrics
        success_rate = successful_predictions / total_samples if total_samples > 0 else 0
        cost_per_1k_docs = (estimated_cost * 1000 / total_samples) if total_samples > 0 else 0
        
        # Validate against criteria
        validations = [
            ("threat_classification_f1", f1_score),
            ("avg_processing_latency", avg_latency),
            ("p95_latency", p95_latency),
            ("success_rate", success_rate),
            ("cost_per_1k_docs", cost_per_1k_docs),
        ]
        
        for criterion_name, actual_value in validations:
            if criterion_name in self.criteria:
                criterion = self.criteria[criterion_name]
                target_value = criterion["target_value"]
                tolerance = criterion.get("tolerance", 0)
                comparison = criterion["comparison"]
                severity = criterion.get("severity", "info")
                
                meets_target = self._compare_values(
                    actual_value, target_value, comparison, tolerance
                )
                
                deviation = self._calculate_deviation(
                    actual_value, target_value, comparison
                )
                
                results.append(CriteriaResult(
                    criterion=f"{analyzer_name}_{criterion_name}",
                    target_value=f"{target_value} ({comparison})",
                    actual_value=f"{actual_value:.3f}",
                    meets_target=meets_target,
                    deviation=deviation,
                    severity=severity
                ))
        
        return results
    
    def validate_all_baselines(self, baseline_dir: Path) -> Dict[str, List[CriteriaResult]]:
        """Validate all baseline reports in a directory"""
        baseline_files = list(baseline_dir.glob("*_baseline.json"))
        validation_results = {}
        
        for baseline_file in baseline_files:
            analyzer_name = baseline_file.stem.replace("_baseline", "")
            results = self.validate_baseline_report(baseline_file, analyzer_name)
            validation_results[analyzer_name] = results
        
        return validation_results
    
    def generate_validation_report(self, validation_results: Dict[str, List[CriteriaResult]]) -> Dict:
        """Generate comprehensive validation report"""
        all_results = []
        for analyzer_results in validation_results.values():
            all_results.extend(analyzer_results)
        
        critical_failures = [r for r in all_results if not r.meets_target and r.severity == "critical"]
        warnings = [r for r in all_results if not r.meets_target and r.severity == "warning"]
        passed = [r for r in all_results if r.meets_target]
        
        report = {
            "validation_summary": {
                "total_criteria": len(all_results),
                "passed": len(passed),
                "warnings": len(warnings),
                "critical_failures": len(critical_failures),
                "overall_status": "PASS" if len(critical_failures) == 0 else "FAIL"
            },
            "results_by_analyzer": {
                analyzer: [
                    {
                        "criterion": r.criterion,
                        "target": r.target_value,
                        "actual": r.actual_value,
                        "meets_target": r.meets_target,
                        "deviation": r.deviation,
                        "severity": r.severity
                    }
                    for r in results
                ]
                for analyzer, results in validation_results.items()
            },
            "critical_failures": [
                {
                    "criterion": r.criterion,
                    "target": r.target_value,
                    "actual": r.actual_value,
                    "deviation": r.deviation,
                    "severity": r.severity
                }
                for r in critical_failures
            ],
            "warnings": [
                {
                    "criterion": r.criterion,
                    "target": r.target_value,
                    "actual": r.actual_value,
                    "deviation": r.deviation,
                    "severity": r.severity
                }
                for r in warnings
            ],
            "acceptance_criteria_met": len(critical_failures) == 0
        }
        
        return report


def main():
    parser = argparse.ArgumentParser(description="Validate baseline metrics against acceptance criteria")
    parser.add_argument("--baseline-dir", required=True, help="Directory containing baseline reports")
    parser.add_argument("--criteria-file", help="JSON file with custom acceptance criteria")
    parser.add_argument("--output", help="Output file for validation report")
    parser.add_argument("--fail-on-critical", action="store_true", 
                       help="Exit with error code if critical criteria are not met")
    
    args = parser.parse_args()
    
    baseline_dir = Path(args.baseline_dir)
    if not baseline_dir.exists():
        print(f"Error: Baseline directory does not exist: {baseline_dir}")
        return 1
    
    validator = AcceptanceCriteriaValidator(args.criteria_file)
    validation_results = validator.validate_all_baselines(baseline_dir)
    report = validator.generate_validation_report(validation_results)
    
    # Save report if output specified
    if args.output:
        output_file = Path(args.output)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"Validation report saved to: {output_file}")
    
    # Print summary
    summary = report["validation_summary"]
    print(f"\n✅ Validation Summary:")
    print(f"   Total criteria checked: {summary['total_criteria']}")
    print(f"   Passed: {summary['passed']}")
    print(f"   Warnings: {summary['warnings']}")
    print(f"   Critical failures: {summary['critical_failures']}")
    print(f"   Overall status: {summary['overall_status']}")
    
    # Print critical failures
    if report["critical_failures"]:
        print(f"\n❌ Critical Failures:")
        for failure in report["critical_failures"]:
            print(f"   - {failure['criterion']}: {failure['actual']} (target: {failure['target']})")
    
    # Print warnings
    if report["warnings"]:
        print(f"\n⚠️ Warnings:")
        for warning in report["warnings"]:
            print(f"   - {warning['criterion']}: {warning['actual']} (target: {warning['target']})")
    
    # Exit with error if critical failures and flag is set
    if args.fail_on_critical and summary['critical_failures'] > 0:
        print(f"\n❌ Critical acceptance criteria not met - failing validation")
        return 1
    
    print(f"\n✅ Validation completed successfully")
    return 0


if __name__ == "__main__":
    exit(main())