#!/usr/bin/env python3
"""
HTML Report Generator for PBI-16 Baseline Metrics

Generates comprehensive HTML reports with visualizations for baseline
metrics and performance tracking.
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PBI-16 Baseline Metrics Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1, h2, h3 {
            color: #333;
            margin-top: 30px;
        }
        h1 {
            border-bottom: 3px solid #007acc;
            padding-bottom: 10px;
        }
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        .metric-card {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #007acc;
        }
        .metric-card.warning {
            border-left-color: #ffa500;
        }
        .metric-card.critical {
            border-left-color: #dc3545;
        }
        .metric-card.success {
            border-left-color: #28a745;
        }
        .metric-value {
            font-size: 2em;
            font-weight: bold;
            color: #007acc;
        }
        .metric-label {
            color: #666;
            font-size: 0.9em;
            margin-top: 5px;
        }
        .metric-change {
            font-size: 0.8em;
            margin-top: 5px;
        }
        .positive {
            color: #28a745;
        }
        .negative {
            color: #dc3545;
        }
        .chart-container {
            position: relative;
            height: 400px;
            margin: 20px 0;
        }
        .analyzer-section {
            margin: 30px 0;
            padding: 20px;
            border: 1px solid #ddd;
            border-radius: 8px;
        }
        .criteria-table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        .criteria-table th,
        .criteria-table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        .criteria-table th {
            background-color: #f8f9fa;
            font-weight: 600;
        }
        .status-pass {
            color: #28a745;
            font-weight: bold;
        }
        .status-fail {
            color: #dc3545;
            font-weight: bold;
        }
        .status-warning {
            color: #ffa500;
            font-weight: bold;
        }
        .timestamp {
            color: #666;
            font-size: 0.9em;
        }
        .nav-tabs {
            display: flex;
            border-bottom: 1px solid #ddd;
            margin: 20px 0 0 0;
        }
        .nav-tab {
            padding: 10px 20px;
            cursor: pointer;
            border: 1px solid #ddd;
            border-bottom: none;
            background: #f8f9fa;
            margin-right: 5px;
        }
        .nav-tab.active {
            background: white;
            border-bottom: 1px solid white;
            margin-bottom: -1px;
        }
        .tab-content {
            display: none;
            padding: 20px 0;
        }
        .tab-content.active {
            display: block;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎯 PBI-16 Baseline Metrics Report</h1>
        <div class="timestamp">Generated: {{ timestamp }}</div>
        
        <h2>📊 Executive Summary</h2>
        <div class="summary-grid">
            <div class="metric-card {{ overall_status_class }}">
                <div class="metric-value">{{ overall_status }}</div>
                <div class="metric-label">Overall Status</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ analyzers_count }}</div>
                <div class="metric-label">Analyzers Tested</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ avg_f1_score }}</div>
                <div class="metric-label">Average F1 Score</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ avg_latency }}ms</div>
                <div class="metric-label">Average Latency</div>
            </div>
        </div>

        <div class="nav-tabs">
            <div class="nav-tab active" onclick="showTab('overview')">Overview</div>
            <div class="nav-tab" onclick="showTab('performance')">Performance</div>
            <div class="nav-tab" onclick="showTab('criteria')">Acceptance Criteria</div>
            <div class="nav-tab" onclick="showTab('details')">Detailed Results</div>
        </div>

        <div id="overview" class="tab-content active">
            <h3>Performance Overview</h3>
            <div class="chart-container">
                <canvas id="performanceChart"></canvas>
            </div>
            
            <h3>📈 Key Metrics Comparison</h3>
            <div class="chart-container">
                <canvas id="metricsChart"></canvas>
            </div>
        </div>

        <div id="performance" class="tab-content">
            <h3>⚡ Performance Analysis</h3>
            <div class="chart-container">
                <canvas id="latencyChart"></canvas>
            </div>
            
            <div class="chart-container">
                <canvas id="throughputChart"></canvas>
            </div>
        </div>

        <div id="criteria" class="tab-content">
            <h3>✅ Acceptance Criteria Validation</h3>
            <table class="criteria-table">
                <thead>
                    <tr>
                        <th>Criterion</th>
                        <th>Target</th>
                        <th>Actual</th>
                        <th>Status</th>
                        <th>Deviation</th>
                    </tr>
                </thead>
                <tbody>
                    {{ criteria_rows }}
                </tbody>
            </table>
        </div>

        <div id="details" class="tab-content">
            <h3>🔍 Detailed Results by Analyzer</h3>
            {{ analyzer_sections }}
        </div>
    </div>

    <script>
        function showTab(tabName) {
            // Hide all tab contents
            const contents = document.querySelectorAll('.tab-content');
            contents.forEach(content => content.classList.remove('active'));
            
            // Remove active class from all tabs
            const tabs = document.querySelectorAll('.nav-tab');
            tabs.forEach(tab => tab.classList.remove('active'));
            
            // Show selected tab content
            document.getElementById(tabName).classList.add('active');
            
            // Add active class to clicked tab
            event.target.classList.add('active');
        }

        // Chart data
        const chartData = {{ chart_data }};
        
        // Performance Overview Chart
        const performanceCtx = document.getElementById('performanceChart').getContext('2d');
        new Chart(performanceCtx, {
            type: 'radar',
            data: {
                labels: ['F1 Score', 'Precision', 'Recall', 'Throughput', 'Latency (inv)', 'Cost (inv)'],
                datasets: chartData.performance_datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    r: {
                        beginAtZero: true,
                        max: 1
                    }
                }
            }
        });

        // Metrics Comparison Chart
        const metricsCtx = document.getElementById('metricsChart').getContext('2d');
        new Chart(metricsCtx, {
            type: 'bar',
            data: {
                labels: chartData.analyzer_names,
                datasets: [
                    {
                        label: 'F1 Score',
                        data: chartData.f1_scores,
                        backgroundColor: 'rgba(54, 162, 235, 0.8)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    },
                    {
                        label: 'Accuracy',
                        data: chartData.accuracies,
                        backgroundColor: 'rgba(75, 192, 192, 0.8)',
                        borderColor: 'rgba(75, 192, 192, 1)',
                        borderWidth: 1
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        max: 1
                    }
                }
            }
        });

        // Latency Chart
        const latencyCtx = document.getElementById('latencyChart').getContext('2d');
        new Chart(latencyCtx, {
            type: 'line',
            data: {
                labels: chartData.analyzer_names,
                datasets: [
                    {
                        label: 'Average Latency (ms)',
                        data: chartData.avg_latencies,
                        borderColor: 'rgba(255, 99, 132, 1)',
                        backgroundColor: 'rgba(255, 99, 132, 0.2)',
                        tension: 0.1
                    },
                    {
                        label: 'P95 Latency (ms)',
                        data: chartData.p95_latencies,
                        borderColor: 'rgba(255, 159, 64, 1)',
                        backgroundColor: 'rgba(255, 159, 64, 0.2)',
                        tension: 0.1
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });

        // Throughput Chart
        const throughputCtx = document.getElementById('throughputChart').getContext('2d');
        new Chart(throughputCtx, {
            type: 'doughnut',
            data: {
                labels: chartData.analyzer_names,
                datasets: [{
                    data: chartData.throughputs,
                    backgroundColor: [
                        'rgba(255, 99, 132, 0.8)',
                        'rgba(54, 162, 235, 0.8)',
                        'rgba(255, 205, 86, 0.8)',
                        'rgba(75, 192, 192, 0.8)',
                        'rgba(153, 102, 255, 0.8)'
                    ]
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom'
                    }
                }
            }
        });
    </script>
</body>
</html>
"""

class HTMLReportGenerator:
    """Generate HTML reports for baseline metrics"""
    
    def __init__(self):
        self.template = HTML_TEMPLATE
    
    def load_baseline_reports(self, baseline_dir: Path) -> Dict[str, Dict]:
        """Load all baseline reports from directory"""
        reports = {}
        baseline_files = list(baseline_dir.glob("*_baseline.json"))
        
        for baseline_file in baseline_files:
            analyzer_name = baseline_file.stem.replace("_baseline", "")
            try:
                with open(baseline_file) as f:
                    reports[analyzer_name] = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError) as e:
                logger.warning(f"Could not load {baseline_file}: {e}")
        
        return reports
    
    def load_validation_report(self, baseline_dir: Path) -> Dict:
        """Load validation report if available"""
        validation_file = baseline_dir / "validation_report.json"
        if validation_file.exists():
            try:
                with open(validation_file) as f:
                    return json.load(f)
            except (FileNotFoundError, json.JSONDecodeError) as e:
                logger.warning(f"Could not load validation report: {e}")
        return {}
    
    def generate_chart_data(self, reports: Dict[str, Dict]) -> Dict:
        """Generate data for charts"""
        analyzer_names = list(reports.keys())
        f1_scores = []
        accuracies = []
        avg_latencies = []
        p95_latencies = []
        throughputs = []
        
        for analyzer, report in reports.items():
            metrics = report.get("baseline_metrics", {})
            f1_scores.append(metrics.get("f1_score", 0))
            accuracies.append(metrics.get("accuracy", 0))
            avg_latencies.append(metrics.get("avg_latency_ms", 0))
            p95_latencies.append(metrics.get("p95_latency_ms", 0))
            throughputs.append(metrics.get("throughput_docs_per_sec", 0))
        
        # Generate performance radar datasets
        performance_datasets = []
        colors = [
            'rgba(255, 99, 132, 0.6)',
            'rgba(54, 162, 235, 0.6)',
            'rgba(255, 205, 86, 0.6)',
            'rgba(75, 192, 192, 0.6)',
            'rgba(153, 102, 255, 0.6)'
        ]
        
        for i, (analyzer, report) in enumerate(reports.items()):
            metrics = report.get("baseline_metrics", {})
            color = colors[i % len(colors)]
            
            # Normalize metrics for radar chart (0-1 scale)
            f1 = metrics.get("f1_score", 0)
            precision = metrics.get("precision", 0)
            recall = metrics.get("recall", 0)
            throughput_norm = min(metrics.get("throughput_docs_per_sec", 0) / 10, 1)  # Normalize to 10 docs/sec max
            latency_inv = max(0, 1 - (metrics.get("avg_latency_ms", 0) / 1000))  # Inverted latency
            cost_inv = max(0, 1 - (metrics.get("estimated_cost_usd", 0) / metrics.get("total_samples", 1) * 1000))  # Inverted cost
            
            performance_datasets.append({
                'label': analyzer,
                'data': [f1, precision, recall, throughput_norm, latency_inv, cost_inv],
                'borderColor': color.replace('0.6', '1'),
                'backgroundColor': color,
                'pointBackgroundColor': color.replace('0.6', '1'),
                'pointBorderColor': '#fff',
                'pointHoverBackgroundColor': '#fff',
                'pointHoverBorderColor': color.replace('0.6', '1')
            })
        
        return {
            'analyzer_names': analyzer_names,
            'f1_scores': f1_scores,
            'accuracies': accuracies,
            'avg_latencies': avg_latencies,
            'p95_latencies': p95_latencies,
            'throughputs': throughputs,
            'performance_datasets': performance_datasets
        }
    
    def generate_criteria_rows(self, validation_report: Dict) -> str:
        """Generate HTML rows for criteria table"""
        if not validation_report:
            return '<tr><td colspan="5">No validation data available</td></tr>'
        
        rows = []
        for analyzer, results in validation_report.get("results_by_analyzer", {}).items():
            for result in results:
                status_class = "status-pass" if result["meets_target"] else "status-fail"
                status_text = "✅ PASS" if result["meets_target"] else "❌ FAIL"
                
                rows.append(f"""
                <tr>
                    <td>{result["criterion"]}</td>
                    <td>{result["target"]}</td>
                    <td>{result["actual"]}</td>
                    <td class="{status_class}">{status_text}</td>
                    <td>{result["deviation"]:.3f}</td>
                </tr>
                """)
        
        return "".join(rows)
    
    def generate_analyzer_sections(self, reports: Dict[str, Dict]) -> str:
        """Generate detailed sections for each analyzer"""
        sections = []
        
        for analyzer, report in reports.items():
            metrics = report.get("baseline_metrics", {})
            
            section = f"""
            <div class="analyzer-section">
                <h4>🔍 {analyzer.title()} Analyzer</h4>
                <div class="summary-grid">
                    <div class="metric-card">
                        <div class="metric-value">{metrics.get("f1_score", 0):.3f}</div>
                        <div class="metric-label">F1 Score</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{metrics.get("avg_latency_ms", 0):.1f}ms</div>
                        <div class="metric-label">Average Latency</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{metrics.get("successful_predictions", 0)}/{metrics.get("total_samples", 0)}</div>
                        <div class="metric-label">Success Rate</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">${(metrics.get("estimated_cost_usd", 0) * 1000 / max(metrics.get("total_samples", 1), 1)):.4f}</div>
                        <div class="metric-label">Cost per 1K docs</div>
                    </div>
                </div>
                
                <h5>Performance Details</h5>
                <table class="criteria-table">
                    <tr><td>Precision</td><td>{metrics.get("precision", 0):.3f}</td></tr>
                    <tr><td>Recall</td><td>{metrics.get("recall", 0):.3f}</td></tr>
                    <tr><td>Accuracy</td><td>{metrics.get("accuracy", 0):.3f}</td></tr>
                    <tr><td>P95 Latency</td><td>{metrics.get("p95_latency_ms", 0):.1f}ms</td></tr>
                    <tr><td>P99 Latency</td><td>{metrics.get("p99_latency_ms", 0):.1f}ms</td></tr>
                    <tr><td>Throughput</td><td>{metrics.get("throughput_docs_per_sec", 0):.2f} docs/sec</td></tr>
                    <tr><td>Total Tokens</td><td>{metrics.get("total_tokens", 0)}</td></tr>
                    <tr><td>Test Duration</td><td>{metrics.get("test_duration_sec", 0):.1f}s</td></tr>
                </table>
            </div>
            """
            sections.append(section)
        
        return "".join(sections)
    
    def generate_html_report(self, baseline_dir: Path, output_file: Path) -> None:
        """Generate complete HTML report"""
        reports = self.load_baseline_reports(baseline_dir)
        validation_report = self.load_validation_report(baseline_dir)
        
        if not reports:
            logger.error("No baseline reports found")
            return
        
        # Calculate summary statistics
        all_f1_scores = [r.get("baseline_metrics", {}).get("f1_score", 0) for r in reports.values()]
        all_latencies = [r.get("baseline_metrics", {}).get("avg_latency_ms", 0) for r in reports.values()]
        
        avg_f1_score = sum(all_f1_scores) / len(all_f1_scores) if all_f1_scores else 0
        avg_latency = sum(all_latencies) / len(all_latencies) if all_latencies else 0
        
        # Determine overall status
        validation_summary = validation_report.get("validation_summary", {})
        overall_status = validation_summary.get("overall_status", "UNKNOWN")
        overall_status_class = {
            "PASS": "success",
            "FAIL": "critical",
            "UNKNOWN": "warning"
        }.get(overall_status, "warning")
        
        # Generate template data
        template_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "overall_status": overall_status,
            "overall_status_class": overall_status_class,
            "analyzers_count": len(reports),
            "avg_f1_score": f"{avg_f1_score:.3f}",
            "avg_latency": f"{avg_latency:.1f}",
            "chart_data": json.dumps(self.generate_chart_data(reports)),
            "criteria_rows": self.generate_criteria_rows(validation_report),
            "analyzer_sections": self.generate_analyzer_sections(reports)
        }
        
        # Render template
        html_content = self.template
        for key, value in template_data.items():
            html_content = html_content.replace("{{ " + key + " }}", str(value))
        
        # Write to file
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Generate HTML report for baseline metrics")
    parser.add_argument("--baseline-dir", required=True, help="Directory containing baseline reports")
    parser.add_argument("--output", default="baseline.html", help="Output HTML file")
    
    args = parser.parse_args()
    
    baseline_dir = Path(args.baseline_dir)
    output_file = Path(args.output)
    
    if not baseline_dir.exists():
        print(f"Error: Baseline directory does not exist: {baseline_dir}")
        return 1
    
    generator = HTMLReportGenerator()
    generator.generate_html_report(baseline_dir, output_file)
    
    print(f"✅ HTML report generated: {output_file}")
    return 0


if __name__ == "__main__":
    exit(main())