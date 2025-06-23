"""
Instrumented DSPy Modules with OPIK Observability

This module provides DSPy modules that are fully instrumented with OPIK tracing
for comprehensive observability of threat intelligence processing pipelines.
"""

import dspy
import time
import json
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

from .enhanced_opik_integration import (
    get_opik_manager, 
    trace_threat_analysis,
    ThreatIntelTraceMetrics,
    OPIK_AVAILABLE
)
from .advanced_dspy_modules import (
    SecurityThreatAnalysis,
    APTAttributionAnalysis, 
    ThreatIntelExtraction,
    ConfidenceScoring
)

@dataclass
class ThreatAnalysisResult:
    """Result container for threat analysis with metrics"""
    threat_classification: str
    severity_assessment: str
    attribution_analysis: str
    reasoning_chain: List[str]
    confidence_score: float
    processing_metrics: ThreatIntelTraceMetrics
    raw_dspy_result: Any = None

class InstrumentedSecurityThreatAnalyzer(dspy.Module):
    """
    Security threat analyzer with comprehensive OPIK instrumentation.
    
    Provides detailed tracing of the analysis pipeline including:
    - Input/output logging
    - Performance metrics
    - Token usage tracking
    - Quality scoring
    """
    
    def __init__(self):
        super().__init__()
        self.analyze = dspy.ChainOfThought(SecurityThreatAnalysis)
        self.opik_manager = get_opik_manager()
    
    @trace_threat_analysis(
        name="SecurityThreatAnalyzer.forward",
        tags=["security_analysis", "chain_of_thought"]
    )
    def forward(self, content: str, context: str = "", analysis_depth: str = "comprehensive"):
        """
        Perform advanced threat analysis with comprehensive tracing.
        """
        start_time = time.time()
        
        # Create OPIK trace for this analysis
        with self.opik_manager.trace_threat_pipeline(
            name="security_threat_analysis",
            input_data={
                "content_length": len(content),
                "context_provided": bool(context),
                "analysis_depth": analysis_depth,
                "content_preview": content[:200] + "..." if len(content) > 200 else content
            },
            tags=["threat_analysis", "dspy", "security"],
            metadata={
                "module": "InstrumentedSecurityThreatAnalyzer",
                "dspy_signature": "SecurityThreatAnalysis"
            }
        ) as trace:
            
            # Create span for DSPy execution
            dspy_span = self.opik_manager.create_span(
                trace,
                "dspy_chain_of_thought_execution",
                span_type="llm",
                input_data={
                    "content": content[:500] + "..." if len(content) > 500 else content,
                    "context": context,
                    "analysis_depth": analysis_depth
                }
            )
            
            try:
                # Execute DSPy analysis
                result = self.analyze(
                    content=content,
                    context=context,
                    analysis_depth=analysis_depth
                )
                
                # End DSPy span with results
                if dspy_span:
                    dspy_span.end(
                        output={
                            "threat_classification": getattr(result, 'threat_classification', 'Unknown'),
                            "severity": getattr(result, 'severity_assessment', 'Unknown'),
                            "confidence": getattr(result, 'confidence_score', 0.0)
                        }
                    )
                
                # Calculate processing metrics
                processing_time = (time.time() - start_time) * 1000
                
                # Estimate token usage (this would be more accurate with actual token counting)
                estimated_tokens = len(content.split()) + len(context.split()) + 100
                estimated_cost = estimated_tokens * 0.00002  # Rough estimate for GPT-4
                
                processing_metrics = ThreatIntelTraceMetrics(
                    processing_time_ms=processing_time,
                    token_count=estimated_tokens,
                    cost_usd=estimated_cost,
                    confidence_score=getattr(result, 'confidence_score', None),
                    threat_level=getattr(result, 'severity_assessment', None),
                    data_source="user_input"
                )
                
                # Log metrics to OPIK
                self.opik_manager.log_metrics(trace, processing_metrics, "security_threat_analyzer")
                
                # Enhanced reasoning chain formatting
                reasoning_chain = getattr(result, 'reasoning_chain', [])
                if isinstance(reasoning_chain, str):
                    reasoning_chain = [step.strip() for step in reasoning_chain.split('\n') if step.strip()]
                
                # Create comprehensive result
                analysis_result = ThreatAnalysisResult(
                    threat_classification=getattr(result, 'threat_classification', 'Unknown'),
                    severity_assessment=getattr(result, 'severity_assessment', 'Unknown'),
                    attribution_analysis=getattr(result, 'attribution_analysis', 'Unknown'),
                    reasoning_chain=reasoning_chain,
                    confidence_score=getattr(result, 'confidence_score', 0.0),
                    processing_metrics=processing_metrics,
                    raw_dspy_result=result
                )
                
                return analysis_result
                
            except Exception as e:
                if dspy_span:
                    dspy_span.end(
                        output={"error": str(e)},
                        metadata={"error_type": type(e).__name__}
                    )
                raise

class InstrumentedAPTAttributionAnalyzer(dspy.Module):
    """
    APT attribution analyzer with OPIK instrumentation and ReAct reasoning.
    """
    
    def __init__(self):
        super().__init__()
        self.analyze = dspy.ChainOfThought(APTAttributionAnalysis)
        self.opik_manager = get_opik_manager()
    
    @trace_threat_analysis(
        name="APTAttributionAnalyzer.forward",
        tags=["apt_attribution", "chain_of_thought"]
    )
    def forward(self, indicators: str, context: str = ""):
        """
        Perform APT attribution analysis with Chain of Thought reasoning and full tracing.
        """
        start_time = time.time()
        
        with self.opik_manager.trace_threat_pipeline(
            name="apt_attribution_analysis",
            input_data={
                "indicators_length": len(indicators),
                "context_provided": bool(context),
                "indicators_preview": indicators[:300] + "..." if len(indicators) > 300 else indicators
            },
            tags=["apt_attribution", "dspy", "chain_of_thought"],
            metadata={
                "module": "InstrumentedAPTAttributionAnalyzer",
                "dspy_signature": "APTAttributionAnalysis",
                "reasoning_type": "ChainOfThought"
            }
        ) as trace:
            
            # Create span for Chain of Thought execution
            cot_span = self.opik_manager.create_span(
                trace,
                "dspy_chain_of_thought_reasoning",
                span_type="llm",
                input_data={
                    "indicators": indicators[:400] + "..." if len(indicators) > 400 else indicators,
                    "context": context
                }
            )
            
            try:
                result = self.analyze(
                    indicators=indicators,
                    context=context
                )
                
                # Process alternative actors
                alternative_actors = getattr(result, 'alternative_actors', [])
                if isinstance(alternative_actors, str):
                    alternative_actors = [actor.strip() for actor in alternative_actors.split(',') if actor.strip()]
                result.alternative_actors = alternative_actors
                
                # End Chain of Thought span
                if cot_span:
                    cot_span.end(
                        output={
                            "attribution": getattr(result, 'attribution', 'Unknown'),
                            "confidence": getattr(result, 'confidence', 0.0),
                            "alternative_count": len(alternative_actors)
                        }
                    )
                
                # Calculate metrics
                processing_time = (time.time() - start_time) * 1000
                estimated_tokens = len(indicators.split()) + len(context.split()) + 150
                estimated_cost = estimated_tokens * 0.00002
                
                processing_metrics = ThreatIntelTraceMetrics(
                    processing_time_ms=processing_time,
                    token_count=estimated_tokens,
                    cost_usd=estimated_cost,
                    confidence_score=getattr(result, 'confidence', None),
                    data_source="threat_indicators"
                )
                
                self.opik_manager.log_metrics(trace, processing_metrics, "apt_attribution_analyzer")
                
                return result
                
            except Exception as e:
                if cot_span:
                    cot_span.end(
                        output={"error": str(e)},
                        metadata={"error_type": type(e).__name__}
                    )
                raise

class InstrumentedThreatIntelExtractor(dspy.Module):
    """
    Threat intelligence extractor with validation and comprehensive tracing.
    """
    
    def __init__(self):
        super().__init__()
        self.extract = dspy.ChainOfThought(ThreatIntelExtraction)
        self.opik_manager = get_opik_manager()
    
    @trace_threat_analysis(
        name="ThreatIntelExtractor.forward",
        tags=["intel_extraction", "validation"]
    )
    def forward(self, raw_content: str):
        """
        Extract threat intelligence with validation and full observability.
        """
        start_time = time.time()
        
        with self.opik_manager.trace_threat_pipeline(
            name="threat_intel_extraction",
            input_data={
                "content_length": len(raw_content),
                "content_preview": raw_content[:400] + "..." if len(raw_content) > 400 else raw_content
            },
            tags=["intel_extraction", "dspy", "ioc"],
            metadata={
                "module": "InstrumentedThreatIntelExtractor",
                "dspy_signature": "ThreatIntelExtraction"
            }
        ) as trace:
            
            # Create extraction span
            extraction_span = self.opik_manager.create_span(
                trace,
                "ioc_extraction_process",
                span_type="llm",
                input_data={"raw_content": raw_content[:500] + "..." if len(raw_content) > 500 else raw_content}
            )
            
            try:
                result = self.extract(raw_content=raw_content)
                
                # Process extracted IOCs
                extracted_iocs = getattr(result, 'extracted_iocs', [])
                if isinstance(extracted_iocs, str):
                    extracted_iocs = [ioc.strip() for ioc in extracted_iocs.split('\n') if ioc.strip()]
                result.extracted_iocs = extracted_iocs
                
                # End extraction span
                if extraction_span:
                    extraction_span.end(
                        output={
                            "ioc_count": len(extracted_iocs),
                            "confidence": getattr(result, 'confidence', 0.0),
                            "threat_summary": getattr(result, 'threat_summary', 'N/A')[:200]
                        }
                    )
                
                # Validate IOCs (basic validation span)
                validation_span = self.opik_manager.create_span(
                    trace,
                    "ioc_validation",
                    span_type="processing",
                    input_data={"ioc_count": len(extracted_iocs)}
                )
                
                # Simple IOC validation logic
                valid_iocs = []
                for ioc in extracted_iocs:
                    if self._is_valid_ioc(ioc):
                        valid_iocs.append(ioc)
                
                if validation_span:
                    validation_span.end(
                        output={
                            "valid_iocs": len(valid_iocs),
                            "invalid_iocs": len(extracted_iocs) - len(valid_iocs),
                            "validation_rate": len(valid_iocs) / max(len(extracted_iocs), 1)
                        }
                    )
                
                # Calculate metrics
                processing_time = (time.time() - start_time) * 1000
                estimated_tokens = len(raw_content.split()) + 120
                estimated_cost = estimated_tokens * 0.00002
                
                processing_metrics = ThreatIntelTraceMetrics(
                    processing_time_ms=processing_time,
                    token_count=estimated_tokens,
                    cost_usd=estimated_cost,
                    confidence_score=getattr(result, 'confidence', None),
                    ioc_count=len(valid_iocs),
                    data_source="raw_intel"
                )
                
                self.opik_manager.log_metrics(trace, processing_metrics, "threat_intel_extractor")
                
                # Update result with validated IOCs
                result.extracted_iocs = valid_iocs
                
                return result
                
            except Exception as e:
                if extraction_span:
                    extraction_span.end(
                        output={"error": str(e)},
                        metadata={"error_type": type(e).__name__}
                    )
                raise
    
    def _is_valid_ioc(self, ioc: str) -> bool:
        """Basic IOC validation logic"""
        import re
        
        # Simple patterns for common IOCs
        ip_pattern = r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
        domain_pattern = r'\b[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
        hash_pattern = r'\b[a-fA-F0-9]{32,64}\b'
        
        if (re.match(ip_pattern, ioc) or 
            re.match(domain_pattern, ioc) or 
            re.match(hash_pattern, ioc)):
            return True
        
        return False

class InstrumentedAdvancedThreatAnalyzer(dspy.Module):
    """
    Comprehensive threat analyzer that orchestrates multiple analysis components
    with full OPIK observability across the entire pipeline.
    """
    
    def __init__(self):
        super().__init__()
        self.threat_analyzer = InstrumentedSecurityThreatAnalyzer()
        self.apt_analyzer = InstrumentedAPTAttributionAnalyzer()
        self.intel_extractor = InstrumentedThreatIntelExtractor()
        self.opik_manager = get_opik_manager()
    
    @trace_threat_analysis(
        name="AdvancedThreatAnalyzer.forward",
        tags=["comprehensive_analysis", "orchestration"]
    )
    def forward(self, content: str, context: str = "", analysis_depth: str = "comprehensive"):
        """
        Perform comprehensive threat analysis with full pipeline tracing.
        """
        start_time = time.time()
        
        with self.opik_manager.trace_threat_pipeline(
            name="comprehensive_threat_analysis",
            input_data={
                "content_length": len(content),
                "context_provided": bool(context),
                "analysis_depth": analysis_depth
            },
            tags=["comprehensive_analysis", "pipeline", "orchestration"],
            metadata={
                "module": "InstrumentedAdvancedThreatAnalyzer",
                "pipeline_version": "v2.0",
                "components": ["threat_analyzer", "apt_analyzer", "intel_extractor"]
            }
        ) as trace:
            
            results = {
                "threat_analysis": None,
                "apt_attribution": None,
                "intel_extraction": None,
                "pipeline_metrics": None,
                "reasoning_chain": []
            }
            
            try:
                # Step 1: Basic threat analysis
                threat_span = self.opik_manager.create_span(
                    trace, "threat_analysis_step", "processing"
                )
                
                results["threat_analysis"] = self.threat_analyzer(
                    content=content,
                    context=context,
                    analysis_depth=analysis_depth
                )
                
                if threat_span:
                    threat_span.end(output={"status": "completed"})
                
                results["reasoning_chain"].append(("threat_analysis", "completed"))
                
                # Step 2: APT attribution if security threat detected
                threat_classification = results["threat_analysis"].threat_classification
                if threat_classification.lower() not in ['none', 'benign']:
                    
                    apt_span = self.opik_manager.create_span(
                        trace, "apt_attribution_step", "processing"
                    )
                    
                    indicators = f"Threat type: {threat_classification}, Content: {content[:500]}"
                    results["apt_attribution"] = self.apt_analyzer(
                        indicators=indicators,
                        context=context
                    )
                    
                    if apt_span:
                        apt_span.end(output={"status": "completed"})
                    
                    results["reasoning_chain"].append(("apt_attribution", "completed"))
                
                # Step 3: Extract threat intelligence
                intel_span = self.opik_manager.create_span(
                    trace, "intel_extraction_step", "processing"
                )
                
                results["intel_extraction"] = self.intel_extractor(
                    raw_content=content
                )
                
                if intel_span:
                    intel_span.end(output={"status": "completed"})
                
                results["reasoning_chain"].append(("intel_extraction", "completed"))
                
                # Calculate overall pipeline metrics
                total_processing_time = (time.time() - start_time) * 1000
                
                # Aggregate metrics from components
                total_tokens = sum([
                    results["threat_analysis"].processing_metrics.token_count,
                    getattr(results["apt_attribution"], 'token_count', 0) if results["apt_attribution"] else 0,
                    results["intel_extraction"].token_count if hasattr(results["intel_extraction"], 'token_count') else 0
                ])
                
                total_cost = sum([
                    results["threat_analysis"].processing_metrics.cost_usd,
                    0.0,  # Would calculate from apt_attribution if available
                    0.0   # Would calculate from intel_extraction if available
                ])
                
                pipeline_metrics = ThreatIntelTraceMetrics(
                    processing_time_ms=total_processing_time,
                    token_count=total_tokens,
                    cost_usd=total_cost,
                    confidence_score=results["threat_analysis"].confidence_score,
                    ioc_count=len(getattr(results["intel_extraction"], 'extracted_iocs', [])),
                    threat_level=results["threat_analysis"].severity_assessment,
                    data_source="comprehensive_pipeline"
                )
                
                results["pipeline_metrics"] = pipeline_metrics
                
                # Log overall pipeline metrics
                self.opik_manager.log_metrics(trace, pipeline_metrics, "comprehensive_pipeline")
                
                return results
                
            except Exception as e:
                results["reasoning_chain"].append(("error", f"Pipeline failed: {str(e)}"))
                raise

# Factory function for creating instrumented modules
def create_instrumented_threat_analyzer(analyzer_type: str = "comprehensive"):
    """
    Factory function to create instrumented threat analyzer modules.
    
    Args:
        analyzer_type: Type of analyzer to create
                      ("security", "apt", "intel", "comprehensive")
    
    Returns:
        Instrumented DSPy module with OPIK tracing
    """
    analyzers = {
        "security": InstrumentedSecurityThreatAnalyzer,
        "apt": InstrumentedAPTAttributionAnalyzer,
        "intel": InstrumentedThreatIntelExtractor,
        "comprehensive": InstrumentedAdvancedThreatAnalyzer
    }
    
    if analyzer_type not in analyzers:
        raise ValueError(f"Unknown analyzer type: {analyzer_type}. Available: {list(analyzers.keys())}")
    
    return analyzers[analyzer_type]()

# Module initialization helper
def setup_instrumented_dspy_pipeline(
    lm: Any,
    analyzer_types: List[str] = ["comprehensive"],
    opik_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Set up a complete instrumented DSPy pipeline with OPIK tracing.
    
    Args:
        lm: DSPy language model instance
        analyzer_types: List of analyzer types to create
        opik_config: Optional OPIK configuration overrides
        
    Returns:
        Dictionary containing analyzers and OPIK manager
    """
    from .enhanced_opik_integration import OpikConfig, OpikIntegrationManager
    
    # Initialize OPIK with custom config if provided
    if opik_config:
        config = OpikConfig(**opik_config)
        manager = OpikIntegrationManager(config)
    else:
        manager = get_opik_manager()
    
    # Configure DSPy with OPIK
    manager.configure_dspy(lm)
    
    # Create requested analyzers
    analyzers = {}
    for analyzer_type in analyzer_types:
        analyzers[analyzer_type] = create_instrumented_threat_analyzer(analyzer_type)
    
    return {
        "analyzers": analyzers,
        "opik_manager": manager,
        "dashboard_url": manager.get_dashboard_url(),
        "health_status": manager.health_check()
    }

# Export main classes and functions
__all__ = [
    "InstrumentedSecurityThreatAnalyzer",
    "InstrumentedAPTAttributionAnalyzer", 
    "InstrumentedThreatIntelExtractor",
    "InstrumentedAdvancedThreatAnalyzer",
    "ThreatAnalysisResult",
    "create_instrumented_threat_analyzer",
    "setup_instrumented_dspy_pipeline"
]