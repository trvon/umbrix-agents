"""
Advanced DSPy Modules for Enhanced Threat Intelligence Analysis

This module provides sophisticated DSPy signatures and modules that implement
transformer-like reasoning through advanced prompt engineering techniques.

Key Features:
- Multi-step reasoning chains with ChainOfThought
- ReAct-based dynamic investigation
- Confidence scoring and uncertainty quantification
- Virtual attention mechanisms through prompt engineering
- Modular components for threat analysis pipeline
"""

import dspy
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


# =============================================================================
# Core DSPy Signatures
# =============================================================================

class SecurityThreatAnalysis(dspy.Signature):
    """Comprehensive security threat analysis with reasoning chains."""
    
    content: str = dspy.InputField(desc="Raw content to analyze for security threats")
    context: str = dspy.InputField(desc="Additional context and metadata about the content")
    analysis_depth: str = dspy.InputField(desc="Depth of analysis required (basic/comprehensive/deep)")
    
    threat_classification: str = dspy.OutputField(desc="Type of threat identified (e.g., 'APT Campaign', 'Ransomware', 'Phishing')")
    severity_assessment: str = dspy.OutputField(desc="Severity level (Critical/High/Medium/Low)")
    attribution_analysis: str = dspy.OutputField(desc="Attribution assessment and confidence")
    reasoning_chain: List[str] = dspy.OutputField(desc="Step-by-step reasoning process used")
    confidence_score: float = dspy.OutputField(desc="Overall confidence in analysis (0.0-1.0)")


class APTAttributionAnalysis(dspy.Signature):
    """Advanced APT attribution analysis with evidence synthesis."""
    
    indicators: str = dspy.InputField(desc="Technical indicators and patterns observed")
    context: str = dspy.InputField(desc="Contextual information about the attack")
    
    attribution: str = dspy.OutputField(desc="Most likely threat actor or group")
    confidence: float = dspy.OutputField(desc="Attribution confidence (0.0-1.0)")
    reasoning: str = dspy.OutputField(desc="Evidence-based reasoning for attribution")
    alternative_actors: List[str] = dspy.OutputField(desc="Other possible threat actors")


class ThreatIntelExtraction(dspy.Signature):
    """Extract and validate threat intelligence indicators."""
    
    raw_content: str = dspy.InputField(desc="Raw content containing potential threat intelligence")
    
    extracted_iocs: List[str] = dspy.OutputField(desc="Valid indicators of compromise extracted")
    threat_summary: str = dspy.OutputField(desc="Summary of the threat described")
    confidence: float = dspy.OutputField(desc="Extraction confidence (0.0-1.0)")
    validation_notes: str = dspy.OutputField(desc="Notes on IOC validation and quality")


class ConfidenceScoring(dspy.Signature):
    """Advanced confidence scoring with uncertainty quantification."""
    
    analysis_result: str = dspy.InputField(desc="Analysis result to score")
    factors: List[str] = dspy.InputField(desc="Factors that influence confidence")
    
    confidence_score: float = dspy.OutputField(desc="Overall confidence score (0.0-1.0)")
    uncertainty_factors: List[str] = dspy.OutputField(desc="Sources of uncertainty")
    confidence_breakdown: Dict[str, float] = dspy.OutputField(desc="Confidence by analysis component")


# =============================================================================
# Advanced DSPy Modules
# =============================================================================

class SecurityThreatAnalyzer(dspy.Module):
    """
    Advanced security threat analyzer using Chain of Thought reasoning.
    
    This module implements sophisticated threat analysis with step-by-step
    reasoning chains, providing transparency into the analysis process.
    """
    
    def __init__(self):
        super().__init__()
        self.analyze = dspy.ChainOfThought(SecurityThreatAnalysis)
    
    def forward(self, content: str, context: str = "", analysis_depth: str = "comprehensive"):
        """
        Perform advanced threat analysis with reasoning chains.
        
        Args:
            content: Raw content to analyze
            context: Additional context information
            analysis_depth: Required depth of analysis
            
        Returns:
            Analysis result with reasoning chain
        """
        result = self.analyze(
            content=content,
            context=context,
            analysis_depth=analysis_depth
        )
        
        # Enhanced reasoning chain formatting
        if hasattr(result, 'reasoning_chain') and isinstance(result.reasoning_chain, str):
            # Convert string to list if needed
            result.reasoning_chain = [step.strip() for step in result.reasoning_chain.split('\n') if step.strip()]
        
        return result


class APTAttributionAnalyzer(dspy.Module):
    """
    Advanced APT attribution analyzer using ReAct reasoning.
    
    This module uses ReAct (Reasoning + Acting) to dynamically investigate
    threat actor attribution based on available evidence.
    """
    
    def __init__(self):
        super().__init__()
        # ReAct has API changes, use ChainOfThought as more stable alternative
        self.analyze = dspy.ChainOfThought(APTAttributionAnalysis)
    
    def forward(self, indicators: str, context: str = ""):
        """
        Perform APT attribution analysis with dynamic reasoning.
        
        Args:
            indicators: Technical indicators and patterns
            context: Contextual information
            
        Returns:
            Attribution analysis with confidence scoring
        """
        result = self.analyze(
            indicators=indicators,
            context=context
        )
        
        # Ensure alternative_actors is a list
        if hasattr(result, 'alternative_actors') and isinstance(result.alternative_actors, str):
            result.alternative_actors = [actor.strip() for actor in result.alternative_actors.split(',') if actor.strip()]
        
        return result


class ThreatIntelExtractor(dspy.Module):
    """
    Advanced threat intelligence extractor with validation.
    
    This module extracts and validates threat intelligence indicators
    from raw content using sophisticated pattern recognition.
    """
    
    def __init__(self):
        super().__init__()
        self.extract = dspy.ChainOfThought(ThreatIntelExtraction)
    
    def forward(self, raw_content: str):
        """
        Extract threat intelligence with validation.
        
        Args:
            raw_content: Raw content to extract from
            
        Returns:
            Extracted and validated threat intelligence
        """
        result = self.extract(raw_content=raw_content)
        
        # Ensure extracted_iocs is a list
        if hasattr(result, 'extracted_iocs') and isinstance(result.extracted_iocs, str):
            result.extracted_iocs = [ioc.strip() for ioc in result.extracted_iocs.split('\n') if ioc.strip()]
        
        return result


class ConfidenceScorer(dspy.Module):
    """
    Advanced confidence scorer with uncertainty quantification.
    
    This module provides sophisticated confidence scoring that considers
    multiple factors and quantifies uncertainty in analysis results.
    """
    
    def __init__(self):
        super().__init__()
        self.score = dspy.ChainOfThought(ConfidenceScoring)
    
    def forward(self, analysis_result: str, factors: List[str]):
        """
        Score confidence with uncertainty quantification.
        
        Args:
            analysis_result: Result to score
            factors: Factors influencing confidence
            
        Returns:
            Detailed confidence assessment
        """
        # Convert factors list to string for DSPy
        factors_str = ", ".join(factors) if isinstance(factors, list) else str(factors)
        
        result = self.score(
            analysis_result=analysis_result,
            factors=factors_str
        )
        
        # Parse confidence breakdown if it's a string
        if hasattr(result, 'confidence_breakdown') and isinstance(result.confidence_breakdown, str):
            try:
                # Simple parsing for demo purposes
                breakdown = {}
                for item in result.confidence_breakdown.split(','):
                    if ':' in item:
                        key, value = item.split(':', 1)
                        breakdown[key.strip()] = float(value.strip())
                result.confidence_breakdown = breakdown
            except:
                result.confidence_breakdown = {"overall": result.confidence_score}
        
        # Ensure uncertainty_factors is a list
        if hasattr(result, 'uncertainty_factors') and isinstance(result.uncertainty_factors, str):
            result.uncertainty_factors = [factor.strip() for factor in result.uncertainty_factors.split(',') if factor.strip()]
        
        return result


# =============================================================================
# Composite Analysis Module
# =============================================================================

class AdvancedThreatAnalyzer(dspy.Module):
    """
    Composite module that orchestrates multiple analysis components.
    
    This module combines security threat analysis, APT attribution,
    threat intelligence extraction, and confidence scoring into a
    comprehensive threat analysis pipeline.
    """
    
    def __init__(self):
        super().__init__()
        self.threat_analyzer = SecurityThreatAnalyzer()
        self.apt_analyzer = APTAttributionAnalyzer()
        self.intel_extractor = ThreatIntelExtractor()
        self.confidence_scorer = ConfidenceScorer()
    
    def forward(self, content: str, context: str = "", analysis_depth: str = "comprehensive"):
        """
        Perform comprehensive threat analysis using all components.
        
        Args:
            content: Raw content to analyze
            context: Additional context information
            analysis_depth: Required depth of analysis
            
        Returns:
            Comprehensive analysis result with all components
        """
        # Create analysis result container
        @dataclass
        class ComprehensiveAnalysisResult:
            threat_analysis: Any = None
            apt_attribution: Any = None
            intel_extraction: Any = None
            confidence_assessment: Any = None
            reasoning_chain: List[tuple] = None
            
        result = ComprehensiveAnalysisResult()
        result.reasoning_chain = []
        
        try:
            # Step 1: Basic threat analysis
            result.threat_analysis = self.threat_analyzer(
                content=content,
                context=context,
                analysis_depth=analysis_depth
            )
            result.reasoning_chain.append(("threat_analysis", "completed"))
            
            # Step 2: APT attribution if security threat detected
            if (hasattr(result.threat_analysis, 'threat_classification') and 
                result.threat_analysis.threat_classification.lower() not in ['none', 'benign']):
                
                indicators = f"Threat type: {result.threat_analysis.threat_classification}, Content: {content[:500]}"
                result.apt_attribution = self.apt_analyzer(
                    indicators=indicators,
                    context=context
                )
                result.reasoning_chain.append(("apt_attribution", "completed"))
            
            # Step 3: Extract threat intelligence
            result.intel_extraction = self.intel_extractor(
                raw_content=content
            )
            result.reasoning_chain.append(("intel_extraction", "completed"))
            
            # Step 4: Overall confidence assessment
            analysis_summary = f"Threat: {getattr(result.threat_analysis, 'threat_classification', 'Unknown')}"
            if result.apt_attribution:
                analysis_summary += f", Attribution: {getattr(result.apt_attribution, 'attribution', 'Unknown')}"
            
            confidence_factors = [
                "content_quality",
                "indicator_validation",
                "contextual_evidence"
            ]
            
            result.confidence_assessment = self.confidence_scorer(
                analysis_result=analysis_summary,
                factors=confidence_factors
            )
            result.reasoning_chain.append(("confidence_assessment", "completed"))
            
        except Exception as e:
            result.reasoning_chain.append(("error", f"Analysis failed: {str(e)}"))
        
        return result


# =============================================================================
# Virtual Attention Mechanisms
# =============================================================================

class VirtualAttentionAnalyzer(dspy.Module):
    """
    Virtual attention mechanism that highlights important content segments.
    
    This module simulates transformer attention by identifying and weighting
    the most important parts of the input content for analysis decisions.
    """
    
    def __init__(self):
        super().__init__()
        self.attention_signature = dspy.Signature(
            "content -> key_phrases, attention_weights, reasoning",
            doc="Identify key phrases and their importance weights in security content"
        )
        self.analyze_attention = dspy.ChainOfThought(self.attention_signature)
    
    def forward(self, content: str, max_phrases: int = 10):
        """
        Analyze content and provide attention weights for key phrases.
        
        Args:
            content: Content to analyze
            max_phrases: Maximum number of key phrases to identify
            
        Returns:
            Attention analysis with key phrases and weights
        """
        result = self.analyze_attention(content=content)
        
        # Parse key phrases and weights
        try:
            phrases = []
            weights = []
            
            if hasattr(result, 'key_phrases') and hasattr(result, 'attention_weights'):
                phrase_list = result.key_phrases.split(',') if isinstance(result.key_phrases, str) else result.key_phrases
                weight_list = result.attention_weights.split(',') if isinstance(result.attention_weights, str) else result.attention_weights
                
                for i, phrase in enumerate(phrase_list[:max_phrases]):
                    phrases.append(phrase.strip())
                    if i < len(weight_list):
                        try:
                            weights.append(float(weight_list[i].strip()))
                        except:
                            weights.append(0.5)  # Default weight
                    else:
                        weights.append(0.5)
            
            # Normalize weights
            if weights:
                max_weight = max(weights)
                if max_weight > 0:
                    weights = [w / max_weight for w in weights]
            
            return {
                'key_phrases': phrases,
                'attention_weights': weights,
                'reasoning': getattr(result, 'reasoning', 'Attention analysis completed')
            }
            
        except Exception as e:
            return {
                'key_phrases': [],
                'attention_weights': [],
                'reasoning': f"Attention analysis failed: {str(e)}"
            }


# =============================================================================
# Module Factory and Registry
# =============================================================================

class DSPyModuleRegistry:
    """Registry for managing and creating DSPy modules."""
    
    _modules = {
        'security_threat_analyzer': SecurityThreatAnalyzer,
        'apt_attribution_analyzer': APTAttributionAnalyzer,
        'threat_intel_extractor': ThreatIntelExtractor,
        'confidence_scorer': ConfidenceScorer,
        'advanced_threat_analyzer': AdvancedThreatAnalyzer,
        'virtual_attention_analyzer': VirtualAttentionAnalyzer
    }
    
    @classmethod
    def create_module(cls, module_name: str, **kwargs):
        """Create a module instance by name."""
        if module_name not in cls._modules:
            raise ValueError(f"Unknown module: {module_name}")
        
        module_class = cls._modules[module_name]
        return module_class(**kwargs)
    
    @classmethod
    def list_modules(cls) -> List[str]:
        """List available module names."""
        return list(cls._modules.keys())
    
    @classmethod
    def register_module(cls, name: str, module_class):
        """Register a new module class."""
        cls._modules[name] = module_class


# =============================================================================
# Usage Examples and Testing
# =============================================================================

if __name__ == "__main__":
    # Example usage of the advanced DSPy modules
    
    # Note: In practice, you would configure DSPy with a real LLM
    # dspy.settings.configure(lm=your_llm_instance)
    
    print("Advanced DSPy Modules for Threat Intelligence Analysis")
    print("=" * 60)
    
    # Create module instances
    threat_analyzer = SecurityThreatAnalyzer()
    apt_analyzer = APTAttributionAnalyzer()
    intel_extractor = ThreatIntelExtractor()
    confidence_scorer = ConfidenceScorer()
    advanced_analyzer = AdvancedThreatAnalyzer()
    attention_analyzer = VirtualAttentionAnalyzer()
    
    print(f"✓ Created {len(DSPyModuleRegistry.list_modules())} advanced DSPy modules")
    print("Available modules:", ", ".join(DSPyModuleRegistry.list_modules()))
    
    # Example content for analysis
    sample_content = """
    New APT29 campaign discovered targeting government agencies.
    The threat actors are using sophisticated malware with multiple
    zero-day exploits. IOCs include evil-domain.com and 192.168.1.100.
    Attribution confidence is high based on TTPs and infrastructure overlap.
    """
    
    print(f"\nSample content: {sample_content[:100]}...")
    print("Modules ready for optimization and production use.")
    
    # Test module registry
    registry_module = DSPyModuleRegistry.create_module('security_threat_analyzer')
    print(f"✓ Registry successfully created module: {type(registry_module).__name__}")