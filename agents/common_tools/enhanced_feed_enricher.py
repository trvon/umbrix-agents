"""
Enhanced Feed Enricher with DSPy Optimization Integration

This module shows how to integrate the accuracy improvements into the live system
to handle various types of content including non-security articles.
"""

import dspy
import json
import logging
from typing import Dict, Any, List, Optional
from .models.feed_record import FeedRecord
from .dspy_optimization import ThreatAnalysisOptimizer
from .advanced_dspy_modules import AdvancedThreatAnalyzer
from .dspy_signatures import EnrichedFeedData
from .intelligent_content_analyzer import IntelligentContentAnalyzer, ContentAnalysisResult
# Import optimization manager when needed to avoid circular imports
def _get_optimization_manager():
    """Lazy import of optimization manager to avoid circular dependency."""
    try:
        from .dspy_optimization_manager import get_optimized_module
        return get_optimized_module
    except ImportError:
        return None

logger = logging.getLogger(__name__)


class EnhancedFeedEnricher:
    """
    Enhanced feed enricher that integrates DSPy optimization for improved accuracy
    on all types of content, not just security-focused articles.
    """
    
    def __init__(self, use_optimized: bool = True, fallback_enabled: bool = True):
        """
        Initialize enhanced enricher with optimization support
        
        Args:
            use_optimized: Whether to use optimized modules (requires training)
            fallback_enabled: Whether to fallback to basic enrichment on errors
        """
        self.use_optimized = use_optimized
        self.fallback_enabled = fallback_enabled
        
        # Basic enricher (current implementation)
        self.basic_enricher = dspy.ChainOfThought(EnrichedFeedData)
        
        # Advanced analyzer (new implementation)
        self.advanced_analyzer = None
        if use_optimized:
            self.advanced_analyzer = self._load_optimized_analyzer()
        
        # Optimized DSPy modules from disk cache
        self.optimized_modules = self._load_optimized_modules() if use_optimized else {}
        
        # Intelligent content analyzer for sophisticated detection
        self.content_analyzer = IntelligentContentAnalyzer()
    
    def _load_optimized_analyzer(self) -> Optional[AdvancedThreatAnalyzer]:
        """Load optimized analyzer from disk cache using optimization manager"""
        try:
            # Load from optimization manager cache
            get_optimized_module = _get_optimization_manager()
            if not get_optimized_module:
                return None
            analyzer = get_optimized_module(
                'advanced_threat_analyzer', 
                AdvancedThreatAnalyzer
            )
            
            if analyzer:
                logger.info("Loaded optimized threat analyzer from cache")
            else:
                logger.info("Using basic threat analyzer (optimization not available)")
            
            return analyzer
            
        except Exception as e:
            logger.warning(f"Failed to load optimized analyzer: {e}")
            return None
    
    def _load_optimized_modules(self) -> Dict[str, Any]:
        """Load all optimized DSPy modules from disk cache"""
        modules = {}
        
        module_mappings = {
            'security_threat_analyzer': ('SecurityThreatAnalyzer', 'security threat analysis'),
            'apt_attribution_analyzer': ('APTAttributionAnalyzer', 'APT attribution'),
            'threat_intel_extractor': ('ThreatIntelExtractor', 'threat intelligence extraction'),
            'confidence_scorer': ('ConfidenceScorer', 'confidence scoring')
        }
        
        for module_name, (class_name, description) in module_mappings.items():
            try:
                # Get optimized module from cache
                get_optimized_module = _get_optimization_manager()
                module = get_optimized_module(module_name) if get_optimized_module else None
                
                if module:
                    modules[module_name] = module
                    logger.info(f"Loaded optimized {description} module")
                else:
                    logger.debug(f"No optimized module available for {description}")
                    
            except Exception as e:
                logger.warning(f"Failed to load optimized {description} module: {e}")
        
        logger.info(f"Loaded {len(modules)} optimized DSPy modules from cache")
        return modules
    
    def _run_optimized_analysis(self, content: str, context_info: Dict, analysis: ContentAnalysisResult):
        """
        Run analysis using optimized DSPy modules from cache
        
        This method coordinates multiple optimized modules to perform
        comprehensive threat analysis with improved accuracy.
        """
        
        # Mock analysis result structure for compatibility
        class OptimizedAnalysisResult:
            def __init__(self):
                self.threat_analysis = None
                self.confidence_synthesis = None
                self.ioc_validation = None
                self.reasoning_chain = []
                self.attention_analysis = None
        
        result = OptimizedAnalysisResult()
        
        try:
            # Use Security Threat Analyzer if available
            if 'security_threat_analyzer' in self.optimized_modules:
                threat_analyzer = self.optimized_modules['security_threat_analyzer']
                
                # Run threat analysis with optimized module
                threat_result = threat_analyzer(
                    content=content,
                    context=json.dumps(context_info)
                )
                
                # Create mock threat analysis structure
                class ThreatAnalysis:
                    def __init__(self, threat_result):
                        # Extract from optimized module result
                        if hasattr(threat_result, 'threat_type'):
                            self.threat_classification = threat_result.threat_type
                        else:
                            self.threat_classification = "Unknown Threat"
                        
                        if hasattr(threat_result, 'severity'):
                            self.severity_assessment = threat_result.severity
                        else:
                            self.severity_assessment = "Medium"
                        
                        if hasattr(threat_result, 'attribution'):
                            self.attribution_analysis = threat_result.attribution
                        else:
                            self.attribution_analysis = "Attribution under investigation"
                
                result.threat_analysis = ThreatAnalysis(threat_result)
                result.reasoning_chain.append(("security_threat_analysis", "completed"))
            
            # Use APT Attribution Analyzer if available
            if 'apt_attribution_analyzer' in self.optimized_modules:
                apt_analyzer = self.optimized_modules['apt_attribution_analyzer']
                
                apt_result = apt_analyzer(
                    indicators=json.dumps(analysis.detected_entities),
                    context=content[:500]  # Truncate for performance
                )
                
                if result.threat_analysis:
                    if hasattr(apt_result, 'attribution'):
                        result.threat_analysis.attribution_analysis = apt_result.attribution
                
                result.reasoning_chain.append(("apt_attribution_analysis", "completed"))
            
            # Use Threat Intel Extractor if available
            if 'threat_intel_extractor' in self.optimized_modules:
                intel_extractor = self.optimized_modules['threat_intel_extractor']
                
                intel_result = intel_extractor(
                    raw_content=content
                )
                
                # Create mock IOC validation structure
                class IOCValidation:
                    def __init__(self, intel_result):
                        if hasattr(intel_result, 'extracted_iocs'):
                            self.validated_iocs = intel_result.extracted_iocs
                        else:
                            self.validated_iocs = []
                
                result.ioc_validation = IOCValidation(intel_result)
                result.reasoning_chain.append(("threat_intel_extraction", "completed"))
            
            # Use Confidence Scorer if available
            if 'confidence_scorer' in self.optimized_modules:
                confidence_scorer = self.optimized_modules['confidence_scorer']
                
                confidence_result = confidence_scorer(
                    analysis_result=f"Threat analysis completed for content type: {analysis.content_type}",
                    factors=list(analysis.security_indicators.keys())
                )
                
                # Create mock confidence synthesis structure
                class ConfidenceSynthesis:
                    def __init__(self, confidence_result):
                        if hasattr(confidence_result, 'confidence_score'):
                            self.confidence_breakdown = {
                                'overall': confidence_result.confidence_score,
                                'threat_detection': analysis.confidence,
                                'attribution': 0.7,  # Default
                                'ioc_validation': 0.8   # Default
                            }
                        else:
                            self.confidence_breakdown = {
                                'overall': analysis.confidence,
                                'threat_detection': analysis.confidence,
                                'attribution': 0.7,
                                'ioc_validation': 0.8
                            }
                
                result.confidence_synthesis = ConfidenceSynthesis(confidence_result)
                result.reasoning_chain.append(("confidence_scoring", "completed"))
            
            # Add virtual attention analysis if content analyzer provided context clues
            if analysis.context_clues:
                class AttentionAnalysis:
                    def __init__(self, context_clues):
                        self.key_phrases = context_clues[:10]  # Top 10 important phrases
                
                result.attention_analysis = AttentionAnalysis(analysis.context_clues)
                result.reasoning_chain.append(("attention_analysis", "completed"))
            
            logger.info(f"Optimized analysis completed with {len(result.reasoning_chain)} steps")
            return result
            
        except Exception as e:
            logger.error(f"Optimized analysis failed: {e}")
            # Return minimal result to prevent crash
            if not result.threat_analysis:
                class DefaultThreatAnalysis:
                    threat_classification = "Security Content"
                    severity_assessment = "Medium"
                    attribution_analysis = "Analysis incomplete"
                
                result.threat_analysis = DefaultThreatAnalysis()
            
            return result
    
    def analyze_content(self, record: FeedRecord) -> ContentAnalysisResult:
        """
        Perform intelligent content analysis to determine appropriate processing
        
        Returns:
            ContentAnalysisResult with detailed analysis and recommendations
        """
        return self.content_analyzer.analyze_content(
            title=record.title or '',
            content=record.raw_content,
            url=str(record.url) if record.url else None,
            metadata=record.metadata
        )
    
    def enrich(self, record: FeedRecord) -> FeedRecord:
        """
        Enrich a feed record with optimized analysis
        
        Handles different content types appropriately:
        - Security content: Full threat analysis
        - API content: Technical analysis focus
        - General tech: Basic enrichment
        - Non-tech: Minimal processing
        """
        
        if not record.raw_content:
            logger.debug(f"Skipping enrichment for {record.url} - no content")
            setattr(record.metadata, 'enrichment_status', 'skipped_no_content')
            return record
        
        try:
            # Perform intelligent content analysis
            analysis = self.analyze_content(record)
            
            # Log analysis results
            logger.debug(f"Content analysis for {record.url}:")
            logger.debug(f"  Type: {analysis.content_type} (confidence: {analysis.confidence:.2%})")
            logger.debug(f"  Recommendation: {analysis.recommendation}")
            logger.debug(f"  Analysis depth: {analysis.analysis_depth}")
            
            # Apply appropriate enrichment based on intelligent analysis
            if self.use_optimized and self.advanced_analyzer and \
               analysis.content_type in ['security_threat', 'security_advisory', 'potential_security']:
                # Use advanced analysis for security content
                enrichment_result = self._advanced_enrichment(record, analysis)
                
            elif analysis.content_type in ['api_documentation', 'security_tools']:
                # Use technical enrichment for API/tool content
                enrichment_result = self._technical_enrichment(record, analysis)
                
            elif analysis.content_type in ['mixed_content', 'unclear']:
                # Use hybrid approach for unclear content
                enrichment_result = self._hybrid_enrichment(record, analysis)
                
            else:
                # Use basic enrichment for non-security content
                enrichment_result = self._basic_enrichment(record, analysis)
            
            # Apply enrichments to record
            self._apply_enrichments(record, enrichment_result)
            
            # Add analysis metadata using Pydantic field assignment
            setattr(record.metadata, 'content_analysis', {
                'type': analysis.content_type,
                'confidence': analysis.confidence,
                'security_indicators': analysis.security_indicators,
                'detected_entities': analysis.detected_entities,
                'analysis_depth': analysis.analysis_depth
            })
            setattr(record.metadata, 'enrichment_status', 'success')
            
        except Exception as e:
            logger.error(f"Enrichment error for {record.url}: {e}")
            
            if self.fallback_enabled:
                # Fallback to basic enrichment
                try:
                    enrichment_result = self._basic_enrichment(record, {})
                    self._apply_enrichments(record, enrichment_result)
                    setattr(record.metadata, 'enrichment_status', 'fallback_success')
                except Exception as fallback_error:
                    logger.error(f"Fallback enrichment also failed: {fallback_error}")
                    setattr(record.metadata, 'enrichment_status', 'error')
                    setattr(record.metadata, 'enrichment_error', str(fallback_error))
            else:
                setattr(record.metadata, 'enrichment_status', 'error')
                setattr(record.metadata, 'enrichment_error', str(e))
        
        return record
    
    def _advanced_enrichment(self, record: FeedRecord, analysis: ContentAnalysisResult) -> Dict[str, Any]:
        """
        Apply advanced threat analysis for security content
        
        This uses the optimized DSPy modules with reasoning chains
        """
        
        # Prepare content for analysis
        full_content = f"Title: {record.title or 'Unknown'}\n\nContent: {record.raw_content}"
        
        # Run advanced analysis with context from intelligent analyzer
        context_info = {
            'url': str(record.url),
            'content_type': analysis.content_type,
            'confidence': analysis.confidence,
            'entities': analysis.detected_entities,
            'context_clues': analysis.context_clues[:5]
        }
        
        # Use optimized modules if available, fallback to advanced analyzer
        if self.optimized_modules:
            analysis_result = self._run_optimized_analysis(full_content, context_info, analysis)
        elif self.advanced_analyzer:
            analysis_result = self.advanced_analyzer(
                content=full_content,
                context=json.dumps(context_info),
                analysis_depth=analysis.analysis_depth
            )
        else:
            # Fallback to basic analysis
            return self._basic_enrichment(record, analysis)
        
        # Extract enrichments from advanced analysis
        enrichment = {
            'enriched_title': record.title or self._generate_title_from_analysis(analysis_result),
            'enriched_description': self._generate_description_from_analysis(analysis_result),
            'is_security_focused': True,  # Already detected as security
            'threat_level': analysis_result.threat_analysis.severity_assessment,
            'threat_type': analysis_result.threat_analysis.threat_classification,
            'confidence_scores': analysis_result.confidence_synthesis.confidence_breakdown,
            'iocs': analysis_result.ioc_validation.validated_iocs if hasattr(analysis_result, 'ioc_validation') else [],
            'attribution': analysis_result.threat_analysis.attribution_analysis,
            'reasoning_summary': self._summarize_reasoning(analysis_result.reasoning_chain),
            'requires_payment': self._detect_paywall(record.raw_content),
            'vendor_mentioned': self._extract_vendors(analysis_result)
        }
        
        # Add virtual attention data (what parts of content were most important)
        if hasattr(analysis_result, 'attention_analysis'):
            enrichment['attention_highlights'] = analysis_result.attention_analysis.key_phrases
        
        return enrichment
    
    def _technical_enrichment(self, record: FeedRecord, analysis: ContentAnalysisResult) -> Dict[str, Any]:
        """
        Apply technical analysis for API/tech content
        
        Focuses on technical details rather than threat analysis
        """
        
        # Use basic enricher but with technical focus
        try:
            prediction = self.basic_enricher(
                raw_content=record.raw_content,
                current_title=record.title,
                current_description=record.description
            )
            
            enrichment = {
                'enriched_title': prediction.guessed_title or record.title,
                'enriched_description': prediction.guessed_description or record.description,
                'is_security_focused': prediction.is_security_focused,
                'requires_payment': prediction.requires_payment,
                'vendor_mentioned': prediction.standardized_vendor_name,
                'content_focus': 'technical',
                'analysis_type': analysis.content_type
            }
            
            # Add technical metadata based on analysis
            enrichment['technical_category'] = analysis.content_type
            
            # Include any detected security references even in technical docs
            if analysis.detected_entities.get('vulnerabilities'):
                enrichment['referenced_cves'] = analysis.detected_entities['vulnerabilities']
            
        except Exception as e:
            logger.warning(f"Technical enrichment failed: {e}")
            enrichment = self._minimal_enrichment(record)
        
        return enrichment
    
    def _hybrid_enrichment(self, record: FeedRecord, analysis: ContentAnalysisResult) -> Dict[str, Any]:
        """
        Apply hybrid enrichment for unclear or mixed content
        
        Uses both basic enrichment and extracts any security indicators found
        """
        
        try:
            # Start with basic enrichment
            prediction = self.basic_enricher(
                raw_content=record.raw_content,
                current_title=record.title,
                current_description=record.description
            )
            
            enrichment = {
                'enriched_title': prediction.guessed_title or record.title,
                'enriched_description': prediction.guessed_description or record.description,
                'is_security_focused': analysis.confidence > 0.3,  # Based on analysis confidence
                'requires_payment': prediction.requires_payment,
                'vendor_mentioned': prediction.standardized_vendor_name,
                'content_focus': 'mixed',
                'analysis_confidence': analysis.confidence,
                'security_indicators_found': len(analysis.context_clues) > 0
            }
            
            # Add any detected security entities
            if analysis.detected_entities:
                security_entities = {}
                for entity_type, values in analysis.detected_entities.items():
                    if values:
                        security_entities[entity_type] = values
                if security_entities:
                    enrichment['detected_security_entities'] = security_entities
            
            # Add context clues for potential manual review
            if analysis.context_clues:
                enrichment['security_context_clues'] = analysis.context_clues[:5]
            
        except Exception as e:
            logger.warning(f"Hybrid enrichment failed: {e}")
            enrichment = self._minimal_enrichment(record)
        
        return enrichment
    
    def _basic_enrichment(self, record: FeedRecord, analysis: ContentAnalysisResult) -> Dict[str, Any]:
        """
        Apply basic enrichment for non-security content
        
        Minimal processing for content not related to security or threats
        """
        
        try:
            prediction = self.basic_enricher(
                raw_content=record.raw_content,
                current_title=record.title,
                current_description=record.description
            )
            
            enrichment = {
                'enriched_title': prediction.guessed_title or record.title,
                'enriched_description': prediction.guessed_description or record.description,
                'is_security_focused': False,  # Already detected as non-security
                'requires_payment': prediction.requires_payment,
                'vendor_mentioned': prediction.standardized_vendor_name,
                'content_focus': 'general'
            }
            
        except Exception as e:
            logger.warning(f"Basic enrichment failed: {e}")
            enrichment = self._minimal_enrichment(record)
        
        return enrichment
    
    def _minimal_enrichment(self, record: FeedRecord) -> Dict[str, Any]:
        """Minimal enrichment when all else fails"""
        
        return {
            'enriched_title': record.title or 'Unknown',
            'enriched_description': record.description or record.raw_content[:200] + '...',
            'is_security_focused': False,
            'requires_payment': False,
            'vendor_mentioned': None,
            'content_focus': 'unknown'
        }
    
    def _apply_enrichments(self, record: FeedRecord, enrichment: Dict[str, Any]):
        """Apply enrichment results to the record"""
        
        # Update record fields
        if enrichment.get('enriched_title') and (not record.title or len(record.title) < 10):
            record.title = enrichment['enriched_title']
        
        if enrichment.get('enriched_description') and (not record.description or len(record.description) < 20):
            record.description = enrichment['enriched_description']
        
        record.is_security_focused = enrichment.get('is_security_focused', False)
        record.requires_payment = enrichment.get('requires_payment', False)
        
        if enrichment.get('vendor_mentioned'):
            record.vendor_name = enrichment['vendor_mentioned']
        
        # Add all enrichment data to metadata
        setattr(record.metadata, 'enrichment', enrichment)
    
    def _generate_title_from_analysis(self, analysis_result) -> str:
        """Generate title from advanced analysis results"""
        
        if hasattr(analysis_result, 'threat_analysis'):
            threat_type = analysis_result.threat_analysis.threat_classification
            severity = analysis_result.threat_analysis.severity_assessment
            
            if threat_type and severity:
                return f"{severity.title()} {threat_type}: Security Alert"
        
        return "Security Content Detected"
    
    def _generate_description_from_analysis(self, analysis_result) -> str:
        """Generate description from advanced analysis results"""
        
        parts = []
        
        if hasattr(analysis_result, 'threat_analysis'):
            if analysis_result.threat_analysis.threat_classification:
                parts.append(f"Threat Type: {analysis_result.threat_analysis.threat_classification}")
            
            if analysis_result.threat_analysis.severity_assessment:
                parts.append(f"Severity: {analysis_result.threat_analysis.severity_assessment}")
        
        if hasattr(analysis_result, 'ioc_validation') and analysis_result.ioc_validation:
            ioc_count = len(analysis_result.ioc_validation.validated_iocs)
            if ioc_count > 0:
                parts.append(f"{ioc_count} IOCs detected")
        
        return ". ".join(parts) if parts else "Security-related content analyzed"
    
    def _summarize_reasoning(self, reasoning_chain: List) -> str:
        """Summarize the reasoning chain for metadata"""
        
        if not reasoning_chain:
            return "No detailed reasoning available"
        
        # Take first few steps of reasoning
        summary_parts = []
        for step_name, step_data in reasoning_chain[:3]:
            summary_parts.append(f"{step_name}: completed")
        
        return " → ".join(summary_parts)
    
    def _detect_paywall(self, content: str) -> bool:
        """Detect if content is behind a paywall"""
        
        paywall_indicators = [
            'subscribe to read', 'premium content', 'members only',
            'paid subscribers', 'unlock full article', 'paywall'
        ]
        
        content_lower = content.lower()
        return any(indicator in content_lower for indicator in paywall_indicators)
    
    def _extract_vendors(self, analysis_result) -> Optional[str]:
        """Extract vendor names from analysis"""
        
        if hasattr(analysis_result, 'threat_analysis') and analysis_result.threat_analysis.attribution_analysis:
            attribution = analysis_result.threat_analysis.attribution_analysis
            
            # Look for known security vendors
            known_vendors = [
                'Microsoft', 'Google', 'Amazon', 'Apple', 'Cisco',
                'Palo Alto', 'FireEye', 'CrowdStrike', 'Symantec'
            ]
            
            for vendor in known_vendors:
                if vendor.lower() in attribution.lower():
                    return vendor
        
        return None


# Example usage showing how it handles different content types
if __name__ == "__main__":
    
    # Configure DSPy (would use real LLM in production)
    import dspy
    # dspy.settings.configure(lm=your_llm_instance)
    
    # Initialize enhanced enricher
    enricher = EnhancedFeedEnricher(use_optimized=True, fallback_enabled=True)
    
    # Example 1: Security-focused content
    security_record = FeedRecord(
        url="https://example.com/apt29-campaign",
        raw_content="""
        New APT29 campaign discovered targeting government agencies.
        The threat actors are using sophisticated malware with multiple
        zero-day exploits. IOCs include evil.com and 192.168.1.100.
        """,
        title="APT29 Campaign Alert"
    )
    
    enriched_security = enricher.enrich(security_record)
    print(f"Security content enriched: {enriched_security.metadata}")
    
    # Example 2: API documentation (non-threat)
    api_record = FeedRecord(
        url="https://api.example.com/docs",
        raw_content="""
        New REST API endpoints released for user authentication.
        The API now supports OAuth 2.0 and includes new endpoints
        for token refresh and user profile management.
        """,
        title="API Update v2.0"
    )
    
    enriched_api = enricher.enrich(api_record)
    print(f"API content enriched: {enriched_api.metadata}")
    
    # Example 3: General tech news (non-security)
    tech_record = FeedRecord(
        url="https://tech.example.com/news",
        raw_content="""
        Company announces new cloud storage features including
        improved performance and reduced pricing. The update
        will roll out to all customers next month.
        """,
        title="Cloud Storage Update"
    )
    
    enriched_tech = enricher.enrich(tech_record)
    print(f"Tech content enriched: {enriched_tech.metadata}")
    
    # Example 4: Business news (completely non-technical)
    business_record = FeedRecord(
        url="https://business.example.com/merger",
        raw_content="""
        Two major retail companies announce merger plans.
        The deal is valued at $10 billion and is expected
        to close by end of year pending regulatory approval.
        """,
        title="Retail Merger Announced"
    )
    
    enriched_business = enricher.enrich(business_record)
    print(f"Business content enriched: {enriched_business.metadata}")