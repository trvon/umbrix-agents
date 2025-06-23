"""
Comprehensive tests for Enhanced Feed Enricher - Advanced AI-powered feed enrichment system.

Tests cover:
- EnhancedFeedEnricher initialization with optimization options
- Intelligent content analysis integration
- Advanced threat analysis with DSPy optimization
- Multiple enrichment strategies (advanced, technical, hybrid, basic)
- Optimized module loading and caching
- Fallback mechanisms and error handling
- Content type detection and appropriate processing
- Metadata generation and enrichment application
"""

import pytest
import json
import logging
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

from common_tools.enhanced_feed_enricher import EnhancedFeedEnricher
from common_tools.models.feed_record import FeedRecord
from common_tools.intelligent_content_analyzer import ContentAnalysisResult
from pydantic import AnyUrl


class TestEnhancedFeedEnricherInitialization:
    """Test suite for EnhancedFeedEnricher initialization and setup."""
    
    @patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought')
    @patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer')
    def test_initialization_with_optimization_enabled(self, mock_analyzer_class, mock_cot):
        """Test initialization with optimization enabled."""
        mock_analyzer_class.return_value = Mock()
        mock_cot.return_value = Mock()
        
        with patch.object(EnhancedFeedEnricher, '_load_optimized_analyzer', return_value=Mock()) as mock_load_analyzer, \
             patch.object(EnhancedFeedEnricher, '_load_optimized_modules', return_value={'test': Mock()}) as mock_load_modules:
            
            enricher = EnhancedFeedEnricher(use_optimized=True, fallback_enabled=True)
            
            assert enricher.use_optimized is True
            assert enricher.fallback_enabled is True
            assert enricher.basic_enricher is not None
            assert enricher.content_analyzer is not None
            
            # Verify optimization methods were called
            mock_load_analyzer.assert_called_once()
            mock_load_modules.assert_called_once()
    
    @patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought')
    @patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer')
    def test_initialization_without_optimization(self, mock_analyzer_class, mock_cot):
        """Test initialization with optimization disabled."""
        mock_analyzer_class.return_value = Mock()
        mock_cot.return_value = Mock()
        
        enricher = EnhancedFeedEnricher(use_optimized=False, fallback_enabled=False)
        
        assert enricher.use_optimized is False
        assert enricher.fallback_enabled is False
        assert enricher.advanced_analyzer is None
        assert enricher.optimized_modules == {}
        assert enricher.basic_enricher is not None
        assert enricher.content_analyzer is not None
    
    @patch('common_tools.enhanced_feed_enricher._get_optimization_manager')
    @patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought')
    @patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer')
    def test_load_optimized_analyzer_success(self, mock_analyzer_class, mock_cot, mock_get_manager):
        """Test successful loading of optimized analyzer."""
        mock_analyzer_class.return_value = Mock()
        mock_cot.return_value = Mock()
        
        # Mock optimization manager
        mock_manager = Mock()
        mock_analyzer = Mock()
        mock_manager.return_value = mock_analyzer
        mock_get_manager.return_value = mock_manager
        
        enricher = EnhancedFeedEnricher(use_optimized=True)
        
        assert enricher.advanced_analyzer == mock_analyzer
        mock_manager.assert_called_with('advanced_threat_analyzer', Mock)
    
    @patch('common_tools.enhanced_feed_enricher._get_optimization_manager')
    @patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought')
    @patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer')
    def test_load_optimized_analyzer_failure(self, mock_analyzer_class, mock_cot, mock_get_manager):
        """Test handling of optimized analyzer loading failure."""
        mock_analyzer_class.return_value = Mock()
        mock_cot.return_value = Mock()
        
        # Mock optimization manager failure
        mock_get_manager.return_value = None
        
        enricher = EnhancedFeedEnricher(use_optimized=True)
        
        assert enricher.advanced_analyzer is None
    
    @patch('common_tools.enhanced_feed_enricher._get_optimization_manager')
    @patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought')
    @patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer')
    def test_load_optimized_modules_success(self, mock_analyzer_class, mock_cot, mock_get_manager):
        """Test successful loading of optimized modules."""
        mock_analyzer_class.return_value = Mock()
        mock_cot.return_value = Mock()
        
        # Mock optimization manager with modules
        mock_manager = Mock()
        mock_modules = {
            'security_threat_analyzer': Mock(),
            'apt_attribution_analyzer': Mock(),
            'confidence_scorer': Mock()
        }
        mock_manager.side_effect = lambda name: mock_modules.get(name)
        mock_get_manager.return_value = mock_manager
        
        enricher = EnhancedFeedEnricher(use_optimized=True)
        
        # Should have loaded the available modules
        assert len(enricher.optimized_modules) == 3
        assert 'security_threat_analyzer' in enricher.optimized_modules
        assert 'apt_attribution_analyzer' in enricher.optimized_modules
        assert 'confidence_scorer' in enricher.optimized_modules
    
    @patch('common_tools.enhanced_feed_enricher._get_optimization_manager')
    @patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought')
    @patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer')
    def test_load_optimized_modules_partial_failure(self, mock_analyzer_class, mock_cot, mock_get_manager):
        """Test handling of partial module loading failure."""
        mock_analyzer_class.return_value = Mock()
        mock_cot.return_value = Mock()
        
        # Mock optimization manager with some modules failing
        mock_manager = Mock()
        def mock_get_module(name):
            if name == 'security_threat_analyzer':
                return Mock()
            elif name == 'apt_attribution_analyzer':
                raise Exception("Module loading failed")
            else:
                return None
        
        mock_manager.side_effect = mock_get_module
        mock_get_manager.return_value = mock_manager
        
        enricher = EnhancedFeedEnricher(use_optimized=True)
        
        # Should have loaded only the successful module
        assert len(enricher.optimized_modules) == 1
        assert 'security_threat_analyzer' in enricher.optimized_modules


class TestEnhancedFeedEnricherContentAnalysis:
    """Test suite for content analysis functionality."""
    
    @pytest.fixture
    def enricher(self):
        """Create EnhancedFeedEnricher instance for testing."""
        with patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought'), \
             patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer'):
            return EnhancedFeedEnricher(use_optimized=False, fallback_enabled=True)
    
    @pytest.fixture
    def sample_feed_record(self):
        """Create sample FeedRecord for testing."""
        return FeedRecord(
            url=AnyUrl("https://security.example.com/apt-campaign"),
            raw_content="APT29 campaign discovered targeting government agencies with sophisticated malware.",
            title="Security Alert: APT29 Campaign",
            description="Advanced threat actor targeting government systems"
        )
    
    @pytest.fixture
    def mock_content_analysis(self):
        """Create mock ContentAnalysisResult."""
        return ContentAnalysisResult(
            content_type='security_threat',
            confidence=0.95,
            security_indicators={'threat_actors': 0.9, 'attack_techniques': 0.8},
            context_clues=['APT29', 'campaign', 'sophisticated malware'],
            detected_entities={'threats': ['APT29'], 'targets': ['government agencies']},
            recommendation='full_analysis',
            analysis_depth='comprehensive'
        )
    
    def test_analyze_content_basic(self, enricher, sample_feed_record, mock_content_analysis):
        """Test basic content analysis functionality."""
        # Mock the content analyzer
        enricher.content_analyzer.analyze_content = Mock(return_value=mock_content_analysis)
        
        result = enricher.analyze_content(sample_feed_record)
        
        assert result == mock_content_analysis
        enricher.content_analyzer.analyze_content.assert_called_once_with(
            title="Security Alert: APT29 Campaign",
            content="APT29 campaign discovered targeting government agencies with sophisticated malware.",
            url="https://security.example.com/apt-campaign",
            metadata=sample_feed_record.metadata
        )
    
    def test_analyze_content_with_none_values(self, enricher, mock_content_analysis):
        """Test content analysis with None values."""
        record = FeedRecord(
            url=None,
            raw_content="Test content",
            title=None,
            description=None
        )
        
        enricher.content_analyzer.analyze_content = Mock(return_value=mock_content_analysis)
        
        result = enricher.analyze_content(record)
        
        enricher.content_analyzer.analyze_content.assert_called_once_with(
            title="",
            content="Test content",
            url=None,
            metadata=record.metadata
        )


class TestEnhancedFeedEnricherEnrichmentStrategies:
    """Test suite for different enrichment strategies."""
    
    @pytest.fixture
    def enricher_with_optimization(self):
        """Create enricher with optimization enabled."""
        with patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought'), \
             patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer'):
            enricher = EnhancedFeedEnricher(use_optimized=True, fallback_enabled=True)
            # Mock the optimized modules
            enricher.optimized_modules = {
                'security_threat_analyzer': Mock(),
                'apt_attribution_analyzer': Mock(),
                'threat_intel_extractor': Mock(),
                'confidence_scorer': Mock()
            }
            enricher.advanced_analyzer = Mock()
            return enricher
    
    @pytest.fixture
    def security_analysis_result(self):
        """Create security-focused analysis result."""
        return ContentAnalysisResult(
            content_type='security_threat',
            confidence=0.95,
            security_indicators={'threat_actors': 0.9},
            context_clues=['APT29', 'malware'],
            detected_entities={'threats': ['APT29']},
            recommendation='full_analysis',
            analysis_depth='comprehensive'
        )
    
    @pytest.fixture
    def api_analysis_result(self):
        """Create API documentation analysis result."""
        return ContentAnalysisResult(
            content_type='api_documentation',
            confidence=0.85,
            security_indicators={},
            context_clues=['REST API', 'authentication'],
            detected_entities={'vulnerabilities': ['CVE-2023-1234']},
            recommendation='technical_focus',
            analysis_depth='moderate'
        )
    
    @pytest.fixture
    def mixed_analysis_result(self):
        """Create mixed content analysis result."""
        return ContentAnalysisResult(
            content_type='mixed_content',
            confidence=0.45,
            security_indicators={'security_context': 0.3},
            context_clues=['security', 'update'],
            detected_entities={},
            recommendation='hybrid_approach',
            analysis_depth='basic'
        )
    
    def test_enrich_security_content_with_optimization(self, enricher_with_optimization, security_analysis_result):
        """Test enrichment of security content with optimization."""
        record = FeedRecord(
            url=AnyUrl("https://security.example.com/threat"),
            raw_content="APT29 campaign with sophisticated malware targeting government agencies.",
            title="Threat Alert"
        )
        
        # Mock content analysis
        enricher_with_optimization.content_analyzer.analyze_content = Mock(return_value=security_analysis_result)
        
        # Mock optimized analysis
        with patch.object(enricher_with_optimization, '_run_optimized_analysis') as mock_optimized:
            mock_analysis_result = Mock()
            mock_analysis_result.threat_analysis.severity_assessment = "High"
            mock_analysis_result.threat_analysis.threat_classification = "APT Campaign"
            mock_analysis_result.threat_analysis.attribution_analysis = "APT29"
            mock_analysis_result.confidence_synthesis.confidence_breakdown = {'overall': 0.95}
            mock_analysis_result.ioc_validation.validated_iocs = ['evil.com']
            mock_analysis_result.reasoning_chain = [('security_analysis', 'completed')]
            mock_analysis_result.attention_analysis.key_phrases = ['APT29', 'malware']
            mock_optimized.return_value = mock_analysis_result
            
            result = enricher_with_optimization.enrich(record)
            
            # Verify advanced enrichment was used
            mock_optimized.assert_called_once()
            
            # Check enrichment metadata
            assert hasattr(result.metadata, 'enrichment_status')
            assert result.metadata.enrichment_status == 'success'
            assert hasattr(result.metadata, 'content_analysis')
            assert result.metadata.content_analysis['type'] == 'security_threat'
    
    def test_enrich_api_content_technical_strategy(self, enricher_with_optimization, api_analysis_result):
        """Test enrichment of API content with technical strategy."""
        record = FeedRecord(
            url=AnyUrl("https://api.example.com/docs"),
            raw_content="New REST API endpoints for authentication with OAuth 2.0 support.",
            title="API Update"
        )
        
        # Mock content analysis
        enricher_with_optimization.content_analyzer.analyze_content = Mock(return_value=api_analysis_result)
        
        # Mock basic enricher
        mock_prediction = Mock()
        mock_prediction.guessed_title = "API Documentation Update"
        mock_prediction.guessed_description = "REST API authentication endpoints"
        mock_prediction.is_security_focused = False
        mock_prediction.requires_payment = False
        mock_prediction.standardized_vendor_name = "ExampleAPI"
        enricher_with_optimization.basic_enricher.return_value = mock_prediction
        
        result = enricher_with_optimization.enrich(record)
        
        # Verify technical enrichment was applied
        assert hasattr(result.metadata, 'enrichment')
        enrichment = result.metadata.enrichment
        assert enrichment['content_focus'] == 'technical'
        assert enrichment['analysis_type'] == 'api_documentation'
        assert enrichment['referenced_cves'] == ['CVE-2023-1234']
    
    def test_enrich_mixed_content_hybrid_strategy(self, enricher_with_optimization, mixed_analysis_result):
        """Test enrichment of mixed content with hybrid strategy."""
        record = FeedRecord(
            url=AnyUrl("https://news.example.com/security-update"),
            raw_content="Company releases security update for their platform with new features.",
            title="Platform Update"
        )
        
        # Mock content analysis
        enricher_with_optimization.content_analyzer.analyze_content = Mock(return_value=mixed_analysis_result)
        
        # Mock basic enricher
        mock_prediction = Mock()
        mock_prediction.guessed_title = "Security Platform Update"
        mock_prediction.guessed_description = "Platform security improvements"
        mock_prediction.is_security_focused = True
        mock_prediction.requires_payment = False
        mock_prediction.standardized_vendor_name = "PlatformCorp"
        enricher_with_optimization.basic_enricher.return_value = mock_prediction
        
        result = enricher_with_optimization.enrich(record)
        
        # Verify hybrid enrichment was applied
        assert hasattr(result.metadata, 'enrichment')
        enrichment = result.metadata.enrichment
        assert enrichment['content_focus'] == 'mixed'
        assert enrichment['analysis_confidence'] == 0.45
        assert enrichment['security_indicators_found'] is True
        assert enrichment['security_context_clues'] == ['security', 'update']
    
    def test_enrich_general_content_basic_strategy(self, enricher_with_optimization):
        """Test enrichment of general content with basic strategy."""
        record = FeedRecord(
            url=AnyUrl("https://news.example.com/business"),
            raw_content="Company announces quarterly earnings and expansion plans.",
            title="Business News"
        )
        
        # Mock content analysis for general content
        general_analysis = ContentAnalysisResult(
            content_type='business_news',
            confidence=0.15,
            security_indicators={},
            context_clues=[],
            detected_entities={},
            recommendation='minimal_processing',
            analysis_depth='basic'
        )
        
        enricher_with_optimization.content_analyzer.analyze_content = Mock(return_value=general_analysis)
        
        # Mock basic enricher
        mock_prediction = Mock()
        mock_prediction.guessed_title = "Business Quarterly Update"
        mock_prediction.guessed_description = "Financial results and expansion"
        mock_prediction.is_security_focused = False
        mock_prediction.requires_payment = False
        mock_prediction.standardized_vendor_name = None
        enricher_with_optimization.basic_enricher.return_value = mock_prediction
        
        result = enricher_with_optimization.enrich(record)
        
        # Verify basic enrichment was applied
        assert hasattr(result.metadata, 'enrichment')
        enrichment = result.metadata.enrichment
        assert enrichment['content_focus'] == 'general'
        assert enrichment['is_security_focused'] is False


class TestEnhancedFeedEnricherOptimizedAnalysis:
    """Test suite for optimized analysis functionality."""
    
    @pytest.fixture
    def enricher_with_modules(self):
        """Create enricher with mocked optimized modules."""
        with patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought'), \
             patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer'):
            enricher = EnhancedFeedEnricher(use_optimized=True, fallback_enabled=True)
            
            # Mock optimized modules
            enricher.optimized_modules = {
                'security_threat_analyzer': Mock(),
                'apt_attribution_analyzer': Mock(),
                'threat_intel_extractor': Mock(),
                'confidence_scorer': Mock()
            }
            
            return enricher
    
    def test_run_optimized_analysis_complete(self, enricher_with_modules):
        """Test complete optimized analysis with all modules."""
        content = "APT29 campaign using sophisticated malware"
        context_info = {'content_type': 'security_threat', 'confidence': 0.95}
        analysis = Mock()
        analysis.detected_entities = {'threats': ['APT29']}
        analysis.context_clues = ['APT29', 'malware', 'campaign']
        analysis.confidence = 0.95
        
        # Mock module responses
        threat_result = Mock()
        threat_result.threat_type = "APT Campaign"
        threat_result.severity = "High"
        threat_result.attribution = "APT29"
        enricher_with_modules.optimized_modules['security_threat_analyzer'].return_value = threat_result
        
        apt_result = Mock()
        apt_result.attribution = "APT29 - Cozy Bear"
        enricher_with_modules.optimized_modules['apt_attribution_analyzer'].return_value = apt_result
        
        intel_result = Mock()
        intel_result.extracted_iocs = ['evil.com', '192.168.1.100']
        enricher_with_modules.optimized_modules['threat_intel_extractor'].return_value = intel_result
        
        confidence_result = Mock()
        confidence_result.confidence_score = 0.92
        enricher_with_modules.optimized_modules['confidence_scorer'].return_value = confidence_result
        
        result = enricher_with_modules._run_optimized_analysis(content, context_info, analysis)
        
        # Verify all modules were called
        enricher_with_modules.optimized_modules['security_threat_analyzer'].assert_called_once()
        enricher_with_modules.optimized_modules['apt_attribution_analyzer'].assert_called_once()
        enricher_with_modules.optimized_modules['threat_intel_extractor'].assert_called_once()
        enricher_with_modules.optimized_modules['confidence_scorer'].assert_called_once()
        
        # Verify result structure
        assert result.threat_analysis.threat_classification == "APT Campaign"
        assert result.threat_analysis.severity_assessment == "High"
        assert result.threat_analysis.attribution_analysis == "APT29 - Cozy Bear"
        assert result.ioc_validation.validated_iocs == ['evil.com', '192.168.1.100']
        assert result.confidence_synthesis.confidence_breakdown['overall'] == 0.92
        assert len(result.reasoning_chain) == 5  # 4 modules + attention analysis
    
    def test_run_optimized_analysis_partial_modules(self, enricher_with_modules):
        """Test optimized analysis with only some modules available."""
        # Remove some modules
        del enricher_with_modules.optimized_modules['apt_attribution_analyzer']
        del enricher_with_modules.optimized_modules['confidence_scorer']
        
        content = "Security vulnerability discovered"
        context_info = {'content_type': 'security_threat'}
        analysis = Mock()
        analysis.detected_entities = {}
        analysis.context_clues = []
        analysis.confidence = 0.8
        
        # Mock remaining module responses
        threat_result = Mock()
        threat_result.threat_type = "Vulnerability"
        threat_result.severity = "Medium"
        enricher_with_modules.optimized_modules['security_threat_analyzer'].return_value = threat_result
        
        intel_result = Mock()
        intel_result.extracted_iocs = []
        enricher_with_modules.optimized_modules['threat_intel_extractor'].return_value = intel_result
        
        result = enricher_with_modules._run_optimized_analysis(content, context_info, analysis)
        
        # Verify only available modules were called
        enricher_with_modules.optimized_modules['security_threat_analyzer'].assert_called_once()
        enricher_with_modules.optimized_modules['threat_intel_extractor'].assert_called_once()
        
        # Verify result has threat analysis but not apt attribution
        assert result.threat_analysis.threat_classification == "Vulnerability"
        assert result.ioc_validation.validated_iocs == []
        assert len(result.reasoning_chain) == 2  # 2 modules only
    
    def test_run_optimized_analysis_error_handling(self, enricher_with_modules):
        """Test error handling in optimized analysis."""
        content = "Test content"
        context_info = {}
        analysis = Mock()
        analysis.detected_entities = {}
        analysis.context_clues = []
        analysis.confidence = 0.5
        
        # Mock module to raise exception
        enricher_with_modules.optimized_modules['security_threat_analyzer'].side_effect = Exception("Module failed")
        
        result = enricher_with_modules._run_optimized_analysis(content, context_info, analysis)
        
        # Should return default result without crashing
        assert result.threat_analysis.threat_classification == "Security Content"
        assert result.threat_analysis.severity_assessment == "Medium"
        assert len(result.reasoning_chain) == 0


class TestEnhancedFeedEnricherErrorHandling:
    """Test suite for error handling and fallback mechanisms."""
    
    @pytest.fixture
    def enricher_with_fallback(self):
        """Create enricher with fallback enabled."""
        with patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought'), \
             patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer'):
            return EnhancedFeedEnricher(use_optimized=True, fallback_enabled=True)
    
    @pytest.fixture
    def enricher_no_fallback(self):
        """Create enricher with fallback disabled."""
        with patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought'), \
             patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer'):
            return EnhancedFeedEnricher(use_optimized=True, fallback_enabled=False)
    
    def test_enrich_no_content(self, enricher_with_fallback):
        """Test enrichment when record has no content."""
        record = FeedRecord(
            url=AnyUrl("https://example.com/empty"),
            raw_content="",
            title="Empty Record"
        )
        
        result = enricher_with_fallback.enrich(record)
        
        assert hasattr(result.metadata, 'enrichment_status')
        assert result.metadata.enrichment_status == 'skipped_no_content'
    
    def test_enrich_content_analysis_error_with_fallback(self, enricher_with_fallback):
        """Test error in content analysis with fallback enabled."""
        record = FeedRecord(
            url=AnyUrl("https://example.com/test"),
            raw_content="Test content",
            title="Test Record"
        )
        
        # Mock content analyzer to raise exception
        enricher_with_fallback.content_analyzer.analyze_content.side_effect = Exception("Analysis failed")
        
        # Mock basic enricher for fallback
        mock_prediction = Mock()
        mock_prediction.guessed_title = "Fallback Title"
        mock_prediction.guessed_description = "Fallback Description"
        mock_prediction.is_security_focused = False
        mock_prediction.requires_payment = False
        mock_prediction.standardized_vendor_name = None
        enricher_with_fallback.basic_enricher.return_value = mock_prediction
        
        result = enricher_with_fallback.enrich(record)
        
        assert hasattr(result.metadata, 'enrichment_status')
        assert result.metadata.enrichment_status == 'fallback_success'
        assert hasattr(result.metadata, 'enrichment')
    
    def test_enrich_content_analysis_error_no_fallback(self, enricher_no_fallback):
        """Test error in content analysis with fallback disabled."""
        record = FeedRecord(
            url=AnyUrl("https://example.com/test"),
            raw_content="Test content",
            title="Test Record"
        )
        
        # Mock content analyzer to raise exception
        enricher_no_fallback.content_analyzer.analyze_content.side_effect = Exception("Analysis failed")
        
        result = enricher_no_fallback.enrich(record)
        
        assert hasattr(result.metadata, 'enrichment_status')
        assert result.metadata.enrichment_status == 'error'
        assert hasattr(result.metadata, 'enrichment_error')
        assert "Analysis failed" in result.metadata.enrichment_error
    
    def test_enrich_fallback_also_fails(self, enricher_with_fallback):
        """Test when both main and fallback enrichment fail."""
        record = FeedRecord(
            url=AnyUrl("https://example.com/test"),
            raw_content="Test content",
            title="Test Record"
        )
        
        # Mock both content analyzer and basic enricher to fail
        enricher_with_fallback.content_analyzer.analyze_content.side_effect = Exception("Analysis failed")
        enricher_with_fallback.basic_enricher.side_effect = Exception("Fallback failed")
        
        result = enricher_with_fallback.enrich(record)
        
        assert hasattr(result.metadata, 'enrichment_status')
        assert result.metadata.enrichment_status == 'error'
        assert hasattr(result.metadata, 'enrichment_error')
        assert "Fallback failed" in result.metadata.enrichment_error


class TestEnhancedFeedEnricherHelperMethods:
    """Test suite for helper methods and utilities."""
    
    @pytest.fixture
    def enricher(self):
        """Create enricher for testing helper methods."""
        with patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought'), \
             patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer'):
            return EnhancedFeedEnricher(use_optimized=False)
    
    def test_apply_enrichments_title_replacement(self, enricher):
        """Test title replacement in apply enrichments."""
        record = FeedRecord(
            url=AnyUrl("https://example.com/test"),
            raw_content="Test content",
            title="Short"  # Less than 10 characters
        )
        
        enrichment = {
            'enriched_title': 'Enhanced Long Title',
            'enriched_description': 'Enhanced Description',
            'is_security_focused': True,
            'requires_payment': False,
            'vendor_mentioned': 'TestVendor'
        }
        
        enricher._apply_enrichments(record, enrichment)
        
        assert record.title == 'Enhanced Long Title'
        assert record.description == 'Enhanced Description'
        assert record.is_security_focused is True
        assert record.requires_payment is False
        assert record.vendor_name == 'TestVendor'
        assert hasattr(record.metadata, 'enrichment')
    
    def test_apply_enrichments_title_preservation(self, enricher):
        """Test title preservation when existing title is good."""
        record = FeedRecord(
            url=AnyUrl("https://example.com/test"),
            raw_content="Test content",
            title="This is a sufficiently long existing title"
        )
        
        enrichment = {
            'enriched_title': 'Enhanced Title',
            'is_security_focused': False
        }
        
        enricher._apply_enrichments(record, enrichment)
        
        # Original title should be preserved
        assert record.title == "This is a sufficiently long existing title"
    
    def test_generate_title_from_analysis(self, enricher):
        """Test title generation from analysis results."""
        # Mock analysis result
        analysis_result = Mock()
        analysis_result.threat_analysis.threat_classification = "APT Campaign"
        analysis_result.threat_analysis.severity_assessment = "high"
        
        title = enricher._generate_title_from_analysis(analysis_result)
        
        assert title == "High APT Campaign: Security Alert"
    
    def test_generate_title_from_analysis_fallback(self, enricher):
        """Test title generation fallback."""
        # Mock analysis result without complete data
        analysis_result = Mock()
        analysis_result.threat_analysis.threat_classification = None
        
        title = enricher._generate_title_from_analysis(analysis_result)
        
        assert title == "Security Content Detected"
    
    def test_generate_description_from_analysis(self, enricher):
        """Test description generation from analysis results."""
        # Mock analysis result
        analysis_result = Mock()
        analysis_result.threat_analysis.threat_classification = "Malware"
        analysis_result.threat_analysis.severity_assessment = "Critical"
        analysis_result.ioc_validation.validated_iocs = ['evil.com', '192.168.1.1']
        
        description = enricher._generate_description_from_analysis(analysis_result)
        
        assert "Threat Type: Malware" in description
        assert "Severity: Critical" in description
        assert "2 IOCs detected" in description
    
    def test_summarize_reasoning(self, enricher):
        """Test reasoning chain summarization."""
        reasoning_chain = [
            ('security_threat_analysis', 'completed'),
            ('apt_attribution_analysis', 'completed'),
            ('ioc_validation', 'completed'),
            ('confidence_scoring', 'completed')
        ]
        
        summary = enricher._summarize_reasoning(reasoning_chain)
        
        assert "security_threat_analysis: completed" in summary
        assert "apt_attribution_analysis: completed" in summary
        assert "ioc_validation: completed" in summary
        # Should only include first 3 steps
        assert "confidence_scoring: completed" not in summary
    
    def test_detect_paywall_positive(self, enricher):
        """Test paywall detection with positive indicators."""
        content = "This is premium content. Subscribe to read the full article."
        
        result = enricher._detect_paywall(content)
        
        assert result is True
    
    def test_detect_paywall_negative(self, enricher):
        """Test paywall detection with no indicators."""
        content = "This is free content available to all readers."
        
        result = enricher._detect_paywall(content)
        
        assert result is False
    
    def test_extract_vendors(self, enricher):
        """Test vendor extraction from analysis."""
        # Mock analysis result with vendor mention
        analysis_result = Mock()
        analysis_result.threat_analysis.attribution_analysis = "Microsoft discovered this threat"
        
        vendor = enricher._extract_vendors(analysis_result)
        
        assert vendor == "Microsoft"
    
    def test_extract_vendors_no_match(self, enricher):
        """Test vendor extraction with no known vendors."""
        # Mock analysis result without known vendors
        analysis_result = Mock()
        analysis_result.threat_analysis.attribution_analysis = "Unknown vendor discovered this threat"
        
        vendor = enricher._extract_vendors(analysis_result)
        
        assert vendor is None
    
    def test_minimal_enrichment(self, enricher):
        """Test minimal enrichment fallback."""
        record = FeedRecord(
            url=AnyUrl("https://example.com/test"),
            raw_content="Test content for minimal enrichment",
            title="Test Title"
        )
        
        enrichment = enricher._minimal_enrichment(record)
        
        assert enrichment['enriched_title'] == "Test Title"
        assert enrichment['is_security_focused'] is False
        assert enrichment['requires_payment'] is False
        assert enrichment['vendor_mentioned'] is None
        assert enrichment['content_focus'] == 'unknown'


class TestEnhancedFeedEnricherIntegration:
    """Integration tests for complete enrichment workflows."""
    
    @pytest.fixture
    def full_enricher(self):
        """Create fully configured enricher for integration tests."""
        with patch('common_tools.enhanced_feed_enricher.dspy.ChainOfThought') as mock_cot, \
             patch('common_tools.enhanced_feed_enricher.IntelligentContentAnalyzer') as mock_analyzer_class:
            
            # Mock basic enricher
            mock_basic_enricher = Mock()
            mock_cot.return_value = mock_basic_enricher
            
            # Mock content analyzer
            mock_analyzer = Mock()
            mock_analyzer_class.return_value = mock_analyzer
            
            enricher = EnhancedFeedEnricher(use_optimized=True, fallback_enabled=True)
            enricher.basic_enricher = mock_basic_enricher
            enricher.content_analyzer = mock_analyzer
            
            return enricher
    
    def test_complete_security_workflow(self, full_enricher):
        """Test complete workflow for security content."""
        record = FeedRecord(
            url=AnyUrl("https://security.example.com/apt29"),
            raw_content="APT29 campaign discovered using sophisticated malware to target government agencies.",
            title="APT29 Alert"
        )
        
        # Mock content analysis
        security_analysis = ContentAnalysisResult(
            content_type='security_threat',
            confidence=0.95,
            security_indicators={'threat_actors': 0.9},
            context_clues=['APT29', 'malware', 'government'],
            detected_entities={'threats': ['APT29'], 'targets': ['government agencies']},
            recommendation='full_analysis',
            analysis_depth='comprehensive'
        )
        full_enricher.content_analyzer.analyze_content.return_value = security_analysis
        
        # Mock optimized modules
        full_enricher.optimized_modules = {'security_threat_analyzer': Mock()}
        
        # Mock optimized analysis
        with patch.object(full_enricher, '_run_optimized_analysis') as mock_optimized:
            mock_result = Mock()
            mock_result.threat_analysis.severity_assessment = "Critical"
            mock_result.threat_analysis.threat_classification = "APT Campaign"
            mock_result.threat_analysis.attribution_analysis = "APT29"
            mock_result.confidence_synthesis.confidence_breakdown = {'overall': 0.95}
            mock_result.ioc_validation.validated_iocs = ['evil.com']
            mock_result.reasoning_chain = [('security_analysis', 'completed')]
            mock_result.attention_analysis.key_phrases = ['APT29', 'malware']
            mock_optimized.return_value = mock_result
            
            result = full_enricher.enrich(record)
            
            # Verify complete workflow
            assert result.metadata.enrichment_status == 'success'
            assert result.metadata.content_analysis['type'] == 'security_threat'
            assert result.metadata.content_analysis['confidence'] == 0.95
            assert result.is_security_focused is True
    
    def test_complete_non_security_workflow(self, full_enricher):
        """Test complete workflow for non-security content."""
        record = FeedRecord(
            url=AnyUrl("https://business.example.com/merger"),
            raw_content="Two major companies announce merger plans valued at $10 billion.",
            title="Business Merger"
        )
        
        # Mock content analysis for business content
        business_analysis = ContentAnalysisResult(
            content_type='business_news',
            confidence=0.1,
            security_indicators={},
            context_clues=[],
            detected_entities={},
            recommendation='minimal_processing',
            analysis_depth='basic'
        )
        full_enricher.content_analyzer.analyze_content.return_value = business_analysis
        
        # Mock basic enricher
        mock_prediction = Mock()
        mock_prediction.guessed_title = "Corporate Merger Announcement"
        mock_prediction.guessed_description = "Major business acquisition details"
        mock_prediction.is_security_focused = False
        mock_prediction.requires_payment = False
        mock_prediction.standardized_vendor_name = None
        full_enricher.basic_enricher.return_value = mock_prediction
        
        result = full_enricher.enrich(record)
        
        # Verify basic workflow was used
        assert result.metadata.enrichment_status == 'success'
        assert result.metadata.content_analysis['type'] == 'business_news'
        assert result.is_security_focused is False
        assert result.metadata.enrichment['content_focus'] == 'general'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])