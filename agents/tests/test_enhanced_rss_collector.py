"""
Comprehensive tests for Enhanced RSS Collector Agent

Tests cover:
- Intelligent content analysis
- Enhanced enrichment routing
- Retry logic for all operations
- Security content detection
- Performance metrics
- Error handling and fallbacks
"""

import pytest
import json
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
from pydantic import AnyUrl

from collector_agent.enhanced_rss_collector import EnhancedRssCollectorAgent
from common_tools.models.feed_record import FeedRecord
from common_tools.intelligent_content_analyzer import ContentAnalysisResult
from common_tools.enhanced_feed_enricher import EnhancedFeedEnricher


class TestEnhancedRssCollectorAgent:
    """Test suite for enhanced RSS collector."""

    @pytest.fixture
    def mock_kafka_config(self):
        """Mock Kafka configuration."""
        return {
            'bootstrap_servers': 'localhost:9092',
            'use_kafka': False,  # Disable for testing
            'topic': 'feeds.discovered',
            'prometheus_port': None
        }

    @pytest.fixture
    def sample_feed_entries(self):
        """Sample feed entries for testing."""
        return [
            {
                'id': 'entry1',
                'link': 'https://example.com/security-alert',
                'title': 'Critical Security Alert',
                'summary': 'New APT29 campaign discovered targeting financial institutions...',
                'published': '2024-01-15T10:00:00Z',
                'tags': [{'term': 'security'}, {'term': 'apt'}]
            },
            {
                'id': 'entry2', 
                'link': 'https://api.example.com/docs/v2',
                'title': 'API Documentation v2.0',
                'summary': 'Updated REST API documentation with new OAuth endpoints...',
                'published': '2024-01-15T11:00:00Z',
                'tags': [{'term': 'api'}, {'term': 'documentation'}]
            },
            {
                'id': 'entry3',
                'link': 'https://blog.example.com/company-news',
                'title': 'Quarterly Earnings Report',
                'summary': 'Company reports strong Q4 earnings with 25% growth...',
                'published': '2024-01-15T12:00:00Z',
                'tags': [{'term': 'business'}, {'term': 'earnings'}]
            }
        ]

    @pytest.fixture
    def mock_content_analysis_results(self):
        """Mock content analysis results for different content types."""
        return {
            'security_content': {
                'content_type': 'security_threat',
                'confidence': 0.85,
                'security_indicators': {'threat_actors': 0.9, 'attack_techniques': 0.8},
                'detected_entities': {'threat_actors': ['APT29'], 'ips': ['192.168.1.1']},
                'recommendation': 'Use advanced threat analysis',
                'analysis_depth': 'comprehensive'
            },
            'api_content': {
                'content_type': 'api_documentation',
                'confidence': 0.3,
                'security_indicators': {'security_tools': 0.2},
                'detected_entities': {},
                'recommendation': 'Use technical enrichment',
                'analysis_depth': 'basic'
            },
            'business_content': {
                'content_type': 'business_content',
                'confidence': 0.1,
                'security_indicators': {},
                'detected_entities': {},
                'recommendation': 'Use minimal enrichment',
                'analysis_depth': 'basic'
            }
        }

    @pytest.fixture
    def enhanced_agent(self, mock_kafka_config):
        """Create enhanced RSS collector agent for testing."""
        with patch('collector_agent.enhanced_rss_collector.RssFeedFetcherTool'), \
             patch('collector_agent.enhanced_rss_collector.ArticleExtractorTool'), \
             patch('collector_agent.enhanced_rss_collector.RedisDedupeStore'):
            
            agent = EnhancedRssCollectorAgent(
                **mock_kafka_config,
                use_optimized_enrichment=True,
                fallback_enabled=True
            )
            
            # Mock components
            agent.fetcher = Mock()
            agent.extractor = Mock()
            agent.dedupe_store = Mock()
            agent.producer = Mock()
            agent.schema_validator = Mock()
            
            return agent

    def test_enhanced_agent_initialization(self, enhanced_agent):
        """Test that enhanced agent initializes correctly."""
        assert isinstance(enhanced_agent.feed_enricher, EnhancedFeedEnricher)
        assert hasattr(enhanced_agent, 'content_analyzer')
        assert hasattr(enhanced_agent, 'enrichment_stats')
        assert enhanced_agent.enrichment_stats['total'] == 0

    def test_content_analysis_with_retry(self, enhanced_agent):
        """Test content analysis with retry logic."""
        # Mock a feed record
        record = FeedRecord(
            url=AnyUrl('https://example.com/test'),
            title='Test Security Alert',
            raw_content='APT29 campaign targeting financial institutions...',
            metadata={}
        )
        
        # Mock the content analyzer
        with patch.object(enhanced_agent.content_analyzer, 'analyze_content') as mock_analyze:
            mock_analyze.return_value = Mock(
                content_type='security_threat',
                confidence=0.85,
                security_indicators={'threat_actors': 0.9},
                detected_entities={'threat_actors': ['APT29']},
                recommendation='Use advanced analysis',
                analysis_depth='comprehensive'
            )
            
            result = enhanced_agent._analyze_content_with_retry(record)
            
            assert result['content_type'] == 'security_threat'
            assert result['confidence'] == 0.85
            assert 'APT29' in result['detected_entities']['threat_actors']
            mock_analyze.assert_called_once()

    def test_security_content_detection(self, enhanced_agent, mock_content_analysis_results):
        """Test that security content is correctly detected and routed."""
        record = FeedRecord(
            url=AnyUrl('https://example.com/security-alert'),
            title='Critical Security Alert',
            raw_content='APT29 campaign discovered targeting financial institutions...',
            metadata={}
        )
        
        # Mock content analysis to return security content
        with patch.object(enhanced_agent, '_analyze_content_with_retry') as mock_analyze:
            mock_analyze.return_value = mock_content_analysis_results['security_content']
            
            should_enhance = enhanced_agent._should_use_enhanced_enrichment(record)
            
            assert should_enhance is True
            mock_analyze.assert_called_once_with(record)

    def test_api_content_detection(self, enhanced_agent, mock_content_analysis_results):
        """Test that API content is correctly identified but doesn't trigger security analysis."""
        record = FeedRecord(
            url=AnyUrl('https://api.example.com/docs'),
            title='API Documentation v2.0 - Complete REST API Guide',
            description='This comprehensive API documentation covers all REST endpoints, authentication methods, and integration examples for developers.',
            raw_content='REST API endpoints with OAuth authentication...',
            metadata={}
        )
        
        # Mock content analysis to return API content
        with patch.object(enhanced_agent, '_analyze_content_with_retry') as mock_analyze:
            mock_analyze.return_value = mock_content_analysis_results['api_content']
            
            should_enhance = enhanced_agent._should_use_enhanced_enrichment(record)
            
            # API content should not trigger enhanced enrichment
            assert should_enhance is False
            mock_analyze.assert_called_once_with(record)

    def test_enrichment_routing_logic(self, enhanced_agent, mock_content_analysis_results):
        """Test that enrichment routing works correctly for different content types."""
        # Test security content routing
        security_record = FeedRecord(
            url=AnyUrl('https://example.com/security'),
            title='Short title',  # Needs enrichment
            raw_content='APT29 campaign...',
            metadata={'content_analysis': mock_content_analysis_results['security_content']}
        )
        
        # Test business content routing
        business_record = FeedRecord(
            url=AnyUrl('https://example.com/business'),
            title='This is a sufficiently long title that does not need enrichment',
            description='This is a sufficiently long description that provides adequate context and information',
            raw_content='Company earnings report...',
            metadata={'content_analysis': mock_content_analysis_results['business_content']}
        )
        
        with patch.object(enhanced_agent, '_analyze_content_with_retry') as mock_analyze:
            # Security content should use enhanced enrichment
            mock_analyze.return_value = mock_content_analysis_results['security_content']
            assert enhanced_agent._should_use_enhanced_enrichment(security_record) is True
            
            # Business content with good title/description should not
            mock_analyze.return_value = mock_content_analysis_results['business_content']
            assert enhanced_agent._should_use_enhanced_enrichment(business_record) is False

    def test_retry_logic_content_extraction(self, enhanced_agent):
        """Test retry logic for content extraction."""
        url = 'https://example.com/test'
        
        # Mock extractor to fail twice then succeed
        call_count = 0
        def mock_call(test_url):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ConnectionError("Network error")
            return {
                'raw_html': '<html>Test content</html>',
                'text': 'Test content',
                'extraction_quality': 'good'
            }
        
        enhanced_agent.extractor.call = mock_call
        
        # Should succeed after retries
        result = enhanced_agent._extract_content_with_retry(url)
        
        assert result['text'] == 'Test content'
        assert call_count == 3  # Failed twice, succeeded on third try

    def test_retry_logic_feed_enrichment(self, enhanced_agent):
        """Test retry logic for feed enrichment."""
        record = FeedRecord(
            url=AnyUrl('https://example.com/test'),
            title='Test',
            raw_content='Test content',
            metadata={}
        )
        
        # Mock enricher to fail once then succeed
        call_count = 0
        def mock_enrich(test_record):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Enrichment error")
            setattr(test_record.metadata, 'enrichment_status', 'success')
            return test_record
        
        enhanced_agent.feed_enricher.enrich = mock_enrich
        
        # Should succeed after retry
        result = enhanced_agent._enrich_feed_with_retry(record)
        
        assert hasattr(result.metadata, 'enrichment_status') and getattr(result.metadata, 'enrichment_status') == 'success'
        assert call_count == 2  # Failed once, succeeded on second try

    def test_process_feed_entry_security_content(self, enhanced_agent, sample_feed_entries, mock_content_analysis_results):
        """Test processing of security content feed entry."""
        entry = sample_feed_entries[0]  # Security alert entry
        feed_url = 'https://feeds.example.com/security'
        
        # Mock all dependencies
        enhanced_agent.dedupe_store.set_if_not_exists.return_value = True
        enhanced_agent.extractor.call.return_value = {
            'raw_html': '<html>APT29 campaign details...</html>',
            'text': 'APT29 campaign targeting financial institutions...',
            'extraction_quality': 'good',
            'extraction_method': 'readability'
        }
        
        with patch('requests.head') as mock_head, \
             patch.object(enhanced_agent, '_analyze_content_with_retry') as mock_analyze, \
             patch.object(enhanced_agent.feed_enricher, 'enrich') as mock_enrich:
            
            # Mock HEAD request
            mock_response = Mock()
            mock_response.headers = {'Content-Type': 'text/html'}
            mock_head.return_value = mock_response
            
            # Mock content analysis
            mock_analyze.return_value = mock_content_analysis_results['security_content']
            
            # Mock enrichment
            def enrich_record(record):
                setattr(record.metadata, 'enrichment_type', 'advanced_security')
                setattr(record.metadata, 'threat_classification', 'APT Campaign')
                setattr(record.metadata, 'content_analysis', mock_content_analysis_results['security_content'])
                return record
            mock_enrich.side_effect = enrich_record
            
            # Process entry
            result = enhanced_agent._process_feed_entry(entry, feed_url)
            
            assert result is not None
            assert result.title == 'Critical Security Alert'
            assert hasattr(result.metadata, 'content_analysis')
            mock_enrich.assert_called_once()

    def test_process_feed_entry_api_content(self, enhanced_agent, sample_feed_entries, mock_content_analysis_results):
        """Test processing of API documentation entry."""
        entry = sample_feed_entries[1]  # API docs entry
        feed_url = 'https://feeds.example.com/tech'
        
        # Mock dependencies
        enhanced_agent.dedupe_store.set_if_not_exists.return_value = True
        enhanced_agent.extractor.call.return_value = {
            'raw_html': '<html>API documentation...</html>',
            'text': 'REST API endpoints with OAuth authentication...',
            'extraction_quality': 'good'
        }
        
        with patch('requests.head') as mock_head, \
             patch.object(enhanced_agent, '_analyze_content_with_retry') as mock_analyze, \
             patch.object(enhanced_agent.feed_enricher, 'enrich') as mock_enrich:
            
            # Mock HEAD request
            mock_response = Mock()
            mock_response.headers = {'Content-Type': 'text/html'}
            mock_head.return_value = mock_response
            
            # Mock content analysis (API content)
            mock_analyze.return_value = mock_content_analysis_results['api_content']
            
            # Process entry
            result = enhanced_agent._process_feed_entry(entry, feed_url)
            
            assert result is not None
            assert result.title == 'API Documentation v2.0'
            assert hasattr(result.metadata, 'enrichment_status') and getattr(result.metadata, 'enrichment_status') == 'skipped'
            # Should not call enrichment for API content
            mock_enrich.assert_not_called()

    def test_enrichment_stats_tracking(self, enhanced_agent):
        """Test that enrichment statistics are tracked correctly."""
        # Simulate processing different content types
        enhanced_agent.enrichment_stats = {
            'total': 10,
            'security_content': 3,
            'api_content': 2,
            'general_content': 4,
            'errors': 1
        }
        
        stats = enhanced_agent.get_enrichment_stats()
        
        assert stats['total'] == 10
        assert stats['security_percentage'] == 30.0
        assert stats['api_percentage'] == 20.0
        assert stats['general_percentage'] == 40.0
        assert stats['error_rate'] == 10.0

    def test_error_handling_extraction_failure(self, enhanced_agent, sample_feed_entries):
        """Test error handling when content extraction fails."""
        entry = sample_feed_entries[0]
        feed_url = 'https://feeds.example.com/test'
        
        # Mock dedupe to allow processing
        enhanced_agent.dedupe_store.set_if_not_exists.return_value = True
        
        # Mock extractor to always fail
        enhanced_agent.extractor.call.side_effect = Exception("Network error")
        
        with patch('requests.head') as mock_head:
            # Mock HEAD request
            mock_response = Mock()
            mock_response.headers = {'Content-Type': 'text/html'}
            mock_head.return_value = mock_response
            
            # Process should continue despite extraction failure
            result = enhanced_agent._process_feed_entry(entry, feed_url)
            
            assert result is not None
            assert hasattr(result.metadata, 'extraction_status') and getattr(result.metadata, 'extraction_status') == 'failed'
            assert hasattr(result.metadata, 'extraction_error')

    def test_error_handling_enrichment_failure(self, enhanced_agent, sample_feed_entries, mock_content_analysis_results):
        """Test error handling when enrichment fails."""
        entry = sample_feed_entries[0]
        feed_url = 'https://feeds.example.com/test'
        
        # Mock dependencies
        enhanced_agent.dedupe_store.set_if_not_exists.return_value = True
        enhanced_agent.extractor.call.return_value = {
            'raw_html': '<html>Test content</html>',
            'text': 'Test content'
        }
        
        with patch('requests.head') as mock_head, \
             patch.object(enhanced_agent, '_analyze_content_with_retry') as mock_analyze, \
             patch.object(enhanced_agent.feed_enricher, 'enrich') as mock_enrich:
            
            # Mock HEAD request
            mock_response = Mock()
            mock_response.headers = {'Content-Type': 'text/html'}
            mock_head.return_value = mock_response
            
            # Mock content analysis (security content)
            mock_analyze.return_value = mock_content_analysis_results['security_content']
            
            # Mock enrichment to fail
            mock_enrich.side_effect = Exception("Enrichment error")
            
            # Process should continue despite enrichment failure
            result = enhanced_agent._process_feed_entry(entry, feed_url)
            
            assert result is not None
            # Should have attempted enrichment (with retries) but fell back to original record
            assert mock_enrich.call_count >= 1  # At least one attempt, possibly more due to retries

    def test_schema_validation_and_publishing(self, enhanced_agent):
        """Test schema validation and publishing flow."""
        record = FeedRecord(
            url=AnyUrl('https://example.com/test'),
            title='Test Article',
            description='Test description',
            raw_content='Test content',
            metadata={}
        )
        
        # Mock successful validation
        enhanced_agent.schema_validator.validate.return_value = None
        
        # Test successful publishing
        enhanced_agent._publish_enriched_record(record)
        
        enhanced_agent.schema_validator.validate.assert_called_once()
        enhanced_agent.producer.send.assert_called_once_with(
            enhanced_agent.raw_intel_topic, 
            record
        )

    def test_schema_validation_failure(self, enhanced_agent):
        """Test handling of schema validation failures."""
        record = FeedRecord(
            url=AnyUrl('https://example.com/test'),
            title='Test Article',
            metadata={}
        )
        
        # Mock validation failure
        from jsonschema import ValidationError
        enhanced_agent.schema_validator.validate.side_effect = ValidationError("Invalid schema")
        
        # Should send to DLQ
        enhanced_agent._publish_enriched_record(record)
        
        # Should have called send twice: once for DLQ
        assert enhanced_agent.producer.send.call_count == 1
        dlq_call = enhanced_agent.producer.send.call_args_list[0]
        assert dlq_call[0][0].startswith('dead-letter.')

    def test_deduplication_logic(self, enhanced_agent, sample_feed_entries):
        """Test that deduplication prevents reprocessing."""
        entry = sample_feed_entries[0]
        feed_url = 'https://feeds.example.com/test'
        
        # Mock dedupe to indicate already seen
        enhanced_agent.dedupe_store.set_if_not_exists.return_value = False
        
        result = enhanced_agent._process_feed_entry(entry, feed_url)
        
        # Should return None for duplicate
        assert result is None
        enhanced_agent.dedupe_store.set_if_not_exists.assert_called_once()

    def test_content_type_filtering(self, enhanced_agent, sample_feed_entries):
        """Test filtering of non-HTML content types."""
        entry = sample_feed_entries[0]
        feed_url = 'https://feeds.example.com/test'
        
        # Mock dedupe to allow processing
        enhanced_agent.dedupe_store.set_if_not_exists.return_value = True
        
        with patch('requests.head') as mock_head:
            # Mock HEAD request with non-HTML content type
            mock_response = Mock()
            mock_response.headers = {'Content-Type': 'application/pdf'}
            mock_head.return_value = mock_response
            
            result = enhanced_agent._process_feed_entry(entry, feed_url)
            
            # Should return None for non-HTML content
            assert result is None

    def test_feed_url_extraction(self, enhanced_agent):
        """Test feed URL extraction from different message formats."""
        # Test dict format
        dict_message = {'url': 'https://feeds.example.com/rss'}
        url = enhanced_agent._extract_feed_url(dict_message)
        assert url == 'https://feeds.example.com/rss'
        
        # Test string format
        string_message = 'https://feeds.example.com/rss'
        url = enhanced_agent._extract_feed_url(string_message)
        assert url == 'https://feeds.example.com/rss'
        
        # Test invalid URL
        invalid_message = 'not-a-url'
        url = enhanced_agent._extract_feed_url(invalid_message)
        assert url is None

    @pytest.mark.asyncio
    async def test_performance_metrics(self, enhanced_agent):
        """Test that performance metrics are collected."""
        from collector_agent.enhanced_rss_collector import (
            CONTENT_ANALYSIS_COUNTER,
            ENRICHMENT_METHOD_COUNTER,
            SECURITY_DETECTION_HISTOGRAM,
            ENRICHMENT_DURATION_HISTOGRAM
        )
        
        # These should be available and callable
        assert hasattr(CONTENT_ANALYSIS_COUNTER, 'labels')
        assert hasattr(ENRICHMENT_METHOD_COUNTER, 'labels')
        assert hasattr(SECURITY_DETECTION_HISTOGRAM, 'observe')
        assert hasattr(ENRICHMENT_DURATION_HISTOGRAM, 'labels')

    def test_fallback_to_basic_enricher(self, mock_kafka_config):
        """Test fallback to basic enricher when enhanced enricher fails."""
        with patch('collector_agent.enhanced_rss_collector.EnhancedFeedEnricher') as mock_enhanced:
            # Mock enhanced enricher initialization to fail
            mock_enhanced.side_effect = Exception("Enhanced enricher not available")
            
            with patch('collector_agent.enhanced_rss_collector.RssFeedFetcherTool'), \
                 patch('collector_agent.enhanced_rss_collector.ArticleExtractorTool'), \
                 patch('collector_agent.enhanced_rss_collector.RedisDedupeStore'):
                
                agent = EnhancedRssCollectorAgent(**mock_kafka_config)
                
                # Should have fallen back to parent's enricher
                # (The parent class sets up the basic enricher)
                assert hasattr(agent, 'feed_enricher')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])