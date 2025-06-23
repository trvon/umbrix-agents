"""
Enhanced tests for DSPy-based feed enrichment functionality with improved stability.

Tests cover:
- DSPyFeedEnricher initialization with and without LM configuration
- Feed enrichment with various content scenarios
- Field update logic for titles, descriptions, and metadata
- Error handling for missing configurations and processing failures
- Edge cases and boundary conditions
- DSPy integration and ChainOfThought usage
"""

import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
from pydantic import AnyUrl

from common_tools.feed_enricher import DSPyFeedEnricher
from common_tools.models.feed_record import FeedRecord


class TestDSPyFeedEnricher:
    """Test suite for DSPyFeedEnricher class with improved stability."""
    
    @pytest.fixture
    def mock_dspy_prediction(self):
        """Create a mock DSPy prediction object."""
        prediction = Mock()
        prediction.guessed_title = "AI-Enhanced Security Alert"
        prediction.guessed_description = "Comprehensive analysis of critical security vulnerability affecting multiple systems"
        prediction.is_security_focused = True
        prediction.standardized_vendor_name = "CyberSec Corp"
        prediction.requires_payment = False
        prediction.rationale = "Content analysis indicates security-focused material with vulnerability information"
        return prediction
    
    @pytest.fixture
    def sample_feed_record(self):
        """Create a sample FeedRecord for testing."""
        return FeedRecord(
            url=AnyUrl("https://security.example.com/alert-123"),
            raw_content="<html><body><h1>Critical Vulnerability</h1><p>A severe security flaw has been discovered...</p></body></html>",
            title="Vuln Alert",
            description="Brief info"
        )
    
    def test_initialization_with_configured_lm(self):
        """Test DSPyFeedEnricher initialization when DSPy LM is properly configured."""
        mock_lm = Mock()
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought') as mock_cot, \
             patch('builtins.print') as mock_print:
            
            mock_cot.return_value = Mock()
            
            enricher = DSPyFeedEnricher()
            
            # Should not print any warnings when LM is configured
            mock_print.assert_not_called()
            
            # Should create ChainOfThought instance
            mock_cot.assert_called_once()
            assert hasattr(enricher, 'enrich_feed_item')
    
    def test_initialization_without_lm_shows_warning(self):
        """Test DSPyFeedEnricher initialization shows warning when DSPy LM is not configured."""
        with patch('dspy.settings.lm', None), \
             patch('dspy.ChainOfThought') as mock_cot, \
             patch('builtins.print') as mock_print:
            
            mock_cot.return_value = Mock()
            
            enricher = DSPyFeedEnricher()
            
            # Should print warning about missing LM configuration
            mock_print.assert_called_once()
            warning_args = mock_print.call_args[0]
            assert "Warning: DSPy LM is not configured" in warning_args[0]
            
            # Should verify stderr is used
            warning_kwargs = mock_print.call_args[1]
            assert warning_kwargs.get('file') == sys.stderr
    
    def test_enrich_without_lm_configured_returns_error_status(self):
        """Test enrichment when DSPy LM is not configured returns error status."""
        with patch('dspy.settings.lm', None), \
             patch('dspy.ChainOfThought'):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Test content</body></html>",
                title="Test Title"
            )
            
            result = enricher.enrich(record)
            
            # Should set error status in metadata
            assert hasattr(result.metadata, 'dspy_enrichment_status')
            assert result.metadata.dspy_enrichment_status == 'error_dspy_lm_not_configured'
            
            # Original record should be preserved
            assert result.url == record.url
            assert result.raw_content == record.raw_content
            assert result.title == record.title
    
    def test_enrich_without_raw_content_returns_skipped_status(self):
        """Test enrichment when record has no raw_content returns skipped status."""
        mock_lm = Mock()
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought'):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                title="Test Title"
                # No raw_content provided
            )
            
            result = enricher.enrich(record)
            
            # Should set skipped status in metadata
            assert hasattr(result.metadata, 'dspy_enrichment_status')
            assert result.metadata.dspy_enrichment_status == 'skipped_no_raw_content'
            
            # Original record should be preserved
            assert result.url == record.url
            assert result.title == record.title
    
    def test_enrich_with_empty_raw_content_returns_skipped_status(self):
        """Test enrichment when record has empty raw_content returns skipped status."""
        mock_lm = Mock()
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought'):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="",  # Empty string
                title="Test Title"
            )
            
            result = enricher.enrich(record)
            
            # Should set skipped status for empty content
            assert hasattr(result.metadata, 'dspy_enrichment_status')
            assert result.metadata.dspy_enrichment_status == 'skipped_no_raw_content'
    
    def test_successful_enrichment_updates_all_fields(self, mock_dspy_prediction, sample_feed_record):
        """Test successful enrichment updates all fields appropriately."""
        mock_lm = Mock()
        mock_enricher = Mock()
        mock_enricher.return_value = mock_dspy_prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            result = enricher.enrich(sample_feed_record)
            
            # Verify DSPy enricher was called with correct parameters
            mock_enricher.assert_called_once_with(
                raw_content=sample_feed_record.raw_content,
                current_title="Vuln Alert",
                current_description="Brief info"
            )
            
            # Check success status
            assert result.metadata.dspy_enrichment_status == 'success'
            
            # Check enrichment metadata
            enrichment_data = result.metadata.dspy_enrichment
            assert enrichment_data['guessed_title'] == "AI-Enhanced Security Alert"
            assert enrichment_data['guessed_description'] == "Comprehensive analysis of critical security vulnerability affecting multiple systems"
            assert enrichment_data['is_security_focused'] is True
            assert enrichment_data['standardized_vendor_name'] == "CyberSec Corp"
            assert enrichment_data['requires_payment'] is False
            assert enrichment_data['rationale'] == "Content analysis indicates security-focused material with vulnerability information"
    
    def test_enrichment_replaces_short_title(self, mock_dspy_prediction):
        """Test that enrichment replaces titles shorter than 10 characters."""
        mock_lm = Mock()
        mock_enricher = Mock()
        mock_enricher.return_value = mock_dspy_prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Content</body></html>",
                title="Short"  # Less than 10 characters
            )
            
            result = enricher.enrich(record)
            
            # Title should be replaced with guessed title
            assert result.title == "AI-Enhanced Security Alert"
    
    def test_enrichment_preserves_long_title(self, mock_dspy_prediction):
        """Test that enrichment preserves titles 10 characters or longer."""
        mock_lm = Mock()
        mock_enricher = Mock()
        mock_enricher.return_value = mock_dspy_prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Content</body></html>",
                title="This is a sufficiently long original title"
            )
            
            result = enricher.enrich(record)
            
            # Original title should be preserved
            assert result.title == "This is a sufficiently long original title"
    
    def test_enrichment_replaces_short_description(self, mock_dspy_prediction):
        """Test that enrichment replaces descriptions shorter than 20 characters."""
        mock_lm = Mock()
        mock_enricher = Mock()
        mock_enricher.return_value = mock_dspy_prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Content</body></html>",
                description="Brief desc"  # Less than 20 characters
            )
            
            result = enricher.enrich(record)
            
            # Description should be replaced with guessed description
            assert result.description == "Comprehensive analysis of critical security vulnerability affecting multiple systems"
    
    def test_enrichment_preserves_long_description(self, mock_dspy_prediction):
        """Test that enrichment preserves descriptions 20 characters or longer."""
        mock_lm = Mock()
        mock_enricher = Mock()
        mock_enricher.return_value = mock_dspy_prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Content</body></html>",
                description="This is a sufficiently long original description that should be preserved"
            )
            
            result = enricher.enrich(record)
            
            # Original description should be preserved
            assert result.description == "This is a sufficiently long original description that should be preserved"
    
    def test_enrichment_handles_missing_title_and_description(self, mock_dspy_prediction):
        """Test enrichment when original record has no title or description."""
        mock_lm = Mock()
        mock_enricher = Mock()
        mock_enricher.return_value = mock_dspy_prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Content</body></html>"
                # No title or description
            )
            
            result = enricher.enrich(record)
            
            # Should set both title and description from predictions
            assert result.title == "AI-Enhanced Security Alert"
            assert result.description == "Comprehensive analysis of critical security vulnerability affecting multiple systems"
    
    def test_enrichment_updates_boolean_fields(self, mock_dspy_prediction):
        """Test that enrichment properly updates boolean fields."""
        mock_lm = Mock()
        mock_enricher = Mock()
        mock_enricher.return_value = mock_dspy_prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Content</body></html>"
            )
            
            result = enricher.enrich(record)
            
            # Boolean fields should be updated from predictions
            assert result.is_security_focused is True
            assert result.requires_payment is False
    
    def test_enrichment_sets_vendor_name(self, mock_dspy_prediction):
        """Test that enrichment sets vendor name when provided."""
        mock_lm = Mock()
        mock_enricher = Mock()
        mock_enricher.return_value = mock_dspy_prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Content</body></html>"
            )
            
            result = enricher.enrich(record)
            
            # Vendor name should be set from prediction
            assert result.vendor_name == "CyberSec Corp"
    
    def test_enrichment_skips_vendor_name_when_none(self):
        """Test that enrichment doesn't set vendor name when prediction returns None."""
        mock_lm = Mock()
        mock_enricher = Mock()
        
        # Create prediction with None vendor name
        prediction = Mock()
        prediction.guessed_title = "Title"
        prediction.guessed_description = "Description"
        prediction.is_security_focused = False
        prediction.standardized_vendor_name = None  # Explicitly None
        prediction.requires_payment = True
        
        mock_enricher.return_value = prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Content</body></html>"
            )
            
            result = enricher.enrich(record)
            
            # Vendor name should not be set
            assert not hasattr(result, 'vendor_name') or result.vendor_name is None
    
    def test_enrichment_handles_prediction_without_rationale(self):
        """Test enrichment when prediction doesn't have rationale attribute."""
        mock_lm = Mock()
        mock_enricher = Mock()
        
        # Create prediction without rationale
        prediction = Mock()
        prediction.guessed_title = "Title"
        prediction.guessed_description = "Description"
        prediction.is_security_focused = True
        prediction.standardized_vendor_name = "Vendor"
        prediction.requires_payment = False
        # Don't set rationale attribute
        if hasattr(prediction, 'rationale'):
            delattr(prediction, 'rationale')
        
        mock_enricher.return_value = prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Content</body></html>"
            )
            
            result = enricher.enrich(record)
            
            # Should succeed without rationale
            assert result.metadata.dspy_enrichment_status == 'success'
            enrichment_data = result.metadata.dspy_enrichment
            assert 'rationale' not in enrichment_data
            assert enrichment_data['is_security_focused'] is True
    
    def test_enrichment_exception_handling(self):
        """Test that exceptions during DSPy processing are handled gracefully."""
        mock_lm = Mock()
        mock_enricher = Mock()
        mock_enricher.side_effect = Exception("DSPy processing failed unexpectedly")
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Content</body></html>",
                title="Original Title",
                description="Original Description"
            )
            
            result = enricher.enrich(record)
            
            # Should set error status in metadata
            assert result.metadata.dspy_enrichment_status == 'error'
            assert result.metadata.dspy_enrichment_error == "DSPy processing failed unexpectedly"
            
            # Original record should be preserved unchanged
            assert result.title == "Original Title"
            assert result.description == "Original Description"
            assert result.raw_content == "<html><body>Content</body></html>"
    
    def test_enrichment_with_complex_content(self, mock_dspy_prediction):
        """Test enrichment with complex HTML content and special characters."""
        mock_lm = Mock()
        mock_enricher = Mock()
        mock_enricher.return_value = mock_dspy_prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            complex_content = """
            <html>
                <head><title>Complex Page</title></head>
                <body>
                    <h1>Security Alert: CVE-2023-12345</h1>
                    <p>Vulnerability found in <code>example.dll</code></p>
                    <script>alert('xss');</script>
                    <div class="metadata">
                        Special chars: áéíóú, 中文, русский, العربية
                        Symbols: !@#$%^&*()_+-=[]{}|;:'"<>?,./ 
                    </div>
                </body>
            </html>
            """
            
            record = FeedRecord(
                url=AnyUrl("https://security.example.com/cve-2023-12345"),
                raw_content=complex_content,
                title="CVE Alert"
            )
            
            result = enricher.enrich(record)
            
            # Should handle complex content successfully
            assert result.metadata.dspy_enrichment_status == 'success'
            
            # DSPy should have been called with the complex content
            mock_enricher.assert_called_once_with(
                raw_content=complex_content,
                current_title="CVE Alert",
                current_description=None  # record has no description set
            )
    
    def test_multiple_enrichments_are_independent(self, mock_dspy_prediction):
        """Test that multiple enrichments don't interfere with each other."""
        mock_lm = Mock()
        mock_enricher = Mock()
        mock_enricher.return_value = mock_dspy_prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            
            # First record
            record1 = FeedRecord(
                url=AnyUrl("https://example.com/test1"),
                raw_content="<html><body>First content</body></html>",
                title="First"
            )
            
            # Second record
            record2 = FeedRecord(
                url=AnyUrl("https://example.com/test2"),
                raw_content="<html><body>Second content</body></html>",
                title="Second"
            )
            
            # Enrich both records
            result1 = enricher.enrich(record1)
            result2 = enricher.enrich(record2)
            
            # Both should be enriched successfully
            assert result1.metadata.dspy_enrichment_status == 'success'
            assert result2.metadata.dspy_enrichment_status == 'success'
            
            # Should have called DSPy enricher twice
            assert mock_enricher.call_count == 2
            
            # Verify independence - results should have same enriched values but different original URLs
            assert result1.url == AnyUrl("https://example.com/test1")
            assert result2.url == AnyUrl("https://example.com/test2")
            assert result1.title == "AI-Enhanced Security Alert"  # Both get same enriched title
            assert result2.title == "AI-Enhanced Security Alert"


class TestDSPyFeedEnricherEdgeCases:
    """Test edge cases and boundary conditions for DSPyFeedEnricher."""
    
    def test_enrichment_with_extremely_long_content(self):
        """Test enrichment with very long content."""
        mock_lm = Mock()
        mock_enricher = Mock()
        
        prediction = Mock()
        prediction.guessed_title = "Long Content Analysis"
        prediction.guessed_description = "Analysis of extremely long content"
        prediction.is_security_focused = False
        prediction.standardized_vendor_name = "ContentAnalyzer"
        prediction.requires_payment = False
        prediction.rationale = "Content is very long but not security-related"
        
        mock_enricher.return_value = prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            
            # Create very long content (50KB)
            long_content = "<html><body>" + "A" * 50000 + "</body></html>"
            
            record = FeedRecord(
                url=AnyUrl("https://example.com/long"),
                raw_content=long_content,
                title="Long"
            )
            
            result = enricher.enrich(record)
            
            # Should handle long content successfully
            assert result.metadata.dspy_enrichment_status == 'success'
            assert result.title == "Long Content Analysis"
    
    def test_enrichment_with_unicode_content(self):
        """Test enrichment with Unicode and special characters."""
        mock_lm = Mock()
        mock_enricher = Mock()
        
        prediction = Mock()
        prediction.guessed_title = "Unicode Content Analyzed"
        prediction.guessed_description = "Analysis of international content"
        prediction.is_security_focused = True
        prediction.standardized_vendor_name = "GlobalSec"
        prediction.requires_payment = False
        
        mock_enricher.return_value = prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            
            unicode_content = """
            <html><body>
                <h1>国际安全警报 🔒</h1>
                <p>Уведомление о безопасности</p>
                <p>تنبيه أمني هام</p>
                <p>Alerte de sécurité française</p>
                <p>Emoji test: 🚨🔐💻🛡️⚠️</p>
            </body></html>
            """
            
            record = FeedRecord(
                url=AnyUrl("https://global.example.com/alert"),
                raw_content=unicode_content,
                title="国际警报"
            )
            
            result = enricher.enrich(record)
            
            # Should handle Unicode content successfully
            assert result.metadata.dspy_enrichment_status == 'success'
            assert result.title == "Unicode Content Analyzed"
    
    def test_enrichment_preserves_existing_metadata(self):
        """Test that enrichment preserves existing metadata and adds new enrichment data."""
        mock_lm = Mock()
        mock_enricher = Mock()
        
        prediction = Mock()
        prediction.guessed_title = "Enhanced Title"
        prediction.guessed_description = "Enhanced Description"
        prediction.is_security_focused = True
        prediction.standardized_vendor_name = "Vendor"
        prediction.requires_payment = False
        prediction.rationale = "Analysis rationale"
        
        mock_enricher.return_value = prediction
        
        with patch('dspy.settings.lm', mock_lm), \
             patch('dspy.ChainOfThought', return_value=mock_enricher):
            
            enricher = DSPyFeedEnricher()
            
            record = FeedRecord(
                url=AnyUrl("https://example.com/test"),
                raw_content="<html><body>Content</body></html>",
                title="Title"
            )
            
            # Add some existing metadata
            record.metadata.existing_field = "existing_value"
            record.metadata.another_field = {"nested": "data"}
            
            result = enricher.enrich(record)
            
            # Should preserve existing metadata
            assert result.metadata.existing_field == "existing_value"
            assert result.metadata.another_field == {"nested": "data"}
            
            # Should add new enrichment metadata
            assert result.metadata.dspy_enrichment_status == 'success'
            assert hasattr(result.metadata, 'dspy_enrichment')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])