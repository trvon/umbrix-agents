import sys
import os
import pytest
from unittest.mock import patch, MagicMock
sys.path.append(os.path.join(os.path.dirname(__file__), '../agents'))

from common_tools.content_tools import (
    ArticleExtractorTool, 
    NonArticleContentError,
    ContentExtractionError
)

# Mock HTML content for testing
MOCK_ARTICLE_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Test Article</title>
    <meta name="description" content="This is a test article for content extraction">
</head>
<body>
    <article>
        <h1>Test Article Heading</h1>
        <p>This is the first paragraph of the test article with good content.</p>
        <p>This is the second paragraph with more detailed information about the subject.</p>
        <p>This is the third paragraph that concludes the article with a summary.</p>
    </article>
</body>
</html>
"""

MOCK_NON_ARTICLE_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Login Page</title>
    <meta name="description" content="Login to your account">
</head>
<body>
    <div class="login-form">
        <h1>Sign In</h1>
        <form>
            <input type="text" placeholder="Username">
            <input type="password" placeholder="Password">
            <button type="submit">Login</button>
        </form>
    </div>
</body>
</html>
"""


class TestContentExtraction:
    
    @patch('common_tools.content_tools.requests.get')
    @patch('common_tools.content_tools.PageClassifierModule')
    def test_article_extraction_fallback(self, mock_classifier, mock_get):
        """Test that content extraction works with fallbacks"""
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.text = MOCK_ARTICLE_HTML
        mock_response.raise_for_status.return_value = None  # Don't raise exception
        mock_get.return_value = mock_response
        
        # Mock classifier to return "article" type
        mock_classifier_instance = MagicMock()
        mock_prediction = MagicMock()
        mock_prediction.page_type = "article"
        mock_classifier_instance.forward.return_value = mock_prediction
        mock_classifier.return_value = mock_classifier_instance
        
        # We need to ensure all extraction methods fail to test the final fallback
        patches = [
            patch('common_tools.content_tools.BeautifulSoup', return_value=None),
            patch.object(ArticleExtractorTool, '_extract_with_bs4', side_effect=Exception("BS4 extraction failed")),
            patch.object(ArticleExtractorTool, '_extract_with_readability', side_effect=Exception("Readability extraction failed")),
            patch.object(ArticleExtractorTool, '_extract_with_newspaper', side_effect=Exception("Newspaper extraction failed"))
        ]
        
        # Apply all patches to force the fallback path
        for p in patches:
            p.start()
        
        try:
            # Create extractor and extract content
            extractor = ArticleExtractorTool()
            result = extractor.call("https://example.com/test-article")
            
            # Check that extraction succeeded despite all methods failing
            assert result is not None
            assert 'text' in result
            assert 'extraction_method' in result
            assert 'extraction_quality' in result
            assert 'extraction_confidence' in result
            assert 'extraction_metrics' in result
            assert 'extraction_attempts' in result
            
            # We should still get some content, even if it's just the raw HTML
            assert len(result['text']) > 0
            assert result['extraction_confidence'] > 0
            
            # The extraction method should be one of our fallbacks
            assert result['extraction_method'] in ['error_fallback', 'fallback_all_text']
        finally:
            # Stop all patches
            for p in patches:
                p.stop()
    
    @patch('common_tools.content_tools.requests.get')
    @patch('common_tools.content_tools.PageClassifierModule')
    def test_non_article_detection(self, mock_classifier, mock_get):
        """Test that non-article content is detected and handled properly"""
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.text = MOCK_NON_ARTICLE_HTML
        mock_response.raise_for_status.return_value = None  # Don't raise exception
        mock_get.return_value = mock_response
        
        # Mock classifier to return "login_page" type
        mock_classifier_instance = MagicMock()
        mock_prediction = MagicMock()
        mock_prediction.page_type = "login_page"
        mock_classifier_instance.forward.return_value = mock_prediction
        mock_classifier.return_value = mock_classifier_instance
        
        # Create extractor
        extractor = ArticleExtractorTool()
        
        # Should extract content but mark it as a non-article page type
        result = extractor.call("https://example.com/login")
        
        # Verify page type is correctly identified
        assert result is not None
        assert 'page_type' in result
        assert result['page_type'] == 'login_page'
    
    @patch('common_tools.content_tools.requests.get')
    @patch('common_tools.content_tools.PageClassifierModule')
    @patch('common_tools.content_tools.BeautifulSoup')
    def test_extraction_failure_fallback(self, mock_bs, mock_classifier, mock_get):
        """Test that when primary extraction fails, fallbacks are attempted"""
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.text = MOCK_ARTICLE_HTML
        mock_response.raise_for_status.return_value = None  # Don't raise exception
        mock_get.return_value = mock_response
        
        # Mock classifier to return "article" type
        mock_classifier_instance = MagicMock()
        mock_prediction = MagicMock()
        mock_prediction.page_type = "article"
        mock_classifier_instance.forward.return_value = mock_prediction
        mock_classifier.return_value = mock_classifier_instance
        
        # Make BS4 extraction method fail by mocking soup
        mock_soup = MagicMock()
        mock_soup.find.return_value = None  # No article tag
        mock_soup.get_text.return_value = "Fallback text content"
        mock_bs.return_value = mock_soup
        
        # Create extractor with mocked methods to control behavior
        extractor = ArticleExtractorTool()
        
        # Replace extraction methods with mocks, ensuring they fail except newspaper
        original_methods = extractor.extraction_methods.copy()
        extractor.extraction_methods = []
        
        # Mock BS4 to fail
        mock_bs4 = MagicMock(side_effect=Exception("BS4 extraction failed"))
        extractor.extraction_methods.append(mock_bs4)
        
        # Mock newspaper to succeed with known result
        mock_newspaper_result = {
            'text': 'Content extracted with newspaper',
            'method': 'newspaper3k', 
            'confidence': 0.75,
            'metrics': {'word_count': 10}
        }
        mock_newspaper = MagicMock(return_value=mock_newspaper_result)
        extractor.extraction_methods.append(mock_newspaper)
        
        # If readability was originally available, mock it to fail so newspaper wins
        if len(original_methods) > 2:  # bs4, newspaper, readability
            mock_readability = MagicMock(side_effect=Exception("Readability extraction failed"))
            extractor.extraction_methods.append(mock_readability)
        
        # Should succeed with newspaper extraction
        result = extractor.call("https://example.com/test-article")
        
        # Should use newspaper method
        assert result['extraction_method'] == 'newspaper3k'
        assert result['text'] == 'Content extracted with newspaper'
        
        # Should record the failed attempt
        assert len(result['extraction_attempts']) >= 2  # At least 2 attempts (failed BS4, successful newspaper)
        
        # Check if BS4 attempt was recorded as failed
        bs4_attempt = next((a for a in result['extraction_attempts'] if a.get('method') == '_extract_with_bs4'), None)
        assert bs4_attempt is not None
        assert bs4_attempt['success'] is False
    
    @patch('common_tools.content_tools.requests.get')
    @patch('common_tools.content_tools.PageClassifierModule')
    def test_normal_article_extraction(self, mock_classifier, mock_get):
        """Test that normal article extraction works as expected"""
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.text = MOCK_ARTICLE_HTML
        mock_response.raise_for_status.return_value = None  # Don't raise exception
        mock_get.return_value = mock_response
        
        # Mock classifier to return "article" type
        mock_classifier_instance = MagicMock()
        mock_prediction = MagicMock()
        mock_prediction.page_type = "article"
        mock_classifier_instance.forward.return_value = mock_prediction
        mock_classifier.return_value = mock_classifier_instance
        
        # Create extractor and extract content
        extractor = ArticleExtractorTool()
        result = extractor.call("https://example.com/test-article")
        
        # Check that extraction succeeded
        assert result is not None
        assert 'text' in result
        assert 'extraction_method' in result
        assert 'extraction_quality' in result
        assert 'extraction_confidence' in result
        assert 'extraction_metrics' in result
        assert 'extraction_attempts' in result
        
        # At least some content should be extracted
        assert len(result['text']) > 0
        assert result['extraction_confidence'] > 0
        
        # Should find the paragraphs in the article
        assert "first paragraph" in result['text']
        assert "second paragraph" in result['text']
        assert "third paragraph" in result['text']
