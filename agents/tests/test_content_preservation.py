"""
Test Content Preservation for Google ADK + DSPy Integration

This test suite validates the improved content extraction that preserves
structure for DSPy processing instead of "squashing" content.
"""

import pytest
from bs4 import BeautifulSoup
from unittest.mock import patch, MagicMock

from common_tools.content_preservation import (
    StructurePreservingExtractor, 
    ContentSection, 
    PreservedContent,
    create_dspy_optimized_content
)


class TestStructurePreservingExtractor:
    """Test the structure-preserving content extractor."""
    
    def setup_method(self):
        """Setup test instances."""
        self.extractor = StructurePreservingExtractor()
    
    def test_extractor_initialization(self):
        """Test that the extractor initializes properly."""
        assert self.extractor is not None
        assert hasattr(self.extractor, 'section_selectors')
        assert 'headline' in self.extractor.section_selectors
        assert 'paragraph' in self.extractor.section_selectors
        assert 'list' in self.extractor.section_selectors
    
    def test_extract_title_from_title_tag(self):
        """Test title extraction from HTML title tag."""
        html = """
        <html>
            <head><title>Advanced Persistent Threat Analysis</title></head>
            <body><h1>Some other heading</h1></body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        title = self.extractor._extract_title(soup)
        assert title == "Advanced Persistent Threat Analysis"
    
    def test_extract_title_fallback_to_h1(self):
        """Test title extraction falls back to h1 when title tag is missing."""
        html = """
        <html>
            <body><h1>Cybersecurity Threat Report</h1><p>Content here</p></body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        title = self.extractor._extract_title(soup)
        assert title == "Cybersecurity Threat Report"
    
    def test_find_main_content_semantic_html(self):
        """Test finding main content using semantic HTML elements."""
        html = """
        <html>
            <body>
                <header>Header content</header>
                <main>
                    <h1>Main Article</h1>
                    <p>This is the main content.</p>
                </main>
                <footer>Footer content</footer>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        main_content = self.extractor._find_main_content(soup)
        assert main_content is not None
        assert main_content.name == 'main'
        assert 'Main Article' in main_content.get_text()
    
    def test_find_main_content_article_tag(self):
        """Test finding main content using article tag."""
        html = """
        <html>
            <body>
                <div>Sidebar content</div>
                <article>
                    <h1>Security Analysis</h1>
                    <p>Detailed analysis of security threats.</p>
                </article>
                <div>Related links</div>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        main_content = self.extractor._find_main_content(soup)
        assert main_content is not None
        assert main_content.name == 'article'
        assert 'Security Analysis' in main_content.get_text()
    
    def test_determine_section_type(self):
        """Test section type determination for different HTML elements."""
        assert self.extractor._determine_section_type('h1') == 'headline'
        assert self.extractor._determine_section_type('h2') == 'headline'
        assert self.extractor._determine_section_type('p') == 'paragraph'
        assert self.extractor._determine_section_type('ul') == 'list'
        assert self.extractor._determine_section_type('blockquote') == 'quote'
        assert self.extractor._determine_section_type('code') == 'code'
    
    def test_determine_level(self):
        """Test hierarchy level determination."""
        assert self.extractor._determine_level('h1') == 1
        assert self.extractor._determine_level('h2') == 2
        assert self.extractor._determine_level('h6') == 6
        assert self.extractor._determine_level('p') == 0
    
    def test_extract_formatted_content_list(self):
        """Test list content extraction with preserved formatting."""
        html = """
        <ul>
            <li>First threat indicator</li>
            <li>Second threat indicator</li>
            <li>Third threat indicator</li>
        </ul>
        """
        element = BeautifulSoup(html, 'html.parser').find('ul')
        content = self.extractor._extract_formatted_content(element)
        
        assert '• First threat indicator' in content
        assert '• Second threat indicator' in content
        assert '• Third threat indicator' in content
        assert content.count('•') == 3
    
    def test_extract_formatted_content_quote(self):
        """Test quote content extraction with preserved formatting."""
        html = '<blockquote>This is a critical security finding.</blockquote>'
        element = BeautifulSoup(html, 'html.parser').find('blockquote')
        content = self.extractor._extract_formatted_content(element)
        
        assert content == '"This is a critical security finding."'
    
    def test_extract_formatted_content_code(self):
        """Test code content extraction with preserved formatting."""
        html = '<code>import security_scanner</code>'
        element = BeautifulSoup(html, 'html.parser').find('code')
        content = self.extractor._extract_formatted_content(element)
        
        assert content.startswith('```')
        assert content.endswith('```')
        assert 'import security_scanner' in content
    
    def test_extract_section_metadata(self):
        """Test section metadata extraction."""
        html = """
        <p class="important" id="findings">
            Critical finding with <a href="https://example.com">external link</a> 
            and <strong>emphasized text</strong>.
        </p>
        """
        element = BeautifulSoup(html, 'html.parser').find('p')
        metadata = self.extractor._extract_section_metadata(element, 'https://base.com')
        
        assert metadata['tag'] == 'p'
        assert 'important' in metadata['classes']
        assert metadata['id'] == 'findings'
        assert metadata['word_count'] > 0
        assert len(metadata['links']) == 1
        assert metadata['links'][0]['url'] == 'https://example.com'
        assert 'emphasized text' in metadata['emphasized_text']
    
    def test_extract_preserved_content_complete(self):
        """Test complete content extraction with structure preservation."""
        html = """
        <html>
            <head><title>Threat Intelligence Report</title></head>
            <body>
                <article>
                    <h1>Executive Summary</h1>
                    <p>This report analyzes recent cybersecurity threats.</p>
                    
                    <h2>Key Findings</h2>
                    <p>We identified several critical vulnerabilities.</p>
                    <ul>
                        <li>SQL injection in login form</li>
                        <li>Cross-site scripting vulnerability</li>
                        <li>Insecure direct object references</li>
                    </ul>
                    
                    <h2>Recommendations</h2>
                    <blockquote>
                        Immediate patching is required for all identified vulnerabilities.
                    </blockquote>
                    <p>Priority should be given to the SQL injection fix.</p>
                </article>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        preserved = self.extractor.extract_preserved_content(soup, 'https://security.com/report')
        
        # Test basic structure
        assert preserved.title == "Threat Intelligence Report"
        assert len(preserved.sections) > 0
        assert preserved.preservation_quality in ['excellent', 'good', 'fair', 'poor']
        
        # Test section types
        section_types = [section.section_type for section in preserved.sections]
        assert 'headline' in section_types
        assert 'paragraph' in section_types
        assert 'list' in section_types
        assert 'quote' in section_types
        
        # Test hierarchy
        headlines = [s for s in preserved.sections if s.section_type == 'headline']
        assert len(headlines) >= 2  # h1 and h2 elements
        
        # Test structured data
        assert 'document_title' in preserved.structured_data
        assert 'outline' in preserved.structured_data
        assert 'content_hierarchy' in preserved.structured_data
        assert preserved.structured_data['section_count'] == len(preserved.sections)
    
    def test_assess_preservation_quality(self):
        """Test quality assessment for preserved content."""
        # Create high-quality content sections
        high_quality_sections = [
            ContentSection("Executive Summary", "headline", 1, {}, 0),
            ContentSection("This is a detailed paragraph with analysis.", "paragraph", 0, {'word_count': 8}, 1),
            ContentSection("• Key finding 1\n• Key finding 2", "list", 0, {'word_count': 6}, 2),
            ContentSection('"Critical quote about security."', "quote", 0, {'word_count': 5}, 3),
        ]
        
        soup = BeautifulSoup('<html><body><p>test</p></body></html>', 'html.parser')
        quality = self.extractor._assess_preservation_quality(high_quality_sections, soup)
        
        assert quality in ['excellent', 'good', 'fair']
        
        # Test low-quality content
        low_quality_sections = [
            ContentSection("short", "paragraph", 0, {'word_count': 1}, 0)
        ]
        
        quality_low = self.extractor._assess_preservation_quality(low_quality_sections, soup)
        assert quality_low in ['poor', 'fair']


class TestDSPyOptimization:
    """Test DSPy content optimization."""
    
    def test_create_dspy_optimized_content(self):
        """Test creation of DSPy-optimized content structure."""
        # Create sample preserved content
        sections = [
            ContentSection("Security Alert", "headline", 1, {'word_count': 2}, 0),
            ContentSection("Critical vulnerability discovered.", "paragraph", 0, {'word_count': 3}, 1),
            ContentSection("• Patch immediately\n• Monitor systems", "list", 0, {'word_count': 4}, 2),
        ]
        
        preserved = PreservedContent(
            title="Security Report",
            sections=sections,
            raw_text="Security Alert\n\nCritical vulnerability discovered.\n\n• Patch immediately\n• Monitor systems",
            structured_data={
                'document_title': 'Security Report',
                'outline': [{'level': 1, 'text': 'Security Alert', 'position': 0}],
                'section_count': 3
            },
            preservation_quality='good'
        )
        
        dspy_content = create_dspy_optimized_content(preserved)
        
        # Test structure
        assert dspy_content['title'] == "Security Report"
        assert len(dspy_content['content_blocks']) == 3
        assert 'document_structure' in dspy_content
        assert 'raw_fallback' in dspy_content
        assert 'processing_instructions' in dspy_content
        
        # Test content blocks
        first_block = dspy_content['content_blocks'][0]
        assert first_block['content'] == "Security Alert"
        assert first_block['type'] == "headline"
        assert first_block['level'] == 1
        assert 'context' in first_block
        
        # Test processing instructions
        instructions = dspy_content['processing_instructions']
        assert instructions['maintain_context'] is True
        assert instructions['preserve_hierarchy'] is True
        assert instructions['consider_relationships'] is True
    
    def test_context_preservation(self):
        """Test that context is preserved for DSPy processing."""
        sections = [
            ContentSection("Introduction", "headline", 1, {}, 0),
            ContentSection("Background information.", "paragraph", 0, {}, 1),
            ContentSection("Main Analysis", "headline", 2, {}, 2),
            ContentSection("Detailed findings here.", "paragraph", 0, {}, 3),
            ContentSection("Conclusion", "headline", 1, {}, 4),
        ]
        
        preserved = PreservedContent(
            title="Test Report",
            sections=sections,
            raw_text="Test content",
            structured_data={},
            preservation_quality='good'
        )
        
        dspy_content = create_dspy_optimized_content(preserved)
        
        # Check that middle section has context
        main_analysis_block = dspy_content['content_blocks'][2]  # "Main Analysis"
        context = main_analysis_block['context']
        
        assert 'preceding_content' in context
        assert 'following_content' in context
        assert len(context['preceding_content']) > 0  # Should have "Background information."
        assert len(context['following_content']) > 0  # Should have "Detailed findings here."


class TestContentToolsIntegration:
    """Test integration with existing content tools."""
    
    @patch('common_tools.content_tools.requests.get')
    def test_article_extractor_with_preservation(self, mock_get):
        """Test that ArticleExtractorTool includes structure preservation."""
        from common_tools.content_tools import ArticleExtractorTool
        
        # Mock response
        mock_response = MagicMock()
        mock_response.text = """
        <html>
            <head><title>Security Analysis</title></head>
            <body>
                <article>
                    <h1>Threat Assessment</h1>
                    <p>This article analyzes current security threats.</p>
                    <h2>Findings</h2>
                    <ul>
                        <li>Critical vulnerability found</li>
                        <li>Immediate action required</li>
                    </ul>
                </article>
            </body>
        </html>
        """
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Test extraction
        extractor = ArticleExtractorTool()
        result = extractor.call("https://example.com/security")
        
        # Verify basic extraction still works
        assert 'text' in result
        assert 'extraction_method' in result
        assert 'extraction_confidence' in result
        
        # Verify structure preservation is added
        assert 'preserved_content' in result
        assert 'dspy_content' in result
        
        # Check preserved content structure
        preserved = result['preserved_content']
        if preserved:  # May be None if extraction failed
            assert 'title' in preserved
            assert 'sections' in preserved
            assert 'preservation_quality' in preserved
            assert len(preserved['sections']) > 0
        
        # Check DSPy optimization
        dspy_content = result['dspy_content']
        assert 'title' in dspy_content
        assert 'content_blocks' in dspy_content
        assert 'processing_instructions' in dspy_content
        
        # Verify processing instructions for DSPy
        instructions = dspy_content['processing_instructions']
        assert instructions['maintain_context'] is True
        assert instructions['preserve_hierarchy'] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])