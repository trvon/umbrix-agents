"""
Content Preservation for Google ADK + DSPy Integration

This module provides improved content extraction that preserves rich content structure
for better DSPy processing, addressing the "squashing" issue where content is flattened
before being passed to DSPy for intelligence processing.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from bs4 import BeautifulSoup, Tag
import json
import re
from urllib.parse import urljoin, urlparse


@dataclass
class ContentSection:
    """Represents a structured content section with preserved metadata."""
    content: str
    section_type: str  # 'headline', 'paragraph', 'list', 'quote', 'code', etc.
    level: int  # hierarchy level (h1=1, h2=2, etc.)
    metadata: Dict[str, Any]
    position: int  # order in document


@dataclass
class PreservedContent:
    """Container for content that maintains structure for DSPy processing."""
    title: str
    sections: List[ContentSection]
    raw_text: str  # flattened version for fallback
    structured_data: Dict[str, Any]  # additional metadata
    preservation_quality: str  # 'excellent', 'good', 'fair', 'poor'


class StructurePreservingExtractor:
    """
    Advanced content extractor that maintains document structure for DSPy intelligence.
    
    Instead of flattening content into plain text, this preserves:
    - Document hierarchy (headings, sections)
    - Content types (paragraphs, lists, quotes, code blocks)
    - Semantic relationships between content sections
    - Contextual metadata for each content piece
    """

    def __init__(self):
        self.section_selectors = {
            'headline': ['h1', 'h2', 'h3', 'h4', 'h5', 'h6'],
            'paragraph': ['p'],
            'list': ['ul', 'ol', 'li'],
            'quote': ['blockquote', 'q'],
            'code': ['code', 'pre'],
            'emphasis': ['strong', 'b', 'em', 'i'],
            'link': ['a'],
            'media': ['img', 'video', 'audio']
        }

    def extract_preserved_content(self, soup: BeautifulSoup, url: str) -> PreservedContent:
        """
        Extract content while preserving structure for DSPy processing.
        
        Args:
            soup: BeautifulSoup parsed HTML
            url: Source URL for context
            
        Returns:
            PreservedContent with structured data intact
        """
        sections = []
        position = 0
        
        # Extract title
        title = self._extract_title(soup)
        
        # Find main content container
        main_container = self._find_main_content(soup)
        if not main_container:
            main_container = soup
        
        # Extract structured sections
        for element in main_container.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 
                                               'blockquote', 'ul', 'ol', 'pre', 'code', 'article']):
            section = self._extract_section(element, position, url)
            if section and section.content.strip():
                sections.append(section)
                position += 1
        
        # Generate fallback raw text
        raw_text = self._generate_raw_text(sections)
        
        # Create structured data for DSPy
        structured_data = self._create_structured_data(sections, title, url)
        
        # Assess preservation quality
        quality = self._assess_preservation_quality(sections, soup)
        
        return PreservedContent(
            title=title,
            sections=sections,
            raw_text=raw_text,
            structured_data=structured_data,
            preservation_quality=quality
        )

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title with fallbacks."""
        # Try <title> tag first
        title_tag = soup.find('title')
        if title_tag and title_tag.string:
            return title_tag.string.strip()
        
        # Try first h1
        h1 = soup.find('h1')
        if h1:
            return h1.get_text().strip()
        
        # Try meta title
        meta_title = soup.find('meta', attrs={'property': 'og:title'})
        if meta_title and meta_title.get('content'):
            return meta_title['content'].strip()
        
        return "Untitled Document"

    def _find_main_content(self, soup: BeautifulSoup) -> Optional[Tag]:
        """Find the main content container using semantic HTML and heuristics."""
        # Try semantic HTML5 elements first
        for selector in ['main', 'article', '[role="main"]']:
            container = soup.select_one(selector)
            if container:
                return container
        
        # Try common content class names
        for class_name in ['content', 'main', 'article', 'post', 'entry']:
            container = soup.find(attrs={'class': re.compile(class_name, re.I)})
            if container:
                return container
        
        # Try to find container with most text content
        candidates = soup.find_all(['div', 'section'])
        if candidates:
            best_candidate = max(candidates, 
                               key=lambda x: len(x.get_text().strip()))
            if len(best_candidate.get_text().strip()) > 200:
                return best_candidate
        
        return None

    def _extract_section(self, element: Tag, position: int, url: str) -> Optional[ContentSection]:
        """Extract a content section with preserved structure."""
        if not element or not element.get_text().strip():
            return None
        
        # Determine section type
        tag_name = element.name.lower()
        section_type = self._determine_section_type(tag_name)
        
        # Determine hierarchy level
        level = self._determine_level(tag_name)
        
        # Extract content with preserved formatting
        content = self._extract_formatted_content(element)
        
        # Extract metadata
        metadata = self._extract_section_metadata(element, url)
        
        return ContentSection(
            content=content,
            section_type=section_type,
            level=level,
            metadata=metadata,
            position=position
        )

    def _determine_section_type(self, tag_name: str) -> str:
        """Determine the semantic type of a content section."""
        if tag_name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            return 'headline'
        elif tag_name == 'p':
            return 'paragraph'
        elif tag_name in ['ul', 'ol']:
            return 'list'
        elif tag_name == 'li':
            return 'list_item'
        elif tag_name in ['blockquote', 'q']:
            return 'quote'
        elif tag_name in ['code', 'pre']:
            return 'code'
        elif tag_name == 'article':
            return 'article_section'
        else:
            return 'content'

    def _determine_level(self, tag_name: str) -> int:
        """Determine hierarchy level for content organization."""
        level_map = {
            'h1': 1, 'h2': 2, 'h3': 3, 'h4': 4, 'h5': 5, 'h6': 6
        }
        return level_map.get(tag_name, 0)

    def _extract_formatted_content(self, element: Tag) -> str:
        """Extract content while preserving important formatting."""
        # Handle different element types
        if element.name in ['ul', 'ol']:
            # Preserve list structure
            items = []
            for li in element.find_all('li'):
                item_text = li.get_text().strip()
                if item_text:
                    items.append(f"• {item_text}")
            return '\n'.join(items)
        
        elif element.name == 'blockquote':
            # Preserve quote formatting
            quote_text = element.get_text().strip()
            return f'"{quote_text}"'
        
        elif element.name in ['code', 'pre']:
            # Preserve code formatting
            return f"```\n{element.get_text()}\n```"
        
        else:
            # Regular text with preserved line breaks
            return element.get_text().strip()

    def _extract_section_metadata(self, element: Tag, url: str) -> Dict[str, Any]:
        """Extract metadata for a content section."""
        metadata = {
            'tag': element.name,
            'classes': element.get('class', []),
            'id': element.get('id'),
            'text_length': len(element.get_text().strip()),
            'word_count': len(element.get_text().split())
        }
        
        # Extract links
        links = []
        for link in element.find_all('a', href=True):
            link_url = urljoin(url, link['href'])
            links.append({
                'text': link.get_text().strip(),
                'url': link_url,
                'is_external': urlparse(link_url).netloc != urlparse(url).netloc
            })
        metadata['links'] = links
        
        # Extract emphasized text
        emphasized = []
        for em in element.find_all(['strong', 'b', 'em', 'i']):
            emphasized.append(em.get_text().strip())
        metadata['emphasized_text'] = emphasized
        
        return metadata

    def _generate_raw_text(self, sections: List[ContentSection]) -> str:
        """Generate fallback raw text from structured sections."""
        text_parts = []
        
        for section in sections:
            if section.section_type == 'headline':
                # Add extra spacing for headlines
                text_parts.append(f"\n{section.content}\n")
            else:
                text_parts.append(section.content)
        
        return '\n\n'.join(text_parts).strip()

    def _create_structured_data(self, sections: List[ContentSection], 
                              title: str, url: str) -> Dict[str, Any]:
        """Create structured data optimized for DSPy processing."""
        # Organize content by type
        content_by_type = {}
        for section in sections:
            if section.section_type not in content_by_type:
                content_by_type[section.section_type] = []
            content_by_type[section.section_type].append(section)
        
        # Create document outline
        outline = []
        for section in sections:
            if section.section_type == 'headline':
                outline.append({
                    'level': section.level,
                    'text': section.content,
                    'position': section.position
                })
        
        # Extract key information for DSPy
        structured = {
            'document_title': title,
            'source_url': url,
            'content_types': list(content_by_type.keys()),
            'outline': outline,
            'section_count': len(sections),
            'total_words': sum(section.metadata.get('word_count', 0) for section in sections),
            'content_hierarchy': self._build_content_hierarchy(sections),
            'key_topics': self._extract_key_topics(sections),
            'content_density': self._calculate_content_density(sections)
        }
        
        return structured

    def _build_content_hierarchy(self, sections: List[ContentSection]) -> List[Dict[str, Any]]:
        """Build hierarchical structure of content sections."""
        hierarchy = []
        current_section = None
        
        for section in sections:
            if section.section_type == 'headline':
                # Start new section
                current_section = {
                    'headline': section.content,
                    'level': section.level,
                    'content': [],
                    'metadata': section.metadata
                }
                hierarchy.append(current_section)
            elif current_section:
                # Add content to current section
                current_section['content'].append({
                    'type': section.section_type,
                    'text': section.content,
                    'metadata': section.metadata
                })
        
        return hierarchy

    def _extract_key_topics(self, sections: List[ContentSection]) -> List[str]:
        """Extract key topics from emphasized text and headlines."""
        topics = set()
        
        for section in sections:
            if section.section_type == 'headline':
                # Headlines are key topics
                topics.add(section.content.lower())
            
            # Extract emphasized text as potential topics
            for emphasized in section.metadata.get('emphasized_text', []):
                if len(emphasized.split()) <= 3:  # Short phrases only
                    topics.add(emphasized.lower())
        
        return list(topics)[:10]  # Limit to top 10

    def _calculate_content_density(self, sections: List[ContentSection]) -> Dict[str, float]:
        """Calculate content density metrics for quality assessment."""
        if not sections:
            return {'overall': 0.0}
        
        total_sections = len(sections)
        section_types = {}
        
        for section in sections:
            section_type = section.section_type
            if section_type not in section_types:
                section_types[section_type] = 0
            section_types[section_type] += 1
        
        # Calculate density ratios
        density = {}
        for section_type, count in section_types.items():
            density[f'{section_type}_ratio'] = count / total_sections
        
        # Overall content quality score
        density['overall'] = min(1.0, sum([
            section_types.get('headline', 0) * 0.3,
            section_types.get('paragraph', 0) * 0.4,
            section_types.get('list', 0) * 0.2,
            section_types.get('quote', 0) * 0.1
        ]) / max(1, total_sections))
        
        return density

    def _assess_preservation_quality(self, sections: List[ContentSection], 
                                   soup: BeautifulSoup) -> str:
        """Assess how well content structure was preserved."""
        if not sections:
            return 'poor'
        
        # Count different content types
        content_types = set(section.section_type for section in sections)
        
        # Assess quality based on structure diversity and completeness
        score = 0
        
        # Bonus for having headlines (structure)
        if 'headline' in content_types:
            score += 30
        
        # Bonus for having multiple content types
        score += len(content_types) * 10
        
        # Bonus for adequate content volume
        total_words = sum(section.metadata.get('word_count', 0) for section in sections)
        if total_words > 100:
            score += 20
        if total_words > 500:
            score += 20
        
        # Bonus for structured elements (lists, quotes)
        if any(t in content_types for t in ['list', 'quote', 'code']):
            score += 20
        
        # Determine quality level
        if score >= 80:
            return 'excellent'
        elif score >= 60:
            return 'good'
        elif score >= 40:
            return 'fair'
        else:
            return 'poor'


def create_dspy_optimized_content(preserved_content: PreservedContent) -> Dict[str, Any]:
    """
    Create content structure optimized for DSPy processing.
    
    This function takes the preserved content structure and formats it
    specifically for DSPy intelligence processing, maintaining context
    and relationships that would be lost in traditional flattening.
    """
    
    # Create contextual content blocks for DSPy
    content_blocks = []
    
    for section in preserved_content.sections:
        block = {
            'content': section.content,
            'type': section.section_type,
            'level': section.level,
            'position': section.position,
            'context': {
                'preceding_content': _get_preceding_context(preserved_content.sections, section.position),
                'following_content': _get_following_context(preserved_content.sections, section.position),
                'section_metadata': section.metadata
            }
        }
        content_blocks.append(block)
    
    # Create DSPy-optimized structure
    dspy_content = {
        'title': preserved_content.title,
        'content_blocks': content_blocks,
        'document_structure': preserved_content.structured_data,
        'raw_fallback': preserved_content.raw_text,
        'preservation_quality': preserved_content.preservation_quality,
        'processing_instructions': {
            'maintain_context': True,
            'preserve_hierarchy': True,
            'consider_relationships': True
        }
    }
    
    return dspy_content


def _get_preceding_context(sections: List[ContentSection], position: int, context_size: int = 2) -> List[str]:
    """Get preceding content sections for context."""
    start = max(0, position - context_size)
    return [s.content for s in sections[start:position]]


def _get_following_context(sections: List[ContentSection], position: int, context_size: int = 2) -> List[str]:
    """Get following content sections for context."""
    end = min(len(sections), position + context_size + 1)
    return [s.content for s in sections[position + 1:end]]