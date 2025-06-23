"""
ContentFetcher - Content extraction and normalization for discovered CTI sources.

This module fetches content from discovered URLs and extracts structured data
using direct HTTP requests with DSPy-powered intelligent extraction.
"""

import asyncio
import aiohttp
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
import os
import json
from datetime import datetime
import re
import time
import random
from bs4 import BeautifulSoup, Comment
from common_tools.dspy_extraction_tool import DSPyExtractionTool
from common_tools.retry_utils import retry_with_policy

# Import DSPy for intelligent extraction
try:
    import dspy
    DSPY_AVAILABLE = True
except ImportError:
    print("Warning: DSPy not available, falling back to basic extraction", file=sys.stderr)
    DSPY_AVAILABLE = False


@dataclass
class ExtractedContent:
    """Represents extracted content from a source."""
    url: str
    title: str
    content: str
    metadata: Dict[str, Any]
    extraction_method: str
    timestamp: datetime
    success: bool = True
    error: Optional[str] = None
    # DSPy extraction results
    structured_data: Optional[Dict[str, Any]] = None
    threat_indicators: Optional[List[str]] = None
    threat_actors: Optional[List[str]] = None
    malware_families: Optional[List[str]] = None
    attack_techniques: Optional[List[str]] = None


class ContentFetcher:
    """
    Fetches and extracts content from CTI sources.
    
    Uses:
    - Direct HTTP requests with intelligent HTML parsing
    - Content cleaning and text extraction
    - DSPy extraction for structured CTI data extraction
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Request configuration
        self.timeout = aiohttp.ClientTimeout(total=30)
        self.max_content_length = 10 * 1024 * 1024  # 10MB limit
        
        # User agent for HTTP requests
        self.user_agent = "Umbrix-CTI-Crawler/1.0 (https://umbrix.io; threat-intel-crawler)"
        
        # Retry configuration
        self.max_retries = 3
        self.base_delay = 1.0
        self.max_delay = 30.0
        
        # Content extraction configuration
        self.max_text_length = 500000  # 500KB of text content limit
        
        # Initialize DSPy extraction if available
        self.dspy_extractor = None
        if DSPY_AVAILABLE:
            try:
                self.dspy_extractor = DSPyExtractionTool()
                self.logger.info("DSPy extraction tool initialized")
            except Exception as e:
                self.logger.warning(f"Failed to initialize DSPy extractor: {e}")
        
        self.logger.info("ContentFetcher initialized for intelligent HTTP crawling")
    
    def _is_supported_content_type(self, content_type: str) -> bool:
        """Check if content type is supported for text extraction."""
        content_type = content_type.lower()
        supported_types = [
            'text/html',
            'text/plain', 
            'application/xhtml+xml',
            'application/xml',
            'text/xml'
        ]
        return any(ct in content_type for ct in supported_types)
    
    def _clean_html_content(self, html: str) -> str:
        """
        Clean HTML content using BeautifulSoup for better text extraction.
        
        Args:
            html: Raw HTML content
            
        Returns:
            Cleaned text content
        """
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Remove script, style, and other non-content elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 
                               'aside', 'form', 'iframe', 'noscript']):
                element.decompose()
            
            # Remove HTML comments
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                comment.extract()
            
            # Remove common noise elements by class/id patterns
            noise_patterns = [
                'advertisement', 'ads', 'ad-', 'sidebar', 'menu', 'navigation',
                'breadcrumb', 'social', 'share', 'related', 'recommended',
                'cookie', 'popup', 'modal', 'banner'
            ]
            
            for pattern in noise_patterns:
                for element in soup.find_all(attrs={'class': re.compile(pattern, re.I)}):
                    element.decompose()
                for element in soup.find_all(attrs={'id': re.compile(pattern, re.I)}):
                    element.decompose()
            
            # Extract main content areas (prioritize article, main, content divs)
            main_content = None
            for selector in ['article', 'main', '[role="main"]', '.content', '.post', '.article']:
                main_content = soup.select_one(selector)
                if main_content:
                    break
            
            # If no main content found, use the whole body
            if main_content is None:
                main_content = soup.find('body') or soup
            
            # Extract text with proper spacing
            text = main_content.get_text(separator=' ', strip=True)
            
            # Clean up whitespace and normalize
            text = re.sub(r'\s+', ' ', text)
            text = text.strip()
            
            # Limit text length to prevent memory issues
            if len(text) > self.max_text_length:
                text = text[:self.max_text_length] + "... [truncated]"
            
            return text
            
        except Exception as e:
            self.logger.warning(f"BeautifulSoup parsing failed, falling back to regex: {e}")
            # Fallback to basic regex-based cleaning
            return self._clean_html_content_fallback(html)
    
    def _clean_html_content_fallback(self, html: str) -> str:
        """Fallback regex-based HTML cleaning."""
        # Remove script and style elements
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html)
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:self.max_text_length] if len(text) > self.max_text_length else text
    
    def _extract_metadata_from_html(self, html: str) -> Dict[str, Any]:
        """
        Extract metadata from HTML content using BeautifulSoup.
        
        Args:
            html: Raw HTML content
            
        Returns:
            Dictionary containing extracted metadata
        """
        metadata = {}
        
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract title
            title_elem = soup.find('title')
            if title_elem:
                metadata['title'] = title_elem.get_text().strip()
            
            # Extract meta tags
            meta_mappings = {
                'description': ['name', 'description'],
                'keywords': ['name', 'keywords'],
                'author': ['name', 'author'],
                'published_time': ['property', 'article:published_time'],
                'modified_time': ['property', 'article:modified_time'],
                'og_title': ['property', 'og:title'],
                'og_description': ['property', 'og:description'],
                'og_type': ['property', 'og:type'],
                'twitter_title': ['name', 'twitter:title'],
                'twitter_description': ['name', 'twitter:description'],
            }
            
            for key, (attr, value) in meta_mappings.items():
                meta_tag = soup.find('meta', attrs={attr: value})
                if meta_tag and meta_tag.get('content'):
                    metadata[key] = meta_tag['content']
            
            # Extract canonical URL
            canonical = soup.find('link', rel='canonical')
            if canonical and canonical.get('href'):
                metadata['canonical_url'] = canonical['href']
            
            # Extract language
            html_elem = soup.find('html')
            if html_elem and html_elem.get('lang'):
                metadata['language'] = html_elem['lang']
            
            # Extract headings for content structure
            headings = []
            for level in range(1, 7):
                for heading in soup.find_all(f'h{level}'):
                    headings.append({
                        'level': level,
                        'text': heading.get_text().strip()
                    })
            metadata['headings'] = headings[:10]  # Limit to first 10 headings
            
            # Extract important links (limit to avoid bloat)
            links = []
            for link in soup.find_all('a', href=True)[:50]:  # Limit to 50 links
                href = link['href']
                text = link.get_text().strip()
                if href and text and len(text) < 200:
                    links.append({'url': href, 'text': text})
            metadata['links'] = links
            
            # Extract images
            images = []
            for img in soup.find_all('img', src=True)[:10]:  # Limit to 10 images
                src = img['src']
                alt = img.get('alt', '')
                if src:
                    images.append({'src': src, 'alt': alt})
            metadata['images'] = images
            
        except Exception as e:
            self.logger.warning(f"BeautifulSoup metadata extraction failed: {e}")
            # Fallback to regex-based extraction
            metadata = self._extract_metadata_fallback(html)
        
        return metadata
    
    def _extract_metadata_fallback(self, html: str) -> Dict[str, Any]:
        """Fallback regex-based metadata extraction."""
        metadata = {}
        
        # Extract title
        title_match = re.search(r'<title[^>]*>([^<]+)</title>', html, re.IGNORECASE)
        if title_match:
            metadata['title'] = title_match.group(1).strip()
        
        # Extract basic meta tags
        meta_patterns = {
            'description': r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']*)["\']',
            'keywords': r'<meta[^>]*name=["\']keywords["\'][^>]*content=["\']([^"\']*)["\']',
        }
        
        for key, pattern in meta_patterns.items():
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                metadata[key] = match.group(1)
        
        return metadata
    
    async def _extract_with_dspy(self, content: str, url: str) -> Dict[str, Any]:
        """
        Extract structured CTI data using DSPy.
        
        Args:
            content: Cleaned text content
            url: Source URL for context
            
        Returns:
            Dictionary containing structured extraction results
        """
        if not self.dspy_extractor:
            return {}
        
        try:
            # Use DSPy to extract structured CTI data
            extraction_result = await self.dspy_extractor.extract_cti_data(
                content=content,
                source_url=url,
                extraction_type="threat_intelligence"
            )
            
            if extraction_result.get("success"):
                extracted_data = extraction_result.get("extracted_data", {})
                
                return {
                    "threat_indicators": extracted_data.get("indicators", []),
                    "threat_actors": extracted_data.get("threat_actors", []),
                    "malware_families": extracted_data.get("malware_families", []),
                    "attack_techniques": extracted_data.get("attack_techniques", []),
                    "campaign_references": extracted_data.get("campaigns", []),
                    "cve_references": extracted_data.get("cves", []),
                    "attribution": extracted_data.get("attribution", {}),
                    "key_findings": extracted_data.get("key_findings", []),
                    "confidence_score": extracted_data.get("confidence_score", 0.0),
                    "extraction_metadata": {
                        "model_used": extraction_result.get("model_used"),
                        "extraction_time": extraction_result.get("extraction_time"),
                        "token_count": extraction_result.get("token_count")
                    }
                }
            else:
                self.logger.warning(f"DSPy extraction failed for {url}: {extraction_result.get('error')}")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error in DSPy extraction for {url}: {e}")
            return {}
    
    async def fetch_with_direct_http(self, url: str) -> ExtractedContent:
        """
        Fetch content using direct HTTP requests with retry logic and intelligent extraction.
        
        Args:
            url: URL to fetch
            
        Returns:
            ExtractedContent (success flag indicates if fetch was successful)
        """
        try:
            # Standardized retry for external APIs
            decorated_fetch = retry_with_policy('external_apis')(self._fetch_url_content)
            return await decorated_fetch(url)
        except Exception as e:
            self.logger.error(f"Failed to fetch {url} after all retries: {e}")
            return ExtractedContent(
                url=url,
                title="",
                content="",
                metadata={},
                extraction_method="direct_http",
                timestamp=datetime.utcnow(),
                success=False,
                error=str(e)
            )
    
    async def _fetch_url_content(self, url: str) -> ExtractedContent:
        """Internal method to fetch content from a URL."""
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }
        
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(url, headers=headers, allow_redirects=True) as response:
                    if response.status == 200:
                        # Check content type
                        content_type = response.headers.get('content-type', '')
                        if not self._is_supported_content_type(content_type):
                            self.logger.warning(f"Unsupported content type for {url}: {content_type}")
                            return ExtractedContent(
                                url=url,
                                title="",
                                content="",
                                metadata={"content_type": content_type},
                                extraction_method="direct_http",
                                timestamp=datetime.utcnow(),
                                success=False,
                                error=f"Unsupported content type: {content_type}"
                            )
                        
                        # Check content length
                        content_length = response.headers.get('content-length')
                        if content_length and int(content_length) > self.max_content_length:
                            self.logger.warning(f"Content too large for {url}: {content_length} bytes")
                            return ExtractedContent(
                                url=url,
                                title="",
                                content="",
                                metadata={"content_length": content_length},
                                extraction_method="direct_http",
                                timestamp=datetime.utcnow(),
                                success=False,
                                error=f"Content too large: {content_length} bytes"
                            )
                        
                        # Get raw HTML content
                        raw_html = await response.text()
                        
                        # Extract metadata (includes title)
                        metadata = self._extract_metadata_from_html(raw_html)
                        title = metadata.get('title', '') or self._extract_title_from_html(raw_html)
                        metadata.update({
                            "content_length": len(raw_html),
                            "status_code": response.status,
                            "content_type": response.headers.get('content-type', ''),
                            "final_url": str(response.url)  # In case of redirects
                        })
                        
                        # Clean and extract text content
                        text_content = self._clean_html_content(raw_html)
                        
                        # Extract structured data using DSPy
                        dspy_extraction = await self._extract_with_dspy(text_content, url)
                        
                        return ExtractedContent(
                            url=url,
                            title=title,
                            content=text_content,
                            metadata=metadata,
                            extraction_method="direct_http_with_dspy" if dspy_extraction else "direct_http",
                            timestamp=datetime.utcnow(),
                            success=True,
                            structured_data=dspy_extraction,
                            threat_indicators=dspy_extraction.get("threat_indicators", []),
                            threat_actors=dspy_extraction.get("threat_actors", []),
                            malware_families=dspy_extraction.get("malware_families", []),
                            attack_techniques=dspy_extraction.get("attack_techniques", [])
                        )
                    else:
                        self.logger.error(f"HTTP error for {url}: {response.status}")
                        return ExtractedContent(
                            url=url,
                            title="",
                            content="",
                            metadata={"status_code": response.status},
                            extraction_method="direct_http",
                            timestamp=datetime.utcnow(),
                            success=False,
                            error=f"HTTP {response.status}"
                        )
                        
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout fetching {url}")
            return ExtractedContent(
                url=url,
                title="",
                content="",
                metadata={},
                extraction_method="direct_http",
                timestamp=datetime.utcnow(),
                success=False,
                error="Request timeout"
            )
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return ExtractedContent(
                url=url,
                title="",
                content="",
                metadata={},
                extraction_method="direct_http",
                timestamp=datetime.utcnow(),
                success=False,
                error=str(e)
            )
    
    def _extract_title_from_html(self, html: str) -> str:
        """Extract title from HTML content (basic implementation)."""
        # In production, use BeautifulSoup or similar
        import re
        title_match = re.search(r'<title[^>]*>([^<]+)</title>', html, re.IGNORECASE)
        return title_match.group(1).strip() if title_match else ""
    
    def _extract_text_from_html(self, html: str) -> str:
        """Extract text content from HTML (basic implementation)."""
        # In production, use BeautifulSoup or similar for proper HTML parsing
        import re
        # Remove script and style elements
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', html)
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    async def fetch_content(self, url: str) -> ExtractedContent:
        """
        Fetch content using direct HTTP with intelligent DSPy extraction.
        
        Args:
            url: URL to fetch
            
        Returns:
            ExtractedContent (may indicate failure)
        """
        self.logger.info(f"Fetching content from: {url}")
        
        # Use direct HTTP with DSPy extraction
        result = await self.fetch_with_direct_http(url)
        
        if result.success:
            self.logger.info(f"Successfully fetched {url} with DSPy extraction: "
                           f"{len(result.threat_indicators)} indicators, "
                           f"{len(result.threat_actors)} actors, "
                           f"{len(result.malware_families)} malware families")
        else:
            self.logger.warning(f"Failed to fetch {url}: {result.error}")
        
        return result
    
    async def fetch_multiple_urls(self, urls: List[str], max_concurrent: int = 5) -> List[ExtractedContent]:
        """
        Fetch content from multiple URLs concurrently.
        
        Args:
            urls: List of URLs to fetch
            max_concurrent: Maximum concurrent requests
            
        Returns:
            List of ExtractedContent results
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(url: str) -> ExtractedContent:
            async with semaphore:
                return await self.fetch_content(url)
        
        tasks = [fetch_with_semaphore(url) for url in urls]
        
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle any exceptions
            extracted_results = []
            for i, result in enumerate(results):
                if isinstance(result, ExtractedContent):
                    extracted_results.append(result)
                elif isinstance(result, Exception):
                    self.logger.error(f"Exception fetching {urls[i]}: {result}")
                    extracted_results.append(ExtractedContent(
                        url=urls[i],
                        title="",
                        content="",
                        metadata={},
                        extraction_method="error",
                        timestamp=datetime.utcnow(),
                        success=False,
                        error=str(result)
                    ))
            
            successful = sum(1 for r in extracted_results if r.success)
            self.logger.info(f"Fetched {len(urls)} URLs: {successful} successful, {len(urls) - successful} failed")
            
            return extracted_results
            
        except Exception as e:
            self.logger.error(f"Error in batch fetch: {e}")
            return []