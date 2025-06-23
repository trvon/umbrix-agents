"""
Shared utilities for feed and source discovery across discovery agents.
Consolidates RSS/Atom/TAXII discovery logic to eliminate code duplication.
"""

import asyncio
import aiohttp
import feedparser
import re
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional, Set
import json
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from dataclasses import dataclass
import time
import sys


@dataclass
class DiscoveredFeed:
    """Represents a discovered feed with metadata."""
    url: str
    feed_type: str  # 'rss', 'atom', 'taxii', 'json'
    title: Optional[str] = None
    description: Optional[str] = None
    language: Optional[str] = None
    last_updated: Optional[str] = None
    confidence_score: float = 0.7
    discovery_method: Optional[str] = None
    source_url: Optional[str] = None


class FeedDiscoveryTools:
    """Consolidated feed discovery utilities for RSS, Atom, and TAXII feeds."""
    
    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self.session = session
        self._should_close_session = session is None
        self.common_feed_paths = [
            '/rss', '/rss.xml', '/feed', '/feed.xml', '/feeds', '/feeds.xml',
            '/atom', '/atom.xml', '/blog/feed', '/blog/rss', '/news/feed',
            '/news/rss', '/index.xml', '/rss2.xml', '/feed.rss', '/articles/feed'
        ]
        self.taxii_endpoints = [
            '/taxii2/', '/taxii/', '/stix/taxii2/', '/api/taxii2/', '/feeds/taxii2/'
        ]
        self.feed_patterns = [
            re.compile(r'<link[^>]*type="application/rss\+xml"[^>]*href="([^"]*)"', re.IGNORECASE),
            re.compile(r'<link[^>]*type="application/atom\+xml"[^>]*href="([^"]*)"', re.IGNORECASE),
            re.compile(r'<link[^>]*type="application/json"[^>]*href="([^"]*)"', re.IGNORECASE),
            re.compile(r'href="([^"]*\.xml)"', re.IGNORECASE),
            re.compile(r'href="([^"]*feed[^"]*)"', re.IGNORECASE),
            re.compile(r'href="([^"]*rss[^"]*)"', re.IGNORECASE),
        ]

    async def __aenter__(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={'User-Agent': 'Umbrix-CTI-Feed-Discovery/1.0'}
            )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._should_close_session and self.session:
            await self.session.close()

    async def discover_from_url(self, base_url: str) -> List[DiscoveredFeed]:
        """
        Discover feeds from a base URL using multiple discovery methods.
        
        Args:
            base_url: The base URL to search for feeds
            
        Returns:
            List of discovered feeds
        """
        discovered = []
        
        try:
            # Method 1: Check common feed paths
            feeds_from_paths = await self._discover_from_common_paths(base_url)
            discovered.extend(feeds_from_paths)
            
            # Method 2: Parse HTML for feed links
            feeds_from_html = await self._discover_from_html_links(base_url)
            discovered.extend(feeds_from_html)
            
            # Method 3: Check for TAXII endpoints
            taxii_feeds = await self._discover_taxii_endpoints(base_url)
            discovered.extend(taxii_feeds)
            
            # Method 4: Look for JSON feeds
            json_feeds = await self._discover_json_feeds(base_url)
            discovered.extend(json_feeds)
            
        except Exception as e:
            print(f"[DiscoveryTools] Error discovering feeds from {base_url}: {e}", file=sys.stderr)
            
        # Deduplicate by URL
        seen_urls = set()
        unique_feeds = []
        for feed in discovered:
            if feed.url not in seen_urls:
                seen_urls.add(feed.url)
                unique_feeds.append(feed)
                
        return unique_feeds

    async def _discover_from_common_paths(self, base_url: str) -> List[DiscoveredFeed]:
        """Check common feed paths for valid feeds."""
        discovered = []
        
        for path in self.common_feed_paths:
            feed_url = urljoin(base_url, path)
            
            try:
                async with self.session.get(feed_url) as response:
                    if response.status == 200:
                        content = await response.text()
                        feed_type = await self._determine_feed_type(content, feed_url)
                        
                        if feed_type:
                            feed_info = await self._extract_feed_metadata(content, feed_type)
                            discovered.append(DiscoveredFeed(
                                url=feed_url,
                                feed_type=feed_type,
                                title=feed_info.get('title'),
                                description=feed_info.get('description'),
                                language=feed_info.get('language'),
                                last_updated=feed_info.get('last_updated'),
                                discovery_method='common_paths',
                                source_url=base_url,
                                confidence_score=0.9
                            ))
            except Exception:
                continue  # Skip failed URLs
                
        return discovered

    async def _discover_from_html_links(self, base_url: str) -> List[DiscoveredFeed]:
        """Parse HTML page for feed link tags."""
        discovered = []
        
        try:
            async with self.session.get(base_url) as response:
                if response.status != 200:
                    return discovered
                    
                html_content = await response.text()
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Look for proper feed link tags
                feed_links = soup.find_all('link', attrs={
                    'type': ['application/rss+xml', 'application/atom+xml', 'application/json']
                })
                
                for link in feed_links:
                    href = link.get('href')
                    if href:
                        feed_url = urljoin(base_url, href)
                        feed_type = self._link_type_to_feed_type(link.get('type', ''))
                        title = link.get('title')
                        
                        # Verify the feed is actually valid
                        if await self._verify_feed(feed_url):
                            discovered.append(DiscoveredFeed(
                                url=feed_url,
                                feed_type=feed_type,
                                title=title,
                                discovery_method='html_links',
                                source_url=base_url,
                                confidence_score=0.95
                            ))
                            
                # Also check for feeds mentioned in href attributes
                for pattern in self.feed_patterns:
                    matches = pattern.findall(html_content)
                    for match in matches:
                        feed_url = urljoin(base_url, match)
                        if await self._verify_feed(feed_url):
                            content = await self._fetch_content(feed_url)
                            if content:
                                feed_type = await self._determine_feed_type(content, feed_url)
                                if feed_type:
                                    feed_info = await self._extract_feed_metadata(content, feed_type)
                                    discovered.append(DiscoveredFeed(
                                        url=feed_url,
                                        feed_type=feed_type,
                                        title=feed_info.get('title'),
                                        description=feed_info.get('description'),
                                        discovery_method='html_patterns',
                                        source_url=base_url,
                                        confidence_score=0.8
                                    ))
                                    
        except Exception as e:
            print(f"[DiscoveryTools] Error parsing HTML from {base_url}: {e}", file=sys.stderr)
            
        return discovered

    async def _discover_taxii_endpoints(self, base_url: str) -> List[DiscoveredFeed]:
        """Check for TAXII 2.0/2.1 endpoints."""
        discovered = []
        
        for endpoint in self.taxii_endpoints:
            taxii_url = urljoin(base_url, endpoint)
            
            try:
                # Check for TAXII discovery endpoint
                discovery_url = urljoin(taxii_url, 'discovery/')
                async with self.session.get(discovery_url, 
                                          headers={'Accept': 'application/taxii+json;version=2.1'}) as response:
                    if response.status == 200:
                        content = await response.text()
                        try:
                            taxii_data = json.loads(content)
                            if 'api_roots' in taxii_data:
                                discovered.append(DiscoveredFeed(
                                    url=discovery_url,
                                    feed_type='taxii',
                                    title=taxii_data.get('title', 'TAXII 2.1 Server'),
                                    description=taxii_data.get('description'),
                                    discovery_method='taxii_discovery',
                                    source_url=base_url,
                                    confidence_score=1.0
                                ))
                        except json.JSONDecodeError:
                            continue
                            
            except Exception:
                continue
                
        return discovered

    async def _discover_json_feeds(self, base_url: str) -> List[DiscoveredFeed]:
        """Check for JSON feeds (JSON Feed format)."""
        discovered = []
        json_paths = ['/feed.json', '/feeds.json', '/json', '/feed/json']
        
        for path in json_paths:
            json_url = urljoin(base_url, path)
            
            try:
                async with self.session.get(json_url) as response:
                    if response.status == 200:
                        content = await response.text()
                        try:
                            json_data = json.loads(content)
                            if json_data.get('version') and 'items' in json_data:
                                discovered.append(DiscoveredFeed(
                                    url=json_url,
                                    feed_type='json',
                                    title=json_data.get('title'),
                                    description=json_data.get('description'),
                                    discovery_method='json_feed',
                                    source_url=base_url,
                                    confidence_score=0.9
                                ))
                        except json.JSONDecodeError:
                            continue
                            
            except Exception:
                continue
                
        return discovered

    async def _determine_feed_type(self, content: str, url: str) -> Optional[str]:
        """Determine the type of feed from content."""
        content_lower = content.lower().strip()
        
        # Check for XML-based feeds
        if content_lower.startswith('<?xml') or content_lower.startswith('<rss') or content_lower.startswith('<feed'):
            if '<rss' in content_lower:
                return 'rss'
            elif '<feed' in content_lower and 'xmlns="http://www.w3.org/2005/atom"' in content_lower:
                return 'atom'
                
        # Check for JSON feeds
        try:
            json_data = json.loads(content)
            if json_data.get('version') and 'items' in json_data:
                return 'json'
            elif 'api_roots' in json_data:  # TAXII discovery
                return 'taxii'
        except json.JSONDecodeError:
            pass
            
        # Check URL patterns as fallback
        url_lower = url.lower()
        if 'rss' in url_lower:
            return 'rss'
        elif 'atom' in url_lower:
            return 'atom'
        elif 'json' in url_lower:
            return 'json'
        elif 'taxii' in url_lower:
            return 'taxii'
            
        return None

    async def _extract_feed_metadata(self, content: str, feed_type: str) -> Dict:
        """Extract metadata from feed content."""
        metadata = {}
        
        if feed_type in ['rss', 'atom']:
            # Use feedparser for RSS/Atom
            parsed = feedparser.parse(content)
            if parsed.feed:
                metadata['title'] = parsed.feed.get('title', '')
                metadata['description'] = parsed.feed.get('description', '')
                metadata['language'] = parsed.feed.get('language', '')
                metadata['last_updated'] = parsed.feed.get('updated', '')
                
        elif feed_type == 'json':
            try:
                json_data = json.loads(content)
                metadata['title'] = json_data.get('title', '')
                metadata['description'] = json_data.get('description', '')
                metadata['language'] = json_data.get('language', '')
            except json.JSONDecodeError:
                pass
                
        return metadata

    async def _verify_feed(self, feed_url: str) -> bool:
        """Verify that a URL actually contains a valid feed."""
        try:
            async with self.session.get(feed_url) as response:
                if response.status == 200:
                    content = await response.text()
                    feed_type = await self._determine_feed_type(content, feed_url)
                    return feed_type is not None
        except Exception:
            pass
        return False

    async def _fetch_content(self, url: str) -> Optional[str]:
        """Fetch content from URL."""
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.text()
        except Exception:
            pass
        return None

    def _link_type_to_feed_type(self, link_type: str) -> str:
        """Convert HTML link type to feed type."""
        type_mapping = {
            'application/rss+xml': 'rss',
            'application/atom+xml': 'atom',
            'application/json': 'json'
        }
        return type_mapping.get(link_type, 'unknown')


class FeedValidationTools:
    """Tools for validating and scoring discovered feeds."""
    
    @staticmethod
    def calculate_feed_quality_score(feed: DiscoveredFeed, content: str = None) -> float:
        """Calculate a quality score for a discovered feed."""
        score = feed.confidence_score
        
        # Boost score for feeds with good metadata
        if feed.title:
            score += 0.1
        if feed.description:
            score += 0.1
        if feed.language:
            score += 0.05
            
        # Discovery method weighting
        method_weights = {
            'html_links': 0.2,
            'common_paths': 0.15,
            'taxii_discovery': 0.3,
            'json_feed': 0.1,
            'html_patterns': 0.05
        }
        score += method_weights.get(feed.discovery_method, 0)
        
        # Content analysis if available
        if content:
            # Check for recent updates
            if 'pubDate' in content or 'updated' in content:
                score += 0.1
            # Check for multiple items
            item_count = content.count('<item>') + content.count('<entry>')
            if item_count > 10:
                score += 0.1
            elif item_count > 5:
                score += 0.05
                
        return min(score, 1.0)  # Cap at 1.0

    @staticmethod
    def is_cti_relevant(feed: DiscoveredFeed, content: str = None) -> bool:
        """Determine if a feed is relevant for CTI purposes."""
        cti_keywords = [
            'security', 'threat', 'vulnerability', 'malware', 'cyber', 'attack',
            'breach', 'incident', 'advisory', 'alert', 'intelligence', 'ioc',
            'indicator', 'stix', 'taxii', 'apt', 'campaign', 'phishing'
        ]
        
        # Check title and description
        text_to_check = f"{feed.title or ''} {feed.description or ''}".lower()
        if any(keyword in text_to_check for keyword in cti_keywords):
            return True
            
        # Check content if available
        if content:
            content_lower = content.lower()
            keyword_count = sum(1 for keyword in cti_keywords if keyword in content_lower)
            return keyword_count >= 2
            
        return False


async def discover_feeds_from_urls(urls: List[str]) -> List[DiscoveredFeed]:
    """
    Convenience function to discover feeds from multiple URLs.
    
    Args:
        urls: List of URLs to check for feeds
        
    Returns:
        List of all discovered feeds
    """
    all_feeds = []
    
    async with FeedDiscoveryTools() as discovery:
        tasks = [discovery.discover_from_url(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, list):
                all_feeds.extend(result)
            elif isinstance(result, Exception):
                print(f"[DiscoveryTools] Error in batch discovery: {result}", file=sys.stderr)
                
    return all_feeds


# Backward compatibility functions for existing tests
def find_feed_links(html_content: str, base_url: str) -> Set[str]:
    """
    Find feed links in HTML content (sync version for backward compatibility).
    
    Args:
        html_content: HTML content to parse
        base_url: Base URL for resolving relative links
        
    Returns:
        Set of discovered feed URLs
    """
    import re
    from urllib.parse import urljoin
    
    feed_urls = set()
    
    # Patterns to find feed links
    patterns = [
        re.compile(r'<link[^>]*type="application/rss\+xml"[^>]*href="([^"]*)"', re.IGNORECASE),
        re.compile(r'<link[^>]*type="application/atom\+xml"[^>]*href="([^"]*)"', re.IGNORECASE),
        re.compile(r'<link[^>]*href="([^"]*)"[^>]*type="application/rss\+xml"', re.IGNORECASE),
        re.compile(r'<link[^>]*href="([^"]*)"[^>]*type="application/atom\+xml"', re.IGNORECASE),
        re.compile(r'href="([^"]*\.xml)"', re.IGNORECASE),
        re.compile(r'href="([^"]*feed[^"]*)"', re.IGNORECASE),
        re.compile(r'href="([^"]*rss[^"]*)"', re.IGNORECASE),
    ]
    
    # Find all matches
    for pattern in patterns:
        matches = pattern.findall(html_content)
        for match in matches:
            full_url = urljoin(base_url, match)
            feed_urls.add(full_url)
    
    return feed_urls