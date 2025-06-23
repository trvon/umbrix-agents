"""
Shared utilities for data collection across collector agents.
Provides common patterns for HTTP requests, content fetching, and data parsing.
"""

import asyncio
import aiohttp
import feedparser
import json
import time
import sys
from typing import Dict, List, Optional, Any, AsyncGenerator
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
from dataclasses import dataclass
import hashlib
import re
from io import StringIO


@dataclass
class CollectionResult:
    """Represents the result of a collection operation."""
    success: bool
    data: Optional[Dict] = None
    error: Optional[str] = None
    source_url: Optional[str] = None
    collected_at: Optional[str] = None
    content_type: Optional[str] = None
    item_count: int = 0


@dataclass
class FeedItem:
    """Represents a single item from a feed."""
    id: str
    title: Optional[str]
    content: Optional[str]
    summary: Optional[str]
    link: Optional[str]
    published: Optional[str]
    updated: Optional[str]
    author: Optional[str]
    categories: List[str]
    source_url: str
    content_type: str = 'feed_item'


class HttpCollectionClient:
    """Shared HTTP client for collection operations with proper headers and retry logic."""
    
    def __init__(self, 
                 user_agent: str = "Umbrix-CTI-Collector/1.0",
                 timeout: int = 30,
                 max_retries: int = 3,
                 backoff_factor: float = 1.0):
        self.user_agent = user_agent
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={'User-Agent': self.user_agent}
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            
    async def fetch_with_retry(self, 
                              url: str, 
                              headers: Optional[Dict] = None,
                              method: str = 'GET',
                              **kwargs) -> CollectionResult:
        """Fetch content with exponential backoff retry logic."""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                async with self.session.request(method, url, headers=headers, **kwargs) as response:
                    content = await response.text()
                    
                    return CollectionResult(
                        success=True,
                        data={
                            'content': content,
                            'status_code': response.status,
                            'headers': dict(response.headers),
                            'url': str(response.url)
                        },
                        source_url=url,
                        collected_at=datetime.now(timezone.utc).isoformat(),
                        content_type=response.headers.get('content-type', 'unknown')
                    )
                    
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    delay = self.backoff_factor * (2 ** attempt)
                    print(f"[CollectionTools] Retry {attempt + 1} for {url} after {delay}s: {e}", file=sys.stderr)
                    await asyncio.sleep(delay)
                    
        return CollectionResult(
            success=False,
            error=f"Failed after {self.max_retries} attempts: {last_exception}",
            source_url=url,
            collected_at=datetime.now(timezone.utc).isoformat()
        )


class FeedCollectionTools:
    """Tools for collecting and parsing RSS/Atom feeds."""
    
    def __init__(self, http_client: Optional[HttpCollectionClient] = None):
        self.http_client = http_client
        
    async def collect_feed_items(self, feed_url: str) -> CollectionResult:
        """Collect and parse items from an RSS/Atom feed."""
        client = self.http_client or HttpCollectionClient()
        
        async with client:
            # Fetch feed content
            result = await client.fetch_with_retry(feed_url)
            
            if not result.success:
                return result
                
            content = result.data['content']
            
            try:
                # Parse feed using feedparser
                parsed = feedparser.parse(content)
                
                if parsed.bozo and parsed.bozo_exception:
                    print(f"[CollectionTools] Feed parsing warning for {feed_url}: {parsed.bozo_exception}", file=sys.stderr)
                    
                # Extract feed metadata
                feed_info = {
                    'title': parsed.feed.get('title', ''),
                    'description': parsed.feed.get('description', ''),
                    'link': parsed.feed.get('link', ''),
                    'language': parsed.feed.get('language', ''),
                    'updated': parsed.feed.get('updated', ''),
                    'generator': parsed.feed.get('generator', '')
                }
                
                # Extract items
                items = []
                for entry in parsed.entries:
                    item = self._parse_feed_entry(entry, feed_url)
                    if item:
                        items.append(item.__dict__)
                        
                return CollectionResult(
                    success=True,
                    data={
                        'feed_info': feed_info,
                        'items': items,
                        'raw_content': content
                    },
                    source_url=feed_url,
                    collected_at=datetime.now(timezone.utc).isoformat(),
                    content_type='rss/atom',
                    item_count=len(items)
                )
                
            except Exception as e:
                return CollectionResult(
                    success=False,
                    error=f"Feed parsing failed: {e}",
                    source_url=feed_url,
                    collected_at=datetime.now(timezone.utc).isoformat()
                )
                
    def _parse_feed_entry(self, entry: Any, source_url: str) -> Optional[FeedItem]:
        """Parse a single feed entry into a FeedItem."""
        try:
            # Generate unique ID
            entry_id = (entry.get('id') or 
                       entry.get('guid') or 
                       entry.get('link') or 
                       hashlib.md5(f"{entry.get('title', '')}{entry.get('published', '')}".encode()).hexdigest())
            
            # Extract content (prefer content over summary)
            content = None
            if hasattr(entry, 'content') and entry.content:
                content = entry.content[0].value if isinstance(entry.content, list) else entry.content
            elif hasattr(entry, 'description'):
                content = entry.description
                
            # Extract categories/tags
            categories = []
            if hasattr(entry, 'tags'):
                categories = [tag.term for tag in entry.tags if hasattr(tag, 'term')]
            elif hasattr(entry, 'category'):
                if isinstance(entry.category, list):
                    categories = entry.category
                else:
                    categories = [entry.category]
                    
            return FeedItem(
                id=entry_id,
                title=entry.get('title'),
                content=content,
                summary=entry.get('summary'),
                link=entry.get('link'),
                published=entry.get('published'),
                updated=entry.get('updated'),
                author=entry.get('author'),
                categories=categories,
                source_url=source_url
            )
            
        except Exception as e:
            print(f"[CollectionTools] Error parsing feed entry: {e}", file=sys.stderr)
            return None


class TaxiiCollectionTools:
    """Tools for collecting STIX objects from TAXII 2.x servers."""
    
    def __init__(self, http_client: Optional[HttpCollectionClient] = None):
        self.http_client = http_client
        
    async def discover_api_roots(self, discovery_url: str) -> CollectionResult:
        """Discover TAXII API roots from discovery endpoint."""
        client = self.http_client or HttpCollectionClient()
        
        headers = {
            'Accept': 'application/taxii+json;version=2.1',
            'Content-Type': 'application/taxii+json;version=2.1'
        }
        
        async with client:
            result = await client.fetch_with_retry(discovery_url, headers=headers)
            
            if not result.success:
                return result
                
            try:
                discovery_data = json.loads(result.data['content'])
                
                return CollectionResult(
                    success=True,
                    data=discovery_data,
                    source_url=discovery_url,
                    collected_at=datetime.now(timezone.utc).isoformat(),
                    content_type='taxii_discovery'
                )
                
            except json.JSONDecodeError as e:
                return CollectionResult(
                    success=False,
                    error=f"Invalid TAXII discovery response: {e}",
                    source_url=discovery_url,
                    collected_at=datetime.now(timezone.utc).isoformat()
                )
                
    async def get_collections(self, api_root_url: str) -> CollectionResult:
        """Get collections from a TAXII API root."""
        collections_url = urljoin(api_root_url, 'collections/')
        client = self.http_client or HttpCollectionClient()
        
        headers = {
            'Accept': 'application/taxii+json;version=2.1'
        }
        
        async with client:
            result = await client.fetch_with_retry(collections_url, headers=headers)
            
            if not result.success:
                return result
                
            try:
                collections_data = json.loads(result.data['content'])
                
                return CollectionResult(
                    success=True,
                    data=collections_data,
                    source_url=collections_url,
                    collected_at=datetime.now(timezone.utc).isoformat(),
                    content_type='taxii_collections'
                )
                
            except json.JSONDecodeError as e:
                return CollectionResult(
                    success=False,
                    error=f"Invalid TAXII collections response: {e}",
                    source_url=collections_url,
                    collected_at=datetime.now(timezone.utc).isoformat()
                )
                
    async def get_objects(self, 
                         api_root_url: str, 
                         collection_id: str,
                         added_after: Optional[str] = None,
                         limit: Optional[int] = None) -> CollectionResult:
        """Get STIX objects from a TAXII collection."""
        objects_url = urljoin(api_root_url, f'collections/{collection_id}/objects/')
        client = self.http_client or HttpCollectionClient()
        
        headers = {
            'Accept': 'application/stix+json;version=2.1'
        }
        
        params = {}
        if added_after:
            params['added_after'] = added_after
        if limit:
            params['limit'] = limit
            
        async with client:
            result = await client.fetch_with_retry(objects_url, headers=headers, params=params)
            
            if not result.success:
                return result
                
            try:
                objects_data = json.loads(result.data['content'])
                objects = objects_data.get('objects', [])
                
                return CollectionResult(
                    success=True,
                    data={
                        'objects': objects,
                        'more': objects_data.get('more', False),
                        'next': objects_data.get('next')
                    },
                    source_url=objects_url,
                    collected_at=datetime.now(timezone.utc).isoformat(),
                    content_type='stix_objects',
                    item_count=len(objects)
                )
                
            except json.JSONDecodeError as e:
                return CollectionResult(
                    success=False,
                    error=f"Invalid STIX objects response: {e}",
                    source_url=objects_url,
                    collected_at=datetime.now(timezone.utc).isoformat()
                )


class MispCollectionTools:
    """Tools for collecting IOCs from MISP feeds."""
    
    def __init__(self, http_client: Optional[HttpCollectionClient] = None):
        self.http_client = http_client
        
    async def collect_misp_feed(self, feed_url: str, format_type: str = 'json') -> CollectionResult:
        """Collect IOCs from a MISP feed."""
        client = self.http_client or HttpCollectionClient()
        
        async with client:
            result = await client.fetch_with_retry(feed_url)
            
            if not result.success:
                return result
                
            content = result.data['content']
            
            try:
                if format_type.lower() == 'json':
                    return self._parse_misp_json(content, feed_url)
                elif format_type.lower() == 'csv':
                    return self._parse_misp_csv(content, feed_url)
                else:
                    return CollectionResult(
                        success=False,
                        error=f"Unsupported MISP format: {format_type}",
                        source_url=feed_url,
                        collected_at=datetime.now(timezone.utc).isoformat()
                    )
                    
            except Exception as e:
                return CollectionResult(
                    success=False,
                    error=f"MISP feed parsing failed: {e}",
                    source_url=feed_url,
                    collected_at=datetime.now(timezone.utc).isoformat()
                )
                
    def _parse_misp_json(self, content: str, source_url: str) -> CollectionResult:
        """Parse MISP JSON format."""
        try:
            data = json.loads(content)
            
            # Handle different MISP JSON structures
            if isinstance(data, list):
                indicators = data
            elif isinstance(data, dict) and 'response' in data:
                indicators = data['response']
            elif isinstance(data, dict) and 'Attribute' in data:
                indicators = data['Attribute'] if isinstance(data['Attribute'], list) else [data['Attribute']]
            else:
                indicators = [data]
                
            # Normalize indicators
            normalized_indicators = []
            for indicator in indicators:
                if isinstance(indicator, dict):
                    normalized = {
                        'value': indicator.get('value', ''),
                        'type': indicator.get('type', ''),
                        'category': indicator.get('category', ''),
                        'comment': indicator.get('comment', ''),
                        'timestamp': indicator.get('timestamp', ''),
                        'to_ids': indicator.get('to_ids', False)
                    }
                    normalized_indicators.append(normalized)
                    
            return CollectionResult(
                success=True,
                data={
                    'indicators': normalized_indicators,
                    'raw_content': content
                },
                source_url=source_url,
                collected_at=datetime.now(timezone.utc).isoformat(),
                content_type='misp_json',
                item_count=len(normalized_indicators)
            )
            
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON in MISP feed: {e}")
            
    def _parse_misp_csv(self, content: str, source_url: str) -> CollectionResult:
        """Parse MISP CSV format."""
        try:
            lines = content.strip().split('\n')
            if not lines:
                return CollectionResult(
                    success=True,
                    data={'indicators': []},
                    source_url=source_url,
                    collected_at=datetime.now(timezone.utc).isoformat(),
                    content_type='misp_csv',
                    item_count=0
                )
                
            # Simple CSV parsing (assuming value,type format)
            indicators = []
            for line in lines:
                if line.strip() and not line.startswith('#'):
                    parts = line.split(',')
                    if len(parts) >= 2:
                        indicators.append({
                            'value': parts[0].strip(),
                            'type': parts[1].strip(),
                            'comment': parts[2].strip() if len(parts) > 2 else ''
                        })
                        
            return CollectionResult(
                success=True,
                data={
                    'indicators': indicators,
                    'raw_content': content
                },
                source_url=source_url,
                collected_at=datetime.now(timezone.utc).isoformat(),
                content_type='misp_csv',
                item_count=len(indicators)
            )
            
        except Exception as e:
            raise Exception(f"Error parsing MISP CSV: {e}")


class ShodanCollectionTools:
    """Tools for collecting data from Shodan streams."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://stream.shodan.io"
        
    async def stream_banners(self, 
                           filters: Optional[Dict] = None) -> AsyncGenerator[Dict, None]:
        """Stream real-time banner data from Shodan."""
        url = f"{self.base_url}/shodan/banners"
        params = {'key': self.api_key}
        
        if filters:
            for key, value in filters.items():
                params[key] = value
                
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    raise Exception(f"Shodan stream error: {response.status}")
                    
                async for line in response.content:
                    try:
                        data = json.loads(line.decode('utf-8'))
                        yield data
                    except json.JSONDecodeError:
                        continue
                        
    async def stream_alerts(self) -> AsyncGenerator[Dict, None]:
        """Stream Shodan network alerts."""
        url = f"{self.base_url}/shodan/alert"
        params = {'key': self.api_key}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    raise Exception(f"Shodan alerts stream error: {response.status}")
                    
                async for line in response.content:
                    try:
                        data = json.loads(line.decode('utf-8'))
                        yield data
                    except json.JSONDecodeError:
                        continue


def normalize_timestamp(timestamp_str: Optional[str]) -> Optional[str]:
    """Normalize various timestamp formats to ISO 8601."""
    if not timestamp_str:
        return None
        
    try:
        # Try parsing common formats
        formats = [
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%d %H:%M:%S',
            '%a, %d %b %Y %H:%M:%S %Z',
            '%Y-%m-%d'
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(timestamp_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except ValueError:
                continue
                
        # If all parsing fails, return original
        return timestamp_str
        
    except Exception:
        return timestamp_str


def extract_indicators_from_text(text: str) -> List[Dict]:
    """Extract common indicators (IPs, domains, URLs, hashes) from text."""
    indicators = []
    
    # IP addresses
    ip_pattern = r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
    for ip in re.findall(ip_pattern, text):
        indicators.append({'type': 'ipv4', 'value': ip})
        
    # Domain names
    domain_pattern = r'\b[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}\b'
    for domain in re.findall(domain_pattern, text):
        if isinstance(domain, tuple):
            domain = ''.join(domain)
        indicators.append({'type': 'domain', 'value': domain})
        
    # URLs
    url_pattern = r'https?://[^\s<>"\'`|(){}[\]\\]+'
    for url in re.findall(url_pattern, text):
        indicators.append({'type': 'url', 'value': url})
        
    # MD5 hashes
    md5_pattern = r'\b[a-fA-F0-9]{32}\b'
    for hash_val in re.findall(md5_pattern, text):
        indicators.append({'type': 'md5', 'value': hash_val.lower()})
        
    # SHA1 hashes
    sha1_pattern = r'\b[a-fA-F0-9]{40}\b'
    for hash_val in re.findall(sha1_pattern, text):
        indicators.append({'type': 'sha1', 'value': hash_val.lower()})
        
    # SHA256 hashes
    sha256_pattern = r'\b[a-fA-F0-9]{64}\b'
    for hash_val in re.findall(sha256_pattern, text):
        indicators.append({'type': 'sha256', 'value': hash_val.lower()})
        
    return indicators