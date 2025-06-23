"""
Shared utilities for data processing and enrichment across processing agents.
Provides common patterns for content processing, validation, and enrichment.
"""

import asyncio
import json
import re
import sys
import time
import hashlib
from typing import Dict, List, Optional, Any, Union, Set
from datetime import datetime, timezone
from dataclasses import dataclass
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
import ipaddress
from enum import Enum


class ContentType(Enum):
    """Enumeration of supported content types."""
    RSS_FEED = "rss_feed"
    ATOM_FEED = "atom_feed"
    JSON_FEED = "json_feed"
    MISP_FEED = "misp_feed"
    TAXII_CONTENT = "taxii_content"
    STIX_BUNDLE = "stix_bundle"
    PLAIN_TEXT = "plain_text"
    HTML_CONTENT = "html_content"
    UNKNOWN = "unknown"


@dataclass
class ProcessingResult:
    """Result of a processing operation."""
    success: bool
    processed_data: Optional[Dict] = None
    error: Optional[str] = None
    processing_time: Optional[float] = None
    input_size: Optional[int] = None
    output_size: Optional[int] = None
    items_processed: int = 0


@dataclass
class ExtractedEntity:
    """Represents an extracted entity from content."""
    entity_type: str
    value: str
    confidence: float
    context: Optional[str] = None
    source_location: Optional[str] = None
    metadata: Optional[Dict] = None


@dataclass
class ExtractedRelation:
    """Represents a relationship between entities."""
    source_entity: str
    target_entity: str
    relation_type: str
    confidence: float
    context: Optional[str] = None
    metadata: Optional[Dict] = None


class ContentNormalizer:
    """Tools for normalizing various content formats into standard structure."""
    
    @staticmethod
    def normalize_content(content: Dict, content_type: ContentType) -> ProcessingResult:
        """Normalize content based on its type."""
        start_time = time.time()
        
        try:
            if content_type == ContentType.RSS_FEED:
                result = ContentNormalizer._normalize_rss_content(content)
            elif content_type == ContentType.ATOM_FEED:
                result = ContentNormalizer._normalize_atom_content(content)
            elif content_type == ContentType.MISP_FEED:
                result = ContentNormalizer._normalize_misp_content(content)
            elif content_type == ContentType.STIX_BUNDLE:
                result = ContentNormalizer._normalize_stix_content(content)
            else:
                result = ContentNormalizer._normalize_generic_content(content)
                
            processing_time = time.time() - start_time
            result.processing_time = processing_time
            
            return result
            
        except Exception as e:
            return ProcessingResult(
                success=False,
                error=f"Content normalization failed: {e}",
                processing_time=time.time() - start_time
            )
    
    @staticmethod
    def _normalize_rss_content(content: Dict) -> ProcessingResult:
        """Normalize RSS feed content."""
        normalized_items = []
        
        items = content.get('items', [])
        for item in items:
            normalized_item = {
                'id': item.get('id') or item.get('guid') or item.get('link'),
                'title': ContentNormalizer._clean_text(item.get('title', '')),
                'content': ContentNormalizer._clean_text(item.get('content', '')),
                'summary': ContentNormalizer._clean_text(item.get('summary', '')),
                'link': item.get('link'),
                'published': ContentNormalizer._normalize_timestamp(item.get('published')),
                'updated': ContentNormalizer._normalize_timestamp(item.get('updated')),
                'author': item.get('author'),
                'categories': item.get('categories', []),
                'source_type': 'rss_item'
            }
            normalized_items.append(normalized_item)
            
        return ProcessingResult(
            success=True,
            processed_data={
                'feed_info': content.get('feed_info', {}),
                'items': normalized_items,
                'content_type': 'rss_feed'
            },
            items_processed=len(normalized_items)
        )
    
    @staticmethod
    def _normalize_misp_content(content: Dict) -> ProcessingResult:
        """Normalize MISP feed content."""
        indicators = content.get('indicators', [])
        normalized_indicators = []
        
        for indicator in indicators:
            normalized = {
                'value': indicator.get('value', '').strip(),
                'type': IndicatorTools.normalize_indicator_type(indicator.get('type', '')),
                'category': indicator.get('category', ''),
                'comment': indicator.get('comment', ''),
                'timestamp': ContentNormalizer._normalize_timestamp(indicator.get('timestamp')),
                'to_ids': indicator.get('to_ids', False),
                'confidence': IndicatorTools.calculate_indicator_confidence(indicator),
                'source_type': 'misp_indicator'
            }
            
            # Validate indicator
            if IndicatorTools.is_valid_indicator(normalized['value'], normalized['type']):
                normalized_indicators.append(normalized)
                
        return ProcessingResult(
            success=True,
            processed_data={
                'indicators': normalized_indicators,
                'content_type': 'misp_feed'
            },
            items_processed=len(normalized_indicators)
        )
    
    @staticmethod
    def _normalize_stix_content(content: Dict) -> ProcessingResult:
        """Normalize STIX bundle content."""
        objects = content.get('objects', [])
        normalized_objects = []
        
        for obj in objects:
            normalized = {
                'id': obj.get('id'),
                'type': obj.get('type'),
                'spec_version': obj.get('spec_version'),
                'created': ContentNormalizer._normalize_timestamp(obj.get('created')),
                'modified': ContentNormalizer._normalize_timestamp(obj.get('modified')),
                'labels': obj.get('labels', []),
                'confidence': obj.get('confidence', 50),
                'raw_object': obj,
                'source_type': 'stix_object'
            }
            normalized_objects.append(normalized)
            
        return ProcessingResult(
            success=True,
            processed_data={
                'objects': normalized_objects,
                'content_type': 'stix_bundle'
            },
            items_processed=len(normalized_objects)
        )
    
    @staticmethod
    def _normalize_atom_content(content: Dict) -> ProcessingResult:
        """Normalize Atom feed content."""
        normalized_items = []
        
        items = content.get('items', [])
        for item in items:
            normalized_item = {
                'id': item.get('id') or item.get('guid') or item.get('link'),
                'title': ContentNormalizer._clean_text(item.get('title', '')),
                'content': ContentNormalizer._clean_text(item.get('content', '')),
                'summary': ContentNormalizer._clean_text(item.get('summary', '')),
                'link': item.get('link'),
                'published': ContentNormalizer._normalize_timestamp(item.get('published')),
                'updated': ContentNormalizer._normalize_timestamp(item.get('updated')),
                'author': item.get('author'),
                'categories': item.get('categories', []),
                'source_type': 'atom_entry'
            }
            normalized_items.append(normalized_item)
            
        return ProcessingResult(
            success=True,
            processed_data={
                'feed_info': content.get('feed_info', {}),
                'items': normalized_items,
                'content_type': 'atom_feed'
            },
            items_processed=len(normalized_items)
        )
    
    @staticmethod
    def _normalize_generic_content(content: Dict) -> ProcessingResult:
        """Normalize generic content."""
        return ProcessingResult(
            success=True,
            processed_data={
                'content': content,
                'content_type': 'generic',
                'normalized_at': datetime.now(timezone.utc).isoformat()
            },
            items_processed=1
        )
    
    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""
            
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove common encoding artifacts
        text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        text = text.replace('&quot;', '"').replace('&#39;', "'")
        
        return text
    
    @staticmethod
    def _normalize_timestamp(timestamp: Optional[str]) -> Optional[str]:
        """Normalize timestamp to ISO 8601 format."""
        if not timestamp:
            return None
            
        try:
            # Common timestamp formats
            formats = [
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S.%fZ',
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%d %H:%M:%S',
                '%a, %d %b %Y %H:%M:%S %Z',
                '%Y-%m-%d'
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(timestamp, fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.isoformat()
                except ValueError:
                    continue
                    
        except Exception:
            pass
            
        return timestamp


class IndicatorTools:
    """Tools for working with threat indicators."""
    
    INDICATOR_TYPE_MAPPING = {
        'ip': 'ipv4',
        'ip-address': 'ipv4',
        'ip_address': 'ipv4',
        'ipaddr': 'ipv4',
        'domain': 'domain',
        'hostname': 'domain',
        'fqdn': 'domain',
        'url': 'url',
        'uri': 'url',
        'hash': 'hash',
        'md5': 'md5',
        'sha1': 'sha1',
        'sha256': 'sha256',
        'sha512': 'sha512',
        'email': 'email',
        'email-address': 'email',
        'filename': 'filename',
        'file': 'filename',
        'registry-key': 'registry_key',
        'mutex': 'mutex',
        'user-agent': 'user_agent'
    }
    
    @staticmethod
    def normalize_indicator_type(indicator_type: str) -> str:
        """Normalize indicator type to standard format."""
        normalized = indicator_type.lower().replace('-', '_').replace(' ', '_')
        return IndicatorTools.INDICATOR_TYPE_MAPPING.get(normalized, normalized)
    
    @staticmethod
    def is_valid_indicator(value: str, indicator_type: str) -> bool:
        """Validate an indicator value against its type."""
        if not value or not indicator_type:
            return False
            
        try:
            if indicator_type == 'ipv4':
                ipaddress.IPv4Address(value)
                return True
            elif indicator_type == 'ipv6':
                ipaddress.IPv6Address(value)
                return True
            elif indicator_type == 'domain':
                return IndicatorTools._is_valid_domain(value)
            elif indicator_type == 'url':
                return IndicatorTools._is_valid_url(value)
            elif indicator_type in ['md5', 'sha1', 'sha256', 'sha512']:
                return IndicatorTools._is_valid_hash(value, indicator_type)
            elif indicator_type == 'email':
                return IndicatorTools._is_valid_email(value)
            else:
                # For unknown types, just check if value is non-empty
                return len(value.strip()) > 0
                
        except Exception:
            return False
    
    @staticmethod
    def _is_valid_domain(domain: str) -> bool:
        """Check if a string is a valid domain name."""
        if len(domain) > 253:
            return False
            
        domain_pattern = re.compile(
            r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?'
            r'(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$'
        )
        
        return bool(domain_pattern.match(domain))
    
    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """Check if a string is a valid URL."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    @staticmethod
    def _is_valid_hash(hash_value: str, hash_type: str) -> bool:
        """Check if a string is a valid hash of the specified type."""
        hash_lengths = {
            'md5': 32,
            'sha1': 40,
            'sha256': 64,
            'sha512': 128
        }
        
        expected_length = hash_lengths.get(hash_type)
        if not expected_length:
            return False
            
        if len(hash_value) != expected_length:
            return False
            
        return bool(re.match(r'^[a-fA-F0-9]+$', hash_value))
    
    @staticmethod
    def _is_valid_email(email: str) -> bool:
        """Check if a string is a valid email address."""
        email_pattern = re.compile(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        )
        return bool(email_pattern.match(email))
    
    @staticmethod
    def calculate_indicator_confidence(indicator: Dict) -> float:
        """Calculate confidence score for an indicator."""
        base_confidence = 0.5
        
        # Higher confidence for to_ids indicators
        if indicator.get('to_ids'):
            base_confidence += 0.3
            
        # Higher confidence for categorized indicators
        if indicator.get('category'):
            base_confidence += 0.1
            
        # Higher confidence for indicators with comments
        if indicator.get('comment'):
            base_confidence += 0.1
            
        return min(base_confidence, 1.0)


class EntityExtractor:
    """Tools for extracting entities from text content."""
    
    @staticmethod
    def extract_entities(text: str) -> List[ExtractedEntity]:
        """Extract various entities from text."""
        entities = []
        
        # Extract IP addresses
        ip_pattern = r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
        for match in re.finditer(ip_pattern, text):
            entities.append(ExtractedEntity(
                entity_type='ipv4',
                value=match.group(),
                confidence=0.9,
                context=text[max(0, match.start()-20):match.end()+20],
                source_location=f"char_{match.start()}-{match.end()}"
            ))
        
        # Extract domains
        domain_pattern = r'\b[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}\b'
        for match in re.finditer(domain_pattern, text):
            domain = match.group()
            if '.' in domain and not domain.replace('.', '').isdigit():
                entities.append(ExtractedEntity(
                    entity_type='domain',
                    value=domain,
                    confidence=0.8,
                    context=text[max(0, match.start()-20):match.end()+20],
                    source_location=f"char_{match.start()}-{match.end()}"
                ))
        
        # Extract URLs
        url_pattern = r'https?://[^\s<>"\'`|(){}[\]\\]+'
        for match in re.finditer(url_pattern, text):
            entities.append(ExtractedEntity(
                entity_type='url',
                value=match.group(),
                confidence=0.95,
                context=text[max(0, match.start()-20):match.end()+20],
                source_location=f"char_{match.start()}-{match.end()}"
            ))
        
        # Extract hashes
        hash_patterns = [
            (r'\b[a-fA-F0-9]{32}\b', 'md5'),
            (r'\b[a-fA-F0-9]{40}\b', 'sha1'),
            (r'\b[a-fA-F0-9]{64}\b', 'sha256')
        ]
        
        for pattern, hash_type in hash_patterns:
            for match in re.finditer(pattern, text):
                entities.append(ExtractedEntity(
                    entity_type=hash_type,
                    value=match.group().lower(),
                    confidence=0.9,
                    context=text[max(0, match.start()-20):match.end()+20],
                    source_location=f"char_{match.start()}-{match.end()}"
                ))
        
        # Extract CVE identifiers
        cve_pattern = r'\bCVE-\d{4}-\d{4,}\b'
        for match in re.finditer(cve_pattern, text, re.IGNORECASE):
            entities.append(ExtractedEntity(
                entity_type='cve',
                value=match.group().upper(),
                confidence=0.95,
                context=text[max(0, match.start()-20):match.end()+20],
                source_location=f"char_{match.start()}-{match.end()}"
            ))
        
        return entities


class QualityScorer:
    """Tools for scoring content and indicator quality."""
    
    @staticmethod
    def score_content_quality(content: Dict) -> float:
        """Calculate quality score for content."""
        score = 0.0
        
        # Check for required fields
        if content.get('title'):
            score += 0.2
        if content.get('content') or content.get('summary'):
            score += 0.3
        if content.get('published') or content.get('timestamp'):
            score += 0.1
        if content.get('author'):
            score += 0.1
        
        # Content length scoring
        text = content.get('content', '') or content.get('summary', '')
        if len(text) > 100:
            score += 0.2
        elif len(text) > 50:
            score += 0.1
            
        # Check for structured data
        if content.get('categories') or content.get('tags'):
            score += 0.1
            
        return min(score, 1.0)
    
    @staticmethod
    def score_indicator_quality(indicator: Dict) -> float:
        """Calculate quality score for an indicator."""
        score = 0.5  # Base score
        
        # Validation bonus
        if IndicatorTools.is_valid_indicator(indicator.get('value', ''), indicator.get('type', '')):
            score += 0.3
        
        # Metadata bonus
        if indicator.get('context'):
            score += 0.1
        if indicator.get('source_location'):
            score += 0.05
        if indicator.get('confidence', 0) > 0.8:
            score += 0.15
            
        return min(score, 1.0)


class DeduplicationTools:
    """Tools for deduplicating content and indicators."""
    
    @staticmethod
    def generate_content_hash(content: Dict) -> str:
        """Generate a hash for content deduplication."""
        # Use key fields for hash generation
        key_fields = ['title', 'link', 'published', 'content']
        hash_data = {}
        
        for field in key_fields:
            if field in content and content[field]:
                hash_data[field] = content[field]
                
        # Create deterministic hash
        content_str = json.dumps(hash_data, sort_keys=True)
        return hashlib.sha256(content_str.encode()).hexdigest()
    
    @staticmethod
    def generate_indicator_hash(indicator: Dict) -> str:
        """Generate a hash for indicator deduplication."""
        hash_data = {
            'value': indicator.get('value', '').lower().strip(),
            'type': indicator.get('type', '').lower().strip()
        }
        
        content_str = json.dumps(hash_data, sort_keys=True)
        return hashlib.md5(content_str.encode()).hexdigest()
    
    @staticmethod
    def deduplicate_indicators(indicators: List[Dict]) -> List[Dict]:
        """Remove duplicate indicators from a list."""
        seen_hashes = set()
        unique_indicators = []
        
        for indicator in indicators:
            hash_val = DeduplicationTools.generate_indicator_hash(indicator)
            if hash_val not in seen_hashes:
                seen_hashes.add(hash_val)
                unique_indicators.append(indicator)
                
        return unique_indicators


def detect_content_type(content: Any, source_url: str = None) -> ContentType:
    """Detect the type of content based on structure and metadata."""
    if isinstance(content, dict):
        # Check for STIX bundle
        if content.get('type') == 'bundle' and 'objects' in content:
            return ContentType.STIX_BUNDLE
            
        # Check for MISP feed
        if 'indicators' in content or 'Attribute' in content:
            return ContentType.MISP_FEED
            
        # Check for feed content
        if 'feed_info' in content and 'items' in content:
            feed_type = content.get('feed_info', {}).get('type', '')
            if 'rss' in feed_type.lower():
                return ContentType.RSS_FEED
            elif 'atom' in feed_type.lower():
                return ContentType.ATOM_FEED
                
        # Check for JSON feed
        if content.get('version') and 'items' in content:
            return ContentType.JSON_FEED
            
    # Check URL patterns
    if source_url:
        url_lower = source_url.lower()
        if 'taxii' in url_lower:
            return ContentType.TAXII_CONTENT
        elif any(term in url_lower for term in ['rss', 'feed.xml']):
            return ContentType.RSS_FEED
        elif 'atom' in url_lower:
            return ContentType.ATOM_FEED
            
    return ContentType.UNKNOWN