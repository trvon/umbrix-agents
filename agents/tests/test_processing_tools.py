"""
Test coverage for processing_agent/processing_tools.py

This module provides comprehensive test coverage for the data processing and enrichment
system, including content normalization, entity extraction, validation, and quality scoring.
"""

import pytest
import json
import time
from datetime import datetime, timezone
from typing import Dict, List
from unittest.mock import Mock, patch, MagicMock

from processing_agent.processing_tools import (
    ContentType,
    ProcessingResult,
    ExtractedEntity,
    ExtractedRelation,
    ContentNormalizer,
    IndicatorTools,
    EntityExtractor,
    QualityScorer,
    DeduplicationTools,
    detect_content_type
)


class TestContentType:
    """Test the ContentType enum."""
    
    def test_content_type_values(self):
        """Test ContentType enum values."""
        assert ContentType.RSS_FEED.value == "rss_feed"
        assert ContentType.ATOM_FEED.value == "atom_feed"
        assert ContentType.JSON_FEED.value == "json_feed"
        assert ContentType.MISP_FEED.value == "misp_feed"
        assert ContentType.TAXII_CONTENT.value == "taxii_content"
        assert ContentType.STIX_BUNDLE.value == "stix_bundle"
        assert ContentType.PLAIN_TEXT.value == "plain_text"
        assert ContentType.HTML_CONTENT.value == "html_content"
        assert ContentType.UNKNOWN.value == "unknown"

    def test_content_type_enum_membership(self):
        """Test ContentType enum membership."""
        assert ContentType.RSS_FEED in ContentType
        assert ContentType.STIX_BUNDLE in ContentType
        assert ContentType.UNKNOWN in ContentType


class TestProcessingResult:
    """Test the ProcessingResult dataclass."""
    
    def test_processing_result_creation_minimal(self):
        """Test creating ProcessingResult with minimal required fields."""
        result = ProcessingResult(success=True)
        
        assert result.success is True
        assert result.processed_data is None
        assert result.error is None
        assert result.processing_time is None
        assert result.input_size is None
        assert result.output_size is None
        assert result.items_processed == 0

    def test_processing_result_creation_full(self):
        """Test creating ProcessingResult with all fields."""
        processed_data = {"items": [{"id": "1", "title": "Test"}]}
        
        result = ProcessingResult(
            success=True,
            processed_data=processed_data,
            error=None,
            processing_time=1.5,
            input_size=1024,
            output_size=512,
            items_processed=5
        )
        
        assert result.success is True
        assert result.processed_data == processed_data
        assert result.error is None
        assert result.processing_time == 1.5
        assert result.input_size == 1024
        assert result.output_size == 512
        assert result.items_processed == 5

    def test_processing_result_error_case(self):
        """Test creating ProcessingResult for error cases."""
        result = ProcessingResult(
            success=False,
            error="Processing failed due to invalid input",
            processing_time=0.1
        )
        
        assert result.success is False
        assert result.error == "Processing failed due to invalid input"
        assert result.processing_time == 0.1
        assert result.processed_data is None


class TestExtractedEntity:
    """Test the ExtractedEntity dataclass."""
    
    def test_extracted_entity_creation_minimal(self):
        """Test creating ExtractedEntity with minimal required fields."""
        entity = ExtractedEntity(
            entity_type="ipv4",
            value="192.168.1.1",
            confidence=0.9
        )
        
        assert entity.entity_type == "ipv4"
        assert entity.value == "192.168.1.1"
        assert entity.confidence == 0.9
        assert entity.context is None
        assert entity.source_location is None
        assert entity.metadata is None

    def test_extracted_entity_creation_full(self):
        """Test creating ExtractedEntity with all fields."""
        metadata = {"source": "threat_intel", "validated": True}
        
        entity = ExtractedEntity(
            entity_type="domain",
            value="malicious.example.com",
            confidence=0.85,
            context="Found in phishing email at malicious.example.com/login",
            source_location="line_42_col_15",
            metadata=metadata
        )
        
        assert entity.entity_type == "domain"
        assert entity.value == "malicious.example.com"
        assert entity.confidence == 0.85
        assert entity.context == "Found in phishing email at malicious.example.com/login"
        assert entity.source_location == "line_42_col_15"
        assert entity.metadata == metadata


class TestExtractedRelation:
    """Test the ExtractedRelation dataclass."""
    
    def test_extracted_relation_creation_minimal(self):
        """Test creating ExtractedRelation with minimal required fields."""
        relation = ExtractedRelation(
            source_entity="192.168.1.1",
            target_entity="malicious.example.com",
            relation_type="resolves_to",
            confidence=0.8
        )
        
        assert relation.source_entity == "192.168.1.1"
        assert relation.target_entity == "malicious.example.com"
        assert relation.relation_type == "resolves_to"
        assert relation.confidence == 0.8
        assert relation.context is None
        assert relation.metadata is None

    def test_extracted_relation_creation_full(self):
        """Test creating ExtractedRelation with all fields."""
        metadata = {"confidence_source": "DNS_lookup", "timestamp": "2025-01-01T12:00:00Z"}
        
        relation = ExtractedRelation(
            source_entity="malware.exe",
            target_entity="c2-server.evil.com",
            relation_type="communicates_with",
            confidence=0.95,
            context="Traffic observed from infected host",
            metadata=metadata
        )
        
        assert relation.source_entity == "malware.exe"
        assert relation.target_entity == "c2-server.evil.com"
        assert relation.relation_type == "communicates_with"
        assert relation.confidence == 0.95
        assert relation.context == "Traffic observed from infected host"
        assert relation.metadata == metadata


class TestContentNormalizer:
    """Test the ContentNormalizer class."""
    
    def test_normalize_content_rss_feed(self):
        """Test normalizing RSS feed content."""
        rss_content = {
            "feed_info": {"title": "Security News", "type": "rss"},
            "items": [
                {
                    "id": "item1",
                    "title": "Security Alert",
                    "content": "A new vulnerability has been discovered",
                    "link": "https://security.example.com/alert1",
                    "published": "2025-01-01T12:00:00Z",
                    "author": "Security Team"
                }
            ]
        }
        
        result = ContentNormalizer.normalize_content(rss_content, ContentType.RSS_FEED)
        
        assert result.success is True
        assert result.processed_data["content_type"] == "rss_feed"
        assert len(result.processed_data["items"]) == 1
        assert result.items_processed == 1
        
        item = result.processed_data["items"][0]
        assert item["id"] == "item1"
        assert item["title"] == "Security Alert"
        assert item["source_type"] == "rss_item"

    def test_normalize_content_misp_feed(self):
        """Test normalizing MISP feed content."""
        misp_content = {
            "indicators": [
                {
                    "value": "192.168.1.100",
                    "type": "ip",
                    "category": "Network activity",
                    "comment": "Suspicious IP",
                    "to_ids": True,
                    "timestamp": "2025-01-01T12:00:00Z"
                }
            ]
        }
        
        result = ContentNormalizer.normalize_content(misp_content, ContentType.MISP_FEED)
        
        assert result.success is True
        assert result.processed_data["content_type"] == "misp_feed"
        assert len(result.processed_data["indicators"]) == 1
        
        indicator = result.processed_data["indicators"][0]
        assert indicator["value"] == "192.168.1.100"
        assert indicator["type"] == "ipv4"  # Normalized type
        assert indicator["source_type"] == "misp_indicator"

    def test_normalize_content_stix_bundle(self):
        """Test normalizing STIX bundle content."""
        stix_content = {
            "objects": [
                {
                    "id": "indicator--12345",
                    "type": "indicator",
                    "spec_version": "2.1",
                    "created": "2025-01-01T12:00:00Z",
                    "modified": "2025-01-01T12:00:00Z",
                    "labels": ["malicious-activity"],
                    "confidence": 85
                }
            ]
        }
        
        result = ContentNormalizer.normalize_content(stix_content, ContentType.STIX_BUNDLE)
        
        assert result.success is True
        assert result.processed_data["content_type"] == "stix_bundle"
        assert len(result.processed_data["objects"]) == 1
        
        obj = result.processed_data["objects"][0]
        assert obj["id"] == "indicator--12345"
        assert obj["type"] == "indicator"
        assert obj["source_type"] == "stix_object"

    def test_normalize_content_generic(self):
        """Test normalizing generic content."""
        generic_content = {"custom_field": "value", "data": [1, 2, 3]}
        
        result = ContentNormalizer.normalize_content(generic_content, ContentType.UNKNOWN)
        
        assert result.success is True
        assert result.processed_data["content_type"] == "generic"
        assert result.processed_data["content"] == generic_content
        assert "normalized_at" in result.processed_data

    def test_normalize_content_exception_handling(self):
        """Test content normalization with exception handling."""
        # Pass invalid content that will cause an exception
        invalid_content = None
        
        result = ContentNormalizer.normalize_content(invalid_content, ContentType.RSS_FEED)
        
        assert result.success is False
        assert "Content normalization failed" in result.error
        assert result.processing_time is not None

    def test_normalize_rss_content_with_guid(self):
        """Test RSS normalization with GUID instead of ID."""
        rss_content = {
            "items": [
                {
                    "guid": "unique-guid-123",
                    "title": "Test Article",
                    "summary": "Test summary"
                }
            ]
        }
        
        result = ContentNormalizer._normalize_rss_content(rss_content)
        
        assert result.success is True
        item = result.processed_data["items"][0]
        assert item["id"] == "unique-guid-123"

    def test_normalize_rss_content_fallback_to_link(self):
        """Test RSS normalization falling back to link for ID."""
        rss_content = {
            "items": [
                {
                    "link": "https://example.com/article1",
                    "title": "Test Article"
                }
            ]
        }
        
        result = ContentNormalizer._normalize_rss_content(rss_content)
        
        assert result.success is True
        item = result.processed_data["items"][0]
        assert item["id"] == "https://example.com/article1"

    def test_clean_text_html_removal(self):
        """Test text cleaning with HTML tag removal."""
        html_text = "<p>This is <strong>bold</strong> text with <a href='#'>link</a></p>"
        
        cleaned = ContentNormalizer._clean_text(html_text)
        
        assert cleaned == "This is bold text with link"

    def test_clean_text_whitespace_normalization(self):
        """Test text cleaning with whitespace normalization."""
        messy_text = "This   has\t\tmultiple\n\nwhitespace   issues"
        
        cleaned = ContentNormalizer._clean_text(messy_text)
        
        assert cleaned == "This has multiple whitespace issues"

    def test_clean_text_html_entities(self):
        """Test text cleaning with HTML entity replacement."""
        entity_text = "Johnson &amp; Associates said &quot;Hello&quot; &lt;test&gt;"
        
        cleaned = ContentNormalizer._clean_text(entity_text)
        
        assert cleaned == "Johnson & Associates said \"Hello\" <test>"

    def test_clean_text_empty_string(self):
        """Test text cleaning with empty string."""
        assert ContentNormalizer._clean_text("") == ""
        assert ContentNormalizer._clean_text(None) == ""

    def test_normalize_timestamp_iso_format(self):
        """Test timestamp normalization with ISO format."""
        timestamp = "2025-01-01T12:00:00Z"
        
        normalized = ContentNormalizer._normalize_timestamp(timestamp)
        
        assert normalized is not None
        assert "2025-01-01T12:00:00" in normalized

    def test_normalize_timestamp_rfc_format(self):
        """Test timestamp normalization with RFC format."""
        timestamp = "Mon, 01 Jan 2025 12:00:00 GMT"
        
        normalized = ContentNormalizer._normalize_timestamp(timestamp)
        
        assert normalized is not None
        assert "2025-01-01T12:00:00" in normalized

    def test_normalize_timestamp_date_only(self):
        """Test timestamp normalization with date only."""
        timestamp = "2025-01-01"
        
        normalized = ContentNormalizer._normalize_timestamp(timestamp)
        
        assert normalized is not None
        assert "2025-01-01T00:00:00" in normalized

    def test_normalize_timestamp_invalid_format(self):
        """Test timestamp normalization with invalid format."""
        timestamp = "invalid-timestamp"
        
        normalized = ContentNormalizer._normalize_timestamp(timestamp)
        
        # Should return the original string if can't parse
        assert normalized == "invalid-timestamp"

    def test_normalize_timestamp_none(self):
        """Test timestamp normalization with None input."""
        assert ContentNormalizer._normalize_timestamp(None) is None
        assert ContentNormalizer._normalize_timestamp("") is None


class TestIndicatorTools:
    """Test the IndicatorTools class."""
    
    def test_normalize_indicator_type_known_types(self):
        """Test indicator type normalization for known types."""
        assert IndicatorTools.normalize_indicator_type("ip") == "ipv4"
        assert IndicatorTools.normalize_indicator_type("ip-address") == "ipv4"
        assert IndicatorTools.normalize_indicator_type("domain") == "domain"
        assert IndicatorTools.normalize_indicator_type("hostname") == "domain"
        assert IndicatorTools.normalize_indicator_type("email-address") == "email_address"  # Normalized from hyphen to underscore
        assert IndicatorTools.normalize_indicator_type("SHA256") == "sha256"

    def test_normalize_indicator_type_case_insensitive(self):
        """Test indicator type normalization is case insensitive."""
        assert IndicatorTools.normalize_indicator_type("IP") == "ipv4"
        assert IndicatorTools.normalize_indicator_type("Domain") == "domain"
        assert IndicatorTools.normalize_indicator_type("EMAIL") == "email"

    def test_normalize_indicator_type_with_spaces_and_hyphens(self):
        """Test indicator type normalization handles spaces and hyphens."""
        assert IndicatorTools.normalize_indicator_type("ip address") == "ipv4"
        assert IndicatorTools.normalize_indicator_type("registry-key") == "registry_key"
        assert IndicatorTools.normalize_indicator_type("user agent") == "user_agent"

    def test_normalize_indicator_type_unknown(self):
        """Test indicator type normalization for unknown types."""
        unknown_type = "custom_indicator_type"
        assert IndicatorTools.normalize_indicator_type(unknown_type) == "custom_indicator_type"

    def test_is_valid_indicator_ipv4_valid(self):
        """Test IPv4 indicator validation with valid addresses."""
        assert IndicatorTools.is_valid_indicator("192.168.1.1", "ipv4") is True
        assert IndicatorTools.is_valid_indicator("10.0.0.1", "ipv4") is True
        assert IndicatorTools.is_valid_indicator("255.255.255.255", "ipv4") is True

    def test_is_valid_indicator_ipv4_invalid(self):
        """Test IPv4 indicator validation with invalid addresses."""
        assert IndicatorTools.is_valid_indicator("256.1.1.1", "ipv4") is False
        assert IndicatorTools.is_valid_indicator("192.168.1", "ipv4") is False
        assert IndicatorTools.is_valid_indicator("not-an-ip", "ipv4") is False

    def test_is_valid_indicator_ipv6_valid(self):
        """Test IPv6 indicator validation with valid addresses."""
        assert IndicatorTools.is_valid_indicator("2001:db8::1", "ipv6") is True
        assert IndicatorTools.is_valid_indicator("::1", "ipv6") is True
        assert IndicatorTools.is_valid_indicator("fe80::1", "ipv6") is True

    def test_is_valid_indicator_ipv6_invalid(self):
        """Test IPv6 indicator validation with invalid addresses."""
        assert IndicatorTools.is_valid_indicator("not-ipv6", "ipv6") is False
        assert IndicatorTools.is_valid_indicator("192.168.1.1", "ipv6") is False

    def test_is_valid_indicator_domain_valid(self):
        """Test domain indicator validation with valid domains."""
        assert IndicatorTools.is_valid_indicator("example.com", "domain") is True
        assert IndicatorTools.is_valid_indicator("sub.example.com", "domain") is True
        assert IndicatorTools.is_valid_indicator("test-domain.org", "domain") is True

    def test_is_valid_indicator_domain_invalid(self):
        """Test domain indicator validation with invalid domains."""
        assert IndicatorTools.is_valid_indicator("", "domain") is False
        assert IndicatorTools.is_valid_indicator("invalid..domain", "domain") is False
        assert IndicatorTools.is_valid_indicator("a" * 300, "domain") is False  # Too long

    def test_is_valid_indicator_url_valid(self):
        """Test URL indicator validation with valid URLs."""
        assert IndicatorTools.is_valid_indicator("https://example.com", "url") is True
        assert IndicatorTools.is_valid_indicator("http://test.org/path", "url") is True
        assert IndicatorTools.is_valid_indicator("ftp://files.example.com", "url") is True

    def test_is_valid_indicator_url_invalid(self):
        """Test URL indicator validation with invalid URLs."""
        assert IndicatorTools.is_valid_indicator("not-a-url", "url") is False
        assert IndicatorTools.is_valid_indicator("://missing-scheme", "url") is False
        assert IndicatorTools.is_valid_indicator("http://", "url") is False

    def test_is_valid_indicator_hash_valid(self):
        """Test hash indicator validation with valid hashes."""
        md5_hash = "d41d8cd98f00b204e9800998ecf8427e"
        sha1_hash = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        sha256_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        
        assert IndicatorTools.is_valid_indicator(md5_hash, "md5") is True
        assert IndicatorTools.is_valid_indicator(sha1_hash, "sha1") is True
        assert IndicatorTools.is_valid_indicator(sha256_hash, "sha256") is True

    def test_is_valid_indicator_hash_invalid(self):
        """Test hash indicator validation with invalid hashes."""
        assert IndicatorTools.is_valid_indicator("too-short", "md5") is False
        assert IndicatorTools.is_valid_indicator("g" * 32, "md5") is False  # Invalid characters
        assert IndicatorTools.is_valid_indicator("a" * 33, "md5") is False  # Wrong length

    def test_is_valid_indicator_email_valid(self):
        """Test email indicator validation with valid emails."""
        assert IndicatorTools.is_valid_indicator("test@example.com", "email") is True
        assert IndicatorTools.is_valid_indicator("user.name+tag@domain.co.uk", "email") is True

    def test_is_valid_indicator_email_invalid(self):
        """Test email indicator validation with invalid emails."""
        assert IndicatorTools.is_valid_indicator("not-an-email", "email") is False
        assert IndicatorTools.is_valid_indicator("@domain.com", "email") is False
        assert IndicatorTools.is_valid_indicator("user@", "email") is False

    def test_is_valid_indicator_unknown_type(self):
        """Test indicator validation for unknown types."""
        assert IndicatorTools.is_valid_indicator("some-value", "unknown_type") is True
        assert IndicatorTools.is_valid_indicator("", "unknown_type") is False
        assert IndicatorTools.is_valid_indicator("   ", "unknown_type") is False

    def test_is_valid_indicator_empty_values(self):
        """Test indicator validation with empty values."""
        assert IndicatorTools.is_valid_indicator("", "ipv4") is False
        assert IndicatorTools.is_valid_indicator("value", "") is False
        assert IndicatorTools.is_valid_indicator("", "") is False

    def test_is_valid_domain_edge_cases(self):
        """Test domain validation edge cases."""
        assert IndicatorTools._is_valid_domain("a.co") is True  # Short but valid
        assert IndicatorTools._is_valid_domain("test") is True  # Single word is valid in this implementation
        assert IndicatorTools._is_valid_domain(".example.com") is False  # Starts with dot
        assert IndicatorTools._is_valid_domain("example.com.") is False  # Ends with dot

    def test_is_valid_url_edge_cases(self):
        """Test URL validation edge cases."""
        assert IndicatorTools._is_valid_url("https://example.com/") is True
        assert IndicatorTools._is_valid_url("file:///path/to/file") is False  # File URLs don't have netloc
        assert IndicatorTools._is_valid_url("mailto:test@example.com") is False  # Mailto URLs don't have netloc

    def test_is_valid_hash_unknown_type(self):
        """Test hash validation with unknown hash type."""
        assert IndicatorTools._is_valid_hash("abcd1234", "unknown_hash") is False

    def test_is_valid_email_edge_cases(self):
        """Test email validation edge cases."""
        assert IndicatorTools._is_valid_email("user@domain") is False  # No TLD
        assert IndicatorTools._is_valid_email("user@.com") is False  # Invalid domain
        assert IndicatorTools._is_valid_email("user@domain.c") is False  # TLD too short

    def test_calculate_indicator_confidence_base(self):
        """Test indicator confidence calculation with base score."""
        indicator = {}
        confidence = IndicatorTools.calculate_indicator_confidence(indicator)
        assert confidence == 0.5

    def test_calculate_indicator_confidence_to_ids(self):
        """Test indicator confidence calculation with to_ids flag."""
        indicator = {"to_ids": True}
        confidence = IndicatorTools.calculate_indicator_confidence(indicator)
        assert confidence == 0.8  # 0.5 + 0.3

    def test_calculate_indicator_confidence_with_category(self):
        """Test indicator confidence calculation with category."""
        indicator = {"category": "Network activity"}
        confidence = IndicatorTools.calculate_indicator_confidence(indicator)
        assert confidence == 0.6  # 0.5 + 0.1

    def test_calculate_indicator_confidence_with_comment(self):
        """Test indicator confidence calculation with comment."""
        indicator = {"comment": "Suspicious activity detected"}
        confidence = IndicatorTools.calculate_indicator_confidence(indicator)
        assert confidence == 0.6  # 0.5 + 0.1

    def test_calculate_indicator_confidence_maximum(self):
        """Test indicator confidence calculation at maximum."""
        indicator = {
            "to_ids": True,
            "category": "Malware",
            "comment": "High confidence indicator"
        }
        confidence = IndicatorTools.calculate_indicator_confidence(indicator)
        assert confidence == 1.0  # Capped at 1.0


class TestEntityExtractor:
    """Test the EntityExtractor class."""
    
    def test_extract_entities_ipv4(self):
        """Test IPv4 entity extraction."""
        text = "Suspicious traffic from 192.168.1.100 and 10.0.0.5 detected."
        
        entities = EntityExtractor.extract_entities(text)
        
        ip_entities = [e for e in entities if e.entity_type == 'ipv4']
        assert len(ip_entities) == 2
        assert ip_entities[0].value == "192.168.1.100"
        assert ip_entities[1].value == "10.0.0.5"
        assert ip_entities[0].confidence == 0.9

    def test_extract_entities_domains(self):
        """Test domain entity extraction."""
        text = "Malicious domains include evil.example.com and bad-actor.org found in traffic."
        
        entities = EntityExtractor.extract_entities(text)
        
        domain_entities = [e for e in entities if e.entity_type == 'domain']
        assert len(domain_entities) == 2
        assert domain_entities[0].value == "evil.example.com"
        assert domain_entities[1].value == "bad-actor.org"
        assert domain_entities[0].confidence == 0.8

    def test_extract_entities_urls(self):
        """Test URL entity extraction."""
        text = "Phishing site at https://fake-bank.com/login and http://malware.example.org/download"
        
        entities = EntityExtractor.extract_entities(text)
        
        url_entities = [e for e in entities if e.entity_type == 'url']
        assert len(url_entities) == 2
        assert url_entities[0].value == "https://fake-bank.com/login"
        assert url_entities[1].value == "http://malware.example.org/download"
        assert url_entities[0].confidence == 0.95

    def test_extract_entities_hashes(self):
        """Test hash entity extraction."""
        text = "File hashes: MD5 d41d8cd98f00b204e9800998ecf8427e, SHA1 da39a3ee5e6b4b0d3255bfef95601890afd80709, SHA256 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        
        entities = EntityExtractor.extract_entities(text)
        
        hash_entities = [e for e in entities if e.entity_type in ['md5', 'sha1', 'sha256']]
        assert len(hash_entities) == 3
        
        md5_entity = next(e for e in hash_entities if e.entity_type == 'md5')
        assert md5_entity.value == "d41d8cd98f00b204e9800998ecf8427e"
        
        sha1_entity = next(e for e in hash_entities if e.entity_type == 'sha1')
        assert sha1_entity.value == "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        
        sha256_entity = next(e for e in hash_entities if e.entity_type == 'sha256')
        assert sha256_entity.value == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    def test_extract_entities_cve(self):
        """Test CVE identifier extraction."""
        text = "Vulnerabilities CVE-2021-44228 and CVE-2023-12345 found in analysis."
        
        entities = EntityExtractor.extract_entities(text)
        
        cve_entities = [e for e in entities if e.entity_type == 'cve']
        assert len(cve_entities) == 2
        assert cve_entities[0].value == "CVE-2021-44228"
        assert cve_entities[1].value == "CVE-2023-12345"
        assert cve_entities[0].confidence == 0.95

    def test_extract_entities_context_and_location(self):
        """Test that entities include context and source location."""
        text = "The malicious IP 192.168.1.100 was detected"
        
        entities = EntityExtractor.extract_entities(text)
        
        ip_entity = next(e for e in entities if e.entity_type == 'ipv4')
        assert "malicious IP 192.168.1.100 was" in ip_entity.context
        assert "char_" in ip_entity.source_location
        assert "-" in ip_entity.source_location

    def test_extract_entities_domain_filtering(self):
        """Test that domains are properly filtered (no pure numeric domains)."""
        text = "IP 192.168.1.1 and domain example.com but not 1.2.3.4"
        
        entities = EntityExtractor.extract_entities(text)
        
        # Should find IPs and real domain, but not treat IP as domain
        ip_entities = [e for e in entities if e.entity_type == 'ipv4']
        domain_entities = [e for e in entities if e.entity_type == 'domain']
        
        assert len(ip_entities) == 2  # Both IPs
        assert len(domain_entities) == 1  # Only example.com
        assert domain_entities[0].value == "example.com"

    def test_extract_entities_empty_text(self):
        """Test entity extraction with empty text."""
        entities = EntityExtractor.extract_entities("")
        assert entities == []

    def test_extract_entities_no_matches(self):
        """Test entity extraction with text containing no recognizable entities."""
        text = "This is just plain text with no indicators or entities."
        entities = EntityExtractor.extract_entities(text)
        assert entities == []


class TestQualityScorer:
    """Test the QualityScorer class."""
    
    def test_score_content_quality_full_content(self):
        """Test content quality scoring with all fields present."""
        content = {
            "title": "Important Security Alert",
            "content": "This is a detailed security alert with substantial content that provides valuable information about a new threat.",
            "published": "2025-01-01T12:00:00Z",
            "author": "Security Team",
            "categories": ["security", "alert"]
        }
        
        score = QualityScorer.score_content_quality(content)
        
        # Should get max score: 0.2 + 0.3 + 0.1 + 0.1 + 0.2 + 0.1 = 1.0 (with floating point precision)
        assert abs(score - 1.0) < 0.0001

    def test_score_content_quality_minimal_content(self):
        """Test content quality scoring with minimal fields."""
        content = {
            "title": "Alert"
        }
        
        score = QualityScorer.score_content_quality(content)
        
        # Should get: 0.2 (title only)
        assert score == 0.2

    def test_score_content_quality_medium_content(self):
        """Test content quality scoring with medium length content."""
        content = {
            "title": "Medium Alert",
            "summary": "This is a medium length summary with some detail and more text to make it over 50 characters",  # > 50 chars
            "author": "Team"
        }
        
        score = QualityScorer.score_content_quality(content)
        
        # Should get: 0.2 + 0.3 + 0.1 + 0.1 = 0.7
        assert score == 0.7

    def test_score_content_quality_short_content(self):
        """Test content quality scoring with short content."""
        content = {
            "title": "Short",
            "summary": "Short text"  # < 50 chars
        }
        
        score = QualityScorer.score_content_quality(content)
        
        # Should get: 0.2 + 0.3 = 0.5 (no bonus for content length)
        assert score == 0.5

    def test_score_content_quality_with_summary_instead_of_content(self):
        """Test content quality scoring using summary when content is missing."""
        content = {
            "title": "Test",
            "summary": "This is a long summary that should count as content and provide substantial information for quality scoring."
        }
        
        score = QualityScorer.score_content_quality(content)
        
        # Should get: 0.2 + 0.3 + 0.2 = 0.7
        assert score == 0.7

    def test_score_content_quality_empty_content(self):
        """Test content quality scoring with empty content."""
        content = {}
        
        score = QualityScorer.score_content_quality(content)
        
        assert score == 0.0

    def test_score_indicator_quality_high_quality(self):
        """Test indicator quality scoring for high quality indicator."""
        indicator = {
            "value": "192.168.1.100",
            "type": "ipv4",
            "context": "Found in malicious traffic",
            "source_location": "line_42",
            "confidence": 0.9
        }
        
        score = QualityScorer.score_indicator_quality(indicator)
        
        # Should get: 0.5 + 0.3 + 0.1 + 0.05 + 0.15 = 1.1, capped at 1.0
        assert score == 1.0

    def test_score_indicator_quality_basic(self):
        """Test indicator quality scoring for basic indicator."""
        indicator = {
            "value": "example.com",
            "type": "domain"
        }
        
        score = QualityScorer.score_indicator_quality(indicator)
        
        # Should get: 0.5 + 0.3 = 0.8 (valid indicator)
        assert score == 0.8

    def test_score_indicator_quality_invalid(self):
        """Test indicator quality scoring for invalid indicator."""
        indicator = {
            "value": "not-valid-ip",
            "type": "ipv4"
        }
        
        score = QualityScorer.score_indicator_quality(indicator)
        
        # Should get: 0.5 (base score only, no validation bonus)
        assert score == 0.5

    def test_score_indicator_quality_low_confidence(self):
        """Test indicator quality scoring with low confidence."""
        indicator = {
            "value": "192.168.1.1",
            "type": "ipv4",
            "confidence": 0.5  # Below 0.8 threshold
        }
        
        score = QualityScorer.score_indicator_quality(indicator)
        
        # Should get: 0.5 + 0.3 = 0.8 (no confidence bonus)
        assert score == 0.8


class TestDeduplicationTools:
    """Test the DeduplicationTools class."""
    
    def test_generate_content_hash_consistent(self):
        """Test that content hash generation is consistent."""
        content = {
            "title": "Test Article",
            "link": "https://example.com/article",
            "published": "2025-01-01T12:00:00Z",
            "content": "Article content"
        }
        
        hash1 = DeduplicationTools.generate_content_hash(content)
        hash2 = DeduplicationTools.generate_content_hash(content)
        
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex length

    def test_generate_content_hash_different_content(self):
        """Test that different content produces different hashes."""
        content1 = {
            "title": "Article 1",
            "link": "https://example.com/1"
        }
        
        content2 = {
            "title": "Article 2", 
            "link": "https://example.com/2"
        }
        
        hash1 = DeduplicationTools.generate_content_hash(content1)
        hash2 = DeduplicationTools.generate_content_hash(content2)
        
        assert hash1 != hash2

    def test_generate_content_hash_field_order_independence(self):
        """Test that field order doesn't affect hash."""
        content1 = {
            "title": "Test",
            "link": "https://example.com",
            "content": "Content"
        }
        
        content2 = {
            "content": "Content",
            "title": "Test",
            "link": "https://example.com"
        }
        
        hash1 = DeduplicationTools.generate_content_hash(content1)
        hash2 = DeduplicationTools.generate_content_hash(content2)
        
        assert hash1 == hash2

    def test_generate_content_hash_ignores_non_key_fields(self):
        """Test that non-key fields don't affect hash."""
        content1 = {
            "title": "Test",
            "link": "https://example.com",
            "author": "Author 1",
            "extra_field": "extra"
        }
        
        content2 = {
            "title": "Test",
            "link": "https://example.com",
            "author": "Author 2",
            "different_field": "different"
        }
        
        hash1 = DeduplicationTools.generate_content_hash(content1)
        hash2 = DeduplicationTools.generate_content_hash(content2)
        
        # Hashes should be the same since only key fields matter
        assert hash1 == hash2

    def test_generate_indicator_hash_consistent(self):
        """Test that indicator hash generation is consistent."""
        indicator = {
            "value": "192.168.1.100",
            "type": "ipv4"
        }
        
        hash1 = DeduplicationTools.generate_indicator_hash(indicator)
        hash2 = DeduplicationTools.generate_indicator_hash(indicator)
        
        assert hash1 == hash2
        assert len(hash1) == 32  # MD5 hex length

    def test_generate_indicator_hash_case_insensitive(self):
        """Test that indicator hash is case insensitive."""
        indicator1 = {
            "value": "Example.COM",
            "type": "DOMAIN"
        }
        
        indicator2 = {
            "value": "example.com",
            "type": "domain"
        }
        
        hash1 = DeduplicationTools.generate_indicator_hash(indicator1)
        hash2 = DeduplicationTools.generate_indicator_hash(indicator2)
        
        assert hash1 == hash2

    def test_generate_indicator_hash_strips_whitespace(self):
        """Test that indicator hash strips whitespace."""
        indicator1 = {
            "value": "  192.168.1.1  ",
            "type": "  ipv4  "
        }
        
        indicator2 = {
            "value": "192.168.1.1",
            "type": "ipv4"
        }
        
        hash1 = DeduplicationTools.generate_indicator_hash(indicator1)
        hash2 = DeduplicationTools.generate_indicator_hash(indicator2)
        
        assert hash1 == hash2

    def test_deduplicate_indicators_removes_duplicates(self):
        """Test that duplicate indicators are removed."""
        indicators = [
            {"value": "192.168.1.1", "type": "ipv4", "id": 1},
            {"value": "example.com", "type": "domain", "id": 2},
            {"value": "192.168.1.1", "type": "ipv4", "id": 3},  # Duplicate
            {"value": "other.com", "type": "domain", "id": 4}
        ]
        
        unique_indicators = DeduplicationTools.deduplicate_indicators(indicators)
        
        assert len(unique_indicators) == 3
        
        # Should keep first occurrence of each unique indicator
        ids = [ind["id"] for ind in unique_indicators]
        assert 1 in ids  # First IP
        assert 2 in ids  # First domain
        assert 4 in ids  # Second domain
        assert 3 not in ids  # Duplicate IP should be removed

    def test_deduplicate_indicators_preserves_order(self):
        """Test that deduplication preserves order of first occurrences."""
        indicators = [
            {"value": "c.com", "type": "domain"},
            {"value": "a.com", "type": "domain"},
            {"value": "b.com", "type": "domain"},
            {"value": "a.com", "type": "domain"}  # Duplicate
        ]
        
        unique_indicators = DeduplicationTools.deduplicate_indicators(indicators)
        
        assert len(unique_indicators) == 3
        assert unique_indicators[0]["value"] == "c.com"
        assert unique_indicators[1]["value"] == "a.com"
        assert unique_indicators[2]["value"] == "b.com"

    def test_deduplicate_indicators_empty_list(self):
        """Test deduplication with empty list."""
        result = DeduplicationTools.deduplicate_indicators([])
        assert result == []

    def test_deduplicate_indicators_no_duplicates(self):
        """Test deduplication when no duplicates exist."""
        indicators = [
            {"value": "192.168.1.1", "type": "ipv4"},
            {"value": "example.com", "type": "domain"}
        ]
        
        unique_indicators = DeduplicationTools.deduplicate_indicators(indicators)
        
        assert len(unique_indicators) == 2
        assert unique_indicators == indicators


class TestDetectContentType:
    """Test the detect_content_type function."""
    
    def test_detect_content_type_stix_bundle(self):
        """Test STIX bundle detection."""
        content = {
            "type": "bundle",
            "objects": [
                {"id": "indicator--123", "type": "indicator"}
            ]
        }
        
        detected_type = detect_content_type(content)
        assert detected_type == ContentType.STIX_BUNDLE

    def test_detect_content_type_misp_feed_indicators(self):
        """Test MISP feed detection with indicators field."""
        content = {
            "indicators": [
                {"value": "192.168.1.1", "type": "ip"}
            ]
        }
        
        detected_type = detect_content_type(content)
        assert detected_type == ContentType.MISP_FEED

    def test_detect_content_type_misp_feed_attribute(self):
        """Test MISP feed detection with Attribute field."""
        content = {
            "Attribute": [
                {"value": "example.com", "type": "domain"}
            ]
        }
        
        detected_type = detect_content_type(content)
        assert detected_type == ContentType.MISP_FEED

    def test_detect_content_type_rss_feed_by_structure(self):
        """Test RSS feed detection by structure."""
        content = {
            "feed_info": {"type": "rss", "title": "Security Feed"},
            "items": [
                {"title": "Alert", "link": "https://example.com"}
            ]
        }
        
        detected_type = detect_content_type(content)
        assert detected_type == ContentType.RSS_FEED

    def test_detect_content_type_atom_feed_by_structure(self):
        """Test Atom feed detection by structure."""
        content = {
            "feed_info": {"type": "atom", "title": "News Feed"},
            "items": [
                {"title": "News", "link": "https://news.example.com"}
            ]
        }
        
        detected_type = detect_content_type(content)
        assert detected_type == ContentType.ATOM_FEED

    def test_detect_content_type_json_feed(self):
        """Test JSON feed detection."""
        content = {
            "version": "https://jsonfeed.org/version/1",
            "items": [
                {"id": "1", "title": "Article"}
            ]
        }
        
        detected_type = detect_content_type(content)
        assert detected_type == ContentType.JSON_FEED

    def test_detect_content_type_by_url_taxii(self):
        """Test content type detection by TAXII URL."""
        content = {"data": "some content"}
        
        detected_type = detect_content_type(content, "https://taxii.example.com/collections/123")
        assert detected_type == ContentType.TAXII_CONTENT

    def test_detect_content_type_by_url_rss(self):
        """Test content type detection by RSS URL patterns."""
        content = {"data": "some content"}
        
        # Test with 'rss' in URL
        detected_type = detect_content_type(content, "https://example.com/rss")
        assert detected_type == ContentType.RSS_FEED
        
        # Test with 'feed.xml' in URL
        detected_type = detect_content_type(content, "https://example.com/feed.xml")
        assert detected_type == ContentType.RSS_FEED

    def test_detect_content_type_by_url_atom(self):
        """Test content type detection by Atom URL."""
        content = {"data": "some content"}
        
        detected_type = detect_content_type(content, "https://example.com/atom")
        assert detected_type == ContentType.ATOM_FEED

    def test_detect_content_type_unknown_dict(self):
        """Test unknown content type for unrecognized dict."""
        content = {
            "custom_field": "value",
            "data": [1, 2, 3]
        }
        
        detected_type = detect_content_type(content)
        assert detected_type == ContentType.UNKNOWN

    def test_detect_content_type_non_dict(self):
        """Test content type detection for non-dict content."""
        content = "This is plain text content"
        
        detected_type = detect_content_type(content)
        assert detected_type == ContentType.UNKNOWN

    def test_detect_content_type_none_url(self):
        """Test content type detection with None URL."""
        content = {"data": "content"}
        
        detected_type = detect_content_type(content, None)
        assert detected_type == ContentType.UNKNOWN

    def test_detect_content_type_case_insensitive_url(self):
        """Test that URL pattern matching is case insensitive."""
        content = {"data": "content"}
        
        detected_type = detect_content_type(content, "https://example.com/RSS")
        assert detected_type == ContentType.RSS_FEED
        
        detected_type = detect_content_type(content, "https://TAXII.example.com/api")
        assert detected_type == ContentType.TAXII_CONTENT


class TestNormalizeAtomContent:
    """Test the _normalize_atom_content method (via ATOM_FEED content type)."""
    
    def test_normalize_atom_content(self):
        """Test Atom feed content normalization."""
        atom_content = {
            "feed_info": {"title": "Atom Feed", "type": "atom"},
            "items": [
                {
                    "id": "atom-entry-1",
                    "title": "Atom Entry",
                    "content": "Atom content",
                    "updated": "2025-01-01T12:00:00Z"
                }
            ]
        }
        
        result = ContentNormalizer.normalize_content(atom_content, ContentType.ATOM_FEED)
        
        assert result.success is True
        assert result.processed_data["content_type"] == "atom_feed"
        assert result.processed_data["feed_info"] == {"title": "Atom Feed", "type": "atom"}
        assert len(result.processed_data["items"]) == 1
        assert result.items_processed == 1
        
        # Check normalized item
        item = result.processed_data["items"][0]
        assert item["id"] == "atom-entry-1"
        assert item["title"] == "Atom Entry"
        assert item["content"] == "Atom content"
        assert item["source_type"] == "atom_entry"
        assert item["updated"] == "2025-01-01T12:00:00+00:00"


class TestIntegrationScenarios:
    """Integration tests combining multiple components."""
    
    def test_complete_processing_workflow_rss(self):
        """Test complete processing workflow for RSS content."""
        # Raw RSS-like content
        raw_content = {
            "feed_info": {"title": "Threat Intelligence Feed", "type": "rss"},
            "items": [
                {
                    "id": "alert-1",
                    "title": "Malicious IP Detected",
                    "content": "Suspicious activity from IP 192.168.1.100 and domain evil.example.com detected. Hash: d41d8cd98f00b204e9800998ecf8427e",
                    "published": "2025-01-01T12:00:00Z",
                    "categories": ["threat-intel", "network"]
                }
            ]
        }
        
        # 1. Detect content type
        content_type = detect_content_type(raw_content)
        assert content_type == ContentType.RSS_FEED
        
        # 2. Normalize content
        normalized_result = ContentNormalizer.normalize_content(raw_content, content_type)
        assert normalized_result.success is True
        
        # 3. Extract entities from content
        item_content = normalized_result.processed_data["items"][0]["content"]
        entities = EntityExtractor.extract_entities(item_content)
        
        # Should find IP, domain, and hash
        entity_types = [e.entity_type for e in entities]
        assert "ipv4" in entity_types
        assert "domain" in entity_types
        assert "md5" in entity_types
        
        # 4. Score content quality
        quality_score = QualityScorer.score_content_quality(normalized_result.processed_data["items"][0])
        assert quality_score > 0.7  # Should be high quality
        
        # 5. Generate content hash for deduplication
        content_hash = DeduplicationTools.generate_content_hash(normalized_result.processed_data["items"][0])
        assert len(content_hash) == 64

    def test_complete_processing_workflow_misp(self):
        """Test complete processing workflow for MISP content."""
        # Raw MISP-like content
        raw_content = {
            "indicators": [
                {
                    "value": "MALICIOUS.EXAMPLE.COM",  # Test case normalization
                    "type": "domain",
                    "category": "Network activity",
                    "comment": "C2 server",
                    "to_ids": True,
                    "timestamp": "2025-01-01T12:00:00Z"
                },
                {
                    "value": "invalid-ip-address",  # Invalid indicator
                    "type": "ip",
                    "category": "Network activity"
                }
            ]
        }
        
        # 1. Detect content type
        content_type = detect_content_type(raw_content)
        assert content_type == ContentType.MISP_FEED
        
        # 2. Normalize content (includes validation)
        normalized_result = ContentNormalizer.normalize_content(raw_content, content_type)
        assert normalized_result.success is True
        
        # Should only include valid indicator
        indicators = normalized_result.processed_data["indicators"]
        assert len(indicators) == 1
        assert indicators[0]["value"] == "MALICIOUS.EXAMPLE.COM"
        assert indicators[0]["type"] == "domain"
        
        # 3. Score indicator quality
        quality_score = QualityScorer.score_indicator_quality(indicators[0])
        assert quality_score > 0.8  # Should be high quality (valid + metadata)
        
        # 4. Generate indicator hash
        indicator_hash = DeduplicationTools.generate_indicator_hash(indicators[0])
        assert len(indicator_hash) == 32

    def test_deduplication_workflow(self):
        """Test deduplication workflow with multiple similar indicators."""
        indicators = [
            {"value": "192.168.1.1", "type": "ipv4", "source": "feed1"},
            {"value": "example.com", "type": "domain", "source": "feed1"},
            {"value": "192.168.1.1", "type": "ipv4", "source": "feed2"},  # Duplicate
            {"value": "EXAMPLE.COM", "type": "DOMAIN", "source": "feed2"},  # Case duplicate
            {"value": "  192.168.1.1  ", "type": "  ipv4  ", "source": "feed3"},  # Whitespace duplicate
            {"value": "other.com", "type": "domain", "source": "feed3"}
        ]
        
        # Deduplicate
        unique_indicators = DeduplicationTools.deduplicate_indicators(indicators)
        
        # Should remove duplicates (case and whitespace insensitive)
        assert len(unique_indicators) == 3
        
        # Verify we kept first occurrence of each
        sources = [ind["source"] for ind in unique_indicators]
        assert "feed1" in sources  # First IP
        assert "feed1" in sources  # First domain
        assert "feed3" in sources  # Unique domain

    def test_entity_extraction_with_validation(self):
        """Test entity extraction followed by validation."""
        text = """
        Threat report contains the following IOCs:
        - IP: 192.168.1.100 (valid)
        - IP: 999.999.999.999 (invalid)
        - Domain: evil.example.com (valid)
        - Domain: invalid..domain (invalid)
        - Hash: d41d8cd98f00b204e9800998ecf8427e (valid MD5)
        - URL: https://phishing.example.com/login
        """
        
        # Extract entities
        entities = EntityExtractor.extract_entities(text)
        
        # Validate extracted entities
        valid_entities = []
        for entity in entities:
            if IndicatorTools.is_valid_indicator(entity.value, entity.entity_type):
                valid_entities.append(entity)
        
        # Should filter out invalid entities
        assert len(valid_entities) < len(entities)
        
        # Check that valid entities are present
        valid_values = [e.value for e in valid_entities]
        assert "192.168.1.100" in valid_values
        assert "evil.example.com" in valid_values
        assert "d41d8cd98f00b204e9800998ecf8427e" in valid_values
        assert "https://phishing.example.com/login" in valid_values
        
        # Invalid entities should not be present
        assert "999.999.999.999" not in valid_values


if __name__ == "__main__":
    pytest.main([__file__, "-v"])