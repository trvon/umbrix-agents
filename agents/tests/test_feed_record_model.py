"""
Unit tests for the canonical FeedRecord data model.

Tests cover:
- Model creation and validation
- Serialization/deserialization
- Field validation and constraints
- Cross-language compatibility
- Processing stage transitions
"""

import json
import pytest
import uuid
from datetime import datetime, timezone
from typing import Dict, Any

from agents.common_tools.models.feed_record import (
    FeedRecord,
    FeedRecordMetadata,
    FeedRecordData,
    ProcessingError,
    SourceType,
    ContentType,
    RecordType,
    ProcessingStage,
    ExtractionQuality,
    create_raw_feed_record,
    create_enriched_feed_record,
    create_normalized_feed_record
)


class TestFeedRecordCreation:
    """Test cases for FeedRecord creation and basic properties."""
    
    def test_minimal_feed_record(self):
        """Test creating a minimal FeedRecord with only required fields."""
        record = FeedRecord(url="https://example.com")
        
        assert str(record.url) == "https://example.com/"
        assert record.record_type == RecordType.RAW
        assert record.schema_version == "1.0.0"
        assert record.discovered_at is not None
        assert len(record.id) > 0
        assert record.tags == []
        assert isinstance(record.metadata, FeedRecordMetadata)
    
    def test_full_feed_record(self):
        """Test creating a FeedRecord with all fields populated."""
        metadata = FeedRecordMetadata(
            extraction_quality=ExtractionQuality.GOOD,
            extraction_confidence=0.85,
            threat_indicators=["192.168.1.1", "malware.exe"],
            cve_ids=["CVE-2024-1234"]
        )
        
        data = FeedRecordData(
            original_feed_entry={"title": "Original Title"}
        )
        
        record = FeedRecord(
            url="https://example.com/feed",
            title="Test Feed Record",
            description="A test feed record for validation",
            source_name="Test Feed",
            source_type=SourceType.RSS,
            vendor_name="Test Vendor",
            requires_payment=False,
            is_security_focused=True,
            published_at=datetime.now(timezone.utc),
            raw_content_type=ContentType.HTML,
            raw_content="<html><body>Test content</body></html>",
            tags=["malware", "threat", "analysis"],
            metadata=metadata,
            record_type=RecordType.ENRICHED,
            data=data
        )
        
        assert record.title == "Test Feed Record"
        assert record.source_type == SourceType.RSS
        assert record.metadata.extraction_quality == ExtractionQuality.GOOD
        assert "malware" in record.tags
        assert record.metadata.cve_ids == ["CVE-2024-1234"]
    
    def test_factory_functions(self):
        """Test factory functions for creating different record types."""
        # Test raw record creation
        raw_record = create_raw_feed_record(
            url="https://example.com",
            source_type=SourceType.RSS,
            title="Raw Record"
        )
        assert raw_record.record_type == RecordType.RAW
        assert raw_record.source_type == SourceType.RSS
        
        # Test enriched record creation
        enriched_record = create_enriched_feed_record(
            raw_record,
            vendor_name="Enhanced Vendor",
            is_security_focused=True
        )
        assert enriched_record.record_type == RecordType.ENRICHED
        assert enriched_record.vendor_name == "Enhanced Vendor"
        assert enriched_record.title == "Raw Record"  # Preserved from base
        
        # Test normalized record creation
        normalized_record = create_normalized_feed_record(
            enriched_record,
            metadata=FeedRecordMetadata(processing_stage=ProcessingStage.NORMALIZED)
        )
        assert normalized_record.record_type == RecordType.NORMALIZED
        assert normalized_record.metadata.processing_stage == ProcessingStage.NORMALIZED


class TestFeedRecordValidation:
    """Test cases for FeedRecord validation."""
    
    def test_url_validation(self):
        """Test URL field validation."""
        # Valid URL
        record = FeedRecord(url="https://example.com")
        assert str(record.url) == "https://example.com/"
        
        # Invalid URL should raise validation error
        with pytest.raises(Exception):  # Pydantic validation error
            FeedRecord(url="not-a-url")
    
    def test_tag_validation(self):
        """Test tag validation and cleaning."""
        record = FeedRecord(
            url="https://example.com",
            tags=["  tag1  ", "tag2", "tag1", "", "tag3"]  # Duplicates and whitespace
        )
        
        # Should remove duplicates, empty tags, and trim whitespace
        expected_tags = ["tag1", "tag2", "tag3"]
        assert record.tags == expected_tags
    
    def test_tag_length_limit(self):
        """Test tag length limitations."""
        long_tag = "a" * 100  # 100 characters
        record = FeedRecord(
            url="https://example.com",
            tags=[long_tag]
        )
        
        # Tag should be truncated to 50 characters
        assert len(record.tags[0]) == 50
    
    def test_max_tags_limit(self):
        """Test maximum number of tags."""
        many_tags = [f"tag{i}" for i in range(60)]  # More than 50 tags
        # This should raise a validation error since we have more than 50 tags
        with pytest.raises(Exception):  # Pydantic validation error
            FeedRecord(
                url="https://example.com",
                tags=many_tags
            )
    
    def test_content_length_limits(self):
        """Test content field length limitations."""
        # Test title length limit
        long_title = "a" * 600  # More than 500 characters
        with pytest.raises(Exception):  # Pydantic validation error
            FeedRecord(url="https://example.com", title=long_title)
        
        # Test description length limit
        long_description = "a" * 6000  # More than 5000 characters
        with pytest.raises(Exception):  # Pydantic validation error
            FeedRecord(url="https://example.com", description=long_description)
        
        # Test raw content length limit
        long_content = "a" * 110000  # More than 100000 characters
        with pytest.raises(Exception):  # Pydantic validation error
            FeedRecord(url="https://example.com", raw_content=long_content)
    
    def test_cve_validation(self):
        """Test CVE ID format validation."""
        # Valid CVE IDs
        valid_metadata = FeedRecordMetadata(
            cve_ids=["CVE-2024-1234", "CVE-2023-12345"]
        )
        record = FeedRecord(url="https://example.com", metadata=valid_metadata)
        assert record.metadata.cve_ids == ["CVE-2024-1234", "CVE-2023-12345"]
        
        # Invalid CVE format should raise validation error
        with pytest.raises(Exception):  # Pydantic validation error
            FeedRecordMetadata(cve_ids=["INVALID-CVE"])
    
    def test_confidence_score_validation(self):
        """Test confidence score range validation."""
        # Valid confidence scores
        metadata = FeedRecordMetadata(
            extraction_confidence=0.5,
            confidence_score=1.0
        )
        record = FeedRecord(url="https://example.com", metadata=metadata)
        assert record.metadata.extraction_confidence == 0.5
        
        # Invalid confidence scores (outside 0-1 range)
        with pytest.raises(Exception):  # Pydantic validation error
            FeedRecordMetadata(extraction_confidence=1.5)
        
        with pytest.raises(Exception):  # Pydantic validation error
            FeedRecordMetadata(confidence_score=-0.1)
    
    def test_schema_version_validation(self):
        """Test schema version format validation."""
        # Valid schema version
        record = FeedRecord(url="https://example.com", schema_version="1.2.3")
        assert record.schema_version == "1.2.3"
        
        # Invalid schema version format
        with pytest.raises(Exception):  # Pydantic validation error
            FeedRecord(url="https://example.com", schema_version="invalid")


class TestFeedRecordSerialization:
    """Test cases for FeedRecord serialization and deserialization."""
    
    def test_json_serialization(self):
        """Test JSON serialization and deserialization."""
        original = FeedRecord(
            url="https://example.com",
            title="Test Record",
            source_type=SourceType.RSS,
            tags=["test", "serialization"],
            metadata=FeedRecordMetadata(
                extraction_quality=ExtractionQuality.GOOD,
                threat_indicators=["192.168.1.1"]
            )
        )
        
        # Serialize to JSON
        json_str = original.to_json()
        assert isinstance(json_str, str)
        
        # Deserialize from JSON
        deserialized = FeedRecord.from_json(json_str)
        
        # Verify all fields match
        assert deserialized.url == original.url
        assert deserialized.title == original.title
        assert deserialized.source_type == original.source_type
        assert deserialized.tags == original.tags
        assert deserialized.metadata.extraction_quality == original.metadata.extraction_quality
        assert deserialized.metadata.threat_indicators == original.metadata.threat_indicators
    
    def test_dict_serialization(self):
        """Test dictionary serialization and deserialization."""
        original = FeedRecord(
            url="https://example.com",
            title="Test Record",
            tags=["test"]
        )
        
        # Serialize to dict
        data_dict = original.to_dict()
        assert isinstance(data_dict, dict)
        assert data_dict["url"] == "https://example.com/"
        assert data_dict["title"] == "Test Record"
        
        # Deserialize from dict
        deserialized = FeedRecord.from_dict(data_dict)
        assert deserialized.url == original.url
        assert deserialized.title == original.title
    
    def test_exclude_none_serialization(self):
        """Test serialization with None values excluded."""
        record = FeedRecord(
            url="https://example.com",
            title="Test Record"
            # Many fields left as None
        )
        
        # Serialize excluding None values
        data_dict = record.to_dict(exclude_none=True)
        
        # None fields should not be present
        assert "description" not in data_dict
        assert "vendor_name" not in data_dict
        assert "published_at" not in data_dict
        
        # Non-None fields should be present
        assert "url" in data_dict
        assert "title" in data_dict
        assert "discovered_at" in data_dict


class TestFeedRecordMethods:
    """Test cases for FeedRecord utility methods."""
    
    def test_add_tag(self):
        """Test adding individual tags."""
        record = FeedRecord(url="https://example.com")
        
        record.add_tag("malware")
        assert "malware" in record.tags
        
        # Adding duplicate should not create duplicate
        record.add_tag("malware")
        assert record.tags.count("malware") == 1
        
        # Adding empty tag should be ignored
        record.add_tag("")
        record.add_tag("   ")
        assert len([tag for tag in record.tags if not tag.strip()]) == 0
    
    def test_add_tags(self):
        """Test adding multiple tags."""
        record = FeedRecord(url="https://example.com")
        
        record.add_tags(["tag1", "tag2", "tag3"])
        assert all(tag in record.tags for tag in ["tag1", "tag2", "tag3"])
    
    def test_clone(self):
        """Test cloning records with overrides."""
        original = FeedRecord(
            url="https://example.com",
            title="Original Title",
            source_type=SourceType.RSS
        )
        
        # Clone with overrides
        cloned = original.clone(
            title="Cloned Title",
            record_type=RecordType.ENRICHED
        )
        
        # Original should be unchanged
        assert original.title == "Original Title"
        assert original.record_type == RecordType.RAW
        
        # Clone should have overrides applied
        assert cloned.title == "Cloned Title"
        assert cloned.record_type == RecordType.ENRICHED
        assert cloned.url == original.url  # Preserved
        assert cloned.source_type == original.source_type  # Preserved
    
    def test_set_processing_stage(self):
        """Test setting processing stage."""
        record = FeedRecord(url="https://example.com")
        
        record.set_processing_stage(ProcessingStage.ENRICHED)
        assert record.metadata.processing_stage == ProcessingStage.ENRICHED
    
    def test_add_processing_error(self):
        """Test adding processing errors."""
        record = FeedRecord(url="https://example.com")
        
        record.add_processing_error(
            stage="extraction",
            error_type="timeout",
            error_message="Request timed out after 30 seconds"
        )
        
        assert record.metadata.processing_errors is not None
        assert len(record.metadata.processing_errors) == 1
        
        error = record.metadata.processing_errors[0]
        assert error.stage == "extraction"
        assert error.error_type == "timeout"
        assert error.error_message == "Request timed out after 30 seconds"
        assert error.timestamp is not None
    
    def test_kafka_key_generation(self):
        """Test Kafka key generation."""
        record = FeedRecord(url="https://example.com")
        key = record.get_kafka_key()
        
        assert key.startswith("feed_record:")
        assert record.id in key
    
    def test_duplicate_detection(self):
        """Test duplicate detection logic."""
        record1 = FeedRecord(
            url="https://example.com",
            title="Same Title",
            raw_content="Same content"
        )
        
        record2 = FeedRecord(
            url="https://example.com",
            title="Same Title",
            raw_content="Same content"
        )
        
        record3 = FeedRecord(
            url="https://different.com",
            title="Same Title"
        )
        
        record4 = FeedRecord(
            url="https://example.com",
            title="Same Title",
            raw_content="Different content"
        )
        
        # Same URL and content should be duplicate
        assert record1.is_duplicate_of(record2)
        
        # Different URL should not be duplicate
        assert not record1.is_duplicate_of(record3)
        
        # Same URL but different content should not be duplicate
        assert not record1.is_duplicate_of(record4)


class TestCrossLanguageCompatibility:
    """Test cases for cross-language compatibility."""
    
    def test_json_schema_compliance(self):
        """Test that serialized records comply with the JSON schema."""
        record = FeedRecord(
            url="https://example.com",
            title="Test Record",
            source_type=SourceType.RSS,
            record_type=RecordType.ENRICHED,
            metadata=FeedRecordMetadata(
                extraction_quality=ExtractionQuality.GOOD,
                cve_ids=["CVE-2024-1234"]
            )
        )
        
        # Serialize to JSON
        json_data = json.loads(record.to_json())
        
        # Check required fields are present
        assert "url" in json_data
        assert "discovered_at" in json_data
        assert "record_type" in json_data
        assert "schema_version" in json_data
        
        # Check enum serialization
        assert json_data["source_type"] == "rss"
        assert json_data["record_type"] == "enriched"
        assert json_data["metadata"]["extraction_quality"] == "good"
    
    def test_datetime_serialization(self):
        """Test datetime serialization format."""
        record = FeedRecord(
            url="https://example.com",
            published_at=datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        )
        
        json_data = json.loads(record.to_json())
        
        # Should be ISO format
        assert json_data["published_at"] == "2024-01-15T12:30:45Z"
        assert isinstance(json_data["discovered_at"], str)
    
    def test_rust_compatible_format(self):
        """Test that JSON format is compatible with Rust expectations."""
        record = FeedRecord(
            url="https://example.com",
            source_type=SourceType.SHODAN_STREAM,
            raw_content_type=ContentType.JSON,
            metadata=FeedRecordMetadata(
                processing_stage=ProcessingStage.COLLECTED
            )
        )
        
        json_data = json.loads(record.to_json())
        
        # Check snake_case enum values (Rust convention)
        assert json_data["source_type"] == "shodan_stream"
        assert json_data["raw_content_type"] == "json"
        assert json_data["metadata"]["processing_stage"] == "collected"


class TestFeedRecordMetadata:
    """Test cases specific to FeedRecordMetadata."""
    
    def test_metadata_creation(self):
        """Test metadata creation with various fields."""
        metadata = FeedRecordMetadata(
            extraction_quality=ExtractionQuality.EXCELLENT,
            extraction_confidence=0.95,
            threat_indicators=["malicious.com", "bad.exe"],
            threat_actors=["APT29", "Lazarus"],
            malware_families=["Zeus", "Emotet"],
            attack_techniques=["T1566.001", "T1055"],
            cve_ids=["CVE-2024-1234", "CVE-2024-5678"],
            geographic_regions=["North America", "Europe"],
            industry_sectors=["Finance", "Healthcare"],
            needs_manual_review=True,
            correlation_id="corr-123-456"
        )
        
        assert metadata.extraction_quality == ExtractionQuality.EXCELLENT
        assert metadata.extraction_confidence == 0.95
        assert len(metadata.threat_indicators) == 2
        assert "APT29" in metadata.threat_actors
        assert "T1566.001" in metadata.attack_techniques
        assert metadata.needs_manual_review is True
    
    def test_processing_errors(self):
        """Test processing error tracking."""
        error = ProcessingError(
            stage="enrichment",
            error_type="api_error",
            error_message="API rate limit exceeded",
            timestamp=datetime.now(timezone.utc)
        )
        
        assert error.stage == "enrichment"
        assert error.error_type == "api_error"
        assert isinstance(error.timestamp, datetime)


class TestFeedRecordData:
    """Test cases specific to FeedRecordData."""
    
    def test_data_container(self):
        """Test data container functionality."""
        data = FeedRecordData(
            original_feed_entry={"rss_title": "Original Title"},
            enrichment_results={"sentiment": "negative"},
            graph_entities=[
                {"type": "domain", "value": "malicious.com"},
                {"type": "ip", "value": "192.168.1.1"}
            ]
        )
        
        assert data.original_feed_entry["rss_title"] == "Original Title"
        assert data.enrichment_results["sentiment"] == "negative"
        assert len(data.graph_entities) == 2
    
    def test_additional_fields(self):
        """Test that additional fields are allowed in data."""
        data = FeedRecordData(custom_field="custom_value")
        
        # Should be allowed due to extra="allow"
        assert data.custom_field == "custom_value"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])