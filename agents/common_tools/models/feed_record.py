"""
Canonical FeedRecord data model for the Umbrix CTI platform.

This module provides the authoritative data model for all threat intelligence
feed records across the system. All agents must use this model to ensure
consistency and proper validation.
"""

import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Literal
from pydantic import BaseModel, AnyUrl, Field, field_validator, model_validator, ConfigDict
from enum import Enum


class SourceType(str, Enum):
    """Enumeration of supported source types."""
    RSS = "rss"
    ATOM = "atom"
    TAXII = "taxii"
    MISP_FEED = "misp_feed"
    SHODAN_STREAM = "shodan_stream"
    MANUAL = "manual"
    API = "api"
    CSV = "csv"
    JSON = "json"
    XML = "xml"
    INTELLIGENT_CRAWL = "intelligent_crawl"


class ContentType(str, Enum):
    """Enumeration of supported content types."""
    HTML = "html"
    JSON = "json"
    XML = "xml"
    TEXT = "text"
    PDF = "pdf"
    BINARY = "binary"


class RecordType(str, Enum):
    """Enumeration of record processing stages."""
    RAW = "raw"
    ENRICHED = "enriched"
    NORMALIZED = "normalized"
    PROCESSED = "processed"


class ProcessingStage(str, Enum):
    """Enumeration of processing stages."""
    DISCOVERED = "discovered"
    COLLECTED = "collected"
    ENRICHED = "enriched"
    NORMALIZED = "normalized"
    INGESTED = "ingested"


class ExtractionQuality(str, Enum):
    """Enumeration of extraction quality levels."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    VERY_POOR = "very_poor"


class ProcessingError(BaseModel):
    """Model for tracking processing errors."""
    stage: str
    error_type: str
    error_message: str
    timestamp: datetime


class FeedRecordMetadata(BaseModel):
    """Metadata container for FeedRecord enrichments and processing details."""
    
    # Content extraction metadata
    extracted_clean_text: Optional[str] = None
    extraction_quality: Optional[ExtractionQuality] = None
    extraction_method: Optional[str] = None
    extraction_confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    extraction_metrics: Optional[Dict[str, Any]] = None
    extraction_attempts: Optional[List[Dict[str, Any]]] = None
    classified_page_type: Optional[str] = None
    
    # Threat intelligence extracted data
    threat_indicators: Optional[List[str]] = None
    threat_actors: Optional[List[str]] = None
    malware_families: Optional[List[str]] = None
    attack_techniques: Optional[List[str]] = None
    cve_ids: Optional[List[str]] = None
    geographic_regions: Optional[List[str]] = None
    industry_sectors: Optional[List[str]] = None
    
    # Processing metadata
    confidence_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    needs_manual_review: Optional[bool] = None
    processing_stage: Optional[ProcessingStage] = None
    correlation_id: Optional[str] = None
    parent_record_id: Optional[str] = None
    processing_errors: Optional[List[ProcessingError]] = None
    
    model_config = ConfigDict(extra="allow")  # Allow additional metadata fields
        
    @field_validator('cve_ids')
    @classmethod
    def validate_cve_format(cls, v):
        """Validate CVE ID format."""
        if v is not None:
            import re
            cve_pattern = re.compile(r'^CVE-\d{4}-\d{4,}$')
            for cve_id in v:
                if not cve_pattern.match(cve_id):
                    raise ValueError(f"Invalid CVE format: {cve_id}")
        return v


class FeedRecordData(BaseModel):
    """Stage-specific data payload container."""
    
    original_feed_entry: Optional[Dict[str, Any]] = None
    enrichment_results: Optional[Dict[str, Any]] = None
    normalization_results: Optional[Dict[str, Any]] = None
    graph_entities: Optional[List[Dict[str, Any]]] = None
    
    model_config = ConfigDict(extra="allow")  # Allow additional data fields


class FeedRecord(BaseModel):
    """
    Canonical FeedRecord model for all threat intelligence feed records.
    
    This model represents a single feed record at any stage of processing,
    from initial discovery through final graph ingestion. All agents must
    use this model to ensure data consistency and proper validation.
    """
    
    # Core identification
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    # Basic content fields
    title: Optional[str] = Field(None, max_length=500)
    description: Optional[str] = Field(None, max_length=5000)
    url: AnyUrl
    
    # Source information
    source_name: Optional[str] = Field(None, max_length=200)
    source_type: Optional[SourceType] = None
    vendor_name: Optional[str] = Field(None, max_length=100)
    
    # Content classification (DSPy enriched)
    requires_payment: Optional[bool] = None
    is_security_focused: Optional[bool] = None
    
    # Timestamps
    published_at: Optional[datetime] = None
    discovered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Raw content
    raw_content_type: Optional[ContentType] = None
    raw_content: Optional[str] = Field(None, max_length=500000)  # Increased for long articles
    
    # Tags and metadata
    tags: List[str] = Field(default_factory=list, max_length=50)
    metadata: FeedRecordMetadata = Field(default_factory=FeedRecordMetadata)
    
    # Processing information
    record_type: RecordType = RecordType.RAW
    schema_version: str = "1.0.0"
    data: Optional[FeedRecordData] = Field(default_factory=FeedRecordData)
    
    model_config = ConfigDict(
        extra="forbid",  # Strict schema enforcement
        validate_assignment=True
    )
    
    @field_validator('tags')
    @classmethod
    def validate_tags(cls, v):
        """Validate and clean tags."""
        if v is not None:
            # Remove duplicates while preserving order
            seen = set()
            unique_tags = []
            for tag in v:
                if tag and len(tag.strip()) > 0 and tag not in seen:
                    cleaned_tag = tag.strip()[:50]  # Limit tag length
                    unique_tags.append(cleaned_tag)
                    seen.add(cleaned_tag)
            return unique_tags
        return v
    
    @field_validator('schema_version')
    @classmethod
    def validate_schema_version(cls, v):
        """Validate semantic version format."""
        import re
        if not re.match(r'^\d+\.\d+\.\d+$', v):
            raise ValueError("Schema version must be in semantic version format (x.y.z)")
        return v
    
    @model_validator(mode='after')
    def validate_record_consistency(self):
        """Validate record consistency across fields."""
        record_type = self.record_type
        metadata = self.metadata or FeedRecordMetadata()
        
        # Ensure processing stage matches record type where applicable
        if hasattr(metadata, 'processing_stage') and metadata.processing_stage:
            stage_to_type = {
                ProcessingStage.DISCOVERED: [RecordType.RAW],
                ProcessingStage.COLLECTED: [RecordType.RAW],
                ProcessingStage.ENRICHED: [RecordType.ENRICHED],
                ProcessingStage.NORMALIZED: [RecordType.NORMALIZED],
                ProcessingStage.INGESTED: [RecordType.PROCESSED]
            }
            
            if metadata.processing_stage in stage_to_type:
                valid_types = stage_to_type[metadata.processing_stage]
                if record_type not in valid_types:
                    raise ValueError(
                        f"Processing stage {metadata.processing_stage} "
                        f"incompatible with record type {record_type}"
                    )
        
        return self
    
    def to_dict(self, exclude_none: bool = True) -> Dict[str, Any]:
        """Convert to dictionary with proper serialization."""
        data = self.model_dump(
            exclude_none=exclude_none,
            by_alias=True
        )
        # Convert AnyUrl to string for proper serialization
        if 'url' in data and data['url'] is not None:
            data['url'] = str(data['url'])
        return data
    
    def to_json(self, exclude_none: bool = True, indent: Optional[int] = None) -> str:
        """Convert to JSON string with proper serialization."""
        json_bytes = self.model_dump_json(
            exclude_none=exclude_none,
            by_alias=True,
            indent=indent
        )
        return json_bytes.decode('utf-8') if isinstance(json_bytes, bytes) else json_bytes
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FeedRecord':
        """Create FeedRecord from dictionary."""
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'FeedRecord':
        """Create FeedRecord from JSON string."""
        return cls.model_validate_json(json_str)
    
    def clone(self, **overrides) -> 'FeedRecord':
        """Create a copy of this record with optional field overrides."""
        data = self.model_dump()
        data.update(overrides)
        return self.__class__(**data)
    
    def add_tag(self, tag: str) -> None:
        """Add a tag to the record if not already present."""
        if tag and tag.strip() and tag not in self.tags:
            self.tags.append(tag.strip()[:50])
    
    def add_tags(self, tags: List[str]) -> None:
        """Add multiple tags to the record."""
        for tag in tags:
            self.add_tag(tag)
    
    def set_processing_stage(self, stage: ProcessingStage) -> None:
        """Set the processing stage in metadata."""
        if self.metadata is None:
            self.metadata = FeedRecordMetadata()
        self.metadata.processing_stage = stage
    
    def add_processing_error(
        self,
        stage: str,
        error_type: str,
        error_message: str
    ) -> None:
        """Add a processing error to the record."""
        if self.metadata is None:
            self.metadata = FeedRecordMetadata()
        
        if self.metadata.processing_errors is None:
            self.metadata.processing_errors = []
        
        error = ProcessingError(
            stage=stage,
            error_type=error_type,
            error_message=error_message,
            timestamp=datetime.now(timezone.utc)
        )
        
        self.metadata.processing_errors.append(error)
    
    def get_kafka_key(self) -> str:
        """Generate a Kafka key for this record."""
        return f"feed_record:{self.id}"
    
    def is_duplicate_of(self, other: 'FeedRecord') -> bool:
        """Check if this record is a duplicate of another based on URL and content."""
        if str(self.url) != str(other.url):
            return False
        
        # Compare content if available
        if self.raw_content and other.raw_content:
            return self.raw_content == other.raw_content
        
        # Compare titles if available
        if self.title and other.title:
            return self.title == other.title
        
        # Default to URL comparison
        return True


# Type aliases for convenience
RawFeedRecord = FeedRecord
EnrichedFeedRecord = FeedRecord
NormalizedFeedRecord = FeedRecord
ProcessedFeedRecord = FeedRecord


# Factory functions for different record types
def create_raw_feed_record(
    url: str,
    source_type: Optional[SourceType] = None,
    **kwargs
) -> FeedRecord:
    """Create a new raw feed record."""
    return FeedRecord(
        url=url,
        source_type=source_type,
        record_type=RecordType.RAW,
        **kwargs
    )


def create_enriched_feed_record(
    base_record: FeedRecord,
    **enrichments
) -> FeedRecord:
    """Create an enriched feed record from a base record."""
    return base_record.clone(
        record_type=RecordType.ENRICHED,
        **enrichments
    )


def create_normalized_feed_record(
    base_record: FeedRecord,
    **normalizations
) -> FeedRecord:
    """Create a normalized feed record from a base record."""
    return base_record.clone(
        record_type=RecordType.NORMALIZED,
        **normalizations
    )