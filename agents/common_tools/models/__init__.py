"""
Common data models for the Umbrix CTI platform.

This module provides canonical data models used across all agents to ensure
consistency and proper validation throughout the system.
"""

from .feed_record import (
    FeedRecord,
    FeedRecordMetadata,
    FeedRecordData,
    ProcessingError,
    SourceType,
    ContentType,
    RecordType,
    ProcessingStage,
    ExtractionQuality,
    RawFeedRecord,
    EnrichedFeedRecord,
    NormalizedFeedRecord,
    ProcessedFeedRecord,
    create_raw_feed_record,
    create_enriched_feed_record,
    create_normalized_feed_record
)

__all__ = [
    "FeedRecord",
    "FeedRecordMetadata", 
    "FeedRecordData",
    "ProcessingError",
    "SourceType",
    "ContentType",
    "RecordType",
    "ProcessingStage",
    "ExtractionQuality",
    "RawFeedRecord",
    "EnrichedFeedRecord",
    "NormalizedFeedRecord",
    "ProcessedFeedRecord",
    "create_raw_feed_record",
    "create_enriched_feed_record",
    "create_normalized_feed_record"
]