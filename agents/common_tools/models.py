# Legacy import - redirect to new canonical model
# This maintains backwards compatibility while transitioning to the new model
from .models.feed_record import FeedRecord, SourceType, ContentType, RecordType

# Re-export for backwards compatibility
__all__ = ["FeedRecord", "SourceType", "ContentType", "RecordType"]