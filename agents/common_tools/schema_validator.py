"""
Schema validation functionality for Kafka messages.

This module provides the SchemaValidator class used by agents to validate
messages against JSON schemas before publishing to Kafka topics.
"""

from __future__ import annotations

import logging as std_logging
from pathlib import Path
from typing import Dict, Any, Optional

from .validation import (
    SchemaValidationError,
    get_schema_for_topic,
    validate_message,
    ValidationError
)

logger = std_logging.getLogger(__name__)

# Re-export for backwards compatibility
__all__ = ['SchemaValidator', 'SchemaValidationError', 'ValidationError']


class SchemaValidator:
    """
    Schema validator for Kafka messages.
    
    This class provides validation of messages against JSON schemas,
    with automatic schema loading based on topic names.
    """
    
    def __init__(self, schema_dir: Optional[Path] = None):
        """
        Initialize the schema validator.
        
        Args:
            schema_dir: Directory containing schema files. Defaults to
                       'schemas' directory relative to this module.
        """
        if schema_dir is None:
            # Default to config/schemas directory relative to project root
            current_dir = Path(__file__).parent.parent
            schema_dir = current_dir / "config" / "schemas"
        
        self.schema_dir = Path(schema_dir)
        self._schema_cache: Dict[str, Dict[str, Any]] = {}
        
        logger.info(f"SchemaValidator initialized with schema directory: {self.schema_dir}")
    
    def validate(self, topic: str, message: Dict[str, Any]) -> None:
        """
        Validate a message against the schema for the given topic.
        
        Args:
            topic: Kafka topic name
            message: Message to validate
            
        Raises:
            SchemaValidationError: If validation fails
        """
        schema = self._get_schema_for_topic(topic)
        if schema is None:
            logger.warning(f"No schema found for topic '{topic}', skipping validation")
            return
        
        # Validate the message (raises SchemaValidationError on failure)
        validate_message(message, schema, raise_on_error=True)
        logger.debug(f"Message validation successful for topic '{topic}'")
    
    def _get_schema_for_topic(self, topic: str) -> Optional[Dict[str, Any]]:
        """
        Get the schema for a given topic, with caching.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            Schema dictionary if found, None otherwise
        """
        if topic in self._schema_cache:
            return self._schema_cache[topic]
        
        schema = get_schema_for_topic(topic, self.schema_dir)
        if schema:
            self._schema_cache[topic] = schema
            logger.debug(f"Loaded schema for topic '{topic}'")
        else:
            logger.debug(f"No schema found for topic '{topic}'")
        
        return schema
    
    def clear_cache(self) -> None:
        """Clear the schema cache to force reloading of schemas."""
        self._schema_cache.clear()
        logger.debug("Schema cache cleared")
