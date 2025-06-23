"""
JSON Schema validation utilities for Kafka message validation.

This module provides helper functions for loading schemas, validating messages,
and handling validation errors in a consistent way across all agents.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from jsonschema import validate, ValidationError, Draft7Validator, RefResolver

logger = logging.getLogger(__name__)


class SchemaValidationError(Exception):
    """Custom exception for schema validation errors."""
    
    def __init__(self, message: str, errors: List[ValidationError]):
        super().__init__(message)
        self.errors = errors
    
    def get_error_details(self) -> List[Dict[str, Any]]:
        """Get detailed error information."""
        return [
            {
                "path": list(error.path),
                "message": error.message,
                "schema_path": list(error.schema_path),
                "instance": error.instance,
                "validator": error.validator,
                "validator_value": error.validator_value
            }
            for error in self.errors
        ]


def load_schema(schema_path: Path) -> Dict[str, Any]:
    """
    Load a JSON schema from file.
    
    Args:
        schema_path: Path to the schema file
    
    Returns:
        Loaded schema dictionary
    
    Raises:
        FileNotFoundError: If schema file doesn't exist
        json.JSONDecodeError: If schema file is invalid JSON
    """
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    
    with open(schema_path, 'r') as f:
        return json.load(f)


def validate_message(
    message: Dict[str, Any],
    schema: Dict[str, Any],
    raise_on_error: bool = True
) -> Tuple[bool, Optional[List[ValidationError]]]:
    """
    Validate a message against a JSON schema.
    
    Args:
        message: Message to validate
        schema: JSON schema to validate against
        raise_on_error: Whether to raise exception on validation error
    
    Returns:
        Tuple of (is_valid, errors)
        - is_valid: True if message is valid
        - errors: List of validation errors (empty if valid)
    
    Raises:
        SchemaValidationError: If raise_on_error=True and validation fails
    """
    validator = Draft7Validator(schema)
    errors = list(validator.iter_errors(message))
    
    if errors and raise_on_error:
        raise SchemaValidationError(
            f"Message validation failed with {len(errors)} errors",
            errors
        )
    
    return len(errors) == 0, errors


def create_error_report(
    message: Dict[str, Any],
    errors: List[ValidationError],
    topic: str,
    agent_name: str
) -> Dict[str, Any]:
    """
    Create a detailed error report for validation failures.
    
    Args:
        message: The message that failed validation
        errors: List of validation errors
        topic: Kafka topic name
        agent_name: Name of the agent that encountered the error
    
    Returns:
        Detailed error report dictionary
    """
    return {
        "topic": topic,
        "agent": agent_name,
        "message_keys": list(message.keys()) if isinstance(message, dict) else None,
        "error_count": len(errors),
        "errors": [
            {
                "path": ".".join(str(p) for p in error.path),
                "message": error.message,
                "validator": error.validator,
                "invalid_value": str(error.instance)[:100]  # Truncate long values
            }
            for error in errors
        ]
    }


def get_schema_for_topic(topic: str, schema_dir: Path) -> Optional[Dict[str, Any]]:
    """
    Get the appropriate schema for a Kafka topic.
    
    This function handles various naming conventions:
    - Exact match: feeds.discovered -> feeds.discovered.json
    - Underscore replacement: raw.intel -> raw_intel.json
    - Pattern matching: agent.delegate.* -> agent.delegate.json
    
    Args:
        topic: Kafka topic name
        schema_dir: Directory containing schema files
    
    Returns:
        Schema dictionary if found, None otherwise
    """
    # Try exact match
    exact_path = schema_dir / f"{topic}.json"
    if exact_path.exists():
        return load_schema(exact_path)
    
    # Try with underscores instead of dots
    underscore_path = schema_dir / f"{topic.replace('.', '_')}.json"
    if underscore_path.exists():
        return load_schema(underscore_path)
    
    # Try pattern matching for hierarchical topics
    parts = topic.split('.')
    if len(parts) > 2:
        # Try base pattern (e.g., agent.delegate.* -> agent.delegate)
        base_topic = '.'.join(parts[:2])
        base_path = schema_dir / f"{base_topic}.json"
        if base_path.exists():
            return load_schema(base_path)
    
    return None


def merge_schemas(base_schema: Dict[str, Any], *additional_schemas: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge multiple JSON schemas, useful for creating composite schemas.
    
    Args:
        base_schema: Base schema to start with
        *additional_schemas: Additional schemas to merge
    
    Returns:
        Merged schema dictionary
    """
    result = base_schema.copy()
    
    for schema in additional_schemas:
        # Merge properties
        if 'properties' in schema:
            if 'properties' not in result:
                result['properties'] = {}
            result['properties'].update(schema['properties'])
        
        # Merge required fields
        if 'required' in schema:
            if 'required' not in result:
                result['required'] = []
            result['required'].extend(
                field for field in schema['required']
                if field not in result['required']
            )
        
        # Update other top-level properties
        for key, value in schema.items():
            if key not in ['properties', 'required']:
                result[key] = value
    
    return result


def extract_validation_path(error: ValidationError) -> str:
    """
    Extract a human-readable path from a validation error.
    
    Args:
        error: ValidationError instance
    
    Returns:
        Human-readable path string (e.g., "payload.items[0].url")
    """
    segments = []
    for part in error.path:
        if isinstance(part, int):
            # Index into an array – attach directly to previous segment
            if segments:
                segments[-1] += f"[{part}]"
            else:
                segments.append(f"[{part}]")
        else:
            segments.append(str(part))

    return '.'.join(segments) if segments else 'root'