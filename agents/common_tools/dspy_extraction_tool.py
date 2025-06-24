import json
import sys
from google.adk.tools import FunctionTool as Tool
import dspy
from .advanced_dspy_modules import ThreatIntelExtractor
from .dspy_config_manager import configure_dspy_from_config

class DSPyExtractionTool(Tool):
    """Tool that uses DSPy to extract entities and relations from arbitrary text."""

    # Provide Config for loader to check extra behavior
    class Config:
        extra = "allow"

    def __init__(self, *args, **kwargs):
        # Accept loader-provided arguments (e.g., name) but wrap only the extract method
        super().__init__(func=self.extract)
        
        # Configure DSPy if not already configured
        try:
            configure_dspy_from_config()
        except Exception as e:
            print(f"[DSPyExtraction] Warning: Failed to configure DSPy: {e}", file=sys.stderr)
        
        # Initialize the extractor
        self.extractor = ThreatIntelExtractor()

    def extract(self, text: str, patterns: list[str] | None = None) -> dict:
        """Extract entities/relations from text.

        Args:
            text: Raw text content.
            patterns: Optional list of patterns or entity types to extract.
        Returns:
            dict with keys `entities` and optionally `relations`.
        """
        try:
            # Use ThreatIntelExtractor
            result = self.extractor.forward(text)
            
            # Parse extracted IOCs into entity categories
            entities = {
                'ip': [],
                'domain': [],
                'url': [],
                'hash': [],
                'email': [],
                'cve': [],
                'threat_actor': [],
                'malware': [],
                'attack_pattern': []
            }
            
            if hasattr(result, 'extracted_iocs') and result.extracted_iocs:
                for ioc in result.extracted_iocs:
                    ioc = ioc.strip()
                    if not ioc:
                        continue
                    
                    # Simple pattern matching for IOC types
                    if ioc.startswith('CVE-'):
                        entities['cve'].append(ioc)
                    elif '@' in ioc and '.' in ioc:
                        entities['email'].append(ioc)
                    elif ioc.startswith(('http://', 'https://')):
                        entities['url'].append(ioc)
                    elif '.' in ioc and all(part.isdigit() for part in ioc.split('.')):
                        entities['ip'].append(ioc)
                    elif '.' in ioc and not '/' in ioc:
                        entities['domain'].append(ioc)
                    elif len(ioc) in [32, 40, 64, 128] and all(c in '0123456789abcdefABCDEF' for c in ioc):
                        entities['hash'].append(ioc)
                    # Check for threat actors mentioned in the extraction summary
                    elif any(keyword in ioc.lower() for keyword in ['apt', 'group', 'actor']):
                        entities['threat_actor'].append(ioc)
            
            # Extract threat actors from extraction_summary if available
            if hasattr(result, 'extraction_summary') and result.extraction_summary:
                summary_lower = result.extraction_summary.lower()
                # Look for APT groups
                import re
                apt_pattern = r'apt\d+'
                for match in re.finditer(apt_pattern, summary_lower):
                    apt_name = match.group().upper()
                    if apt_name not in entities['threat_actor']:
                        entities['threat_actor'].append(apt_name)
            
            # Remove empty entity types
            entities = {k: v for k, v in entities.items() if v}
            
            return {"entities": entities, "relations": []}
            
        except Exception as e:
            print(f"[DSPyExtraction] Extraction error: {e}", file=sys.stderr)
            # On failure return empty structure with error message
            return {"error": str(e), "entities": {}, "relations": []} 