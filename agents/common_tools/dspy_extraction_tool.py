import json
from google.adk.tools import FunctionTool as Tool
import dspy

class DSPyExtractionTool(Tool):
    """Tool that uses DSPy to extract entities and relations from arbitrary text."""

    # Provide Config for loader to check extra behavior
    class Config:
        extra = "allow"

    def __init__(self, *args, **kwargs):
        # Accept loader-provided arguments (e.g., name) but wrap only the extract method
        super().__init__(func=self.extract)

    def extract(self, text: str, patterns: list[str] | None = None) -> dict:
        """Extract entities/relations from text.

        Args:
            text: Raw text content.
            patterns: Optional list of patterns or entity types to extract.
        Returns:
            dict with keys `entities` and optionally `relations`.
        """
        try:
            result = dspy.extract(text, patterns=patterns or [])
            # Ensure JSON serializable
            return json.loads(json.dumps(result))
        except Exception as e:
            # On failure return empty structure with error message
            return {"error": str(e), "entities": {}, "relations": []} 