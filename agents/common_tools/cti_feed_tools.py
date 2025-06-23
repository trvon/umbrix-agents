from google.adk.tools import BaseTool
from taxii2client.v21 import Server
from typing import Optional, List
import requests
import csv
import io
import json
from .retry_framework import retry_with_policy


class TaxiiClientTool(BaseTool):
    """
    Tool to interact with TAXII 2.1 servers: discover collections and fetch STIX objects.
    """
    def __init__(self):
        super().__init__(
            name="taxii_client",
            description="TAXII 2.1 client tool for discovering collections and fetching STIX objects"
        )

    def discover_collections(
        self,
        server_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None
    ) -> List[dict]:
        """
        Discover available collections on a TAXII server.
        Returns a list of dicts with keys: id, title, description.
        """
        server = Server(server_url, user=username, password=password)
        collections = []
        for api_root in server.api_roots:
            for coll in api_root.collections:
                collections.append({
                    "id": coll.id,
                    "title": getattr(coll, "title", ""),
                    "description": getattr(coll, "description", "")
                })
        return collections

    def fetch_objects(
        self,
        server_url: str,
        collection_id: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        added_after: Optional[str] = None
    ) -> List[dict]:
        """
        Fetch STIX objects from a specific collection. Optionally filter by added_after timestamp.
        Returns a list of STIX object dicts.
        """
        server = Server(server_url, user=username, password=password)
        api_root = server.api_roots[0]
        target_coll = None
        for coll in api_root.collections:
            if coll.id == collection_id:
                target_coll = coll
                break
        if not target_coll:
            raise Exception(f"TAXII collection '{collection_id}' not found on {server_url}")

        # Prepare parameters
        params = {}
        if added_after:
            params["added_after"] = added_after

        # Fetch bundle of objects
        # Standardized retry for external APIs (TAXII)
        decorated_fetch = retry_with_policy('external_apis')(lambda: target_coll.get(**params))
        bundle = decorated_fetch()
        objects = bundle.get("objects", [])

        # TODO: implement pagination if bundle.get('next') is provided
        return objects

class MispFeedTool(BaseTool):
    """
    Tool to fetch and parse MISP OSINT feeds.
    """
    def __init__(self):
        super().__init__(
            name="misp_feed_tool",
            description="Fetch and parse MISP OSINT feeds and IOCs"
        )

    def get_feed_index(self, index_url: str) -> List[dict]:
        """Fetch the MISP feed index JSON and return list of feed entries."""
        # Standardized retry for external APIs (MISP index)
        decorated_get = retry_with_policy('external_apis')(lambda: requests.get(index_url, timeout=10))
        response = decorated_get()
        response.raise_for_status()
        return response.json()

    def fetch_feed(self, feed_url: str) -> str:
        """Fetch raw feed content (CSV, JSON, or text)."""
        # Standardized retry for external APIs (MISP feed)
        decorated_get = retry_with_policy('external_apis')(lambda: requests.get(feed_url, timeout=10))
        response = decorated_get()
        response.raise_for_status()
        return response.text

    def parse_feed(self, index_entry: dict, content: str) -> List[dict]:
        """Parse feed content based on declared format (CSV, JSON, or fallback)."""
        fmt = index_entry.get('format', '').lower()
        if fmt == 'csv':
            reader = csv.DictReader(io.StringIO(content))
            return list(reader)
        elif fmt in ('json', 'misp-json'):
            return json.loads(content)
        else:
            # fallback: treat each non-empty line as raw IOC entry
            return [{'raw': line} for line in content.splitlines() if line.strip()] 