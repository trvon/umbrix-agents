"""
Comprehensive tests for CTI Feed Tools - TAXII client and MISP feed processing.

Tests cover:
- TaxiiClientTool: TAXII 2.1 server discovery and object fetching
- MispFeedTool: MISP feed index retrieval and content parsing
- Various feed formats: CSV, JSON, raw text
- Error handling for network failures and invalid data
- Retry framework integration for external API calls
- Authentication handling for TAXII servers
"""

import pytest
import json
import csv
import io
from unittest.mock import Mock, patch, MagicMock
from requests.exceptions import RequestException, HTTPError

from common_tools.cti_feed_tools import TaxiiClientTool, MispFeedTool


class TestTaxiiClientTool:
    """Test suite for TaxiiClientTool class."""
    
    @pytest.fixture
    def taxii_tool(self):
        """Create TaxiiClientTool instance for testing."""
        return TaxiiClientTool()
    
    @pytest.fixture
    def mock_taxii_server(self):
        """Mock TAXII server structure for testing."""
        # Create mock collection
        mock_collection = Mock()
        mock_collection.id = "collection-123"
        mock_collection.title = "Test Collection"
        mock_collection.description = "Test collection for unit tests"
        
        # Create mock API root
        mock_api_root = Mock()
        mock_api_root.collections = [mock_collection]
        
        # Create mock server
        mock_server = Mock()
        mock_server.api_roots = [mock_api_root]
        
        return mock_server, mock_collection
    
    def test_tool_initialization(self, taxii_tool):
        """Test TaxiiClientTool initialization."""
        assert taxii_tool.name == "taxii_client"
        assert "TAXII 2.1 client tool" in taxii_tool.description
    
    @patch('common_tools.cti_feed_tools.Server')
    def test_discover_collections_basic(self, mock_server_class, taxii_tool, mock_taxii_server):
        """Test basic collection discovery without authentication."""
        mock_server, mock_collection = mock_taxii_server
        mock_server_class.return_value = mock_server
        
        server_url = "https://test-taxii.example.com"
        
        collections = taxii_tool.discover_collections(server_url)
        
        # Verify server was initialized correctly
        mock_server_class.assert_called_once_with(server_url, user=None, password=None)
        
        # Verify collections structure
        assert len(collections) == 1
        assert collections[0]["id"] == "collection-123"
        assert collections[0]["title"] == "Test Collection"
        assert collections[0]["description"] == "Test collection for unit tests"
    
    @patch('common_tools.cti_feed_tools.Server')
    def test_discover_collections_with_auth(self, mock_server_class, taxii_tool, mock_taxii_server):
        """Test collection discovery with authentication."""
        mock_server, mock_collection = mock_taxii_server
        mock_server_class.return_value = mock_server
        
        server_url = "https://secure-taxii.example.com"
        username = "test_user"
        password = "test_pass"
        
        collections = taxii_tool.discover_collections(server_url, username, password)
        
        # Verify server was initialized with credentials
        mock_server_class.assert_called_once_with(server_url, user=username, password=password)
        
        assert len(collections) == 1
        assert collections[0]["id"] == "collection-123"
    
    @patch('common_tools.cti_feed_tools.Server')
    def test_discover_collections_multiple(self, mock_server_class, taxii_tool):
        """Test discovery of multiple collections."""
        # Create multiple collections
        collections_data = [
            ("coll-1", "Collection One", "First collection"),
            ("coll-2", "Collection Two", "Second collection"),
            ("coll-3", "Collection Three", "")  # No description
        ]
        
        mock_collections = []
        for coll_id, title, desc in collections_data:
            mock_coll = Mock()
            mock_coll.id = coll_id
            mock_coll.title = title
            mock_coll.description = desc
            mock_collections.append(mock_coll)
        
        mock_api_root = Mock()
        mock_api_root.collections = mock_collections
        
        mock_server = Mock()
        mock_server.api_roots = [mock_api_root]
        mock_server_class.return_value = mock_server
        
        collections = taxii_tool.discover_collections("https://test.example.com")
        
        assert len(collections) == 3
        assert collections[0]["id"] == "coll-1"
        assert collections[1]["title"] == "Collection Two"
        assert collections[2]["description"] == ""
    
    @patch('common_tools.cti_feed_tools.Server')
    def test_discover_collections_missing_attributes(self, mock_server_class, taxii_tool):
        """Test discovery when collections are missing title/description attributes."""
        mock_collection = Mock()
        mock_collection.id = "basic-collection"
        # Remove title and description attributes
        del mock_collection.title
        del mock_collection.description
        
        mock_api_root = Mock()
        mock_api_root.collections = [mock_collection]
        
        mock_server = Mock()
        mock_server.api_roots = [mock_api_root]
        mock_server_class.return_value = mock_server
        
        collections = taxii_tool.discover_collections("https://test.example.com")
        
        assert len(collections) == 1
        assert collections[0]["id"] == "basic-collection"
        assert collections[0]["title"] == ""  # getattr default
        assert collections[0]["description"] == ""  # getattr default
    
    @patch('common_tools.cti_feed_tools.Server')
    @patch('common_tools.cti_feed_tools.retry_with_policy')
    def test_fetch_objects_basic(self, mock_retry, mock_server_class, taxii_tool, mock_taxii_server):
        """Test basic STIX object fetching."""
        mock_server, mock_collection = mock_taxii_server
        mock_server_class.return_value = mock_server
        
        # Mock the bundle response
        mock_bundle = {
            "objects": [
                {"type": "indicator", "id": "indicator--123", "pattern": "[file:hashes.MD5 = 'abc123']"},
                {"type": "malware", "id": "malware--456", "name": "TestMalware"}
            ]
        }
        mock_collection.get.return_value = mock_bundle
        
        # Mock retry decorator
        mock_retry.return_value = lambda func: func
        
        server_url = "https://test-taxii.example.com"
        collection_id = "collection-123"
        
        objects = taxii_tool.fetch_objects(server_url, collection_id)
        
        # Verify server and collection setup
        mock_server_class.assert_called_once_with(server_url, user=None, password=None)
        mock_collection.get.assert_called_once_with()
        
        # Verify objects
        assert len(objects) == 2
        assert objects[0]["type"] == "indicator"
        assert objects[1]["type"] == "malware"
    
    @patch('common_tools.cti_feed_tools.Server')
    @patch('common_tools.cti_feed_tools.retry_with_policy')
    def test_fetch_objects_with_auth_and_filter(self, mock_retry, mock_server_class, taxii_tool, mock_taxii_server):
        """Test STIX object fetching with authentication and timestamp filter."""
        mock_server, mock_collection = mock_taxii_server
        mock_server_class.return_value = mock_server
        
        mock_bundle = {"objects": [{"type": "indicator", "id": "indicator--789"}]}
        mock_collection.get.return_value = mock_bundle
        
        # Mock retry decorator
        mock_retry.return_value = lambda func: func
        
        server_url = "https://secure-taxii.example.com"
        collection_id = "collection-123"
        username = "api_user"
        password = "api_pass"
        added_after = "2023-10-27T00:00:00Z"
        
        objects = taxii_tool.fetch_objects(server_url, collection_id, username, password, added_after)
        
        # Verify authentication
        mock_server_class.assert_called_once_with(server_url, user=username, password=password)
        
        # Verify filter parameter
        mock_collection.get.assert_called_once_with(added_after=added_after)
        
        assert len(objects) == 1
        assert objects[0]["id"] == "indicator--789"
    
    @patch('common_tools.cti_feed_tools.Server')
    def test_fetch_objects_collection_not_found(self, mock_server_class, taxii_tool):
        """Test fetching from non-existent collection."""
        # Create server with different collection ID
        mock_collection = Mock()
        mock_collection.id = "different-collection"
        
        mock_api_root = Mock()
        mock_api_root.collections = [mock_collection]
        
        mock_server = Mock()
        mock_server.api_roots = [mock_api_root]
        mock_server_class.return_value = mock_server
        
        server_url = "https://test-taxii.example.com"
        collection_id = "nonexistent-collection"
        
        with pytest.raises(Exception) as exc_info:
            taxii_tool.fetch_objects(server_url, collection_id)
        
        assert "TAXII collection 'nonexistent-collection' not found" in str(exc_info.value)
    
    @patch('common_tools.cti_feed_tools.Server')
    @patch('common_tools.cti_feed_tools.retry_with_policy')
    def test_fetch_objects_empty_bundle(self, mock_retry, mock_server_class, taxii_tool, mock_taxii_server):
        """Test fetching when bundle contains no objects."""
        mock_server, mock_collection = mock_taxii_server
        mock_server_class.return_value = mock_server
        
        # Empty bundle
        mock_bundle = {"objects": []}
        mock_collection.get.return_value = mock_bundle
        
        # Mock retry decorator
        mock_retry.return_value = lambda func: func
        
        objects = taxii_tool.fetch_objects("https://test.example.com", "collection-123")
        
        assert objects == []
    
    @patch('common_tools.cti_feed_tools.Server')
    @patch('common_tools.cti_feed_tools.retry_with_policy')
    def test_fetch_objects_missing_objects_key(self, mock_retry, mock_server_class, taxii_tool, mock_taxii_server):
        """Test fetching when bundle is missing 'objects' key."""
        mock_server, mock_collection = mock_taxii_server
        mock_server_class.return_value = mock_server
        
        # Bundle without objects key
        mock_bundle = {"some_other_key": "value"}
        mock_collection.get.return_value = mock_bundle
        
        # Mock retry decorator
        mock_retry.return_value = lambda func: func
        
        objects = taxii_tool.fetch_objects("https://test.example.com", "collection-123")
        
        assert objects == []
    
    @patch('common_tools.cti_feed_tools.Server')
    @patch('common_tools.cti_feed_tools.retry_with_policy')
    def test_fetch_objects_retry_integration(self, mock_retry, mock_server_class, taxii_tool, mock_taxii_server):
        """Test that retry framework is properly integrated."""
        mock_server, mock_collection = mock_taxii_server
        mock_server_class.return_value = mock_server
        
        mock_bundle = {"objects": []}
        mock_collection.get.return_value = mock_bundle
        
        # Mock retry decorator to verify it's called
        mock_decorator = Mock()
        mock_decorator.return_value = lambda: mock_bundle
        mock_retry.return_value = mock_decorator
        
        taxii_tool.fetch_objects("https://test.example.com", "collection-123")
        
        # Verify retry policy was applied
        mock_retry.assert_called_once_with('external_apis')
        mock_decorator.assert_called_once()


class TestMispFeedTool:
    """Test suite for MispFeedTool class."""
    
    @pytest.fixture
    def misp_tool(self):
        """Create MispFeedTool instance for testing."""
        return MispFeedTool()
    
    @pytest.fixture
    def sample_feed_index(self):
        """Sample MISP feed index data for testing."""
        return [
            {
                "name": "malware-domains",
                "url": "https://feeds.example.com/malware-domains.csv",
                "format": "csv",
                "description": "Malicious domains feed"
            },
            {
                "name": "threat-indicators", 
                "url": "https://feeds.example.com/indicators.json",
                "format": "json",
                "description": "JSON threat indicators"
            },
            {
                "name": "raw-iocs",
                "url": "https://feeds.example.com/raw.txt",
                "format": "text",
                "description": "Raw IOC list"
            }
        ]
    
    def test_tool_initialization(self, misp_tool):
        """Test MispFeedTool initialization."""
        assert misp_tool.name == "misp_feed_tool"
        assert "Fetch and parse MISP OSINT feeds" in misp_tool.description
    
    @patch('common_tools.cti_feed_tools.requests.get')
    @patch('common_tools.cti_feed_tools.retry_with_policy')
    def test_get_feed_index_success(self, mock_retry, mock_get, misp_tool, sample_feed_index):
        """Test successful feed index retrieval."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = sample_feed_index
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Mock retry decorator
        mock_retry.return_value = lambda func: func
        
        index_url = "https://feeds.example.com/index.json"
        
        result = misp_tool.get_feed_index(index_url)
        
        # Verify request
        mock_get.assert_called_once_with(index_url, timeout=10)
        mock_response.raise_for_status.assert_called_once()
        
        # Verify result
        assert result == sample_feed_index
        assert len(result) == 3
        assert result[0]["name"] == "malware-domains"
    
    @patch('common_tools.cti_feed_tools.requests.get')
    @patch('common_tools.cti_feed_tools.retry_with_policy')
    def test_get_feed_index_retry_integration(self, mock_retry, mock_get, misp_tool):
        """Test that retry framework is properly integrated for index retrieval."""
        mock_response = Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        
        # Mock retry decorator to verify it's called
        mock_decorator = Mock()
        mock_decorator.return_value = mock_response
        mock_retry.return_value = mock_decorator
        
        misp_tool.get_feed_index("https://feeds.example.com/index.json")
        
        # Verify retry policy was applied
        mock_retry.assert_called_once_with('external_apis')
        mock_decorator.assert_called_once()
    
    @patch('common_tools.cti_feed_tools.requests.get')
    @patch('common_tools.cti_feed_tools.retry_with_policy')
    def test_get_feed_index_http_error(self, mock_retry, mock_get, misp_tool):
        """Test feed index retrieval with HTTP error."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        # Mock retry decorator
        mock_retry.return_value = lambda func: func
        
        with pytest.raises(HTTPError):
            misp_tool.get_feed_index("https://feeds.example.com/nonexistent.json")
    
    @patch('common_tools.cti_feed_tools.requests.get')
    @patch('common_tools.cti_feed_tools.retry_with_policy')
    def test_fetch_feed_success(self, mock_retry, mock_get, misp_tool):
        """Test successful feed content retrieval."""
        feed_content = "domain,category\nmalicious.com,malware\nphishing.net,phishing"
        
        mock_response = Mock()
        mock_response.text = feed_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Mock retry decorator
        mock_retry.return_value = lambda func: func
        
        feed_url = "https://feeds.example.com/domains.csv"
        
        result = misp_tool.fetch_feed(feed_url)
        
        # Verify request
        mock_get.assert_called_once_with(feed_url, timeout=10)
        mock_response.raise_for_status.assert_called_once()
        
        # Verify result
        assert result == feed_content
    
    @patch('common_tools.cti_feed_tools.requests.get')
    @patch('common_tools.cti_feed_tools.retry_with_policy') 
    def test_fetch_feed_retry_integration(self, mock_retry, mock_get, misp_tool):
        """Test that retry framework is properly integrated for feed fetching."""
        mock_response = Mock()
        mock_response.text = "test content"
        mock_response.raise_for_status.return_value = None
        
        # Mock retry decorator to verify it's called
        mock_decorator = Mock()
        mock_decorator.return_value = mock_response
        mock_retry.return_value = mock_decorator
        
        misp_tool.fetch_feed("https://feeds.example.com/test.txt")
        
        # Verify retry policy was applied
        mock_retry.assert_called_once_with('external_apis')
        mock_decorator.assert_called_once()
    
    @patch('common_tools.cti_feed_tools.requests.get')
    @patch('common_tools.cti_feed_tools.retry_with_policy')
    def test_fetch_feed_network_error(self, mock_retry, mock_get, misp_tool):
        """Test feed retrieval with network error."""
        mock_get.side_effect = RequestException("Connection timeout")
        
        # Mock retry decorator
        mock_retry.return_value = lambda func: func
        
        with pytest.raises(RequestException):
            misp_tool.fetch_feed("https://unreachable.example.com/feed.txt")
    
    def test_parse_feed_csv_format(self, misp_tool):
        """Test parsing CSV format feed."""
        index_entry = {"format": "csv"}
        csv_content = "domain,category,confidence\nmalicious.com,malware,high\nphishing.net,phishing,medium"
        
        result = misp_tool.parse_feed(index_entry, csv_content)
        
        assert len(result) == 2
        assert result[0]["domain"] == "malicious.com"
        assert result[0]["category"] == "malware"
        assert result[0]["confidence"] == "high"
        assert result[1]["domain"] == "phishing.net"
        assert result[1]["category"] == "phishing"
    
    def test_parse_feed_csv_empty(self, misp_tool):
        """Test parsing empty CSV feed."""
        index_entry = {"format": "csv"}
        csv_content = "domain,category\n"  # Headers only
        
        result = misp_tool.parse_feed(index_entry, csv_content)
        
        assert result == []
    
    def test_parse_feed_json_format(self, misp_tool):
        """Test parsing JSON format feed."""
        index_entry = {"format": "json"}
        json_content = json.dumps([
            {"type": "indicator", "value": "192.168.1.1", "category": "network"},
            {"type": "hash", "value": "abc123", "category": "file"}
        ])
        
        result = misp_tool.parse_feed(index_entry, json_content)
        
        assert len(result) == 2
        assert result[0]["type"] == "indicator"
        assert result[0]["value"] == "192.168.1.1"
        assert result[1]["type"] == "hash"
        assert result[1]["value"] == "abc123"
    
    def test_parse_feed_misp_json_format(self, misp_tool):
        """Test parsing MISP-JSON format feed."""
        index_entry = {"format": "misp-json"}
        misp_json_content = json.dumps({
            "Event": {
                "info": "Test event",
                "Attribute": [
                    {"type": "domain", "value": "evil.com"},
                    {"type": "ip-dst", "value": "1.2.3.4"}
                ]
            }
        })
        
        result = misp_tool.parse_feed(index_entry, misp_json_content)
        
        assert "Event" in result
        assert result["Event"]["info"] == "Test event"
        assert len(result["Event"]["Attribute"]) == 2
    
    def test_parse_feed_json_invalid(self, misp_tool):
        """Test parsing invalid JSON feed."""
        index_entry = {"format": "json"}
        invalid_json_content = "{ invalid json content"
        
        with pytest.raises(json.JSONDecodeError):
            misp_tool.parse_feed(index_entry, invalid_json_content)
    
    def test_parse_feed_text_fallback(self, misp_tool):
        """Test parsing text format (fallback) feed."""
        index_entry = {"format": "text"}
        text_content = "malicious.com\n192.168.1.1\n\nphishing.net\n"
        
        result = misp_tool.parse_feed(index_entry, text_content)
        
        assert len(result) == 3  # Empty line should be skipped
        assert result[0]["raw"] == "malicious.com"
        assert result[1]["raw"] == "192.168.1.1"
        assert result[2]["raw"] == "phishing.net"
    
    def test_parse_feed_unknown_format_fallback(self, misp_tool):
        """Test parsing unknown format falls back to text processing."""
        index_entry = {"format": "unknown"}
        text_content = "indicator1\nindicator2\nindicator3"
        
        result = misp_tool.parse_feed(index_entry, text_content)
        
        assert len(result) == 3
        assert result[0]["raw"] == "indicator1"
        assert result[1]["raw"] == "indicator2"
        assert result[2]["raw"] == "indicator3"
    
    def test_parse_feed_missing_format(self, misp_tool):
        """Test parsing when format field is missing."""
        index_entry = {}  # No format field
        text_content = "line1\nline2"
        
        result = misp_tool.parse_feed(index_entry, text_content)
        
        assert len(result) == 2
        assert result[0]["raw"] == "line1"
        assert result[1]["raw"] == "line2"
    
    def test_parse_feed_empty_content(self, misp_tool):
        """Test parsing empty feed content."""
        index_entry = {"format": "csv"}
        empty_content = ""
        
        result = misp_tool.parse_feed(index_entry, empty_content)
        
        assert result == []
    
    def test_parse_feed_whitespace_only_content(self, misp_tool):
        """Test parsing feed with only whitespace."""
        index_entry = {"format": "text"}
        whitespace_content = "\n   \n\t\n"
        
        result = misp_tool.parse_feed(index_entry, whitespace_content)
        
        assert result == []  # All lines should be stripped and filtered out


class TestCTIFeedToolsIntegration:
    """Integration tests for CTI feed tools."""
    
    @pytest.fixture
    def taxii_tool(self):
        return TaxiiClientTool()
    
    @pytest.fixture
    def misp_tool(self):
        return MispFeedTool()
    
    def test_tools_can_be_instantiated_together(self, taxii_tool, misp_tool):
        """Test that both tools can be instantiated and used together."""
        assert taxii_tool.name == "taxii_client"
        assert misp_tool.name == "misp_feed_tool"
        
        # Verify they have different names
        assert taxii_tool.name != misp_tool.name
    
    @patch('common_tools.cti_feed_tools.Server')
    def test_taxii_tool_error_handling(self, mock_server_class, taxii_tool):
        """Test TAXII tool error handling."""
        mock_server_class.side_effect = Exception("TAXII server connection failed")
        
        with pytest.raises(Exception) as exc_info:
            taxii_tool.discover_collections("https://invalid-server.com")
        
        assert "TAXII server connection failed" in str(exc_info.value)
    
    def test_misp_tool_csv_with_special_characters(self, misp_tool):
        """Test MISP tool handling CSV with special characters."""
        index_entry = {"format": "csv"}
        csv_content = 'domain,note\n"evil,domain.com","Contains, comma"\n"quote""domain.com","Contains "" quote"'
        
        result = misp_tool.parse_feed(index_entry, csv_content)
        
        assert len(result) == 2
        assert result[0]["domain"] == "evil,domain.com"
        assert result[0]["note"] == "Contains, comma"
        assert result[1]["domain"] == 'quote"domain.com'
        assert result[1]["note"] == 'Contains " quote'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])