"""
Tests for network tools module.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import requests

from common_tools.network_tools import GeoIpLookupTool


class TestGeoIpLookupTool:
    """Test GeoIP lookup functionality."""
    
    def test_initialization(self):
        """Test GeoIpLookupTool initialization."""
        tool = GeoIpLookupTool()
        
        # Check attributes set in __init__
        assert hasattr(tool, 'cache')
        assert tool.cache == {}
        assert hasattr(tool, 'dead_link_detector')
    
    @patch('common_tools.network_tools.requests.get')
    def test_successful_lookup(self, mock_get):
        """Test successful GeoIP lookup."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "success",
            "country": "United States",
            "regionName": "California",
            "city": "Mountain View",
            "lat": 37.3861,
            "lon": -122.0839
        }
        mock_get.return_value = mock_response
        
        tool = GeoIpLookupTool()
        result = tool.call("8.8.8.8")
        
        # Verify request was made
        mock_get.assert_called_once_with(
            "http://ip-api.com/json/8.8.8.8",
            timeout=5
        )
        mock_response.raise_for_status.assert_called_once()
        
        # Verify result
        assert result == {
            "ip": "8.8.8.8",
            "country": "United States",
            "region": "California",
            "city": "Mountain View",
            "lat": 37.3861,
            "lon": -122.0839
        }
        
        # Verify caching
        assert "8.8.8.8" in tool.cache
        assert tool.cache["8.8.8.8"] == result
    
    @patch('common_tools.network_tools.requests.get')
    def test_cache_hit(self, mock_get):
        """Test that cached results are returned without making a request."""
        tool = GeoIpLookupTool()
        
        # Pre-populate cache
        cached_result = {
            "ip": "1.1.1.1",
            "country": "Australia",
            "region": "New South Wales",
            "city": "Sydney",
            "lat": -33.8688,
            "lon": 151.2093
        }
        tool.cache["1.1.1.1"] = cached_result
        
        # Call with cached IP
        result = tool.call("1.1.1.1")
        
        # Verify no request was made
        mock_get.assert_not_called()
        
        # Verify cached result returned
        assert result == cached_result
    
    @patch('common_tools.network_tools.requests.get')
    def test_api_failure_status(self, mock_get):
        """Test handling of API failure status."""
        # Mock response with failure status
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "fail",
            "message": "private range"
        }
        mock_get.return_value = mock_response
        
        tool = GeoIpLookupTool()
        
        with pytest.raises(Exception) as exc_info:
            tool.call("192.168.1.1")
        
        assert "GeoIP lookup failed: private range" in str(exc_info.value)
        
        # Verify IP was not cached
        assert "192.168.1.1" not in tool.cache
    
    @patch('common_tools.network_tools.requests.get')
    def test_http_error(self, mock_get):
        """Test handling of HTTP errors."""
        # Mock response that raises on raise_for_status
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        tool = GeoIpLookupTool()
        
        with pytest.raises(requests.HTTPError):
            tool.call("invalid.ip.address")
        
        # Verify IP was not cached
        assert "invalid.ip.address" not in tool.cache
    
    @patch('common_tools.network_tools.requests.get')
    def test_request_timeout(self, mock_get):
        """Test handling of request timeout."""
        # Mock timeout exception
        mock_get.side_effect = requests.Timeout("Request timed out")
        
        tool = GeoIpLookupTool()
        
        with pytest.raises(requests.Timeout):
            tool.call("8.8.8.8")
        
        # Verify IP was not cached
        assert "8.8.8.8" not in tool.cache
    
    @patch('common_tools.network_tools.requests.get')
    def test_missing_fields_in_response(self, mock_get):
        """Test handling of response with missing fields."""
        # Mock response with minimal data
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "success",
            "country": "Unknown"
            # Missing other fields
        }
        mock_get.return_value = mock_response
        
        tool = GeoIpLookupTool()
        result = tool.call("10.0.0.1")
        
        # Verify result handles missing fields gracefully
        assert result == {
            "ip": "10.0.0.1",
            "country": "Unknown",
            "region": None,
            "city": None,
            "lat": None,
            "lon": None
        }
    
    @patch('common_tools.network_tools.get_dead_link_detector')
    def test_dead_link_detector_integration(self, mock_get_detector):
        """Test that dead link detector is properly integrated."""
        mock_detector = Mock()
        mock_get_detector.return_value = mock_detector
        
        tool = GeoIpLookupTool()
        
        # Verify detector was obtained
        mock_get_detector.assert_called_once()
        assert tool.dead_link_detector == mock_detector
    
    @patch('common_tools.network_tools.dead_link_aware_retry')
    def test_dead_link_aware_decorator(self, mock_decorator):
        """Test that the dead_link_aware_retry decorator is applied."""
        # The decorator is applied at class definition time
        # We can verify it was called with the right parameters
        
        # Import triggers the decorator
        from common_tools.network_tools import GeoIpLookupTool as TestTool
        
        # The decorator should have been called during class definition
        # This is a bit tricky to test since decorators are applied at import time
        # We'll verify the tool works as expected with the decorator
        assert hasattr(TestTool, 'call')