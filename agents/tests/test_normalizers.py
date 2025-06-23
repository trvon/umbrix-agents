"""
Tests for data normalization functionality.
"""
import pytest
from pydantic import AnyUrl

from common_tools.normalizers import FeedDataNormalizer
from common_tools.models.feed_record import FeedRecord


class TestFeedDataNormalizer:
    """Test FeedDataNormalizer functionality."""
    
    def test_normalize_url_basic(self):
        """Test basic URL normalization."""
        # Test adding default scheme
        result = FeedDataNormalizer.normalize_url("example.com/path")
        assert result == "https://example.com/path"
        
        # Test lowercasing domain
        result = FeedDataNormalizer.normalize_url("https://EXAMPLE.COM/Path")
        assert result == "https://example.com/Path"
        
        # Test keeping existing scheme
        result = FeedDataNormalizer.normalize_url("http://example.com/path")
        assert result == "http://example.com/path"
    
    def test_normalize_url_none(self):
        """Test URL normalization with None input."""
        result = FeedDataNormalizer.normalize_url(None)
        assert result is None
    
    def test_normalize_url_empty_string(self):
        """Test URL normalization with empty string."""
        result = FeedDataNormalizer.normalize_url("")
        assert result is None
    
    def test_normalize_url_tracking_parameters(self):
        """Test removal of tracking parameters."""
        # UTM parameters should be removed
        result = FeedDataNormalizer.normalize_url(
            "https://example.com/article?utm_source=google&utm_medium=cpc&param=keep"
        )
        assert "utm_source" not in result
        assert "utm_medium" not in result
        assert "param=keep" in result
        
        # Facebook and Google click IDs should be removed
        result = FeedDataNormalizer.normalize_url(
            "https://example.com/article?fbclid=123&gclid=456&keep=this"
        )
        assert "fbclid" not in result
        assert "gclid" not in result
        assert "keep=this" in result
    
    def test_normalize_url_fragment_removal(self):
        """Test removal of URL fragments."""
        result = FeedDataNormalizer.normalize_url("https://example.com/path#fragment")
        assert result == "https://example.com/path"
    
    def test_normalize_url_complex(self):
        """Test complex URL normalization with multiple components."""
        original = "EXAMPLE.COM/path?utm_source=twitter&keep=this&fbclid=abc#section"
        result = FeedDataNormalizer.normalize_url(original)
        
        assert result.startswith("https://")
        assert "example.com" in result
        assert "/path" in result
        assert "keep=this" in result
        assert "utm_source" not in result
        assert "fbclid" not in result
        assert "#section" not in result
    
    def test_normalize_url_malformed(self):
        """Test URL normalization with malformed URLs."""
        # URLs without schemes get https:// prepended
        malformed_url = "not-a-valid-url"
        result = FeedDataNormalizer.normalize_url(malformed_url)
        assert result == "https://not-a-valid-url"
    
    def test_normalize_text_field_basic(self):
        """Test basic text field normalization."""
        # Test trimming whitespace
        result = FeedDataNormalizer.normalize_text_field("  Hello World  ")
        assert result == "Hello World"
        
        # Test None input
        result = FeedDataNormalizer.normalize_text_field(None)
        assert result is None
        
        # Test empty string
        result = FeedDataNormalizer.normalize_text_field("")
        assert result == ""
        
        # Test already clean text
        result = FeedDataNormalizer.normalize_text_field("Clean text")
        assert result == "Clean text"
    
    def test_normalize_tags_basic(self):
        """Test basic tag normalization."""
        # Test normalization and deduplication
        result = FeedDataNormalizer.normalize_tags(["Security", "MALWARE", "security", "  Phishing  "])
        expected = ["malware", "phishing", "security"]  # sorted and deduplicated
        assert result == expected
    
    def test_normalize_tags_empty(self):
        """Test tag normalization with empty input."""
        # Test empty list
        result = FeedDataNormalizer.normalize_tags([])
        assert result == []
        
        # Test None
        result = FeedDataNormalizer.normalize_tags(None)
        assert result == []
    
    def test_normalize_tags_mixed_types(self):
        """Test tag normalization with mixed types."""
        # Only strings should be processed
        result = FeedDataNormalizer.normalize_tags(["Valid", 123, None, "  Another  "])
        expected = ["another", "valid"]
        assert result == expected
    
    def test_normalize_feed_record_complete(self):
        """Test complete feed record normalization."""
        # Create a feed record with various fields to normalize
        record = FeedRecord(
            url=AnyUrl("https://EXAMPLE.COM/article?utm_source=twitter&keep=param"),
            title="  Article Title  ",
            description="  Description text  ",
            source_name="  Source Name  ",
            vendor_name="  Vendor Name  ",
            tags=["Security", "MALWARE", "security"]
        )
        
        # Normalize the record
        result = FeedDataNormalizer.normalize_feed_record(record)
        
        # Verify URL normalization
        assert str(result.url).startswith("https://example.com")
        assert "utm_source" not in str(result.url)
        assert "keep=param" in str(result.url)
        
        # Verify text field normalization
        assert result.title == "Article Title"
        assert result.description == "Description text"
        assert result.source_name == "Source Name"
        assert result.vendor_name == "Vendor Name"
        
        # Verify tag normalization
        expected_tags = ["malware", "security"]
        assert result.tags == expected_tags
        
        # Verify the same record instance is returned (in-place modification)
        assert result is record
    
    def test_normalize_feed_record_minimal(self):
        """Test feed record normalization with minimal data."""
        # Create record with only required URL field
        record = FeedRecord(url=AnyUrl("https://EXAMPLE.COM"))
        
        result = FeedDataNormalizer.normalize_feed_record(record)
        
        # URL should be normalized (lowercased, may have trailing slash)
        assert str(result.url) == "https://example.com/"
        
        # Other fields should remain as defaults
        assert result.title is None
        assert result.description is None
        assert result.tags == []
    
    def test_normalize_feed_record_none_fields(self):
        """Test feed record normalization with None fields."""
        record = FeedRecord(
            url=AnyUrl("https://example.com"),
            title=None,
            description=None,
            source_name=None,
            vendor_name=None,
            tags=[]
        )
        
        result = FeedDataNormalizer.normalize_feed_record(record)
        
        # None fields should remain None
        assert result.title is None
        assert result.description is None
        assert result.source_name is None
        assert result.vendor_name is None
        assert result.tags == []
    
    def test_normalize_feed_records_list(self):
        """Test normalizing a list of feed records."""
        records = [
            FeedRecord(
                url=AnyUrl("https://EXAMPLE.COM/article1"),
                title="  Title 1  ",
                tags=["TAG1", "tag2"]
            ),
            FeedRecord(
                url=AnyUrl("https://ANOTHER.COM/article2"),
                title="  Title 2  ",
                tags=["TAG2", "tag3"]
            )
        ]
        
        result = FeedDataNormalizer.normalize_feed_records(records)
        
        # Should return same number of records
        assert len(result) == 2
        
        # Verify first record normalization
        assert str(result[0].url).startswith("https://example.com")
        assert result[0].title == "Title 1"
        assert result[0].tags == ["tag1", "tag2"]
        
        # Verify second record normalization
        assert str(result[1].url).startswith("https://another.com")
        assert result[1].title == "Title 2"
        assert result[1].tags == ["tag2", "tag3"]
    
    def test_normalize_feed_records_empty_list(self):
        """Test normalizing an empty list."""
        result = FeedDataNormalizer.normalize_feed_records([])
        assert result == []