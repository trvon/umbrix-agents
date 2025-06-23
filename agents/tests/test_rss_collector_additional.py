"""
Additional focused tests for rss_collector.py to improve coverage from 40% to 70%.

This file focuses on key missing coverage areas with simpler, more targeted tests.
"""

import pytest
import sys
import os
import tempfile
import yaml
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock

# Import the module under test
from collector_agent.rss_collector import (
    parse_feed_date_to_datetime,
    RssCollectorAgent
)


class TestDateParsing:
    """Test date parsing functionality."""
    
    def test_parse_feed_date_valid_formats(self):
        """Test parsing various valid date formats."""
        test_cases = [
            "2023-10-26T10:20:30Z",
            "2023-10-26T10:20:30+00:00",
            "2023-10-26T10:20:30-05:00"
        ]
        
        for date_str in test_cases:
            result = parse_feed_date_to_datetime(date_str)
            assert result is not None
            assert isinstance(result, datetime)
    
    def test_parse_feed_date_invalid_formats(self):
        """Test parsing invalid date formats."""
        # This tests the ValueError handling and return None paths
        invalid_dates = [
            "invalid-date-string",
            "2023-13-32T25:70:80Z",
            "not a date at all",
            "2023/10/26 10:20:30"  # Wrong format
        ]
        
        for invalid_date in invalid_dates:
            result = parse_feed_date_to_datetime(invalid_date)
            assert result is None
    
    def test_parse_feed_date_empty_input(self):
        """Test parsing empty or None input."""
        # This tests lines 138-139
        assert parse_feed_date_to_datetime("") is None
        assert parse_feed_date_to_datetime(None) is None


class TestConfigurationHandling:
    """Test configuration file handling."""
    
    def test_yaml_parsing_error_handling(self, monkeypatch):
        """Test YAML parsing error handling during module load."""
        # This tests lines 58-59
        
        # Create invalid YAML file
        invalid_yaml = """
rate_limiting:
  default_request_delay_seconds: 2.0
  - invalid: yaml: structure
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(invalid_yaml)
            temp_config_path = f.name
        
        try:
            # Set environment variable to point to invalid YAML
            monkeypatch.setenv("AGENT_CONFIG_PATH", temp_config_path)
            
            with patch('builtins.print') as mock_print:
                # Re-import the module to trigger config loading
                import importlib
                import collector_agent.rss_collector
                importlib.reload(collector_agent.rss_collector)
                
                # Check that error was printed
                error_calls = [call for call in mock_print.call_args_list 
                             if "Error parsing YAML" in str(call)]
                assert len(error_calls) > 0
                
        finally:
            # Clean up
            os.unlink(temp_config_path)


class TestRssCollectorInitialization:
    """Test RssCollectorAgent initialization scenarios."""
    
    @patch('collector_agent.rss_collector.setup_agent_logging')
    @patch('collector_agent.rss_collector.setup_agent_metrics')
    @patch('collector_agent.rss_collector.RedisDedupeStore')
    def test_basic_initialization_success(self, mock_redis, mock_metrics, mock_logging):
        """Test successful basic initialization."""
        # This tests the main initialization path
        mock_logging.return_value = Mock()
        mock_metrics.return_value = Mock()
        mock_redis.return_value = Mock()
        
        agent = RssCollectorAgent(
            bootstrap_servers="localhost:9092",
            use_kafka=False,  # Disable Kafka for simplicity
            feeds=['http://example.com/feed.xml']
        )
        
        # Should have initialized basic attributes
        assert agent.feeds == ['http://example.com/feed.xml']
        assert agent.producer is None  # Should be None when use_kafka=False
        assert agent.consumer is None
        
        # Should have called setup functions
        mock_logging.assert_called_once()
        mock_metrics.assert_called_once()
    
    @patch('collector_agent.rss_collector.setup_agent_logging')
    @patch('collector_agent.rss_collector.setup_agent_metrics')
    @patch('collector_agent.rss_collector.RedisDedupeStore')
    @patch('collector_agent.rss_collector.start_http_server')
    def test_initialization_with_prometheus(self, mock_server, mock_redis, mock_metrics, mock_logging):
        """Test initialization with Prometheus server."""
        # This tests lines 335-343 (Prometheus server start)
        mock_logging.return_value = Mock()
        mock_metrics.return_value = Mock() 
        mock_redis.return_value = Mock()
        
        agent = RssCollectorAgent(
            bootstrap_servers="ignored",
            use_kafka=False,
            prometheus_port=8000
        )
        
        # Should have started Prometheus server
        mock_server.assert_called_once()
    
    @patch('collector_agent.rss_collector.setup_agent_logging')
    @patch('collector_agent.rss_collector.setup_agent_metrics')
    @patch('collector_agent.rss_collector.RedisDedupeStore')
    def test_topic_placeholder_resolution(self, mock_redis, mock_metrics, mock_logging):
        """Test topic placeholder resolution."""
        # This tests lines 290-299 (topic placeholder handling)
        mock_logger = Mock()
        mock_logging.return_value = mock_logger
        mock_metrics.return_value = Mock()
        mock_redis.return_value = Mock()
        
        # Test with placeholder topic name
        agent = RssCollectorAgent(
            bootstrap_servers="ignored",
            use_kafka=False,
            topic="${kafka_topics.feeds_discovered}"
        )
        
        # Should have logged a warning about placeholder resolution
        mock_logger.warning.assert_called()
        warning_call = mock_logger.warning.call_args
        assert "placeholder topic name" in str(warning_call)


class TestRunOnceEdgeCases:
    """Test run_once method edge cases."""
    
    @patch('collector_agent.rss_collector.setup_agent_logging')
    @patch('collector_agent.rss_collector.setup_agent_metrics')
    @patch('collector_agent.rss_collector.RedisDedupeStore')
    def test_run_once_no_feeds(self, mock_redis, mock_metrics, mock_logging):
        """Test run_once with no feeds to process."""
        # This tests lines 164-166
        mock_logging.return_value = Mock()
        mock_metrics.return_value = Mock()
        mock_redis.return_value = Mock()
        
        agent = RssCollectorAgent(
            bootstrap_servers="ignored",
            use_kafka=False,
            feeds=[]  # No feeds
        )
        
        with patch('builtins.print') as mock_print:
            agent.run_once()
            
            # Should have printed no feeds message
            no_feeds_calls = [call for call in mock_print.call_args_list 
                            if "No feeds to process" in str(call)]
            assert len(no_feeds_calls) > 0
    
    @patch('collector_agent.rss_collector.setup_agent_logging')
    @patch('collector_agent.rss_collector.setup_agent_metrics')
    @patch('collector_agent.rss_collector.RedisDedupeStore')
    def test_run_once_feed_fetch_error(self, mock_redis, mock_metrics, mock_logging):
        """Test run_once when feed fetching fails."""
        # This tests lines 250-255 (feed fetch error handling)
        mock_logger = Mock()
        mock_logging.return_value = mock_logger
        mock_metrics_obj = Mock()
        mock_metrics.return_value = mock_metrics_obj
        mock_redis.return_value = Mock()
        
        agent = RssCollectorAgent(
            bootstrap_servers="ignored",
            use_kafka=False,
            feeds=['http://example.com/feed.xml']
        )
        
        # Mock fetcher to raise exception
        agent.fetcher = Mock()
        agent.fetcher.call = Mock(side_effect=Exception("Network error"))
        
        # Mock metrics function to avoid None errors
        with patch('collector_agent.rss_collector._get_or_create_rss_metrics') as mock_get_metrics:
            mock_counters = [Mock() for _ in range(6)]  # 6 metrics returned
            mock_get_metrics.return_value = tuple(mock_counters)
            
            with patch('builtins.print') as mock_print:
                agent.run_once()
                
                # Should have printed error message
                error_calls = [call for call in mock_print.call_args_list 
                             if "Error fetching feed" in str(call)]
                assert len(error_calls) > 0
                
                # Should have incremented error counter
                mock_counters[1].inc.assert_called()  # feed_fetch_error_counter
    
    @patch('collector_agent.rss_collector.setup_agent_logging')
    @patch('collector_agent.rss_collector.setup_agent_metrics')
    @patch('collector_agent.rss_collector.RedisDedupeStore')
    def test_run_once_entry_processing_error(self, mock_redis, mock_metrics, mock_logging):
        """Test run_once when entry processing fails."""
        # This tests lines 247-249 (entry processing error)
        mock_logging.return_value = Mock()
        mock_metrics_obj = Mock()
        mock_metrics.return_value = mock_metrics_obj
        mock_redis.return_value = Mock()
        
        agent = RssCollectorAgent(
            bootstrap_servers="ignored",
            use_kafka=False,
            feeds=['http://example.com/feed.xml']
        )
        
        # Mock fetcher to return entry with valid link
        agent.fetcher = Mock()
        agent.fetcher.call = Mock(return_value=[
            {'id': 'test', 'link': 'http://example.com/article', 'title': 'Test'}
        ])
        
        # Mock extractor to raise exception during extraction
        agent.extractor = Mock()
        agent.extractor.call = Mock(side_effect=Exception("Extraction failed"))
        
        # Mock producer to avoid None errors
        agent.producer = Mock()
        
        # Mock metrics function
        with patch('collector_agent.rss_collector._get_or_create_rss_metrics') as mock_get_metrics:
            mock_counters = [Mock() for _ in range(6)]
            mock_get_metrics.return_value = tuple(mock_counters)
            
            with patch('builtins.print') as mock_print:
                agent.run_once()
                
                # Should have printed error message for entry processing
                error_calls = [call for call in mock_print.call_args_list 
                             if "Error processing entry" in str(call)]
                assert len(error_calls) > 0


class TestErrorPublishing:
    """Test error publishing functionality with simpler setup."""
    
    @patch('collector_agent.rss_collector.setup_agent_logging')
    @patch('collector_agent.rss_collector.setup_agent_metrics')
    @patch('collector_agent.rss_collector.RedisDedupeStore')
    def test_publish_error_basic_functionality(self, mock_redis, mock_metrics, mock_logging):
        """Test basic error publishing functionality."""
        # This tests the _publish_error_to_kafka method
        mock_logging.return_value = Mock()
        mock_metrics.return_value = Mock()
        mock_redis.return_value = Mock()
        
        agent = RssCollectorAgent(
            bootstrap_servers="ignored",
            use_kafka=False,
            name="test_agent"  # Pass name explicitly
        )
        
        # Explicitly set name attribute for the test
        agent.name = "test_agent"
        
        # Mock producer
        mock_producer = Mock()
        agent.producer = mock_producer
        
        # Mock time.strftime for consistent timestamp
        with patch('time.strftime', return_value="2023-10-26T10:20:30Z"):
            agent._publish_error_to_kafka(
                error_type="TestError",
                source_url="http://example.com/article",
                feed_url="http://example.com/feed.xml",
                severity="ERROR",
                details={"test": "data"}
            )
        
        # Should have called producer.send
        mock_producer.send.assert_called_once()
        
        # Verify the message structure
        call_args = mock_producer.send.call_args
        topic = call_args[0][0]
        message = call_args[0][1]
        
        assert topic == agent.agent_errors_topic
        assert message["error_type"] == "TestError"
        assert message["source_url"] == "http://example.com/article"
        assert message["feed_url"] == "http://example.com/feed.xml"
        assert message["severity"] == "ERROR"
        assert message["timestamp"] == "2023-10-26T10:20:30Z"
        assert message["details"] == {"test": "data"}
    
    @patch('collector_agent.rss_collector.setup_agent_logging')  
    @patch('collector_agent.rss_collector.setup_agent_metrics')
    @patch('collector_agent.rss_collector.RedisDedupeStore')
    def test_publish_error_kafka_failure(self, mock_redis, mock_metrics, mock_logging):
        """Test error publishing when Kafka send fails."""
        # This tests lines 430-431 (Kafka send failure)
        mock_logging.return_value = Mock()
        mock_metrics.return_value = Mock()
        mock_redis.return_value = Mock()
        
        agent = RssCollectorAgent(
            bootstrap_servers="ignored",
            use_kafka=False,
            name="test_agent"  # Pass name explicitly
        )
        
        # Explicitly set name attribute for the test
        agent.name = "test_agent"
        
        # Mock producer that fails
        mock_producer = Mock()
        mock_producer.send.side_effect = Exception("Kafka connection lost")
        agent.producer = mock_producer
        
        with patch('builtins.print') as mock_print:
            agent._publish_error_to_kafka(
                error_type="TestError",
                source_url="http://example.com/article"
            )
            
            # Should have printed critical error message
            critical_calls = [call for call in mock_print.call_args_list 
                            if "CRITICAL: Failed to publish error" in str(call)]
            assert len(critical_calls) > 0


class TestRunMethodBasics:
    """Test basic run method functionality with minimal mocking."""
    
    @patch('collector_agent.rss_collector.setup_agent_logging')
    @patch('collector_agent.rss_collector.setup_agent_metrics')
    @patch('collector_agent.rss_collector.RedisDedupeStore')
    def test_run_method_basic_structure(self, mock_redis, mock_metrics, mock_logging):
        """Test basic run method structure and setup."""
        # This tests lines 434-435 (basic run method setup)
        mock_logging.return_value = Mock()
        mock_metrics.return_value = Mock()
        mock_redis.return_value = Mock()
        
        agent = RssCollectorAgent(
            bootstrap_servers="ignored",
            use_kafka=False
        )
        
        # Mock consumer with no messages (empty iterator)
        mock_consumer = Mock()
        mock_consumer.__iter__ = Mock(return_value=iter([]))
        agent.consumer = mock_consumer
        
        # Mock global metrics to avoid None errors
        with patch('collector_agent.rss_collector.FEED_FETCH_COUNTER', Mock()):
            with patch('builtins.print') as mock_print:
                try:
                    agent.run()
                except StopIteration:
                    pass
                
                # Should have printed listening message
                listening_calls = [call for call in mock_print.call_args_list 
                                 if "Listening for feed URLs" in str(call)]
                assert len(listening_calls) > 0
    
    @patch('collector_agent.rss_collector.setup_agent_logging')
    @patch('collector_agent.rss_collector.setup_agent_metrics')
    @patch('collector_agent.rss_collector.RedisDedupeStore')
    def test_run_method_with_string_url_message(self, mock_redis, mock_metrics, mock_logging):
        """Test run method processing string URL message."""
        # This tests lines 463-474 (string URL processing)
        mock_logging.return_value = Mock()
        mock_metrics.return_value = Mock()
        mock_redis.return_value = Mock()
        
        agent = RssCollectorAgent(
            bootstrap_servers="ignored",
            use_kafka=False
        )
        agent.name = "test_agent"
        
        # Mock consumer with string URL message
        mock_message = Mock()
        mock_message.value = "http://example.com/feed.xml"
        mock_consumer = Mock()
        mock_consumer.__iter__ = Mock(return_value=iter([mock_message]))
        agent.consumer = mock_consumer
        
        # Mock fetcher that returns empty entries
        agent.fetcher = Mock()
        agent.fetcher.call = Mock(return_value=[])
        
        # Mock producer
        agent.producer = Mock()
        
        # Mock all global metrics
        with patch('collector_agent.rss_collector.FEED_FETCH_COUNTER', Mock()) as mock_fetch_counter:
            with patch('collector_agent.rss_collector.ENTRIES_RETRIEVED_COUNTER', Mock()) as mock_entries_counter:
                try:
                    agent.run()
                except StopIteration:
                    pass
                
                # Should have called fetcher with the URL
                agent.fetcher.call.assert_called_with("http://example.com/feed.xml")
                # Should have incremented fetch counter
                mock_fetch_counter.inc.assert_called()


class TestValidationErrorHandling:
    """Test validation error handling scenarios."""
    
    @patch('collector_agent.rss_collector.setup_agent_logging')
    @patch('collector_agent.rss_collector.setup_agent_metrics')
    @patch('collector_agent.rss_collector.RedisDedupeStore')
    def test_pydantic_validation_error_in_run(self, mock_redis, mock_metrics, mock_logging):
        """Test Pydantic validation error during run method."""
        # This tests lines 568-571 (Pydantic validation error)
        mock_logging.return_value = Mock()
        mock_metrics.return_value = Mock()
        mock_redis.return_value = Mock()
        
        agent = RssCollectorAgent(
            bootstrap_servers="ignored",
            use_kafka=False
        )
        agent.name = "test_agent"
        
        # Mock consumer with valid URL
        mock_message = Mock()
        mock_message.value = "http://example.com/feed.xml"
        mock_consumer = Mock()
        mock_consumer.__iter__ = Mock(return_value=iter([mock_message]))
        agent.consumer = mock_consumer
        
        # Mock fetcher that returns entries
        agent.fetcher = Mock()
        agent.fetcher.call = Mock(return_value=[
            {"id": "test_id", "link": "invalid-url-format", "title": "Test"}
        ])
        
        # Mock dedupe store
        mock_dedupe = Mock()
        mock_dedupe.set_if_not_exists = Mock(return_value=True)
        agent.dedupe_store = mock_dedupe
        
        # Mock producer
        agent.producer = Mock()
        
        # Mock requests.head to succeed
        with patch('requests.head') as mock_head:
            mock_response = Mock()
            mock_response.headers = {'Content-Type': 'text/html'}
            mock_response.raise_for_status = Mock()
            mock_head.return_value = mock_response
            
            # Mock global metrics
            with patch('collector_agent.rss_collector.FEED_FETCH_COUNTER', Mock()):
                with patch('collector_agent.rss_collector.ENTRIES_RETRIEVED_COUNTER', Mock()):
                    with patch('builtins.print') as mock_print:
                        try:
                            agent.run()
                        except StopIteration:
                            pass
                        
                        # Should have printed validation error
                        validation_calls = [call for call in mock_print.call_args_list 
                                          if "validation error" in str(call).lower()]
                        assert len(validation_calls) > 0


class TestMetricsInitialization:
    """Test metrics initialization with simplified approach."""
    
    def test_metrics_function_returns_values(self):
        """Test that metrics initialization function returns values."""
        # This tests the _get_or_create_rss_metrics function
        from collector_agent.rss_collector import _get_or_create_rss_metrics
        
        # Mock prometheus to avoid real metrics creation
        with patch('prometheus_client.Counter') as mock_counter:
            mock_counter.return_value = Mock()
            
            result = _get_or_create_rss_metrics()
            
            # Should return tuple of 6 metrics
            assert isinstance(result, tuple)
            assert len(result) == 6
            assert all(metric is not None for metric in result)