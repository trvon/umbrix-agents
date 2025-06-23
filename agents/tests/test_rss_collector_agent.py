import sys
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from agents.collector_agent.rss_collector import RssCollectorAgent

class DummyProducer:
    def __init__(self):
        self.sent = []
    def send(self, topic, msg):
        self.sent.append((topic, msg))

@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    # Prevent actual sleeping
    monkeypatch.setattr('agents.collector_agent.rss_collector.time.sleep', lambda x: None)

@patch('agents.collector_agent.rss_collector.requests.head')
def test_run_once(mock_head_request, monkeypatch):
    # Mock HTTP head request
    mock_head_response = MagicMock()
    mock_head_response.headers = {'Content-Type': 'text/html'}
    mock_head_response.raise_for_status = lambda: None
    mock_head_request.return_value = mock_head_response
    
    # Mock the datetime.now so we get a consistent value
    fixed_dt = datetime(2021, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr('agents.collector_agent.rss_collector.datetime', type('MockDatetime', (), {
        'now': lambda tz=None: fixed_dt,
        'fromisoformat': datetime.fromisoformat,
        'fromtimestamp': datetime.fromtimestamp,
        'timezone': timezone
    }))

    # Create test data
    test_url = "http://example.com/1"
    test_title = "title1"
    test_summary = "sum1"
    test_date = "2021-01-01"
    
    # Create mocks
    mock_fetcher = MagicMock()
    mock_fetcher.call.return_value = [
        {'id': 'guid1', 'link': test_url, 'published': test_date, 'summary': test_summary, 'title': test_title}
    ]
    
    mock_extractor = MagicMock()
    mock_extractor.call.return_value = {
        'text': 'fulltext', 
        'extraction_method': 'test_mock',
        'extraction_quality': 'good',
        'extraction_confidence': 0.9,
        'raw_html': '<html><body>Raw HTML</body></html>',
        'page_type': 'article',
        'extraction_metrics': {},
        'extraction_attempts': []
    }
    
    # Mock normalizer and enricher
    mock_normalizer = MagicMock()
    mock_normalizer.normalize_feed_record = lambda record: record
    
    mock_enricher = MagicMock()
    
    mock_schema_validator = MagicMock()
    mock_schema_validator.validate_schema.return_value = True
    
    mock_dedupe_store = MagicMock()
    mock_dedupe_store.set_if_not_exists.return_value = True
    
    # Create producer with a clear method to track sends
    class TestProducer:
        def __init__(self):
            self.sent = []
        def send(self, topic, msg):
            # Store the topic and message for verification
            self.sent.append((topic, msg))
            # Return a future-like object that simulates Kafka's behavior
            future = MagicMock()
            future.get.return_value = MagicMock()
            return future  
        def flush(self):
            pass # Simulate flush operation
    
    producer = TestProducer()
    
    # Create a plain record class that mimics FeedRecord
    class Record:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)
            self.metadata = {}
        
        def model_dump(self):
            result = {key: value for key, value in self.__dict__.items() if key != 'metadata'}
            result['metadata'] = self.metadata
            return result
            
        def model_dump_json(self):
            import json
            return json.dumps(self.model_dump())
    
    # Patch the FeedRecord class
    monkeypatch.setattr('agents.collector_agent.rss_collector.FeedRecord', Record)
    
    # Create agent with mocks
    agent = RssCollectorAgent(feeds=['http://dummy'], bootstrap_servers='ignored', use_kafka=False )
    agent.fetcher = mock_fetcher
    agent.extractor = mock_extractor
    agent.feed_normalizer = mock_normalizer
    agent.feed_enricher = mock_enricher
    
    
    # Set up schema validator with mocked validate_schema method
    agent.schema_validator = mock_schema_validator
    agent.schema_validator.validate_schema = MagicMock(return_value=True)
    
    # Set up dedupe store with mocked set_if_not_exists method
    agent.dedupe_store = mock_dedupe_store
    agent.dedupe_store.set_if_not_exists = MagicMock(return_value=True)
    
    # Set up producer
    agent.producer = producer
    agent.raw_intel_topic = 'raw.intel'
    
    # Run the agent
    agent.run_once()
    
    # Debug output
    print(f"Producer sent messages: {len(producer.sent)}")
    if producer.sent:
        for i, (topic, msg) in enumerate(producer.sent):
            print(f"Message {i}, Topic: {topic}")
    
    # Assertions
    assert len(producer.sent) == 1, f"Should send one message to the producer, got {len(producer.sent)}"
    
    if producer.sent:
        topic, feed_record = producer.sent[0]
        assert topic == 'raw.intel', f"Should send to raw.intel topic, got {topic}"
        
        # Get the record data - handle both dictionary and object formats
        msg = feed_record.model_dump() if hasattr(feed_record, 'model_dump') else feed_record
        
        # Verify record content
        assert msg['url'] == test_url, f"URL should be {test_url}, got {msg.get('url')}"
        assert msg.get('metadata', {}).get('extracted_clean_text') == 'fulltext', "Extraction text not found"
        assert msg['description'] == test_summary, f"Description should be {test_summary}"
        assert msg['source_name'] == 'http://dummy', f"Source name should be the feed URL" 
        assert msg['published_at'] == test_date, f"Date should be {test_date}"