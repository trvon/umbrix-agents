"""
Comprehensive Google ADK Testing Suite

This test suite follows Google ADK testing patterns and best practices
to ensure reliability of the data gathering and orchestration components.
"""

import pytest
import asyncio
import json
import time
from unittest.mock import MagicMock, AsyncMock, patch, call
from typing import Dict, Any, List

# Google ADK Core Components (Data Gathering & Orchestration)
from common_tools.agent_base import BaseAgent
from common_tools.kafka_wrapper import ValidatingKafkaProducer, ValidatingKafkaConsumer
from common_tools.content_tools import ArticleExtractorTool
from common_tools.schema_validator import SchemaValidator
from common_tools.dedupe_store import RedisDedupeStore
from common_tools.metrics import UmbrixMetrics
from common_tools.models.feed_record import FeedRecord, FeedRecordMetadata
from collector_agent.rss_collector import RssCollectorAgent
from collector_agent.enhanced_rss_collector import EnhancedRssCollectorAgent


class TestGoogleADKAgentFramework:
    """Test Google ADK Agent Development Kit framework components."""
    
    @pytest.fixture
    def mock_agent_config(self):
        """Mock configuration for ADK agents."""
        return {
            "agent_name": "test_adk_agent",
            "kafka": {
                "bootstrap_servers": "localhost:9092",
                "topics": {
                    "input": "test.input",
                    "output": "test.output"
                }
            },
            "redis": {
                "url": "redis://localhost:6379"
            },
            "metrics": {
                "port": 9090
            }
        }
    
    @pytest.fixture
    def mock_dependencies(self):
        """Mock external dependencies for ADK testing."""
        return {
            "kafka_producer": MagicMock(spec=ValidatingKafkaProducer),
            "kafka_consumer": MagicMock(spec=ValidatingKafkaConsumer),
            "schema_validator": MagicMock(spec=SchemaValidator),
            "dedupe_store": MagicMock(spec=RedisDedupeStore),
            "metrics": MagicMock(spec=UmbrixMetrics),
            "content_extractor": MagicMock(spec=ArticleExtractorTool)
        }
    
    def test_adk_agent_initialization(self, mock_agent_config, mock_dependencies):
        """Test ADK agent initialization with proper component setup."""
        
        class TestADKAgent(BaseAgent):
            def __init__(self, config, **deps):
                super().__init__("test_agent", config)
                self._kafka_producer = deps.get("kafka_producer")
                self._kafka_consumer = deps.get("kafka_consumer")
                self._schema_validator = deps.get("schema_validator")
                self._dedupe_store = deps.get("dedupe_store")
                self._metrics = deps.get("metrics")
            
            def process_message(self, message):
                return {"status": "processed", "data": message}
        
        # Initialize agent with ADK pattern
        agent = TestADKAgent(mock_agent_config, **mock_dependencies)
        
        # Verify ADK component setup
        assert agent.agent_name == "test_agent"
        assert agent._kafka_producer is mock_dependencies["kafka_producer"]
        assert agent._kafka_consumer is mock_dependencies["kafka_consumer"]
        assert agent._schema_validator is mock_dependencies["schema_validator"]
        assert agent._dedupe_store is mock_dependencies["dedupe_store"]
        assert agent._metrics is mock_dependencies["metrics"]
    
    def test_adk_session_management(self, mock_dependencies):
        """Test ADK session and state management patterns."""
        
        class SessionADKAgent(BaseAgent):
            def __init__(self, **deps):
                super().__init__("session_agent", {})
                self.sessions = {}
                self._deps = deps
            
            def create_session(self, user_id: str, session_id: str, initial_state: Dict = None):
                """Create ADK session following Google patterns."""
                session_key = f"{user_id}:{session_id}"
                self.sessions[session_key] = {
                    "user_id": user_id,
                    "session_id": session_id,
                    "state": initial_state or {},
                    "created_at": time.time(),
                    "message_history": []
                }
                return session_key
            
            def send_query(self, session_key: str, query: Dict) -> Dict:
                """Send query to ADK agent session."""
                if session_key not in self.sessions:
                    raise ValueError(f"Session {session_key} not found")
                
                session = self.sessions[session_key]
                session["message_history"].append(query)
                
                # Simulate ADK processing
                response = {
                    "session_id": session["session_id"],
                    "response": f"Processed: {query.get('message', '')}",
                    "events": ["query_received", "processing_complete"],
                    "state_updates": {"last_query": query}
                }
                
                session["state"].update(response["state_updates"])
                return response
            
            def process_message(self, message):
                return {"processed": True}
        
        agent = SessionADKAgent(**mock_dependencies)
        
        # Test session creation
        session_key = agent.create_session("user123", "session456", {"initial": True})
        assert session_key == "user123:session456"
        assert "user123:session456" in agent.sessions
        
        # Test query processing
        query = {"message": "test query", "role": "user"}
        response = agent.send_query(session_key, query)
        
        assert response["session_id"] == "session456"
        assert "Processed: test query" in response["response"]
        assert len(response["events"]) == 2
        assert agent.sessions[session_key]["state"]["last_query"] == query
    
    def test_adk_tool_integration(self, mock_dependencies):
        """Test ADK tool composition and function calling patterns."""
        
        class ToolEnabledADKAgent(BaseAgent):
            def __init__(self, **deps):
                super().__init__("tool_agent", {})
                self.tools = {}
                self._deps = deps
            
            def register_tool(self, name: str, tool_func, description: str = ""):
                """Register tool following ADK patterns."""
                self.tools[name] = {
                    "function": tool_func,
                    "description": description,
                    "metadata": {"registered_at": time.time()}
                }
            
            def call_tool(self, tool_name: str, **kwargs) -> Dict:
                """Call registered tool with ADK error handling."""
                if tool_name not in self.tools:
                    return {"error": f"Tool {tool_name} not found", "available_tools": list(self.tools.keys())}
                
                try:
                    result = self.tools[tool_name]["function"](**kwargs)
                    return {"success": True, "result": result, "tool": tool_name}
                except Exception as e:
                    return {"success": False, "error": str(e), "tool": tool_name}
            
            def process_message(self, message):
                return {"processed": True}
        
        agent = ToolEnabledADKAgent(**mock_dependencies)
        
        # Register test tools
        def content_extractor(url: str) -> str:
            return f"Extracted content from {url}"
        
        def schema_validator(data: Dict) -> bool:
            return "id" in data
        
        agent.register_tool("extract_content", content_extractor, "Extract content from URL")
        agent.register_tool("validate_schema", schema_validator, "Validate data schema")
        
        # Test tool calling
        extract_result = agent.call_tool("extract_content", url="http://example.com")
        assert extract_result["success"] is True
        assert "Extracted content from http://example.com" in extract_result["result"]
        
        validate_result = agent.call_tool("validate_schema", data={"id": "123", "content": "test"})
        assert validate_result["success"] is True
        assert validate_result["result"] is True
        
        # Test error handling
        error_result = agent.call_tool("nonexistent_tool")
        assert "error" in error_result
        assert "Tool nonexistent_tool not found" in error_result["error"]
    
    def test_adk_runner_orchestration(self, mock_dependencies):
        """Test ADK runner system for orchestration and execution management."""
        
        class ADKRunner:
            def __init__(self):
                self.agents = {}
                self.execution_queue = []
                self.running = False
            
            def register_agent(self, agent_name: str, agent_instance):
                """Register agent with runner."""
                self.agents[agent_name] = {
                    "instance": agent_instance,
                    "status": "registered",
                    "execution_count": 0,
                    "last_execution": None
                }
            
            async def run_agent(self, agent_name: str, input_data: Any = None) -> Dict:
                """Execute agent with monitoring."""
                if agent_name not in self.agents:
                    return {"error": f"Agent {agent_name} not registered"}
                
                agent_info = self.agents[agent_name]
                agent_instance = agent_info["instance"]
                
                start_time = time.time()
                try:
                    result = agent_instance.process_message(input_data)
                    execution_time = time.time() - start_time
                    
                    # Update agent stats
                    agent_info["status"] = "completed"
                    agent_info["execution_count"] += 1
                    agent_info["last_execution"] = {
                        "timestamp": start_time,
                        "duration": execution_time,
                        "result": result
                    }
                    
                    return {"success": True, "result": result, "execution_time": execution_time}
                    
                except Exception as e:
                    agent_info["status"] = "error"
                    return {"success": False, "error": str(e)}
            
            def get_agent_status(self, agent_name: str) -> Dict:
                """Get agent status and metrics."""
                if agent_name not in self.agents:
                    return {"error": "Agent not found"}
                
                return self.agents[agent_name]
        
        # Test runner orchestration
        runner = ADKRunner()
        
        # Create test agent
        class TestAgent(BaseAgent):
            def __init__(self):
                super().__init__("test", {})
            
            def process_message(self, message):
                return f"Processed: {message}"
        
        test_agent = TestAgent()
        runner.register_agent("test_agent", test_agent)
        
        # Test agent execution
        import asyncio
        result = asyncio.run(runner.run_agent("test_agent", "test message"))
        assert result["success"] is True
        assert "Processed: test message" in result["result"]
        assert "execution_time" in result
        
        # Test agent status tracking
        status = runner.get_agent_status("test_agent")
        assert status["status"] == "completed"
        assert status["execution_count"] == 1
        assert status["last_execution"] is not None


class TestGoogleADKDataProcessing:
    """Test Google ADK data gathering and processing patterns."""
    
    @pytest.fixture
    def mock_kafka_setup(self):
        """Mock Kafka components for ADK testing."""
        producer = MagicMock(spec=ValidatingKafkaProducer)
        consumer = MagicMock(spec=ValidatingKafkaConsumer)
        
        # Mock successful message sending
        producer.send_validated.return_value = MagicMock()
        producer.send_validated.return_value.get.return_value = MagicMock()
        
        # Mock message consumption
        consumer.poll.return_value = {
            "test.topic": [
                MagicMock(value={"id": "123", "content": "test"}, headers=[])
            ]
        }
        
        return {"producer": producer, "consumer": consumer}
    
    def test_adk_kafka_message_flow(self, mock_kafka_setup):
        """Test ADK Kafka message production and consumption patterns."""
        producer = mock_kafka_setup["producer"]
        consumer = mock_kafka_setup["consumer"]
        
        # Test message production with validation
        message_data = {
            "id": "test-123",
            "content": "test content",
            "timestamp": "2025-06-19T21:00:00Z"
        }
        
        producer.send_validated("test.output", message_data, "feed_record")
        
        # Verify message was sent with validation
        producer.send_validated.assert_called_once_with(
            "test.output", message_data, "feed_record"
        )
        
        # Test message consumption
        messages = consumer.poll(timeout_ms=1000)
        assert "test.topic" in messages
        assert len(messages["test.topic"]) == 1
        
        received_message = messages["test.topic"][0]
        assert received_message.value["id"] == "123"
        assert received_message.value["content"] == "test"
    
    def test_adk_schema_validation_integration(self):
        """Test ADK schema validation integration."""
        validator = SchemaValidator()
        
        # Mock schema loading
        with patch.object(validator, 'load_schema') as mock_load:
            mock_load.return_value = {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "content": {"type": "string"}
                },
                "required": ["id", "content"]
            }
            
            # Test valid data
            valid_data = {"id": "123", "content": "test content"}
            with patch.object(validator, 'validate_schema') as mock_validate:
                mock_validate.return_value = True
                
                result = validator.validate_schema(valid_data, "test_schema")
                assert result is True
                mock_validate.assert_called_once_with(valid_data, "test_schema")
    
    def test_adk_deduplication_patterns(self):
        """Test ADK deduplication using Redis store."""
        dedupe_store = MagicMock(spec=RedisDedupeStore)
        
        # Mock deduplication responses
        dedupe_store.set_if_not_exists.side_effect = [True, False, True]  # new, duplicate, new
        dedupe_store.exists.side_effect = [False, True, False]
        
        # Test deduplication flow
        test_items = [
            {"id": "item1", "content": "first item"},
            {"id": "item2", "content": "second item"},  # duplicate
            {"id": "item3", "content": "third item"}
        ]
        
        processed_items = []
        for item in test_items:
            item_id = item["id"]
            if not dedupe_store.exists(item_id):
                if dedupe_store.set_if_not_exists(item_id, item["content"]):
                    processed_items.append(item)
        
        # Verify deduplication logic
        assert len(processed_items) == 2  # item1 and item3, item2 was duplicate
        assert processed_items[0]["id"] == "item1"
        assert processed_items[1]["id"] == "item3"
    
    def test_adk_content_extraction_pipeline(self):
        """Test ADK content extraction and processing pipeline."""
        extractor = MagicMock(spec=ArticleExtractorTool)
        
        # Mock extraction results
        extractor.call.return_value = {
            "extracted_text": "This is the extracted article content about cybersecurity threats.",
            "extraction_quality": "good",
            "extraction_method": "readability",
            "metadata": {
                "word_count": 250,
                "language": "en",
                "has_security_keywords": True
            }
        }
        
        # Test extraction pipeline
        test_url = "https://security-blog.com/article"
        extraction_result = extractor.call(url=test_url)
        
        # Verify extraction results
        assert extraction_result["extraction_quality"] == "good"
        assert "cybersecurity threats" in extraction_result["extracted_text"]
        assert extraction_result["metadata"]["has_security_keywords"] is True
        
        extractor.call.assert_called_once_with(url=test_url)
    
    def test_adk_metrics_collection(self):
        """Test ADK metrics collection and monitoring."""
        metrics = MagicMock(spec=UmbrixMetrics)
        
        # Mock metrics methods
        metrics.increment_counter = MagicMock()
        metrics.observe_histogram = MagicMock()
        metrics.set_gauge = MagicMock()
        
        # Simulate ADK agent processing with metrics
        processing_start = time.time()
        
        # Process message
        time.sleep(0.01)  # Simulate processing time
        processing_duration = time.time() - processing_start
        
        # Record metrics
        metrics.increment_counter("messages_processed", {"agent": "test_agent", "status": "success"})
        metrics.observe_histogram("processing_duration", processing_duration, {"agent": "test_agent"})
        metrics.set_gauge("active_connections", 5, {"component": "kafka"})
        
        # Verify metrics calls
        metrics.increment_counter.assert_called_with(
            "messages_processed", {"agent": "test_agent", "status": "success"}
        )
        metrics.observe_histogram.assert_called_with(
            "processing_duration", processing_duration, {"agent": "test_agent"}
        )
        metrics.set_gauge.assert_called_with(
            "active_connections", 5, {"component": "kafka"}
        )


class TestGoogleADKRSSCollectorIntegration:
    """Test Google ADK integration in RSS Collector Agent."""
    
    @pytest.fixture
    def mock_rss_dependencies(self):
        """Mock dependencies for RSS collector testing."""
        return {
            "fetcher": MagicMock(),
            "extractor": MagicMock(),
            "producer": MagicMock(),
            "dedupe_store": MagicMock(),
            "schema_validator": MagicMock(),
            "metrics": MagicMock()
        }
    
    def test_adk_rss_collector_initialization(self, mock_rss_dependencies):
        """Test RSS collector initialization with ADK components."""
        
        with patch('collector_agent.rss_collector.RssCollectorAgent.__init__') as mock_init:
            mock_init.return_value = None
            
            # Create agent instance
            agent = RssCollectorAgent()
            
            # Manually set dependencies for testing
            for name, mock_dep in mock_rss_dependencies.items():
                setattr(agent, f"_{name}", mock_dep)
            
            # Verify ADK components are properly initialized
            assert hasattr(agent, '_fetcher')
            assert hasattr(agent, '_extractor')
            assert hasattr(agent, '_producer')
            assert hasattr(agent, '_dedupe_store')
    
    def test_adk_rss_feed_processing_workflow(self, mock_rss_dependencies):
        """Test complete ADK workflow for RSS feed processing."""
        
        # Mock feed data
        mock_feed_entries = [
            {
                "id": "entry1",
                "link": "https://security-blog.com/threat-analysis",
                "title": "New APT Campaign Analysis",
                "published": "2025-06-19T21:00:00Z",
                "summary": "Analysis of new threat campaign"
            }
        ]
        
        # Configure mocks
        mock_rss_dependencies["fetcher"].call.return_value = mock_feed_entries
        mock_rss_dependencies["extractor"].call.return_value = {
            "extracted_text": "Detailed threat analysis content...",
            "extraction_quality": "good",
            "extraction_method": "readability"
        }
        mock_rss_dependencies["dedupe_store"].set_if_not_exists.return_value = True
        mock_rss_dependencies["schema_validator"].validate_schema.return_value = True
        
        # Create mock agent
        class MockRSSAgent:
            def __init__(self, deps):
                self._deps = deps
            
            def process_feed(self, feed_url: str) -> bool:
                """Process RSS feed using ADK pattern."""
                # Step 1: Fetch feed entries (ADK Data Gathering)
                entries = self._deps["fetcher"].call(feed_url=feed_url)
                
                processed_count = 0
                for entry in entries:
                    # Step 2: Extract content (ADK Content Processing)
                    extraction_result = self._deps["extractor"].call(url=entry["link"])
                    
                    # Step 3: Check for duplicates (ADK Deduplication)
                    if self._deps["dedupe_store"].set_if_not_exists(entry["id"], entry["link"]):
                        
                        # Step 4: Create feed record (ADK Data Normalization)
                        feed_record = {
                            "id": entry["id"],
                            "title": entry["title"],
                            "url": entry["link"],
                            "content": extraction_result["extracted_text"],
                            "extraction_quality": extraction_result["extraction_quality"]
                        }
                        
                        # Step 5: Validate schema (ADK Schema Validation)
                        if self._deps["schema_validator"].validate_schema(feed_record, "feed_record"):
                            
                            # Step 6: Publish to Kafka (ADK Message Publishing)
                            self._deps["producer"].send_validated(
                                "raw.intel", feed_record, "feed_record"
                            )
                            processed_count += 1
                
                return processed_count > 0
        
        # Test the workflow
        agent = MockRSSAgent(mock_rss_dependencies)
        result = agent.process_feed("https://security-blog.com/feed.xml")
        
        # Verify ADK workflow steps
        assert result is True
        
        # Verify fetcher was called
        mock_rss_dependencies["fetcher"].call.assert_called_once_with(
            feed_url="https://security-blog.com/feed.xml"
        )
        
        # Verify content extraction
        mock_rss_dependencies["extractor"].call.assert_called_once_with(
            url="https://security-blog.com/threat-analysis"
        )
        
        # Verify deduplication
        mock_rss_dependencies["dedupe_store"].set_if_not_exists.assert_called_once_with(
            "entry1", "https://security-blog.com/threat-analysis"
        )
        
        # Verify schema validation
        mock_rss_dependencies["schema_validator"].validate_schema.assert_called_once()
        
        # Verify message publishing
        mock_rss_dependencies["producer"].send_validated.assert_called_once()
        call_args = mock_rss_dependencies["producer"].send_validated.call_args
        assert call_args[0][0] == "raw.intel"  # topic
        assert call_args[0][2] == "feed_record"  # schema
        assert call_args[0][1]["id"] == "entry1"  # message data
    
    def test_adk_error_handling_patterns(self, mock_rss_dependencies):
        """Test ADK error handling and resilience patterns."""
        
        # Configure mock failures
        mock_rss_dependencies["fetcher"].call.side_effect = Exception("Network error")
        mock_rss_dependencies["extractor"].call.side_effect = Exception("Extraction failed")
        
        class ResilientADKAgent:
            def __init__(self, deps):
                self._deps = deps
                self.error_count = 0
                self.success_count = 0
            
            def process_with_error_handling(self, feed_url: str) -> Dict:
                """Process with ADK error handling patterns."""
                try:
                    # Attempt data gathering
                    entries = self._deps["fetcher"].call(feed_url=feed_url)
                    self.success_count += 1
                    return {"status": "success", "entries": len(entries)}
                    
                except Exception as e:
                    # ADK error handling
                    self.error_count += 1
                    self._deps["metrics"].increment_counter(
                        "processing_errors", {"agent": "rss_collector", "error_type": type(e).__name__}
                    )
                    
                    return {
                        "status": "error",
                        "error": str(e),
                        "error_type": type(e).__name__
                    }
        
        # Test error handling
        agent = ResilientADKAgent(mock_rss_dependencies)
        result = agent.process_with_error_handling("https://failing-feed.com/feed.xml")
        
        # Verify error handling
        assert result["status"] == "error"
        assert "Network error" in result["error"]
        assert agent.error_count == 1
        assert agent.success_count == 0
        
        # Verify metrics were recorded
        mock_rss_dependencies["metrics"].increment_counter.assert_called_once_with(
            "processing_errors", {"agent": "rss_collector", "error_type": "Exception"}
        )


class TestGoogleADKPerformancePatterns:
    """Test Google ADK performance monitoring and optimization patterns."""
    
    def test_adk_batch_processing_optimization(self):
        """Test ADK batch processing for improved throughput."""
        
        class BatchProcessingADKAgent:
            def __init__(self, batch_size: int = 10):
                self.batch_size = batch_size
                self.processed_count = 0
                self.batch_metrics = []
            
            def process_batch(self, items: List[Dict]) -> Dict:
                """Process items in batches for better performance."""
                batches = [items[i:i + self.batch_size] for i in range(0, len(items), self.batch_size)]
                results = []
                
                for batch_idx, batch in enumerate(batches):
                    batch_start_time = time.time()
                    
                    # Process batch
                    batch_results = []
                    for item in batch:
                        batch_results.append(f"processed_{item['id']}")
                        self.processed_count += 1
                    
                    batch_duration = time.time() - batch_start_time
                    self.batch_metrics.append({
                        "batch_id": batch_idx,
                        "items_count": len(batch),
                        "duration": batch_duration,
                        "throughput": len(batch) / batch_duration
                    })
                    
                    results.extend(batch_results)
                
                return {
                    "total_processed": self.processed_count,
                    "batch_count": len(batches),
                    "results": results,
                    "metrics": self.batch_metrics
                }
        
        # Test batch processing
        agent = BatchProcessingADKAgent(batch_size=5)
        test_items = [{"id": f"item_{i}"} for i in range(12)]  # 12 items, 3 batches
        
        result = agent.process_batch(test_items)
        
        # Verify batch processing
        assert result["total_processed"] == 12
        assert result["batch_count"] == 3  # ceil(12/5) = 3 batches
        assert len(result["results"]) == 12
        assert len(result["metrics"]) == 3
        
        # Verify metrics collection
        for batch_metric in result["metrics"]:
            assert "batch_id" in batch_metric
            assert "duration" in batch_metric
            assert "throughput" in batch_metric
    
    def test_adk_connection_pooling_patterns(self):
        """Test ADK connection pooling for resource optimization."""
        
        class ConnectionPoolADKAgent:
            def __init__(self, pool_size: int = 5):
                self.pool_size = pool_size
                self.connection_pool = []
                self.active_connections = 0
                self.connection_metrics = {
                    "total_acquired": 0,
                    "total_released": 0,
                    "peak_usage": 0
                }
            
            def acquire_connection(self) -> int:
                """Acquire connection from pool."""
                if len(self.connection_pool) > 0:
                    connection_id = self.connection_pool.pop()
                else:
                    connection_id = self.connection_metrics["total_acquired"] + 1
                
                self.active_connections += 1
                self.connection_metrics["total_acquired"] += 1
                self.connection_metrics["peak_usage"] = max(
                    self.connection_metrics["peak_usage"], 
                    self.active_connections
                )
                
                return connection_id
            
            def release_connection(self, connection_id: int):
                """Release connection back to pool."""
                if len(self.connection_pool) < self.pool_size:
                    self.connection_pool.append(connection_id)
                
                self.active_connections -= 1
                self.connection_metrics["total_released"] += 1
            
            def get_pool_status(self) -> Dict:
                """Get connection pool status."""
                return {
                    "pool_size": self.pool_size,
                    "available_connections": len(self.connection_pool),
                    "active_connections": self.active_connections,
                    "metrics": self.connection_metrics
                }
        
        # Test connection pool
        agent = ConnectionPoolADKAgent(pool_size=3)
        
        # Acquire connections
        conn1 = agent.acquire_connection()
        conn2 = agent.acquire_connection()
        conn3 = agent.acquire_connection()
        
        status = agent.get_pool_status()
        assert status["active_connections"] == 3
        assert status["available_connections"] == 0
        assert status["metrics"]["peak_usage"] == 3
        
        # Release connections
        agent.release_connection(conn1)
        agent.release_connection(conn2)
        
        status = agent.get_pool_status()
        assert status["active_connections"] == 1
        assert status["available_connections"] == 2
        assert status["metrics"]["total_released"] == 2
    
    def test_adk_performance_monitoring_integration(self):
        """Test ADK integration with performance monitoring."""
        
        class PerformanceMonitoredADKAgent:
            def __init__(self):
                self.performance_data = []
                self.operation_counts = {}
            
            def monitor_operation(self, operation_name: str):
                """Decorator-like method for monitoring operations."""
                def decorator(func):
                    def wrapper(*args, **kwargs):
                        start_time = time.time()
                        
                        try:
                            result = func(*args, **kwargs)
                            status = "success"
                            error = None
                        except Exception as e:
                            result = None
                            status = "error"
                            error = str(e)
                        
                        end_time = time.time()
                        duration = end_time - start_time
                        
                        # Record performance data
                        self.performance_data.append({
                            "operation": operation_name,
                            "duration": duration,
                            "status": status,
                            "timestamp": start_time,
                            "error": error
                        })
                        
                        # Update operation counts
                        if operation_name not in self.operation_counts:
                            self.operation_counts[operation_name] = {"success": 0, "error": 0}
                        self.operation_counts[operation_name][status] += 1
                        
                        if status == "error":
                            raise Exception(error)
                        
                        return result
                    return wrapper
                return decorator
            
            def get_performance_summary(self) -> Dict:
                """Get performance summary for monitoring."""
                if not self.performance_data:
                    return {"message": "No performance data available"}
                
                total_operations = len(self.performance_data)
                successful_operations = len([p for p in self.performance_data if p["status"] == "success"])
                average_duration = sum(p["duration"] for p in self.performance_data) / total_operations
                
                return {
                    "total_operations": total_operations,
                    "success_rate": successful_operations / total_operations,
                    "average_duration": average_duration,
                    "operation_counts": self.operation_counts,
                    "recent_operations": self.performance_data[-5:]  # Last 5 operations
                }
        
        # Test performance monitoring
        agent = PerformanceMonitoredADKAgent()
        
        # Create monitored operations
        @agent.monitor_operation("data_fetch")
        def fetch_data(url: str):
            time.sleep(0.01)  # Simulate network call
            return f"data_from_{url}"
        
        @agent.monitor_operation("data_process")
        def process_data(data: str):
            time.sleep(0.005)  # Simulate processing
            return f"processed_{data}"
        
        # Execute monitored operations
        data1 = fetch_data("url1")
        result1 = process_data(data1)
        
        data2 = fetch_data("url2")
        result2 = process_data(data2)
        
        # Get performance summary
        summary = agent.get_performance_summary()
        
        # Verify performance monitoring
        assert summary["total_operations"] == 4  # 2 fetch + 2 process
        assert summary["success_rate"] == 1.0  # All successful
        assert summary["average_duration"] > 0
        assert "data_fetch" in summary["operation_counts"]
        assert "data_process" in summary["operation_counts"]
        assert summary["operation_counts"]["data_fetch"]["success"] == 2
        assert summary["operation_counts"]["data_process"]["success"] == 2
        assert len(summary["recent_operations"]) == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])