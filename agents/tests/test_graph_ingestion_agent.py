"""
Comprehensive tests for Graph Ingestion Agent - ADK Agent for ingesting raw intelligence into Neo4j.

Tests cover:
- GraphIngestionAgent initialization and configuration
- Kafka consumer setup with multiple topics
- Raw intelligence transformation (RSS, MISP, TAXII)
- CTI backend API integration and HTTP client
- Entity enrichment via backend tools
- LLM-powered enrichment with Gemini
- Error handling and fallback mechanisms
- Pydantic model validation for CTI entities
"""

import pytest
import json
import asyncio
import sys
import requests
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from typing import Dict, Any, List, Optional
from pydantic import ValidationError

from processing_agent.graph_ingestion_agent import (
    GraphIngestionAgent,
    AttackPatternDetails,
    ThreatActorDetails,
    MalwareDetails,
    CampaignDetails,
    VulnerabilityDetails
)


class TestGraphIngestionAgentInitialization:
    """Test suite for GraphIngestionAgent initialization and setup."""
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_initialization_with_default_topics(self, mock_consumer, mock_http_client):
        """Test initialization with default topics."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent()
        
        assert agent.topics == ["raw.intel", "raw.intel.taxii", "raw.intel.misp"]
        assert agent.enrich_with_llm is False
        assert agent.llm is None
        
        # Check that KafkaConsumer was called with correct arguments
        mock_consumer.assert_called_once()
        call_args = mock_consumer.call_args
        assert call_args[0] == ("raw.intel", "raw.intel.taxii", "raw.intel.misp")
        assert call_args[1]['bootstrap_servers'] == 'localhost:9092'
        assert call_args[1]['group_id'] == 'graph_ingestion_group'
        assert call_args[1]['auto_offset_reset'] == 'earliest'
        assert 'value_deserializer' in call_args[1]  # Just check it exists
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_initialization_with_custom_topics_list(self, mock_consumer, mock_http_client):
        """Test initialization with custom topics as list."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        custom_topics = ["custom.topic1", "custom.topic2"]
        agent = GraphIngestionAgent(topics=custom_topics)
        
        assert agent.topics == custom_topics
        
        # Check that KafkaConsumer was called with correct arguments
        mock_consumer.assert_called_once()
        call_args = mock_consumer.call_args
        assert call_args[0] == ("custom.topic1", "custom.topic2")
        assert call_args[1]['bootstrap_servers'] == 'localhost:9092'
        assert call_args[1]['group_id'] == 'graph_ingestion_group'
        assert call_args[1]['auto_offset_reset'] == 'earliest'
        assert 'value_deserializer' in call_args[1]
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_initialization_with_json_string_topics(self, mock_consumer, mock_http_client):
        """Test initialization with topics as JSON string."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        topics_json = '["json.topic1", "json.topic2"]'
        agent = GraphIngestionAgent(topics=topics_json)
        
        assert agent.topics == ["json.topic1", "json.topic2"]
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_initialization_with_invalid_json_topics(self, mock_consumer, mock_http_client):
        """Test initialization with invalid JSON topics falls back to default."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent(topics="invalid json")
        
        assert agent.topics == ["raw.intel", "raw.intel.taxii", "raw.intel.misp"]
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_initialization_with_non_list_json_topics(self, mock_consumer, mock_http_client):
        """Test initialization with non-list JSON topics falls back to default."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent(topics='{"not": "a list"}')
        
        assert agent.topics == ["raw.intel", "raw.intel.taxii", "raw.intel.misp"]
    
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('builtins.print')  # Mock print to capture stderr output
    def test_initialization_missing_backend_url_exits(self, mock_print, mock_consumer):
        """Test initialization without CTI_BACKEND_URL exits."""
        mock_consumer.return_value = Mock()
        
        with patch.dict('os.environ', {}, clear=True), \
             pytest.raises(SystemExit):
            GraphIngestionAgent()


class TestGraphIngestionAgentLLMEnrichment:
    """Test suite for LLM enrichment functionality."""
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', True)
    @patch('processing_agent.graph_ingestion_agent.Gemini')
    def test_llm_enrichment_enabled_with_api_key(self, mock_gemini, mock_consumer, mock_http_client):
        """Test LLM enrichment is enabled with proper configuration."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        mock_gemini_instance = Mock()
        mock_gemini.return_value = mock_gemini_instance
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        assert agent.enrich_with_llm is True
        assert agent.llm == mock_gemini_instance
        assert agent.gemini_api_key == 'test-api-key'
        assert agent.gemini_model == 'gemini-2.5-flash'
        mock_gemini.assert_called_once_with(model='gemini-2.5-flash')
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key',
        'GEMINI_MODEL_NAME': 'custom-model'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', True)
    @patch('processing_agent.graph_ingestion_agent.Gemini')
    def test_llm_enrichment_with_custom_model(self, mock_gemini, mock_consumer, mock_http_client):
        """Test LLM enrichment with custom model name."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        mock_gemini.return_value = Mock()
        
        agent = GraphIngestionAgent(
            enrich_with_llm=True,
            gemini_model='override-model'
        )
        
        assert agent.gemini_model == 'override-model'
        mock_gemini.assert_called_once_with(model='override-model')
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'}, clear=True)
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('builtins.print')  # Mock print to capture stderr output
    def test_llm_enrichment_disabled_without_api_key(self, mock_print, mock_consumer, mock_http_client):
        """Test LLM enrichment is disabled without API key."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        assert agent.enrich_with_llm is False
        assert agent.llm is None
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', False)
    def test_llm_enrichment_disabled_without_adk(self, mock_consumer, mock_http_client):
        """Test LLM enrichment is disabled without Google ADK."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        assert agent.enrich_with_llm is False
        assert agent.llm is None
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', True)
    @patch('processing_agent.graph_ingestion_agent.Gemini')
    def test_llm_enrichment_disabled_on_initialization_error(self, mock_gemini, mock_consumer, mock_http_client):
        """Test LLM enrichment is disabled when Gemini initialization fails."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        mock_gemini.side_effect = Exception("Gemini init failed")
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        assert agent.enrich_with_llm is False
        assert agent.llm is None


class TestGraphIngestionAgentRun:
    """Test suite for agent run loop and message processing."""
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_run_processes_messages(self, mock_consumer, mock_http_client):
        """Test run loop processes Kafka messages."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        
        # Mock Kafka message
        mock_message = Mock()
        mock_message.value = {
            'source_url': 'http://test.com',
            'retrieved_at': '2024-01-01T00:00:00Z',
            'full_text': 'Test content'
        }
        
        mock_consumer_instance = Mock()
        mock_consumer_instance.__iter__ = Mock(return_value=iter([mock_message]))
        mock_consumer.return_value = mock_consumer_instance
        
        # Mock API response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {'status': 'success'}
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        # Stop iteration after first message
        with patch.object(agent.consumer, '__iter__', return_value=iter([mock_message])):
            agent.run()
        
        # Verify API was called
        mock_http_client_instance.post.assert_called_once()
        call_args = mock_http_client_instance.post.call_args
        assert call_args[0][0] == "/v1/graph/ingest"
        assert 'nodes' in call_args[1]['json']
        assert 'relationships' in call_args[1]['json']
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_run_handles_transformation_errors(self, mock_consumer, mock_http_client):
        """Test run loop handles transformation errors gracefully."""
        mock_http_client.return_value = Mock()
        
        # Mock message that will cause transformation error
        mock_message = Mock()
        mock_message.value = None  # Invalid record
        
        mock_consumer_instance = Mock()
        mock_consumer_instance.__iter__ = Mock(return_value=iter([mock_message]))
        mock_consumer.return_value = mock_consumer_instance
        
        agent = GraphIngestionAgent()
        
        # This should not raise an exception
        with patch.object(agent.consumer, '__iter__', return_value=iter([mock_message])):
            agent.run()
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_run_handles_api_errors(self, mock_consumer, mock_http_client):
        """Test run loop handles API errors gracefully."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        
        # Mock message
        mock_message = Mock()
        mock_message.value = {'source_url': 'http://test.com', 'retrieved_at': '2024-01-01T00:00:00Z'}
        
        mock_consumer_instance = Mock()
        mock_consumer_instance.__iter__ = Mock(return_value=iter([mock_message]))
        mock_consumer.return_value = mock_consumer_instance
        
        # Mock API error
        mock_http_client_instance.post.side_effect = Exception("API error")
        
        agent = GraphIngestionAgent()
        
        # This should not raise an exception
        with patch.object(agent.consumer, '__iter__', return_value=iter([mock_message])):
            agent.run()


class TestGraphIngestionAgentTransformation:
    """Test suite for raw intelligence transformation logic."""
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_transform_rss_record(self, mock_consumer, mock_http_client):
        """Test transformation of RSS-style records."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent()
        
        record = {
            'source_url': 'http://security-blog.com/article1',
            'retrieved_at': '2024-01-01T10:00:00Z',
            'full_text': 'Security article content',
            'summary': 'Article summary',
            'source_name': 'Security Blog'
        }
        
        result = agent._transform(record)
        
        assert 'nodes' in result
        assert 'relationships' in result
        
        # Should have Source, SightingEvent, and Article nodes
        nodes = result['nodes']
        assert len(nodes) == 3
        
        # Check node types
        node_labels = [node['labels'][0] for node in nodes]
        assert 'Source' in node_labels
        assert 'SightingEvent' in node_labels
        assert 'Article' in node_labels
        
        # Check relationships
        relationships = result['relationships']
        assert len(relationships) == 2
        assert any(rel['type'] == 'REPORTED_BY' for rel in relationships)
        assert any(rel['type'] == 'ABOUT' for rel in relationships)
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_transform_misp_record_regular_ioc(self, mock_consumer, mock_http_client):
        """Test transformation of MISP records with regular IOC."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent()
        
        record = {
            'source': 'misp',
            'feed_url': 'http://misp.example.com/feed',
            'fetched_at': '2024-01-01T10:00:00Z',
            'ioc_record': {
                'value': '192.168.1.100',
                'type': 'ip',
                'threat_level': 'medium'
            }
        }
        
        result = agent._transform(record)
        
        nodes = result['nodes']
        assert len(nodes) == 2
        
        # Should have MispEvent and Indicator nodes
        node_labels = [node['labels'][0] for node in nodes]
        assert 'MispEvent' in node_labels
        assert 'Indicator' in node_labels
        
        # Check relationships
        relationships = result['relationships']
        assert len(relationships) == 1
        assert relationships[0]['type'] == 'APPEARED_IN'
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_transform_misp_record_apt_group(self, mock_consumer, mock_http_client):
        """Test transformation of MISP records with APT group."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent()
        
        record = {
            'source': 'misp',
            'feed_url': 'http://misp.example.com/feed',
            'fetched_at': '2024-01-01T10:00:00Z',
            'ioc_record': {
                'value': 'APT29',
                'description': 'Advanced Persistent Threat group',
                'sophistication': 'high'
            }
        }
        
        result = agent._transform(record)
        
        nodes = result['nodes']
        assert len(nodes) == 2
        
        # Should have MispEvent and ThreatActor nodes
        node_labels = [node['labels'][0] for node in nodes]
        assert 'MispEvent' in node_labels
        assert 'ThreatActor' in node_labels
        
        # Find ThreatActor node
        threat_actor_node = next(node for node in nodes if 'ThreatActor' in node['labels'])
        assert threat_actor_node['properties']['name'] == 'APT29'
        assert threat_actor_node['properties']['description'] == 'Advanced Persistent Threat group'
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_transform_taxii_relationship(self, mock_consumer, mock_http_client):
        """Test transformation of TAXII STIX relationship objects."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent()
        
        record = {
            'source': 'taxii',
            'fetched_at': '2024-01-01T10:00:00Z',
            'stix_object': {
                'id': 'relationship--12345',
                'type': 'relationship',
                'relationship_type': 'uses',
                'source_ref': 'malware--abcdef',
                'target_ref': 'attack-pattern--67890',
                'confidence': 85
            }
        }
        
        result = agent._transform(record)
        
        # Should have no nodes for relationship objects
        assert len(result['nodes']) == 0
        
        # Should have one relationship
        relationships = result['relationships']
        assert len(relationships) == 1
        rel = relationships[0]
        assert rel['type'] == 'USES'
        assert rel['source_id'] == 'malware--abcdef'
        assert rel['target_id'] == 'attack-pattern--67890'
        assert rel['properties']['confidence'] == 85
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_transform_taxii_attack_pattern(self, mock_consumer, mock_http_client):
        """Test transformation of TAXII STIX attack-pattern objects."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent()
        
        record = {
            'source': 'taxii',
            'fetched_at': '2024-01-01T10:00:00Z',
            'stix_object': {
                'id': 'attack-pattern--12345',
                'type': 'attack-pattern',
                'name': 'Spear Phishing Attachment',
                'description': 'Adversaries may use spear phishing...',
                'external_references': [
                    {'external_id': 'T1566.001', 'source_name': 'mitre-attack'}
                ]
            }
        }
        
        result = agent._transform(record)
        
        nodes = result['nodes']
        assert len(nodes) == 1
        
        node = nodes[0]
        assert node['labels'] == ['AttackPattern']
        assert node['id'] == 'attack-pattern--12345'
        assert node['properties']['name'] == 'Spear Phishing Attachment'
        assert node['properties']['fetched_at'] == '2024-01-01T10:00:00Z'
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_transform_taxii_unmapped_type(self, mock_consumer, mock_http_client):
        """Test transformation of TAXII STIX objects with unmapped types."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent()
        
        record = {
            'source': 'taxii',
            'fetched_at': '2024-01-01T10:00:00Z',
            'stix_object': {
                'id': 'custom-object--12345',
                'type': 'custom-object',
                'name': 'Custom STIX object'
            }
        }
        
        result = agent._transform(record)
        
        nodes = result['nodes']
        assert len(nodes) == 1
        
        node = nodes[0]
        assert node['labels'] == ['CustomObject']  # Title case with dash removal
        assert node['id'] == 'custom-object--12345'
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_transform_fallback_generic_record(self, mock_consumer, mock_http_client):
        """Test transformation fallback for unrecognized records."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent()
        
        record = {
            'unknown_field': 'unknown_value',
            'data': 'generic data'
        }
        
        result = agent._transform(record)
        
        nodes = result['nodes']
        assert len(nodes) == 1
        
        node = nodes[0]
        assert node['labels'] == ['RawIntel']
        assert node['properties'] == record


class TestGraphIngestionAgentAPIIntegration:
    """Test suite for CTI backend API integration."""
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_call_ingest_api_success(self, mock_consumer, mock_http_client):
        """Test successful API call to ingest endpoint."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock successful response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {'status': 'success', 'nodes_created': 3}
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        changeset = {'nodes': [], 'relationships': []}
        
        result = agent._call_ingest_api(changeset)
        
        assert result == {'status': 'success', 'nodes_created': 3}
        mock_http_client_instance.post.assert_called_once_with(
            "/v1/graph/ingest",
            json=changeset,
            timeout=30
        )
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_call_ingest_api_http_error(self, mock_consumer, mock_http_client):
        """Test API call handling HTTP errors."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock HTTP error response
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("HTTP 500 Error")
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        changeset = {'nodes': [], 'relationships': []}
        
        result = agent._call_ingest_api(changeset)
        
        assert result['status'] == 500
        assert result['error'] == "Internal Server Error"
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_call_ingest_api_json_decode_error(self, mock_consumer, mock_http_client):
        """Test API call handling JSON decode errors."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock response with non-JSON content
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = ValueError("Not valid JSON")
        mock_response.status_code = 200
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        changeset = {'nodes': [], 'relationships': []}
        
        result = agent._call_ingest_api(changeset)
        
        assert result == {'status': 200}
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_call_tool_api_success(self, mock_consumer, mock_http_client):
        """Test successful tool API call."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock successful response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {'tool_result': 'success'}
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        result = agent._call_tool_api("test_tool", {"param": "value"})
        
        assert result == {'tool_result': 'success'}
        mock_http_client_instance.post.assert_called_once_with(
            "/v1/tools/test_tool",
            json={"param": "value"},
            timeout=30
        )
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_call_tool_api_error(self, mock_consumer, mock_http_client):
        """Test tool API call error handling."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock HTTP error
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("HTTP Error")
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        with pytest.raises(requests.HTTPError, match="HTTP Error"):
            agent._call_tool_api("test_tool", {"param": "value"})


class TestGraphIngestionAgentEnrichment:
    """Test suite for entity enrichment functionality."""
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_enrich_attack_pattern_success(self, mock_consumer, mock_http_client):
        """Test successful attack pattern enrichment."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock tool API response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            'id': 'T1566.001',
            'name': 'Spear Phishing Attachment',
            'description': 'Adversaries may use spear phishing...',
            'mitre_attack_id': 'T1566.001',
            'mitre_url': 'https://attack.mitre.org/techniques/T1566/001/',
            'detection': 'Monitor for suspicious email attachments',
            'mitigation': 'Implement email security controls',
            'platforms': ['Windows', 'Linux'],
            'data_sources': ['Email', 'File monitoring']
        }
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        result = agent._enrich_attack_pattern('T1566.001')
        
        assert result is not None
        assert result.id == 'T1566.001'
        assert result.name == 'Spear Phishing Attachment'
        assert result.platforms == ['Windows', 'Linux']
        mock_http_client_instance.post.assert_called_once_with(
            "/v1/tools/get_attack_pattern_details",
            json={"attack_pattern_identifier": "T1566.001"},
            timeout=30
        )
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_enrich_attack_pattern_not_found(self, mock_consumer, mock_http_client):
        """Test attack pattern enrichment when not found."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock tool API response indicating not found
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"message": "Attack pattern not found"}
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        result = agent._enrich_attack_pattern('T9999.999')
        
        assert result is None
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_enrich_attack_pattern_http_error(self, mock_consumer, mock_http_client):
        """Test attack pattern enrichment with HTTP error."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock HTTP error
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("HTTP Error")
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        result = agent._enrich_attack_pattern('T1566.001')
        
        assert result is None
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_enrich_attack_pattern_validation_error(self, mock_consumer, mock_http_client):
        """Test attack pattern enrichment with validation error."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock tool API response with invalid data
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            'id': 'T1566.001',
            'cvss_v3_score': 'invalid_score'  # Should be float
        }
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        result = agent._enrich_attack_pattern('T1566.001')
        
        assert result is None
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_enrich_generic_success(self, mock_consumer, mock_http_client):
        """Test successful generic enrichment."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock tool API response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            'name': 'APT29',
            'description': 'Russian threat actor',
            'sophistication': 'high',
            'primary_motivation': 'espionage',
            'resource_level': 'government',
            'first_seen': '2008-01-01',
            'last_seen': '2024-01-01',
            'aliases': ['Cozy Bear', 'The Dukes']
        }
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        result = agent._enrich_generic(
            'APT29',
            'get_threat_actor_summary',
            ThreatActorDetails,
            'actor_name'
        )
        
        assert result is not None
        assert result['name'] == 'APT29'
        assert result['sophistication'] == 'high'
        assert 'aliases' in result
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_enrich_generic_not_found(self, mock_consumer, mock_http_client):
        """Test generic enrichment when entity not found."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock tool API response indicating not found
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"message": "Entity not found"}
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        result = agent._enrich_generic(
            'UnknownActor',
            'get_threat_actor_summary',
            ThreatActorDetails,
            'actor_name'
        )
        
        assert result is None


class TestGraphIngestionAgentTTPEnrichment:
    """Test suite for TTP (attack pattern) enrichment in transformation."""
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_transform_with_ttp_enrichment(self, mock_consumer, mock_http_client):
        """Test transformation with TTP indicator enrichment."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock enrichment API response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            'id': 'T1566.001',
            'name': 'Spear Phishing Attachment',
            'description': 'Adversaries may use spear phishing...',
            'mitre_attack_id': 'T1566.001',
            'mitre_url': 'https://attack.mitre.org/techniques/T1566/001/',
            'detection': 'Monitor for suspicious email attachments',
            'mitigation': 'Implement email security controls'
        }
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        # Record with TTP indicator
        record = {
            'source': 'misp',
            'feed_url': 'http://misp.example.com/feed',
            'fetched_at': '2024-01-01T10:00:00Z',
            'ioc_record': {
                'value': 'T1566.001',
                'type': 'ttp'
            }
        }
        
        result = agent._transform(record)
        
        nodes = result['nodes']
        # Should have MispEvent, Indicator, and AttackPattern nodes
        assert len(nodes) == 3
        
        node_labels = [node['labels'][0] for node in nodes]
        assert 'MispEvent' in node_labels
        assert 'Indicator' in node_labels
        assert 'AttackPattern' in node_labels
        
        # Find AttackPattern node
        attack_pattern_node = next(node for node in nodes if 'AttackPattern' in node['labels'])
        assert attack_pattern_node['properties']['name'] == 'Spear Phishing Attachment'
        
        # Should have relationship from Indicator to AttackPattern
        relationships = result['relationships']
        describes_rel = next(rel for rel in relationships if rel['type'] == 'DESCRIBES')
        assert describes_rel['source_id'] == 'T1566.001'
        assert describes_rel['target_id'] == 'T1566.001'
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_transform_with_invalid_ttp_pattern(self, mock_consumer, mock_http_client):
        """Test transformation with invalid TTP pattern (doesn't match T\\d+...)."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent()
        
        # Record with non-TTP indicator that looks like TTP but doesn't match pattern
        record = {
            'source': 'misp',
            'feed_url': 'http://misp.example.com/feed',
            'fetched_at': '2024-01-01T10:00:00Z',
            'ioc_record': {
                'value': 'TT1566.001',  # Double T, doesn't match pattern
                'type': 'indicator'
            }
        }
        
        result = agent._transform(record)
        
        nodes = result['nodes']
        # Should only have MispEvent and Indicator nodes
        assert len(nodes) == 2
        
        node_labels = [node['labels'][0] for node in nodes]
        assert 'MispEvent' in node_labels
        assert 'Indicator' in node_labels
        assert 'AttackPattern' not in node_labels


class TestGraphIngestionAgentPostTransform:
    """Test suite for post-transformation enrichment."""
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_post_transform_threat_actor_enrichment(self, mock_consumer, mock_http_client):
        """Test post-transformation ThreatActor enrichment."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock enrichment API response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            'name': 'APT29',
            'description': 'Russian state-sponsored threat actor',
            'sophistication': 'high',
            'primary_motivation': 'espionage',
            'resource_level': 'government',
            'first_seen': '2008-01-01',
            'last_seen': '2024-01-01',
            'aliases': ['Cozy Bear', 'The Dukes']
        }
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        nodes = [
            {
                'id': 'APT29',
                'labels': ['ThreatActor'],
                'properties': {'name': 'APT29'}
            }
        ]
        relationships = []
        
        agent._post_transform(nodes, relationships)
        
        # Check that node properties were updated
        threat_actor_node = nodes[0]
        props = threat_actor_node['properties']
        assert props['description'] == 'Russian state-sponsored threat actor'
        assert props['sophistication'] == 'high'
        assert props['primary_motivation'] == 'espionage'
        assert props['aliases'] == ['Cozy Bear', 'The Dukes']
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_post_transform_malware_enrichment(self, mock_consumer, mock_http_client):
        """Test post-transformation Malware enrichment."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock enrichment API response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            'name': 'Emotet',
            'description': 'Banking trojan and malware downloader',
            'malware_types': ['trojan', 'downloader'],
            'is_family': True,
            'first_seen': '2014-01-01',
            'last_seen': '2021-01-01',
            'capabilities_description': 'Banking credential theft and malware delivery'
        }
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        nodes = [
            {
                'id': 'malware--emotet',
                'labels': ['Malware'],
                'properties': {'name': 'Emotet'}
            }
        ]
        relationships = []
        
        agent._post_transform(nodes, relationships)
        
        # Check that node properties were updated
        malware_node = nodes[0]
        props = malware_node['properties']
        assert props['description'] == 'Banking trojan and malware downloader'
        assert props['malware_types'] == ['trojan', 'downloader']
        assert props['is_family'] is True
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_post_transform_campaign_enrichment(self, mock_consumer, mock_http_client):
        """Test post-transformation Campaign enrichment."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock enrichment API response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            'id': 'campaign--aurora',
            'name': 'Operation Aurora',
            'description': 'Cyber espionage campaign',
            'objective': 'Intellectual property theft',
            'first_seen': '2009-01-01',
            'last_seen': '2010-01-01',
            'targeted_sectors': ['technology', 'government']
        }
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        nodes = [
            {
                'id': 'campaign--aurora',
                'labels': ['Campaign'],
                'properties': {'name': 'Operation Aurora'}
            }
        ]
        relationships = []
        
        agent._post_transform(nodes, relationships)
        
        # Check that node properties were updated
        campaign_node = nodes[0]
        props = campaign_node['properties']
        assert props['description'] == 'Cyber espionage campaign'
        assert props['objective'] == 'Intellectual property theft'
        assert props['targeted_sectors'] == ['technology', 'government']
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_post_transform_vulnerability_enrichment(self, mock_consumer, mock_http_client):
        """Test post-transformation Vulnerability enrichment."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock enrichment API response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            'id': 'CVE-2021-34527',
            'name': 'PrintNightmare',
            'description': 'Windows Print Spooler vulnerability',
            'cvss_v2_score': 9.0,
            'cvss_v2_vector': 'AV:N/AC:L/Au:S/C:C/I:C/A:C',
            'cvss_v3_score': 8.8,
            'cvss_v3_vector': 'CVSS:3.1/AV:N/AC:L/PR:L/UI:N/S:U/C:H/I:H/A:H',
            'published_date': '2021-07-01',
            'modified_date': '2021-07-15'
        }
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        nodes = [
            {
                'id': 'CVE-2021-34527',
                'labels': ['Vulnerability'],
                'properties': {'id': 'CVE-2021-34527'}
            }
        ]
        relationships = []
        
        agent._post_transform(nodes, relationships)
        
        # Check that node properties were updated
        vuln_node = nodes[0]
        props = vuln_node['properties']
        assert props['name'] == 'PrintNightmare'
        assert props['description'] == 'Windows Print Spooler vulnerability'
        assert props['cvss_v3_score'] == 8.8
        assert props['published_date'] == '2021-07-01'
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_post_transform_enrichment_failure_graceful(self, mock_consumer, mock_http_client):
        """Test post-transformation handles enrichment failures gracefully."""
        mock_http_client_instance = Mock()
        mock_http_client.return_value = mock_http_client_instance
        mock_consumer.return_value = Mock()
        
        # Mock enrichment API error
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("API Error")
        mock_http_client_instance.post.return_value = mock_response
        
        agent = GraphIngestionAgent()
        
        nodes = [
            {
                'id': 'APT29',
                'labels': ['ThreatActor'],
                'properties': {'name': 'APT29'}
            }
        ]
        relationships = []
        
        # Should not raise exception
        agent._post_transform(nodes, relationships)
        
        # Original properties should remain unchanged
        threat_actor_node = nodes[0]
        assert threat_actor_node['properties'] == {'name': 'APT29'}


class TestGraphIngestionAgentLLMProcessing:
    """Test suite for LLM processing functionality."""
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', True)
    @patch('processing_agent.graph_ingestion_agent.Gemini')
    def test_should_enrich_node_article_sufficient_text(self, mock_gemini, mock_consumer, mock_http_client):
        """Test should_enrich_node returns True for Article with sufficient text."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        mock_gemini.return_value = Mock()
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        node = {
            'labels': ['Article'],
            'properties': {
                'full_text': 'A' * 400  # Sufficient length
            }
        }
        
        result = agent._should_enrich_node(node)
        assert result is True
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', True)
    @patch('processing_agent.graph_ingestion_agent.Gemini')
    def test_should_enrich_node_article_insufficient_text(self, mock_gemini, mock_consumer, mock_http_client):
        """Test should_enrich_node returns False for Article with insufficient text."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        mock_gemini.return_value = Mock()
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        node = {
            'labels': ['Article'],
            'properties': {
                'full_text': 'Short text'  # Insufficient length
            }
        }
        
        result = agent._should_enrich_node(node)
        assert result is False
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', True)
    @patch('processing_agent.graph_ingestion_agent.Gemini')
    def test_should_enrich_node_threat_actor_sufficient_description(self, mock_gemini, mock_consumer, mock_http_client):
        """Test should_enrich_node returns True for ThreatActor with sufficient description."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        mock_gemini.return_value = Mock()
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        node = {
            'labels': ['ThreatActor'],
            'properties': {
                'description': 'A' * 200  # Sufficient length
            }
        }
        
        result = agent._should_enrich_node(node)
        assert result is True
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', True)
    @patch('processing_agent.graph_ingestion_agent.Gemini')
    def test_should_enrich_node_other_labels(self, mock_gemini, mock_consumer, mock_http_client):
        """Test should_enrich_node returns False for other node labels."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        mock_gemini.return_value = Mock()
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        node = {
            'labels': ['Indicator'],
            'properties': {
                'value': '192.168.1.1'
            }
        }
        
        result = agent._should_enrich_node(node)
        assert result is False
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', True)
    @patch('processing_agent.graph_ingestion_agent.Gemini')
    @patch('asyncio.run')
    def test_llm_process_text_success(self, mock_asyncio_run, mock_gemini, mock_consumer, mock_http_client):
        """Test successful LLM text processing."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        # Mock Gemini response
        mock_response = Mock()
        mock_response.text = "Processed summary text"
        mock_asyncio_run.return_value = mock_response
        
        mock_gemini_instance = Mock()
        mock_gemini.return_value = mock_gemini_instance
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        result = agent._llm_process_text(
            "Input text for processing",
            "Provide a summary"
        )
        
        assert result == "Processed summary text"
        mock_asyncio_run.assert_called_once()
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', True)
    @patch('processing_agent.graph_ingestion_agent.Gemini')
    @patch('asyncio.run')
    def test_llm_process_text_error(self, mock_asyncio_run, mock_gemini, mock_consumer, mock_http_client):
        """Test LLM text processing with error."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        # Mock asyncio error
        mock_asyncio_run.side_effect = Exception("LLM processing error")
        
        mock_gemini_instance = Mock()
        mock_gemini.return_value = mock_gemini_instance
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        result = agent._llm_process_text(
            "Input text for processing",
            "Provide a summary"
        )
        
        assert "Error processing text" in result
        assert "LLM processing error" in result
    
    @patch.dict('os.environ', {'CTI_BACKEND_URL': 'http://test-backend:8000/api'})
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    def test_llm_process_text_disabled(self, mock_consumer, mock_http_client):
        """Test LLM text processing when LLM is disabled."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        agent = GraphIngestionAgent(enrich_with_llm=False)
        
        result = agent._llm_process_text(
            "Input text for processing",
            "Provide a summary"
        )
        
        assert "LLM summary not available for: Input text for processing" in result
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', True)
    @patch('processing_agent.graph_ingestion_agent.Gemini')
    def test_post_transform_with_llm_enrichment(self, mock_gemini, mock_consumer, mock_http_client):
        """Test post-transformation with LLM enrichment."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        mock_gemini_instance = Mock()
        mock_gemini.return_value = mock_gemini_instance
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        # Mock LLM processing
        with patch.object(agent, '_llm_process_text', return_value="LLM generated summary"):
            nodes = [
                {
                    'id': 'article-1',
                    'labels': ['Article'],
                    'properties': {
                        'full_text': 'A' * 400  # Sufficient length for LLM processing
                    }
                }
            ]
            relationships = []
            
            agent._post_transform(nodes, relationships)
            
            # Check that LLM summary was added
            article_node = nodes[0]
            assert article_node['properties']['llm_summary'] == "LLM generated summary"
    
    @patch.dict('os.environ', {
        'CTI_BACKEND_URL': 'http://test-backend:8000/api',
        'GOOGLE_API_KEY': 'test-api-key'
    })
    @patch('processing_agent.graph_ingestion_agent.AgentHttpClient')
    @patch('processing_agent.graph_ingestion_agent.KafkaConsumer')
    @patch('processing_agent.graph_ingestion_agent.GOOGLE_ADK_AVAILABLE', True)
    @patch('processing_agent.graph_ingestion_agent.Gemini')
    def test_post_transform_llm_enrichment_error_handling(self, mock_gemini, mock_consumer, mock_http_client):
        """Test post-transformation handles LLM enrichment errors gracefully."""
        mock_http_client.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        mock_gemini_instance = Mock()
        mock_gemini.return_value = mock_gemini_instance
        
        agent = GraphIngestionAgent(enrich_with_llm=True)
        
        # Mock LLM processing error
        with patch.object(agent, '_llm_process_text', side_effect=Exception("LLM error")):
            nodes = [
                {
                    'id': 'article-1',
                    'labels': ['Article'],
                    'properties': {
                        'full_text': 'A' * 400
                    }
                }
            ]
            relationships = []
            
            # Should not raise exception
            agent._post_transform(nodes, relationships)
            
            # Original properties should remain unchanged
            article_node = nodes[0]
            assert 'llm_summary' not in article_node['properties']


class TestPydanticModels:
    """Test suite for Pydantic model validation."""
    
    def test_attack_pattern_details_validation(self):
        """Test AttackPatternDetails model validation."""
        data = {
            'id': 'T1566.001',
            'name': 'Spear Phishing Attachment',
            'description': 'Adversaries may use spear phishing...',
            'mitre_attack_id': 'T1566.001',
            'mitre_url': 'https://attack.mitre.org/techniques/T1566/001/',
            'detection': 'Monitor for suspicious email attachments',
            'mitigation': 'Implement email security controls',
            'platforms': ['Windows', 'Linux'],
            'data_sources': ['Email', 'File monitoring']
        }
        
        details = AttackPatternDetails(**data)
        assert details.id == 'T1566.001'
        assert details.name == 'Spear Phishing Attachment'
        assert details.platforms == ['Windows', 'Linux']
        assert details.aliases == []  # Default empty list
    
    def test_threat_actor_details_validation(self):
        """Test ThreatActorDetails model validation."""
        data = {
            'name': 'APT29',
            'description': 'Russian threat actor',
            'sophistication': 'high',
            'primary_motivation': 'espionage',
            'resource_level': 'government',
            'first_seen': '2008-01-01',
            'last_seen': '2024-01-01',
            'aliases': ['Cozy Bear', 'The Dukes']
        }
        
        details = ThreatActorDetails(**data)
        assert details.name == 'APT29'
        assert details.sophistication == 'high'
        assert details.aliases == ['Cozy Bear', 'The Dukes']
        assert details.associated_ttps == []  # Default empty list
    
    def test_malware_details_validation(self):
        """Test MalwareDetails model validation."""
        data = {
            'name': 'Emotet',
            'description': 'Banking trojan',
            'malware_types': ['trojan', 'downloader'],
            'is_family': True,
            'first_seen': '2014-01-01',
            'last_seen': '2021-01-01',
            'capabilities_description': 'Banking credential theft and malware delivery'
        }
        
        details = MalwareDetails(**data)
        assert details.name == 'Emotet'
        assert details.malware_types == ['trojan', 'downloader']
        assert details.is_family is True
        assert details.aliases == []  # Default empty list
    
    def test_campaign_details_validation(self):
        """Test CampaignDetails model validation."""
        data = {
            'id': 'campaign--aurora',
            'name': 'Operation Aurora',
            'description': 'Cyber espionage campaign',
            'objective': 'Intellectual property theft',
            'first_seen': '2009-01-01',
            'last_seen': '2010-01-01',
            'targeted_sectors': ['technology', 'government']
        }
        
        details = CampaignDetails(**data)
        assert details.name == 'Operation Aurora'
        assert details.objective == 'Intellectual property theft'
        assert details.targeted_sectors == ['technology', 'government']
        assert details.aliases == []  # Default empty list
    
    def test_vulnerability_details_validation(self):
        """Test VulnerabilityDetails model validation."""
        data = {
            'id': 'CVE-2021-34527',
            'name': 'PrintNightmare',
            'description': 'Windows Print Spooler vulnerability',
            'cvss_v2_score': 9.0,
            'cvss_v2_vector': 'AV:N/AC:L/Au:S/C:C/I:C/A:C',
            'cvss_v3_score': 8.8,
            'cvss_v3_vector': 'CVSS:3.1/AV:N/AC:L/PR:L/UI:N/S:U/C:H/I:H/A:H',
            'published_date': '2021-07-01',
            'modified_date': '2021-07-15'
        }
        
        details = VulnerabilityDetails(**data)
        assert details.id == 'CVE-2021-34527'
        assert details.name == 'PrintNightmare'
        assert details.cvss_v3_score == 8.8
        assert details.references == []  # Default empty list
    
    def test_model_validation_with_invalid_data(self):
        """Test model validation with invalid data types."""
        with pytest.raises(ValidationError):
            AttackPatternDetails(platforms="should be list not string")
        
        with pytest.raises(ValidationError):
            VulnerabilityDetails(cvss_v3_score="invalid_score")
    
    def test_model_validation_allows_none_values(self):
        """Test model validation explicitly allows None for optional fields."""
        data = {
            'id': None,
            'name': 'Test Attack Pattern',
            'description': None,
            'mitre_attack_id': None,
            'mitre_url': None,
            'detection': None,
            'mitigation': None,
            'aliases': ['test'],
            'platforms': []
        }
        
        details = AttackPatternDetails(**data)
        assert details.name == 'Test Attack Pattern'
        assert details.id is None
        assert details.description is None
        assert details.aliases == ['test']
        assert details.platforms == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])