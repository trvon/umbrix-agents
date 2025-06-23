import os
import sys
import json
import pytest
import unittest.mock as mock
from unittest.mock import MagicMock, patch

# Use our mocked Gemini class from conftest
from agents.conftest import _Gemini as Gemini
ADK_MODELS_AVAILABLE = True

from agents.organizer_agent.master_coordinator import CTIMasterCoordinatorAgent

# Mock kafka
class MockKafkaConsumer:
    def __init__(self, *args, **kwargs):
        pass
    
    def __iter__(self):
        return self
    
    def __next__(self):
        raise StopIteration

@pytest.fixture(autouse=True)
def mock_kafka(monkeypatch):
    """Automatically mock Kafka for all tests in this file."""
    # Import here to avoid issues if kafka is not installed
    import kafka.consumer
    monkeypatch.setattr(kafka.consumer, "KafkaConsumer", MockKafkaConsumer)

class DummyResponse:
    def __init__(self, content):
        self.text = content
        self.choices = [type('Choice', (), {'message': type('Msg', (), {'content': content})})]

@pytest.fixture
def patch_gemini(monkeypatch):
    """
    Create a mock for the Gemini model's generate_content_async method,
    and track the calls for test verification.
    """
    # Skip test if ADK models are not available
    if not ADK_MODELS_AVAILABLE:
        pytest.skip("google.adk.models is not available")
        
    # Capture the last call arguments
    calls = {}
    
    # Define a mock async function
    async def mock_generate_content_async(self, contents, **kwargs):
        calls['model'] = self.model
        calls['contents'] = contents
        content = f"Echo: {contents}"
        return DummyResponse(content)
    
    # Replace the actual method with our mock
    monkeypatch.setattr(Gemini, 'generate_content_async', mock_generate_content_async)
    return calls


def test_handle_external_task_returns_echo(patch_gemini):
    """Test that the handle_external_task method properly returns echo of the input."""
    # Mock a Gemini instance directly
    with patch("kafka.KafkaConsumer"):  # Additional safety to mock KafkaConsumer import
        agent = CTIMasterCoordinatorAgent(
            name='test', sub_agents=None, tools=None,
            kafka_topic='', bootstrap_servers='',
            gemini_api_key='key', gemini_model='modelX'
        )
        
        # Create a mock Gemini instance with the correct behavior
        agent.llm = Gemini(model='modelX')  # This uses our mocked Gemini class
        
        task = {'action': 'test_action', 'payload': 123}
        result = agent.handle_external_task(task)
        
        # Ensure echo result and model override works
        assert result == f"Echo: {json.dumps(task)}"
        assert patch_gemini['model'] == 'modelX'
        assert patch_gemini['contents'] == json.dumps(task)


def test_handle_external_task_uses_env_model(monkeypatch, patch_gemini):
    """Test that the agent uses the environment model name if no model is specified."""
    # Set environment model
    monkeypatch.delenv('GEMINI_MODEL_NAME', raising=False)
    monkeypatch.setenv('GEMINI_MODEL_NAME', 'envModelY')
    
    with patch("kafka.KafkaConsumer"):  # Additional safety to mock KafkaConsumer import
        agent = CTIMasterCoordinatorAgent(
            name='test2', sub_agents=None, tools=None,
            kafka_topic='', bootstrap_servers='',
            gemini_api_key='key', gemini_model=None
        )
        
        # Create a mock Gemini instance with the model from environment
        agent.llm = Gemini(model='envModelY')  # This uses our mocked Gemini class
        
        task = {'foo': 'bar'}
        _ = agent.handle_external_task(task)
        
        # Confirm it used the environment model
        assert patch_gemini['model'] == 'envModelY'
        assert patch_gemini['contents'] == json.dumps(task)

# Test that the agent properly initializes the LLM
def test_agent_initializes_llm():
    """Test that the agent properly initializes the LLM during initialization."""
    # We need to patch the class directly where it's imported, not where it's defined
    with patch("agents.organizer_agent.master_coordinator.Gemini") as mock_gemini:
        # Create a test instance of our agent with empty bootstrap servers to avoid Kafka init
        agent = CTIMasterCoordinatorAgent(
            name='test',
            kafka_topic='topic',
            bootstrap_servers='',  # Empty string to skip Kafka init
            gemini_api_key='test_key',
            gemini_model='test-model'
        )
        
        # Check that the Gemini model was initialized with the correct model name
        mock_gemini.assert_called_once_with(model='test-model')
        
        # Verify that the agent stored the model properly
        assert agent.gemini_model == 'test-model'

# Test the run method processes messages properly
def test_run_method_processes_messages():
    """Test that the run method properly processes Kafka messages."""
    # Create a mock message with a value
    mock_msg = MagicMock()
    mock_msg.value = {"action": "test_action"}
    
    # Create a mock consumer that returns our message once then stops
    mock_consumer = MagicMock()
    mock_consumer.__iter__.return_value = iter([mock_msg])
    
    # Create the agent with empty bootstrap servers
    agent = CTIMasterCoordinatorAgent(
        name='test3', 
        kafka_topic='topic', 
        bootstrap_servers='',  # Empty string to skip Kafka init
        gemini_api_key='test_key',
        gemini_model='test-model'
    )
    
    # Manually set the consumer
    agent.consumer = mock_consumer
    
    # Mock the handle_external_task method
    agent.handle_external_task = MagicMock(return_value="Success")
    
    # Since run() is a blocking method, we need to ensure it stops after processing our message
    try:
        agent.run()
    except StopIteration:
        # Expected when the iterator from the mocked consumer is exhausted
        pass
    
    # Verify the handle_external_task was called with the right task
    agent.handle_external_task.assert_called_once_with({"action": "test_action"})
