"""
Strategic tests for llm_tools.py to achieve 30% coverage.

Tests focus on critical, testable functionality without external LLM dependencies:
- Core data structures (LLMRequest, LLMResponse, LLMError)
- BaseLLMProvider initialization and common patterns
- LLMManager core functionality
- PromptManager utility functions
- Key helper functions and error handling
"""

import pytest
import sys
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from typing import Dict, Any

# Mock external LLM dependencies before importing
mock_dspy = MagicMock()
mock_dspy.DSPY_AVAILABLE = False

mock_genai = MagicMock()
mock_openai = MagicMock()

sys.modules['dspy'] = mock_dspy
sys.modules['google'] = MagicMock()
sys.modules['google.generativeai'] = mock_genai
sys.modules['openai'] = mock_openai

# Now import the module
from common_tools.llm_tools import (
    LLMRequest, LLMResponse, LLMError, BaseLLMProvider,
    LLMManager, PromptManager
)


class TestDataStructures:
    """Test LLM data structures."""
    
    def test_llm_request_creation(self):
        """Test LLMRequest dataclass creation."""
        request = LLMRequest(
            prompt="Test prompt",
            model="gpt-3.5-turbo",
            temperature=0.5,
            max_tokens=100,
            system_prompt="You are a helpful assistant",
            metadata={"source": "test"}
        )
        
        assert request.prompt == "Test prompt"
        assert request.model == "gpt-3.5-turbo"
        assert request.temperature == 0.5
        assert request.max_tokens == 100
        assert request.system_prompt == "You are a helpful assistant"
        assert request.metadata == {"source": "test"}
    
    def test_llm_request_minimal(self):
        """Test LLMRequest with minimal required fields."""
        request = LLMRequest(
            prompt="Minimal prompt",
            model="gpt-4"
        )
        
        assert request.prompt == "Minimal prompt"
        assert request.model == "gpt-4"
        assert request.temperature == 0.7  # Default value
        assert request.max_tokens is None
        assert request.system_prompt is None
        assert request.metadata is None
    
    def test_llm_response_creation(self):
        """Test LLMResponse dataclass creation."""
        timestamp = datetime.now()
        usage = {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
        metadata = {"model_version": "1.0", "finish_reason": "stop"}
        
        response = LLMResponse(
            content="Test response content",
            model="gpt-3.5-turbo",
            usage=usage,
            metadata=metadata,
            timestamp=timestamp,
            request_id="req_123",
            from_cache=True
        )
        
        assert response.content == "Test response content"
        assert response.model == "gpt-3.5-turbo"
        assert response.usage == usage
        assert response.metadata == metadata
        assert response.timestamp == timestamp
        assert response.request_id == "req_123"
        assert response.from_cache is True
    
    def test_llm_error_creation(self):
        """Test LLMError exception creation."""
        error = LLMError(
            message="Test error occurred",
            model="gpt-4",
            error_code="rate_limit_exceeded"
        )
        
        assert str(error) == "Test error occurred"
        assert error.model == "gpt-4"
        assert error.error_code == "rate_limit_exceeded"
    
    def test_llm_error_minimal(self):
        """Test LLMError with minimal parameters."""
        error = LLMError("Simple error")
        
        assert str(error) == "Simple error"
        assert error.model is None
        assert error.error_code is None


class TestBaseLLMProvider:
    """Test BaseLLMProvider abstract base class."""
    
    def test_base_provider_cannot_be_instantiated(self):
        """Test that BaseLLMProvider cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseLLMProvider("test-model")
    
    def test_concrete_provider_initialization(self):
        """Test initialization through a concrete provider."""
        # Create a minimal concrete implementation for testing
        class TestProvider(BaseLLMProvider):
            async def generate_async(self, request):
                return LLMResponse(
                    content="test",
                    model=self.model_name,
                    usage={},
                    metadata={},
                    timestamp=datetime.now()
                )
            
            def generate(self, request):
                return LLMResponse(
                    content="test",
                    model=self.model_name,
                    usage={},
                    metadata={},
                    timestamp=datetime.now()
                )
            
            def is_available(self):
                return True
        
        provider = TestProvider(
            model_name="test-model",
            temperature=0.8,
            max_tokens=500,
            timeout=30
        )
        
        assert provider.model_name == "test-model"
        assert provider.default_temperature == 0.8
        assert provider.default_max_tokens == 500
        assert provider.timeout == 30
        assert provider.logger is not None
        
        # Test default stats
        expected_stats = {
            'requests_made': 0,
            'requests_successful': 0,
            'requests_failed': 0,
            'total_tokens_used': 0,
            'total_cost': 0.0,
            'cache_hits': 0
        }
        assert provider.stats == expected_stats
    
    def test_provider_default_configuration(self):
        """Test provider with default configuration."""
        class TestProvider(BaseLLMProvider):
            async def generate_async(self, request):
                pass
            
            def generate(self, request):
                pass
            
            def is_available(self):
                return True
        
        provider = TestProvider("default-model")
        
        assert provider.model_name == "default-model"
        assert provider.default_temperature == 0.7
        assert provider.default_max_tokens == 1000
        assert provider.timeout == 60


class TestLLMManager:
    """Test LLMManager core functionality."""
    
    def test_llm_manager_initialization(self):
        """Test LLMManager initialization."""
        manager = LLMManager()
        
        assert manager.providers == {}
        assert manager.default_provider is None
        assert manager.logger is not None
        assert hasattr(manager, 'cache_enabled')
        assert hasattr(manager, 'global_stats')
        
        # Test global_stats initialization
        expected_stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'provider_failures': 0,
            'fallback_uses': 0
        }
        assert manager.global_stats == expected_stats
    
    def test_add_provider(self):
        """Test adding providers to LLMManager."""
        manager = LLMManager()
        
        # Mock provider
        mock_provider = Mock()
        mock_provider.model_name = "test-model"
        
        manager.add_provider("test", mock_provider)
        
        assert "test" in manager.providers
        assert manager.providers["test"] == mock_provider
    
    def test_provider_access(self):
        """Test accessing providers from LLMManager."""
        manager = LLMManager()
        
        # Mock provider
        mock_provider = Mock()
        mock_provider.model_name = "test-model"
        
        manager.add_provider("test", mock_provider)
        
        # Test that provider was added
        assert "test" in manager.providers
        assert manager.providers["test"] == mock_provider
    
    def test_manager_configuration(self):
        """Test LLMManager configuration options."""
        # Test with cache configuration
        manager = LLMManager(cache_ttl=7200)
        
        assert manager.cache_ttl == 7200
        assert manager.cache_enabled is False  # No redis_client provided
        assert manager.redis_client is None
        
        # Test fallback order exists
        assert hasattr(manager, 'fallback_order')
        assert isinstance(manager.fallback_order, list)


class TestPromptManager:
    """Test PromptManager utility functionality."""
    
    def test_prompt_manager_initialization(self):
        """Test PromptManager initialization."""
        llm_manager = LLMManager()
        manager = PromptManager(llm_manager)
        
        assert manager.llm_manager == llm_manager
        assert hasattr(manager, 'templates')
        assert manager.logger is not None
    
    def test_add_template(self):
        """Test adding prompt templates."""
        llm_manager = LLMManager()
        manager = PromptManager(llm_manager)
        
        template = {"prompt": "Hello {name}, you are a {role}."}
        manager.add_template("greeting", template)
        
        assert "greeting" in manager.templates
        assert manager.templates["greeting"] == template
    
    def test_template_storage(self):
        """Test that templates are stored correctly."""
        llm_manager = LLMManager()
        manager = PromptManager(llm_manager)
        
        # Test basic template storage
        template1 = {"prompt": "Template 1 content"}
        template2 = {"prompt": "Template 2 content"}
        
        manager.add_template("test1", template1)
        manager.add_template("test2", template2)
        
        assert "test1" in manager.templates
        assert "test2" in manager.templates
        assert manager.templates["test1"] == template1
        assert manager.templates["test2"] == template2


class TestUtilityFunctions:
    """Test utility functions and error handling."""
    
    def test_llm_response_timing(self):
        """Test LLM response with timing information."""
        start_time = time.time()
        time.sleep(0.01)  # Small delay
        end_time = time.time()
        
        response = LLMResponse(
            content="Timed response",
            model="test-model",
            usage={"total_tokens": 50},
            metadata={"response_time": end_time - start_time},
            timestamp=datetime.fromtimestamp(end_time)
        )
        
        assert response.content == "Timed response"
        assert response.metadata["response_time"] > 0
        assert isinstance(response.timestamp, datetime)
    
    def test_error_handling_patterns(self):
        """Test common error handling patterns."""
        # Test various error scenarios
        errors = [
            LLMError("Rate limit exceeded", "gpt-4", "rate_limit"),
            LLMError("Invalid API key", "gemini-pro", "auth_error"),
            LLMError("Model not found", error_code="model_error"),
            LLMError("Network timeout")
        ]
        
        for error in errors:
            assert isinstance(error, Exception)
            assert isinstance(error, LLMError)
            assert len(str(error)) > 0
    
    def test_request_response_compatibility(self):
        """Test that LLMRequest and LLMResponse work together."""
        request = LLMRequest(
            prompt="Test compatibility",
            model="test-model",
            temperature=0.5,
            metadata={"test": True}
        )
        
        response = LLMResponse(
            content="Compatible response",
            model=request.model,  # Use same model
            usage={"tokens": 25},
            metadata={"request_metadata": request.metadata},
            timestamp=datetime.now()
        )
        
        # Verify compatibility
        assert response.model == request.model
        assert response.metadata["request_metadata"] == request.metadata
        assert isinstance(response.timestamp, datetime)


class TestConfigurationAndStats:
    """Test configuration and statistics tracking."""
    
    def test_manager_statistics_tracking(self):
        """Test that LLMManager tracks statistics correctly."""
        manager = LLMManager()
        
        # Initial global_stats should be zero
        assert manager.global_stats["total_requests"] == 0
        assert manager.global_stats["cache_hits"] == 0
        assert manager.global_stats["provider_failures"] == 0
        assert manager.global_stats["fallback_uses"] == 0
        
        # Test stats structure
        assert isinstance(manager.global_stats, dict)
        assert "total_requests" in manager.global_stats
        assert "cache_hits" in manager.global_stats
    
    def test_provider_stats_update(self):
        """Test provider statistics updates."""
        class TestProvider(BaseLLMProvider):
            async def generate_async(self, request):
                pass
            
            def generate(self, request):
                pass
            
            def is_available(self):
                return True
        
        provider = TestProvider("test-model")
        
        # Test manual stats update
        provider.stats["requests_made"] += 1
        provider.stats["requests_successful"] += 1
        provider.stats["total_tokens_used"] += 100
        
        assert provider.stats["requests_made"] == 1
        assert provider.stats["requests_successful"] == 1
        assert provider.stats["total_tokens_used"] == 100
        assert provider.stats["requests_failed"] == 0


if __name__ == "__main__":
    pytest.main([__file__])