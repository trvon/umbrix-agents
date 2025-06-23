"""
Comprehensive tests for llm_tools.py to improve coverage from 36% to 70%.

This file focuses on testing the missing functionality:
- GeminiProvider and OpenAIProvider classes
- LLMManager advanced functionality (caching, fallbacks, provider management)
- DSPyIntegration class
- PromptManager execution methods
- Error handling scenarios
- Provider availability checks
"""

import pytest
import asyncio
import json
import hashlib
import time
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from typing import Dict, Any

# Mock external dependencies before importing
mock_dspy = MagicMock()
mock_dspy.DSPY_AVAILABLE = False

mock_genai = MagicMock()
mock_openai = MagicMock()

import sys
sys.modules['dspy'] = mock_dspy
sys.modules['google'] = MagicMock()
sys.modules['google.generativeai'] = mock_genai
sys.modules['openai'] = mock_openai

# Now import the module
from common_tools.llm_tools import (
    LLMRequest, LLMResponse, LLMError, BaseLLMProvider,
    GeminiProvider, OpenAIProvider, LLMManager, DSPyIntegration,
    PromptManager, PROMPT_TEMPLATES
)


class TestGeminiProvider:
    """Test GeminiProvider functionality."""
    
    def test_gemini_provider_initialization_when_not_available(self):
        """Test GeminiProvider when google-generativeai is not available."""
        # This tests lines 128-129 (ImportError path)
        with patch('common_tools.llm_tools.GEMINI_AVAILABLE', False):
            with pytest.raises(ImportError, match="google-generativeai package is required"):
                GeminiProvider(api_key="test-key", model_name="gemini-pro")
    
    def test_gemini_provider_initialization_success(self):
        """Test successful GeminiProvider initialization."""
        # This tests lines 127-146 (successful initialization)
        with patch('common_tools.llm_tools.GEMINI_AVAILABLE', True):
            with patch('common_tools.llm_tools.genai') as mock_genai:
                mock_model = Mock()
                mock_genai.GenerativeModel.return_value = mock_model
                
                provider = GeminiProvider(
                    api_key="test-api-key",
                    model_name="gemini-1.5-flash",
                    temperature=0.8,
                    safety_settings=[{"category": "test"}]
                )
                
                # Should have configured genai and created model
                mock_genai.configure.assert_called_once_with(api_key="test-api-key")
                mock_genai.GenerativeModel.assert_called_once_with("gemini-1.5-flash")
                
                # Should have set up pricing and safety settings
                assert provider.pricing['input_tokens'] == 0.00001
                assert provider.safety_settings == [{"category": "test"}]
                assert provider.model == mock_model
    
    def test_gemini_provider_is_available(self):
        """Test Gemini provider availability check."""
        # This tests lines 148-150 (is_available method)
        with patch('common_tools.llm_tools.GEMINI_AVAILABLE', True):
            with patch('common_tools.llm_tools.genai'):
                provider = GeminiProvider(api_key="test-key")
                assert provider.is_available() is True
                
        # Test with None API key - need to create provider first then modify
        with patch('common_tools.llm_tools.GEMINI_AVAILABLE', True):
            with patch('common_tools.llm_tools.genai'):
                provider = GeminiProvider(api_key="test-key")
                provider.api_key = None
                assert provider.is_available() is False
    
    @pytest.mark.asyncio
    async def test_gemini_generate_async_success(self):
        """Test successful Gemini async generation."""
        # This tests lines 152-208 (generate_async success path)
        with patch('common_tools.llm_tools.GEMINI_AVAILABLE', True):
            with patch('common_tools.llm_tools.genai') as mock_genai:
                # Mock the model and response
                mock_model = Mock()
                mock_genai.GenerativeModel.return_value = mock_model
                
                # Mock response object
                mock_response = Mock()
                mock_response.text = "Generated response text"
                mock_response.usage_metadata = Mock()
                mock_response.usage_metadata.prompt_token_count = 10
                mock_response.usage_metadata.candidates_token_count = 20
                mock_response.usage_metadata.total_token_count = 30
                mock_response.candidates = [Mock()]
                mock_response.candidates[0].finish_reason = "stop"
                
                mock_model.generate_content_async = AsyncMock(return_value=mock_response)
                
                provider = GeminiProvider(api_key="test-key")
                
                request = LLMRequest(
                    prompt="Test prompt",
                    model="gemini-1.5-flash",
                    temperature=0.7,
                    max_tokens=100,
                    system_prompt="You are helpful",
                    metadata={"request_id": "test-123"}
                )
                
                response = await provider.generate_async(request)
                
                # Should have called the model correctly
                mock_model.generate_content_async.assert_called_once()
                call_args = mock_model.generate_content_async.call_args
                assert "You are helpful\n\nTest prompt" in call_args[0][0]
                
                # Should have returned proper response
                assert response.content == "Generated response text"
                assert response.model == "gemini-1.5-flash"
                assert response.usage['input_tokens'] == 10
                assert response.usage['output_tokens'] == 20
                assert response.usage['total_tokens'] == 30
                assert response.metadata['provider'] == 'gemini'
                assert response.metadata['finish_reason'] == 'stop'
                assert response.request_id == "test-123"
                
                # Should have updated stats
                assert provider.stats['requests_made'] == 1
                assert provider.stats['requests_successful'] == 1
                assert provider.stats['total_tokens_used'] == 30
    
    @pytest.mark.asyncio
    async def test_gemini_generate_async_error(self):
        """Test Gemini async generation error handling."""
        # This tests lines 210-213 (generate_async error path)
        with patch('common_tools.llm_tools.GEMINI_AVAILABLE', True):
            with patch('common_tools.llm_tools.genai') as mock_genai:
                mock_model = Mock()
                mock_genai.GenerativeModel.return_value = mock_model
                mock_model.generate_content_async = AsyncMock(side_effect=Exception("API Error"))
                
                provider = GeminiProvider(api_key="test-key")
                request = LLMRequest(prompt="Test prompt", model="gemini-pro")
                
                with pytest.raises(LLMError, match="Gemini generation failed"):
                    await provider.generate_async(request)
                
                # Should have updated error stats
                assert provider.stats['requests_made'] == 1
                assert provider.stats['requests_failed'] == 1
    
    def test_gemini_generate_sync(self):
        """Test Gemini synchronous generation."""
        # This tests lines 215-222 (generate sync method)
        with patch('common_tools.llm_tools.GEMINI_AVAILABLE', True):
            with patch('common_tools.llm_tools.genai'):
                provider = GeminiProvider(api_key="test-key")
                
                # Mock the async method to return a response
                mock_response = LLMResponse(
                    content="sync response",
                    model="gemini-pro",
                    usage={"total_tokens": 10},
                    metadata={},
                    timestamp=datetime.now()
                )
                provider.generate_async = AsyncMock(return_value=mock_response)
                
                request = LLMRequest(prompt="Test prompt", model="gemini-pro")
                result = provider.generate(request)
                
                assert result.content == "sync response"
                provider.generate_async.assert_called_once_with(request)


class TestOpenAIProvider:
    """Test OpenAIProvider functionality."""
    
    def test_openai_provider_initialization_when_not_available(self):
        """Test OpenAIProvider when openai package is not available."""
        # This tests lines 233-234 (ImportError path)
        with patch('common_tools.llm_tools.OPENAI_AVAILABLE', False):
            with pytest.raises(ImportError, match="openai package is required"):
                OpenAIProvider(api_key="test-key")
    
    def test_openai_provider_initialization_success(self):
        """Test successful OpenAIProvider initialization."""
        # This tests lines 232-249 (successful initialization)
        with patch('common_tools.llm_tools.OPENAI_AVAILABLE', True):
            with patch('common_tools.llm_tools.openai') as mock_openai:
                mock_async_client = Mock()
                mock_sync_client = Mock()
                mock_openai.AsyncOpenAI.return_value = mock_async_client
                mock_openai.OpenAI.return_value = mock_sync_client
                
                provider = OpenAIProvider(
                    api_key="test-api-key",
                    model_name="gpt-4",
                    organization="test-org"
                )
                
                # Should have created clients
                mock_openai.AsyncOpenAI.assert_called_once_with(api_key="test-api-key")
                mock_openai.OpenAI.assert_called_once_with(api_key="test-api-key")
                
                # Should have set configuration
                assert provider.client == mock_async_client
                assert provider.sync_client == mock_sync_client
                assert provider.organization == "test-org"
                assert "gpt-4" in provider.pricing
    
    def test_openai_provider_is_available(self):
        """Test OpenAI provider availability check."""
        # This tests lines 251-253 (is_available method)
        with patch('common_tools.llm_tools.OPENAI_AVAILABLE', True):
            with patch('common_tools.llm_tools.openai'):
                provider = OpenAIProvider(api_key="test-key")
                assert provider.is_available() is True
                
        # Test when OpenAI not available by modifying the flag after creation
        with patch('common_tools.llm_tools.OPENAI_AVAILABLE', True):
            with patch('common_tools.llm_tools.openai'):
                provider = OpenAIProvider(api_key="test-key")
                # Test the availability logic
                with patch('common_tools.llm_tools.OPENAI_AVAILABLE', False):
                    assert provider.is_available() is False
    
    @pytest.mark.asyncio
    async def test_openai_generate_async_success(self):
        """Test successful OpenAI async generation."""
        # This tests lines 255-309 (generate_async success path)
        with patch('common_tools.llm_tools.OPENAI_AVAILABLE', True):
            with patch('common_tools.llm_tools.openai') as mock_openai:
                # Mock the client and response
                mock_client = Mock()
                mock_openai.AsyncOpenAI.return_value = mock_client
                
                mock_response = Mock()
                mock_response.choices = [Mock()]
                mock_response.choices[0].message.content = "OpenAI response"
                mock_response.choices[0].finish_reason = "stop"
                mock_response.usage = Mock()
                mock_response.usage.prompt_tokens = 15
                mock_response.usage.completion_tokens = 25
                mock_response.usage.total_tokens = 40
                
                mock_client.chat.completions.create = AsyncMock(return_value=mock_response)
                
                provider = OpenAIProvider(api_key="test-key", model_name="gpt-3.5-turbo")
                
                request = LLMRequest(
                    prompt="Test prompt",
                    model="gpt-3.5-turbo",
                    temperature=0.8,
                    max_tokens=150,
                    system_prompt="You are an assistant"
                )
                
                response = await provider.generate_async(request)
                
                # Should have called API correctly
                mock_client.chat.completions.create.assert_called_once()
                call_kwargs = mock_client.chat.completions.create.call_args[1]
                assert call_kwargs['model'] == 'gpt-3.5-turbo'
                assert call_kwargs['temperature'] == 0.8
                assert call_kwargs['max_tokens'] == 150
                assert len(call_kwargs['messages']) == 2
                assert call_kwargs['messages'][0]['role'] == 'system'
                assert call_kwargs['messages'][1]['role'] == 'user'
                
                # Should have returned proper response
                assert response.content == "OpenAI response"
                assert response.usage['input_tokens'] == 15
                assert response.usage['output_tokens'] == 25
                assert response.metadata['provider'] == 'openai'
                
                # Should have updated stats
                assert provider.stats['requests_made'] == 1
                assert provider.stats['requests_successful'] == 1
    
    def test_openai_generate_sync_success(self):
        """Test successful OpenAI sync generation."""
        # This tests lines 316-366 (generate sync success path)
        with patch('common_tools.llm_tools.OPENAI_AVAILABLE', True):
            with patch('common_tools.llm_tools.openai') as mock_openai:
                # Mock clients
                mock_async_client = Mock()
                mock_sync_client = Mock()
                mock_openai.AsyncOpenAI.return_value = mock_async_client
                mock_openai.OpenAI.return_value = mock_sync_client
                
                # Mock sync response
                mock_response = Mock()
                mock_response.choices = [Mock()]
                mock_response.choices[0].message.content = "Sync response"
                mock_response.choices[0].finish_reason = "stop"
                mock_response.usage = Mock()
                mock_response.usage.prompt_tokens = 10
                mock_response.usage.completion_tokens = 20
                mock_response.usage.total_tokens = 30
                
                mock_sync_client.chat.completions.create.return_value = mock_response
                
                provider = OpenAIProvider(api_key="test-key")
                
                request = LLMRequest(
                    prompt="Sync test",
                    model="gpt-3.5-turbo",
                    system_prompt="You are helpful"
                )
                
                response = provider.generate(request)
                
                # Should have used sync client
                mock_sync_client.chat.completions.create.assert_called_once()
                
                # Should return proper response
                assert response.content == "Sync response"
                assert response.usage['total_tokens'] == 30
                assert provider.stats['requests_successful'] == 1
    
    def test_openai_generate_sync_error(self):
        """Test OpenAI sync generation error handling."""
        # This tests lines 368-371 (generate sync error path)
        with patch('common_tools.llm_tools.OPENAI_AVAILABLE', True):
            with patch('common_tools.llm_tools.openai') as mock_openai:
                mock_sync_client = Mock()
                mock_openai.OpenAI.return_value = mock_sync_client
                mock_sync_client.chat.completions.create.side_effect = Exception("API Error")
                
                provider = OpenAIProvider(api_key="test-key")
                request = LLMRequest(prompt="Test", model="gpt-3.5-turbo")
                
                with pytest.raises(LLMError, match="OpenAI sync generation failed"):
                    provider.generate(request)
                
                assert provider.stats['requests_failed'] == 1


class TestLLMManagerAdvanced:
    """Test LLMManager advanced functionality."""
    
    def test_llm_manager_set_fallback_order_success(self):
        """Test setting fallback order with valid providers."""
        # This tests lines 410-418 (set_fallback_order success)
        manager = LLMManager()
        
        # Add some mock providers
        provider1 = Mock(spec=BaseLLMProvider)
        provider2 = Mock(spec=BaseLLMProvider)
        manager.add_provider("provider1", provider1)
        manager.add_provider("provider2", provider2)
        
        manager.set_fallback_order(["provider1", "provider2"])
        
        assert manager.fallback_order == ["provider1", "provider2"]
    
    def test_llm_manager_set_fallback_order_invalid_provider(self):
        """Test setting fallback order with invalid provider."""
        # This tests lines 412-415 (validation error path)
        manager = LLMManager()
        
        with pytest.raises(ValueError, match="Unknown provider: invalid_provider"):
            manager.set_fallback_order(["invalid_provider"])
    
    def test_llm_manager_cache_key_generation(self):
        """Test cache key generation."""
        # This tests lines 420-432 (_get_cache_key method)
        manager = LLMManager()
        
        request = LLMRequest(
            prompt="Test prompt",
            model="gpt-3.5-turbo",
            temperature=0.7,
            max_tokens=100,
            system_prompt="System message"
        )
        
        cache_key = manager._get_cache_key(request, "test_provider")
        
        # Should be a proper cache key format
        assert cache_key.startswith("llm_cache:")
        assert len(cache_key) == 42  # "llm_cache:" + 32 hex chars
        
        # Same request should generate same key
        cache_key2 = manager._get_cache_key(request, "test_provider")
        assert cache_key == cache_key2
        
        # Different request should generate different key
        request2 = LLMRequest(prompt="Different prompt", model="gpt-3.5-turbo")
        cache_key3 = manager._get_cache_key(request2, "test_provider")
        assert cache_key != cache_key3
    
    @pytest.mark.asyncio
    async def test_llm_manager_get_cached_response_disabled(self):
        """Test getting cached response when caching is disabled."""
        # This tests lines 434-437 (cache disabled path)
        manager = LLMManager(redis_client=None)
        
        result = await manager._get_cached_response("test_key")
        assert result is None
    
    @pytest.mark.asyncio 
    async def test_llm_manager_get_cached_response_success(self):
        """Test successful cached response retrieval."""
        # This tests lines 439-456 (successful cache retrieval)
        mock_redis = Mock()
        manager = LLMManager(redis_client=mock_redis)
        
        # Mock cached response data
        cached_data = {
            'content': 'Cached response',
            'model': 'gpt-3.5-turbo',
            'usage': {'total_tokens': 50},
            'metadata': {'provider': 'openai'},
            'timestamp': '2023-10-26T10:20:30.123456',
            'request_id': 'test-123'
        }
        
        mock_redis.aget = AsyncMock(return_value=json.dumps(cached_data))
        
        result = await manager._get_cached_response("test_key")
        
        assert result is not None
        assert result.content == 'Cached response'
        assert result.from_cache is True
        assert manager.global_stats['cache_hits'] == 1
    
    @pytest.mark.asyncio
    async def test_llm_manager_get_cached_response_error(self):
        """Test cached response retrieval error handling."""
        # This tests lines 458-461 (cache error path)
        mock_redis = Mock()
        manager = LLMManager(redis_client=mock_redis)
        
        mock_redis.aget = AsyncMock(side_effect=Exception("Redis error"))
        
        result = await manager._get_cached_response("test_key")
        assert result is None
    
    @pytest.mark.asyncio
    async def test_llm_manager_cache_response_disabled(self):
        """Test caching response when caching is disabled."""
        # This tests lines 463-466 (cache disabled path)
        manager = LLMManager(redis_client=None)
        
        response = LLMResponse(
            content="test",
            model="gpt-3.5-turbo",
            usage={},
            metadata={},
            timestamp=datetime.now()
        )
        
        # Should not raise exception
        await manager._cache_response("test_key", response)
    
    @pytest.mark.asyncio
    async def test_llm_manager_cache_response_success(self):
        """Test successful response caching."""
        # This tests lines 468-482 (successful caching)
        mock_redis = Mock()
        manager = LLMManager(redis_client=mock_redis, cache_ttl=1800)
        
        response = LLMResponse(
            content="test response",
            model="gpt-3.5-turbo", 
            usage={'total_tokens': 30},
            metadata={'provider': 'openai'},
            timestamp=datetime.now(),
            request_id="req-123"
        )
        
        mock_redis.aset = AsyncMock()
        
        await manager._cache_response("test_key", response)
        
        # Should have called aset with correct parameters
        mock_redis.aset.assert_called_once()
        call_args = mock_redis.aset.call_args
        assert call_args[0][0] == "test_key"  # key
        assert call_args[1]['ttl'] == 1800  # TTL
        
        # Verify the cached data structure
        cached_data = json.loads(call_args[0][1])
        assert cached_data['content'] == "test response"
        assert cached_data['model'] == "gpt-3.5-turbo"
    
    @pytest.mark.asyncio
    async def test_llm_manager_generate_async_no_default_provider(self):
        """Test generate_async when no provider is specified and no default."""
        # This tests lines 498-500 (no provider error)
        manager = LLMManager()
        
        request = LLMRequest(prompt="test", model="gpt-3.5-turbo")
        
        with pytest.raises(LLMError, match="No provider specified and no default provider set"):
            await manager.generate_async(request)
    
    @pytest.mark.asyncio
    async def test_llm_manager_generate_async_unknown_provider(self):
        """Test generate_async with unknown provider."""
        # This tests lines 502-503 (unknown provider error)
        manager = LLMManager()
        
        request = LLMRequest(prompt="test", model="gpt-3.5-turbo")
        
        with pytest.raises(LLMError, match="Unknown provider: unknown_provider"):
            await manager.generate_async(request, provider_name="unknown_provider")
    
    @pytest.mark.asyncio
    async def test_llm_manager_generate_async_with_cache_hit(self):
        """Test generate_async with successful cache hit."""
        # This tests lines 505-510 (cache hit path)
        mock_redis = Mock()
        manager = LLMManager(redis_client=mock_redis)
        
        # Add a mock provider
        mock_provider = Mock(spec=BaseLLMProvider)
        manager.add_provider("test_provider", mock_provider)
        
        # Mock cache hit
        cached_response = LLMResponse(
            content="cached content",
            model="gpt-3.5-turbo",
            usage={},
            metadata={},
            timestamp=datetime.now(),
            from_cache=True
        )
        
        manager._get_cached_response = AsyncMock(return_value=cached_response)
        
        request = LLMRequest(prompt="test", model="gpt-3.5-turbo")
        result = await manager.generate_async(request, provider_name="test_provider")
        
        assert result.from_cache is True
        assert result.content == "cached content"
        assert manager.global_stats['total_requests'] == 1
    
    @pytest.mark.asyncio
    async def test_llm_manager_generate_async_provider_unavailable(self):
        """Test generate_async when provider is unavailable."""
        # This tests lines 525-527 (provider unavailable path)
        manager = LLMManager()
        
        # Add unavailable provider
        mock_provider = Mock(spec=BaseLLMProvider)
        mock_provider.is_available.return_value = False
        manager.add_provider("unavailable_provider", mock_provider)
        
        request = LLMRequest(prompt="test", model="gpt-3.5-turbo")
        
        with pytest.raises(LLMError, match="All providers failed"):
            await manager.generate_async(request, provider_name="unavailable_provider")
    
    @pytest.mark.asyncio
    async def test_llm_manager_generate_async_with_fallbacks(self):
        """Test generate_async with fallback providers."""
        # This tests lines 512-515, 547-550 (fallback logic)
        manager = LLMManager()
        
        # Add providers
        failing_provider = Mock(spec=BaseLLMProvider)
        failing_provider.is_available.return_value = True
        failing_provider.generate_async = AsyncMock(side_effect=Exception("Provider failed"))
        
        success_provider = Mock(spec=BaseLLMProvider)
        success_provider.is_available.return_value = True
        success_provider.model_name = "gpt-3.5-turbo"
        mock_response = LLMResponse(
            content="fallback response",
            model="gpt-3.5-turbo",
            usage={},
            metadata={},
            timestamp=datetime.now()
        )
        success_provider.generate_async = AsyncMock(return_value=mock_response)
        
        manager.add_provider("failing_provider", failing_provider)
        manager.add_provider("success_provider", success_provider)
        manager.set_fallback_order(["failing_provider", "success_provider"])
        
        request = LLMRequest(prompt="test", model="gpt-3.5-turbo")
        result = await manager.generate_async(
            request, 
            provider_name="failing_provider",
            use_cache=False
        )
        
        assert result.content == "fallback response"
        assert manager.global_stats['fallback_uses'] == 1
        assert manager.global_stats['provider_failures'] == 1
    
    def test_llm_manager_get_provider_stats(self):
        """Test getting provider statistics."""
        # This tests lines 580-585 (get_provider_stats method)
        manager = LLMManager()
        
        # Add providers with stats
        provider1 = Mock(spec=BaseLLMProvider)
        provider1.get_stats.return_value = {'requests': 5, 'success': 4}
        provider2 = Mock(spec=BaseLLMProvider)
        provider2.get_stats.return_value = {'requests': 3, 'success': 3}
        
        manager.add_provider("provider1", provider1)
        manager.add_provider("provider2", provider2)
        
        stats = manager.get_provider_stats()
        
        assert stats["provider1"] == {'requests': 5, 'success': 4}
        assert stats["provider2"] == {'requests': 3, 'success': 3}
    
    def test_llm_manager_get_global_stats(self):
        """Test getting global manager statistics."""
        # This tests lines 587-589 (get_global_stats method)
        manager = LLMManager()
        
        # Modify some stats
        manager.global_stats['total_requests'] = 10
        manager.global_stats['cache_hits'] = 3
        
        stats = manager.get_global_stats()
        
        assert stats['total_requests'] == 10
        assert stats['cache_hits'] == 3
        # Should be a copy, not the original
        stats['total_requests'] = 999
        assert manager.global_stats['total_requests'] == 10


class TestPromptManagerAdvanced:
    """Test PromptManager advanced functionality."""
    
    def test_prompt_manager_get_template_unknown(self):
        """Test getting unknown template."""
        # This tests lines 737-741 (get_template error path)
        manager = LLMManager()
        prompt_manager = PromptManager(manager)
        
        with pytest.raises(ValueError, match="Unknown template: nonexistent"):
            prompt_manager.get_template("nonexistent")
    
    def test_prompt_manager_get_template_success(self):
        """Test successful template retrieval."""
        # This tests lines 737-741 (get_template success path)
        manager = LLMManager()
        prompt_manager = PromptManager(manager)
        
        template = prompt_manager.get_template("content_extraction")
        
        assert 'system' in template
        assert 'user' in template
        assert "extracting structured information" in template['system']
    
    @pytest.mark.asyncio
    async def test_prompt_manager_execute_template_async(self):
        """Test async template execution."""
        # This tests lines 743-764 (execute_template_async method)
        manager = LLMManager()
        manager.generate_async = AsyncMock(return_value=LLMResponse(
            content="Extracted data: {...}",
            model="gpt-3.5-turbo",
            usage={},
            metadata={},
            timestamp=datetime.now()
        ))
        
        prompt_manager = PromptManager(manager)
        
        variables = {
            'content': 'Sample article content to extract from'
        }
        
        response = await prompt_manager.execute_template_async(
            template_name="content_extraction",
            variables=variables,
            model="gpt-3.5-turbo",
            temperature=0.5
        )
        
        assert response.content == "Extracted data: {...}"
        
        # Verify the LLM manager was called with correct request
        manager.generate_async.assert_called_once()
        call_args = manager.generate_async.call_args[0][0]
        assert isinstance(call_args, LLMRequest)
        assert "Sample article content to extract from" in call_args.prompt
        assert "extracting structured information" in call_args.system_prompt
        assert call_args.temperature == 0.5
    
    def test_prompt_manager_execute_template_sync(self):
        """Test sync template execution."""
        # This tests lines 766-776 (execute_template sync method)
        manager = LLMManager()
        prompt_manager = PromptManager(manager)
        
        # Mock the async method
        mock_response = LLMResponse(
            content="Analysis result",
            model="gpt-4",
            usage={},
            metadata={},
            timestamp=datetime.now()
        )
        prompt_manager.execute_template_async = AsyncMock(return_value=mock_response)
        
        variables = {
            'content': 'Threat intelligence content',
            'url': 'https://example.com'
        }
        
        response = prompt_manager.execute_template(
            template_name="threat_analysis",
            variables=variables
        )
        
        assert response.content == "Analysis result"
        prompt_manager.execute_template_async.assert_called_once()


class TestDSPyIntegrationBasic:
    """Test DSPyIntegration basic functionality."""
    
    def test_dspy_integration_not_available(self):
        """Test DSPyIntegration when DSPy is not available."""
        # This tests lines 601-602 (ImportError path)
        with patch('common_tools.llm_tools.DSPY_AVAILABLE', False):
            manager = LLMManager()
            
            with pytest.raises(ImportError, match="dspy package is required"):
                DSPyIntegration(manager)
    
    def test_dspy_integration_initialization(self):
        """Test DSPyIntegration initialization when DSPy is available."""
        # This tests lines 600-608 (successful initialization)
        with patch('common_tools.llm_tools.DSPY_AVAILABLE', True):
            with patch('common_tools.llm_tools.dspy') as mock_dspy:
                manager = LLMManager()
                
                integration = DSPyIntegration(manager)
                
                assert integration.llm_manager == manager
                # Should have configured DSPy
                mock_dspy.configure.assert_called_once()
    
    def test_dspy_create_signature(self):
        """Test DSPy signature creation."""
        # This tests lines 631-649 (create_signature method)
        with patch('common_tools.llm_tools.DSPY_AVAILABLE', True):
            with patch('common_tools.llm_tools.dspy') as mock_dspy:
                mock_signature_class = Mock()
                mock_dspy.Signature.return_value = mock_signature_class
                
                manager = LLMManager()
                integration = DSPyIntegration(manager)
                
                input_fields = {"text": "Input text to analyze"}
                output_fields = {"sentiment": "Sentiment analysis result"}
                
                result = integration.create_signature(input_fields, output_fields)
                
                # Should have called dspy.Signature with correct string
                mock_dspy.Signature.assert_called_once()
                call_args = mock_dspy.Signature.call_args[0][0]
                assert "text: Input text to analyze" in call_args
                assert "sentiment: Sentiment analysis result" in call_args
                assert " -> " in call_args
    
    def test_dspy_create_chain_of_thought(self):
        """Test DSPy Chain of Thought creation."""
        # This tests lines 651-653 (create_chain_of_thought method)
        with patch('common_tools.llm_tools.DSPY_AVAILABLE', True):
            with patch('common_tools.llm_tools.dspy') as mock_dspy:
                mock_cot = Mock()
                mock_dspy.ChainOfThought.return_value = mock_cot
                
                manager = LLMManager()
                integration = DSPyIntegration(manager)
                
                mock_signature = Mock()
                result = integration.create_chain_of_thought(mock_signature)
                
                mock_dspy.ChainOfThought.assert_called_once_with(mock_signature)
                assert result == mock_cot
    
    def test_dspy_create_program_of_thought(self):
        """Test DSPy Program of Thought creation."""
        # This tests lines 655-657 (create_program_of_thought method)
        with patch('common_tools.llm_tools.DSPY_AVAILABLE', True):
            with patch('common_tools.llm_tools.dspy') as mock_dspy:
                mock_pot = Mock()
                mock_dspy.ProgramOfThought.return_value = mock_pot
                
                manager = LLMManager()
                integration = DSPyIntegration(manager)
                
                mock_signature = Mock()
                result = integration.create_program_of_thought(mock_signature)
                
                mock_dspy.ProgramOfThought.assert_called_once_with(mock_signature)
                assert result == mock_pot


class TestAvailabilityFlags:
    """Test the availability flag logic."""
    
    def test_availability_flags_exist(self):
        """Test that availability flags are defined."""
        # The module should have these flags defined
        from common_tools.llm_tools import DSPY_AVAILABLE, GEMINI_AVAILABLE, OPENAI_AVAILABLE
        
        # Flags should be boolean values
        assert isinstance(DSPY_AVAILABLE, bool)
        assert isinstance(GEMINI_AVAILABLE, bool)
        assert isinstance(OPENAI_AVAILABLE, bool)


class TestPromptTemplates:
    """Test prompt template functionality."""
    
    def test_prompt_templates_exist(self):
        """Test that prompt templates are properly defined."""
        # This ensures coverage of the PROMPT_TEMPLATES constant definition
        assert 'content_extraction' in PROMPT_TEMPLATES
        assert 'threat_analysis' in PROMPT_TEMPLATES
        assert 'source_classification' in PROMPT_TEMPLATES
        assert 'search_query_generation' in PROMPT_TEMPLATES
        
        # Check template structure
        for template_name, template in PROMPT_TEMPLATES.items():
            assert 'system' in template
            assert 'user' in template
            assert isinstance(template['system'], str)
            assert isinstance(template['user'], str)