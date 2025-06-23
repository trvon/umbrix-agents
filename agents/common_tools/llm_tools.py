"""
LLM integration tools for Umbrix agents.

This module provides standardized interfaces for integrating with
Large Language Models including Gemini, OpenAI, and local models.
Includes DSPy integration patterns and prompt management.
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional, Union, Callable, Generator
from dataclasses import dataclass
from datetime import datetime
from abc import ABC, abstractmethod
import hashlib

try:
    import dspy
    DSPY_AVAILABLE = True
except ImportError:
    DSPY_AVAILABLE = False

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

from .logging import setup_logging
from .redis_tools import EnhancedRedisClient


@dataclass
class LLMRequest:
    """Represents a request to an LLM."""
    prompt: str
    model: str
    temperature: float = 0.7
    max_tokens: Optional[int] = None
    system_prompt: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class LLMResponse:
    """Represents a response from an LLM."""
    content: str
    model: str
    usage: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: datetime
    request_id: Optional[str] = None
    from_cache: bool = False


class LLMError(Exception):
    """Custom exception for LLM-related errors."""
    
    def __init__(self, message: str, model: Optional[str] = None, error_code: Optional[str] = None):
        super().__init__(message)
        self.model = model
        self.error_code = error_code


class BaseLLMProvider(ABC):
    """
    Abstract base class for LLM providers.
    
    Defines the interface that all LLM providers must implement.
    """
    
    def __init__(self, model_name: str, **kwargs):
        self.model_name = model_name
        self.logger = setup_logging(f"llm_{self.__class__.__name__.lower()}")
        
        # Common configuration
        self.default_temperature = kwargs.get('temperature', 0.7)
        self.default_max_tokens = kwargs.get('max_tokens', 1000)
        self.timeout = kwargs.get('timeout', 60)
        
        # Statistics
        self.stats = {
            'requests_made': 0,
            'requests_successful': 0,
            'requests_failed': 0,
            'total_tokens_used': 0,
            'total_cost': 0.0,
            'cache_hits': 0
        }
    
    @abstractmethod
    async def generate_async(self, request: LLMRequest) -> LLMResponse:
        """Generate response asynchronously."""
        pass
    
    @abstractmethod
    def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate response synchronously."""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if the provider is available and configured."""
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get provider statistics."""
        return self.stats.copy()


class GeminiProvider(BaseLLMProvider):
    """
    Google Gemini LLM provider.
    
    Provides integration with Google's Gemini models through the
    official Google AI SDK.
    """
    
    def __init__(self, api_key: str, model_name: str = "gemini-1.5-flash", **kwargs):
        if not GEMINI_AVAILABLE:
            raise ImportError("google-generativeai package is required for GeminiProvider")
        
        super().__init__(model_name, **kwargs)
        
        self.api_key = api_key
        genai.configure(api_key=api_key)
        
        # Initialize the model
        self.model = genai.GenerativeModel(model_name)
        
        # Gemini-specific configuration
        self.safety_settings = kwargs.get('safety_settings', [])
        
        # Token pricing (approximate, should be updated based on current pricing)
        self.pricing = {
            'input_tokens': 0.00001,  # $0.01 per 1K tokens
            'output_tokens': 0.00003  # $0.03 per 1K tokens
        }
    
    def is_available(self) -> bool:
        """Check if Gemini is available."""
        return GEMINI_AVAILABLE and self.api_key is not None
    
    async def generate_async(self, request: LLMRequest) -> LLMResponse:
        """Generate response using Gemini asynchronously."""
        try:
            self.stats['requests_made'] += 1
            start_time = time.time()
            
            # Prepare the prompt
            full_prompt = request.prompt
            if request.system_prompt:
                full_prompt = f"{request.system_prompt}\n\n{request.prompt}"
            
            # Configure generation parameters
            generation_config = genai.types.GenerationConfig(
                temperature=request.temperature,
                max_output_tokens=request.max_tokens,
            )
            
            # Generate content - handle async generator
            import inspect
            response_generator = self.model.generate_content_async(
                full_prompt,
                generation_config=generation_config,
                safety_settings=self.safety_settings
            )
            
            if inspect.isasyncgen(response_generator):
                # Handle async generator - collect all chunks
                full_response = ""
                async for chunk in response_generator:
                    if hasattr(chunk, 'text'):
                        full_response += chunk.text
                    elif hasattr(chunk, 'content'):
                        full_response += chunk.content
                    else:
                        full_response += str(chunk)
                content = full_response
                # For usage metadata, try to get from last chunk
                response = chunk if 'chunk' in locals() else None
            elif inspect.iscoroutine(response_generator):
                # Handle regular coroutine
                response = await response_generator
                content = response.text if response and hasattr(response, 'text') else ""
            else:
                # Synchronous response
                response = response_generator
                content = response.text if response and hasattr(response, 'text') else ""
            
            # Calculate usage and cost
            usage = {
                'input_tokens': getattr(response.usage_metadata, 'prompt_token_count', 0),
                'output_tokens': getattr(response.usage_metadata, 'candidates_token_count', 0),
                'total_tokens': getattr(response.usage_metadata, 'total_token_count', 0)
            }
            
            cost = (usage['input_tokens'] * self.pricing['input_tokens'] + 
                   usage['output_tokens'] * self.pricing['output_tokens'])
            
            # Update statistics
            self.stats['requests_successful'] += 1
            self.stats['total_tokens_used'] += usage['total_tokens']
            self.stats['total_cost'] += cost
            
            llm_response = LLMResponse(
                content=content,
                model=self.model_name,
                usage=usage,
                metadata={
                    'provider': 'gemini',
                    'response_time': time.time() - start_time,
                    'cost': cost,
                    'finish_reason': getattr(response.candidates[0] if response.candidates else None, 'finish_reason', None)
                },
                timestamp=datetime.now(),
                request_id=request.metadata.get('request_id') if request.metadata else None
            )
            
            return llm_response
            
        except Exception as e:
            self.stats['requests_failed'] += 1
            self.logger.error(f"Gemini generation failed: {e}")
            raise LLMError(f"Gemini generation failed: {e}", model=self.model_name) from e
    
    def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate response using Gemini synchronously."""
        # For simplicity, we'll use the async method with asyncio.run
        # In production, you might want a proper sync implementation
        try:
            return asyncio.run(self.generate_async(request))
        except Exception as e:
            raise LLMError(f"Gemini sync generation failed: {e}", model=self.model_name) from e


class OpenAIProvider(BaseLLMProvider):
    """
    OpenAI LLM provider.
    
    Provides integration with OpenAI's models including GPT-3.5 and GPT-4.
    """
    
    def __init__(self, api_key: str, model_name: str = "gpt-3.5-turbo", **kwargs):
        if not OPENAI_AVAILABLE:
            raise ImportError("openai package is required for OpenAIProvider")
        
        super().__init__(model_name, **kwargs)
        
        self.client = openai.AsyncOpenAI(api_key=api_key)
        self.sync_client = openai.OpenAI(api_key=api_key)
        
        # OpenAI-specific configuration
        self.organization = kwargs.get('organization')
        
        # Token pricing (approximate, should be updated based on current pricing)
        self.pricing = {
            'gpt-3.5-turbo': {'input': 0.0015, 'output': 0.002},
            'gpt-4': {'input': 0.03, 'output': 0.06},
            'gpt-4-turbo': {'input': 0.01, 'output': 0.03}
        }
    
    def is_available(self) -> bool:
        """Check if OpenAI is available."""
        return OPENAI_AVAILABLE and self.client is not None
    
    async def generate_async(self, request: LLMRequest) -> LLMResponse:
        """Generate response using OpenAI asynchronously."""
        try:
            self.stats['requests_made'] += 1
            start_time = time.time()
            
            # Prepare messages
            messages = []
            if request.system_prompt:
                messages.append({"role": "system", "content": request.system_prompt})
            messages.append({"role": "user", "content": request.prompt})
            
            # Make API call
            response = await self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                temperature=request.temperature,
                max_tokens=request.max_tokens,
                timeout=self.timeout
            )
            
            # Parse response
            content = response.choices[0].message.content if response.choices else ""
            
            # Calculate usage and cost
            usage = {
                'input_tokens': response.usage.prompt_tokens,
                'output_tokens': response.usage.completion_tokens,
                'total_tokens': response.usage.total_tokens
            }
            
            model_pricing = self.pricing.get(self.model_name, {'input': 0.001, 'output': 0.002})
            cost = (usage['input_tokens'] * model_pricing['input'] / 1000 + 
                   usage['output_tokens'] * model_pricing['output'] / 1000)
            
            # Update statistics
            self.stats['requests_successful'] += 1
            self.stats['total_tokens_used'] += usage['total_tokens']
            self.stats['total_cost'] += cost
            
            llm_response = LLMResponse(
                content=content,
                model=self.model_name,
                usage=usage,
                metadata={
                    'provider': 'openai',
                    'response_time': time.time() - start_time,
                    'cost': cost,
                    'finish_reason': response.choices[0].finish_reason if response.choices else None
                },
                timestamp=datetime.now(),
                request_id=request.metadata.get('request_id') if request.metadata else None
            )
            
            return llm_response
            
        except Exception as e:
            self.stats['requests_failed'] += 1
            self.logger.error(f"OpenAI generation failed: {e}")
            raise LLMError(f"OpenAI generation failed: {e}", model=self.model_name) from e
    
    def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate response using OpenAI synchronously."""
        try:
            self.stats['requests_made'] += 1
            start_time = time.time()
            
            # Prepare messages
            messages = []
            if request.system_prompt:
                messages.append({"role": "system", "content": request.system_prompt})
            messages.append({"role": "user", "content": request.prompt})
            
            # Make API call
            response = self.sync_client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                temperature=request.temperature,
                max_tokens=request.max_tokens,
                timeout=self.timeout
            )
            
            # Parse response (similar to async version)
            content = response.choices[0].message.content if response.choices else ""
            
            usage = {
                'input_tokens': response.usage.prompt_tokens,
                'output_tokens': response.usage.completion_tokens,
                'total_tokens': response.usage.total_tokens
            }
            
            model_pricing = self.pricing.get(self.model_name, {'input': 0.001, 'output': 0.002})
            cost = (usage['input_tokens'] * model_pricing['input'] / 1000 + 
                   usage['output_tokens'] * model_pricing['output'] / 1000)
            
            self.stats['requests_successful'] += 1
            self.stats['total_tokens_used'] += usage['total_tokens']
            self.stats['total_cost'] += cost
            
            return LLMResponse(
                content=content,
                model=self.model_name,
                usage=usage,
                metadata={
                    'provider': 'openai',
                    'response_time': time.time() - start_time,
                    'cost': cost,
                    'finish_reason': response.choices[0].finish_reason if response.choices else None
                },
                timestamp=datetime.now(),
                request_id=request.metadata.get('request_id') if request.metadata else None
            )
            
        except Exception as e:
            self.stats['requests_failed'] += 1
            self.logger.error(f"OpenAI sync generation failed: {e}")
            raise LLMError(f"OpenAI sync generation failed: {e}", model=self.model_name) from e


class LLMManager:
    """
    Multi-provider LLM manager with caching, fallbacks, and load balancing.
    
    Provides a unified interface for working with multiple LLM providers
    and includes advanced features like response caching and provider fallbacks.
    """
    
    def __init__(self, redis_client: Optional[EnhancedRedisClient] = None, cache_ttl: int = 3600):
        self.providers: Dict[str, BaseLLMProvider] = {}
        self.default_provider = None
        self.fallback_order = []
        
        self.redis_client = redis_client
        self.cache_ttl = cache_ttl
        self.cache_enabled = redis_client is not None
        
        self.logger = setup_logging("llm_manager")
        
        # Global statistics
        self.global_stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'provider_failures': 0,
            'fallback_uses': 0
        }
    
    def add_provider(self, name: str, provider: BaseLLMProvider, is_default: bool = False):
        """Add an LLM provider."""
        self.providers[name] = provider
        
        if is_default or self.default_provider is None:
            self.default_provider = name
        
        self.logger.info(f"Added LLM provider: {name} (default: {is_default})")
    
    def set_fallback_order(self, provider_names: List[str]):
        """Set the order of providers to try if the primary fails."""
        # Validate that all providers exist
        for name in provider_names:
            if name not in self.providers:
                raise ValueError(f"Unknown provider: {name}")
        
        self.fallback_order = provider_names
        self.logger.info(f"Set fallback order: {provider_names}")
    
    def _get_cache_key(self, request: LLMRequest, provider_name: str) -> str:
        """Generate cache key for request."""
        key_data = {
            'provider': provider_name,
            'model': request.model,
            'prompt': request.prompt,
            'system_prompt': request.system_prompt,
            'temperature': request.temperature,
            'max_tokens': request.max_tokens
        }
        
        key_string = json.dumps(key_data, sort_keys=True)
        return f"llm_cache:{hashlib.md5(key_string.encode()).hexdigest()}"
    
    async def _get_cached_response(self, cache_key: str) -> Optional[LLMResponse]:
        """Get cached response if available."""
        if not self.cache_enabled:
            return None
        
        try:
            cached_data = await self.redis_client.aget(cache_key)
            if cached_data:
                response_dict = json.loads(cached_data) if isinstance(cached_data, str) else cached_data
                
                # Reconstruct LLMResponse object
                response = LLMResponse(
                    content=response_dict['content'],
                    model=response_dict['model'],
                    usage=response_dict['usage'],
                    metadata=response_dict['metadata'],
                    timestamp=datetime.fromisoformat(response_dict['timestamp']),
                    request_id=response_dict.get('request_id'),
                    from_cache=True
                )
                
                self.global_stats['cache_hits'] += 1
                return response
                
        except Exception as e:
            self.logger.warning(f"Error retrieving cached response: {e}")
        
        return None
    
    async def _cache_response(self, cache_key: str, response: LLMResponse):
        """Cache response for future use."""
        if not self.cache_enabled:
            return
        
        try:
            response_dict = {
                'content': response.content,
                'model': response.model,
                'usage': response.usage,
                'metadata': response.metadata,
                'timestamp': response.timestamp.isoformat(),
                'request_id': response.request_id
            }
            
            await self.redis_client.aset(
                cache_key, 
                json.dumps(response_dict), 
                ttl=self.cache_ttl
            )
            
        except Exception as e:
            self.logger.warning(f"Error caching response: {e}")
    
    async def generate_async(
        self,
        request: LLMRequest,
        provider_name: Optional[str] = None,
        use_cache: bool = True,
        use_fallbacks: bool = True
    ) -> LLMResponse:
        """Generate response with provider selection, caching, and fallbacks."""
        self.global_stats['total_requests'] += 1
        
        # Determine provider to use
        provider_name = provider_name or self.default_provider
        if not provider_name:
            raise LLMError("No provider specified and no default provider set")
        
        if provider_name not in self.providers:
            raise LLMError(f"Unknown provider: {provider_name}")
        
        # Check cache first
        if use_cache:
            cache_key = self._get_cache_key(request, provider_name)
            cached_response = await self._get_cached_response(cache_key)
            if cached_response:
                return cached_response
        
        # Try providers in order (primary + fallbacks)
        providers_to_try = [provider_name]
        if use_fallbacks:
            providers_to_try.extend([p for p in self.fallback_order if p != provider_name])
        
        last_error = None
        
        for i, current_provider in enumerate(providers_to_try):
            if current_provider not in self.providers:
                continue
            
            provider = self.providers[current_provider]
            
            if not provider.is_available():
                self.logger.warning(f"Provider {current_provider} is not available, skipping")
                continue
            
            try:
                # Update request model to match provider if needed
                if request.model != provider.model_name:
                    request = LLMRequest(
                        prompt=request.prompt,
                        model=provider.model_name,
                        temperature=request.temperature,
                        max_tokens=request.max_tokens,
                        system_prompt=request.system_prompt,
                        metadata=request.metadata
                    )
                
                response = await provider.generate_async(request)
                
                # Cache successful response
                if use_cache and i == 0:  # Only cache primary provider responses
                    await self._cache_response(cache_key, response)
                
                # Track fallback usage
                if i > 0:
                    self.global_stats['fallback_uses'] += 1
                    self.logger.info(f"Used fallback provider {current_provider} after {i} failures")
                
                return response
                
            except Exception as e:
                last_error = e
                self.global_stats['provider_failures'] += 1
                self.logger.warning(f"Provider {current_provider} failed: {e}")
                
                if i < len(providers_to_try) - 1:
                    continue
        
        # All providers failed
        raise LLMError(f"All providers failed, last error: {last_error}")
    
    def generate(
        self,
        request: LLMRequest,
        provider_name: Optional[str] = None,
        use_fallbacks: bool = True
    ) -> LLMResponse:
        """Generate response synchronously."""
        # For simplicity, we'll use the async method
        return asyncio.run(self.generate_async(
            request, 
            provider_name=provider_name, 
            use_cache=False,  # Sync version doesn't use Redis cache
            use_fallbacks=use_fallbacks
        ))
    
    def get_provider_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all providers."""
        stats = {}
        for name, provider in self.providers.items():
            stats[name] = provider.get_stats()
        return stats
    
    def get_global_stats(self) -> Dict[str, Any]:
        """Get global manager statistics."""
        return self.global_stats.copy()


class DSPyIntegration:
    """
    Integration utilities for DSPy framework.
    
    Provides helpers for working with DSPy signatures and modules
    in the context of Umbrix agents.
    """
    
    def __init__(self, llm_manager: LLMManager):
        if not DSPY_AVAILABLE:
            raise ImportError("dspy package is required for DSPyIntegration")
        
        self.llm_manager = llm_manager
        self.logger = setup_logging("dspy_integration")
        
        # Configure DSPy to use our LLM manager
        self._setup_dspy_lm()
    
    def _setup_dspy_lm(self):
        """Configure DSPy to use our LLM manager."""
        class UmbrixLM(dspy.LM):
            def __init__(self, llm_manager: LLMManager):
                self.llm_manager = llm_manager
                super().__init__("umbrix-llm")
            
            def basic_request(self, prompt, **kwargs):
                request = LLMRequest(
                    prompt=prompt,
                    model=kwargs.get('model', 'default'),
                    temperature=kwargs.get('temperature', 0.7),
                    max_tokens=kwargs.get('max_tokens', 1000)
                )
                
                response = self.llm_manager.generate(request)
                return response.content
        
        # Set as default DSPy LM
        dspy.configure(lm=UmbrixLM(self.llm_manager))
    
    def create_signature(self, input_fields: Dict[str, str], output_fields: Dict[str, str]) -> type:
        """
        Create a DSPy signature dynamically.
        
        Args:
            input_fields: Dict of field_name -> description for inputs
            output_fields: Dict of field_name -> description for outputs
            
        Returns:
            DSPy Signature class
        """
        # Build signature string
        inputs = ", ".join([f"{name}: {desc}" for name, desc in input_fields.items()])
        outputs = ", ".join([f"{name}: {desc}" for name, desc in output_fields.items()])
        
        signature_string = f"{inputs} -> {outputs}"
        
        # Create signature class
        return dspy.Signature(signature_string)
    
    def create_chain_of_thought(self, signature: type) -> dspy.ChainOfThought:
        """Create a Chain of Thought module for the given signature."""
        return dspy.ChainOfThought(signature)
    
    def create_program_of_thought(self, signature: type) -> dspy.ProgramOfThought:
        """Create a Program of Thought module for the given signature."""
        return dspy.ProgramOfThought(signature)


# Common prompt templates
PROMPT_TEMPLATES = {
    'content_extraction': {
        'system': "You are an expert at extracting structured information from text content.",
        'user': """Extract the following information from the text below:
- Title
- Summary (max 100 words)
- Key entities (people, organizations, locations)
- Topics/categories
- Sentiment (positive/negative/neutral)

Text: {content}

Return the information in JSON format."""
    },
    
    'threat_analysis': {
        'system': "You are a cybersecurity expert analyzing threat intelligence.",
        'user': """Analyze the following threat intelligence content and extract:
- Threat type and category
- Indicators of Compromise (IoCs)
- Attack vectors
- Affected systems/platforms
- Severity level (1-10)
- Mitigation recommendations

Content: {content}

Provide analysis in structured JSON format."""
    },
    
    'source_classification': {
        'system': "You are an expert at classifying information sources and content types.",
        'user': """Classify the following source:
- Source type (news, blog, research, government, etc.)
- Credibility level (1-10)
- Content relevance to cybersecurity (1-10)
- Language and region
- Update frequency

Source URL: {url}
Content sample: {content}

Return classification in JSON format."""
    },
    
    'search_query_generation': {
        'system': "You are an expert at generating effective search queries for finding relevant information.",
        'user': """Based on the following context, generate 5 diverse search queries that would help find related information:

Context: {context}
Target domain: {domain}
Search scope: {scope}

Return queries as a JSON list."""
    }
}


class PromptManager:
    """
    Manager for prompt templates and generation.
    
    Provides utilities for managing, versioning, and executing
    prompt templates across different LLM providers.
    """
    
    def __init__(self, llm_manager: LLMManager):
        self.llm_manager = llm_manager
        self.templates = PROMPT_TEMPLATES.copy()
        self.logger = setup_logging("prompt_manager")
    
    def add_template(self, name: str, template: Dict[str, str]):
        """Add a new prompt template."""
        self.templates[name] = template
        self.logger.info(f"Added prompt template: {name}")
    
    def get_template(self, name: str) -> Dict[str, str]:
        """Get a prompt template by name."""
        if name not in self.templates:
            raise ValueError(f"Unknown template: {name}")
        return self.templates[name]
    
    async def execute_template_async(
        self,
        template_name: str,
        variables: Dict[str, Any],
        model: str = "default",
        **kwargs
    ) -> LLMResponse:
        """Execute a prompt template with variable substitution."""
        template = self.get_template(template_name)
        
        # Substitute variables in template
        system_prompt = template.get('system', '').format(**variables)
        user_prompt = template.get('user', '').format(**variables)
        
        request = LLMRequest(
            prompt=user_prompt,
            system_prompt=system_prompt,
            model=model,
            **kwargs
        )
        
        return await self.llm_manager.generate_async(request)
    
    def execute_template(
        self,
        template_name: str,
        variables: Dict[str, Any],
        model: str = "default",
        **kwargs
    ) -> LLMResponse:
        """Execute a prompt template synchronously."""
        return asyncio.run(self.execute_template_async(
            template_name, variables, model, **kwargs
        ))