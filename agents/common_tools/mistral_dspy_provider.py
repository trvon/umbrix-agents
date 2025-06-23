"""
Mistral DSPy Provider

Custom DSPy language model provider for Mistral AI models.
Implements the DSPy LM interface to work with Mistral's API.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union

try:
    import dspy
    from dspy.primitives.predict import Predict
    DSPY_AVAILABLE = True
except ImportError:
    DSPY_AVAILABLE = False

try:
    from mistralai.client import MistralClient
    from mistralai.models.chat_completion import ChatMessage
    MISTRAL_AVAILABLE = True
except ImportError:
    MISTRAL_AVAILABLE = False
    # Create dummy classes for testing
    class MistralClient:
        pass
    class ChatMessage:
        def __init__(self, role=None, content=None):
            self.role = role
            self.content = content


logger = logging.getLogger(__name__)


class MistralDSPyProvider:
    """
    Mistral AI provider for DSPy.
    
    Implements the DSPy language model interface for Mistral AI models.
    """
    
    def __init__(
        self,
        model: str = "mistral-large-latest",
        api_key: str = None,
        temperature: float = 0.2,
        max_tokens: int = 2048,
        **kwargs
    ):
        """
        Initialize Mistral DSPy provider.
        
        Args:
            model: Mistral model name
            api_key: Mistral API key
            temperature: Sampling temperature (0.0 to 1.0)
            max_tokens: Maximum tokens to generate
            **kwargs: Additional parameters
        """
        if not MISTRAL_AVAILABLE:
            raise ImportError("mistralai package not available. Install with: pip install mistralai")
        
        if not DSPY_AVAILABLE:
            raise ImportError("dspy package not available. Install with: pip install dspy-ai")
        
        if not api_key:
            raise ValueError("Mistral API key is required")
        
        self.model = model
        self.api_key = api_key
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.kwargs = kwargs
        
        # Initialize Mistral client
        self.client = MistralClient(api_key=api_key)
        
        # Store generation history for DSPy compatibility
        self.history = []
        
        logger.info(f"Initialized Mistral DSPy provider with model: {model}")
    
    def generate(self, prompt: str, **kwargs) -> List[str]:
        """
        Generate text from a prompt.
        
        Args:
            prompt: Input prompt
            **kwargs: Additional generation parameters
            
        Returns:
            List of generated completions
        """
        try:
            # Merge kwargs with instance parameters
            generation_kwargs = {
                'temperature': kwargs.get('temperature', self.temperature),
                'max_tokens': kwargs.get('max_tokens', self.max_tokens),
                **{k: v for k, v in kwargs.items() if k not in ['temperature', 'max_tokens']}
            }
            
            # Prepare messages for chat completion
            messages = [ChatMessage(role="user", content=prompt)]
            
            # Call Mistral API
            response = self.client.chat(
                model=self.model,
                messages=messages,
                temperature=generation_kwargs['temperature'],
                max_tokens=generation_kwargs['max_tokens']
            )
            
            # Extract completion text
            if response.choices and len(response.choices) > 0:
                completion = response.choices[0].message.content
                
                # Store in history for DSPy compatibility
                self.history.append({
                    'prompt': prompt,
                    'response': completion,
                    'kwargs': generation_kwargs
                })
                
                return [completion]
            else:
                logger.warning("No completion received from Mistral API")
                return [""]
                
        except Exception as e:
            logger.error(f"Error calling Mistral API: {e}")
            return [""]
    
    def __call__(self, prompt: str, **kwargs) -> List[str]:
        """Make the provider callable."""
        return self.generate(prompt, **kwargs)
    
    def basic_request(self, prompt: str, **kwargs) -> str:
        """
        Basic request method for DSPy compatibility.
        
        Args:
            prompt: Input prompt
            **kwargs: Additional parameters
            
        Returns:
            Generated text
        """
        completions = self.generate(prompt, **kwargs)
        return completions[0] if completions else ""
    
    def request(self, prompt: str, **kwargs) -> str:
        """
        Request method for DSPy compatibility.
        
        Args:
            prompt: Input prompt
            **kwargs: Additional parameters
            
        Returns:
            Generated text
        """
        return self.basic_request(prompt, **kwargs)
    
    def _generate(self, prompt: str, **kwargs) -> Dict[str, Any]:
        """
        Internal generation method for DSPy compatibility.
        
        Args:
            prompt: Input prompt
            **kwargs: Additional parameters
            
        Returns:
            Generation result with metadata
        """
        completion = self.basic_request(prompt, **kwargs)
        
        return {
            'choices': [{'text': completion}],
            'prompt': prompt,
            'model': self.model
        }
    
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """
        Chat-style completion for multi-turn conversations.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            **kwargs: Additional parameters
            
        Returns:
            Generated response
        """
        try:
            # Convert to Mistral ChatMessage format
            mistral_messages = [
                ChatMessage(role=msg['role'], content=msg['content'])
                for msg in messages
            ]
            
            # Merge kwargs with instance parameters
            generation_kwargs = {
                'temperature': kwargs.get('temperature', self.temperature),
                'max_tokens': kwargs.get('max_tokens', self.max_tokens),
            }
            
            # Call Mistral chat API
            response = self.client.chat(
                model=self.model,
                messages=mistral_messages,
                temperature=generation_kwargs['temperature'],
                max_tokens=generation_kwargs['max_tokens']
            )
            
            if response.choices and len(response.choices) > 0:
                return response.choices[0].message.content
            else:
                logger.warning("No response received from Mistral chat API")
                return ""
                
        except Exception as e:
            logger.error(f"Error in Mistral chat: {e}")
            return ""
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current model."""
        return {
            'provider': 'mistral',
            'model': self.model,
            'temperature': self.temperature,
            'max_tokens': self.max_tokens,
            'api_key_set': bool(self.api_key)
        }
    
    def test_connection(self) -> bool:
        """
        Test the connection to Mistral API.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            test_response = self.generate("Hello, this is a test.", max_tokens=10)
            return bool(test_response and test_response[0])
        except Exception as e:
            logger.error(f"Mistral connection test failed: {e}")
            return False


class MistralDSPy(dspy.LM):
    """
    DSPy-compatible Mistral language model.
    
    This class provides full DSPy integration for Mistral models.
    """
    
    def __init__(
        self,
        model: str = "mistral-large-latest",
        api_key: str = None,
        temperature: float = 0.2,
        max_tokens: int = 2048,
        **kwargs
    ):
        """Initialize DSPy-compatible Mistral model."""
        self.provider = MistralDSPyProvider(
            model=model,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs
        )
        
        # Initialize parent DSPy LM class
        super().__init__()
        
        # Set required attributes for DSPy
        self.model = model
        self.kwargs = kwargs
        self.history = []
    
    def basic_request(self, prompt: str, **kwargs) -> str:
        """DSPy basic request interface."""
        return self.provider.basic_request(prompt, **kwargs)
    
    def generate(self, prompt: str, **kwargs) -> List[str]:
        """DSPy generate interface."""
        return self.provider.generate(prompt, **kwargs)
    
    def __call__(self, prompt: str, **kwargs) -> List[str]:
        """Make the model callable."""
        return self.generate(prompt, **kwargs)


# For backwards compatibility and easier imports
MistralDSPyProvider = MistralDSPyProvider


def create_mistral_dspy_provider(
    model: str = "mistral-large-latest",
    api_key: str = None,
    **kwargs
) -> MistralDSPyProvider:
    """
    Factory function to create a Mistral DSPy provider.
    
    Args:
        model: Mistral model name
        api_key: Mistral API key
        **kwargs: Additional parameters
        
    Returns:
        Configured MistralDSPyProvider instance
    """
    return MistralDSPyProvider(
        model=model,
        api_key=api_key,
        **kwargs
    )


def test_mistral_dspy_provider():
    """Test function for the Mistral DSPy provider."""
    import os
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    api_key = os.getenv('MISTRAL_API_KEY')
    if not api_key:
        print("❌ MISTRAL_API_KEY environment variable not set")
        return False
    
    try:
        # Create provider
        provider = MistralDSPyProvider(
            model="mistral-large-latest",
            api_key=api_key,
            temperature=0.2,
            max_tokens=50
        )
        
        # Test connection
        print("🔧 Testing Mistral connection...")
        if not provider.test_connection():
            print("❌ Connection test failed")
            return False
        
        print("✅ Connection test passed")
        
        # Test generation
        print("🔧 Testing text generation...")
        prompt = "Explain what cybersecurity means in one sentence."
        response = provider.generate(prompt)
        
        if response and response[0]:
            print(f"✅ Generation successful: {response[0][:100]}...")
            return True
        else:
            print("❌ Generation failed")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False


if __name__ == "__main__":
    test_mistral_dspy_provider()