"""
DSPy Configuration Manager

This module provides centralized DSPy configuration management with:
1. OpenAI primary provider with Gemini fallback
2. Proper model selection and environment variable handling
3. Configuration validation and error handling
4. Integration with the existing config.yaml system
"""

import os
import logging
import yaml
from typing import Optional, Dict, Any, List
from pathlib import Path

try:
    import dspy
    DSPY_AVAILABLE = True
except ImportError:
    DSPY_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import google.generativeai as genai
    from dspy import GoogleGenerativeAI
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

try:
    from mistralai.client import MistralClient
    from .mistral_dspy_provider import MistralDSPyProvider
    MISTRAL_AVAILABLE = True
except ImportError:
    MISTRAL_AVAILABLE = False
    MistralDSPyProvider = None


logger = logging.getLogger(__name__)


class DSPyConfigurationError(Exception):
    """Exception raised for DSPy configuration errors."""
    pass


class DSPyConfigManager:
    """
    Centralized DSPy configuration manager.
    
    Handles:
    - Primary provider selection (OpenAI preferred, Gemini fallback)
    - Model configuration based on environment variables
    - Configuration validation
    - Integration with config.yaml
    """
    
    DEFAULT_MODELS = {
        'openai': {
            'primary': 'gpt-4-turbo-preview',
            'fallback': 'gpt-3.5-turbo',
            'fast': 'gpt-3.5-turbo'
        },
        'gemini': {
            'primary': 'gemini-1.5-pro',
            'fallback': 'gemini-1.0-pro',
            'fast': 'gemini-1.0-pro'
        },
        'mistral': {
            'primary': 'mistral-large-latest',
            'fallback': 'mistral-medium-latest',
            'fast': 'mistral-small-latest'
        }
    }
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize DSPy configuration manager.
        
        Args:
            config_path: Path to config.yaml file
        """
        self.config_path = config_path or self._find_config_file()
        self.config = self._load_config()
        self.current_provider = None
        self.current_model = None
        self.fallback_providers = []
        
    def _find_config_file(self) -> Optional[str]:
        """Find the config.yaml file."""
        # Try environment variable first
        config_path = os.getenv('AGENT_CONFIG_PATH')
        if config_path and Path(config_path).exists():
            return config_path
        
        # Try relative paths
        possible_paths = [
            'config.yaml',
            'agents/config.yaml',
            '../config.yaml'
        ]
        
        for path in possible_paths:
            if Path(path).exists():
                return str(Path(path).absolute())
        
        return None
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not self.config_path:
            logger.warning("No config file found, using defaults")
            return {}
        
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f) or {}
            logger.info(f"Loaded configuration from {self.config_path}")
            return config
        except Exception as e:
            logger.warning(f"Failed to load config from {self.config_path}: {e}")
            return {}
    
    def get_dspy_config(self) -> Dict[str, Any]:
        """Get DSPy-specific configuration."""
        return self.config.get('dspy_settings', {})
    
    def get_provider_preference(self) -> List[str]:
        """
        Get provider preference order.
        
        Returns:
            List of providers in preference order
        """
        # Check environment variable for explicit preference
        provider_env = os.getenv('DSPY_PROVIDER', '').lower()
        if provider_env in ['openai', 'gemini', 'mistral']:
            if provider_env == 'openai':
                return ['openai', 'mistral', 'gemini']
            elif provider_env == 'mistral':
                return ['mistral', 'openai', 'gemini']
            else:
                return ['gemini', 'mistral', 'openai']
        
        # Check config file
        dspy_config = self.get_dspy_config()
        configured_provider = dspy_config.get('llm_provider', '').lower()
        
        if 'openai' in configured_provider:
            return ['openai', 'mistral', 'gemini']
        elif 'mistral' in configured_provider:
            return ['mistral', 'openai', 'gemini']
        elif 'gemini' in configured_provider or 'google' in configured_provider:
            return ['gemini', 'mistral', 'openai']
        
        # Default: OpenAI first, Mistral second, Gemini fallback
        return ['openai', 'mistral', 'gemini']
    
    def get_api_keys(self) -> Dict[str, Optional[str]]:
        """Get API keys for all providers."""
        return {
            'openai': os.getenv('OPENAI_API_KEY'),
            'gemini': (os.getenv('GEMINI_API_KEY') or 
                      os.getenv('GOOGLE_API_KEY') or
                      os.getenv('GOOGLE_GENERATIVE_AI_API_KEY')),
            'mistral': os.getenv('MISTRAL_API_KEY')
        }
    
    def get_model_config(self, provider: str, model_type: str = 'primary') -> Dict[str, Any]:
        """
        Get model configuration for a provider.
        
        Args:
            provider: Provider name ('openai' or 'gemini')
            model_type: Model type ('primary', 'fallback', 'fast')
            
        Returns:
            Model configuration dict
        """
        dspy_config = self.get_dspy_config()
        
        # Get model name from environment or config
        if provider == 'openai':
            model_name = (
                os.getenv('OPENAI_MODEL') or
                dspy_config.get('openai_model') or
                self.DEFAULT_MODELS['openai'][model_type]
            )
            return {
                'model': model_name,
                'temperature': dspy_config.get('temperature', 0.2),
                'max_tokens': dspy_config.get('max_output_tokens', 2048)
            }
        
        elif provider == 'gemini':
            model_name = (
                os.getenv('GEMINI_MODEL_NAME') or
                os.getenv('GEMINI_MODEL') or
                dspy_config.get('model_name') or
                self.DEFAULT_MODELS['gemini'][model_type]
            )
            return {
                'model': model_name,
                'temperature': dspy_config.get('temperature', 0.2),
                'max_output_tokens': dspy_config.get('max_output_tokens', 2048)
            }
        
        elif provider == 'mistral':
            model_name = (
                os.getenv('MISTRAL_MODEL') or
                dspy_config.get('mistral_model') or
                self.DEFAULT_MODELS['mistral'][model_type]
            )
            return {
                'model': model_name,
                'temperature': dspy_config.get('temperature', 0.2),
                'max_tokens': dspy_config.get('max_output_tokens', 2048)
            }
        
        else:
            raise DSPyConfigurationError(f"Unknown provider: {provider}")
    
    def create_openai_provider(self, model_type: str = 'primary') -> Optional[object]:
        """
        Create OpenAI DSPy provider.
        
        Args:
            model_type: Model type to use
            
        Returns:
            Configured OpenAI provider or None if not available
        """
        if not OPENAI_AVAILABLE:
            logger.warning("OpenAI not available - package not installed")
            return None
        
        api_keys = self.get_api_keys()
        if not api_keys['openai']:
            logger.warning("OpenAI API key not found in environment")
            return None
        
        try:
            model_config = self.get_model_config('openai', model_type)
            
            # Import DSPy OpenAI provider
            from dspy import OpenAI
            
            provider = OpenAI(
                model=model_config['model'],
                api_key=api_keys['openai'],
                temperature=model_config['temperature'],
                max_tokens=model_config['max_tokens']
            )
            
            logger.info(f"Created OpenAI provider with model {model_config['model']}")
            return provider
            
        except Exception as e:
            logger.error(f"Failed to create OpenAI provider: {e}")
            return None
    
    def create_gemini_provider(self, model_type: str = 'primary') -> Optional[object]:
        """
        Create Gemini DSPy provider.
        
        Args:
            model_type: Model type to use
            
        Returns:
            Configured Gemini provider or None if not available
        """
        if not GEMINI_AVAILABLE:
            logger.warning("Gemini not available - package not installed")
            return None
        
        api_keys = self.get_api_keys()
        if not api_keys['gemini']:
            logger.warning("Gemini API key not found in environment")
            return None
        
        try:
            model_config = self.get_model_config('gemini', model_type)
            
            # Configure Gemini
            genai.configure(api_key=api_keys['gemini'])
            
            # Create DSPy Gemini provider
            provider = GoogleGenerativeAI(
                model=model_config['model'],
                temperature=model_config['temperature'],
                max_output_tokens=model_config['max_output_tokens']
            )
            
            logger.info(f"Created Gemini provider with model {model_config['model']}")
            return provider
            
        except Exception as e:
            logger.error(f"Failed to create Gemini provider: {e}")
            return None
    
    def create_mistral_provider(self, model_type: str = 'primary') -> Optional[object]:
        """
        Create Mistral DSPy provider.
        
        Args:
            model_type: Model type to use
            
        Returns:
            Configured Mistral provider or None if not available
        """
        if not MISTRAL_AVAILABLE or MistralDSPyProvider is None:
            logger.warning("Mistral not available - package not installed")
            return None
        
        api_keys = self.get_api_keys()
        if not api_keys['mistral']:
            logger.warning("Mistral API key not found in environment")
            return None
        
        try:
            model_config = self.get_model_config('mistral', model_type)
            
            # Create custom Mistral DSPy provider
            provider = MistralDSPyProvider(
                model=model_config['model'],
                api_key=api_keys['mistral'],
                temperature=model_config['temperature'],
                max_tokens=model_config['max_tokens']
            )
            
            logger.info(f"Created Mistral provider with model {model_config['model']}")
            return provider
            
        except Exception as e:
            logger.error(f"Failed to create Mistral provider: {e}")
            return None
    
    def configure_dspy(self, force_provider: Optional[str] = None) -> bool:
        """
        Configure DSPy with the best available provider.
        
        Args:
            force_provider: Force a specific provider ('openai' or 'gemini')
            
        Returns:
            True if configuration successful, False otherwise
        """
        if not DSPY_AVAILABLE:
            raise DSPyConfigurationError("DSPy not available - package not installed")
        
        # Determine provider order
        if force_provider:
            provider_order = [force_provider]
        else:
            provider_order = self.get_provider_preference()
        
        # Try each provider in order
        for provider in provider_order:
            logger.info(f"Attempting to configure DSPy with {provider}")
            
            if provider == 'openai':
                lm = self.create_openai_provider()
            elif provider == 'gemini':
                lm = self.create_gemini_provider()
            elif provider == 'mistral':
                lm = self.create_mistral_provider()
            else:
                logger.warning(f"Unknown provider: {provider}")
                continue
            
            if lm:
                try:
                    # Test the provider with a simple call
                    test_result = self._test_provider(lm)
                    if test_result:
                        dspy.settings.configure(lm=lm)
                        self.current_provider = provider
                        self.current_model = lm.model if hasattr(lm, 'model') else 'unknown'
                        logger.info(f"Successfully configured DSPy with {provider}")
                        return True
                    else:
                        logger.warning(f"Provider {provider} failed test")
                except Exception as e:
                    logger.warning(f"Failed to configure {provider}: {e}")
                    continue
        
        logger.error("Failed to configure DSPy with any provider")
        return False
    
    def _test_provider(self, provider) -> bool:
        """
        Test a provider with a simple call.
        
        Args:
            provider: DSPy provider to test
            
        Returns:
            True if test successful, False otherwise
        """
        try:
            # Create a simple signature for testing
            class TestSignature(dspy.Signature):
                input_text: str = dspy.InputField()
                output: str = dspy.OutputField()
            
            # Create a simple module
            test_module = dspy.ChainOfThought(TestSignature)
            
            # Configure temporarily for test
            old_lm = getattr(dspy.settings, 'lm', None)
            dspy.settings.configure(lm=provider)
            
            # Run a simple test
            result = test_module(input_text="Hello")
            
            # Restore old configuration
            if old_lm:
                dspy.settings.configure(lm=old_lm)
            
            # Check if we got a reasonable response
            return bool(result and hasattr(result, 'output') and result.output)
            
        except Exception as e:
            logger.debug(f"Provider test failed: {e}")
            return False
    
    def get_current_config(self) -> Dict[str, Any]:
        """Get current DSPy configuration status."""
        return {
            'provider': self.current_provider,
            'model': self.current_model,
            'dspy_available': DSPY_AVAILABLE,
            'openai_available': OPENAI_AVAILABLE,
            'gemini_available': GEMINI_AVAILABLE,
            'mistral_available': MISTRAL_AVAILABLE,
            'api_keys_present': {
                'openai': bool(self.get_api_keys()['openai']),
                'gemini': bool(self.get_api_keys()['gemini']),
                'mistral': bool(self.get_api_keys()['mistral'])
            },
            'configured': hasattr(dspy.settings, 'lm') and dspy.settings.lm is not None
        }
    
    def reconfigure_with_fallback(self) -> bool:
        """
        Reconfigure DSPy with fallback provider if current fails.
        
        Returns:
            True if reconfiguration successful, False otherwise
        """
        if not self.current_provider:
            return self.configure_dspy()
        
        # Try the other providers in order
        all_providers = ['openai', 'mistral', 'gemini']
        fallback_providers = [p for p in all_providers if p != self.current_provider]
        
        for provider in fallback_providers:
            if self.configure_dspy(force_provider=provider):
                logger.info(f"Successfully failed over to {provider}")
                return True
        
        logger.error("All fallback providers failed")
        return False


# Global configuration manager instance
_config_manager: Optional[DSPyConfigManager] = None


def get_dspy_config_manager() -> DSPyConfigManager:
    """Get or create global DSPy configuration manager."""
    global _config_manager
    if _config_manager is None:
        _config_manager = DSPyConfigManager()
    return _config_manager


def configure_dspy_from_config(force_provider: Optional[str] = None) -> bool:
    """
    Configure DSPy using the configuration manager.
    
    Args:
        force_provider: Force a specific provider
        
    Returns:
        True if configuration successful, False otherwise
    """
    manager = get_dspy_config_manager()
    return manager.configure_dspy(force_provider=force_provider)


def get_dspy_status() -> Dict[str, Any]:
    """Get current DSPy configuration status."""
    manager = get_dspy_config_manager()
    return manager.get_current_config()


def reconfigure_dspy_with_fallback() -> bool:
    """Reconfigure DSPy with fallback provider."""
    manager = get_dspy_config_manager()
    return manager.reconfigure_with_fallback()


# Usage example and testing
if __name__ == "__main__":
    import sys
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("DSPy Configuration Manager Test")
    print("=" * 50)
    
    # Create configuration manager
    manager = DSPyConfigManager()
    
    # Show available providers
    api_keys = manager.get_api_keys()
    print(f"Available API keys:")
    print(f"  OpenAI: {'✓' if api_keys['openai'] else '✗'}")
    print(f"  Gemini: {'✓' if api_keys['gemini'] else '✗'}")
    print(f"  Mistral: {'✓' if api_keys['mistral'] else '✗'}")
    
    # Show provider preference
    preference = manager.get_provider_preference()
    print(f"Provider preference: {' → '.join(preference)}")
    
    # Try to configure DSPy
    print(f"\nAttempting DSPy configuration...")
    success = manager.configure_dspy()
    
    if success:
        status = manager.get_current_config()
        print(f"✓ DSPy configured successfully!")
        print(f"  Provider: {status['provider']}")
        print(f"  Model: {status['model']}")
    else:
        print("✗ DSPy configuration failed")
        sys.exit(1)
    
    # Show final status
    print(f"\nFinal configuration status:")
    status = get_dspy_status()
    for key, value in status.items():
        print(f"  {key}: {value}")