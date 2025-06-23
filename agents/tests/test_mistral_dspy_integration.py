"""
Test suite for Mistral DSPy integration.

Tests the Mistral provider integration with DSPy framework.
"""

import os
import pytest
import logging
from unittest.mock import Mock, patch, MagicMock

# Setup test environment
os.environ['TESTING'] = 'true'

try:
    from common_tools.mistral_dspy_provider import MistralDSPyProvider, create_mistral_dspy_provider
    from common_tools.dspy_config_manager import DSPyConfigManager
    MISTRAL_AVAILABLE = True
except ImportError:
    MISTRAL_AVAILABLE = False

# Skip all tests if Mistral not available
pytestmark = pytest.mark.skipif(not MISTRAL_AVAILABLE, reason="Mistral integration not available")


@patch('common_tools.mistral_dspy_provider.MISTRAL_AVAILABLE', True)
@patch('common_tools.mistral_dspy_provider.DSPY_AVAILABLE', True)
class TestMistralDSPyProvider:
    """Test the Mistral DSPy provider implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.api_key = "test_api_key_12345"
        self.model = "mistral-large-latest"
        
    def test_provider_initialization(self):
        """Test basic provider initialization."""
        with patch('common_tools.mistral_dspy_provider.MistralClient'):
            provider = MistralDSPyProvider(
                model=self.model,
                api_key=self.api_key,
                temperature=0.2,
                max_tokens=1024
            )
            
            assert provider.model == self.model
            assert provider.api_key == self.api_key
            assert provider.temperature == 0.2
            assert provider.max_tokens == 1024
    
    def test_provider_initialization_no_api_key(self):
        """Test provider initialization fails without API key."""
        with pytest.raises(ValueError, match="Mistral API key is required"):
            MistralDSPyProvider(model=self.model, api_key=None)
    
    @patch('common_tools.mistral_dspy_provider.MistralClient')
    @patch('common_tools.mistral_dspy_provider.ChatMessage')
    def test_generate_method(self, mock_chat_message, mock_client_class):
        """Test the generate method with mocked API response."""
        # Setup mock response
        mock_client = Mock()
        mock_response = Mock()
        mock_choice = Mock()
        mock_choice.message.content = "This is a test response from Mistral."
        mock_response.choices = [mock_choice]
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        # Mock ChatMessage
        mock_chat_message.return_value = Mock()
        
        # Create provider and test
        provider = MistralDSPyProvider(model=self.model, api_key=self.api_key)
        result = provider.generate("Test prompt")
        
        assert len(result) == 1
        assert result[0] == "This is a test response from Mistral."
        assert len(provider.history) == 1
        assert provider.history[0]['prompt'] == "Test prompt"
    
    @patch('common_tools.mistral_dspy_provider.MistralClient')
    def test_generate_method_empty_response(self, mock_client_class):
        """Test generate method with empty API response."""
        # Setup mock with empty response
        mock_client = Mock()
        mock_response = Mock()
        mock_response.choices = []
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        provider = MistralDSPyProvider(model=self.model, api_key=self.api_key)
        result = provider.generate("Test prompt")
        
        assert result == [""]
    
    @patch('common_tools.mistral_dspy_provider.MistralClient')
    def test_generate_method_api_error(self, mock_client_class):
        """Test generate method with API error."""
        # Setup mock to raise exception
        mock_client = Mock()
        mock_client.chat.side_effect = Exception("API Error")
        mock_client_class.return_value = mock_client
        
        provider = MistralDSPyProvider(model=self.model, api_key=self.api_key)
        result = provider.generate("Test prompt")
        
        assert result == [""]
    
    @patch('common_tools.mistral_dspy_provider.MistralClient')
    @patch('common_tools.mistral_dspy_provider.ChatMessage')
    def test_chat_method(self, mock_chat_message, mock_client_class):
        """Test the chat method for multi-turn conversations."""
        # Setup mock response
        mock_client = Mock()
        mock_response = Mock()
        mock_choice = Mock()
        mock_choice.message.content = "Chat response from Mistral."
        mock_response.choices = [mock_choice]
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        # Mock ChatMessage
        mock_chat_message.return_value = Mock()
        
        provider = MistralDSPyProvider(model=self.model, api_key=self.api_key)
        
        messages = [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
            {"role": "user", "content": "How are you?"}
        ]
        
        result = provider.chat(messages)
        assert result == "Chat response from Mistral."
    
    @patch('common_tools.mistral_dspy_provider.MistralClient')
    @patch('common_tools.mistral_dspy_provider.ChatMessage')
    def test_test_connection_success(self, mock_chat_message, mock_client_class):
        """Test successful connection test."""
        # Setup mock for successful connection
        mock_client = Mock()
        mock_response = Mock()
        mock_choice = Mock()
        mock_choice.message.content = "Test successful"
        mock_response.choices = [mock_choice]
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        # Mock ChatMessage
        mock_chat_message.return_value = Mock()
        
        provider = MistralDSPyProvider(model=self.model, api_key=self.api_key)
        assert provider.test_connection() == True
    
    @patch('common_tools.mistral_dspy_provider.MistralClient')
    def test_test_connection_failure(self, mock_client_class):
        """Test failed connection test."""
        # Setup mock to raise exception
        mock_client = Mock()
        mock_client.chat.side_effect = Exception("Connection failed")
        mock_client_class.return_value = mock_client
        
        provider = MistralDSPyProvider(model=self.model, api_key=self.api_key)
        assert provider.test_connection() == False
    
    def test_get_model_info(self):
        """Test model information retrieval."""
        with patch('common_tools.mistral_dspy_provider.MistralClient'):
            provider = MistralDSPyProvider(
                model=self.model,
                api_key=self.api_key,
                temperature=0.3,
                max_tokens=2048
            )
            
            info = provider.get_model_info()
            
            assert info['provider'] == 'mistral'
            assert info['model'] == self.model
            assert info['temperature'] == 0.3
            assert info['max_tokens'] == 2048
            assert info['api_key_set'] == True
    
    def test_factory_function(self):
        """Test the factory function for creating providers."""
        with patch('common_tools.mistral_dspy_provider.MistralClient'):
            provider = create_mistral_dspy_provider(
                model=self.model,
                api_key=self.api_key
            )
            
            assert isinstance(provider, MistralDSPyProvider)
            assert provider.model == self.model
            assert provider.api_key == self.api_key


class TestDSPyConfigManagerMistralIntegration:
    """Test Mistral integration with DSPy configuration manager."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.test_config = {
            'dspy_settings': {
                'llm_provider': 'mistral',
                'mistral_model': 'mistral-large-latest',
                'temperature': 0.2,
                'max_output_tokens': 2048
            }
        }
    
    def test_mistral_in_provider_preferences(self):
        """Test that Mistral is included in provider preferences."""
        with patch.dict(os.environ, {'DSPY_PROVIDER': 'mistral'}):
            manager = DSPyConfigManager()
            manager.config = self.test_config
            
            preferences = manager.get_provider_preference()
            assert 'mistral' in preferences
            assert preferences[0] == 'mistral'  # Should be first when explicitly set
    
    def test_mistral_api_key_detection(self):
        """Test API key detection for Mistral."""
        with patch.dict(os.environ, {'MISTRAL_API_KEY': 'test_key_123'}):
            manager = DSPyConfigManager()
            api_keys = manager.get_api_keys()
            
            assert 'mistral' in api_keys
            assert api_keys['mistral'] == 'test_key_123'
    
    def test_mistral_model_config(self):
        """Test Mistral model configuration."""
        with patch.dict(os.environ, {'MISTRAL_MODEL': 'mistral-medium-latest'}):
            manager = DSPyConfigManager()
            manager.config = self.test_config
            
            config = manager.get_model_config('mistral', 'primary')
            
            assert config['model'] == 'mistral-medium-latest'
            assert config['temperature'] == 0.2
            assert config['max_tokens'] == 2048
    
    @patch('common_tools.dspy_config_manager.MistralDSPyProvider')
    def test_create_mistral_provider_success(self, mock_provider_class):
        """Test successful Mistral provider creation."""
        # Setup mock
        mock_provider = Mock()
        mock_provider_class.return_value = mock_provider
        
        with patch.dict(os.environ, {'MISTRAL_API_KEY': 'test_key'}):
            manager = DSPyConfigManager()
            manager.config = self.test_config
            
            provider = manager.create_mistral_provider()
            
            assert provider is not None
            mock_provider_class.assert_called_once()
    
    def test_create_mistral_provider_no_api_key(self):
        """Test Mistral provider creation without API key."""
        with patch.dict(os.environ, {}, clear=True):
            manager = DSPyConfigManager()
            manager.config = self.test_config
            
            provider = manager.create_mistral_provider()
            assert provider is None
    
    def test_get_current_config_includes_mistral(self):
        """Test that current config includes Mistral status."""
        manager = DSPyConfigManager()
        config = manager.get_current_config()
        
        assert 'mistral_available' in config
        assert 'mistral' in config['api_keys_present']
    
    def test_fallback_includes_mistral(self):
        """Test that fallback system includes Mistral."""
        manager = DSPyConfigManager()
        manager.current_provider = 'openai'
        
        # Mock the configure_dspy method to simulate successful fallback
        with patch.object(manager, 'configure_dspy', return_value=True):
            success = manager.reconfigure_with_fallback()
            assert success == True


class TestMistralDSPyIntegration:
    """Integration tests for Mistral with DSPy framework."""
    
    @patch('common_tools.mistral_dspy_provider.MistralClient')
    @patch('common_tools.dspy_config_manager.dspy')
    def test_dspy_configuration_with_mistral(self, mock_dspy, mock_client_class):
        """Test configuring DSPy to use Mistral provider."""
        # Setup mocks
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_dspy.settings = Mock()
        
        # Mock successful provider creation and testing
        with patch('common_tools.dspy_config_manager.MistralDSPyProvider') as mock_provider_class:
            mock_provider = Mock()
            mock_provider.model = 'mistral-large-latest'
            mock_provider_class.return_value = mock_provider
            
            # Mock successful test
            with patch.object(DSPyConfigManager, '_test_provider', return_value=True):
                with patch.dict(os.environ, {'MISTRAL_API_KEY': 'test_key'}):
                    manager = DSPyConfigManager()
                    manager.config = {
                        'dspy_settings': {
                            'llm_provider': 'mistral',
                            'mistral_model': 'mistral-large-latest'
                        }
                    }
                    
                    success = manager.configure_dspy(force_provider='mistral')
                    
                    assert success == True
                    assert manager.current_provider == 'mistral'
                    mock_dspy.settings.configure.assert_called_once_with(lm=mock_provider)


class TestMistralEnvironmentVariables:
    """Test environment variable handling for Mistral."""
    
    def test_mistral_model_environment_override(self):
        """Test that MISTRAL_MODEL environment variable overrides config."""
        with patch.dict(os.environ, {'MISTRAL_MODEL': 'mistral-small-latest'}):
            manager = DSPyConfigManager()
            config = manager.get_model_config('mistral', 'primary')
            
            assert config['model'] == 'mistral-small-latest'
    
    def test_dspy_provider_environment_variable(self):
        """Test DSPY_PROVIDER environment variable for Mistral."""
        with patch.dict(os.environ, {'DSPY_PROVIDER': 'mistral'}):
            manager = DSPyConfigManager()
            preferences = manager.get_provider_preference()
            
            assert preferences[0] == 'mistral'


# Utility functions for testing
def test_mistral_provider_without_dspy():
    """Test Mistral provider can work independently of DSPy."""
    with patch('common_tools.mistral_dspy_provider.MistralClient'):
        provider = MistralDSPyProvider(
            model="mistral-large-latest",
            api_key="test_key",
            temperature=0.2
        )
        
        info = provider.get_model_info()
        assert info['provider'] == 'mistral'


if __name__ == "__main__":
    # Run basic tests when executed directly
    import sys
    
    if not MISTRAL_AVAILABLE:
        print("❌ Mistral integration not available - skipping tests")
        sys.exit(0)
    
    print("🔧 Running Mistral DSPy integration tests...")
    
    # Run pytest
    exit_code = pytest.main([__file__, "-v"])
    sys.exit(exit_code)