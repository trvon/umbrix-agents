"""
Test coverage for discovery_agent/config.py

This module provides comprehensive test coverage for the discovery agent configuration
system, including all dataclasses, validation, environment loading, and management.
"""

import pytest
import os
import tempfile
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from typing import Dict, Any

from discovery_agent.config import (
    KafkaConfig,
    LLMConfig,
    SearchConfig,
    ContentConfig,
    RedisConfig,
    MonitoringConfig,
    AgentConfig,
    ConfigManager,
    get_config_manager,
    get_config
)


class TestKafkaConfig:
    """Test the KafkaConfig dataclass."""
    
    def test_kafka_config_defaults(self):
        """Test default Kafka configuration values."""
        config = KafkaConfig()
        
        assert config.bootstrap_servers == "localhost:9092"
        assert config.consumer_group_id == "intelligent-crawler-group"
        assert config.auto_offset_reset == "latest"
        assert config.enable_auto_commit is True
        assert config.auto_commit_interval_ms == 1000
        assert config.session_timeout_ms == 30000
        assert config.heartbeat_interval_ms == 3000
        assert config.max_poll_records == 500
        assert config.fetch_min_bytes == 1
        assert config.fetch_max_wait_ms == 500
        
        # Check default topics
        assert "normalized.intel" in config.input_topics
        assert "user.submissions" in config.input_topics
        assert "feeds.discovered" in config.input_topics
        assert "raw.intel.crawled" in config.output_topics
        assert "feeds.discovered" in config.output_topics

    def test_kafka_config_custom_values(self):
        """Test Kafka configuration with custom values."""
        config = KafkaConfig(
            bootstrap_servers="kafka1:9092,kafka2:9092",
            consumer_group_id="custom-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=100,
            input_topics=["custom.input"],
            output_topics=["custom.output"]
        )
        
        assert config.bootstrap_servers == "kafka1:9092,kafka2:9092"
        assert config.consumer_group_id == "custom-group"
        assert config.auto_offset_reset == "earliest"
        assert config.enable_auto_commit is False
        assert config.max_poll_records == 100
        assert config.input_topics == ["custom.input"]
        assert config.output_topics == ["custom.output"]

    def test_kafka_config_validation_empty_servers(self):
        """Test Kafka config validation with empty bootstrap servers."""
        with pytest.raises(ValueError, match="bootstrap_servers cannot be empty"):
            KafkaConfig(bootstrap_servers="")

    def test_kafka_config_validation_empty_group_id(self):
        """Test Kafka config validation with empty consumer group ID."""
        with pytest.raises(ValueError, match="consumer_group_id cannot be empty"):
            KafkaConfig(consumer_group_id="")

    def test_kafka_config_post_init_valid(self):
        """Test that valid Kafka config passes post_init validation."""
        # Should not raise any exceptions
        config = KafkaConfig(
            bootstrap_servers="valid:9092",
            consumer_group_id="valid-group"
        )
        assert config.bootstrap_servers == "valid:9092"
        assert config.consumer_group_id == "valid-group"


class TestLLMConfig:
    """Test the LLMConfig dataclass."""
    
    def test_llm_config_defaults(self):
        """Test default LLM configuration values."""
        with patch('logging.warning') as mock_warning:
            config = LLMConfig()
            
            assert config.gemini_api_key == ""
            assert config.google_api_key == ""
            assert config.model_name == "gemini-1.5-flash-latest"
            assert config.max_tokens == 1000
            assert config.temperature == 0.7
            assert config.timeout_seconds == 30
            assert config.max_retries == 3
            
            # Should log warning about missing API keys
            mock_warning.assert_called_once_with(
                "No LLM API keys provided - LLM functionality will be limited"
            )

    def test_llm_config_with_api_keys(self):
        """Test LLM configuration with API keys."""
        with patch('logging.warning') as mock_warning:
            config = LLMConfig(
                gemini_api_key="test_gemini_key",
                model_name="gemini-1.5-pro",
                max_tokens=2000,
                temperature=0.5
            )
            
            assert config.gemini_api_key == "test_gemini_key"
            assert config.model_name == "gemini-1.5-pro"
            assert config.max_tokens == 2000
            assert config.temperature == 0.5
            
            # Should not log warning when API key is provided
            mock_warning.assert_not_called()

    def test_llm_config_temperature_validation(self):
        """Test LLM config temperature validation."""
        # Test temperature too low
        with pytest.raises(ValueError, match="temperature must be between 0.0 and 2.0"):
            LLMConfig(temperature=-0.1)
        
        # Test temperature too high
        with pytest.raises(ValueError, match="temperature must be between 0.0 and 2.0"):
            LLMConfig(temperature=2.1)
        
        # Test valid temperatures
        LLMConfig(temperature=0.0)  # Should not raise
        LLMConfig(temperature=2.0)  # Should not raise
        LLMConfig(temperature=1.0)  # Should not raise

    def test_llm_config_max_tokens_validation(self):
        """Test LLM config max_tokens validation."""
        with pytest.raises(ValueError, match="max_tokens must be positive"):
            LLMConfig(max_tokens=0)
        
        with pytest.raises(ValueError, match="max_tokens must be positive"):
            LLMConfig(max_tokens=-1)
        
        # Valid max_tokens
        config = LLMConfig(max_tokens=1)
        assert config.max_tokens == 1


class TestSearchConfig:
    """Test the SearchConfig dataclass."""
    
    def test_search_config_defaults(self):
        """Test default search configuration values."""
        config = SearchConfig()
        
        assert config.brave_api_key == ""
        assert config.google_cse_api_key == ""
        assert config.google_cse_id == ""
        assert config.max_results_per_query == 20
        assert config.max_concurrent_searches == 5
        assert config.search_timeout_seconds == 30
        assert config.brave_rate_limit == 100
        assert config.google_rate_limit == 100
        assert config.brave_enabled is True
        assert config.google_enabled is True

    def test_search_config_custom_values(self):
        """Test search configuration with custom values."""
        config = SearchConfig(
            brave_api_key="brave_key_123",
            google_cse_api_key="google_key_456",
            google_cse_id="cse_id_789",
            max_results_per_query=50,
            max_concurrent_searches=10,
            brave_enabled=False,
            google_enabled=True
        )
        
        assert config.brave_api_key == "brave_key_123"
        assert config.google_cse_api_key == "google_key_456"
        assert config.google_cse_id == "cse_id_789"
        assert config.max_results_per_query == 50
        assert config.max_concurrent_searches == 10
        assert config.brave_enabled is False
        assert config.google_enabled is True

    def test_search_config_validation_max_results(self):
        """Test search config validation for max_results_per_query."""
        with pytest.raises(ValueError, match="max_results_per_query must be positive"):
            SearchConfig(max_results_per_query=0)
        
        with pytest.raises(ValueError, match="max_results_per_query must be positive"):
            SearchConfig(max_results_per_query=-1)

    def test_search_config_validation_max_concurrent(self):
        """Test search config validation for max_concurrent_searches."""
        with pytest.raises(ValueError, match="max_concurrent_searches must be positive"):
            SearchConfig(max_concurrent_searches=0)
        
        with pytest.raises(ValueError, match="max_concurrent_searches must be positive"):
            SearchConfig(max_concurrent_searches=-1)


class TestContentConfig:
    """Test the ContentConfig dataclass."""
    
    def test_content_config_defaults(self):
        """Test default content configuration values."""
        config = ContentConfig()
        
        assert config.fetch_timeout_seconds == 30
        assert config.max_content_size_bytes == 1048576  # 1MB
        assert config.max_concurrent_fetches == 10
        assert config.user_agent == "Umbrix CTI Crawler 1.0"
        assert config.max_retries == 3
        assert config.retry_delay_seconds == 1.0
        assert config.retry_backoff_factor == 2.0
        
        # Check default allowed content types
        assert "text/html" in config.allowed_content_types
        assert "application/xhtml+xml" in config.allowed_content_types
        assert "application/rss+xml" in config.allowed_content_types
        assert "application/atom+xml" in config.allowed_content_types

    def test_content_config_custom_values(self):
        """Test content configuration with custom values."""
        custom_content_types = ["text/plain", "application/json"]
        
        config = ContentConfig(
            fetch_timeout_seconds=60,
            max_content_size_bytes=2097152,  # 2MB
            max_concurrent_fetches=20,
            user_agent="Custom Crawler 2.0",
            max_retries=5,
            retry_delay_seconds=2.0,
            retry_backoff_factor=1.5,
            allowed_content_types=custom_content_types
        )
        
        assert config.fetch_timeout_seconds == 60
        assert config.max_content_size_bytes == 2097152
        assert config.max_concurrent_fetches == 20
        assert config.user_agent == "Custom Crawler 2.0"
        assert config.max_retries == 5
        assert config.retry_delay_seconds == 2.0
        assert config.retry_backoff_factor == 1.5
        assert config.allowed_content_types == custom_content_types

    def test_content_config_validation_timeout(self):
        """Test content config validation for fetch timeout."""
        with pytest.raises(ValueError, match="fetch_timeout_seconds must be positive"):
            ContentConfig(fetch_timeout_seconds=0)
        
        with pytest.raises(ValueError, match="fetch_timeout_seconds must be positive"):
            ContentConfig(fetch_timeout_seconds=-1)

    def test_content_config_validation_max_size(self):
        """Test content config validation for max content size."""
        with pytest.raises(ValueError, match="max_content_size_bytes must be positive"):
            ContentConfig(max_content_size_bytes=0)
        
        with pytest.raises(ValueError, match="max_content_size_bytes must be positive"):
            ContentConfig(max_content_size_bytes=-1)


class TestRedisConfig:
    """Test the RedisConfig dataclass."""
    
    def test_redis_config_defaults(self):
        """Test default Redis configuration values."""
        config = RedisConfig()
        
        assert config.url == "redis://localhost:6379"
        assert config.database == 0
        assert config.password is None
        assert config.connection_pool_size == 10
        assert config.socket_timeout == 30
        assert config.socket_connect_timeout == 30
        assert config.retry_on_timeout is True
        assert config.health_check_interval == 30

    def test_redis_config_custom_values(self):
        """Test Redis configuration with custom values."""
        config = RedisConfig(
            url="redis://redis-server:6379",
            database=1,
            password="secret123",
            connection_pool_size=20,
            socket_timeout=60,
            retry_on_timeout=False
        )
        
        assert config.url == "redis://redis-server:6379"
        assert config.database == 1
        assert config.password == "secret123"
        assert config.connection_pool_size == 20
        assert config.socket_timeout == 60
        assert config.retry_on_timeout is False

    def test_redis_config_validation_valid_urls(self):
        """Test Redis config validation with valid URLs."""
        # Standard redis URL
        config1 = RedisConfig(url="redis://localhost:6379")
        assert config1.url == "redis://localhost:6379"
        
        # SSL redis URL
        config2 = RedisConfig(url="rediss://secure-redis:6380")
        assert config2.url == "rediss://secure-redis:6380"

    def test_redis_config_validation_invalid_url_scheme(self):
        """Test Redis config validation with invalid URL scheme."""
        with pytest.raises(ValueError, match="Redis URL must use redis:// or rediss:// scheme"):
            RedisConfig(url="http://localhost:6379")
        
        with pytest.raises(ValueError, match="Redis URL must use redis:// or rediss:// scheme"):
            RedisConfig(url="tcp://localhost:6379")

    def test_redis_config_validation_malformed_url(self):
        """Test Redis config validation with malformed URL."""
        with pytest.raises(ValueError, match="Invalid Redis URL"):
            RedisConfig(url="not-a-valid-url")


class TestMonitoringConfig:
    """Test the MonitoringConfig dataclass."""
    
    def test_monitoring_config_defaults(self):
        """Test default monitoring configuration values."""
        config = MonitoringConfig()
        
        assert config.metrics_port == 8080
        assert config.metrics_path == "/metrics"
        assert config.health_check_path == "/health"
        assert config.readiness_check_path == "/ready"
        assert config.log_level == "INFO"
        assert config.log_format == "json"
        assert config.enable_correlation_id is True
        assert config.log_file is None

    def test_monitoring_config_custom_values(self):
        """Test monitoring configuration with custom values."""
        config = MonitoringConfig(
            metrics_port=9090,
            metrics_path="/custom-metrics",
            health_check_path="/custom-health",
            readiness_check_path="/custom-ready",
            log_level="DEBUG",
            log_format="text",
            enable_correlation_id=False,
            log_file="/var/log/agent.log"
        )
        
        assert config.metrics_port == 9090
        assert config.metrics_path == "/custom-metrics"
        assert config.health_check_path == "/custom-health"
        assert config.readiness_check_path == "/custom-ready"
        assert config.log_level == "DEBUG"
        assert config.log_format == "text"
        assert config.enable_correlation_id is False
        assert config.log_file == "/var/log/agent.log"

    def test_monitoring_config_validation_log_level(self):
        """Test monitoring config validation for log level."""
        # Valid log levels
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            config = MonitoringConfig(log_level=level)
            assert config.log_level == level
        
        # Invalid log level
        with pytest.raises(ValueError, match="log_level must be one of"):
            MonitoringConfig(log_level="INVALID")

    def test_monitoring_config_validation_case_insensitive_log_level(self):
        """Test monitoring config accepts lowercase log levels."""
        config = MonitoringConfig(log_level="debug")
        # The validation should work with uppercase comparison
        assert config.log_level == "debug"

    def test_monitoring_config_validation_metrics_port(self):
        """Test monitoring config validation for metrics port."""
        # Valid ports
        config1 = MonitoringConfig(metrics_port=1)
        assert config1.metrics_port == 1
        
        config2 = MonitoringConfig(metrics_port=65535)
        assert config2.metrics_port == 65535
        
        # Invalid ports
        with pytest.raises(ValueError, match="metrics_port must be between 1 and 65535"):
            MonitoringConfig(metrics_port=0)
        
        with pytest.raises(ValueError, match="metrics_port must be between 1 and 65535"):
            MonitoringConfig(metrics_port=65536)


class TestAgentConfig:
    """Test the AgentConfig dataclass."""
    
    def test_agent_config_defaults(self):
        """Test default agent configuration values."""
        with patch('logging.warning'):  # Suppress LLM warning
            config = AgentConfig()
            
            assert config.agent_name == "intelligent-crawler-agent"
            assert config.agent_description == "LLM-guided CTI source discovery agent"
            assert config.cti_backend_url == "http://localhost:8080/api"
            assert config.cti_api_key == ""
            
            # Check that sub-configs are properly initialized
            assert isinstance(config.kafka, KafkaConfig)
            assert isinstance(config.llm, LLMConfig)
            assert isinstance(config.search, SearchConfig)
            assert isinstance(config.content, ContentConfig)
            assert isinstance(config.redis, RedisConfig)
            assert isinstance(config.monitoring, MonitoringConfig)

    def test_agent_config_custom_values(self):
        """Test agent configuration with custom values."""
        with patch('logging.warning'):  # Suppress LLM warning
            kafka_config = KafkaConfig(bootstrap_servers="custom:9092")
            llm_config = LLMConfig(gemini_api_key="test_key")
            
            config = AgentConfig(
                kafka=kafka_config,
                llm=llm_config,
                agent_name="custom-agent",
                agent_description="Custom description",
                cti_backend_url="http://custom:8080/api",
                cti_api_key="custom_api_key"
            )
            
            assert config.kafka.bootstrap_servers == "custom:9092"
            assert config.llm.gemini_api_key == "test_key"
            assert config.agent_name == "custom-agent"
            assert config.agent_description == "Custom description"
            assert config.cti_backend_url == "http://custom:8080/api"
            assert config.cti_api_key == "custom_api_key"

    def test_agent_config_validation_empty_name(self):
        """Test agent config validation with empty name."""
        with patch('logging.warning'):  # Suppress LLM warning
            with pytest.raises(ValueError, match="agent_name cannot be empty"):
                AgentConfig(agent_name="")


class TestConfigManager:
    """Test the ConfigManager class."""
    
    def test_config_manager_initialization(self):
        """Test ConfigManager initialization."""
        manager = ConfigManager()
        assert manager.config_file is None
        assert manager.config is None
        
        manager_with_file = ConfigManager(config_file="/path/to/config.yaml")
        assert manager_with_file.config_file == "/path/to/config.yaml"
        assert manager_with_file.config is None

    def test_load_config_no_file(self):
        """Test loading configuration with no config file."""
        manager = ConfigManager()
        
        with patch.object(manager, '_load_from_environment') as mock_load_env:
            mock_load_env.return_value = {'agent_name': 'test-agent'}
            
            with patch('logging.warning'):  # Suppress LLM warning
                config = manager.load_config()
            
            assert isinstance(config, AgentConfig)
            assert config.agent_name == 'test-agent'
            mock_load_env.assert_called_once()

    def test_load_config_with_file(self):
        """Test loading configuration with config file."""
        config_data = {
            'agent_name': 'file-agent',
            'kafka': {'bootstrap_servers': 'file-kafka:9092'},
            'llm': {'gemini_api_key': 'file-key'}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            manager = ConfigManager(config_file=temp_file)
            
            with patch.object(manager, '_load_from_environment') as mock_load_env:
                mock_load_env.return_value = {}
                config = manager.load_config()
            
            assert config.agent_name == 'file-agent'
            assert config.kafka.bootstrap_servers == 'file-kafka:9092'
            assert config.llm.gemini_api_key == 'file-key'
            
        finally:
            os.unlink(temp_file)

    def test_load_config_file_override_with_env(self):
        """Test that environment variables override config file values."""
        config_data = {
            'agent_name': 'file-agent',
            'kafka': {'bootstrap_servers': 'file-kafka:9092'}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            manager = ConfigManager(config_file=temp_file)
            
            env_data = {
                'agent_name': 'env-agent',
                'kafka': {'bootstrap_servers': 'env-kafka:9092'}
            }
            
            with patch.object(manager, '_load_from_environment') as mock_load_env:
                mock_load_env.return_value = env_data
                config = manager.load_config()
            
            # Environment should override file
            assert config.agent_name == 'env-agent'
            assert config.kafka.bootstrap_servers == 'env-kafka:9092'
            
        finally:
            os.unlink(temp_file)

    def test_load_from_environment(self):
        """Test loading configuration from environment variables."""
        manager = ConfigManager()
        
        env_vars = {
            'KAFKA_BOOTSTRAP_SERVERS': 'env-kafka:9092',
            'KAFKA_CONSUMER_GROUP_ID': 'env-group',
            'GEMINI_API_KEY': 'env-gemini-key',
            'BRAVE_API_KEY': 'env-brave-key',
            'REDIS_URL': 'redis://env-redis:6379',
            'LOG_LEVEL': 'DEBUG',
            'AGENT_NAME': 'env-agent'
        }
        
        with patch.dict(os.environ, env_vars):
            env_config = manager._load_from_environment()
        
        assert env_config['kafka']['bootstrap_servers'] == 'env-kafka:9092'
        assert env_config['kafka']['consumer_group_id'] == 'env-group'
        assert env_config['llm']['gemini_api_key'] == 'env-gemini-key'
        assert env_config['search']['brave_api_key'] == 'env-brave-key'
        assert env_config['redis']['url'] == 'redis://env-redis:6379'
        assert env_config['monitoring']['log_level'] == 'DEBUG'
        assert env_config['agent_name'] == 'env-agent'

    def test_load_from_environment_type_conversion(self):
        """Test type conversion when loading from environment."""
        manager = ConfigManager()
        
        env_vars = {
            'LLM_MAX_TOKENS': '2000',
            'LLM_TEMPERATURE': '0.5',
            'SEARCH_MAX_RESULTS': '50',
            'CONTENT_FETCH_TIMEOUT': '60',
            'REDIS_DATABASE': '1',
            'METRICS_PORT': '9090'
        }
        
        with patch.dict(os.environ, env_vars):
            env_config = manager._load_from_environment()
        
        assert env_config['llm']['max_tokens'] == 2000
        assert env_config['llm']['temperature'] == 0.5
        assert env_config['search']['max_results_per_query'] == 50
        assert env_config['content']['fetch_timeout_seconds'] == 60
        assert env_config['redis']['database'] == 1
        assert env_config['monitoring']['metrics_port'] == 9090

    def test_merge_configs(self):
        """Test merging file and environment configurations."""
        manager = ConfigManager()
        
        file_config = {
            'agent_name': 'file-agent',
            'kafka': {
                'bootstrap_servers': 'file-kafka:9092',
                'consumer_group_id': 'file-group'
            },
            'llm': {
                'model_name': 'file-model'
            }
        }
        
        env_config = {
            'agent_name': 'env-agent',
            'kafka': {
                'bootstrap_servers': 'env-kafka:9092'
            },
            'search': {
                'brave_api_key': 'env-brave-key'
            }
        }
        
        merged = manager._merge_configs(file_config, env_config)
        
        # Environment should override
        assert merged['agent_name'] == 'env-agent'
        assert merged['kafka']['bootstrap_servers'] == 'env-kafka:9092'
        
        # File values should remain where not overridden
        assert merged['kafka']['consumer_group_id'] == 'file-group'
        assert merged['llm']['model_name'] == 'file-model'
        
        # New environment values should be added
        assert merged['search']['brave_api_key'] == 'env-brave-key'

    def test_get_config_loads_if_none(self):
        """Test that get_config loads configuration if not already loaded."""
        manager = ConfigManager()
        
        with patch.object(manager, 'load_config') as mock_load:
            with patch('logging.warning'):  # Suppress LLM warning
                mock_config = AgentConfig()
            mock_load.return_value = mock_config
            
            result = manager.get_config()
            
            assert result == mock_config
            mock_load.assert_called_once()

    def test_get_config_returns_existing(self):
        """Test that get_config returns existing configuration."""
        manager = ConfigManager()
        
        with patch('logging.warning'):  # Suppress LLM warning
            existing_config = AgentConfig()
        manager.config = existing_config
        
        with patch.object(manager, 'load_config') as mock_load:
            result = manager.get_config()
            
            assert result == existing_config
            mock_load.assert_not_called()

    def test_reload_config(self):
        """Test reloading configuration."""
        manager = ConfigManager()
        
        with patch.object(manager, 'load_config') as mock_load:
            with patch('logging.warning'):  # Suppress LLM warning
                mock_config = AgentConfig()
            mock_load.return_value = mock_config
            
            result = manager.reload_config()
            
            assert result == mock_config
            mock_load.assert_called_once()

    def test_validate_config_success(self):
        """Test successful configuration validation."""
        manager = ConfigManager()
        
        with patch.object(manager, 'get_config') as mock_get:
            with patch('logging.warning'):  # Suppress LLM warning
                mock_config = AgentConfig()
            mock_get.return_value = mock_config
            
            result = manager.validate_config()
            
            assert result is True
            mock_get.assert_called_once()

    def test_validate_config_failure(self):
        """Test failed configuration validation."""
        manager = ConfigManager()
        
        with patch.object(manager, 'get_config') as mock_get:
            mock_get.side_effect = ValueError("Invalid configuration")
            
            with patch('logging.error') as mock_log_error:
                result = manager.validate_config()
            
            assert result is False
            mock_log_error.assert_called_once_with(
                "Configuration validation failed: Invalid configuration"
            )


class TestGlobalFunctions:
    """Test global configuration functions."""
    
    def test_get_config_manager_creates_instance(self):
        """Test that get_config_manager creates a global instance."""
        # Reset global state
        import discovery_agent.config as config_module
        config_module._config_manager = None
        
        with patch.dict(os.environ, {'AGENT_CONFIG_PATH': '/test/config.yaml'}):
            manager = get_config_manager()
        
        assert isinstance(manager, ConfigManager)
        assert manager.config_file == '/test/config.yaml'
        
        # Second call should return same instance
        manager2 = get_config_manager()
        assert manager is manager2

    def test_get_config_manager_default_path(self):
        """Test get_config_manager with default config path."""
        # Reset global state
        import discovery_agent.config as config_module
        config_module._config_manager = None
        
        # Clear environment variable if set
        with patch.dict(os.environ, {}, clear=True):
            if 'AGENT_CONFIG_PATH' in os.environ:
                del os.environ['AGENT_CONFIG_PATH']
            
            manager = get_config_manager()
        
        assert manager.config_file == '/app/config.yaml'

    def test_get_config_function(self):
        """Test the global get_config function."""
        # Reset global state
        import discovery_agent.config as config_module
        config_module._config_manager = None
        
        with patch('discovery_agent.config.get_config_manager') as mock_get_manager:
            mock_manager = Mock()
            with patch('logging.warning'):  # Suppress LLM warning
                mock_config = AgentConfig()
            mock_manager.get_config.return_value = mock_config
            mock_get_manager.return_value = mock_manager
            
            result = get_config()
            
            assert result == mock_config
            mock_get_manager.assert_called_once()
            mock_manager.get_config.assert_called_once()


class TestConfigIntegration:
    """Integration tests for configuration system."""
    
    def test_full_configuration_workflow(self):
        """Test the complete configuration loading workflow."""
        # Create a temporary config file
        config_data = {
            'agent_name': 'integration-test-agent',
            'kafka': {
                'bootstrap_servers': 'integration-kafka:9092',
                'consumer_group_id': 'integration-group'
            },
            'llm': {
                'gemini_api_key': 'integration-key',
                'model_name': 'integration-model',
                'max_tokens': 1500,
                'temperature': 0.8
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            manager = ConfigManager(config_file=temp_file)
            
            # Mock the environment loading to avoid conflicts
            with patch.object(manager, '_load_from_environment') as mock_env:
                mock_env.return_value = {}
                config = manager.load_config()
            
            # Check that file values are loaded correctly
            assert config.agent_name == 'integration-test-agent'
            assert config.kafka.bootstrap_servers == 'integration-kafka:9092'
            assert config.kafka.consumer_group_id == 'integration-group'
            assert config.llm.gemini_api_key == 'integration-key'
            assert config.llm.model_name == 'integration-model'
            assert config.llm.max_tokens == 1500
            assert config.llm.temperature == 0.8
            
            # Validate that the configuration is valid
            assert manager.validate_config() is True
            
        finally:
            os.unlink(temp_file)

    def test_configuration_error_handling(self):
        """Test configuration error handling scenarios."""
        # Test with invalid YAML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")  # Invalid YAML
            temp_file = f.name
        
        try:
            manager = ConfigManager(config_file=temp_file)
            
            # Should handle YAML parsing errors gracefully
            with pytest.raises(yaml.YAMLError):
                manager.load_config()
                
        finally:
            os.unlink(temp_file)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])