"""
Configuration management for Intelligent Crawler Agent.

This module handles all configuration parameters, environment variables,
validation, and default values for the intelligent crawler agent.
"""

import os
import logging
from typing import Dict, List, Optional, Union
from dataclasses import dataclass, field
from pathlib import Path
import yaml
from urllib.parse import urlparse


@dataclass
class KafkaConfig:
    """Kafka configuration for the intelligent crawler agent."""
    bootstrap_servers: str = "localhost:9092"
    consumer_group_id: str = "intelligent-crawler-group"
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 1000
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 3000
    max_poll_records: int = 500
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    
    # Input topics
    input_topics: List[str] = field(default_factory=lambda: [
        "normalized.intel",
        "user.submissions", 
        "feeds.discovered"
    ])
    
    # Output topics
    output_topics: List[str] = field(default_factory=lambda: [
        "raw.intel.crawled",
        "feeds.discovered"
    ])
    
    def __post_init__(self):
        """Validate Kafka configuration."""
        if not self.bootstrap_servers:
            raise ValueError("Kafka bootstrap_servers cannot be empty")
        if not self.consumer_group_id:
            raise ValueError("Kafka consumer_group_id cannot be empty")


@dataclass
class LLMConfig:
    """LLM configuration for query generation."""
    gemini_api_key: str = ""
    google_api_key: str = ""
    model_name: str = "gemini-1.5-flash-latest"
    max_tokens: int = 1000
    temperature: float = 0.7
    timeout_seconds: int = 30
    max_retries: int = 3
    
    def __post_init__(self):
        """Validate LLM configuration."""
        if not self.gemini_api_key and not self.google_api_key:
            logging.warning("No LLM API keys provided - LLM functionality will be limited")
        if self.temperature < 0.0 or self.temperature > 2.0:
            raise ValueError("LLM temperature must be between 0.0 and 2.0")
        if self.max_tokens < 1:
            raise ValueError("LLM max_tokens must be positive")


@dataclass
class SearchConfig:
    """Search provider configuration."""
    brave_api_key: str = ""
    google_cse_api_key: str = ""
    google_cse_id: str = ""
    
    # Provider settings
    max_results_per_query: int = 20
    max_concurrent_searches: int = 5
    search_timeout_seconds: int = 30
    
    # Rate limiting (requests per hour)
    brave_rate_limit: int = 100
    google_rate_limit: int = 100
    
    # Provider enablement
    brave_enabled: bool = True
    google_enabled: bool = True
    
    def __post_init__(self):
        """Validate search configuration."""
        if self.max_results_per_query < 1:
            raise ValueError("max_results_per_query must be positive")
        if self.max_concurrent_searches < 1:
            raise ValueError("max_concurrent_searches must be positive")


@dataclass
class ContentConfig:
    """Content fetching configuration."""
    fetch_timeout_seconds: int = 30
    max_content_size_bytes: int = 1048576  # 1MB
    max_concurrent_fetches: int = 10
    user_agent: str = "Umbrix CTI Crawler 1.0"
    
    # Retry configuration
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    retry_backoff_factor: float = 2.0
    
    # Content filtering
    allowed_content_types: List[str] = field(default_factory=lambda: [
        "text/html",
        "application/xhtml+xml",
        "text/xml",
        "application/xml",
        "application/rss+xml",
        "application/atom+xml"
    ])
    
    def __post_init__(self):
        """Validate content configuration."""
        if self.fetch_timeout_seconds < 1:
            raise ValueError("fetch_timeout_seconds must be positive")
        if self.max_content_size_bytes < 1:
            raise ValueError("max_content_size_bytes must be positive")


@dataclass
class RedisConfig:
    """Redis configuration for caching and deduplication."""
    url: str = "redis://localhost:6379"
    database: int = 0
    password: Optional[str] = None
    connection_pool_size: int = 10
    socket_timeout: int = 30
    socket_connect_timeout: int = 30
    retry_on_timeout: bool = True
    health_check_interval: int = 30
    
    def __post_init__(self):
        """Validate Redis configuration."""
        try:
            parsed = urlparse(self.url)
            if parsed.scheme not in ('redis', 'rediss'):
                raise ValueError("Redis URL must use redis:// or rediss:// scheme")
        except Exception as e:
            raise ValueError(f"Invalid Redis URL: {e}")


@dataclass
class MonitoringConfig:
    """Monitoring and metrics configuration."""
    metrics_port: int = 8080
    metrics_path: str = "/metrics"
    health_check_path: str = "/health"
    readiness_check_path: str = "/ready"
    
    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "json"
    enable_correlation_id: bool = True
    log_file: Optional[str] = None
    
    def __post_init__(self):
        """Validate monitoring configuration."""
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_log_levels:
            raise ValueError(f"log_level must be one of {valid_log_levels}")
        if self.metrics_port < 1 or self.metrics_port > 65535:
            raise ValueError("metrics_port must be between 1 and 65535")


@dataclass
class AgentConfig:
    """Main configuration class for the Intelligent Crawler Agent."""
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    search: SearchConfig = field(default_factory=SearchConfig)
    content: ContentConfig = field(default_factory=ContentConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    
    # General agent settings
    agent_name: str = "intelligent-crawler-agent"
    agent_description: str = "LLM-guided CTI source discovery agent"
    cti_backend_url: str = "http://localhost:8080/api"
    cti_api_key: str = ""
    
    def __post_init__(self):
        """Validate overall configuration."""
        if not self.agent_name:
            raise ValueError("agent_name cannot be empty")


class ConfigManager:
    """Configuration manager for the Intelligent Crawler Agent."""
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize configuration manager."""
        self.config_file = config_file
        self.config: Optional[AgentConfig] = None
        
    def load_config(self) -> AgentConfig:
        """Load configuration from environment variables and config file."""
        config_data = {}
        
        # Load from config file if provided
        if self.config_file and Path(self.config_file).exists():
            with open(self.config_file, 'r') as f:
                config_data = yaml.safe_load(f) or {}
        
        # Override with environment variables
        env_config = self._load_from_environment()
        config_data = self._merge_configs(config_data, env_config)
        
        # Create configuration objects
        self.config = AgentConfig(
            kafka=KafkaConfig(**config_data.get('kafka', {})),
            llm=LLMConfig(**config_data.get('llm', {})),
            search=SearchConfig(**config_data.get('search', {})),
            content=ContentConfig(**config_data.get('content', {})),
            redis=RedisConfig(**config_data.get('redis', {})),
            monitoring=MonitoringConfig(**config_data.get('monitoring', {})),
            **{k: v for k, v in config_data.items() 
               if k not in ['kafka', 'llm', 'search', 'content', 'redis', 'monitoring']}
        )
        
        return self.config
    
    def _load_from_environment(self) -> Dict:
        """Load configuration from environment variables."""
        env_config = {
            'kafka': {
                'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                'consumer_group_id': os.getenv('KAFKA_CONSUMER_GROUP_ID', 'intelligent-crawler-group'),
                'auto_offset_reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest'),
            },
            'llm': {
                'gemini_api_key': os.getenv('GEMINI_API_KEY', ''),
                'google_api_key': os.getenv('GOOGLE_API_KEY', ''),
                'model_name': os.getenv('GEMINI_MODEL_NAME', 'gemini-1.5-flash-latest'),
                'max_tokens': int(os.getenv('LLM_MAX_TOKENS', '1000')),
                'temperature': float(os.getenv('LLM_TEMPERATURE', '0.7')),
            },
            'search': {
                'brave_api_key': os.getenv('BRAVE_API_KEY', ''),
                'google_cse_api_key': os.getenv('GOOGLE_CSE_API_KEY', ''),
                'google_cse_id': os.getenv('GOOGLE_CSE_ID', ''),
                'max_results_per_query': int(os.getenv('SEARCH_MAX_RESULTS', '20')),
                'max_concurrent_searches': int(os.getenv('SEARCH_MAX_CONCURRENT', '5')),
            },
            'content': {
                'fetch_timeout_seconds': int(os.getenv('CONTENT_FETCH_TIMEOUT', '30')),
                'max_content_size_bytes': int(os.getenv('CONTENT_MAX_SIZE', '1048576')),
                'user_agent': os.getenv('CONTENT_USER_AGENT', 'Umbrix CTI Crawler 1.0'),
            },
            'redis': {
                'url': os.getenv('REDIS_URL', 'redis://localhost:6379'),
                'database': int(os.getenv('REDIS_DATABASE', '0')),
                'password': os.getenv('REDIS_PASSWORD'),
            },
            'monitoring': {
                'metrics_port': int(os.getenv('METRICS_PORT', '8080')),
                'log_level': os.getenv('LOG_LEVEL', 'INFO'),
                'log_format': os.getenv('LOG_FORMAT', 'json'),
            },
            'agent_name': os.getenv('AGENT_NAME', 'intelligent-crawler-agent'),
            'cti_backend_url': os.getenv('CTI_BACKEND_URL', 'http://localhost:8080/api'),
            'cti_api_key': os.getenv('CTI_API_KEY', ''),
        }
        
        return env_config
    
    def _merge_configs(self, file_config: Dict, env_config: Dict) -> Dict:
        """Merge file configuration with environment configuration."""
        merged = file_config.copy()
        
        for key, value in env_config.items():
            if isinstance(value, dict) and key in merged:
                merged[key] = {**merged[key], **value}
            else:
                merged[key] = value
        
        return merged
    
    def get_config(self) -> AgentConfig:
        """Get the current configuration, loading if not already loaded."""
        if self.config is None:
            return self.load_config()
        return self.config
    
    def reload_config(self) -> AgentConfig:
        """Reload configuration from sources."""
        return self.load_config()
    
    def validate_config(self) -> bool:
        """Validate the current configuration."""
        try:
            config = self.get_config()
            return True
        except Exception as e:
            logging.error(f"Configuration validation failed: {e}")
            return False


# Global configuration instance
_config_manager: Optional[ConfigManager] = None


def get_config_manager() -> ConfigManager:
    """Get the global configuration manager instance."""
    global _config_manager
    if _config_manager is None:
        config_file = os.getenv('AGENT_CONFIG_PATH', '/app/config.yaml')
        _config_manager = ConfigManager(config_file)
    return _config_manager


def get_config() -> AgentConfig:
    """Get the current agent configuration."""
    return get_config_manager().get_config()