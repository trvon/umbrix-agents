# Make common_tools a proper Python package
# This allows imports from 'common_tools.schema_validator' to work

# Import new shared utility modules
try:
    from . import kafka_tools  
    from . import redis_tools
    from . import http_tools
    from . import llm_tools
    from . import testing_tools
    
    # Export main classes for easy importing
    from .kafka_tools import EnhancedKafkaWrapper, TopicManager, MessageRouter
    from .redis_tools import EnhancedRedisClient, DeduplicationManager, RateLimiter, DistributedLock
    from .http_tools import EnhancedHttpClient, ApiClient, HttpClientError
    from .llm_tools import LLMManager, GeminiProvider, OpenAIProvider, PromptManager
    from .testing_tools import AgentTestFixture, agent_test_context, create_mock_agent_config
    
    _UTILS_AVAILABLE = True
except ImportError as e:
    # Fallback if dependencies aren't available
    _UTILS_AVAILABLE = False
    print(f"Some utility modules not available: {e}")

# Agent base classes - import separately to avoid circular dependencies
try:
    from . import agent_base
    from .agent_base import BaseAgent, KafkaConsumerAgent, PollingAgent
    _AGENT_BASE_AVAILABLE = True
except ImportError as e:
    _AGENT_BASE_AVAILABLE = False
    print(f"Agent base classes not available: {e}")

__all__ = [
    # Base agent classes
    'BaseAgent',
    'KafkaConsumerAgent', 
    'PollingAgent',
    
    # Kafka utilities
    'EnhancedKafkaWrapper',
    'TopicManager',
    'MessageRouter',
    
    # Redis utilities
    'EnhancedRedisClient',
    'DeduplicationManager',
    'RateLimiter',
    'DistributedLock',
    
    # HTTP utilities
    'EnhancedHttpClient',
    'ApiClient',
    'HttpClientError',
    
    # LLM utilities
    'LLMManager',
    'GeminiProvider',
    'OpenAIProvider',
    'PromptManager',
    
    # Testing utilities
    'AgentTestFixture',
    'agent_test_context',
    'create_mock_agent_config',
    
    # Module exports
    'agent_base',
    'kafka_tools',
    'redis_tools',
    'http_tools',
    'llm_tools',
    'testing_tools'
]
