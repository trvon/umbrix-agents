"""
Tests for deduplication store implementations.
"""
import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from agents.common_tools.dedupe_store import DedupeStore, RedisDedupeStore, InMemoryDedupeStore


class TestDedupeStoreBase:
    """Test the base DedupeStore interface."""
    
    def test_base_class_methods_not_implemented(self):
        """Test that base class methods raise NotImplementedError."""
        store = DedupeStore()
        
        with pytest.raises(NotImplementedError):
            store.set_if_not_exists("test", 100)
            
        with pytest.raises(NotImplementedError):
            store.exists("test")
        
        # close() should not raise an error
        store.close()


class TestRedisDedupeStore:
    """Test Redis-based deduplication store."""
    
    @patch('redis.Redis')
    def test_successful_connection(self, mock_redis_class):
        """Test successful Redis connection."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.return_value = mock_redis
        
        store = RedisDedupeStore("redis://localhost:6379/0", namespace="test")
        
        assert store.redis_client == mock_redis
        assert store.namespace == "test"
        mock_redis.ping.assert_called_once()
    
    @patch('redis.Redis')
    def test_failed_connection_fallback(self, mock_redis_class):
        """Test fallback to memory store when Redis connection fails."""
        mock_redis_class.side_effect = Exception("Connection failed")
        
        store = RedisDedupeStore("redis://localhost:6379/0")
        
        assert store.redis_client is None
        assert hasattr(store, '_memory_store')
        assert hasattr(store, '_expiry_times')
    
    def test_namespaced_key(self):
        """Test key namespacing."""
        with patch('redis.Redis') as mock_redis_class:
            mock_redis = Mock()
            mock_redis.ping.return_value = True
            mock_redis_class.return_value = mock_redis
            
            store = RedisDedupeStore("redis://localhost:6379/0", namespace="myapp")
            namespaced = store._namespaced_key("test_key")
            
            assert namespaced == "myapp:test_key"
    
    @patch('redis.Redis')
    def test_set_if_not_exists_redis_success(self, mock_redis_class):
        """Test successful set_if_not_exists with Redis."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis.set.return_value = True  # Key was set
        mock_redis_class.return_value = mock_redis
        
        store = RedisDedupeStore("redis://localhost:6379/0")
        result = store.set_if_not_exists("test_key", 100)
        
        assert result is True
        mock_redis.set.assert_called_once()
        args, kwargs = mock_redis.set.call_args
        assert args[0] == "dedupe:test_key"
        assert kwargs['nx'] is True
        assert kwargs['ex'] == 100
    
    @patch('redis.Redis')
    def test_set_if_not_exists_redis_exists(self, mock_redis_class):
        """Test set_if_not_exists when key already exists in Redis."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis.set.return_value = None  # Key already exists
        mock_redis_class.return_value = mock_redis
        
        store = RedisDedupeStore("redis://localhost:6379/0")
        result = store.set_if_not_exists("test_key", 100)
        
        assert result is False
    
    @patch('redis.Redis')
    def test_set_if_not_exists_redis_error_fallback(self, mock_redis_class):
        """Test fallback to memory store when Redis operation fails."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis.set.side_effect = Exception("Redis error")
        mock_redis_class.return_value = mock_redis
        
        store = RedisDedupeStore("redis://localhost:6379/0")
        
        # With the fixed code, the memory store should be created on Redis error
        result1 = store.set_if_not_exists("test_key", 100)
        assert result1 is True
        
        # Second call should fail (duplicate)
        result2 = store.set_if_not_exists("test_key", 100)
        assert result2 is False
    
    @patch('redis.Redis')
    def test_exists_redis(self, mock_redis_class):
        """Test exists method with Redis."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis.exists.return_value = 1  # Key exists
        mock_redis_class.return_value = mock_redis
        
        store = RedisDedupeStore("redis://localhost:6379/0")
        result = store.exists("test_key")
        
        assert result is True
        mock_redis.exists.assert_called_once_with("dedupe:test_key")
    
    @patch('redis.Redis')
    def test_exists_redis_not_found(self, mock_redis_class):
        """Test exists method with Redis when key doesn't exist."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis.exists.return_value = 0  # Key doesn't exist
        mock_redis_class.return_value = mock_redis
        
        store = RedisDedupeStore("redis://localhost:6379/0")
        result = store.exists("test_key")
        
        assert result is False
    
    @patch('redis.Redis')
    def test_memory_store_cleanup_expired_keys(self, mock_redis_class):
        """Test cleanup of expired keys in memory store."""
        mock_redis_class.side_effect = Exception("No Redis")
        
        store = RedisDedupeStore("redis://localhost:6379/0")
        
        # Add a key that will expire
        current_time = time.time()
        store._memory_store["test_key"] = current_time
        store._expiry_times["test_key"] = current_time - 1  # Already expired
        
        # Add a key that won't expire
        store._memory_store["valid_key"] = current_time
        store._expiry_times["valid_key"] = current_time + 100
        
        store._cleanup_expired_memory_keys()
        
        assert "test_key" not in store._memory_store
        assert "test_key" not in store._expiry_times
        assert "valid_key" in store._memory_store
        assert "valid_key" in store._expiry_times
    
    @patch('redis.Redis')
    def test_memory_store_set_if_not_exists(self, mock_redis_class):
        """Test set_if_not_exists with memory store fallback."""
        mock_redis_class.side_effect = Exception("No Redis")
        
        store = RedisDedupeStore("redis://localhost:6379/0")
        
        # First call should succeed
        result1 = store.set_if_not_exists("test_key", 100)
        assert result1 is True
        
        # Second call should fail (duplicate)
        result2 = store.set_if_not_exists("test_key", 100)
        assert result2 is False
        
        # Check that key exists in memory store
        assert "dedupe:test_key" in store._memory_store
        assert "dedupe:test_key" in store._expiry_times
    
    @patch('redis.Redis')
    def test_memory_store_exists(self, mock_redis_class):
        """Test exists method with memory store."""
        mock_redis_class.side_effect = Exception("No Redis")
        
        store = RedisDedupeStore("redis://localhost:6379/0")
        
        # Key doesn't exist initially
        assert store.exists("test_key") is False
        
        # Add key to memory store
        store.set_if_not_exists("test_key", 100)
        
        # Key should now exist
        assert store.exists("test_key") is True
    
    @patch('redis.Redis')
    def test_close_with_redis(self, mock_redis_class):
        """Test close method with Redis connection."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.return_value = mock_redis
        
        store = RedisDedupeStore("redis://localhost:6379/0")
        store.close()
        
        mock_redis.close.assert_called_once()
    
    @patch('redis.Redis')
    def test_close_without_redis(self, mock_redis_class):
        """Test close method without Redis connection."""
        mock_redis_class.side_effect = Exception("No Redis")
        
        store = RedisDedupeStore("redis://localhost:6379/0")
        # Should not raise an error
        store.close()


class TestInMemoryDedupeStore:
    """Test in-memory deduplication store."""
    
    def test_set_if_not_exists_new_key(self):
        """Test setting a new key."""
        store = InMemoryDedupeStore()
        
        result = store.set_if_not_exists("test_key", 100)
        assert result is True
    
    def test_set_if_not_exists_existing_key(self):
        """Test setting an existing key."""
        store = InMemoryDedupeStore()
        
        # Set key first time
        store.set_if_not_exists("test_key", 100)
        
        # Try to set again
        result = store.set_if_not_exists("test_key", 100)
        assert result is False
    
    def test_exists(self):
        """Test exists method."""
        store = InMemoryDedupeStore()
        
        # Key doesn't exist initially
        assert store.exists("test_key") is False
        
        # Add key
        store.set_if_not_exists("test_key", 100)
        
        # Key should exist now
        assert store.exists("test_key") is True
    
    @patch('time.time')
    def test_expired_key_cleanup(self, mock_time):
        """Test automatic cleanup of expired keys."""
        # Start at time 100
        mock_time.return_value = 100
        
        store = InMemoryDedupeStore()
        
        # Add a key with 10 second TTL
        result = store.set_if_not_exists("test_key", 10)
        assert result is True
        assert store.exists("test_key") is True
        
        # Move forward 15 seconds (key should be expired)
        mock_time.return_value = 115
        
        # Key should no longer exist
        assert store.exists("test_key") is False
        
        # Should be able to set it again
        result = store.set_if_not_exists("test_key", 10)
        assert result is True
    
    def test_clear(self):
        """Test clearing all keys."""
        store = InMemoryDedupeStore()
        
        # Add some keys
        store.set_if_not_exists("key1", 100)
        store.set_if_not_exists("key2", 100)
        
        assert store.exists("key1") is True
        assert store.exists("key2") is True
        
        # Clear store
        store.clear()
        
        assert store.exists("key1") is False
        assert store.exists("key2") is False