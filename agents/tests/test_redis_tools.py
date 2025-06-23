"""
Test coverage for common_tools/redis_tools.py

This module provides comprehensive test coverage for Redis utilities including
enhanced client, deduplication, rate limiting, and distributed locking.
"""

import pytest
import json
import time
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta
from typing import Dict, Any

import redis
import redis.asyncio as aioredis
from redis.exceptions import RedisError, ConnectionError

from common_tools.redis_tools import (
    CacheEntry,
    EnhancedRedisClient,
    DeduplicationManager,
    RateLimiter,
    DistributedLock
)


class TestCacheEntry:
    """Test the CacheEntry dataclass."""
    
    def test_cache_entry_creation_minimal(self):
        """Test creating CacheEntry with minimal fields."""
        entry = CacheEntry(
            key="test_key",
            value="test_value",
            timestamp=datetime.now()
        )
        
        assert entry.key == "test_key"
        assert entry.value == "test_value"
        assert isinstance(entry.timestamp, datetime)
        assert entry.ttl is None
        assert entry.hit_count == 0
    
    def test_cache_entry_creation_full(self):
        """Test creating CacheEntry with all fields."""
        timestamp = datetime.now()
        entry = CacheEntry(
            key="test_key",
            value={"data": "test"},
            timestamp=timestamp,
            ttl=3600,
            hit_count=5
        )
        
        assert entry.key == "test_key"
        assert entry.value == {"data": "test"}
        assert entry.timestamp == timestamp
        assert entry.ttl == 3600
        assert entry.hit_count == 5


class TestEnhancedRedisClient:
    """Test the EnhancedRedisClient class."""
    
    @pytest.fixture
    def mock_redis_sync(self):
        """Create a mock Redis sync client."""
        mock = Mock()
        mock.set.return_value = True
        mock.setex.return_value = True
        mock.get.return_value = b'{"test": "value"}'
        mock.delete.return_value = 1
        mock.exists.return_value = 1
        mock.close.return_value = None
        mock.incr.return_value = 2
        return mock
    
    @pytest.fixture
    def mock_redis_async(self):
        """Create a mock Redis async client."""
        mock = AsyncMock()
        mock.set.return_value = True
        mock.setex.return_value = True
        mock.get.return_value = b'{"test": "value"}'
        mock.delete.return_value = 1
        mock.exists.return_value = 1
        mock.close = AsyncMock()
        mock.pipeline.return_value = Mock()
        return mock
    
    @pytest.fixture
    def redis_client(self, mock_redis_sync, mock_redis_async):
        """Create an EnhancedRedisClient with mocked connections."""
        with patch('redis.ConnectionPool'):
            with patch('redis.asyncio.ConnectionPool'):
                with patch('redis.Redis', return_value=mock_redis_sync):
                    with patch('redis.asyncio.Redis', return_value=mock_redis_async):
                        with patch('common_tools.redis_tools.setup_logging'):
                            client = EnhancedRedisClient(
                                host='localhost',
                                port=6379,
                                namespace='test'
                            )
                            # Manually set the clients to avoid issues
                            client._sync_client = mock_redis_sync
                            client._async_client = mock_redis_async
                            return client
    
    def test_init_default_parameters(self):
        """Test initialization with default parameters."""
        with patch('redis.ConnectionPool'):
            with patch('redis.asyncio.ConnectionPool'):
                with patch('redis.Redis'):
                    with patch('redis.asyncio.Redis'):
                        with patch('common_tools.redis_tools.setup_logging'):
                            client = EnhancedRedisClient()
                        
                        assert client.host == 'localhost'
                        assert client.port == 6379
                        assert client.db == 0
                        assert client.password is None
                        assert client.namespace == 'umbrix'
                        assert client.retry_attempts == 3
                        assert client.retry_delay == 1.0
    
    def test_init_custom_parameters(self):
        """Test initialization with custom parameters."""
        with patch('redis.ConnectionPool'):
            with patch('redis.asyncio.ConnectionPool'):
                with patch('redis.Redis'):
                    with patch('redis.asyncio.Redis'):
                        with patch('common_tools.redis_tools.setup_logging'):
                            client = EnhancedRedisClient(
                            host='redis.example.com',
                            port=6380,
                            db=1,
                            password='secret',
                            namespace='custom',
                            max_connections=20,
                            retry_attempts=5,
                            retry_delay=2.0
                        )
                        
                        assert client.host == 'redis.example.com'
                        assert client.port == 6380
                        assert client.db == 1
                        assert client.password == 'secret'
                        assert client.namespace == 'custom'
                        assert client.retry_attempts == 5
                        assert client.retry_delay == 2.0
    
    def test_namespaced_key(self, redis_client):
        """Test key namespacing."""
        assert redis_client._namespaced_key("mykey") == "test:mykey"
        assert redis_client._namespaced_key("foo:bar") == "test:foo:bar"
    
    # Sync operation tests
    def test_set_sync_string(self, redis_client):
        """Test synchronous set operation with string value."""
        result = redis_client.set("key1", "value1")
        
        assert result is True
        redis_client._sync_client.set.assert_called_once_with("test:key1", "value1")
    
    def test_set_sync_with_ttl(self, redis_client):
        """Test synchronous set operation with TTL."""
        result = redis_client.set("key1", "value1", ttl=60)
        
        assert result is True
        redis_client._sync_client.setex.assert_called_once_with("test:key1", 60, "value1")
    
    def test_set_sync_json(self, redis_client):
        """Test synchronous set operation with JSON serialization."""
        data = {"foo": "bar", "num": 42}
        result = redis_client.set("key1", data)
        
        assert result is True
        redis_client._sync_client.set.assert_called_once_with("test:key1", json.dumps(data))
    
    def test_get_sync_json(self, redis_client):
        """Test synchronous get operation with JSON deserialization."""
        result = redis_client.get("key1")
        
        assert result == {"test": "value"}
        redis_client._sync_client.get.assert_called_once_with("test:key1")
    
    def test_get_sync_string(self, redis_client):
        """Test synchronous get operation with string value."""
        redis_client._sync_client.get.return_value = b"simple string"
        result = redis_client.get("key1")
        
        assert result == "simple string"
    
    def test_get_sync_none(self, redis_client):
        """Test synchronous get operation when key doesn't exist."""
        redis_client._sync_client.get.return_value = None
        result = redis_client.get("nonexistent")
        
        assert result is None
    
    def test_get_sync_with_default(self, redis_client):
        """Test synchronous get operation with default value."""
        redis_client._sync_client.get.return_value = None
        result = redis_client.get("nonexistent", default="default_value")
        
        assert result == "default_value"
    
    def test_delete_sync(self, redis_client):
        """Test synchronous delete operation."""
        result = redis_client.delete("key1")
        
        assert result is True
        redis_client._sync_client.delete.assert_called_once_with("test:key1")
    
    def test_exists_sync(self, redis_client):
        """Test synchronous exists operation."""
        result = redis_client.exists("key1")
        
        assert result is True
        redis_client._sync_client.exists.assert_called_once_with("test:key1")
    
    # Async operation tests
    @pytest.mark.asyncio
    async def test_aset_string(self, redis_client):
        """Test asynchronous set operation with string value."""
        result = await redis_client.aset("key1", "value1")
        
        assert result is True
        redis_client._async_client.set.assert_called_once_with("test:key1", "value1")
    
    @pytest.mark.asyncio
    async def test_aset_with_ttl(self, redis_client):
        """Test asynchronous set operation with TTL."""
        result = await redis_client.aset("key1", "value1", ttl=60)
        
        assert result is True
        redis_client._async_client.setex.assert_called_once_with("test:key1", 60, "value1")
    
    @pytest.mark.asyncio
    async def test_aset_json(self, redis_client):
        """Test asynchronous set operation with JSON serialization."""
        data = {"foo": "bar", "num": 42}
        result = await redis_client.aset("key1", data)
        
        assert result is True
        redis_client._async_client.set.assert_called_once_with("test:key1", json.dumps(data))
    
    @pytest.mark.asyncio
    async def test_aget_json(self, redis_client):
        """Test asynchronous get operation with JSON deserialization."""
        result = await redis_client.aget("key1")
        
        assert result == {"test": "value"}
        redis_client._async_client.get.assert_called_once_with("test:key1")
    
    @pytest.mark.asyncio
    async def test_aget_none(self, redis_client):
        """Test asynchronous get operation when key doesn't exist."""
        redis_client._async_client.get.return_value = None
        result = await redis_client.aget("nonexistent")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_adelete(self, redis_client):
        """Test asynchronous delete operation."""
        result = await redis_client.adelete("key1")
        
        assert result is True
        redis_client._async_client.delete.assert_called_once_with("test:key1")
    
    @pytest.mark.asyncio
    async def test_aexists(self, redis_client):
        """Test asynchronous exists operation."""
        result = await redis_client.aexists("key1")
        
        assert result is True
        redis_client._async_client.exists.assert_called_once_with("test:key1")
    
    @pytest.mark.asyncio
    async def test_close(self, redis_client):
        """Test closing Redis connections."""
        await redis_client.close()
        
        redis_client._async_client.close.assert_called_once()
        redis_client._sync_client.close.assert_called_once()
    
    # Retry logic tests
    def test_retry_sync_operation_success(self, redis_client):
        """Test sync retry logic with eventual success."""
        operation = Mock(side_effect=[ConnectionError("Failed"), "success"])
        
        with patch('time.sleep'):
            result = redis_client._retry_sync_operation(operation, "arg1", kwarg="val1")
        
        assert result == "success"
        assert operation.call_count == 2
    
    def test_retry_sync_operation_all_failures(self, redis_client):
        """Test sync retry logic when all attempts fail."""
        operation = Mock(side_effect=ConnectionError("Failed"))
        
        with patch('time.sleep'):
            with pytest.raises(ConnectionError):
                redis_client._retry_sync_operation(operation, "arg1")
        
        assert operation.call_count == 3  # Default retry_attempts
    
    @pytest.mark.asyncio
    async def test_retry_async_operation_success(self, redis_client):
        """Test async retry logic with eventual success."""
        operation = AsyncMock(side_effect=[ConnectionError("Failed"), "success"])
        
        result = await redis_client._retry_async_operation(operation, "arg1", kwarg="val1")
        
        assert result == "success"
        assert operation.call_count == 2
    
    @pytest.mark.asyncio
    async def test_retry_async_operation_all_failures(self, redis_client):
        """Test async retry logic when all attempts fail."""
        operation = AsyncMock(side_effect=ConnectionError("Failed"))
        
        with pytest.raises(ConnectionError):
            await redis_client._retry_async_operation(operation, "arg1")
        
        assert operation.call_count == 3  # Default retry_attempts


class TestDeduplicationManager:
    """Test the DeduplicationManager class."""
    
    @pytest.fixture
    def mock_redis_client(self):
        """Create a mock EnhancedRedisClient."""
        mock = Mock(spec=EnhancedRedisClient)
        mock.exists.return_value = False
        mock.set.return_value = True
        mock.aexists = AsyncMock(return_value=False)
        mock.aset = AsyncMock(return_value=True)
        return mock
    
    @pytest.fixture
    def dedupe_manager(self, mock_redis_client):
        """Create a DeduplicationManager instance."""
        return DeduplicationManager(
            redis_client=mock_redis_client,
            strategy='content_hash',
            ttl=3600
        )
    
    def test_init_default_strategy(self, mock_redis_client):
        """Test initialization with default strategy."""
        manager = DeduplicationManager(redis_client=mock_redis_client)
        
        assert manager.strategy == 'content_hash'
        assert manager.ttl == 86400  # 24 hours
        assert manager.max_entries is None
    
    def test_init_custom_parameters(self, mock_redis_client):
        """Test initialization with custom parameters."""
        manager = DeduplicationManager(
            redis_client=mock_redis_client,
            strategy='fast_hash',
            ttl=7200,
            max_entries=10000
        )
        
        assert manager.strategy == 'fast_hash'
        assert manager.ttl == 7200
        assert manager.max_entries == 10000
    
    def test_compute_hash_string(self, dedupe_manager):
        """Test hash computation for string content."""
        content = "test content"
        hash1 = dedupe_manager._compute_hash(content)
        hash2 = dedupe_manager._compute_hash(content)
        
        assert hash1 == hash2  # Same content should produce same hash
        assert len(hash1) == 64  # SHA256 produces 64 character hex string
    
    def test_compute_hash_dict(self, dedupe_manager):
        """Test hash computation for dictionary content."""
        content = {"key": "value", "num": 42}
        hash1 = dedupe_manager._compute_hash(content)
        
        # Same content in different order should produce same hash
        content2 = {"num": 42, "key": "value"}
        hash2 = dedupe_manager._compute_hash(content2)
        
        assert hash1 == hash2
    
    def test_compute_hash_fast_strategy(self, mock_redis_client):
        """Test hash computation with fast_hash strategy."""
        manager = DeduplicationManager(
            redis_client=mock_redis_client,
            strategy='fast_hash'
        )
        
        content = "test content"
        hash_value = manager._compute_hash(content)
        
        assert len(hash_value) == 32  # MD5 produces 32 character hex string
    
    def test_is_duplicate_false(self, dedupe_manager, mock_redis_client):
        """Test checking for duplicate when content is new."""
        content = "new content"
        result = dedupe_manager.is_duplicate(content)
        
        assert result is False
        mock_redis_client.exists.assert_called_once()
    
    def test_is_duplicate_true(self, dedupe_manager, mock_redis_client):
        """Test checking for duplicate when content exists."""
        mock_redis_client.exists.return_value = True
        
        content = "existing content"
        result = dedupe_manager.is_duplicate(content)
        
        assert result is True
        # Should update last seen timestamp
        assert mock_redis_client.set.call_count == 1
    
    def test_is_duplicate_with_identifier(self, dedupe_manager, mock_redis_client):
        """Test checking for duplicate with explicit identifier."""
        result = dedupe_manager.is_duplicate("content", identifier="unique_id")
        
        assert result is False
        # Should check with ID-based key
        call_args = mock_redis_client.exists.call_args[0][0]
        assert "dedupe:id:unique_id" in call_args
    
    @pytest.mark.asyncio
    async def test_is_duplicate_async_false(self, dedupe_manager, mock_redis_client):
        """Test async duplicate check when content is new."""
        content = "new content"
        result = await dedupe_manager.is_duplicate_async(content)
        
        assert result is False
        mock_redis_client.aexists.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_is_duplicate_async_true(self, dedupe_manager, mock_redis_client):
        """Test async duplicate check when content exists."""
        mock_redis_client.aexists.return_value = True
        
        content = "existing content"
        result = await dedupe_manager.is_duplicate_async(content)
        
        assert result is True
        # Should update last seen timestamp
        assert mock_redis_client.aset.call_count == 1
    
    def test_add_content(self, dedupe_manager, mock_redis_client):
        """Test adding content to deduplication store."""
        content = "new content"
        dedupe_manager.add(content)
        
        # Should store entry and last seen timestamp
        assert mock_redis_client.set.call_count == 2
        
        # Check that entry data includes required fields
        call_args = mock_redis_client.set.call_args_list[0]
        entry_data = call_args[0][1]
        assert 'timestamp' in entry_data
        assert 'strategy' in entry_data
        assert entry_data['strategy'] == 'content_hash'
    
    def test_add_with_metadata(self, dedupe_manager, mock_redis_client):
        """Test adding content with metadata."""
        content = "new content"
        metadata = {"source": "test", "priority": "high"}
        dedupe_manager.add(content, metadata=metadata)
        
        # Check that metadata is included
        call_args = mock_redis_client.set.call_args_list[0]
        entry_data = call_args[0][1]
        assert entry_data['metadata'] == metadata
    
    @pytest.mark.asyncio
    async def test_add_async(self, dedupe_manager, mock_redis_client):
        """Test async adding content to deduplication store."""
        content = "new content"
        await dedupe_manager.add_async(content)
        
        # Should store entry and last seen timestamp
        assert mock_redis_client.aset.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_stats_async(self, dedupe_manager):
        """Test getting deduplication statistics."""
        stats = await dedupe_manager.get_stats_async()
        
        assert stats['strategy'] == 'content_hash'
        assert stats['ttl'] == 3600
        assert stats['max_entries'] is None
        assert 'timestamp' in stats


class TestRateLimiter:
    """Test the RateLimiter class."""
    
    @pytest.fixture
    def mock_redis_client(self):
        """Create a mock EnhancedRedisClient."""
        mock = Mock(spec=EnhancedRedisClient)
        mock._sync_client = Mock()
        mock._async_client = AsyncMock()
        mock.get.return_value = 0
        mock.set.return_value = True
        mock._namespaced_key = lambda key: f"test:{key}"
        return mock
    
    @pytest.fixture
    def rate_limiter(self, mock_redis_client):
        """Create a RateLimiter instance."""
        return RateLimiter(
            redis_client=mock_redis_client,
            algorithm='sliding_window',
            default_limit=10,
            default_window=60
        )
    
    def test_init_default_parameters(self, mock_redis_client):
        """Test initialization with default parameters."""
        limiter = RateLimiter(redis_client=mock_redis_client)
        
        assert limiter.algorithm == 'sliding_window'
        assert limiter.default_limit == 100
        assert limiter.default_window == 60
    
    def test_init_custom_parameters(self, mock_redis_client):
        """Test initialization with custom parameters."""
        limiter = RateLimiter(
            redis_client=mock_redis_client,
            algorithm='token_bucket',
            default_limit=50,
            default_window=120
        )
        
        assert limiter.algorithm == 'token_bucket'
        assert limiter.default_limit == 50
        assert limiter.default_window == 120
    
    def test_is_allowed_sync_first_request(self, rate_limiter, mock_redis_client):
        """Test sync rate limiting for first request."""
        mock_redis_client.get.return_value = 0
        
        is_allowed, info = rate_limiter.is_allowed("user123")
        
        assert is_allowed is True
        assert info['current_count'] == 1
        assert info['limit'] == 10
        assert info['window'] == 60
        assert info['algorithm'] == 'counter'
    
    def test_is_allowed_sync_under_limit(self, rate_limiter, mock_redis_client):
        """Test sync rate limiting when under limit."""
        mock_redis_client.get.return_value = 5
        
        is_allowed, info = rate_limiter.is_allowed("user123")
        
        assert is_allowed is True
        assert info['current_count'] == 6
        assert info['limit'] == 10
    
    def test_is_allowed_sync_at_limit(self, rate_limiter, mock_redis_client):
        """Test sync rate limiting when at limit."""
        mock_redis_client.get.return_value = 10
        
        is_allowed, info = rate_limiter.is_allowed("user123")
        
        assert is_allowed is False
        assert info['current_count'] == 10
        assert info['limit'] == 10
    
    def test_is_allowed_sync_custom_limits(self, rate_limiter, mock_redis_client):
        """Test sync rate limiting with custom limits."""
        mock_redis_client.get.return_value = 3
        
        is_allowed, info = rate_limiter.is_allowed("user123", limit=5, window=30)
        
        assert is_allowed is True
        assert info['limit'] == 5
        assert info['window'] == 30
    
    @pytest.mark.asyncio
    async def test_is_allowed_async_sliding_window(self, rate_limiter, mock_redis_client):
        """Test async rate limiting with sliding window algorithm."""
        # Create a proper async pipeline mock that supports chaining
        mock_pipeline = AsyncMock()
        mock_pipeline.zremrangebyscore = Mock(return_value=mock_pipeline)
        mock_pipeline.zcard = Mock(return_value=mock_pipeline)
        mock_pipeline.zadd = Mock(return_value=mock_pipeline)
        mock_pipeline.expire = Mock(return_value=mock_pipeline)
        mock_pipeline.execute = AsyncMock(return_value=[None, 5, None, None])
        mock_pipeline.zrem = Mock(return_value=mock_pipeline)
        
        # Mock the pipeline method to return our pipeline mock
        mock_redis_client._async_client.pipeline = Mock(return_value=mock_pipeline)
        
        is_allowed, info = await rate_limiter.is_allowed_async("user123")
        
        assert is_allowed is True
        assert info['current_count'] == 6  # 5 existing + 1 new
        assert info['algorithm'] == 'sliding_window'
    
    @pytest.mark.asyncio
    async def test_is_allowed_async_sliding_window_limit_exceeded(self, rate_limiter, mock_redis_client):
        """Test async rate limiting when sliding window limit is exceeded."""
        # Create a proper async pipeline mock that supports chaining
        mock_pipeline = AsyncMock()
        mock_pipeline.zremrangebyscore = Mock(return_value=mock_pipeline)
        mock_pipeline.zcard = Mock(return_value=mock_pipeline)
        mock_pipeline.zadd = Mock(return_value=mock_pipeline)
        mock_pipeline.expire = Mock(return_value=mock_pipeline)
        mock_pipeline.execute = AsyncMock(return_value=[None, 10, None, None])
        mock_pipeline.zrem = Mock(return_value=mock_pipeline)
        
        # Mock the pipeline method to return our pipeline mock
        mock_redis_client._async_client.pipeline = Mock(return_value=mock_pipeline)
        mock_redis_client._async_client.zrem = AsyncMock()
        
        is_allowed, info = await rate_limiter.is_allowed_async("user123")
        
        assert is_allowed is False
        assert info['current_count'] == 11  # Would be 11, but not allowed
        # Should remove the request since it's not allowed
        mock_redis_client._async_client.zrem.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_is_allowed_async_token_bucket(self, mock_redis_client):
        """Test async rate limiting with token bucket algorithm."""
        limiter = RateLimiter(
            redis_client=mock_redis_client,
            algorithm='token_bucket',
            default_limit=10,
            default_window=60
        )
        
        # Mock bucket data
        bucket_data = {
            'tokens': 5.0,
            'last_refill': time.time() - 30  # 30 seconds ago
        }
        mock_redis_client.aget = AsyncMock(return_value=bucket_data)
        mock_redis_client.aset = AsyncMock(return_value=True)
        
        is_allowed, info = await limiter.is_allowed_async("user123")
        
        assert is_allowed is True
        assert info['algorithm'] == 'token_bucket'
        assert info['limit'] == 10
        assert 'current_tokens' in info
        assert 'refill_rate' in info
    
    @pytest.mark.asyncio
    async def test_is_allowed_async_unknown_algorithm(self, mock_redis_client):
        """Test async rate limiting with unknown algorithm."""
        limiter = RateLimiter(
            redis_client=mock_redis_client,
            algorithm='unknown_algo'
        )
        
        with pytest.raises(ValueError, match="Unknown rate limiting algorithm"):
            await limiter.is_allowed_async("user123")


class TestDistributedLock:
    """Test the DistributedLock class."""
    
    @pytest.fixture
    def mock_redis_client(self):
        """Create a mock EnhancedRedisClient."""
        mock = Mock(spec=EnhancedRedisClient)
        mock._sync_client = Mock()
        mock._async_client = AsyncMock()
        mock._namespaced_key = lambda key: f"test:{key}"
        return mock
    
    @pytest.fixture
    def distributed_lock(self, mock_redis_client):
        """Create a DistributedLock instance."""
        return DistributedLock(
            redis_client=mock_redis_client,
            lock_name="test_lock",
            timeout=30
        )
    
    def test_init_default_parameters(self, mock_redis_client):
        """Test initialization with default parameters."""
        lock = DistributedLock(
            redis_client=mock_redis_client,
            lock_name="my_lock"
        )
        
        assert lock.lock_name == "my_lock"
        assert lock.timeout == 30
        assert lock.auto_renewal is False
        assert lock.lock_key == "lock:my_lock"
        assert lock._acquired is False
    
    def test_init_with_auto_renewal(self, mock_redis_client):
        """Test initialization with auto renewal enabled."""
        lock = DistributedLock(
            redis_client=mock_redis_client,
            lock_name="my_lock",
            timeout=60,
            auto_renewal=True
        )
        
        assert lock.timeout == 60
        assert lock.auto_renewal is True
    
    def test_acquire_sync_success(self, distributed_lock, mock_redis_client):
        """Test successful synchronous lock acquisition."""
        mock_redis_client._sync_client.set.return_value = True
        
        result = distributed_lock.acquire()
        
        assert result is True
        assert distributed_lock._acquired is True
        
        # Verify set was called with correct parameters
        mock_redis_client._sync_client.set.assert_called_once()
        call_args = mock_redis_client._sync_client.set.call_args
        assert call_args[1]['nx'] is True  # Only set if not exists
        assert call_args[1]['ex'] == 30    # Expiration time
    
    def test_acquire_sync_failure(self, distributed_lock, mock_redis_client):
        """Test failed synchronous lock acquisition."""
        mock_redis_client._sync_client.set.return_value = False
        
        result = distributed_lock.acquire()
        
        assert result is False
        assert distributed_lock._acquired is False
    
    def test_acquire_sync_custom_timeout(self, distributed_lock, mock_redis_client):
        """Test synchronous lock acquisition with custom timeout."""
        mock_redis_client._sync_client.set.return_value = True
        
        result = distributed_lock.acquire(timeout=60)
        
        assert result is True
        call_args = mock_redis_client._sync_client.set.call_args
        assert call_args[1]['ex'] == 60
    
    @pytest.mark.asyncio
    async def test_acquire_async_success(self, distributed_lock, mock_redis_client):
        """Test successful asynchronous lock acquisition."""
        mock_redis_client._async_client.set.return_value = True
        
        result = await distributed_lock.acquire_async()
        
        assert result is True
        assert distributed_lock._acquired is True
    
    @pytest.mark.asyncio
    async def test_acquire_async_with_auto_renewal(self, mock_redis_client):
        """Test async lock acquisition with auto renewal."""
        lock = DistributedLock(
            redis_client=mock_redis_client,
            lock_name="test_lock",
            timeout=30,
            auto_renewal=True
        )
        
        mock_redis_client._async_client.set.return_value = True
        
        with patch('asyncio.create_task') as mock_create_task:
            result = await lock.acquire_async()
            
            assert result is True
            # Should start renewal task
            mock_create_task.assert_called_once()
    
    def test_release_sync_success(self, distributed_lock, mock_redis_client):
        """Test successful synchronous lock release."""
        # First acquire the lock
        distributed_lock._acquired = True
        
        # Mock Lua script execution
        mock_redis_client._sync_client.eval.return_value = 1
        
        result = distributed_lock.release()
        
        assert result is True
        assert distributed_lock._acquired is False
        
        # Verify Lua script was executed
        mock_redis_client._sync_client.eval.assert_called_once()
    
    def test_release_sync_not_acquired(self, distributed_lock, mock_redis_client):
        """Test releasing a lock that wasn't acquired."""
        distributed_lock._acquired = False
        
        result = distributed_lock.release()
        
        assert result is False
        # Should not try to release
        mock_redis_client._sync_client.eval.assert_not_called()
    
    def test_release_sync_wrong_owner(self, distributed_lock, mock_redis_client):
        """Test releasing a lock owned by someone else."""
        distributed_lock._acquired = True
        
        # Mock Lua script returning 0 (lock not owned by us)
        mock_redis_client._sync_client.eval.return_value = 0
        
        result = distributed_lock.release()
        
        assert result is False
        # Should remain acquired since we couldn't release it
        assert distributed_lock._acquired is True
    
    @pytest.mark.asyncio
    async def test_release_async_success(self, distributed_lock, mock_redis_client):
        """Test successful asynchronous lock release."""
        distributed_lock._acquired = True
        
        # Mock Lua script execution
        mock_redis_client._async_client.execute_command.return_value = 1
        
        result = await distributed_lock.release_async()
        
        assert result is True
        assert distributed_lock._acquired is False
    
    @pytest.mark.asyncio
    async def test_release_async_with_renewal_task(self, distributed_lock, mock_redis_client):
        """Test releasing a lock with active renewal task."""
        distributed_lock._acquired = True
        
        # Mock renewal task
        mock_task = Mock()
        mock_task.cancel = Mock()
        distributed_lock._renewal_task = mock_task
        
        mock_redis_client._async_client.execute_command.return_value = 1
        
        result = await distributed_lock.release_async()
        
        assert result is True
        # Should cancel renewal task
        mock_task.cancel.assert_called_once()
        assert distributed_lock._renewal_task is None
    
    # Context manager tests
    def test_sync_context_manager_success(self, distributed_lock, mock_redis_client):
        """Test using lock as sync context manager successfully."""
        mock_redis_client._sync_client.set.return_value = True
        mock_redis_client._sync_client.eval.return_value = 1
        
        with distributed_lock as lock:
            assert lock._acquired is True
        
        assert distributed_lock._acquired is False
    
    def test_sync_context_manager_acquisition_failure(self, distributed_lock, mock_redis_client):
        """Test context manager when lock acquisition fails."""
        mock_redis_client._sync_client.set.return_value = False
        
        with pytest.raises(RuntimeError, match="Failed to acquire lock"):
            with distributed_lock:
                pass
    
    @pytest.mark.asyncio
    async def test_async_context_manager_success(self, distributed_lock, mock_redis_client):
        """Test using lock as async context manager successfully."""
        mock_redis_client._async_client.set.return_value = True
        mock_redis_client._async_client.execute_command.return_value = 1
        
        async with distributed_lock as lock:
            assert lock._acquired is True
        
        assert distributed_lock._acquired is False
    
    @pytest.mark.asyncio
    async def test_async_context_manager_acquisition_failure(self, distributed_lock, mock_redis_client):
        """Test async context manager when lock acquisition fails."""
        mock_redis_client._async_client.set.return_value = False
        
        with pytest.raises(RuntimeError, match="Failed to acquire lock"):
            async with distributed_lock:
                pass


class TestEdgeCasesAndErrors:
    """Test edge cases and error handling."""
    
    @pytest.fixture
    def redis_client_edge(self):
        """Create an EnhancedRedisClient with mocked connections."""
        with patch('redis.ConnectionPool'):
            with patch('redis.asyncio.ConnectionPool'):
                with patch('redis.Redis') as mock_sync:
                    with patch('redis.asyncio.Redis') as mock_async:
                        with patch('common_tools.redis_tools.setup_logging'):
                            client = EnhancedRedisClient()
                            # Create proper mock instances
                            client._sync_client = Mock()
                            client._async_client = AsyncMock()
                            return client
    
    def test_get_sync_redis_error(self, redis_client_edge):
        """Test handling RedisError in sync get operation."""
        redis_client_edge._sync_client.get.side_effect = RedisError("Connection lost")
        
        result = redis_client_edge.get("key1", default="fallback")
        
        assert result == "fallback"
    
    @pytest.mark.asyncio
    async def test_aget_redis_error(self, redis_client_edge):
        """Test handling RedisError in async get operation."""
        redis_client_edge._async_client.get.side_effect = RedisError("Connection lost")
        
        result = await redis_client_edge.aget("key1", default="fallback")
        
        assert result == "fallback"
    
    def test_json_decode_error_handling(self, redis_client_edge):
        """Test handling JSON decode errors."""
        redis_client_edge._sync_client.get.return_value = b"not valid json"
        
        result = redis_client_edge.get("key1")
        
        # Should return the string as-is if JSON decode fails
        assert result == "not valid json"
    
    @pytest.mark.asyncio
    async def test_close_with_error(self, redis_client_edge):
        """Test closing connections when an error occurs."""
        redis_client_edge._async_client.close.side_effect = Exception("Close failed")
        
        # Should not raise exception
        await redis_client_edge.close()
    
    def test_retry_sync_with_increasing_delay(self, redis_client_edge):
        """Test that retry delay increases with attempts."""
        operation = Mock(side_effect=[ConnectionError("Failed"), ConnectionError("Failed"), "success"])
        
        sleep_calls = []
        with patch('time.sleep', side_effect=lambda x: sleep_calls.append(x)):
            result = redis_client_edge._retry_sync_operation(operation)
        
        assert result == "success"
        assert len(sleep_calls) == 2
        assert sleep_calls[0] == 1.0  # retry_delay * 1
        assert sleep_calls[1] == 2.0  # retry_delay * 2
    
    def test_dedupe_unknown_strategy_fallback(self):
        """Test deduplication with unknown strategy falls back to SHA256."""
        mock_redis = Mock(spec=EnhancedRedisClient)
        
        manager = DeduplicationManager(
            redis_client=mock_redis,
            strategy='unknown_strategy'
        )
        
        hash_value = manager._compute_hash("test content")
        
        # Should fall back to SHA256
        assert len(hash_value) == 64
    
    def test_rate_limiter_string_count_conversion(self):
        """Test rate limiter handles string count values."""
        mock_redis = Mock(spec=EnhancedRedisClient)
        mock_redis.get.return_value = "5"  # String instead of int
        mock_redis._sync_client = Mock()
        
        limiter = RateLimiter(redis_client=mock_redis)
        
        is_allowed, info = limiter.is_allowed("user123")
        
        assert is_allowed is True
        assert info['current_count'] == 6


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""
    
    @pytest.mark.asyncio
    async def test_concurrent_rate_limiting(self):
        """Test rate limiting with concurrent requests."""
        mock_redis = Mock(spec=EnhancedRedisClient)
        mock_redis._async_client = AsyncMock()
        
        # Create a proper async pipeline mock that supports chaining
        mock_pipeline = AsyncMock()
        mock_pipeline.zremrangebyscore = Mock(return_value=mock_pipeline)
        mock_pipeline.zcard = Mock(return_value=mock_pipeline)
        mock_pipeline.zadd = Mock(return_value=mock_pipeline)
        mock_pipeline.expire = Mock(return_value=mock_pipeline)
        mock_pipeline.execute = AsyncMock(return_value=[None, 8, None, None])
        mock_pipeline.zrem = Mock(return_value=mock_pipeline)
        
        # Mock the pipeline method to return our pipeline mock
        mock_redis._async_client.pipeline = Mock(return_value=mock_pipeline)
        mock_redis._async_client.zrem = AsyncMock()
        
        limiter = RateLimiter(
            redis_client=mock_redis,
            algorithm='sliding_window',
            default_limit=10
        )
        
        # Simulate concurrent requests
        tasks = []
        for i in range(5):
            task = limiter.is_allowed_async(f"user{i % 2}")
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        # All should be allowed since we're under the limit
        for is_allowed, info in results:
            assert is_allowed is True
    
    def test_deduplication_with_ttl_expiry(self):
        """Test deduplication behavior with TTL."""
        mock_redis = Mock(spec=EnhancedRedisClient)
        mock_redis.exists.side_effect = [False, True]  # First check: not exists, second: exists
        mock_redis.set.return_value = True
        
        manager = DeduplicationManager(
            redis_client=mock_redis,
            ttl=60  # 1 minute TTL
        )
        
        content = "test content"
        
        # First check - should not be duplicate
        assert manager.is_duplicate(content) is False
        
        # Add to dedupe store
        manager.add(content)
        
        # Second check - should be duplicate
        assert manager.is_duplicate(content) is True
    
    @pytest.mark.asyncio
    async def test_distributed_lock_contention(self):
        """Test distributed lock behavior under contention."""
        mock_redis = Mock(spec=EnhancedRedisClient)
        mock_redis._async_client = AsyncMock()
        mock_redis._namespaced_key = lambda key: f"test:{key}"
        
        # First lock succeeds, second fails
        mock_redis._async_client.set.side_effect = [True, False]
        
        lock1 = DistributedLock(redis_client=mock_redis, lock_name="shared_resource")
        lock2 = DistributedLock(redis_client=mock_redis, lock_name="shared_resource")
        
        # First lock acquisition succeeds
        assert await lock1.acquire_async() is True
        
        # Second lock acquisition fails (resource is locked)
        assert await lock2.acquire_async() is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])