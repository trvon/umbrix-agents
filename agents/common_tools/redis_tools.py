"""
Redis utilities for Umbrix agents.

This module provides enhanced Redis functionality including deduplication,
caching, rate limiting, and distributed locking capabilities commonly
used across agents.
"""

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Set, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
import asyncio

import redis
import redis.asyncio as aioredis
from redis.exceptions import RedisError, ConnectionError

from .logging import setup_logging


@dataclass
class CacheEntry:
    """Represents a cached item with metadata."""
    key: str
    value: Any
    timestamp: datetime
    ttl: Optional[int] = None
    hit_count: int = 0


class EnhancedRedisClient:
    """
    Enhanced Redis client with additional utilities for agent development.
    
    Provides async/sync operations, connection pooling, retry logic,
    and higher-level abstractions for common patterns.
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        namespace: str = 'umbrix',
        max_connections: int = 10,
        retry_attempts: int = 3,
        retry_delay: float = 1.0,
        **kwargs
    ):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.namespace = namespace
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        
        self.logger = setup_logging(f"redis_{namespace}")
        
        # Create connection pools
        self._sync_pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
            max_connections=max_connections,
            retry_on_error=[ConnectionError],
            **kwargs
        )
        
        self._async_pool = aioredis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
            max_connections=max_connections,
            **kwargs
        )
        
        self._sync_client = redis.Redis(connection_pool=self._sync_pool)
        self._async_client = aioredis.Redis(connection_pool=self._async_pool)
    
    def _namespaced_key(self, key: str) -> str:
        """Add namespace prefix to key."""
        return f"{self.namespace}:{key}"
    
    async def _retry_async_operation(self, operation, *args, **kwargs):
        """Execute async operation with retry logic."""
        last_error = None
        
        for attempt in range(self.retry_attempts):
            try:
                return await operation(*args, **kwargs)
            except RedisError as e:
                last_error = e
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                    self.logger.warning(f"Redis operation failed, retrying (attempt {attempt + 1}): {e}")
                else:
                    self.logger.error(f"Redis operation failed after {self.retry_attempts} attempts: {e}")
        
        if last_error is not None:
            raise last_error
    def _retry_sync_operation(self, operation, *args, **kwargs):
        """Execute sync operation with retry logic."""
        last_error = None
        
        for attempt in range(self.retry_attempts):
            try:
                return operation(*args, **kwargs)
            except RedisError as e:
                last_error = e
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    self.logger.warning(f"Redis operation failed, retrying (attempt {attempt + 1}): {e}")
                else:
                    self.logger.error(f"Redis operation failed after {self.retry_attempts} attempts: {e}")
        
        if last_error is not None:
            raise last_error
        else:
            raise RuntimeError("Retry operation failed without capturing an exception")
            self.logger.error(f"Redis operation failed after {self.retry_attempts} attempts: {e}")
        
        raise last_error
    
    # Async methods
    async def aset(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a key-value pair asynchronously."""
        namespaced_key = self._namespaced_key(key)
        serialized_value = json.dumps(value) if not isinstance(value, (str, bytes)) else value
        
        async def _set_operation():
            if ttl:
                return await self._async_client.setex(namespaced_key, ttl, serialized_value)
            else:
                return await self._async_client.set(namespaced_key, serialized_value)
        
        result = await self._retry_async_operation(_set_operation)
        return bool(result)
    
    async def aget(self, key: str, default: Any = None) -> Any:
        """Get a value by key asynchronously."""
        namespaced_key = self._namespaced_key(key)
        
        async def _get_operation():
            return await self._async_client.get(namespaced_key)
        
        try:
            value = await self._retry_async_operation(_get_operation)
            if value is None:
                return default
            
            # Try to deserialize JSON, fall back to string
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value.decode('utf-8') if isinstance(value, bytes) else value
                
        except RedisError:
            return default
    
    async def adelete(self, key: str) -> bool:
        """Delete a key asynchronously."""
        namespaced_key = self._namespaced_key(key)
        
        async def _delete_operation():
            return await self._async_client.delete(namespaced_key)
        
        result = await self._retry_async_operation(_delete_operation)
        return bool(result)
    
    async def aexists(self, key: str) -> bool:
        """Check if key exists asynchronously."""
        namespaced_key = self._namespaced_key(key)
        
        async def _exists_operation():
            return await self._async_client.exists(namespaced_key)
        
        result = await self._retry_async_operation(_exists_operation)
        return bool(result)
    
    # Sync methods
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a key-value pair synchronously."""
        namespaced_key = self._namespaced_key(key)
        serialized_value = json.dumps(value) if not isinstance(value, (str, bytes)) else value
        
        def _set_operation():
            if ttl:
                return self._sync_client.setex(namespaced_key, ttl, serialized_value)
            else:
                return self._sync_client.set(namespaced_key, serialized_value)
        
        result = self._retry_sync_operation(_set_operation)
        return bool(result)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a value by key synchronously."""
        namespaced_key = self._namespaced_key(key)
        
        def _get_operation():
            return self._sync_client.get(namespaced_key)
        
        try:
            value = self._retry_sync_operation(_get_operation)
            if value is None:
                return default
            
            # Try to deserialize JSON, fall back to string
            try:
                if isinstance(value, (str, bytes, bytearray)):
                    return json.loads(value)
                else:
                    return value
            except (json.JSONDecodeError, TypeError):
                return value.decode('utf-8') if isinstance(value, bytes) else value
                
        except RedisError:
            return default
    
    def delete(self, key: str) -> bool:
        """Delete a key synchronously."""
        namespaced_key = self._namespaced_key(key)
        
        def _delete_operation():
            return self._sync_client.delete(namespaced_key)
        
        result = self._retry_sync_operation(_delete_operation)
        return bool(result)
    
    def exists(self, key: str) -> bool:
        """Check if key exists synchronously."""
        namespaced_key = self._namespaced_key(key)
        
        def _exists_operation():
            return self._sync_client.exists(namespaced_key)
        
        result = self._retry_sync_operation(_exists_operation)
        return bool(result)
    
    async def close(self):
        """Close Redis connections."""
        try:
            await self._async_client.close()
            self._sync_client.close()
        except Exception as e:
            self.logger.error(f"Error closing Redis connections: {e}")


class DeduplicationManager:
    """
    Advanced deduplication manager with multiple strategies.
    
    Supports content-based, ID-based, and time-window deduplication
    with configurable storage backends and cleanup policies.
    """
    
    def __init__(
        self,
        redis_client: EnhancedRedisClient,
        strategy: str = 'content_hash',
        ttl: int = 86400,  # 24 hours
        max_entries: Optional[int] = None
    ):
        self.redis = redis_client
        self.strategy = strategy
        self.ttl = ttl
        self.max_entries = max_entries
        self.logger = setup_logging(f"dedupe_{strategy}")
        
        # Strategy-specific configurations
        self.hash_algorithms = {
            'content_hash': hashlib.sha256,
            'fast_hash': hashlib.md5,
            'secure_hash': hashlib.sha512
        }
    
    def _compute_hash(self, content: Union[str, Dict[str, Any]]) -> str:
        """Compute hash based on configured strategy."""
        if isinstance(content, dict):
            content = json.dumps(content, sort_keys=True)
        elif not isinstance(content, str):
            content = str(content)
        
        if self.strategy in self.hash_algorithms:
            hash_func = self.hash_algorithms[self.strategy]
            return hash_func(content.encode('utf-8')).hexdigest()
        else:
            # Fallback to SHA256
            return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    async def is_duplicate_async(self, content: Union[str, Dict[str, Any]], identifier: Optional[str] = None) -> bool:
        """Check if content is a duplicate asynchronously."""
        if identifier:
            key = f"dedupe:id:{identifier}"
        else:
            content_hash = self._compute_hash(content)
            key = f"dedupe:hash:{content_hash}"
        
        exists = await self.redis.aexists(key)
        
        if exists:
            # Update last seen timestamp
            await self.redis.aset(f"{key}:last_seen", int(time.time()), ttl=self.ttl)
            self.logger.debug(f"Duplicate detected: {key}")
        
        return exists
    
    def is_duplicate(self, content: Union[str, Dict[str, Any]], identifier: Optional[str] = None) -> bool:
        """Check if content is a duplicate synchronously."""
        if identifier:
            key = f"dedupe:id:{identifier}"
        else:
            content_hash = self._compute_hash(content)
            key = f"dedupe:hash:{content_hash}"
        
        exists = self.redis.exists(key)
        
        if exists:
            # Update last seen timestamp
            self.redis.set(f"{key}:last_seen", int(time.time()), ttl=self.ttl)
            self.logger.debug(f"Duplicate detected: {key}")
        
        return exists
    
    async def add_async(self, content: Union[str, Dict[str, Any]], identifier: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None):
        """Add content to deduplication store asynchronously."""
        if identifier:
            key = f"dedupe:id:{identifier}"
        else:
            content_hash = self._compute_hash(content)
            key = f"dedupe:hash:{content_hash}"
        
        entry_data = {
            'timestamp': int(time.time()),
            'strategy': self.strategy,
            'metadata': metadata or {}
        }
        
        # Store the deduplication entry
        await self.redis.aset(key, entry_data, ttl=self.ttl)
        await self.redis.aset(f"{key}:last_seen", int(time.time()), ttl=self.ttl)
        
        # Enforce max entries limit if configured
        if self.max_entries:
            await self._cleanup_old_entries()
        
        self.logger.debug(f"Added to deduplication store: {key}")
    
    def add(self, content: Union[str, Dict[str, Any]], identifier: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None):
        """Add content to deduplication store synchronously."""
        if identifier:
            key = f"dedupe:id:{identifier}"
        else:
            content_hash = self._compute_hash(content)
            key = f"dedupe:hash:{content_hash}"
        
        entry_data = {
            'timestamp': int(time.time()),
            'strategy': self.strategy,
            'metadata': metadata or {}
        }
        
        # Store the deduplication entry
        self.redis.set(key, entry_data, ttl=self.ttl)
        self.redis.set(f"{key}:last_seen", int(time.time()), ttl=self.ttl)
        
        self.logger.debug(f"Added to deduplication store: {key}")
    
    async def _cleanup_old_entries(self):
        """Clean up old entries to maintain max_entries limit."""
        # This would require scanning keys and removing oldest entries
        # Implementation depends on Redis scanning capabilities
        pass
    
    async def get_stats_async(self) -> Dict[str, Any]:
        """Get deduplication statistics asynchronously."""
        # Count total entries with our namespace
        # This is a simplified implementation
        return {
            'strategy': self.strategy,
            'ttl': self.ttl,
            'max_entries': self.max_entries,
            'timestamp': int(time.time())
        }


class RateLimiter:
    """
    Redis-based distributed rate limiter.
    
    Implements sliding window and token bucket algorithms
    for rate limiting across multiple agent instances.
    """
    
    def __init__(
        self,
        redis_client: EnhancedRedisClient,
        algorithm: str = 'sliding_window',
        default_limit: int = 100,
        default_window: int = 60
    ):
        self.redis = redis_client
        self.algorithm = algorithm
        self.default_limit = default_limit
        self.default_window = default_window
        self.logger = setup_logging(f"rate_limiter_{algorithm}")
    
    async def is_allowed_async(
        self,
        identifier: str,
        limit: Optional[int] = None,
        window: Optional[int] = None
    ) -> tuple[bool, Dict[str, Any]]:
        """
        Check if request is allowed under rate limit asynchronously.
        
        Args:
            identifier: Unique identifier for the rate limit (e.g., user_id, api_key)
            limit: Request limit (uses default if not provided)
            window: Time window in seconds (uses default if not provided)
            
        Returns:
            Tuple of (is_allowed, info_dict)
        """
        limit = limit or self.default_limit
        window = window or self.default_window
        
        if self.algorithm == 'sliding_window':
            return await self._sliding_window_check_async(identifier, limit, window)
        elif self.algorithm == 'token_bucket':
            return await self._token_bucket_check_async(identifier, limit, window)
        else:
            raise ValueError(f"Unknown rate limiting algorithm: {self.algorithm}")
    
    async def _sliding_window_check_async(
        self,
        identifier: str,
        limit: int,
        window: int
    ) -> tuple[bool, Dict[str, Any]]:
        """Sliding window rate limit check."""
        key = f"rate_limit:sliding:{identifier}"
        now = int(time.time())
        window_start = now - window
        
        # Use Redis pipeline for atomic operations
        pipe = self.redis._async_client.pipeline()
        
        # Remove expired entries
        pipe.zremrangebyscore(key, 0, window_start)
        
        # Count current requests in window
        pipe.zcard(key)
        
        # Add current request
        pipe.zadd(key, {str(now): now})
        
        # Set TTL
        pipe.expire(key, window)
        
        results = await pipe.execute()
        current_count = results[1] + 1  # +1 for the request we just added
        
        is_allowed = current_count <= limit
        
        if not is_allowed:
            # Remove the request we just added since it's not allowed
            await self.redis._async_client.zrem(key, str(now))
        
        return is_allowed, {
            'current_count': current_count,
            'limit': limit,
            'window': window,
            'reset_time': now + window,
            'algorithm': 'sliding_window'
        }
    
    async def _token_bucket_check_async(
        self,
        identifier: str,
        limit: int,
        window: int
    ) -> tuple[bool, Dict[str, Any]]:
        """Token bucket rate limit check."""
        key = f"rate_limit:bucket:{identifier}"
        now = time.time()
        
        # Get current bucket state
        bucket_data = await self.redis.aget(key, {
            'tokens': limit,
            'last_refill': now
        })
        
        if isinstance(bucket_data, str):
            bucket_data = json.loads(bucket_data)
        
        # Calculate tokens to add based on time elapsed
        time_elapsed = now - bucket_data['last_refill']
        tokens_to_add = (time_elapsed / window) * limit
        
        # Update token count (capped at limit)
        current_tokens = min(limit, bucket_data['tokens'] + tokens_to_add)
        
        # Check if request is allowed
        is_allowed = current_tokens >= 1
        
        if is_allowed:
            current_tokens -= 1
        
        # Update bucket state
        new_bucket_data = {
            'tokens': current_tokens,
            'last_refill': now
        }
        
        await self.redis.aset(key, new_bucket_data, ttl=window * 2)
        
        return is_allowed, {
            'current_tokens': current_tokens,
            'limit': limit,
            'refill_rate': limit / window,
            'algorithm': 'token_bucket'
        }
    
    def is_allowed(
        self,
        identifier: str,
        limit: Optional[int] = None,
        window: Optional[int] = None
    ) -> tuple[bool, Dict[str, Any]]:
        """Check if request is allowed under rate limit synchronously."""
        # Synchronous version of rate limiting
        # For simplicity, we'll implement a basic counter approach
        limit = limit or self.default_limit
        window = window or self.default_window
        
        key = f"rate_limit:counter:{identifier}"
        
        # Get current count
        current_count = self.redis.get(key, 0)
        
        if isinstance(current_count, str):
            current_count = int(current_count)
        
        is_allowed = current_count < limit
        
        if is_allowed:
            # Increment counter
            if current_count == 0:
                # First request in window, set with TTL
                self.redis.set(key, 1, ttl=window)
            else:
                # Increment existing counter
                self.redis._sync_client.incr(key)
        
        return is_allowed, {
            'current_count': current_count + (1 if is_allowed else 0),
            'limit': limit,
            'window': window,
            'algorithm': 'counter'
        }


class DistributedLock:
    """
    Redis-based distributed lock implementation.
    
    Provides mutual exclusion across multiple agent instances
    with automatic expiration and renewal capabilities.
    """
    
    def __init__(
        self,
        redis_client: EnhancedRedisClient,
        lock_name: str,
        timeout: int = 30,
        auto_renewal: bool = False
    ):
        self.redis = redis_client
        self.lock_name = lock_name
        self.timeout = timeout
        self.auto_renewal = auto_renewal
        self.lock_key = f"lock:{lock_name}"
        self.lock_value = f"{time.time()}:{id(self)}"
        self.logger = setup_logging(f"lock_{lock_name}")
        
        self._renewal_task = None
        self._acquired = False
    
    async def acquire_async(self, timeout: Optional[int] = None) -> bool:
        """Acquire the lock asynchronously."""
        timeout = timeout or self.timeout
        
        # Try to set the lock with NX (only if not exists) and EX (expiration)
        success = await self.redis._async_client.set(
            self.redis._namespaced_key(self.lock_key),
            self.lock_value,
            nx=True,
            ex=timeout
        )
        
        self._acquired = bool(success)
        
        if self._acquired:
            self.logger.debug(f"Acquired lock: {self.lock_name}")
            
            if self.auto_renewal:
                self._start_renewal_task()
        else:
            self.logger.debug(f"Failed to acquire lock: {self.lock_name}")
        
        return self._acquired
    
    def acquire(self, timeout: Optional[int] = None) -> bool:
        """Acquire the lock synchronously."""
        timeout = timeout or self.timeout
        
        # Try to set the lock with NX (only if not exists) and EX (expiration)
        success = self.redis._sync_client.set(
            self.redis._namespaced_key(self.lock_key),
            self.lock_value,
            nx=True,
            ex=timeout
        )
        
        self._acquired = bool(success)
        
        if self._acquired:
            self.logger.debug(f"Acquired lock: {self.lock_name}")
        else:
            self.logger.debug(f"Failed to acquire lock: {self.lock_name}")
        
        return self._acquired
    
    async def release_async(self) -> bool:
        """Release the lock asynchronously."""
        if not self._acquired:
            return False
        
        # Use Lua script to ensure we only delete our own lock
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        
        result = await self.redis._async_client.execute_command(
            'EVAL',
            lua_script,
            1,
            self.redis._namespaced_key(self.lock_key),
            self.lock_value
        )
        
        success = bool(result)
        
        if success:
            self._acquired = False
            self.logger.debug(f"Released lock: {self.lock_name}")
            
            if self._renewal_task:
                self._renewal_task.cancel()
                self._renewal_task = None
        
        return success
    
    def release(self) -> bool:
        """Release the lock synchronously."""
        if not self._acquired:
            return False
        
        # Use Lua script to ensure we only delete our own lock
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        
        result = self.redis._sync_client.eval(
            lua_script,
            1,
            self.redis._namespaced_key(self.lock_key),
            self.lock_value
        )
        
        success = bool(result)
        
        if success:
            self._acquired = False
            self.logger.debug(f"Released lock: {self.lock_name}")
        
        return success
    
    def _start_renewal_task(self):
        """Start automatic lock renewal task."""
        async def renewal_worker():
            while self._acquired:
                try:
                    await asyncio.sleep(self.timeout * 0.7)  # Renew at 70% of timeout
                    
                    if self._acquired:
                        # Extend lock expiration
                        await self.redis._async_client.expire(
                            self.redis._namespaced_key(self.lock_key),
                            self.timeout
                        )
                        self.logger.debug(f"Renewed lock: {self.lock_name}")
                
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(f"Lock renewal failed for {self.lock_name}: {e}")
        
        self._renewal_task = asyncio.create_task(renewal_worker())
    
    async def __aenter__(self):
        """Async context manager entry."""
        success = await self.acquire_async()
        if not success:
            raise RuntimeError(f"Failed to acquire lock: {self.lock_name}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.release_async()
    
    def __enter__(self):
        """Sync context manager entry."""
        success = self.acquire()
        if not success:
            raise RuntimeError(f"Failed to acquire lock: {self.lock_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Sync context manager exit."""
        self.release()