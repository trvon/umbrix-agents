"""
Deduplication store implementations for Umbrix agents.

This module provides various deduplication storage backends including
Redis-based deduplication with TTL support and in-memory stores for testing.
"""

import redis
import time
import logging
from typing import Optional, Union
from urllib.parse import urlparse


class DedupeStore:
    """Base class for deduplication storage."""
    
    def set_if_not_exists(self, key: str, ttl_seconds: int) -> bool:
        """
        Set a key if it doesn't exist.
        
        Args:
            key: The key to set
            ttl_seconds: Time to live in seconds
            
        Returns:
            True if the key was set (not a duplicate), False if it already existed
        """
        raise NotImplementedError("Subclasses must implement set_if_not_exists")
    
    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        raise NotImplementedError("Subclasses must implement exists")
    
    def close(self):
        """Close the connection."""
        pass
    
    def add(self, key: str, ttl_seconds: int = 86400) -> bool:
        """
        Add a key to deduplication store with default TTL.
        Alias for set_if_not_exists.
        """
        return self.set_if_not_exists(key, ttl_seconds)


class RedisDedupeStore(DedupeStore):
    """Redis-based deduplication store with TTL support."""
    
    def __init__(self, redis_url: str, namespace: str = "dedupe"):
        """
        Initialize Redis deduplication store.
        
        Args:
            redis_url: Redis connection URL
            namespace: Key namespace prefix
        """
        self.namespace = namespace
        self.logger = logging.getLogger(__name__)
        
        # Parse Redis URL
        parsed = urlparse(redis_url)
        
        # Create Redis connection
        try:
            self.redis_client = redis.Redis(
                host=parsed.hostname or 'localhost',
                port=parsed.port or 6379,
                db=int(parsed.path.lstrip('/')) if parsed.path else 0,
                password=parsed.password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            
            # Test connection
            self.redis_client.ping()
            self.logger.info(f"Connected to Redis at {parsed.hostname}:{parsed.port}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            # Fallback to in-memory store
            self.redis_client = None
            self._memory_store = {}
            self._expiry_times = {}
    
    def _namespaced_key(self, key: str) -> str:
        """Add namespace prefix to key."""
        return f"{self.namespace}:{key}"
    
    def _cleanup_expired_memory_keys(self):
        """Clean up expired keys from memory store."""
        if not hasattr(self, '_memory_store'):
            return
            
        current_time = time.time()
        expired_keys = [
            key for key, expiry in self._expiry_times.items() 
            if expiry <= current_time
        ]
        
        for key in expired_keys:
            self._memory_store.pop(key, None)
            self._expiry_times.pop(key, None)
    
    def set_if_not_exists(self, key: str, ttl_seconds: int) -> bool:
        """
        Set a key if it doesn't exist.
        
        Args:
            key: The key to set
            ttl_seconds: Time to live in seconds
            
        Returns:
            True if the key was set (not a duplicate), False if it already existed
        """
        namespaced_key = self._namespaced_key(key)
        
        if self.redis_client:
            try:
                # Use Redis SET with NX (only if not exists) and EX (expiration)
                result = self.redis_client.set(
                    namespaced_key, 
                    int(time.time()), 
                    nx=True, 
                    ex=ttl_seconds
                )
                return bool(result)
                
            except Exception as e:
                self.logger.warning(f"Redis operation failed, falling back to memory: {e}")
                # Initialize memory store if not already done
                if not hasattr(self, '_memory_store'):
                    self._memory_store = {}
                    self._expiry_times = {}
                # Fall through to memory store
        
        # Fallback to in-memory store (only if we have memory store attributes)
        if hasattr(self, '_memory_store'):
            self._cleanup_expired_memory_keys()
            
            if namespaced_key in self._memory_store:
                return False
            
            self._memory_store[namespaced_key] = int(time.time())
            self._expiry_times[namespaced_key] = time.time() + ttl_seconds
            return True
        
        # If no fallback available, return False (conservative approach)
        return False
    
    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        namespaced_key = self._namespaced_key(key)
        
        if self.redis_client:
            try:
                return bool(self.redis_client.exists(namespaced_key))
            except Exception as e:
                self.logger.warning(f"Redis operation failed: {e}")
                # Initialize memory store if not already done
                if not hasattr(self, '_memory_store'):
                    self._memory_store = {}
                    self._expiry_times = {}
        
        # Fallback to memory store (only if we have memory store attributes)
        if hasattr(self, '_memory_store'):
            self._cleanup_expired_memory_keys()
            return namespaced_key in self._memory_store
        
        # If no fallback available, return False (conservative approach)
        return False
    
    def close(self):
        """Close the Redis connection."""
        if self.redis_client:
            try:
                self.redis_client.close()
            except Exception as e:
                self.logger.warning(f"Error closing Redis connection: {e}")


class InMemoryDedupeStore(DedupeStore):
    """In-memory deduplication store for testing."""
    
    def __init__(self, namespace: str = "dedupe"):
        """Initialize in-memory deduplication store."""
        self.namespace = namespace
        self._store = {}
        self._expiry_times = {}
    
    def _namespaced_key(self, key: str) -> str:
        """Add namespace prefix to key."""
        return f"{self.namespace}:{key}"
    
    def _cleanup_expired_keys(self):
        """Clean up expired keys."""
        current_time = time.time()
        expired_keys = [
            key for key, expiry in self._expiry_times.items() 
            if expiry <= current_time
        ]
        
        for key in expired_keys:
            self._store.pop(key, None)
            self._expiry_times.pop(key, None)
    
    def set_if_not_exists(self, key: str, ttl_seconds: int) -> bool:
        """Set a key if it doesn't exist."""
        self._cleanup_expired_keys()
        
        namespaced_key = self._namespaced_key(key)
        
        if namespaced_key in self._store:
            return False
        
        self._store[namespaced_key] = int(time.time())
        self._expiry_times[namespaced_key] = time.time() + ttl_seconds
        return True
    
    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        self._cleanup_expired_keys()
        namespaced_key = self._namespaced_key(key)
        return namespaced_key in self._store
    
    def clear(self):
        """Clear all keys (useful for testing)."""
        self._store.clear()
        self._expiry_times.clear()