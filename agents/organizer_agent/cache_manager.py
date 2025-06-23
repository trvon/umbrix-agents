"""
CacheManager: Intelligent caching system for crawled content.

Manages Redis-based caching with TTL, compression, and cache analytics.
"""

import asyncio
import gzip
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urlparse

import redis.asyncio as aioredis
from pydantic import BaseModel


class CacheEntry(BaseModel):
    """Cache entry metadata."""
    
    url: str
    content_hash: str
    content_length: int
    content_type: str
    cached_at: datetime
    ttl: int
    access_count: int = 0
    last_accessed: Optional[datetime] = None
    compressed: bool = False


class CacheStats(BaseModel):
    """Cache statistics."""
    
    total_entries: int = 0
    total_size_bytes: int = 0
    hit_count: int = 0
    miss_count: int = 0
    eviction_count: int = 0
    compression_ratio: float = 0.0
    avg_access_time: float = 0.0


class CacheManager:
    """
    Intelligent cache manager for crawled content.
    
    Features:
    - Redis-based storage with TTL
    - Optional compression for large content
    - Cache analytics and metrics
    - Smart eviction policies
    - Content deduplication
    """
    
    def __init__(self, redis_client: aioredis.Redis, ttl_seconds: int = 3600):
        """Initialize the CacheManager."""
        self.redis = redis_client
        self.default_ttl = ttl_seconds
        self.logger = logging.getLogger(__name__)
        
        # Cache configuration
        self.compression_threshold = 1024  # Compress content > 1KB
        self.max_content_size = 1024 * 1024 * 10  # 10MB max
        
        # Statistics
        self.stats = CacheStats()
        
        # Key prefixes
        self.content_prefix = "crawl_cache:content:"
        self.metadata_prefix = "crawl_cache:meta:"
        self.stats_key = "crawl_cache:stats"
    
    async def get(self, url: str) -> Optional[str]:
        """
        Get cached content for a URL.
        
        Returns:
            Cached content string or None if not found
        """
        start_time = time.time()
        
        try:
            content_key = self._get_content_key(url)
            metadata_key = self._get_metadata_key(url)
            
            # Get metadata first
            metadata_data = await self.redis.get(metadata_key)
            if not metadata_data:
                self.stats.miss_count += 1
                return None
            
            metadata = CacheEntry.model_validate_json(metadata_data)
            
            # Get content
            content_data = await self.redis.get(content_key)
            if not content_data:
                # Metadata exists but content missing - clean up
                await self.redis.delete(metadata_key)
                self.stats.miss_count += 1
                return None
            
            # Decompress if needed
            if metadata.compressed:
                content = gzip.decompress(content_data).decode('utf-8')
            else:
                content = content_data.decode('utf-8')
            
            # Update access statistics
            metadata.access_count += 1
            metadata.last_accessed = datetime.now(timezone.utc)
            await self.redis.set(
                metadata_key,
                metadata.model_dump_json(),
                ex=metadata.ttl
            )
            
            # Update global stats
            self.stats.hit_count += 1
            access_time = time.time() - start_time
            self._update_avg_access_time(access_time)
            
            self.logger.debug(f"Cache hit for {url} (size: {len(content)} bytes)")
            return content
            
        except Exception as e:
            self.logger.error(f"Error getting cached content for {url}: {e}")
            self.stats.miss_count += 1
            return None
    
    async def set(self, url: str, content: str, ttl: Optional[int] = None, 
                  content_type: str = "text/html") -> bool:
        """
        Cache content for a URL.
        
        Args:
            url: URL to cache content for
            content: Content to cache
            ttl: Time to live in seconds (defaults to default_ttl)
            content_type: MIME type of content
            
        Returns:
            True if successfully cached, False otherwise
        """
        try:
            if len(content) > self.max_content_size:
                self.logger.warning(f"Content too large to cache: {len(content)} bytes for {url}")
                return False
            
            content_key = self._get_content_key(url)
            metadata_key = self._get_metadata_key(url)
            cache_ttl = ttl or self.default_ttl
            
            # Determine if we should compress
            should_compress = len(content) > self.compression_threshold
            
            if should_compress:
                content_data = gzip.compress(content.encode('utf-8'))
                compression_ratio = len(content_data) / len(content.encode('utf-8'))
            else:
                content_data = content.encode('utf-8')
                compression_ratio = 1.0
            
            # Create metadata
            metadata = CacheEntry(
                url=url,
                content_hash=self._hash_content(content),
                content_length=len(content),
                content_type=content_type,
                cached_at=datetime.now(timezone.utc),
                ttl=cache_ttl,
                compressed=should_compress
            )
            
            # Store content and metadata
            pipeline = self.redis.pipeline()
            pipeline.set(content_key, content_data, ex=cache_ttl)
            pipeline.set(metadata_key, metadata.model_dump_json(), ex=cache_ttl)
            await pipeline.execute()
            
            # Update statistics
            self.stats.total_entries += 1
            self.stats.total_size_bytes += len(content_data)
            if should_compress:
                current_ratio = self.stats.compression_ratio
                total_entries = max(self.stats.total_entries, 1)
                self.stats.compression_ratio = (
                    (current_ratio * (total_entries - 1) + compression_ratio) / total_entries
                )
            
            self.logger.debug(
                f"Cached content for {url} "
                f"(size: {len(content)} -> {len(content_data)} bytes, "
                f"compressed: {should_compress})"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error caching content for {url}: {e}")
            return False
    
    async def delete(self, url: str) -> bool:
        """
        Delete cached content for a URL.
        
        Returns:
            True if successfully deleted, False otherwise
        """
        try:
            content_key = self._get_content_key(url)
            metadata_key = self._get_metadata_key(url)
            
            # Get metadata for size calculation before deletion
            metadata_data = await self.redis.get(metadata_key)
            
            pipeline = self.redis.pipeline()
            pipeline.delete(content_key)
            pipeline.delete(metadata_key)
            results = await pipeline.execute()
            
            deleted = any(results)
            
            if deleted and metadata_data:
                try:
                    metadata = CacheEntry.model_validate_json(metadata_data)
                    self.stats.total_entries = max(0, self.stats.total_entries - 1)
                    self.stats.eviction_count += 1
                except Exception:
                    pass
            
            return deleted
            
        except Exception as e:
            self.logger.error(f"Error deleting cached content for {url}: {e}")
            return False
    
    async def exists(self, url: str) -> bool:
        """Check if content is cached for a URL."""
        try:
            metadata_key = self._get_metadata_key(url)
            return await self.redis.exists(metadata_key) > 0
        except Exception as e:
            self.logger.error(f"Error checking cache existence for {url}: {e}")
            return False
    
    async def get_metadata(self, url: str) -> Optional[CacheEntry]:
        """Get cache metadata for a URL."""
        try:
            metadata_key = self._get_metadata_key(url)
            metadata_data = await self.redis.get(metadata_key)
            
            if metadata_data:
                return CacheEntry.model_validate_json(metadata_data)
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting cache metadata for {url}: {e}")
            return None
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get current cache statistics."""
        try:
            # Update real-time stats from Redis
            total_keys = 0
            total_memory = 0
            
            # Scan for cache keys to get accurate counts
            async for key in self.redis.scan_iter(match=f"{self.content_prefix}*"):
                total_keys += 1
                try:
                    memory = await self.redis.memory_usage(key)
                    if memory:
                        total_memory += memory
                except Exception:
                    pass
            
            # Update stats
            self.stats.total_entries = total_keys
            self.stats.total_size_bytes = total_memory
            
            # Calculate hit rate
            total_requests = self.stats.hit_count + self.stats.miss_count
            hit_rate = self.stats.hit_count / max(total_requests, 1)
            
            return {
                "stats": self.stats.model_dump(),
                "hit_rate": hit_rate,
                "total_requests": total_requests,
                "avg_entry_size": total_memory / max(total_keys, 1)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting cache stats: {e}")
            return {"error": str(e)}
    
    async def cleanup_expired(self) -> int:
        """
        Clean up expired cache entries.
        
        Returns:
            Number of entries cleaned up
        """
        cleaned = 0
        
        try:
            # Scan for metadata keys
            async for key in self.redis.scan_iter(match=f"{self.metadata_prefix}*"):
                try:
                    ttl = await self.redis.ttl(key)
                    if ttl == -2:  # Key doesn't exist
                        continue
                    elif ttl == -1:  # Key exists but no TTL
                        # Clean up key without TTL
                        await self.redis.delete(key)
                        cleaned += 1
                    
                except Exception as e:
                    self.logger.warning(f"Error checking TTL for {key}: {e}")
            
            if cleaned > 0:
                self.logger.info(f"Cleaned up {cleaned} expired cache entries")
            
        except Exception as e:
            self.logger.error(f"Error during cache cleanup: {e}")
        
        return cleaned
    
    async def clear_all(self) -> int:
        """
        Clear all cached content.
        
        Returns:
            Number of entries cleared
        """
        cleared = 0
        
        try:
            # Delete all content keys
            async for key in self.redis.scan_iter(match=f"{self.content_prefix}*"):
                await self.redis.delete(key)
                cleared += 1
            
            # Delete all metadata keys
            async for key in self.redis.scan_iter(match=f"{self.metadata_prefix}*"):
                await self.redis.delete(key)
            
            # Reset stats
            self.stats = CacheStats()
            
            self.logger.info(f"Cleared {cleared} cache entries")
            
        except Exception as e:
            self.logger.error(f"Error clearing cache: {e}")
        
        return cleared
    
    def _get_content_key(self, url: str) -> str:
        """Get Redis key for content."""
        url_hash = self._hash_content(url)
        return f"{self.content_prefix}{url_hash}"
    
    def _get_metadata_key(self, url: str) -> str:
        """Get Redis key for metadata."""
        url_hash = self._hash_content(url)
        return f"{self.metadata_prefix}{url_hash}"
    
    def _hash_content(self, content: str) -> str:
        """Create hash for content or URL."""
        import hashlib
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
    
    def _update_avg_access_time(self, access_time: float) -> None:
        """Update average access time with new measurement."""
        total_requests = self.stats.hit_count + self.stats.miss_count
        if total_requests <= 1:
            self.stats.avg_access_time = access_time
        else:
            # Rolling average
            self.stats.avg_access_time = (
                (self.stats.avg_access_time * (total_requests - 1) + access_time) / total_requests
            )