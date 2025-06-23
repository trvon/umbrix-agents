"""
Temporal Caching System with time-based priorities and decay functions.

Implements intelligent caching strategies based on content age, access patterns,
and threat intelligence relevance over time.
"""

import asyncio
import logging
import math
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

import redis.asyncio as aioredis
from pydantic import BaseModel


class CachePriority(Enum):
    """Cache priority levels."""
    CRITICAL = "critical"      # Recent threat intel, high-priority IOCs
    HIGH = "high"             # Recent articles, trending threats
    MEDIUM = "medium"         # General threat intel, moderate age
    LOW = "low"              # Old content, background information
    ARCHIVE = "archive"       # Very old content, rarely accessed


@dataclass
class TemporalWeight:
    """Temporal weighting factors for cache priority calculation."""
    recency_weight: float = 0.4      # How much recent content matters
    frequency_weight: float = 0.3    # How much access frequency matters
    relevance_weight: float = 0.2    # How much threat relevance matters
    size_weight: float = 0.1         # How much content size matters (negative factor)


class TemporalCacheEntry(BaseModel):
    """Cache entry with temporal metadata."""
    
    content_hash: str
    url: str
    content_type: str
    content_size: int
    
    # Temporal metadata
    created_at: datetime
    last_accessed: datetime
    access_count: int = 0
    
    # Content metadata
    threat_score: float = 0.0         # Threat intelligence relevance score
    content_freshness: float = 1.0    # How "fresh" the content is (0-1)
    update_frequency: float = 0.0     # How often content updates (times per day)
    
    # Cache metadata
    priority: CachePriority = CachePriority.MEDIUM
    ttl_seconds: int = 86400          # 24 hours default
    decay_rate: float = 0.1           # How fast priority decays over time
    
    # Computed fields
    current_priority_score: Optional[float] = None
    next_evaluation: Optional[datetime] = None


class TemporalCacheManager:
    """
    Temporal cache manager with time-based priority and decay.
    
    Features:
    - Time-based priority calculation
    - Automatic cache warming based on access patterns
    - Decay functions for aging content
    - Intelligent eviction based on temporal relevance
    - Threat intelligence aware caching
    """
    
    def __init__(self, redis_client: aioredis.Redis, 
                 max_cache_size: int = 10000,
                 weights: Optional[TemporalWeight] = None):
        """
        Initialize TemporalCacheManager.
        
        Args:
            redis_client: Redis client for storage
            max_cache_size: Maximum number of cached items
            weights: Temporal weighting configuration
        """
        self.redis = redis_client
        self.max_cache_size = max_cache_size
        self.weights = weights or TemporalWeight()
        self.logger = logging.getLogger(__name__)
        
        # Redis keys
        self.entries_key = "temporal_cache:entries"
        self.priority_queue_key = "temporal_cache:priority_queue"
        self.access_log_key = "temporal_cache:access_log"
        self.stats_key = "temporal_cache:stats"
        
        # Background task for cache maintenance
        self._maintenance_task: Optional[asyncio.Task] = None
    
    async def start_maintenance(self) -> None:
        """Start background maintenance tasks."""
        if self._maintenance_task is None or self._maintenance_task.done():
            self._maintenance_task = asyncio.create_task(self._maintenance_loop())
            self.logger.info("Started temporal cache maintenance")
    
    async def stop_maintenance(self) -> None:
        """Stop background maintenance tasks."""
        if self._maintenance_task and not self._maintenance_task.done():
            self._maintenance_task.cancel()
            try:
                await self._maintenance_task
            except asyncio.CancelledError:
                pass
            self.logger.info("Stopped temporal cache maintenance")
    
    async def _maintenance_loop(self) -> None:
        """Background maintenance loop."""
        while True:
            try:
                await self._update_priorities()
                await self._perform_evictions()
                await self._warm_cache()
                
                # Run maintenance every 5 minutes
                await asyncio.sleep(300)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in maintenance loop: {e}")
                await asyncio.sleep(60)  # Wait before retrying
    
    def _calculate_priority_score(self, entry: TemporalCacheEntry, 
                                 current_time: Optional[datetime] = None) -> float:
        """Calculate current priority score for cache entry."""
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        # Time since creation (hours)
        age_hours = (current_time - entry.created_at).total_seconds() / 3600
        
        # Time since last access (hours)
        access_age_hours = (current_time - entry.last_accessed).total_seconds() / 3600
        
        # Recency factor (decreases with age)
        recency_factor = math.exp(-entry.decay_rate * age_hours)
        
        # Frequency factor (based on access count and recency)
        if access_age_hours > 0:
            frequency_factor = entry.access_count / (1 + math.log(1 + access_age_hours))
        else:
            frequency_factor = entry.access_count
        
        # Relevance factor (threat score and content freshness)
        relevance_factor = entry.threat_score * entry.content_freshness
        
        # Size factor (smaller content gets slight preference)
        size_factor = 1.0 / (1.0 + entry.content_size / 1000000)  # Normalize to MB
        
        # Weighted combination
        priority_score = (
            self.weights.recency_weight * recency_factor +
            self.weights.frequency_weight * frequency_factor +
            self.weights.relevance_weight * relevance_factor +
            self.weights.size_weight * size_factor
        )
        
        return max(0.0, priority_score)
    
    def _calculate_ttl(self, entry: TemporalCacheEntry) -> int:
        """Calculate TTL based on content characteristics."""
        base_ttl = 86400  # 24 hours
        
        # High threat score content lives longer
        threat_multiplier = 1.0 + entry.threat_score
        
        # Frequently updated content has shorter TTL
        update_divisor = 1.0 + entry.update_frequency
        
        # Fresh content lives longer
        freshness_multiplier = 0.5 + entry.content_freshness
        
        ttl = int(base_ttl * threat_multiplier * freshness_multiplier / update_divisor)
        
        # Clamp TTL between 1 hour and 7 days
        return max(3600, min(ttl, 7 * 86400))
    
    def _determine_priority_level(self, priority_score: float) -> CachePriority:
        """Determine priority level from score."""
        if priority_score >= 0.8:
            return CachePriority.CRITICAL
        elif priority_score >= 0.6:
            return CachePriority.HIGH
        elif priority_score >= 0.4:
            return CachePriority.MEDIUM
        elif priority_score >= 0.2:
            return CachePriority.LOW
        else:
            return CachePriority.ARCHIVE
    
    async def store_entry(self, content_hash: str, url: str, content_type: str,
                         content_size: int, threat_score: float = 0.0,
                         content_freshness: float = 1.0,
                         update_frequency: float = 0.0) -> TemporalCacheEntry:
        """Store a new cache entry with temporal metadata."""
        current_time = datetime.now(timezone.utc)
        
        # Create cache entry
        entry = TemporalCacheEntry(
            content_hash=content_hash,
            url=url,
            content_type=content_type,
            content_size=content_size,
            created_at=current_time,
            last_accessed=current_time,
            access_count=1,
            threat_score=threat_score,
            content_freshness=content_freshness,
            update_frequency=update_frequency
        )
        
        # Calculate initial priority
        priority_score = self._calculate_priority_score(entry, current_time)
        entry.current_priority_score = priority_score
        entry.priority = self._determine_priority_level(priority_score)
        entry.ttl_seconds = self._calculate_ttl(entry)
        entry.next_evaluation = current_time + timedelta(hours=1)
        
        # Store in Redis
        await self.redis.hset(
            self.entries_key,
            content_hash,
            entry.json()
        )
        
        # Add to priority queue
        await self.redis.zadd(
            self.priority_queue_key,
            {content_hash: priority_score}
        )
        
        # Check if eviction is needed
        cache_size = await self.redis.hlen(self.entries_key)
        if cache_size > self.max_cache_size:
            await self._perform_evictions()
        
        self.logger.debug(f"Stored temporal cache entry for {url} with priority {entry.priority.value}")
        return entry
    
    async def access_entry(self, content_hash: str) -> Optional[TemporalCacheEntry]:
        """Access a cache entry and update temporal metadata."""
        entry_data = await self.redis.hget(self.entries_key, content_hash)
        if not entry_data:
            return None
        
        entry = TemporalCacheEntry.parse_raw(entry_data)
        current_time = datetime.now(timezone.utc)
        
        # Update access metadata
        entry.last_accessed = current_time
        entry.access_count += 1
        
        # Recalculate priority
        priority_score = self._calculate_priority_score(entry, current_time)
        entry.current_priority_score = priority_score
        entry.priority = self._determine_priority_level(priority_score)
        
        # Update in Redis
        await self.redis.hset(
            self.entries_key,
            content_hash,
            entry.json()
        )
        
        # Update priority queue
        await self.redis.zadd(
            self.priority_queue_key,
            {content_hash: priority_score}
        )
        
        # Log access for analytics
        await self._log_access(content_hash, current_time, entry.priority)
        
        return entry
    
    async def _log_access(self, content_hash: str, access_time: datetime,
                         priority: CachePriority) -> None:
        """Log cache access for analytics."""
        access_data = {
            "content_hash": content_hash,
            "timestamp": access_time.isoformat(),
            "priority": priority.value
        }
        
        # Store in time-series format (daily buckets)
        date_key = access_time.strftime("%Y-%m-%d")
        log_key = f"{self.access_log_key}:{date_key}"
        
        await self.redis.lpush(log_key, access_data)
        await self.redis.expire(log_key, 86400 * 7)  # Keep for 7 days
    
    async def _update_priorities(self) -> None:
        """Update priorities for all cache entries."""
        current_time = datetime.now(timezone.utc)
        
        # Get entries that need evaluation
        all_entries = await self.redis.hgetall(self.entries_key)
        updated_count = 0
        
        for content_hash, entry_data in all_entries.items():
            try:
                entry = TemporalCacheEntry.parse_raw(entry_data)
                
                # Check if entry needs evaluation
                if entry.next_evaluation and current_time < entry.next_evaluation:
                    continue
                
                # Calculate new priority
                old_score = entry.current_priority_score or 0.0
                new_score = self._calculate_priority_score(entry, current_time)
                
                # Update if score changed significantly
                if abs(new_score - old_score) > 0.1:
                    entry.current_priority_score = new_score
                    entry.priority = self._determine_priority_level(new_score)
                    entry.next_evaluation = current_time + timedelta(hours=1)
                    
                    # Update in Redis
                    await self.redis.hset(
                        self.entries_key,
                        content_hash,
                        entry.json()
                    )
                    
                    # Update priority queue
                    await self.redis.zadd(
                        self.priority_queue_key,
                        {content_hash: new_score}
                    )
                    
                    updated_count += 1
                
            except Exception as e:
                self.logger.error(f"Error updating priority for {content_hash}: {e}")
        
        if updated_count > 0:
            self.logger.debug(f"Updated priorities for {updated_count} cache entries")
    
    async def _perform_evictions(self) -> None:
        """Perform cache evictions based on priority and size limits."""
        cache_size = await self.redis.hlen(self.entries_key)
        
        if cache_size <= self.max_cache_size:
            return
        
        # Calculate how many entries to evict
        evict_count = cache_size - self.max_cache_size
        
        # Get lowest priority entries
        lowest_priority = await self.redis.zrange(
            self.priority_queue_key,
            0, evict_count - 1,
            withscores=True
        )
        
        evicted_count = 0
        for content_hash, score in lowest_priority:
            # Remove from cache
            await self.redis.hdel(self.entries_key, content_hash)
            await self.redis.zrem(self.priority_queue_key, content_hash)
            evicted_count += 1
        
        if evicted_count > 0:
            self.logger.info(f"Evicted {evicted_count} low-priority cache entries")
            
            # Update stats
            await self._update_stats("evictions", evicted_count)
    
    async def _warm_cache(self) -> None:
        """Proactively warm cache based on access patterns."""
        # Analyze recent access patterns
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        date_key = yesterday.strftime("%Y-%m-%d")
        log_key = f"{self.access_log_key}:{date_key}"
        
        # Get access patterns from yesterday
        access_logs = await self.redis.lrange(log_key, 0, -1)
        
        if not access_logs:
            return
        
        # Analyze patterns (this is a simplified version)
        # In practice, you'd implement more sophisticated pattern detection
        high_access_content = []
        for log_entry in access_logs:
            try:
                access_data = json.loads(log_entry)
                if access_data.get("priority") in ["critical", "high"]:
                    high_access_content.append(access_data["content_hash"])
            except Exception:
                continue
        
        # This is where you'd implement cache warming logic
        # For now, just log the opportunity
        if high_access_content:
            self.logger.debug(f"Cache warming opportunity: {len(high_access_content)} high-priority items")
    
    async def _update_stats(self, metric: str, value: int) -> None:
        """Update cache statistics."""
        await self.redis.hincrby(self.stats_key, metric, value)
        await self.redis.expire(self.stats_key, 86400 * 7)  # Keep for 7 days
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get temporal cache statistics."""
        cache_size = await self.redis.hlen(self.entries_key)
        queue_size = await self.redis.zcard(self.priority_queue_key)
        stats = await self.redis.hgetall(self.stats_key)
        
        # Count entries by priority
        priority_counts = {priority.value: 0 for priority in CachePriority}
        
        all_entries = await self.redis.hgetall(self.entries_key)
        for entry_data in all_entries.values():
            try:
                entry = TemporalCacheEntry.parse_raw(entry_data)
                priority_counts[entry.priority.value] += 1
            except Exception:
                continue
        
        return {
            "cache_size": cache_size,
            "priority_queue_size": queue_size,
            "priority_distribution": priority_counts,
            "evictions": int(stats.get("evictions", 0)),
            "cache_utilization": cache_size / self.max_cache_size if self.max_cache_size > 0 else 0
        }
    
    async def cleanup_expired_entries(self) -> int:
        """Clean up expired cache entries."""
        current_time = datetime.now(timezone.utc)
        expired_count = 0
        
        all_entries = await self.redis.hgetall(self.entries_key)
        
        for content_hash, entry_data in all_entries.items():
            try:
                entry = TemporalCacheEntry.parse_raw(entry_data)
                
                # Check if entry has expired
                expiry_time = entry.created_at + timedelta(seconds=entry.ttl_seconds)
                if current_time > expiry_time:
                    await self.redis.hdel(self.entries_key, content_hash)
                    await self.redis.zrem(self.priority_queue_key, content_hash)
                    expired_count += 1
                    
            except Exception as e:
                self.logger.error(f"Error checking expiry for {content_hash}: {e}")
                # Remove corrupted entry
                await self.redis.hdel(self.entries_key, content_hash)
                await self.redis.zrem(self.priority_queue_key, content_hash)
                expired_count += 1
        
        if expired_count > 0:
            self.logger.info(f"Cleaned up {expired_count} expired cache entries")
            await self._update_stats("expired_cleanups", expired_count)
        
        return expired_count