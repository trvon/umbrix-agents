"""
CrawlOrganizerAgent: Central coordination for all crawling activities.

This agent manages caching, deduplication, and intelligent crawl prioritization
across the entire system. It maintains a global view of what has been crawled
and intelligently decides what needs to be crawled next.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Any
from urllib.parse import urlparse, urljoin

import redis.asyncio as aioredis
from pydantic import BaseModel, Field

from ..common_tools.models.feed_record import FeedRecord
from ..common_tools.dedupe_store import RedisDedupeStore
from .cache_manager import CacheManager
from .bloom_filter_manager import BloomFilterManager
from .crawl_queue import CrawlQueue, CrawlTask
from .graph_analyzer import GraphAnalyzer


class CrawlMetrics(BaseModel):
    """Metrics for crawl operations."""
    
    urls_queued: int = 0
    urls_processed: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    dedupe_hits: int = 0
    graph_hits: int = 0
    errors: int = 0
    processing_time_total: float = 0.0
    avg_processing_time: float = 0.0


class CrawlStatus(BaseModel):
    """Status information for crawl operations."""
    
    status: str  # 'active', 'paused', 'error'
    queue_size: int
    processing_rate: float  # URLs per minute
    cache_hit_rate: float
    dedupe_rate: float
    last_activity: datetime
    uptime: float
    
    
class CrawlOrganizerAgent:
    """
    Central coordinator for all crawling activities.
    
    Manages caching, deduplication, prioritization, and resource allocation
    across all crawlers in the system.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the CrawlOrganizerAgent."""
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.redis_client: Optional[aioredis.Redis] = None
        self.dedupe_store: Optional[RedisDedupeStore] = None
        self.cache_manager: Optional[CacheManager] = None
        self.bloom_filter: Optional[BloomFilterManager] = None
        self.crawl_queue: Optional[CrawlQueue] = None
        self.graph_analyzer: Optional[GraphAnalyzer] = None
        
        # Metrics and status
        self.metrics = CrawlMetrics()
        self.start_time = datetime.now(timezone.utc)
        self.running = False
        
        # Configuration
        self.max_queue_size = config.get('max_queue_size', 10000)
        self.max_processing_rate = config.get('max_processing_rate', 100)  # URLs per minute
        self.cache_ttl = config.get('cache_ttl', 3600)  # 1 hour
        self.dedupe_ttl = config.get('dedupe_ttl', 86400)  # 24 hours
        
    async def initialize(self) -> None:
        """Initialize all components and connections."""
        try:
            # Initialize Redis connection
            redis_url = self.config.get('redis_url', 'redis://localhost:6379')
            self.redis_client = aioredis.from_url(redis_url)
            
            # Test Redis connection
            await self.redis_client.ping()
            self.logger.info("Connected to Redis successfully")
            
            # Initialize components
            self.dedupe_store = RedisDedupeStore(
                redis_client=self.redis_client,
                ttl_seconds=self.dedupe_ttl
            )
            
            self.cache_manager = CacheManager(
                redis_client=self.redis_client,
                ttl_seconds=self.cache_ttl
            )
            
            self.bloom_filter = BloomFilterManager(
                redis_client=self.redis_client,
                expected_elements=self.config.get('bloom_filter_size', 1000000)
            )
            
            self.crawl_queue = CrawlQueue(
                redis_client=self.redis_client,
                max_size=self.max_queue_size
            )
            
            self.graph_analyzer = GraphAnalyzer(self.config)
            
            await self.graph_analyzer.initialize()
            
            self.logger.info("CrawlOrganizerAgent initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize CrawlOrganizerAgent: {e}")
            raise
    
    async def should_crawl_url(self, url: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Determine if a URL should be crawled based on caching, deduplication, and graph analysis.
        
        Returns:
            Dict with 'should_crawl' boolean and reasoning
        """
        result = {
            'should_crawl': True,
            'reason': 'no_checks_failed',
            'cache_hit': False,
            'dedupe_hit': False,
            'graph_hit': False,
            'priority': 0.5  # Default priority
        }
        
        try:
            # Normalize URL
            normalized_url = self._normalize_url(url)
            
            # Check bloom filter first (fastest check)
            if await self.bloom_filter.contains(normalized_url):
                # URL has been seen before, check other sources
                
                # Check deduplication store
                if await self.dedupe_store.is_duplicate(normalized_url):
                    result.update({
                        'should_crawl': False,
                        'reason': 'dedupe_hit',
                        'dedupe_hit': True
                    })
                    self.metrics.dedupe_hits += 1
                    return result
                
                # Check cache
                cached_content = await self.cache_manager.get(normalized_url)
                if cached_content:
                    result.update({
                        'should_crawl': False,
                        'reason': 'cache_hit',
                        'cache_hit': True
                    })
                    self.metrics.cache_hits += 1
                    return result
                
                # Check graph for existing content
                if await self.graph_analyzer.content_exists(normalized_url):
                    result.update({
                        'should_crawl': False,
                        'reason': 'graph_hit',
                        'graph_hit': True
                    })
                    self.metrics.graph_hits += 1
                    return result
            
            # If we get here, URL should be crawled
            result['priority'] = await self._calculate_priority(normalized_url, context)
            self.metrics.cache_misses += 1
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error checking URL {url}: {e}")
            # On error, default to allowing crawl with low priority
            result.update({
                'should_crawl': True,
                'reason': f'error_checking: {str(e)}',
                'priority': 0.1
            })
            self.metrics.errors += 1
            return result
    
    async def queue_crawl_task(self, url: str, context: Dict[str, Any] = None) -> bool:
        """
        Queue a URL for crawling after checking if it should be crawled.
        
        Returns:
            True if successfully queued, False otherwise
        """
        try:
            # Check if URL should be crawled
            crawl_decision = await self.should_crawl_url(url, context)
            
            if not crawl_decision['should_crawl']:
                self.logger.debug(f"Skipping URL {url}: {crawl_decision['reason']}")
                return False
            
            # Create crawl task
            task = CrawlTask(
                url=url,
                priority=crawl_decision['priority'],
                context=context or {},
                created_at=datetime.now(timezone.utc)
            )
            
            # Add to queue
            success = await self.crawl_queue.add_task(task)
            
            if success:
                # Add to bloom filter
                await self.bloom_filter.add(self._normalize_url(url))
                self.metrics.urls_queued += 1
                self.logger.info(f"Queued URL for crawling: {url} (priority: {task.priority})")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error queuing crawl task for {url}: {e}")
            self.metrics.errors += 1
            return False
    
    async def get_next_crawl_task(self) -> Optional[CrawlTask]:
        """Get the next highest priority crawl task from the queue."""
        try:
            task = await self.crawl_queue.get_next_task()
            if task:
                self.metrics.urls_processed += 1
            return task
        except Exception as e:
            self.logger.error(f"Error getting next crawl task: {e}")
            self.metrics.errors += 1
            return None
    
    async def mark_crawl_complete(self, url: str, content: str = None, success: bool = True) -> None:
        """Mark a crawl task as complete and update caches."""
        try:
            normalized_url = self._normalize_url(url)
            
            # Add to deduplication store
            await self.dedupe_store.add(normalized_url)
            
            # Cache content if provided and successful
            if success and content:
                await self.cache_manager.set(normalized_url, content)
            
            # Update bloom filter
            await self.bloom_filter.add(normalized_url)
            
            self.logger.debug(f"Marked crawl complete for {url} (success: {success})")
            
        except Exception as e:
            self.logger.error(f"Error marking crawl complete for {url}: {e}")
            self.metrics.errors += 1
    
    async def _calculate_priority(self, url: str, context: Dict[str, Any] = None) -> float:
        """
        Calculate priority for a URL based on various factors.
        
        Returns:
            Priority score between 0.0 and 1.0
        """
        priority = 0.5  # Base priority
        
        try:
            # Factor 1: Domain reputation/importance
            domain = urlparse(url).netloc
            if any(important_domain in domain.lower() for important_domain in [
                'security', 'threat', 'cert', 'cisa', 'mitre', 'nist'
            ]):
                priority += 0.2
            
            # Factor 2: URL patterns indicating CTI content
            if any(pattern in url.lower() for pattern in [
                'vulnerability', 'cve', 'advisory', 'threat', 'malware', 'ioc'
            ]):
                priority += 0.15
            
            # Factor 3: Context-based priority
            if context:
                # If this URL is related to recent threat intelligence
                if context.get('is_recent_intel'):
                    priority += 0.15
                
                # If this URL was mentioned in multiple sources
                mention_count = context.get('mention_count', 0)
                if mention_count > 1:
                    priority += min(0.1 * mention_count, 0.2)
            
            # Factor 4: Graph analysis - check if this content would fill gaps
            graph_priority = await self.graph_analyzer.calculate_content_priority(url, context)
            priority += graph_priority * 0.2
            
            # Ensure priority stays within bounds
            priority = max(0.0, min(1.0, priority))
            
        except Exception as e:
            self.logger.warning(f"Error calculating priority for {url}: {e}")
            priority = 0.5  # Default on error
        
        return priority
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL for consistent caching and deduplication."""
        try:
            parsed = urlparse(url)
            # Remove fragment and normalize
            normalized = f"{parsed.scheme}://{parsed.netloc.lower()}{parsed.path}"
            if parsed.query:
                normalized += f"?{parsed.query}"
            return normalized
        except Exception:
            return url.lower()
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics and statistics."""
        uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        
        # Calculate rates
        processing_rate = self.metrics.urls_processed / max(uptime / 60, 1)  # per minute
        cache_hit_rate = self.metrics.cache_hits / max(
            self.metrics.cache_hits + self.metrics.cache_misses, 1
        )
        dedupe_rate = self.metrics.dedupe_hits / max(self.metrics.urls_queued + self.metrics.dedupe_hits, 1)
        
        queue_size = await self.crawl_queue.size() if self.crawl_queue else 0
        
        status = CrawlStatus(
            status='active' if self.running else 'paused',
            queue_size=queue_size,
            processing_rate=processing_rate,
            cache_hit_rate=cache_hit_rate,
            dedupe_rate=dedupe_rate,
            last_activity=datetime.now(timezone.utc),
            uptime=uptime
        )
        
        return {
            'metrics': self.metrics.model_dump(),
            'status': status.model_dump(),
            'config': {
                'max_queue_size': self.max_queue_size,
                'max_processing_rate': self.max_processing_rate,
                'cache_ttl': self.cache_ttl,
                'dedupe_ttl': self.dedupe_ttl
            }
        }
    
    async def cleanup(self) -> None:
        """Cleanup resources and connections."""
        try:
            self.running = False
            
            if self.redis_client:
                await self.redis_client.close()
            
            if self.graph_analyzer:
                await self.graph_analyzer.cleanup()
            
            self.logger.info("CrawlOrganizerAgent cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


# Convenience function for external use
async def create_crawl_organizer(config: Dict[str, Any]) -> CrawlOrganizerAgent:
    """Create and initialize a CrawlOrganizerAgent."""
    organizer = CrawlOrganizerAgent(config)
    await organizer.initialize()
    return organizer