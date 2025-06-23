"""
BloomFilterManager: Scalable bloom filter implementation for URL tracking.

Manages Redis-based bloom filters for efficient URL tracking across millions of URLs.
"""

import asyncio
import hashlib
import logging
import math
from typing import List, Optional, Set
from datetime import datetime, timezone

import redis.asyncio as aioredis
from pydantic import BaseModel


class BloomFilterStats(BaseModel):
    """Bloom filter statistics."""
    
    expected_elements: int
    bit_array_size: int
    hash_functions: int
    estimated_elements: int = 0
    false_positive_rate: float = 0.0
    memory_usage_bytes: int = 0


class BloomFilterManager:
    """
    Redis-based bloom filter for efficient URL tracking.
    
    Features:
    - Scalable to millions of URLs
    - Configurable false positive rate
    - Redis-backed persistence
    - Memory efficient bit operations
    - Statistics and monitoring
    """
    
    def __init__(self, redis_client: aioredis.Redis, expected_elements: int = 1000000, 
                 false_positive_rate: float = 0.01):
        """
        Initialize the BloomFilterManager.
        
        Args:
            redis_client: Redis client for storage
            expected_elements: Expected number of elements
            false_positive_rate: Desired false positive rate (0.01 = 1%)
        """
        self.redis = redis_client
        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate
        self.logger = logging.getLogger(__name__)
        
        # Calculate optimal bloom filter parameters
        self.bit_array_size = self._calculate_bit_array_size()
        self.hash_functions = self._calculate_hash_functions()
        
        # Redis key for the bloom filter
        self.bloom_key = "crawl_bloom_filter"
        self.stats_key = "crawl_bloom_stats"
        
        # Initialize stats
        self.stats = BloomFilterStats(
            expected_elements=expected_elements,
            bit_array_size=self.bit_array_size,
            hash_functions=self.hash_functions
        )
        
        self.logger.info(
            f"BloomFilter initialized: {self.bit_array_size} bits, "
            f"{self.hash_functions} hash functions, "
            f"expected FP rate: {false_positive_rate:.4f}"
        )
    
    async def add(self, url: str) -> None:
        """
        Add a URL to the bloom filter.
        
        Args:
            url: URL to add
        """
        try:
            # Get hash positions for the URL
            positions = self._get_hash_positions(url)
            
            # Set bits in Redis using pipeline for efficiency
            pipeline = self.redis.pipeline()
            for position in positions:
                pipeline.setbit(self.bloom_key, position, 1)
            
            await pipeline.execute()
            
            # Update estimated element count
            self.stats.estimated_elements += 1
            await self._update_stats()
            
            self.logger.debug(f"Added URL to bloom filter: {url}")
            
        except Exception as e:
            self.logger.error(f"Error adding URL to bloom filter: {e}")
            raise
    
    async def contains(self, url: str) -> bool:
        """
        Check if a URL might be in the bloom filter.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL might be in the filter (with potential false positives),
            False if URL is definitely not in the filter
        """
        try:
            # Get hash positions for the URL
            positions = self._get_hash_positions(url)
            
            # Check if all bits are set
            pipeline = self.redis.pipeline()
            for position in positions:
                pipeline.getbit(self.bloom_key, position)
            
            results = await pipeline.execute()
            
            # URL is possibly in the filter only if all bits are set
            is_present = all(bit == 1 for bit in results)
            
            self.logger.debug(f"Bloom filter check for {url}: {is_present}")
            return is_present
            
        except Exception as e:
            self.logger.error(f"Error checking bloom filter for URL: {e}")
            # On error, assume URL might be present to avoid missing checks
            return True
    
    async def add_batch(self, urls: List[str]) -> int:
        """
        Add multiple URLs to the bloom filter in batch.
        
        Args:
            urls: List of URLs to add
            
        Returns:
            Number of URLs successfully added
        """
        added_count = 0
        
        try:
            # Collect all positions for all URLs
            all_positions = set()
            for url in urls:
                positions = self._get_hash_positions(url)
                all_positions.update(positions)
            
            # Set all bits in a single pipeline
            if all_positions:
                pipeline = self.redis.pipeline()
                for position in all_positions:
                    pipeline.setbit(self.bloom_key, position, 1)
                
                await pipeline.execute()
                added_count = len(urls)
                
                # Update estimated element count
                self.stats.estimated_elements += added_count
                await self._update_stats()
                
                self.logger.info(f"Added {added_count} URLs to bloom filter in batch")
            
        except Exception as e:
            self.logger.error(f"Error adding URLs to bloom filter in batch: {e}")
        
        return added_count
    
    async def get_stats(self) -> BloomFilterStats:
        """Get current bloom filter statistics."""
        try:
            # Calculate current false positive rate
            if self.stats.estimated_elements > 0:
                # Approximate false positive rate calculation
                filled_bits = min(self.stats.estimated_elements * self.hash_functions, 
                                 self.bit_array_size)
                fill_ratio = filled_bits / self.bit_array_size
                self.stats.false_positive_rate = (1 - math.exp(-fill_ratio)) ** self.hash_functions
            
            # Get memory usage from Redis
            try:
                memory_usage = await self.redis.memory_usage(self.bloom_key)
                if memory_usage:
                    self.stats.memory_usage_bytes = memory_usage
            except Exception:
                # Estimate memory usage if Redis doesn't support MEMORY USAGE
                self.stats.memory_usage_bytes = self.bit_array_size // 8
            
            return self.stats
            
        except Exception as e:
            self.logger.error(f"Error getting bloom filter stats: {e}")
            return self.stats
    
    async def clear(self) -> None:
        """Clear the bloom filter."""
        try:
            await self.redis.delete(self.bloom_key)
            await self.redis.delete(self.stats_key)
            
            # Reset stats
            self.stats.estimated_elements = 0
            self.stats.false_positive_rate = 0.0
            self.stats.memory_usage_bytes = 0
            
            self.logger.info("Bloom filter cleared")
            
        except Exception as e:
            self.logger.error(f"Error clearing bloom filter: {e}")
            raise
    
    async def export_to_file(self, filepath: str) -> bool:
        """
        Export bloom filter to a file for backup/analysis.
        
        Args:
            filepath: Path to export file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get the entire bit array from Redis
            bit_data = await self.redis.get(self.bloom_key)
            
            if bit_data:
                with open(filepath, 'wb') as f:
                    f.write(bit_data)
                
                self.logger.info(f"Bloom filter exported to {filepath}")
                return True
            else:
                self.logger.warning("No bloom filter data to export")
                return False
                
        except Exception as e:
            self.logger.error(f"Error exporting bloom filter: {e}")
            return False
    
    async def import_from_file(self, filepath: str) -> bool:
        """
        Import bloom filter from a file.
        
        Args:
            filepath: Path to import file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(filepath, 'rb') as f:
                bit_data = f.read()
            
            # Set the bit array in Redis
            await self.redis.set(self.bloom_key, bit_data)
            
            self.logger.info(f"Bloom filter imported from {filepath}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error importing bloom filter: {e}")
            return False
    
    def _calculate_bit_array_size(self) -> int:
        """Calculate optimal bit array size based on expected elements and FP rate."""
        # m = -(n * ln(p)) / (ln(2)^2)
        # where n = expected elements, p = false positive rate
        size = int(-self.expected_elements * math.log(self.false_positive_rate) / 
                  (math.log(2) ** 2))
        # Round up to nearest multiple of 8 for byte alignment
        return ((size + 7) // 8) * 8
    
    def _calculate_hash_functions(self) -> int:
        """Calculate optimal number of hash functions."""
        # k = (m/n) * ln(2)
        # where m = bit array size, n = expected elements
        hash_funcs = int((self.bit_array_size / self.expected_elements) * math.log(2))
        # Ensure at least 1 hash function
        return max(1, hash_funcs)
    
    def _get_hash_positions(self, url: str) -> List[int]:
        """
        Get hash positions for a URL using multiple hash functions.
        
        Args:
            url: URL to hash
            
        Returns:
            List of bit positions to set/check
        """
        positions = []
        
        # Use different hash algorithms to simulate multiple hash functions
        url_bytes = url.encode('utf-8')
        
        for i in range(self.hash_functions):
            # Create different hash values by adding salt
            salted_url = f"{url}_{i}".encode('utf-8')
            
            # Use different hash algorithms
            if i % 3 == 0:
                hash_value = int(hashlib.md5(salted_url).hexdigest(), 16)
            elif i % 3 == 1:
                hash_value = int(hashlib.sha256(salted_url).hexdigest(), 16)
            else:
                hash_value = int(hashlib.sha1(salted_url).hexdigest(), 16)
            
            # Map hash to bit array position
            position = hash_value % self.bit_array_size
            positions.append(position)
        
        return positions
    
    async def _update_stats(self) -> None:
        """Update statistics in Redis."""
        try:
            stats_data = self.stats.model_dump_json()
            await self.redis.set(self.stats_key, stats_data)
        except Exception as e:
            self.logger.warning(f"Error updating bloom filter stats: {e}")
    
    async def _load_stats(self) -> None:
        """Load statistics from Redis."""
        try:
            stats_data = await self.redis.get(self.stats_key)
            if stats_data:
                self.stats = BloomFilterStats.model_validate_json(stats_data)
        except Exception as e:
            self.logger.warning(f"Error loading bloom filter stats: {e}")