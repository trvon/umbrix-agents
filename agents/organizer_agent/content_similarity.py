"""
Content Similarity Detection using LSH (Locality Sensitive Hashing).

Implements MinHash and SimHash algorithms for content deduplication and similarity detection.
"""

import hashlib
import logging
import re
import time
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict
import struct
import mmh3  # MurmurHash3 for better hashing performance

import redis.asyncio as aioredis
from pydantic import BaseModel


@dataclass
class SimilarityResult:
    """Result of content similarity comparison."""
    similarity_score: float
    is_duplicate: bool
    content_hash: str
    similar_hashes: List[str]


class ContentFingerprint(BaseModel):
    """Content fingerprint containing multiple hash signatures."""
    content_hash: str  # SHA-256 of content
    minhash_signature: List[int]  # MinHash signature
    simhash_signature: int  # SimHash signature
    url: str
    timestamp: float
    content_length: int


class MinHashLSH:
    """MinHash implementation for content similarity detection."""
    
    def __init__(self, num_perm: int = 128, threshold: float = 0.8):
        """
        Initialize MinHash LSH.
        
        Args:
            num_perm: Number of permutations for MinHash
            threshold: Similarity threshold for duplicate detection
        """
        self.num_perm = num_perm
        self.threshold = threshold
        self.logger = logging.getLogger(__name__)
    
    def _get_shingles(self, text: str, k: int = 3) -> Set[str]:
        """Extract k-shingles from text."""
        # Clean and normalize text
        text = re.sub(r'\s+', ' ', text.lower().strip())
        if len(text) < k:
            return {text}
        
        shingles = set()
        for i in range(len(text) - k + 1):
            shingles.add(text[i:i + k])
        return shingles
    
    def generate_minhash_signature(self, content: str) -> List[int]:
        """Generate MinHash signature for content."""
        shingles = self._get_shingles(content)
        
        # Convert shingles to hashes
        shingle_hashes = []
        for shingle in shingles:
            shingle_hashes.append(mmh3.hash(shingle.encode('utf-8')))
        
        if not shingle_hashes:
            return [0] * self.num_perm
        
        # Generate MinHash signature
        signature = []
        for i in range(self.num_perm):
            min_hash = float('inf')
            for shingle_hash in shingle_hashes:
                # Apply different hash function for each permutation
                perm_hash = mmh3.hash(struct.pack('i', shingle_hash), seed=i)
                min_hash = min(min_hash, perm_hash)
            signature.append(min_hash)
        
        return signature
    
    def calculate_similarity(self, sig1: List[int], sig2: List[int]) -> float:
        """Calculate Jaccard similarity between two MinHash signatures."""
        if len(sig1) != len(sig2):
            return 0.0
        
        matches = sum(1 for a, b in zip(sig1, sig2) if a == b)
        return matches / len(sig1)


class SimHashLSH:
    """SimHash implementation for content similarity detection."""
    
    def __init__(self, hash_bits: int = 64, threshold: int = 3):
        """
        Initialize SimHash LSH.
        
        Args:
            hash_bits: Number of bits in SimHash (default 64)
            threshold: Hamming distance threshold for similarity
        """
        self.hash_bits = hash_bits
        self.threshold = threshold
        self.logger = logging.getLogger(__name__)
    
    def _get_features(self, content: str) -> Dict[str, int]:
        """Extract features with weights from content."""
        # Simple word-based features with frequency as weight
        words = re.findall(r'\b\w+\b', content.lower())
        features = defaultdict(int)
        
        for word in words:
            if len(word) > 2:  # Filter short words
                features[word] += 1
        
        return dict(features)
    
    def generate_simhash_signature(self, content: str) -> int:
        """Generate SimHash signature for content."""
        features = self._get_features(content)
        
        # Initialize hash array
        hash_array = [0] * self.hash_bits
        
        for feature, weight in features.items():
            # Hash the feature
            feature_hash = mmh3.hash(feature.encode('utf-8'))
            
            # For each bit position
            for i in range(self.hash_bits):
                # Check if bit is set in feature hash
                if (feature_hash >> i) & 1:
                    hash_array[i] += weight
                else:
                    hash_array[i] -= weight
        
        # Convert to final hash
        simhash = 0
        for i in range(self.hash_bits):
            if hash_array[i] > 0:
                simhash |= (1 << i)
        
        return simhash
    
    def calculate_hamming_distance(self, hash1: int, hash2: int) -> int:
        """Calculate Hamming distance between two hashes."""
        xor_result = hash1 ^ hash2
        return bin(xor_result).count('1')
    
    def is_similar(self, hash1: int, hash2: int) -> bool:
        """Check if two hashes are similar based on threshold."""
        distance = self.calculate_hamming_distance(hash1, hash2)
        return distance <= self.threshold


class ContentSimilarityManager:
    """
    Manages content similarity detection using both MinHash and SimHash.
    
    Features:
    - Duplicate content detection
    - Similar content discovery
    - Redis-backed storage
    - Configurable similarity thresholds
    """
    
    def __init__(self, redis_client: aioredis.Redis, 
                 minhash_threshold: float = 0.8,
                 simhash_threshold: int = 3):
        """
        Initialize ContentSimilarityManager.
        
        Args:
            redis_client: Redis client for storage
            minhash_threshold: MinHash similarity threshold
            simhash_threshold: SimHash Hamming distance threshold
        """
        self.redis = redis_client
        self.minhash = MinHashLSH(threshold=minhash_threshold)
        self.simhash = SimHashLSH(threshold=simhash_threshold)
        self.logger = logging.getLogger(__name__)
        
        # Redis keys
        self.fingerprints_key = "content:fingerprints"
        self.minhash_buckets_key = "content:minhash_buckets"
        self.simhash_buckets_key = "content:simhash_buckets"
    
    async def generate_fingerprint(self, url: str, content: str) -> ContentFingerprint:
        """Generate comprehensive fingerprint for content."""
        # Basic content hash
        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        
        # MinHash signature
        minhash_signature = self.minhash.generate_minhash_signature(content)
        
        # SimHash signature
        simhash_signature = self.simhash.generate_simhash_signature(content)
        
        fingerprint = ContentFingerprint(
            content_hash=content_hash,
            minhash_signature=minhash_signature,
            simhash_signature=simhash_signature,
            url=url,
            timestamp=time.time(),
            content_length=len(content)
        )
        
        return fingerprint
    
    async def store_fingerprint(self, fingerprint: ContentFingerprint) -> None:
        """Store fingerprint in Redis with LSH buckets."""
        # Store full fingerprint
        fingerprint_data = fingerprint.json()
        await self.redis.hset(
            self.fingerprints_key,
            fingerprint.content_hash,
            fingerprint_data
        )
        
        # Store in MinHash LSH buckets
        await self._store_minhash_buckets(fingerprint)
        
        # Store in SimHash LSH buckets
        await self._store_simhash_buckets(fingerprint)
        
        self.logger.debug(f"Stored fingerprint for {fingerprint.url}")
    
    async def _store_minhash_buckets(self, fingerprint: ContentFingerprint) -> None:
        """Store fingerprint in MinHash LSH buckets."""
        # Use bands and rows for LSH
        bands = 16
        rows = len(fingerprint.minhash_signature) // bands
        
        for band in range(bands):
            start = band * rows
            end = start + rows
            band_signature = tuple(fingerprint.minhash_signature[start:end])
            
            # Hash the band signature
            band_hash = hashlib.md5(str(band_signature).encode()).hexdigest()
            bucket_key = f"{self.minhash_buckets_key}:{band}:{band_hash}"
            
            await self.redis.sadd(bucket_key, fingerprint.content_hash)
            await self.redis.expire(bucket_key, 86400 * 30)  # 30 days TTL
    
    async def _store_simhash_buckets(self, fingerprint: ContentFingerprint) -> None:
        """Store fingerprint in SimHash LSH buckets."""
        # Create multiple buckets by masking different bit positions
        for mask_bits in range(0, 64, 8):  # Every 8 bits
            masked_hash = fingerprint.simhash_signature & ~((1 << mask_bits) - 1)
            bucket_key = f"{self.simhash_buckets_key}:{mask_bits}:{masked_hash}"
            
            await self.redis.sadd(bucket_key, fingerprint.content_hash)
            await self.redis.expire(bucket_key, 86400 * 30)  # 30 days TTL
    
    async def find_similar_content(self, content: str) -> SimilarityResult:
        """Find similar content using both MinHash and SimHash."""
        # Generate fingerprint for new content
        temp_fingerprint = await self.generate_fingerprint("temp", content)
        
        # Find candidates using LSH buckets
        candidates = set()
        
        # MinHash candidates
        minhash_candidates = await self._find_minhash_candidates(temp_fingerprint)
        candidates.update(minhash_candidates)
        
        # SimHash candidates
        simhash_candidates = await self._find_simhash_candidates(temp_fingerprint)
        candidates.update(simhash_candidates)
        
        # Calculate actual similarities
        similar_hashes = []
        max_similarity = 0.0
        
        for candidate_hash in candidates:
            fingerprint_data = await self.redis.hget(self.fingerprints_key, candidate_hash)
            if not fingerprint_data:
                continue
            
            candidate_fingerprint = ContentFingerprint.parse_raw(fingerprint_data)
            
            # Calculate MinHash similarity
            minhash_sim = self.minhash.calculate_similarity(
                temp_fingerprint.minhash_signature,
                candidate_fingerprint.minhash_signature
            )
            
            # Calculate SimHash similarity (inverse of Hamming distance)
            hamming_dist = self.simhash.calculate_hamming_distance(
                temp_fingerprint.simhash_signature,
                candidate_fingerprint.simhash_signature
            )
            simhash_sim = 1.0 - (hamming_dist / 64.0)
            
            # Use weighted average of both similarities
            combined_similarity = (minhash_sim * 0.7) + (simhash_sim * 0.3)
            
            if combined_similarity > max_similarity:
                max_similarity = combined_similarity
            
            if combined_similarity >= self.minhash.threshold:
                similar_hashes.append(candidate_hash)
        
        is_duplicate = len(similar_hashes) > 0
        
        return SimilarityResult(
            similarity_score=max_similarity,
            is_duplicate=is_duplicate,
            content_hash=temp_fingerprint.content_hash,
            similar_hashes=similar_hashes
        )
    
    async def _find_minhash_candidates(self, fingerprint: ContentFingerprint) -> Set[str]:
        """Find candidate similar content using MinHash LSH buckets."""
        candidates = set()
        bands = 16
        rows = len(fingerprint.minhash_signature) // bands
        
        for band in range(bands):
            start = band * rows
            end = start + rows
            band_signature = tuple(fingerprint.minhash_signature[start:end])
            
            band_hash = hashlib.md5(str(band_signature).encode()).hexdigest()
            bucket_key = f"{self.minhash_buckets_key}:{band}:{band_hash}"
            
            bucket_members = await self.redis.smembers(bucket_key)
            candidates.update(bucket_members)
        
        return candidates
    
    async def _find_simhash_candidates(self, fingerprint: ContentFingerprint) -> Set[str]:
        """Find candidate similar content using SimHash LSH buckets."""
        candidates = set()
        
        for mask_bits in range(0, 64, 8):
            masked_hash = fingerprint.simhash_signature & ~((1 << mask_bits) - 1)
            bucket_key = f"{self.simhash_buckets_key}:{mask_bits}:{masked_hash}"
            
            bucket_members = await self.redis.smembers(bucket_key)
            candidates.update(bucket_members)
        
        return candidates
    
    async def is_duplicate_content(self, content: str) -> bool:
        """Quick check if content is a duplicate."""
        result = await self.find_similar_content(content)
        return result.is_duplicate
    
    async def get_statistics(self) -> Dict[str, int]:
        """Get similarity detection statistics."""
        total_fingerprints = await self.redis.hlen(self.fingerprints_key)
        
        # Count buckets
        minhash_buckets = len(await self.redis.keys(f"{self.minhash_buckets_key}:*"))
        simhash_buckets = len(await self.redis.keys(f"{self.simhash_buckets_key}:*"))
        
        return {
            "total_fingerprints": total_fingerprints,
            "minhash_buckets": minhash_buckets,
            "simhash_buckets": simhash_buckets
        }
    
    async def cleanup_expired_fingerprints(self, max_age_days: int = 30) -> int:
        """Clean up old fingerprints."""
        current_time = time.time()
        max_age_seconds = max_age_days * 86400
        
        # Get all fingerprints
        fingerprints = await self.redis.hgetall(self.fingerprints_key)
        deleted_count = 0
        
        for content_hash, fingerprint_data in fingerprints.items():
            try:
                fingerprint = ContentFingerprint.parse_raw(fingerprint_data)
                if current_time - fingerprint.timestamp > max_age_seconds:
                    await self.redis.hdel(self.fingerprints_key, content_hash)
                    deleted_count += 1
            except Exception as e:
                self.logger.error(f"Error parsing fingerprint {content_hash}: {e}")
                await self.redis.hdel(self.fingerprints_key, content_hash)
                deleted_count += 1
        
        self.logger.info(f"Cleaned up {deleted_count} expired fingerprints")
        return deleted_count