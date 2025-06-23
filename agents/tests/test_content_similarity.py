"""
Test coverage for organizer_agent/content_similarity.py

This module provides comprehensive test coverage for the content similarity detection
system, including MinHash, SimHash, LSH buckets, and Redis storage.
"""

import pytest
import asyncio
import hashlib
import time
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, List, Set

from organizer_agent.content_similarity import (
    SimilarityResult,
    ContentFingerprint,
    MinHashLSH,
    SimHashLSH,
    ContentSimilarityManager
)


class TestSimilarityResult:
    """Test the SimilarityResult dataclass."""
    
    def test_similarity_result_creation(self):
        """Test creating a SimilarityResult."""
        result = SimilarityResult(
            similarity_score=0.85,
            is_duplicate=True,
            content_hash="abc123",
            similar_hashes=["def456", "ghi789"]
        )
        
        assert result.similarity_score == 0.85
        assert result.is_duplicate is True
        assert result.content_hash == "abc123"
        assert result.similar_hashes == ["def456", "ghi789"]

    def test_similarity_result_no_duplicates(self):
        """Test SimilarityResult with no duplicates found."""
        result = SimilarityResult(
            similarity_score=0.1,
            is_duplicate=False,
            content_hash="unique123",
            similar_hashes=[]
        )
        
        assert result.similarity_score == 0.1
        assert result.is_duplicate is False
        assert result.content_hash == "unique123"
        assert result.similar_hashes == []


class TestContentFingerprint:
    """Test the ContentFingerprint model."""
    
    def test_content_fingerprint_creation(self):
        """Test creating a ContentFingerprint."""
        fingerprint = ContentFingerprint(
            content_hash="abc123def456",
            minhash_signature=[1, 2, 3, 4, 5],
            simhash_signature=123456789,
            url="https://example.com/article",
            timestamp=1234567890.0,
            content_length=500
        )
        
        assert fingerprint.content_hash == "abc123def456"
        assert fingerprint.minhash_signature == [1, 2, 3, 4, 5]
        assert fingerprint.simhash_signature == 123456789
        assert fingerprint.url == "https://example.com/article"
        assert fingerprint.timestamp == 1234567890.0
        assert fingerprint.content_length == 500

    def test_content_fingerprint_json_serialization(self):
        """Test JSON serialization/deserialization of ContentFingerprint."""
        fingerprint = ContentFingerprint(
            content_hash="test_hash",
            minhash_signature=[10, 20, 30],
            simhash_signature=987654321,
            url="https://test.com",
            timestamp=time.time(),
            content_length=100
        )
        
        # Serialize to JSON
        json_data = fingerprint.json()
        assert isinstance(json_data, str)
        
        # Deserialize from JSON
        restored = ContentFingerprint.parse_raw(json_data)
        assert restored.content_hash == fingerprint.content_hash
        assert restored.minhash_signature == fingerprint.minhash_signature
        assert restored.simhash_signature == fingerprint.simhash_signature
        assert restored.url == fingerprint.url


class TestMinHashLSH:
    """Test the MinHashLSH class."""
    
    def test_minhash_initialization(self):
        """Test MinHashLSH initialization."""
        minhash = MinHashLSH(num_perm=64, threshold=0.7)
        
        assert minhash.num_perm == 64
        assert minhash.threshold == 0.7
        assert minhash.logger is not None

    def test_minhash_default_initialization(self):
        """Test MinHashLSH with default parameters."""
        minhash = MinHashLSH()
        
        assert minhash.num_perm == 128
        assert minhash.threshold == 0.8

    def test_get_shingles_normal_text(self):
        """Test shingle generation for normal text."""
        minhash = MinHashLSH()
        text = "Hello world test"
        shingles = minhash._get_shingles(text, k=3)
        
        expected_shingles = {'hel', 'ell', 'llo', 'lo ', 'o w', ' wo', 'wor', 'orl', 'rld', 'ld ', 'd t', ' te', 'tes', 'est'}
        assert shingles == expected_shingles

    def test_get_shingles_short_text(self):
        """Test shingle generation for text shorter than k."""
        minhash = MinHashLSH()
        text = "hi"
        shingles = minhash._get_shingles(text, k=3)
        
        assert shingles == {'hi'}

    def test_get_shingles_normalize_whitespace(self):
        """Test that shingles properly normalize whitespace."""
        minhash = MinHashLSH()
        text = "Hello    world\t\ntest"
        shingles = minhash._get_shingles(text, k=3)
        
        # Should normalize to "hello world test"
        assert 'hel' in shingles
        assert 'o w' in shingles

    def test_generate_minhash_signature(self):
        """Test MinHash signature generation."""
        minhash = MinHashLSH(num_perm=10)
        content = "This is a test document for MinHash signature generation."
        
        signature = minhash.generate_minhash_signature(content)
        
        assert len(signature) == 10
        assert all(isinstance(val, int) for val in signature)

    def test_generate_minhash_signature_empty_content(self):
        """Test MinHash signature for empty content."""
        minhash = MinHashLSH(num_perm=5)
        signature = minhash.generate_minhash_signature("")
        
        # Empty content still generates a valid signature (using single character shingle)
        assert len(signature) == 5
        assert all(isinstance(val, int) for val in signature)

    def test_calculate_similarity_identical(self):
        """Test similarity calculation for identical signatures."""
        minhash = MinHashLSH()
        sig1 = [1, 2, 3, 4, 5]
        sig2 = [1, 2, 3, 4, 5]
        
        similarity = minhash.calculate_similarity(sig1, sig2)
        assert similarity == 1.0

    def test_calculate_similarity_different(self):
        """Test similarity calculation for different signatures."""
        minhash = MinHashLSH()
        sig1 = [1, 2, 3, 4, 5]
        sig2 = [6, 7, 8, 9, 10]
        
        similarity = minhash.calculate_similarity(sig1, sig2)
        assert similarity == 0.0

    def test_calculate_similarity_partial_match(self):
        """Test similarity calculation for partially matching signatures."""
        minhash = MinHashLSH()
        sig1 = [1, 2, 3, 4, 5]
        sig2 = [1, 2, 6, 7, 8]
        
        similarity = minhash.calculate_similarity(sig1, sig2)
        assert similarity == 0.4  # 2 out of 5 match

    def test_calculate_similarity_different_lengths(self):
        """Test similarity calculation for signatures of different lengths."""
        minhash = MinHashLSH()
        sig1 = [1, 2, 3]
        sig2 = [1, 2, 3, 4, 5]
        
        similarity = minhash.calculate_similarity(sig1, sig2)
        assert similarity == 0.0

    def test_minhash_signature_consistency(self):
        """Test that same content produces same signature."""
        minhash = MinHashLSH(num_perm=10)
        content = "Consistent content for testing"
        
        sig1 = minhash.generate_minhash_signature(content)
        sig2 = minhash.generate_minhash_signature(content)
        
        assert sig1 == sig2


class TestSimHashLSH:
    """Test the SimHashLSH class."""
    
    def test_simhash_initialization(self):
        """Test SimHashLSH initialization."""
        simhash = SimHashLSH(hash_bits=32, threshold=2)
        
        assert simhash.hash_bits == 32
        assert simhash.threshold == 2
        assert simhash.logger is not None

    def test_simhash_default_initialization(self):
        """Test SimHashLSH with default parameters."""
        simhash = SimHashLSH()
        
        assert simhash.hash_bits == 64
        assert simhash.threshold == 3

    def test_get_features_simple_text(self):
        """Test feature extraction from simple text."""
        simhash = SimHashLSH()
        content = "hello world test hello"
        features = simhash._get_features(content)
        
        assert features['hello'] == 2
        assert features['world'] == 1
        assert features['test'] == 1

    def test_get_features_filters_short_words(self):
        """Test that short words are filtered out."""
        simhash = SimHashLSH()
        content = "a an the hello world"
        features = simhash._get_features(content)
        
        assert 'a' not in features
        assert 'an' not in features
        assert 'hello' in features
        assert 'world' in features

    def test_get_features_case_insensitive(self):
        """Test that feature extraction is case insensitive."""
        simhash = SimHashLSH()
        content = "Hello HELLO hello"
        features = simhash._get_features(content)
        
        assert features['hello'] == 3

    def test_generate_simhash_signature(self):
        """Test SimHash signature generation."""
        simhash = SimHashLSH(hash_bits=32)
        content = "This is a test document for SimHash signature generation."
        
        signature = simhash.generate_simhash_signature(content)
        
        assert isinstance(signature, int)
        assert signature >= 0
        assert signature < (1 << 32)  # Within 32-bit range

    def test_generate_simhash_signature_consistency(self):
        """Test that same content produces same SimHash signature."""
        simhash = SimHashLSH()
        content = "Consistent content for SimHash testing"
        
        sig1 = simhash.generate_simhash_signature(content)
        sig2 = simhash.generate_simhash_signature(content)
        
        assert sig1 == sig2

    def test_calculate_hamming_distance_identical(self):
        """Test Hamming distance for identical hashes."""
        simhash = SimHashLSH()
        hash1 = 0b11001100
        hash2 = 0b11001100
        
        distance = simhash.calculate_hamming_distance(hash1, hash2)
        assert distance == 0

    def test_calculate_hamming_distance_different(self):
        """Test Hamming distance for different hashes."""
        simhash = SimHashLSH()
        hash1 = 0b11001100
        hash2 = 0b00110011
        
        distance = simhash.calculate_hamming_distance(hash1, hash2)
        assert distance == 8  # All bits different

    def test_calculate_hamming_distance_partial(self):
        """Test Hamming distance for partially different hashes."""
        simhash = SimHashLSH()
        hash1 = 0b11001100
        hash2 = 0b11000100
        
        distance = simhash.calculate_hamming_distance(hash1, hash2)
        assert distance == 1  # One bit different

    def test_is_similar_within_threshold(self):
        """Test similarity check within threshold."""
        simhash = SimHashLSH(threshold=3)
        hash1 = 0b11001100
        hash2 = 0b11000100  # Hamming distance = 1
        
        assert simhash.is_similar(hash1, hash2) is True

    def test_is_similar_outside_threshold(self):
        """Test similarity check outside threshold."""
        simhash = SimHashLSH(threshold=2)
        hash1 = 0b11001100
        hash2 = 0b00110011  # Hamming distance = 8
        
        assert simhash.is_similar(hash1, hash2) is False

    def test_is_similar_exact_threshold(self):
        """Test similarity check at exact threshold."""
        simhash = SimHashLSH(threshold=3)
        hash1 = 0b11001100
        hash2 = 0b00001100  # Hamming distance = 3
        
        assert simhash.is_similar(hash1, hash2) is True


class TestContentSimilarityManager:
    """Test the ContentSimilarityManager class."""
    
    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        mock_redis = AsyncMock()
        mock_redis.hset = AsyncMock()
        mock_redis.hget = AsyncMock()
        mock_redis.hgetall = AsyncMock()
        mock_redis.hlen = AsyncMock()
        mock_redis.hdel = AsyncMock()
        mock_redis.sadd = AsyncMock()
        mock_redis.smembers = AsyncMock()
        mock_redis.expire = AsyncMock()
        mock_redis.keys = AsyncMock()
        return mock_redis

    def test_content_similarity_manager_initialization(self, mock_redis):
        """Test ContentSimilarityManager initialization."""
        manager = ContentSimilarityManager(
            redis_client=mock_redis,
            minhash_threshold=0.9,
            simhash_threshold=2
        )
        
        assert manager.redis == mock_redis
        assert manager.minhash.threshold == 0.9
        assert manager.simhash.threshold == 2
        assert manager.fingerprints_key == "content:fingerprints"
        assert manager.minhash_buckets_key == "content:minhash_buckets"
        assert manager.simhash_buckets_key == "content:simhash_buckets"

    def test_content_similarity_manager_default_initialization(self, mock_redis):
        """Test ContentSimilarityManager with default parameters."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        assert manager.minhash.threshold == 0.8
        assert manager.simhash.threshold == 3

    @pytest.mark.asyncio
    async def test_generate_fingerprint(self, mock_redis):
        """Test fingerprint generation."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        url = "https://example.com/article"
        content = "This is test content for fingerprint generation."
        
        fingerprint = await manager.generate_fingerprint(url, content)
        
        assert isinstance(fingerprint, ContentFingerprint)
        assert fingerprint.url == url
        assert fingerprint.content_length == len(content)
        assert len(fingerprint.content_hash) == 64  # SHA-256 hex length
        assert len(fingerprint.minhash_signature) == 128  # Default num_perm
        assert isinstance(fingerprint.simhash_signature, int)
        assert fingerprint.timestamp > 0

    @pytest.mark.asyncio
    async def test_store_fingerprint(self, mock_redis):
        """Test fingerprint storage."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        fingerprint = ContentFingerprint(
            content_hash="test_hash",
            minhash_signature=[1, 2, 3, 4] * 32,  # 128 elements
            simhash_signature=123456789,
            url="https://test.com",
            timestamp=time.time(),
            content_length=100
        )
        
        await manager.store_fingerprint(fingerprint)
        
        # Verify Redis operations were called
        mock_redis.hset.assert_called()
        mock_redis.sadd.assert_called()
        mock_redis.expire.assert_called()

    @pytest.mark.asyncio
    async def test_store_minhash_buckets(self, mock_redis):
        """Test MinHash bucket storage."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        fingerprint = ContentFingerprint(
            content_hash="test_hash",
            minhash_signature=[1, 2, 3, 4] * 32,  # 128 elements
            simhash_signature=123456789,
            url="https://test.com",
            timestamp=time.time(),
            content_length=100
        )
        
        await manager._store_minhash_buckets(fingerprint)
        
        # Should create 16 bands (128 / 8 = 16 bands)
        assert mock_redis.sadd.call_count == 16
        assert mock_redis.expire.call_count == 16

    @pytest.mark.asyncio
    async def test_store_simhash_buckets(self, mock_redis):
        """Test SimHash bucket storage."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        fingerprint = ContentFingerprint(
            content_hash="test_hash",
            minhash_signature=[1] * 128,
            simhash_signature=123456789,
            url="https://test.com",
            timestamp=time.time(),
            content_length=100
        )
        
        await manager._store_simhash_buckets(fingerprint)
        
        # Should create 8 buckets (0, 8, 16, 24, 32, 40, 48, 56)
        assert mock_redis.sadd.call_count == 8
        assert mock_redis.expire.call_count == 8

    @pytest.mark.asyncio
    async def test_find_minhash_candidates(self, mock_redis):
        """Test MinHash candidate finding."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        fingerprint = ContentFingerprint(
            content_hash="test_hash",
            minhash_signature=[1, 2, 3, 4] * 32,  # 128 elements
            simhash_signature=123456789,
            url="https://test.com",
            timestamp=time.time(),
            content_length=100
        )
        
        # Mock Redis to return some candidates
        mock_redis.smembers.return_value = {b'candidate1', b'candidate2'}
        
        candidates = await manager._find_minhash_candidates(fingerprint)
        
        assert b'candidate1' in candidates
        assert b'candidate2' in candidates
        assert mock_redis.smembers.call_count == 16  # 16 bands

    @pytest.mark.asyncio
    async def test_find_simhash_candidates(self, mock_redis):
        """Test SimHash candidate finding."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        fingerprint = ContentFingerprint(
            content_hash="test_hash",
            minhash_signature=[1] * 128,
            simhash_signature=123456789,
            url="https://test.com",
            timestamp=time.time(),
            content_length=100
        )
        
        # Mock Redis to return some candidates
        mock_redis.smembers.return_value = {b'candidate3', b'candidate4'}
        
        candidates = await manager._find_simhash_candidates(fingerprint)
        
        assert b'candidate3' in candidates
        assert b'candidate4' in candidates
        assert mock_redis.smembers.call_count == 8  # 8 buckets

    @pytest.mark.asyncio
    async def test_find_similar_content_no_matches(self, mock_redis):
        """Test finding similar content with no matches."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        # Mock empty candidate sets
        mock_redis.smembers.return_value = set()
        
        content = "Unique content with no similar matches"
        result = await manager.find_similar_content(content)
        
        assert isinstance(result, SimilarityResult)
        assert result.is_duplicate is False
        assert result.similarity_score == 0.0
        assert result.similar_hashes == []
        assert len(result.content_hash) == 64  # SHA-256 hex

    @pytest.mark.asyncio
    async def test_find_similar_content_with_matches(self, mock_redis):
        """Test finding similar content with matches."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        # Mock candidate hash
        candidate_hash = "candidate_hash_123"
        mock_redis.smembers.return_value = {candidate_hash.encode()}
        
        # Mock fingerprint data
        candidate_fingerprint = ContentFingerprint(
            content_hash=candidate_hash,
            minhash_signature=[1] * 128,
            simhash_signature=123456789,
            url="https://candidate.com",
            timestamp=time.time(),
            content_length=50
        )
        mock_redis.hget.return_value = candidate_fingerprint.json().encode()
        
        content = "Test content for similarity detection"
        result = await manager.find_similar_content(content)
        
        assert isinstance(result, SimilarityResult)
        assert result.similarity_score >= 0.0
        assert len(result.content_hash) == 64

    @pytest.mark.asyncio
    async def test_is_duplicate_content_true(self, mock_redis):
        """Test duplicate content detection returning True."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        # Mock find_similar_content to return duplicate
        with patch.object(manager, 'find_similar_content') as mock_find:
            mock_find.return_value = SimilarityResult(
                similarity_score=0.9,
                is_duplicate=True,
                content_hash="test_hash",
                similar_hashes=["similar1"]
            )
            
            result = await manager.is_duplicate_content("test content")
            assert result is True

    @pytest.mark.asyncio
    async def test_is_duplicate_content_false(self, mock_redis):
        """Test duplicate content detection returning False."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        # Mock find_similar_content to return no duplicates
        with patch.object(manager, 'find_similar_content') as mock_find:
            mock_find.return_value = SimilarityResult(
                similarity_score=0.2,
                is_duplicate=False,
                content_hash="test_hash",
                similar_hashes=[]
            )
            
            result = await manager.is_duplicate_content("test content")
            assert result is False

    @pytest.mark.asyncio
    async def test_get_statistics(self, mock_redis):
        """Test getting similarity detection statistics."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        # Mock Redis responses
        mock_redis.hlen.return_value = 100
        mock_redis.keys.side_effect = [
            [b'bucket1', b'bucket2', b'bucket3'],  # MinHash buckets
            [b'bucket4', b'bucket5']                # SimHash buckets
        ]
        
        stats = await manager.get_statistics()
        
        assert stats['total_fingerprints'] == 100
        assert stats['minhash_buckets'] == 3
        assert stats['simhash_buckets'] == 2

    @pytest.mark.asyncio
    async def test_cleanup_expired_fingerprints(self, mock_redis):
        """Test cleaning up expired fingerprints."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        current_time = time.time()
        old_fingerprint = ContentFingerprint(
            content_hash="old_hash",
            minhash_signature=[1] * 128,
            simhash_signature=123,
            url="https://old.com",
            timestamp=current_time - (40 * 86400),  # 40 days old
            content_length=100
        )
        recent_fingerprint = ContentFingerprint(
            content_hash="recent_hash",
            minhash_signature=[2] * 128,
            simhash_signature=456,
            url="https://recent.com",
            timestamp=current_time - (10 * 86400),  # 10 days old
            content_length=200
        )
        
        # Mock Redis to return both fingerprints
        mock_redis.hgetall.return_value = {
            b'old_hash': old_fingerprint.json().encode(),
            b'recent_hash': recent_fingerprint.json().encode()
        }
        
        deleted_count = await manager.cleanup_expired_fingerprints(max_age_days=30)
        
        # Should delete the old one but keep the recent one
        assert deleted_count == 1
        mock_redis.hdel.assert_called_once_with(manager.fingerprints_key, b'old_hash')

    @pytest.mark.asyncio
    async def test_cleanup_invalid_fingerprints(self, mock_redis):
        """Test cleaning up invalid fingerprints."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        # Mock Redis to return invalid data
        mock_redis.hgetall.return_value = {
            b'invalid_hash': b'invalid_json_data'
        }
        
        deleted_count = await manager.cleanup_expired_fingerprints()
        
        # Should delete the invalid fingerprint
        assert deleted_count == 1
        mock_redis.hdel.assert_called_once_with(manager.fingerprints_key, b'invalid_hash')


class TestIntegrationScenarios:
    """Integration tests combining multiple components."""
    
    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client for integration tests."""
        mock_redis = AsyncMock()
        mock_redis.hset = AsyncMock()
        mock_redis.hget = AsyncMock()
        mock_redis.sadd = AsyncMock()
        mock_redis.smembers = AsyncMock()
        mock_redis.expire = AsyncMock()
        return mock_redis

    @pytest.mark.asyncio
    async def test_complete_similarity_workflow(self, mock_redis):
        """Test complete similarity detection workflow."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        # Original content
        content1 = "This is a test article about cybersecurity threats and malware analysis."
        url1 = "https://security.example.com/article1"
        
        # Generate and store fingerprint
        fingerprint1 = await manager.generate_fingerprint(url1, content1)
        await manager.store_fingerprint(fingerprint1)
        
        # Verify fingerprint was created correctly
        assert fingerprint1.url == url1
        assert fingerprint1.content_length == len(content1)
        assert len(fingerprint1.minhash_signature) == 128
        
        # Verify storage operations were called
        mock_redis.hset.assert_called()
        mock_redis.sadd.assert_called()

    @pytest.mark.asyncio
    async def test_similar_content_detection(self, mock_redis):
        """Test detection of similar content."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        # Slightly modified content (should be similar)
        content1 = "This is a test article about cybersecurity threats"
        content2 = "This is a test article about cyber security threats"
        
        # Generate fingerprints
        fp1 = await manager.generate_fingerprint("url1", content1)
        fp2 = await manager.generate_fingerprint("url2", content2)
        
        # Calculate similarity between the two
        minhash_sim = manager.minhash.calculate_similarity(
            fp1.minhash_signature, fp2.minhash_signature
        )
        simhash_sim = 1.0 - (manager.simhash.calculate_hamming_distance(
            fp1.simhash_signature, fp2.simhash_signature
        ) / 64.0)
        
        # Should be quite similar (adjust thresholds based on actual algorithm behavior)
        assert minhash_sim > 0.3  # MinHash is more sensitive to small changes
        assert simhash_sim > 0.3  # SimHash may also be affected by word differences

    @pytest.mark.asyncio
    async def test_different_content_detection(self, mock_redis):
        """Test detection of completely different content."""
        manager = ContentSimilarityManager(redis_client=mock_redis)
        
        # Completely different content
        content1 = "This is about cybersecurity and malware detection systems"
        content2 = "Recipe for chocolate cake with vanilla frosting ingredients"
        
        # Generate fingerprints
        fp1 = await manager.generate_fingerprint("url1", content1)
        fp2 = await manager.generate_fingerprint("url2", content2)
        
        # Calculate similarity
        minhash_sim = manager.minhash.calculate_similarity(
            fp1.minhash_signature, fp2.minhash_signature
        )
        simhash_sim = 1.0 - (manager.simhash.calculate_hamming_distance(
            fp1.simhash_signature, fp2.simhash_signature
        ) / 64.0)
        
        # Should be very different
        assert minhash_sim < 0.3
        assert simhash_sim < 0.7

    def test_minhash_simhash_consistency(self):
        """Test that MinHash and SimHash are deterministic."""
        minhash = MinHashLSH(num_perm=32)
        simhash = SimHashLSH(hash_bits=32)
        
        content = "Consistent test content for hashing algorithms"
        
        # Generate multiple times
        minhash_sig1 = minhash.generate_minhash_signature(content)
        minhash_sig2 = minhash.generate_minhash_signature(content)
        
        simhash_sig1 = simhash.generate_simhash_signature(content)
        simhash_sig2 = simhash.generate_simhash_signature(content)
        
        # Should be identical
        assert minhash_sig1 == minhash_sig2
        assert simhash_sig1 == simhash_sig2

    def test_hash_performance_characteristics(self):
        """Test performance characteristics of hash functions."""
        minhash = MinHashLSH(num_perm=128)
        simhash = SimHashLSH()
        
        # Test with different content sizes
        for size in [100, 1000, 5000]:
            content = "test content " * (size // 13)  # Approximate size
            
            # Should complete without errors
            minhash_sig = minhash.generate_minhash_signature(content)
            simhash_sig = simhash.generate_simhash_signature(content)
            
            assert len(minhash_sig) == 128
            assert isinstance(simhash_sig, int)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])