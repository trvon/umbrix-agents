import pytest
import time
from agents.common_tools.dead_link_detector import (
    DeadLinkDetector, DeadLinkConfig, DeadLinkInfo, DeadLinkStatus, 
    FailureType, FailureRecord, DeadLinkError
)
from agents.common_tools.dead_link_integration import dead_link_aware_retry, init_dead_link_detection


class TestDeadLinkDetector:
    
    def setup_method(self):
        """Set up test environment."""
        self.config = DeadLinkConfig(
            max_consecutive_failures=3,
            failure_window_hours=1,
            min_failures_for_suspicious=2,
            min_failures_for_quarantine=3,
            min_failures_for_dead=5
        )
        self.detector = DeadLinkDetector(self.config)
        
    @pytest.mark.asyncio
    async def test_new_url_is_usable(self):
        """Test that new URLs are considered usable."""
        should_use = await self.detector.should_use_url("https://example.com")
        assert should_use is True
        
    @pytest.mark.asyncio
    async def test_record_success(self):
        """Test recording successful requests."""
        url = "https://example.com"
        
        # Record success
        should_use = await self.detector.record_request_result(url, success=True)
        assert should_use is True
        
        # Check link info
        link_info = self.detector._get_link_info(url)
        assert link_info is not None
        assert link_info.status == DeadLinkStatus.HEALTHY
        assert link_info.success_count == 1
        assert link_info.consecutive_failures == 0
        assert link_info.last_success is not None
        
    @pytest.mark.asyncio
    async def test_record_failure_progression(self):
        """Test failure progression through statuses."""
        url = "https://example.com"
        
        # First failure - should remain healthy
        await self.detector.record_request_result(
            url, success=False, failure_type=FailureType.CONNECTION_TIMEOUT
        )
        link_info = self.detector._get_link_info(url)
        assert link_info.status == DeadLinkStatus.HEALTHY
        assert link_info.consecutive_failures == 1
        
        # Second failure - should become suspicious
        await self.detector.record_request_result(
            url, success=False, failure_type=FailureType.CONNECTION_TIMEOUT
        )
        link_info = self.detector._get_link_info(url)
        assert link_info.status == DeadLinkStatus.SUSPICIOUS
        assert link_info.consecutive_failures == 2
        
        # Third failure - should become quarantined
        await self.detector.record_request_result(
            url, success=False, failure_type=FailureType.CONNECTION_TIMEOUT
        )
        link_info = self.detector._get_link_info(url)
        assert link_info.status == DeadLinkStatus.QUARANTINED
        assert link_info.consecutive_failures == 3
        
    @pytest.mark.asyncio
    async def test_permanent_failure_fast_track_to_dead(self):
        """Test that permanent failures (404, DNS) fast-track to dead status."""
        url = "https://example.com"
        
        # Multiple 404 failures should lead to dead status faster
        for _ in range(5):
            await self.detector.record_request_result(
                url, success=False, failure_type=FailureType.HTTP_404, status_code=404
            )
            
        link_info = self.detector._get_link_info(url)
        assert link_info.status == DeadLinkStatus.DEAD
        
    @pytest.mark.asyncio
    async def test_health_score_updates(self):
        """Test health score calculation."""
        url = "https://example.com"
        
        # Start with success
        await self.detector.record_request_result(url, success=True)
        link_info = self.detector._get_link_info(url)
        initial_score = link_info.health_score
        
        # Add failure
        await self.detector.record_request_result(
            url, success=False, failure_type=FailureType.CONNECTION_TIMEOUT
        )
        link_info = self.detector._get_link_info(url)
        assert link_info.health_score < initial_score
        
        # Add success - should improve score
        await self.detector.record_request_result(url, success=True)
        link_info = self.detector._get_link_info(url)
        assert link_info.health_score > 0.5  # Should recover somewhat
        
    @pytest.mark.asyncio
    async def test_manual_override(self):
        """Test manual override functionality."""
        url = "https://example.com"
        
        # Make URL appear dead
        for _ in range(5):
            await self.detector.record_request_result(
                url, success=False, failure_type=FailureType.HTTP_404, status_code=404
            )
            
        # Should not be usable
        should_use = await self.detector.should_use_url(url)
        assert should_use is False
        
        # Apply manual override
        await self.detector.manually_override_url(url, enabled=True, notes="Test override")
        
        # Should now be usable
        should_use = await self.detector.should_use_url(url)
        assert should_use is True
        
        link_info = self.detector._get_link_info(url)
        assert link_info.manual_override is True
        assert link_info.notes == "Test override"
        
    @pytest.mark.asyncio
    async def test_quarantined_retry_scheduling(self):
        """Test retry scheduling for quarantined URLs."""
        url = "https://example.com"
        
        # Make URL quarantined
        for _ in range(3):
            await self.detector.record_request_result(
                url, success=False, failure_type=FailureType.CONNECTION_TIMEOUT
            )
            
        link_info = self.detector._get_link_info(url)
        assert link_info.status == DeadLinkStatus.QUARANTINED
        assert link_info.next_check_time is not None
        
        # Should not be usable before retry time
        should_use = await self.detector.should_use_url(url)
        assert should_use is False
        
        # Simulate time passing to next check time
        link_info.next_check_time = time.time() - 1  # 1 second ago
        self.detector._save_link_info(link_info)
        
        # Should now be usable for retry
        should_use = await self.detector.should_use_url(url)
        assert should_use is True
        
    @pytest.mark.asyncio
    async def test_dead_link_stats(self):
        """Test dead link statistics generation."""
        # Create URLs in different states
        urls = [
            "https://healthy.com",
            "https://suspicious.com", 
            "https://quarantined.com",
            "https://dead.com"
        ]
        
        # Healthy URL
        await self.detector.record_request_result(urls[0], success=True)
        
        # Suspicious URL
        for _ in range(2):
            await self.detector.record_request_result(
                urls[1], success=False, failure_type=FailureType.CONNECTION_TIMEOUT
            )
            
        # Quarantined URL
        for _ in range(3):
            await self.detector.record_request_result(
                urls[2], success=False, failure_type=FailureType.CONNECTION_TIMEOUT
            )
            
        # Dead URL
        for _ in range(5):
            await self.detector.record_request_result(
                urls[3], success=False, failure_type=FailureType.HTTP_404, status_code=404
            )
            
        stats = await self.detector.get_dead_link_stats()
        
        assert stats['total_links'] == 4
        assert stats['by_status']['healthy'] == 1
        assert stats['by_status']['suspicious'] == 1
        assert stats['by_status']['quarantined'] == 1
        assert stats['by_status']['dead'] == 1
        
    def test_failure_record_serialization(self):
        """Test failure record serialization/deserialization."""
        record = FailureRecord(
            timestamp=time.time(),
            failure_type=FailureType.HTTP_404,
            status_code=404,
            error_message="Not found",
            response_time_ms=1500.0
        )
        
        # Serialize
        data = record.to_dict()
        assert data['failure_type'] == 'http_404'
        assert data['status_code'] == 404
        
        # Deserialize
        restored = FailureRecord.from_dict(data)
        assert restored.failure_type == FailureType.HTTP_404
        assert restored.status_code == 404
        assert restored.error_message == "Not found"
        
    def test_dead_link_info_serialization(self):
        """Test dead link info serialization/deserialization."""
        current_time = time.time()
        link_info = DeadLinkInfo(
            url="https://example.com",
            status=DeadLinkStatus.QUARANTINED,
            first_seen=current_time,
            last_checked=current_time,
            failure_count=3,
            consecutive_failures=3,
            health_score=0.4
        )
        
        # Add failure record
        failure = FailureRecord(
            timestamp=current_time,
            failure_type=FailureType.CONNECTION_TIMEOUT,
            error_message="Timeout"
        )
        link_info.failure_history.append(failure)
        
        # Serialize
        data = link_info.to_dict()
        assert data['status'] == 'quarantined'
        assert data['failure_count'] == 3
        assert len(data['failure_history']) == 1
        
        # Deserialize
        restored = DeadLinkInfo.from_dict(data)
        assert restored.status == DeadLinkStatus.QUARANTINED
        assert restored.failure_count == 3
        assert len(restored.failure_history) == 1
        assert restored.failure_history[0].failure_type == FailureType.CONNECTION_TIMEOUT


class TestDeadLinkIntegration:
    
    def setup_method(self):
        """Set up test environment."""
        init_dead_link_detection()
        
    @pytest.mark.asyncio 
    async def test_dead_link_aware_decorator_success(self):
        """Test dead link aware decorator with successful requests."""
        @dead_link_aware_retry('default', 'test', 'operation')
        async def test_function(url: str):
            return f"Success for {url}"
            
        result = await test_function("https://example.com")
        assert result == "Success for https://example.com"
        
    @pytest.mark.asyncio
    async def test_dead_link_aware_decorator_blocks_dead_urls(self):
        """Test that decorator blocks requests to dead URLs."""
        detector = init_dead_link_detection()
        
        # Mark URL as dead
        url = "https://dead.com"
        await detector.manually_override_url(url, enabled=False)
        
        @dead_link_aware_retry('default', 'test', 'operation')
        async def test_function(url: str):
            return f"Success for {url}"
            
        # Should raise DeadLinkError
        with pytest.raises(DeadLinkError):
            await test_function(url)
            
    def test_url_extraction_from_args(self):
        """Test URL extraction from function arguments."""
        from agents.common_tools.dead_link_integration import DeadLinkAwareRetryDecorator
        from agents.common_tools.dead_link_detector import DeadLinkDetector, DeadLinkConfig
        
        detector = DeadLinkDetector(DeadLinkConfig())
        decorator = DeadLinkAwareRetryDecorator(detector)
        
        # Test positional args
        url = decorator._extract_url_from_args(["https://example.com", "other"], {})
        assert url == "https://example.com"
        
        # Test keyword args
        url = decorator._extract_url_from_args([], {"feed_url": "https://example.com"})
        assert url == "https://example.com"
        
        # Test no URL
        url = decorator._extract_url_from_args(["not_a_url"], {"param": "value"})
        assert url is None
        
    def test_error_classification(self):
        """Test error classification into failure types."""
        from agents.common_tools.dead_link_integration import DeadLinkAwareRetryDecorator
        from agents.common_tools.dead_link_detector import DeadLinkDetector, DeadLinkConfig
        import requests
        
        detector = DeadLinkDetector(DeadLinkConfig())
        decorator = DeadLinkAwareRetryDecorator(detector)
        
        # Mock response error
        class MockResponse:
            status_code = 404
            
        class MockHTTPError(Exception):
            def __init__(self):
                self.response = MockResponse()
                
        failure_type, status_code = decorator._classify_error(MockHTTPError())
        assert failure_type == FailureType.HTTP_404
        assert status_code == 404
        
        # Connection error
        failure_type, status_code = decorator._classify_error(ConnectionError("Connection failed"))
        assert failure_type == FailureType.CONNECTION_REFUSED
        assert status_code is None
        
        # Timeout error
        failure_type, status_code = decorator._classify_error(TimeoutError("Timeout"))
        assert failure_type == FailureType.CONNECTION_TIMEOUT
        assert status_code is None