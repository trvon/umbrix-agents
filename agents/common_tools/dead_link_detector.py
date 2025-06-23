from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set
from enum import Enum
import time
import logging
from datetime import datetime, timedelta
import json
from urllib.parse import urlparse


class FailureType(Enum):
    """Types of failures that can indicate dead links."""
    HTTP_404 = "http_404"
    HTTP_403 = "http_403"
    HTTP_410 = "http_410"  # Gone
    DNS_FAILURE = "dns_failure"
    CONNECTION_TIMEOUT = "connection_timeout"
    CONNECTION_REFUSED = "connection_refused"
    SSL_ERROR = "ssl_error"
    NETWORK_UNREACHABLE = "network_unreachable"
    INVALID_RESPONSE = "invalid_response"


class DeadLinkStatus(Enum):
    """Status of a link in the dead link detection system."""
    HEALTHY = "healthy"
    SUSPICIOUS = "suspicious"  # Some failures but not dead yet
    QUARANTINED = "quarantined"  # Likely dead, reduced checking
    DEAD = "dead"  # Confirmed dead, minimal checking
    MANUALLY_DISABLED = "manually_disabled"
    MANUALLY_ENABLED = "manually_enabled"


@dataclass
class FailureRecord:
    """Record of a single failure event."""
    timestamp: float
    failure_type: FailureType
    status_code: Optional[int] = None
    error_message: str = ""
    response_time_ms: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp,
            'failure_type': self.failure_type.value,
            'status_code': self.status_code,
            'error_message': self.error_message,
            'response_time_ms': self.response_time_ms
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FailureRecord':
        return cls(
            timestamp=data['timestamp'],
            failure_type=FailureType(data['failure_type']),
            status_code=data.get('status_code'),
            error_message=data.get('error_message', ''),
            response_time_ms=data.get('response_time_ms')
        )


@dataclass
class DeadLinkInfo:
    """Complete information about a potentially dead link."""
    url: str
    status: DeadLinkStatus
    first_seen: float
    last_checked: float
    last_success: Optional[float] = None
    failure_count: int = 0
    consecutive_failures: int = 0
    success_count: int = 0
    failure_history: List[FailureRecord] = field(default_factory=list)
    health_score: float = 1.0  # 0.0 = dead, 1.0 = healthy
    next_check_time: Optional[float] = None
    manual_override: Optional[bool] = None
    notes: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'url': self.url,
            'status': self.status.value,
            'first_seen': self.first_seen,
            'last_checked': self.last_checked,
            'last_success': self.last_success,
            'failure_count': self.failure_count,
            'consecutive_failures': self.consecutive_failures,
            'success_count': self.success_count,
            'failure_history': [f.to_dict() for f in self.failure_history[-10:]],  # Keep last 10
            'health_score': self.health_score,
            'next_check_time': self.next_check_time,
            'manual_override': self.manual_override,
            'notes': self.notes
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DeadLinkInfo':
        return cls(
            url=data['url'],
            status=DeadLinkStatus(data['status']),
            first_seen=data['first_seen'],
            last_checked=data['last_checked'],
            last_success=data.get('last_success'),
            failure_count=data['failure_count'],
            consecutive_failures=data['consecutive_failures'],
            success_count=data['success_count'],
            failure_history=[FailureRecord.from_dict(f) for f in data.get('failure_history', [])],
            health_score=data.get('health_score', 1.0),
            next_check_time=data.get('next_check_time'),
            manual_override=data.get('manual_override'),
            notes=data.get('notes', '')
        )


@dataclass
class DeadLinkConfig:
    """Configuration for dead link detection."""
    # Failure thresholds
    max_consecutive_failures: int = 5
    failure_window_hours: int = 24
    min_failures_for_suspicious: int = 3
    min_failures_for_quarantine: int = 5
    min_failures_for_dead: int = 10
    
    # Permanent failure indicators
    permanent_failure_codes: Set[int] = field(default_factory=lambda: {404, 410, 403})
    permanent_failure_types: Set[FailureType] = field(
        default_factory=lambda: {FailureType.HTTP_404, FailureType.HTTP_410, FailureType.DNS_FAILURE}
    )
    
    # Retry scheduling
    healthy_check_interval_hours: int = 24
    suspicious_check_interval_hours: int = 12
    quarantined_check_interval_hours: int = 24 * 7  # Weekly
    dead_check_interval_hours: int = 24 * 30  # Monthly
    
    # Health scoring
    success_score_increment: float = 0.1
    failure_score_decrement: float = 0.2
    permanent_failure_score_decrement: float = 0.5
    min_health_score: float = 0.0
    max_health_score: float = 1.0
    
    # History retention
    max_failure_history: int = 50
    history_retention_days: int = 90
    
    # Storage configuration (simplified - using in-memory for now)
    cache_ttl_seconds: int = 300


class DeadLinkDetector:
    """Main dead link detection and management system."""
    
    def __init__(self, config: DeadLinkConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # In-memory storage for this implementation
        self._link_data: Dict[str, DeadLinkInfo] = {}
        
    async def record_request_result(self, 
                                   url: str, 
                                   success: bool,
                                   failure_type: Optional[FailureType] = None,
                                   status_code: Optional[int] = None,
                                   error_message: str = "",
                                   response_time_ms: Optional[float] = None) -> bool:
        """
        Record the result of a request and update dead link status.
        Returns True if the URL should continue to be used, False if it should be avoided.
        """
        current_time = time.time()
        
        # Get or create link info
        link_info = self._get_link_info(url)
        if link_info is None:
            link_info = DeadLinkInfo(
                url=url,
                status=DeadLinkStatus.HEALTHY,
                first_seen=current_time,
                last_checked=current_time
            )
            
        # Update basic tracking
        link_info.last_checked = current_time
        
        if success:
            self._record_success(link_info)
        else:
            self._record_failure(link_info, failure_type, status_code, error_message, response_time_ms)
            
        # Update health score and status
        self._update_health_score(link_info)
        self._update_status(link_info)
        self._schedule_next_check(link_info)
        
        # Save updated info
        self._save_link_info(link_info)
        
        # Log significant status changes
        self._log_status_changes(link_info)
        
        # Return whether URL should be used
        return await self.should_use_url(url)
        
    async def should_use_url(self, url: str) -> bool:
        """
        Check if a URL should be used based on its dead link status.
        Returns True if URL should be used, False if it should be avoided.
        """
        link_info = self._get_link_info(url)
        if link_info is None:
            return True  # Unknown URLs are considered usable
            
        # Check manual overrides first
        if link_info.manual_override is not None:
            return link_info.manual_override
            
        # Check status
        if link_info.status in [DeadLinkStatus.DEAD, DeadLinkStatus.MANUALLY_DISABLED]:
            return False
        elif link_info.status == DeadLinkStatus.QUARANTINED:
            # For quarantined URLs, check if it's time for a retry
            return self._should_retry_quarantined(link_info)
        else:
            return True
            
    async def get_dead_link_stats(self) -> Dict[str, Any]:
        """Get statistics about dead links."""
        all_links = list(self._link_data.values())
        
        stats = {
            'total_links': len(all_links),
            'by_status': {},
            'health_distribution': {
                'healthy': 0,    # score > 0.8
                'degraded': 0,   # 0.5 < score <= 0.8
                'poor': 0,       # 0.2 < score <= 0.5
                'critical': 0    # score <= 0.2
            },
            'recent_failures': 0,
            'scheduled_for_retry': 0
        }
        
        current_time = time.time()
        last_24h = current_time - (24 * 3600)
        
        for link_info in all_links:
            # Count by status
            status = link_info.status.value
            stats['by_status'][status] = stats['by_status'].get(status, 0) + 1
            
            # Health distribution
            if link_info.health_score > 0.8:
                stats['health_distribution']['healthy'] += 1
            elif link_info.health_score > 0.5:
                stats['health_distribution']['degraded'] += 1
            elif link_info.health_score > 0.2:
                stats['health_distribution']['poor'] += 1
            else:
                stats['health_distribution']['critical'] += 1
                
            # Recent failures
            recent_failures = [f for f in link_info.failure_history if f.timestamp > last_24h]
            if recent_failures:
                stats['recent_failures'] += 1
                
            # Scheduled for retry
            if (link_info.next_check_time and 
                link_info.next_check_time <= current_time and 
                link_info.status in [DeadLinkStatus.QUARANTINED, DeadLinkStatus.DEAD]):
                stats['scheduled_for_retry'] += 1
                
        return stats
        
    async def manually_override_url(self, url: str, enabled: bool, notes: str = "") -> bool:
        """Manually override a URL's status."""
        link_info = self._get_link_info(url)
        if link_info is None:
            # Create new entry for manual override
            current_time = time.time()
            link_info = DeadLinkInfo(
                url=url,
                status=DeadLinkStatus.MANUALLY_ENABLED if enabled else DeadLinkStatus.MANUALLY_DISABLED,
                first_seen=current_time,
                last_checked=current_time
            )
        else:
            link_info.status = DeadLinkStatus.MANUALLY_ENABLED if enabled else DeadLinkStatus.MANUALLY_DISABLED
            
        link_info.manual_override = enabled
        link_info.notes = notes
        
        self._save_link_info(link_info)
        
        self.logger.info(f"Manual override applied to {url}: {'enabled' if enabled else 'disabled'} - {notes}")
        return True
        
    def _record_success(self, link_info: DeadLinkInfo):
        """Record a successful request."""
        current_time = time.time()
        link_info.last_success = current_time
        link_info.success_count += 1
        link_info.consecutive_failures = 0  # Reset consecutive failures
        
    def _record_failure(self, 
                        link_info: DeadLinkInfo,
                        failure_type: Optional[FailureType],
                        status_code: Optional[int],
                        error_message: str,
                        response_time_ms: Optional[float]):
        """Record a failed request."""
        current_time = time.time()
        
        # Create failure record
        failure_record = FailureRecord(
            timestamp=current_time,
            failure_type=failure_type or FailureType.INVALID_RESPONSE,
            status_code=status_code,
            error_message=error_message,
            response_time_ms=response_time_ms
        )
        
        # Add to history
        link_info.failure_history.append(failure_record)
        
        # Trim history if too long
        if len(link_info.failure_history) > self.config.max_failure_history:
            link_info.failure_history = link_info.failure_history[-self.config.max_failure_history:]
            
        # Update counts
        link_info.failure_count += 1
        link_info.consecutive_failures += 1
        
    def _update_health_score(self, link_info: DeadLinkInfo):
        """Update health score based on recent performance."""
        # Start with current score
        score = link_info.health_score
        
        # Get recent failures (last 24 hours)
        current_time = time.time()
        recent_window = current_time - (24 * 3600)
        recent_failures = [f for f in link_info.failure_history if f.timestamp > recent_window]
        
        # Adjust score based on recent activity
        if link_info.last_success and link_info.last_success > recent_window:
            # Recent success - improve score
            score += self.config.success_score_increment
        
        # Penalize for recent failures
        for failure in recent_failures:
            if failure.failure_type in self.config.permanent_failure_types:
                score -= self.config.permanent_failure_score_decrement
            else:
                score -= self.config.failure_score_decrement
                
        # Clamp score to valid range
        score = max(self.config.min_health_score, min(score, self.config.max_health_score))
        link_info.health_score = score
        
    def _update_status(self, link_info: DeadLinkInfo):
        """Update status based on failure patterns and health score."""
        # Don't change manual overrides
        if link_info.manual_override is not None:
            return
            
        current_time = time.time()
        failure_window = current_time - (self.config.failure_window_hours * 3600)
        
        # Count recent failures
        recent_failures = [f for f in link_info.failure_history if f.timestamp > failure_window]
        recent_permanent_failures = [
            f for f in recent_failures 
            if (f.failure_type in self.config.permanent_failure_types or
                f.status_code in self.config.permanent_failure_codes)
        ]
        
        old_status = link_info.status
        
        # Determine new status
        if len(recent_permanent_failures) >= 1 and link_info.consecutive_failures >= self.config.min_failures_for_dead:
            link_info.status = DeadLinkStatus.DEAD
        elif link_info.consecutive_failures >= self.config.min_failures_for_quarantine:
            link_info.status = DeadLinkStatus.QUARANTINED
        elif link_info.consecutive_failures >= self.config.min_failures_for_suspicious:
            link_info.status = DeadLinkStatus.SUSPICIOUS
        elif link_info.health_score > 0.8 and link_info.consecutive_failures == 0:
            link_info.status = DeadLinkStatus.HEALTHY
            
        # Log status changes
        if old_status != link_info.status:
            self.logger.info(f"Status changed for {link_info.url}: {old_status.value} -> {link_info.status.value}")
            
    def _schedule_next_check(self, link_info: DeadLinkInfo):
        """Schedule the next check time based on current status."""
        current_time = time.time()
        
        if link_info.status == DeadLinkStatus.HEALTHY:
            interval_hours = self.config.healthy_check_interval_hours
        elif link_info.status == DeadLinkStatus.SUSPICIOUS:
            interval_hours = self.config.suspicious_check_interval_hours
        elif link_info.status == DeadLinkStatus.QUARANTINED:
            interval_hours = self.config.quarantined_check_interval_hours
        elif link_info.status == DeadLinkStatus.DEAD:
            interval_hours = self.config.dead_check_interval_hours
        else:
            interval_hours = self.config.healthy_check_interval_hours
            
        link_info.next_check_time = current_time + (interval_hours * 3600)
        
    def _should_retry_quarantined(self, link_info: DeadLinkInfo) -> bool:
        """Check if a quarantined URL should be retried."""
        if link_info.next_check_time is None:
            return False
            
        current_time = time.time()
        return current_time >= link_info.next_check_time
        
    def _get_link_info(self, url: str) -> Optional[DeadLinkInfo]:
        """Get link info from storage."""
        return self._link_data.get(url)
                
    def _save_link_info(self, link_info: DeadLinkInfo):
        """Save link info to storage."""
        self._link_data[link_info.url] = link_info
            
    def _log_status_changes(self, link_info: DeadLinkInfo):
        """Log significant status changes for monitoring."""
        # This would integrate with your logging system
        pass


class DeadLinkError(Exception):
    """Raised when attempting to access a known dead link."""
    pass