import functools
import logging
from typing import Optional, Callable, Any
from urllib.parse import urlparse
import requests
import aiohttp

from .dead_link_detector import DeadLinkDetector, DeadLinkError, FailureType
from .retry_framework import retry_with_policy


class DeadLinkAwareRetryDecorator:
    """Retry decorator that integrates with dead link detection."""
    
    def __init__(self, dead_link_detector: DeadLinkDetector):
        self.dead_link_detector = dead_link_detector
        self.logger = logging.getLogger(__name__)
        
    def create_decorator(self, policy_name: str, agent_type: str = 'default', operation: str = 'default'):
        """Create a retry decorator that checks dead link status."""
        
        def decorator(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                # Extract URL from arguments
                url = self._extract_url_from_args(args, kwargs)
                
                if url:
                    # Check if URL should be used
                    should_use = await self.dead_link_detector.should_use_url(url)
                    if not should_use:
                        self.logger.warning(f"Skipping request to dead link: {url}")
                        raise DeadLinkError(f"URL marked as dead: {url}")
                
                # Apply standard retry decorator
                retry_decorator = retry_with_policy(policy_name, agent_type, operation)
                decorated_func = retry_decorator(func)
                
                try:
                    result = await decorated_func(*args, **kwargs)
                    
                    # Record success
                    if url:
                        await self.dead_link_detector.record_request_result(
                            url, success=True
                        )
                        
                    return result
                    
                except Exception as e:
                    # Record failure
                    if url:
                        failure_type, status_code = self._classify_error(e)
                        await self.dead_link_detector.record_request_result(
                            url, 
                            success=False,
                            failure_type=failure_type,
                            status_code=status_code,
                            error_message=str(e)
                        )
                    raise
                    
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                # Extract URL from arguments
                url = self._extract_url_from_args(args, kwargs)
                
                if url:
                    # For sync functions, we'll use simplified checking
                    link_info = self.dead_link_detector._get_link_info(url)
                    if (link_info and 
                        link_info.status in ['dead', 'manually_disabled'] and
                        link_info.manual_override is False):
                        self.logger.warning(f"Skipping request to dead link: {url}")
                        raise DeadLinkError(f"URL marked as dead: {url}")
                
                # Apply standard retry decorator
                retry_decorator = retry_with_policy(policy_name, agent_type, operation)
                decorated_func = retry_decorator(func)
                
                try:
                    result = decorated_func(*args, **kwargs)
                    
                    # Record success (simplified for sync)
                    if url:
                        import asyncio
                        try:
                            loop = asyncio.get_event_loop()
                            loop.create_task(
                                self.dead_link_detector.record_request_result(url, success=True)
                            )
                        except RuntimeError:
                            # No event loop, skip async recording
                            pass
                        
                    return result
                    
                except Exception as e:
                    # Record failure (simplified for sync)
                    if url:
                        failure_type, status_code = self._classify_error(e)
                        import asyncio
                        try:
                            loop = asyncio.get_event_loop()
                            loop.create_task(
                                self.dead_link_detector.record_request_result(
                                    url, 
                                    success=False,
                                    failure_type=failure_type,
                                    status_code=status_code,
                                    error_message=str(e)
                                )
                            )
                        except RuntimeError:
                            # No event loop, skip async recording
                            pass
                    raise
                    
            # Return appropriate wrapper based on function type
            if hasattr(func, '__code__') and func.__code__.co_flags & 0x80:  # CO_COROUTINE
                return async_wrapper
            else:
                return sync_wrapper
                
        return decorator
        
    def _extract_url_from_args(self, args, kwargs) -> Optional[str]:
        """Extract URL from function arguments."""
        # Check common parameter names
        url_params = ['url', 'feed_url', 'endpoint', 'uri']
        
        # Check kwargs first
        for param in url_params:
            if param in kwargs and isinstance(kwargs[param], str):
                value = kwargs[param]
                if value.startswith(('http://', 'https://')):
                    return value
        
        # Check positional args
        for arg in args:
            if isinstance(arg, str) and arg.startswith(('http://', 'https://')):
                return arg
                
        return None
        
    def _classify_error(self, error: Exception) -> tuple[Optional[FailureType], Optional[int]]:
        """Classify error into failure type and status code."""
        status_code = None
        
        # HTTP errors (requests library)
        if hasattr(error, 'response') and hasattr(error.response, 'status_code'):
            status_code = error.response.status_code
            if status_code == 404:
                return FailureType.HTTP_404, status_code
            elif status_code == 403:
                return FailureType.HTTP_403, status_code
            elif status_code == 410:
                return FailureType.HTTP_410, status_code
                
        # aiohttp errors
        if isinstance(error, aiohttp.ClientResponseError):
            status_code = error.status
            if status_code == 404:
                return FailureType.HTTP_404, status_code
            elif status_code == 403:
                return FailureType.HTTP_403, status_code
            elif status_code == 410:
                return FailureType.HTTP_410, status_code
        
        # Connection errors
        if isinstance(error, (ConnectionError, aiohttp.ClientConnectionError)):
            return FailureType.CONNECTION_REFUSED, status_code
        elif isinstance(error, (TimeoutError, aiohttp.ServerTimeoutError)):
            return FailureType.CONNECTION_TIMEOUT, status_code
        elif 'SSL' in str(type(error).__name__):
            return FailureType.SSL_ERROR, status_code
        elif 'DNS' in str(error) or 'Name or service not known' in str(error):
            return FailureType.DNS_FAILURE, status_code
        
        return FailureType.INVALID_RESPONSE, status_code


# Global instance
_dead_link_detector = None
_dead_link_decorator = None


def init_dead_link_detection(config=None):
    """Initialize global dead link detection."""
    global _dead_link_detector, _dead_link_decorator
    
    from .dead_link_detector import DeadLinkConfig
    
    if config is None:
        config = DeadLinkConfig()
    
    _dead_link_detector = DeadLinkDetector(config)
    _dead_link_decorator = DeadLinkAwareRetryDecorator(_dead_link_detector)
    
    return _dead_link_detector


def get_dead_link_detector():
    """Get global dead link detector instance."""
    if _dead_link_detector is None:
        init_dead_link_detection()
    return _dead_link_detector


def dead_link_aware_retry(policy_name: str, agent_type: str = 'default', operation: str = 'default'):
    """Convenience decorator for dead link aware retry."""
    if _dead_link_decorator is None:
        init_dead_link_detection()
    
    return _dead_link_decorator.create_decorator(policy_name, agent_type, operation)


# Update existing feed tools to use dead link detection
def enhance_with_dead_link_detection():
    """Enhance existing tools with dead link detection."""
    try:
        # Initialize if not already done
        if _dead_link_detector is None:
            init_dead_link_detection()
            
        # This would integrate with existing feed tools
        # For now, we'll just ensure initialization
        logging.getLogger(__name__).info("Dead link detection initialized and ready")
        
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to enhance with dead link detection: {e}")


# Auto-initialize when module is imported
enhance_with_dead_link_detection()