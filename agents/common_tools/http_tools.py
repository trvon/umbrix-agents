"""
HTTP utilities for Umbrix agents.

This module provides enhanced HTTP client functionality including
retry mechanisms, rate limiting, session management, and common
patterns used across agents for external API interactions.
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import ssl

import aiohttp
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .logging import setup_logging
from .redis_tools import RateLimiter, EnhancedRedisClient


@dataclass
class RequestConfig:
    """Configuration for HTTP requests."""
    timeout: float = 30.0
    max_retries: int = 3
    backoff_factor: float = 1.0
    retry_on_status: List[int] = None
    headers: Optional[Dict[str, str]] = None
    verify_ssl: bool = True
    follow_redirects: bool = True
    max_redirects: int = 10
    
    def __post_init__(self):
        if self.retry_on_status is None:
            self.retry_on_status = [429, 500, 502, 503, 504]


@dataclass
class ResponseInfo:
    """Information about an HTTP response."""
    status_code: int
    headers: Dict[str, str]
    content: Union[str, bytes, Dict[str, Any]]
    url: str
    elapsed_time: float
    attempt_count: int = 1
    from_cache: bool = False


class HttpClientError(Exception):
    """Custom exception for HTTP client errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[ResponseInfo] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class EnhancedHttpClient:
    """
    Enhanced HTTP client with retry, rate limiting, and session management.
    
    Provides both async and sync interfaces with common patterns
    for external API interactions used across agents.
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        default_headers: Optional[Dict[str, str]] = None,
        rate_limiter: Optional[RateLimiter] = None,
        user_agent: str = "Umbrix-Agent/1.0",
        **config_kwargs
    ):
        self.base_url = base_url
        self.rate_limiter = rate_limiter
        self.logger = setup_logging("http_client")
        
        # Default configuration
        self.config = RequestConfig(**config_kwargs)
        
        # Default headers
        self.default_headers = {
            'User-Agent': user_agent,
            'Accept': 'application/json, text/html, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        if default_headers:
            self.default_headers.update(default_headers)
        
        # Session management
        self._sync_session = None
        self._async_session = None
        
        # Response cache
        self._response_cache = {}
        self._cache_ttl = 300  # 5 minutes default
        
        # Statistics
        self.stats = {
            'requests_made': 0,
            'requests_successful': 0,
            'requests_failed': 0,
            'total_retry_attempts': 0,
            'cache_hits': 0,
            'rate_limit_blocks': 0
        }
    
    def _get_sync_session(self) -> requests.Session:
        """Get or create sync requests session."""
        if self._sync_session is None:
            self._sync_session = requests.Session()
            
            # Configure retry strategy
            retry_strategy = Retry(
                total=self.config.max_retries,
                backoff_factor=self.config.backoff_factor,
                status_forcelist=self.config.retry_on_status,
                allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"]
            )
            
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self._sync_session.mount("http://", adapter)
            self._sync_session.mount("https://", adapter)
            
            # Set default headers
            self._sync_session.headers.update(self.default_headers)
        
        return self._sync_session
    
    async def _get_async_session(self) -> aiohttp.ClientSession:
        """Get or create async aiohttp session."""
        if self._async_session is None:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            
            # Configure SSL context
            ssl_context = ssl.create_default_context() if self.config.verify_ssl else False
            
            connector = aiohttp.TCPConnector(
                ssl=ssl_context,
                limit=100,  # Connection pool size
                limit_per_host=30,
                ttl_dns_cache=300,
                use_dns_cache=True,
            )
            
            self._async_session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self.default_headers,
                auto_decompress=True
            )
        
        return self._async_session
    
    def _build_url(self, url: str) -> str:
        """Build full URL from base URL and path."""
        if self.base_url and not url.startswith(('http://', 'https://')):
            return urljoin(self.base_url, url)
        return url
    
    def _merge_headers(self, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
        """Merge request headers with defaults."""
        merged = self.default_headers.copy()
        if self.config.headers:
            merged.update(self.config.headers)
        if headers:
            merged.update(headers)
        return merged
    
    def _get_cache_key(self, method: str, url: str, params: Optional[Dict] = None) -> str:
        """Generate cache key for request."""
        import hashlib
        key_data = f"{method}:{url}:{json.dumps(params or {}, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _is_cacheable(self, method: str, status_code: int) -> bool:
        """Check if response should be cached."""
        return method.upper() == 'GET' and 200 <= status_code < 300
    
    def _get_cached_response(self, cache_key: str) -> Optional[ResponseInfo]:
        """Get cached response if available and not expired."""
        if cache_key in self._response_cache:
            cached_data, timestamp = self._response_cache[cache_key]
            if time.time() - timestamp < self._cache_ttl:
                self.stats['cache_hits'] += 1
                cached_data.from_cache = True
                return cached_data
            else:
                # Remove expired cache entry
                del self._response_cache[cache_key]
        return None
    
    def _cache_response(self, cache_key: str, response: ResponseInfo):
        """Cache response for future use."""
        self._response_cache[cache_key] = (response, time.time())
    
    async def _check_rate_limit(self, identifier: str = "default"):
        """Check rate limit before making request."""
        if self.rate_limiter:
            allowed, info = await self.rate_limiter.is_allowed_async(identifier)
            if not allowed:
                self.stats['rate_limit_blocks'] += 1
                wait_time = info.get('reset_time', time.time()) - time.time()
                if wait_time > 0:
                    self.logger.warning(f"Rate limit exceeded, waiting {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
                else:
                    raise HttpClientError(f"Rate limit exceeded: {info}")
    
    # Async methods
    async def request_async(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Union[str, bytes, Dict[str, Any]]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
        rate_limit_id: str = "default",
        **kwargs
    ) -> ResponseInfo:
        """Make an async HTTP request with full feature support."""
        full_url = self._build_url(url)
        merged_headers = self._merge_headers(headers)
        
        # Check cache first for GET requests
        cache_key = None
        if use_cache and method.upper() == 'GET':
            cache_key = self._get_cache_key(method, full_url, params)
            cached_response = self._get_cached_response(cache_key)
            if cached_response:
                return cached_response
        
        # Check rate limit
        await self._check_rate_limit(rate_limit_id)
        
        session = await self._get_async_session()
        
        # Prepare request data
        request_kwargs = {
            'params': params,
            'headers': merged_headers,
            **kwargs
        }
        
        if json_data is not None:
            request_kwargs['json'] = json_data
        elif data is not None:
            request_kwargs['data'] = data
        
        # Make request with retry logic
        last_exception = None
        for attempt in range(self.config.max_retries + 1):
            try:
                self.stats['requests_made'] += 1
                if attempt > 0:
                    self.stats['total_retry_attempts'] += 1
                
                start_time = time.time()
                
                async with session.request(method, full_url, **request_kwargs) as response:
                    elapsed_time = time.time() - start_time
                    
                    # Read response content
                    content_type = response.headers.get('content-type', '').lower()
                    
                    if 'application/json' in content_type:
                        try:
                            content = await response.json()
                        except (json.JSONDecodeError, aiohttp.ContentTypeError):
                            content = await response.text()
                    else:
                        content = await response.text()
                    
                    response_info = ResponseInfo(
                        status_code=response.status,
                        headers=dict(response.headers),
                        content=content,
                        url=str(response.url),
                        elapsed_time=elapsed_time,
                        attempt_count=attempt + 1
                    )
                    
                    # Check if request was successful
                    if response.status < 400:
                        self.stats['requests_successful'] += 1
                        
                        # Cache successful GET responses
                        if cache_key and self._is_cacheable(method, response.status):
                            self._cache_response(cache_key, response_info)
                        
                        return response_info
                    
                    # Check if we should retry
                    if response.status in self.config.retry_on_status and attempt < self.config.max_retries:
                        wait_time = self.config.backoff_factor * (2 ** attempt)
                        self.logger.warning(
                            f"Request failed with status {response.status}, retrying in {wait_time}s "
                            f"(attempt {attempt + 1}/{self.config.max_retries + 1})"
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    
                    # No more retries, return the error response
                    self.stats['requests_failed'] += 1
                    return response_info
                    
            except asyncio.TimeoutError as e:
                last_exception = e
                if attempt < self.config.max_retries:
                    wait_time = self.config.backoff_factor * (2 ** attempt)
                    self.logger.warning(f"Request timeout, retrying in {wait_time}s")
                    await asyncio.sleep(wait_time)
                    continue
                
            except Exception as e:
                last_exception = e
                if attempt < self.config.max_retries:
                    wait_time = self.config.backoff_factor * (2 ** attempt)
                    self.logger.warning(f"Request error: {e}, retrying in {wait_time}s")
                    await asyncio.sleep(wait_time)
                    continue
        
        # All retries failed
        self.stats['requests_failed'] += 1
        raise HttpClientError(f"Request failed after {self.config.max_retries + 1} attempts") from last_exception
    
    async def get_async(self, url: str, **kwargs) -> ResponseInfo:
        """Make an async GET request."""
        return await self.request_async('GET', url, **kwargs)
    
    async def post_async(self, url: str, **kwargs) -> ResponseInfo:
        """Make an async POST request."""
        return await self.request_async('POST', url, **kwargs)
    
    async def put_async(self, url: str, **kwargs) -> ResponseInfo:
        """Make an async PUT request."""
        return await self.request_async('PUT', url, **kwargs)
    
    async def delete_async(self, url: str, **kwargs) -> ResponseInfo:
        """Make an async DELETE request."""
        return await self.request_async('DELETE', url, **kwargs)
    
    # Sync methods
    def request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Union[str, bytes, Dict[str, Any]]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
        **kwargs
    ) -> ResponseInfo:
        """Make a sync HTTP request with retry support."""
        full_url = self._build_url(url)
        merged_headers = self._merge_headers(headers)
        
        # Check cache first for GET requests
        cache_key = None
        if use_cache and method.upper() == 'GET':
            cache_key = self._get_cache_key(method, full_url, params)
            cached_response = self._get_cached_response(cache_key)
            if cached_response:
                return cached_response
        
        session = self._get_sync_session()
        
        # Prepare request data
        request_kwargs = {
            'params': params,
            'headers': merged_headers,
            'timeout': self.config.timeout,
            'verify': self.config.verify_ssl,
            'allow_redirects': self.config.follow_redirects,
            **kwargs
        }
        
        if json_data is not None:
            request_kwargs['json'] = json_data
        elif data is not None:
            request_kwargs['data'] = data
        
        # Make request (retries handled by requests adapter)
        try:
            self.stats['requests_made'] += 1
            start_time = time.time()
            
            response = session.request(method, full_url, **request_kwargs)
            elapsed_time = time.time() - start_time
            
            # Parse response content
            content_type = response.headers.get('content-type', '').lower()
            
            if 'application/json' in content_type:
                try:
                    content = response.json()
                except (json.JSONDecodeError, ValueError):
                    content = response.text
            else:
                content = response.text
            
            response_info = ResponseInfo(
                status_code=response.status_code,
                headers=dict(response.headers),
                content=content,
                url=response.url,
                elapsed_time=elapsed_time
            )
            
            if response.status_code < 400:
                self.stats['requests_successful'] += 1
                
                # Cache successful GET responses
                if cache_key and self._is_cacheable(method, response.status_code):
                    self._cache_response(cache_key, response_info)
            else:
                self.stats['requests_failed'] += 1
            
            return response_info
            
        except requests.exceptions.RequestException as e:
            self.stats['requests_failed'] += 1
            raise HttpClientError(f"Request failed: {e}") from e
    
    def get(self, url: str, **kwargs) -> ResponseInfo:
        """Make a sync GET request."""
        return self.request('GET', url, **kwargs)
    
    def post(self, url: str, **kwargs) -> ResponseInfo:
        """Make a sync POST request."""
        return self.request('POST', url, **kwargs)
    
    def put(self, url: str, **kwargs) -> ResponseInfo:
        """Make a sync PUT request."""
        return self.request('PUT', url, **kwargs)
    
    def delete(self, url: str, **kwargs) -> ResponseInfo:
        """Make a sync DELETE request."""
        return self.request('DELETE', url, **kwargs)
    
    # Utility methods
    async def health_check_async(self, url: str, expected_status: int = 200) -> bool:
        """Perform an async health check on a URL."""
        try:
            response = await self.get_async(url, use_cache=False)
            return response.status_code == expected_status
        except Exception as e:
            self.logger.error(f"Health check failed for {url}: {e}")
            return False
    
    def health_check(self, url: str, expected_status: int = 200) -> bool:
        """Perform a sync health check on a URL."""
        try:
            response = self.get(url, use_cache=False)
            return response.status_code == expected_status
        except Exception as e:
            self.logger.error(f"Health check failed for {url}: {e}")
            return False
    
    async def download_file_async(
        self,
        url: str,
        file_path: str,
        chunk_size: int = 8192,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> bool:
        """Download a file asynchronously with progress tracking."""
        try:
            session = await self._get_async_session()
            
            async with session.get(url) as response:
                if response.status != 200:
                    self.logger.error(f"Failed to download file: HTTP {response.status}")
                    return False
                
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                with open(file_path, 'wb') as file:
                    async for chunk in response.content.iter_chunked(chunk_size):
                        file.write(chunk)
                        downloaded += len(chunk)
                        
                        if progress_callback:
                            progress_callback(downloaded, total_size)
                
                self.logger.info(f"Successfully downloaded {url} to {file_path}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error downloading file from {url}: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        total_requests = self.stats['requests_made']
        success_rate = (self.stats['requests_successful'] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            **self.stats,
            'success_rate': success_rate,
            'cache_size': len(self._response_cache),
            'average_retries': (self.stats['total_retry_attempts'] / total_requests) if total_requests > 0 else 0
        }
    
    def clear_cache(self):
        """Clear the response cache."""
        self._response_cache.clear()
        self.logger.info("Response cache cleared")
    
    async def close_async(self):
        """Close async session and cleanup resources."""
        if self._async_session:
            await self._async_session.close()
            self._async_session = None
    
    def close(self):
        """Close sync session and cleanup resources."""
        if self._sync_session:
            self._sync_session.close()
            self._sync_session = None


class ApiClient(EnhancedHttpClient):
    """
    Specialized HTTP client for API interactions.
    
    Provides additional features like automatic authentication,
    API key management, and structured error handling.
    """
    
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        auth_header: str = "Authorization",
        auth_prefix: str = "Bearer",
        **kwargs
    ):
        self.api_key = api_key
        self.auth_header = auth_header
        self.auth_prefix = auth_prefix
        
        # Add authentication headers if API key provided
        default_headers = kwargs.get('default_headers', {})
        if api_key:
            default_headers[auth_header] = f"{auth_prefix} {api_key}"
        
        kwargs['default_headers'] = default_headers
        super().__init__(base_url=base_url, **kwargs)
    
    def set_api_key(self, api_key: str):
        """Update the API key for authentication."""
        self.api_key = api_key
        self.default_headers[self.auth_header] = f"{self.auth_prefix} {api_key}"
        
        # Update existing sessions
        if self._sync_session:
            self._sync_session.headers[self.auth_header] = f"{self.auth_prefix} {api_key}"
        
        if self._async_session:
            self._async_session.headers[self.auth_header] = f"{self.auth_prefix} {api_key}"
    
    async def paginated_request_async(
        self,
        url: str,
        page_param: str = 'page',
        size_param: str = 'limit',
        page_size: int = 100,
        max_pages: Optional[int] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Make paginated API requests asynchronously."""
        all_results = []
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                break
            
            params = kwargs.get('params', {}).copy()
            params[page_param] = page
            params[size_param] = page_size
            
            kwargs['params'] = params
            response = await self.get_async(url, **kwargs)
            
            if response.status_code != 200:
                self.logger.error(f"Paginated request failed: HTTP {response.status_code}")
                break
            
            # Extract results based on common pagination patterns
            data = response.content
            if isinstance(data, dict):
                # Common patterns: data.results, data.items, data.data
                results = data.get('results') or data.get('items') or data.get('data') or data
                if isinstance(results, list):
                    all_results.extend(results)
                    
                    # Check if there are more pages
                    if (len(results) < page_size or 
                        data.get('has_next') is False or
                        data.get('next') is None):
                        break
                else:
                    # Single item response
                    all_results.append(data)
                    break
            elif isinstance(data, list):
                all_results.extend(data)
                if len(data) < page_size:
                    break
            else:
                break
            
            page += 1
        
        return all_results