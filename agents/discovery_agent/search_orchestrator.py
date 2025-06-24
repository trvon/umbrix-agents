"""
SearchOrchestrator - Multi-provider search coordination for CTI source discovery.

This module orchestrates searches across multiple search providers to discover
relevant CTI sources based on generated queries.
"""

import asyncio
import aiohttp
import logging
import time
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass, field
from urllib.parse import quote_plus
import os
from functools import wraps
import random

# Import Google ADK tools for built-in search
try:
    from google.adk.tools import google_search
    GOOGLE_ADK_AVAILABLE = True
except ImportError:
    GOOGLE_ADK_AVAILABLE = False
# from common_tools.retry_framework import retry_with_policy
# Temporary simple retry decorator
def retry_with_policy(max_attempts=3, delay=1):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    await asyncio.sleep(delay * (attempt + 1))
            return await func(*args, **kwargs)
        return wrapper
    return decorator


@dataclass
class SearchResult:
    """Represents a search result from a provider."""
    title: str
    url: str
    snippet: str
    provider: str
    relevance_score: float = 0.0


@dataclass
class SearchProvider:
    """Configuration for a search provider."""
    name: str
    base_url: str
    api_key: Optional[str] = None
    rate_limit: int = 100  # requests per hour
    enabled: bool = True
    last_request_time: float = field(default_factory=time.time)
    request_count: int = 0
    request_window_start: float = field(default_factory=time.time)


class SearchOrchestrator:
    """
    Orchestrates searches across multiple search providers.
    
    Supports:
    - Brave Search API
    - Google Custom Search Engine (CSE)
    - DuckDuckGo (via unofficial API)
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Initialize search providers
        self.providers = {
            "brave": SearchProvider(
                name="brave",
                base_url="https://api.search.brave.com/res/v1/web/search",
                api_key=os.getenv("BRAVE_SEARCH_API_KEY"),
                enabled=bool(os.getenv("BRAVE_SEARCH_API_KEY"))
            ),
            "google_cse": SearchProvider(
                name="google_cse",
                base_url="https://www.googleapis.com/customsearch/v1",
                api_key=os.getenv("GOOGLE_CSE_API_KEY"),
                enabled=bool(os.getenv("GOOGLE_CSE_API_KEY"))
            ),
            "duckduckgo": SearchProvider(
                name="duckduckgo",
                base_url="https://api.duckduckgo.com",
                enabled=True  # No API key required
            )
        }
        
        # Track enabled providers
        self.enabled_providers = [p for p in self.providers.values() if p.enabled]
        self.logger.info(f"Initialized with {len(self.enabled_providers)} enabled search providers")
        
        # Rate limiting configuration
        self.rate_limit_window = 3600  # 1 hour in seconds
        self.max_retries = 3
        self.base_delay = 1.0
        self.max_delay = 30.0
    
    def _check_rate_limit(self, provider: SearchProvider) -> bool:
        """Check if provider is within rate limits."""
        current_time = time.time()
        
        # Reset window if more than rate_limit_window has passed
        if current_time - provider.request_window_start > self.rate_limit_window:
            provider.request_window_start = current_time
            provider.request_count = 0
        
        return provider.request_count < provider.rate_limit
    
    def _update_rate_limit(self, provider: SearchProvider):
        """Update rate limit tracking after a request."""
        current_time = time.time()
        provider.last_request_time = current_time
        provider.request_count += 1
    
    async def search_brave(self, query: str, max_results: int = 10) -> List[SearchResult]:
        """Search using Brave Search API with rate limiting and retry logic."""
        provider = self.providers["brave"]
        if not provider.enabled:
            return []
        
        # Ensure max_results is an integer
        try:
            max_results = int(max_results)
        except (ValueError, TypeError):
            max_results = 10
        
        # Check rate limits
        if not self._check_rate_limit(provider):
            self.logger.warning(f"Rate limit exceeded for {provider.name}, skipping search")
            return []
        
        async def _search():
            headers = {
                "X-Subscription-Token": provider.api_key,
                "Accept": "application/json"
            }
            
            params = {
                "q": query,
                "count": min(max_results, 20),  # Brave API limit
                "search_lang": "en",
                "country": "US",
                "safesearch": "off",
                "freshness": "py"  # Past year for relevant CTI
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(provider.base_url, headers=headers, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = []
                        
                        for item in data.get("web", {}).get("results", []):
                            results.append(SearchResult(
                                title=item.get("title", ""),
                                url=item.get("url", ""),
                                snippet=item.get("description", ""),
                                provider="brave"
                            ))
                        
                        self.logger.info(f"Brave search for '{query}' returned {len(results)} results")
                        return results
                    else:
                        self.logger.error(f"Brave search failed with status {response.status}")
                        raise aiohttp.ClientResponseError(
                            None, None, status=response.status, message=f"HTTP {response.status}"
                        )
        
        try:
            # Standardized retry for external APIs
            decorated_search = retry_with_policy(max_attempts=3, delay=2)(_search)
            results = await decorated_search()
            self._update_rate_limit(provider)
            return results
        except Exception as e:
            self.logger.error(f"Error in Brave search after retries: {e}")
            return []
    
    async def search_google_cse(self, query: str, max_results: int = 10) -> List[SearchResult]:
        """Search using Google ADK built-in google_search tool instead of deprecated CSE API."""
        if not GOOGLE_ADK_AVAILABLE:
            self.logger.warning("Google ADK not available, skipping Google search")
            return []
        
        provider = self.providers.get("google_cse", None)
        if provider and not provider.enabled:
            return []
        
        # Ensure max_results is an integer
        try:
            max_results = int(max_results)
        except (ValueError, TypeError):
            max_results = 10
        
        # Check rate limits if provider is configured
        if provider and not self._check_rate_limit(provider):
            self.logger.warning(f"Rate limit exceeded for Google search, skipping")
            return []
        
        try:
            # Use the google_search tool directly - it's a function, not an object with call method
            search_response = await google_search(
                query=query,
                num_results=min(max_results, 10)
            )
            
            results = []
            
            # Handle Google ADK search response
            if hasattr(search_response, 'results'):
                search_results = search_response.results
            elif isinstance(search_response, list):
                search_results = search_response
            elif isinstance(search_response, dict) and 'results' in search_response:
                search_results = search_response['results']
            else:
                self.logger.warning(f"Unexpected Google search response format: {type(search_response)}")
                return []
            
            for item in search_results:
                if isinstance(item, dict):
                    title = item.get('title', item.get('name', ''))
                    url = item.get('url', item.get('link', ''))
                    snippet = item.get('snippet', item.get('description', ''))
                else:
                    # Handle object-like response
                    title = getattr(item, 'title', getattr(item, 'name', ''))
                    url = getattr(item, 'url', getattr(item, 'link', ''))
                    snippet = getattr(item, 'snippet', getattr(item, 'description', ''))
                
                if url:  # Only add if we have a valid URL
                    results.append(SearchResult(
                        title=title,
                        url=url,
                        snippet=snippet,
                        provider="google_adk"
                    ))
            
            self.logger.info(f"Google ADK search for '{query}' returned {len(results)} results")
            
            # Update rate limit if provider is configured
            if provider:
                self._update_rate_limit(provider)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in Google ADK search: {e}")
            return []
    
    async def search_duckduckgo(self, query: str, max_results: int = 10) -> List[SearchResult]:
        """Search using DuckDuckGo instant answer API with retry logic."""
        # Note: This is a simplified implementation
        # For production, consider using a proper DuckDuckGo search library
        
        # Ensure max_results is an integer
        try:
            max_results = int(max_results)
        except (ValueError, TypeError):
            max_results = 10
        
        async def _search():
            params = {
                "q": query,
                "format": "json",
                "no_html": "1",
                "skip_disambig": "1"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.duckduckgo.com/", params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = []
                        
                        # DuckDuckGo instant answers - limited results
                        if data.get("AbstractText"):
                            results.append(SearchResult(
                                title=data.get("Heading", ""),
                                url=data.get("AbstractURL", ""),
                                snippet=data.get("AbstractText", ""),
                                provider="duckduckgo"
                            ))
                        
                        self.logger.info(f"DuckDuckGo search for '{query}' returned {len(results)} results")
                        return results
                    else:
                        self.logger.error(f"DuckDuckGo search failed with status {response.status}")
                        raise aiohttp.ClientResponseError(
                            None, None, status=response.status, message=f"HTTP {response.status}"
                        )
        
        try:
            # Standardized retry for external APIs
            decorated_search = retry_with_policy(max_attempts=3, delay=2)(_search)
            return await decorated_search()
        except Exception as e:
            self.logger.error(f"Error in DuckDuckGo search after retries: {e}")
            return []
    
    async def search_all_providers(self, query: str, max_results_per_provider: int = 10) -> List[SearchResult]:
        """
        Search across all enabled providers concurrently.
        
        Args:
            query: Search query
            max_results_per_provider: Maximum results per provider
            
        Returns:
            Combined and deduplicated results from all providers
        """
        search_tasks = []
        
        for provider in self.enabled_providers:
            if provider.name == "brave":
                search_tasks.append(self.search_brave(query, max_results_per_provider))
            elif provider.name == "google_cse":
                search_tasks.append(self.search_google_cse(query, max_results_per_provider))
            elif provider.name == "duckduckgo":
                search_tasks.append(self.search_duckduckgo(query, max_results_per_provider))
        
        # Execute searches concurrently
        try:
            results_lists = await asyncio.gather(*search_tasks, return_exceptions=True)
            
            # Combine results
            all_results = []
            for results in results_lists:
                if isinstance(results, list):
                    all_results.extend(results)
                elif isinstance(results, Exception):
                    self.logger.error(f"Search provider failed: {results}")
            
            # Deduplicate by URL
            seen_urls: Set[str] = set()
            deduplicated_results = []
            
            for result in all_results:
                if result.url and result.url not in seen_urls:
                    seen_urls.add(result.url)
                    deduplicated_results.append(result)
            
            self.logger.info(f"Combined search for '{query}': {len(all_results)} total, {len(deduplicated_results)} after deduplication")
            return deduplicated_results
            
        except Exception as e:
            self.logger.error(f"Error in multi-provider search: {e}")
            return []
    
    async def search_multiple_queries(self, queries: List[str], max_results_per_query: int = 10) -> Dict[str, List[SearchResult]]:
        """
        Execute multiple search queries concurrently.
        
        Args:
            queries: List of search queries
            max_results_per_query: Maximum results per query
            
        Returns:
            Dictionary mapping queries to their search results
        """
        search_tasks = []
        
        for query in queries:
            search_tasks.append(self.search_all_providers(query, max_results_per_query))
        
        try:
            results_lists = await asyncio.gather(*search_tasks, return_exceptions=True)
            
            query_results = {}
            for i, results in enumerate(results_lists):
                query = queries[i]
                if isinstance(results, list):
                    query_results[query] = results
                else:
                    self.logger.error(f"Search failed for query '{query}': {results}")
                    query_results[query] = []
            
            total_results = sum(len(results) for results in query_results.values())
            self.logger.info(f"Multi-query search completed: {len(queries)} queries, {total_results} total results")
            
            return query_results
            
        except Exception as e:
            self.logger.error(f"Error in multi-query search: {e}")
            return {query: [] for query in queries}