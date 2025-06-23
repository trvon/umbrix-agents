# Search Provider Integration Guide

This document provides detailed setup and configuration instructions for all supported search providers in the Intelligent Crawler Agent.

## Overview

The Intelligent Crawler Agent supports multiple search providers to maximize coverage and redundancy in threat intelligence source discovery. Each provider has different strengths, rate limits, and setup requirements.

## Supported Providers

### 1. Brave Search API

Brave Search provides privacy-focused web search with good coverage of security-related content.

#### Setup

1. **Create Account**
   - Visit [Brave Search API](https://api.search.brave.com/)
   - Sign up for a developer account
   - Verify your email address

2. **Get API Key**
   - Navigate to the API dashboard
   - Create a new application
   - Copy your API key

3. **Configuration**
   ```bash
   export BRAVE_API_KEY="your-brave-api-key-here"
   ```

#### Rate Limits

- **Free Tier**: 100 requests/hour
- **Paid Tiers**: Higher limits available
- **Rate Limit Headers**: Respected automatically by the agent

#### API Documentation

- [Brave Search API Docs](https://api.search.brave.com/app/documentation)
- Endpoint: `https://api.search.brave.com/res/v1/web/search`
- Response format: JSON with results array

#### Example Response

```json
{
  "query": {
    "original": "APT29 indicators",
    "show_strict_warning": false,
    "is_navigational": false
  },
  "web": {
    "results": [
      {
        "title": "APT29 Threat Intelligence Report",
        "url": "https://example.com/apt29-report",
        "description": "Comprehensive analysis of APT29 indicators and TTPs",
        "date_last_crawled": "2023-12-01",
        "language": "en"
      }
    ]
  }
}
```

### 2. Google Custom Search Engine (CSE)

Google CSE provides high-quality search results with customizable site filtering and result ranking.

#### Setup

1. **Create Custom Search Engine**
   - Visit [Google Custom Search](https://cse.google.com/)
   - Click "Add" to create a new search engine
   - Add sites to search (or leave empty for web-wide search)
   - Note your Search Engine ID

2. **Get API Key**
   - Visit [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select existing
   - Enable the "Custom Search API"
   - Create credentials (API Key)
   - Optionally restrict the API key to Custom Search API

3. **Configuration**
   ```bash
   export GOOGLE_CSE_API_KEY="your-google-api-key"
   export GOOGLE_CSE_ID="your-search-engine-id"
   ```

#### Rate Limits

- **Free Tier**: 100 requests/day
- **Paid Tier**: Up to 10,000 requests/day
- **Rate Limit**: Automatically handled by the agent

#### API Documentation

- [Google CSE API Docs](https://developers.google.com/custom-search/v1/overview)
- Endpoint: `https://www.googleapis.com/customsearch/v1`
- Authentication: API Key

#### Example Response

```json
{
  "kind": "customsearch#search",
  "queries": {
    "request": [
      {
        "title": "Google Custom Search - APT29 indicators",
        "totalResults": "152000",
        "searchTerms": "APT29 indicators"
      }
    ]
  },
  "items": [
    {
      "title": "APT29 Threat Analysis",
      "link": "https://example.com/apt29-analysis",
      "snippet": "Recent APT29 campaign indicators including IOCs and TTPs",
      "displayLink": "example.com"
    }
  ]
}
```

### 3. DuckDuckGo (Future Implementation)

DuckDuckGo integration is planned for future releases.

## Configuration

### Environment Variables

All search provider configurations can be set via environment variables:

```bash
# Brave Search
export BRAVE_API_KEY="your-brave-api-key"

# Google Custom Search Engine  
export GOOGLE_CSE_API_KEY="your-google-api-key"
export GOOGLE_CSE_ID="your-search-engine-id"

# Search behavior
export SEARCH_MAX_RESULTS=20
export SEARCH_MAX_CONCURRENT=5
export SEARCH_TIMEOUT=30
```

### YAML Configuration

Alternatively, use a configuration file:

```yaml
search:
  providers:
    brave:
      enabled: true
      api_key: "${BRAVE_API_KEY}"
      rate_limit: 100
    google_cse:
      enabled: true
      api_key: "${GOOGLE_CSE_API_KEY}"
      search_engine_id: "${GOOGLE_CSE_ID}"
      rate_limit: 100
  
  max_results_per_query: 20
  max_concurrent_searches: 5
  search_timeout: 30
```

## Rate Limiting

The agent implements intelligent rate limiting for each provider:

### Brave Search
- Tracks requests per hour
- Implements exponential backoff on rate limit errors
- Automatically retries after rate limit reset

### Google CSE
- Tracks requests per day
- Handles quota exceeded errors gracefully
- Distributes requests evenly throughout the day

### Rate Limit Strategy

```python
# Automatic rate limit handling
if provider.is_rate_limited():
    wait_time = provider.calculate_backoff()
    await asyncio.sleep(wait_time)
    retry_request()
```

## Provider Selection Strategy

The agent uses all enabled providers simultaneously for maximum coverage:

1. **Parallel Execution**: All providers search simultaneously
2. **Result Deduplication**: Duplicate URLs are removed
3. **Provider Ranking**: Results include provider information for quality assessment
4. **Fallback**: If one provider fails, others continue

## Monitoring and Debugging

### Metrics

Each provider exposes specific metrics:

```
# Brave Search metrics
crawler_searches_brave_total
crawler_searches_brave_errors_total
crawler_searches_brave_rate_limited_total

# Google CSE metrics  
crawler_searches_google_total
crawler_searches_google_errors_total
crawler_searches_google_quota_exceeded_total
```

### Debugging

Enable debug logging to see provider-specific information:

```bash
export LOG_LEVEL=DEBUG
```

Debug logs include:
- Provider selection decisions
- Rate limit status
- API request/response details
- Error handling and retries

### Health Checks

Each provider has health monitoring:

```python
# Check provider health
if not provider.is_healthy():
    logger.warning(f"Provider {provider.name} is unhealthy")
    # Continue with other providers
```

## Best Practices

### API Key Security
- Store API keys in environment variables
- Use secrets management in production
- Never commit API keys to version control
- Rotate keys regularly

### Query Optimization
- Use specific, relevant search terms
- Avoid overly broad queries
- Include threat intelligence context
- Monitor query effectiveness

### Rate Limit Management
- Monitor provider quotas
- Implement caching to reduce API calls
- Use multiple providers for redundancy
- Plan for rate limit recovery

### Error Handling
- Implement graceful degradation
- Log all provider errors
- Monitor error rates
- Have fallback strategies

## Troubleshooting

### Common Issues

1. **Invalid API Key**
   ```
   Error: 401 Unauthorized
   Solution: Verify API key is correct and active
   ```

2. **Rate Limit Exceeded**
   ```
   Error: 429 Too Many Requests
   Solution: Wait for rate limit reset or upgrade plan
   ```

3. **Network Connectivity**
   ```
   Error: Connection timeout
   Solution: Check network connectivity and firewall rules
   ```

4. **Invalid Search Engine ID (Google CSE)**
   ```
   Error: 400 Bad Request - Invalid cx parameter
   Solution: Verify Google CSE ID is correct
   ```

### Diagnostic Commands

```bash
# Test provider connectivity
curl -H "X-Subscription-Token: $BRAVE_API_KEY" \
  "https://api.search.brave.com/res/v1/web/search?q=test"

# Test Google CSE
curl "https://www.googleapis.com/customsearch/v1?key=$GOOGLE_CSE_API_KEY&cx=$GOOGLE_CSE_ID&q=test"

# Check agent health
curl http://localhost:8080/health

# Monitor metrics
curl http://localhost:8080/metrics | grep search
```

## Future Enhancements

### Planned Features
- DuckDuckGo integration
- Bing Search API support
- Custom provider plugins
- Advanced result ranking
- Query learning from feedback

### Custom Provider Support
The architecture supports adding new search providers by implementing the `SearchProvider` interface.

## Support

For issues with search provider integrations:
1. Check provider-specific documentation
2. Verify API key validity
3. Monitor rate limits and quotas
4. Review agent logs for detailed error information