# Intelligent Crawler Agent

The Intelligent Crawler Agent is an LLM-guided CTI (Cyber Threat Intelligence) source discovery system that automatically finds relevant threat intelligence sources based on existing intelligence in the system.

## Overview

This agent uses Large Language Models (LLMs) to generate intelligent search queries from threat intelligence context, searches across multiple providers to discover new sources, and extracts structured data from discovered content using DSPy-powered extraction.

### Key Components

- **IntelligentCrawlerAgent**: Main orchestrator class that coordinates the entire discovery workflow
- **SearchOrchestrator**: Multi-provider search coordination with rate limiting and deduplication
- **ContentFetcher**: Content extraction and normalization with DSPy integration
- **CrawlOrganizerAgent**: Central coordination for caching, deduplication, and prioritization

### Architecture

```
Kafka Topics → IntelligentCrawlerAgent → SearchOrchestrator → ContentFetcher → Kafka Topics
      ↓                    ↓                      ↓                ↓
normalized.intel    LLM Query Generation    Multi-Provider    DSPy Extraction
user.submissions                            Search            
feeds.discovered                                              
                                                              ↓
                                                        raw.intel.crawled
```

## Quick Start

### Prerequisites

- Python 3.12+
- Kafka cluster
- Redis instance
- Neo4j database
- API keys for search providers and LLM services

### Installation

```bash
cd agents/discovery_agent
pip install -r requirements.txt
```

### Configuration

Set the following environment variables:

```bash
# Required
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export GEMINI_API_KEY="your-gemini-api-key"
export CTI_API_KEY="your-cti-api-key"
export REDIS_URL="redis://localhost:6379"

# Optional - Search Providers
export BRAVE_API_KEY="your-brave-api-key"
export GOOGLE_CSE_API_KEY="your-google-cse-key"
export GOOGLE_CSE_ID="your-google-cse-id"

# Optional - Configuration
export SEARCH_MAX_RESULTS=20
export CONTENT_FETCH_TIMEOUT=30
export LOG_LEVEL=INFO
```

### Running the Agent

```bash
# Direct execution
python intelligent_crawler_agent.py

# Using UV (recommended)
uv run python intelligent_crawler_agent.py

# Module execution
python -m discovery_agent.intelligent_crawler_agent
```

### Docker Deployment

```bash
# Build the image
docker build -t intelligent-crawler-agent -f Dockerfile.intelligent_crawler .

# Run with Docker Compose (recommended)
docker-compose up intelligent-crawler-agent
```

## Configuration

The agent supports both environment variables and YAML configuration files. See `config.py` for all available options.

### Key Configuration Sections

#### Kafka Configuration
- **bootstrap_servers**: Kafka broker addresses
- **input_topics**: Topics to consume from (`normalized.intel`, `user.submissions`, `feeds.discovered`)
- **output_topics**: Topics to produce to (`raw.intel.crawled`, `feeds.discovered`)

#### LLM Configuration
- **gemini_api_key**: Google Gemini API key for query generation
- **model_name**: Gemini model to use (default: `gemini-1.5-flash-latest`)
- **temperature**: LLM temperature for query generation (default: 0.7)

#### Search Configuration
- **brave_api_key**: Brave Search API key
- **google_cse_api_key**: Google Custom Search Engine API key
- **max_results_per_query**: Maximum results per search query (default: 20)
- **max_concurrent_searches**: Maximum concurrent search requests (default: 5)

#### Content Configuration
- **fetch_timeout_seconds**: HTTP request timeout (default: 30)
- **max_content_size_bytes**: Maximum content size to process (default: 1MB)
- **user_agent**: User agent string for HTTP requests

## Monitoring

The agent exposes Prometheus metrics on port 8080 (configurable):

- `crawler_queries_generated_total`: Number of LLM queries generated
- `crawler_searches_performed_total`: Number of searches performed by provider
- `crawler_content_fetched_total`: Number of content fetch attempts
- `crawler_processing_duration_seconds`: Processing time histograms

### Health Checks

- **Health endpoint**: `GET /health` - Overall agent health
- **Readiness endpoint**: `GET /ready` - Agent readiness for traffic
- **Metrics endpoint**: `GET /metrics` - Prometheus metrics

## Data Flow

### Input Messages

The agent consumes from three Kafka topics:

1. **normalized.intel**: Normalized threat intelligence data
2. **user.submissions**: User-submitted threat intelligence  
3. **feeds.discovered**: Previously discovered feed information

### Output Messages

The agent produces to:

1. **raw.intel.crawled**: Discovered content with metadata
2. **feeds.discovered**: Newly discovered feed sources

### Message Schemas

See `agents/common_tools/schemas/` for detailed schema definitions.

## Search Providers

### Brave Search API

Brave Search provides web search results with good privacy practices.

**Setup**:
1. Sign up at [Brave Search API](https://api.search.brave.com/)
2. Get your API key
3. Set `BRAVE_API_KEY` environment variable

**Rate Limits**: 100 requests/hour (free tier)

### Google Custom Search Engine

Google CSE provides high-quality search results with customizable filtering.

**Setup**:
1. Create a Custom Search Engine at [Google CSE](https://cse.google.com/)
2. Get your Search Engine ID
3. Get an API key from [Google Cloud Console](https://console.cloud.google.com/)
4. Set `GOOGLE_CSE_API_KEY` and `GOOGLE_CSE_ID` environment variables

**Rate Limits**: 100 requests/day (free tier)

## Security Considerations

- Store API keys securely using environment variables or secrets management
- Use HTTPS for all external API calls
- Implement proper authentication for agent endpoints
- Monitor and log all external requests for security auditing
- Respect robots.txt and rate limits of target websites

## Troubleshooting

### Common Issues

1. **No LLM API Key**: Agent will log warnings but continue with limited functionality
2. **Search Provider Rate Limits**: Agent implements exponential backoff and retries
3. **Content Fetch Timeouts**: Increase `CONTENT_FETCH_TIMEOUT` for slow websites
4. **Memory Issues**: Reduce `CONTENT_MAX_SIZE` or `SEARCH_MAX_RESULTS`

### Debugging

Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
```

Check agent health:
```bash
curl http://localhost:8080/health
```

Monitor metrics:
```bash
curl http://localhost:8080/metrics
```

## Performance Tuning

### Search Optimization
- Adjust `max_concurrent_searches` based on provider rate limits
- Tune `max_results_per_query` for quality vs. quantity trade-off
- Monitor provider-specific rate limits and errors

### Content Fetching
- Increase `max_concurrent_fetches` for better throughput
- Adjust `fetch_timeout_seconds` based on target site performance
- Use `max_content_size_bytes` to prevent memory issues

### Resource Usage
- Monitor memory usage with large content fetches
- Scale horizontally by running multiple agent instances
- Use Redis for shared caching and deduplication

## Development

### Running Tests

```bash
# Install test dependencies
pip install -r requirements.txt

# Run tests
pytest tests/

# Run with coverage
pytest tests/ --cov=discovery_agent
```

### Code Style

This project follows:
- Black code formatting
- Type hints for all functions
- Docstrings for all public methods
- Async/await patterns for I/O operations

### Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Related Documentation

- [Search Providers Setup](docs/search-providers.md)
- [API Integration Details](docs/api-integration.md)
- [Operational Guide](docs/operations.md)
- [Security Considerations](docs/security.md)

## License

This project is part of the Umbrix CTI platform.