# ADK Agents

This directory contains all Google ADK agents, shared tools, and configuration.

## Dependencies & Setup

We use `uv` (uvenv) to manage Python dependencies and environments.

1. Install and activate the environment:
   ```bash
   cd agents
   uv install
   uv activate
   ```

2. Install any additional packages if prompted.

## Configuration

List your RSS feeds in `config.yaml`:
```yaml
feeds:
  - https://feeds.feedburner.com/TroyHunt
  # Add more feeds below
  # - https://example.com/another/rss
```

To override the config path:
```bash
export RSS_CONFIG_PATH=/path/to/your/custom_feeds.yaml
```

## Running Agents

### RSS Collector Agent

The `RssCollectorAgent` is responsible for consuming RSS feed URLs from Kafka (topic: `feeds.discovered`), fetching RSS entries, extracting full article content, and publishing structured messages to the `raw.intel` Kafka topic.

**Key Features:**
- Listens on a configurable Kafka topic for feed URLs (default: `feeds.discovered`).
- Fetches RSS entries using `RssFeedFetcherTool`.
- Deduplicates feed entries based on GUID or link.
- Extracts the full text of each article via `ArticleExtractorTool`.
- Publishes enriched entry data (including URL, summary, published date, title, and extracted content) to Kafka `raw.intel`.
- Exposes Prometheus metrics (optional) for feed processing, errors, and publishing statistics.
- Supports Kafka security via TLS/SASL, configurable by environment variables.

#### Example Usage

```bash
uv run python -m collector_agent.rss_collector
```

#### Important Parameters

- `bootstrap_servers`: Kafka bootstrap servers (default: `localhost:9092`).
- `topic`: Kafka topic to consume feed URLs from (`feeds.discovered` by default).
- `prometheus_port`: (optional) Port to bind Prometheus metrics endpoint; enables metrics if set.

#### Supported Environment Variables for Kafka Security

- `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SSL_CAFILE`, `KAFKA_SSL_CERTFILE`, `KAFKA_SSL_KEYFILE`
- `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`

#### Message Flow

1. The agent consumes messages with a `feed_url` field from the specified Kafka topic.
2. For each new feed item, it attempts to fetch the corresponding article, extract its main text, and package the result into a Kafka message including:
   - `source_url`
   - `retrieved_at` (published timestamp)
   - `content_type: rss`
   - `summary`
   - `source_name` (feed entry title)
   - `full_text` (article, if available)
3. Published messages are sent to the `raw.intel` Kafka topic.

#### Metrics (Prometheus)

- `rss_collector_feed_fetch_total`: Total RSS feed fetch attempts
- `rss_collector_feed_fetch_errors_total`: Total errors fetching feeds
- `rss_collector_entries_retrieved_total`: Total entries retrieved from feeds
- `rss_collector_extraction_errors_total`: Errors during article extraction
- `rss_collector_entries_published_total`: Entries published to Kafka

#### Error Handling

- Failed fetches or extraction attempts are logged and increment error counters.
- Duplicate feeds and entries are skipped based on a seen GUIDs set.

#### Practical Workflow

```
Kafka (feeds.discovered) ---> [ RssCollectorAgent ] ---> Kafka (raw.intel)
```

### GeoIP Enrichment Agent

The `GeoIpEnrichmentAgent` enriches raw intelligence messages by extracting IPv4 addresses and looking up GeoIP context, then publishing the resulting structured data to the `enriched.intel` Kafka topic.

**Key Features:**
- Listens on the `raw.intel` Kafka topic for new messages.
- Extracts IPv4 addresses from incoming message contents (supports both JSON and Python literal message bodies).
- Performs GeoIP lookups for each discovered IP, aggregating results.
- Publishes an enriched message that includes:
  - The original record reference
  - Array of indicators (type/value for IPv4s found)
  - Array of `geo_locations` (enriched GeoIP records)
  - A confidence score (currently static, can be extended based on enrichment logic)
- Handles Prometheus-compatible logging and robust error handling per message.
- Supports complete Kafka security via environment configuration (TLS/SASL, etc.).

#### Example Usage

```bash
uv run python -m enricher_agent.geoip_enricher
```

#### Important Parameters

- `bootstrap_servers`: Kafka bootstrap servers (`localhost:9092` by default).
- Kafka security can be specified via the following environment variables:
  - `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SSL_CAFILE`, `KAFKA_SSL_CERTFILE`, `KAFKA_SSL_KEYFILE`
  - `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`

#### Message Flow

1. Consumes new messages from `raw.intel`.
2. For each message, extracts IPv4 addresses in text fields.
3. For each IP, queries GeoIP databases to retrieve geographic and network information.
4. If GeoIP enrichment is successful, outputs to the `enriched.intel` topic a message containing:
   - `original_raw_data_ref` (reference copy of the input message)
   - `indicators`: list of discovered IPv4 indicators
   - `geo_locations`: list of GeoIP result dicts
   - `confidence_score`: (currently static, implementation-defined)
5. Logging and error-handling are robust; malformed or non-enrichable messages are skipped but logged.

#### Extensibility

- Designed for easy addition of new enrichment stages (e.g. ASN, Whois, etc.).
- Could be extended to enrich with other indicator types found in raw data.

#### Typical Workflow

```
Kafka (raw.intel) ---> [ GeoIpEnrichmentAgent ] ---> Kafka (enriched.intel)
```

### Graph Builder Agent

The `Neo4jGraphBuilderAgent` listens for enriched messages on Kafka and builds or updates a property graph in Neo4j to represent sources, events, articles, indicators, and geolocations.

**Key Features:**
- Consumes from Kafka topic `enriched.intel`.
- Parses each message's enrichment output and emits Cypher commands to Neo4j to:
  - Merge `Source` nodes (by source URL)
  - Merge `SightingEvent` nodes (by timestamp)
  - Create relationships: `REPORTED_BY`, `ABOUT`, `APPEARED_IN`
  - Persist `Article` content (with `url` and `full_text` on `Article`)
  - Merge `Indicator` nodes for each extracted indicator, by type/value
  - Merge and relate `Country` nodes from GeoIP enrichment
  - Create `LOCATED_IN` relationships connecting indicators and countries

**Configuration:**
- `bootstrap_servers`: Kafka bootstrap servers for intake (`localhost:9092` by default)
- Neo4j connectivity (via `Neo4jWriterTool`) is handled by environment variables:
  - `NEO4J_URI` (`bolt://localhost:7687` by default)
  - `NEO4J_USER`, `NEO4J_PASSWORD`

**Example Usage**
```bash
uv run python -m graph_builder_agent.neo4j_builder
```

**Typical Message-Driven Flow:**
1. Consume from Kafka topic `enriched.intel`.
2. For each message:
    - Merge or update `Source`, `SightingEvent`, and related `Article`.
    - Attach `Indicator` nodes (`type`, `value`).
    - Link Indicators to Country (`LOCATED_IN`) based on GeoIP.
3. Cypher queries are executed internally using `Neo4jWriterTool`.

**Resulting Schema:**  
- `Source` —<`REPORTED_BY`— `SightingEvent`
- `Article` —<`ABOUT`— `SightingEvent`
- `Indicator` —<`APPEARED_IN`— `SightingEvent`
- `Indicator` —<`LOCATED_IN`— `Country`

**Inspecting the Graph:**
After running, open the Neo4j Browser and try queries such as:
```cypher
MATCH (a:Article)-[:ABOUT]->(e:SightingEvent) RETURN a.url, substring(a.full_text, 0, 200), e.timestamp LIMIT 5;
```
You can also explore event relationships, sources, indicators, and geolocations with other Cypher statements.

### Feed Seeder Agent

The `FeedSeederAgent` reads a static list of feeds from the YAML config file and publishes their definitions to Kafka, providing an initialization bootstrap mechanism for the system.

**Key Features:**
- Reads `config.yaml` for lists of RSS, CSV, JSON, or plain text feeds.
- Publishes each feed (augmented with `format`) to the `feeds.discovered` Kafka topic.
- Configurable by environment (`CONFIG_PATH` for alternate YAML config path, `kafka_bootstrap_servers` for Kafka connectivity).
- Logs and reports errors if config loading or Kafka connections fail.

**Example Usage**

```bash
uv run python -m collector_agent.feed_seeder_agent
```

**Parameters**
- `kafka_bootstrap_servers`: Kafka bootstrap servers (`localhost:9092` default).
- `config_path`: Path to config YAML (default: `agents/config.yaml` or given by `CONFIG_PATH`).
- `topic`: Kafka topic to produce feed definitions to (default: `feeds.discovered`).

**Expected Config YAML Structure**

```yaml
feeds:
  - https://sample.com/feed.rss
csv:
  - https://sample.com/export.csv
txt:
  - https://sample.com/urls.txt
json:
  - https://sample.com/data.json
```

**Published Kafka Message Format**

Each message sent to the topic will take the form:
```json
{"feed_url": "https://example.com/feed", "format": "rss"}
```
(With the format field set based on which section of the YAML config the feed is found in.)

**Behavior**
- If the config is empty or missing feeds, logs a warning and does not send messages.
- If Kafka connection fails, logs and exits.
- After publishing, flushes the Kafka producer to ensure all messages are sent.

**Workflow**

```
YAML config --> [ FeedSeederAgent ] --> Kafka (feeds.discovered)
```

### MISP Feed Agent

The `MispFeedAgent` polls MISP OSINT feeds for indicators of compromise (IOCs) and publishes normalized IOC records to Kafka.

**Key Features:**
- Reads a list of MISP feed definitions (index URLs) from in-code or config (passed to constructor).
- Fetches feed indexes, then individual feeds, parsing and normalizing each IOC record using `MispFeedTool`.
- Keeps track of state to avoid reprocessing (future extensibility; currently a placeholder).
- Publishes each parsed IOC as a message to the `raw.intel.misp` Kafka topic.
- Emits Prometheus metrics for index/feed fetching, errors, and published IOCs.
- Supports TLS/SASL Kafka security via standard environment variables.

**Example Usage**
```bash
uv run python -m collector_agent.misp_feed_agent
```
Pass the list of feeds when instantiating programmatically, or connect to a shared configuration system.

**Parameters**
- `feeds`: A list of dicts (at least with `index_url` for each).
- `bootstrap_servers`: Kafka bootstrap servers (default: `localhost:9092`).

**Prometheus Metrics**
- `misp_index_fetch_total`: Number of index fetch attempts.
- `misp_feed_fetch_total`: Number of individual feed fetch attempts.
- `misp_errors_total`: Count of errors fetching or parsing MISP feeds.
- `misp_iocs_published_total`: IOCs successfully published to Kafka.

**Published Kafka Message Format**
```json
{
    "source": "misp",
    "index_url": "<MISP index URL>",
    "feed_url": "<actual feed endpoint>",
    "fetched_at": "<UTC timestamp>",
    "ioc_record": { ... } // parsed and normalized IOC/event object
}
```

**Behavior**
- Handles Kafka publishing errors and logs each failure.
- Respects state to improve incremental polling (can be extended).
- Incubates new indicators continuously from the MISP ecosystem.

**Workflow**

```
MISP (remote feed) ---> [ MispFeedAgent ] ---> Kafka (raw.intel.misp)
```

### Additional Agents
Future agents (graph builder, RAG QA) can be run similarly via their module paths.

### RSS Discovery Agent
Discovers RSS/Atom feeds by crawling seed websites (default seeds or overridden via `RSS_DISCOVERY_SEEDS`) and appends them to `config.yaml`:

- Override seeds:  
  ```bash
  export RSS_DISCOVERY_SEEDS="https://site1.com,https://site2.com"
  ```
- Run:
  ```bash
  uv run python rss_discovery_agent.py
  ```

### RSS Discovery Agent

The `RssDiscoveryAgent` crawls a list of cybersecurity-related websites to auto-discover RSS/Atom feed endpoints, updating the config and publishing discoveries into the shared Kafka discovery topic.

### TAXII Pull Agent

The `TaxiiPullAgent` polls STIX objects from TAXII 2.1 servers and publishes those objects as messages to a Kafka topic.

**Key Features:**
- Configurable to connect to multiple TAXII servers and collections, as defined in `config.yaml` or via environment variable (`TAXII_CONFIG_PATH`).
- Authenticates per-server using username/password fields defined in config.
- Maintains per-feed state to fetch only new/updated STIX objects, using timestamps.
- Publishes one message per STIX object to `raw.intel.taxii`.
- Reports Prometheus metrics for polling, errors, and object throughput.
- Supports secure Kafka clusters via environment variables.

**Example Usage**
```bash
uv run python -m collector_agent.taxii_pull_agent
```

**Configuration**
Specify TAXII servers in YAML config (default `config.yaml`, or override with `TAXII_CONFIG_PATH`):
```yaml
servers:
  - url: https://hailataxii.com/taxii/
    collections:
      - 3d594650-3436-4716-8deb-1030ec91a097
    username: myuser
    password: mypass
    poll_interval: 3600  # in seconds
```
- `servers`: List of TAXII endpoints, each with its collections, authentication (optional), and polling interval.
- The path to config can be overridden with `TAXII_CONFIG_PATH`.
- All Kafka and cluster auth as described above.

**Prometheus Metrics**
- `taxii_fetch_requests_total`: Total TAXII fetch attempts.
- `taxii_fetch_errors_total`: Total TAXII fetch or parse failures.
- `taxii_objects_published_total`: Number of STIX objects published to Kafka.

**Message Format Example**
```json
{
  "source": "taxii",
  "server_url": "<TAXII Server URL>",
  "collection_id": "<Collection UUID>",
  "fetched_at": "<UTC timestamp>",
  "stix_object": { ...STIX object... }
}
```

**Security Considerations**
- Use environment variables or secret managers for usernames, passwords, and config paths.
- Ensure endpoints use TLS and validate certificates.
- Do not log sensitive data/credentials.

**Scalability and Reliability**
- Stateless across runs except for internal polling state (consider externalizing for clustered operation).
- Can run multiple agents polling distinct TAXII servers or as horizontally-scalable pollers with partitioned collections.
- Use a robust Kafka cluster and partitioning for high throughput, and monitor with Prometheus metrics.

**Workflow**
```
TAXII Servers ---> [ TaxiiPullAgent ] ---> Kafka (raw.intel.taxii)
```

### Shodan Stream Collector Agent

The `ShodanStreamCollectorAgent` continuously streams live events from the Shodan search API and publishes those raw event messages to a Kafka topic for further downstream enrichment or storage.

**Key Features:**
- Uses Shodan API streaming endpoint to receive real-time cyber event data (requires a valid Shodan API key).
- Publishes each event to the `raw.intel.shodan` Kafka topic, wrapping the data with a uniform message schema.
- Initiates and manages long-running network streams, handling reconnections and error scenarios.
- Emits Prometheus metrics for processed events and stream errors.
- Supports Kafka configuration via environment variables and secures API credentials through env vars.
- Designed for high-throughput ingestion and horizontal scalability (can run multiple collectors).

**Example Usage**
```bash
export SHODAN_API_KEY="<your-api-key>"
uv run python -m collector_agent.shodan_stream_agent
```

**Parameters and Environment**
- `api_key`: Provided in code or via the `SHODAN_API_KEY` environment variable (required).
- `bootstrap_servers`: Kafka bootstrap servers (default: `localhost:9092`).
- `PROMETHEUS_PORT_SHODAN`: Port for exposing Prometheus metrics (default: `9466`).
- Kafka security variables: `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SSL_CAFILE`, etc. (if secured cluster).

**Prometheus Metrics**
- `shodan_events_total`: Number of Shodan events streamed and published.
- `shodan_errors_total`: Streaming or connection errors with Shodan.

**Message Format Example**
```json
{
  "source": "shodan",
  "timestamp": "<event timestamp>",
  "data": { ...shodan_event_payload... }
}
```

**Security Considerations**
- NEVER commit API keys to source control or Docker images.
- Use secrets management or environment variables for all sensitive configs.
- Ensure outbound connectivity used for the agent (API calls and Kafka) uses secure TLS endpoints.

**Scalability & Operations**
- Designed to be stateless: deploy multiple agents for horizontal scaling to handle higher Shodan traffic.
- Use Kafka partitioning and consumer groups for distributed processing downstream.
- Monitor event and error metrics to maintain operational awareness.

**Workflow**
```
Shodan Stream API ---> [ ShodanStreamCollectorAgent ] ---> Kafka (raw.intel.shodan)
```

**Key Features:**
- Crawls seed websites (default, or overridden via `RSS_DISCOVERY_SEEDS` env variable).
- Parses `<link>` and `<a>` HTML tags to extract RSS/Atom endpoints for known cybersecurity blogs, news, and community sites.
- Publishes each newly discovered feed as a message to the `feeds.discovered` Kafka topic.
- Updates the YAML config if needed (config path can be overridden via `RSS_CONFIG_PATH`).
- Emits Prometheus metrics for crawling attempts, discovery count, config updates, and errors.

**Example Usage**
```bash
uv run python rss_discovery_agent.py
```

**Parameters and Environment**
- `kafka_bootstrap_servers`: Kafka bootstrap servers (falls back to `KAFKA_BOOTSTRAP_SERVERS` env var).
- `topic`: Kafka topic to publish feed URLs to (`feeds.discovered` default).
- `config_path`: Location of the config YAML; can be set via the `RSS_CONFIG_PATH` env var.
- `RSS_DISCOVERY_SEEDS`: Comma-separated list of override sites if you want to control sources.

**Metrics (Prometheus)**
- `rss_discovery_sites_crawled_total`: Number of seed sites crawled.
- `rss_discovery_feeds_found_total`: Feeds discovered (cross all crawls).
- `rss_discovery_config_updates_total`: Successful config file updates.
- `rss_discovery_errors_total`: Errors (http/network, parsing, etc.).

**Message Flow**
1. Crawls each seed website for feed links.
2. For each discovered endpoint, publishes a message of the form:
   ```json
   {"feed_url": "https://blog.example.com/rss"}
   ```
   to the configured Kafka topic.
3. Optionally appends the discovered feeds to the config YAML file.
4. All outcomes are logged, and errors are handled gracefully with separate error metrics.

**Workflow**
```
Web (sites) ---> [ RssDiscoveryAgent ] ---> Kafka (feeds.discovered) (+ optional config update)
```

## Future Considerations

### Deploying to GCP with Vertex AI

There are plans to support deploying agents and models (like Gemma or other custom LLMs) on Google Cloud Platform (GCP) using Vertex AI. This will involve:

*   **DSPy Configuration**: Modifying `dspy.settings.configure()` to use a `dspy.VertexAI` language model, which will require the `google-cloud-aiplatform` library.
*   **Model Hosting**: Hosting custom models (e.g., Gemma) as Vertex AI Endpoints.
*   **Authentication**: Ensuring agents have the correct GCP credentials and permissions to access Vertex AI services (typically via service accounts when running in GCP).
*   **Configuration**: Updating `config.yaml` (or using environment variables) to specify the Vertex AI project, location, endpoint ID, and model name.
*   **Rate Limiting**: Leveraging Vertex AI's built-in quota and rate limiting features, which might simplify or supersede some custom rate-limiting logic in the agents for model inference calls.

This will allow for more scalable, managed model serving and potentially better integration with other GCP services.
