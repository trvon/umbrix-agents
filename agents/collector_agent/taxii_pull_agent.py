from google.adk.agents import Agent
from kafka import KafkaProducer
from common_tools.cti_feed_tools import TaxiiClientTool
import json
import time
import os
import yaml
import sys
import dspy
from datetime import datetime, timezone
from prometheus_client import Counter, start_http_server, CollectorRegistry
from urllib.parse import urlparse
from pydantic import AnyUrl, ValidationError
from common_tools.models import FeedRecord
from common_tools.normalizers import FeedDataNormalizer
from common_tools.feed_enricher import DSPyFeedEnricher
from common_tools.schema_validator import SchemaValidator, ValidationError as SchemaValidationError
# Create a registry for this agent's metrics
METRICS_REGISTRY = CollectorRegistry()
# Metrics for TAXII polling
TAXII_FETCH_COUNTER = Counter('taxii_fetch_requests_total', 'Total number of TAXII fetch attempts', registry=METRICS_REGISTRY)
TAXII_FETCH_ERROR_COUNTER = Counter('taxii_fetch_errors_total', 'Total number of TAXII fetch errors', registry=METRICS_REGISTRY)
TAXII_OBJECTS_PUBLISHED_COUNTER = Counter('taxii_objects_published_total', 'Total number of STIX objects published', registry=METRICS_REGISTRY)
TAXII_VALIDATION_ERROR_COUNTER = Counter('taxii_validation_errors_total', 'Total TAXII message schema validation failures', registry=METRICS_REGISTRY)

# Helper to parse STIX dates (similar to rss_collector's)
def parse_stix_date_to_datetime(date_str: str) -> datetime | None:
    if not date_str:
        return None
    try:
        # STIX timestamps are usually ISO 8601 with Z or offset
        # Remove milliseconds if present, as fromisoformat can be picky
        if '.' in date_str and ('Z' in date_str.upper() or '+' in date_str or '-' in date_str[10:]):
            date_str = date_str.split('.')[0] + date_str[date_str.rfind('.')+2:] if date_str.rfind('.') > 18 else date_str

        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.astimezone(timezone.utc)
    except ValueError as e:
        # print(f"[TAXII] Could not parse STIX date {date_str}: {e}", file=sys.stderr)
        pass
    return None

class TaxiiPullAgent(Agent):
    """ADK Agent to poll TAXII 2.1 servers and publish STIX objects to Kafka."""
    class Config:
        extra = "allow"

    def __init__(self, servers: list[dict], bootstrap_servers: str = 'localhost:9092'):
        super().__init__(name="taxii_pull_agent", description="Collects STIX objects from TAXII servers and publishes to Kafka.")
        self.servers = servers
        self.tool = TaxiiClientTool()
        # Kafka producer setup
        security_protocol = os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
        ssl_cafile = os.getenv('KAFKA_SSL_CAFILE')
        ssl_certfile = os.getenv('KAFKA_SSL_CERTFILE')
        ssl_keyfile = os.getenv('KAFKA_SSL_KEYFILE')
        sasl_mechanism = os.getenv('KAFKA_SASL_MECHANISM')
        sasl_username = os.getenv('KAFKA_SASL_USERNAME')
        sasl_password = os.getenv('KAFKA_SASL_PASSWORD')
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_username,
            sasl_plain_password=sasl_password,
            value_serializer=lambda v: v.model_dump_json().encode('utf-8') if isinstance(v, FeedRecord) else json.dumps(v).encode('utf-8')
        )
        # State tracking for incremental fetching
        self.state: dict[str, str] = {}
        # Schema validator for outgoing messages
        self.schema_validator = SchemaValidator()

        # Initialize new components
        self.feed_normalizer = FeedDataNormalizer()
        self.feed_enricher = DSPyFeedEnricher() # Assumes global DSPy LM config

        # Kafka Topics
        # Load from config if available, similar to RssCollectorAgent
        config_path_env = os.getenv("AGENT_CONFIG_PATH", "agents/config.yaml")
        agent_config = {}
        try:
            with open(config_path_env, 'r') as f:
                agent_config = yaml.safe_load(f) or {}
        except Exception:
            pass # Defaults will be used
        kafka_topics_config = agent_config.get('kafka_topics', {})
        self.raw_intel_topic = kafka_topics_config.get('raw_intel', 'raw.intel')
        print(f"[TAXII] Publishing to raw intel topic: {self.raw_intel_topic}", file=sys.stderr)

    def run_once(self):
        for server_config in self.servers: # Renamed 'server' to 'server_config'
            server_url = server_config.get('url')
            username = server_config.get('username')
            password = server_config.get('password')
            for collection_id in server_config.get('collections', []):
                state_key = f"{server_url}|{collection_id}"
                added_after = self.state.get(state_key)
                TAXII_FETCH_COUNTER.inc()
                try:
                    stix_objects = self.tool.fetch_objects(
                        server_url, collection_id, username, password, added_after
                    )
                except Exception as e:
                    print(f"[TAXII] Error fetching {collection_id} from {server_url}: {e}", file=sys.stderr)
                    TAXII_FETCH_ERROR_COUNTER.inc()
                    continue
                
                new_max_ts = added_after
                for stix_obj in stix_objects:
                    stix_id = stix_obj.get('id')
                    created_ts_str = stix_obj.get('created')
                    modified_ts_str = stix_obj.get('modified')
                    
                    # Determine primary timestamp for filtering and FeedRecord.published_at
                    primary_ts_str = modified_ts_str or created_ts_str or datetime.now(timezone.utc).isoformat()

                    if added_after and primary_ts_str <= added_after:
                        continue

                    # Create FeedRecord
                    try:
                        # Determine FeedRecord.url
                        record_url_str = None
                        external_refs = stix_obj.get('external_references', [])
                        if external_refs and isinstance(external_refs, list) and external_refs[0].get('url'):
                            record_url_str = external_refs[0].get('url')
                        else:
                            parsed_server_url = urlparse(server_url)
                            record_url_str = f"taxii://{parsed_server_url.netloc}/{collection_id}/{stix_id}"

                        feed_record = FeedRecord(
                            url=AnyUrl(record_url_str),
                            title=stix_obj.get('name') or stix_obj.get('summary') or stix_obj.get('id'),
                            description=stix_obj.get('description'),
                            published_at=parse_stix_date_to_datetime(primary_ts_str),
                            discovered_at=datetime.now(timezone.utc),
                            source_name=urlparse(server_url).netloc,
                            source_type='taxii',
                            tags=stix_obj.get('labels', []) + stix_obj.get('keywords', []),
                            raw_content_type='json',
                            raw_content=json.dumps(stix_obj),
                            metadata={
                                'stix_object': stix_obj, # Full STIX object
                                'taxii_collection_id': collection_id,
                                'taxii_server_url': server_url,
                                'stix_id': stix_id,
                                'stix_created': created_ts_str,
                                'stix_modified': modified_ts_str
                            }
                        )
                    except ValidationError as ve:
                        print(f"[TAXII] Pydantic validation error for STIX ID {stix_id}: {ve}", file=sys.stderr)
                        continue # Skip this STIX object
                    except Exception as e_create:
                        print(f"[TAXII] Error creating FeedRecord for STIX ID {stix_id}: {e_create}", file=sys.stderr)
                        continue

                    # Normalize
                    try:
                        normalized_record = self.feed_normalizer.normalize_feed_record(feed_record)
                    except Exception as e_norm:
                        print(f"[TAXII] Error normalizing STIX ID {stix_id}: {e_norm}. Using unnormalized.", file=sys.stderr)
                        normalized_record = feed_record
                        # Use setattr for Pydantic model metadata fields
                        setattr(normalized_record.metadata, 'normalization_status', 'error')
                        setattr(normalized_record.metadata, 'normalization_error', str(e_norm))
                    else:
                        setattr(normalized_record.metadata, 'normalization_status', 'success')

                    # Enrich (conditionally or always, for TAXII perhaps always if raw_content is the STIX obj)
                    # For now, let's enrich if title or description seems too short or generic (like just ID)
                    should_enrich = bool(normalized_record.raw_content) and \
                                    (not normalized_record.title or normalized_record.title == stix_id or len(normalized_record.title) < 20 or \
                                     not normalized_record.description or len(normalized_record.description) < 50)
                    
                    if should_enrich:
                        print(f"[TAXII] Enriching STIX ID: {stix_id} from {server_url}", file=sys.stderr)
                        try:
                            enriched_record = self.feed_enricher.enrich(normalized_record)
                        except Exception as e_enrich:
                            print(f"[TAXII] Error during enrichment call for STIX ID {stix_id}: {e_enrich}", file=sys.stderr)
                            enriched_record = normalized_record
                    else:
                        enriched_record = normalized_record
                        setattr(enriched_record.metadata, 'dspy_enrichment_status', 'skipped_heuristic_taxii')

                    # Validate and publish to unified raw.intel topic
                    payload = enriched_record.model_dump()
                    try:
                        self.schema_validator.validate(self.raw_intel_topic, payload)
                    except SchemaValidationError as val_err:
                        TAXII_VALIDATION_ERROR_COUNTER.inc()
                        dlq = f"dead-letter.{self.raw_intel_topic}"
                        self.producer.send(dlq, {"error_type": "SchemaValidationError", "details": str(val_err), "message": payload})
                        continue
                    try:
                        self.producer.send(self.raw_intel_topic, enriched_record)
                        TAXII_OBJECTS_PUBLISHED_COUNTER.inc()
                    except Exception as e_kafka:
                        print(f"[TAXII] Kafka send error for STIX ID {stix_id}: {e_kafka}", file=sys.stderr)
                        # Handle send failure if needed

                    # Track latest timestamp for state
                    if not new_max_ts or primary_ts_str > new_max_ts:
                        new_max_ts = primary_ts_str
                
                if new_max_ts:
                    self.state[state_key] = new_max_ts

    def run(self):
        print(f"[TAXII] Starting TaxiiPullAgent for servers: {[s.get('url') for s in self.servers]}", file=sys.stderr)
        # Initial fetch
        self.run_once()
        # Single-run mode support
        if os.getenv('TAXII_ONCE') == '1':
            return
        # Loop
        while True:
            interval = next((s.get('poll_interval') for s in self.servers if s.get('poll_interval')), 3600)
            time.sleep(interval)
            self.run_once()
            if os.getenv('TAXII_ONCE') == '1':
                break


if __name__ == '__main__':
    # Start Prometheus metrics server
    port = int(os.getenv('PROMETHEUS_PORT_COLLECTOR', '9464'))
    start_http_server(port, registry=METRICS_REGISTRY)
    print(f"[TAXII] Prometheus metrics available on port {port}", file=sys.stderr)
    # Load configuration
    config_path = os.getenv('TAXII_CONFIG_PATH', os.path.join(os.getcwd(), 'config.yaml'))
    if not os.path.isfile(config_path):
        print(f"Config file not found at {config_path}")
        exit(1)
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    servers = cfg.get('servers', [])
    if not servers:
        print('No TAXII servers configured. Please add entries to config.yaml')
        exit(1)
    bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    agent = TaxiiPullAgent(servers, bootstrap)
    agent.run()
