from google.adk.agents import Agent
from kafka import KafkaProducer
from common_tools.shodan_tools import ShodanStreamTool
import os
import sys
import json
import time
from prometheus_client import Counter, start_http_server, CollectorRegistry
from urllib.parse import urlparse
from datetime import datetime, timezone
from pydantic import AnyUrl, ValidationError
from common_tools.models import FeedRecord
from common_tools.normalizers import FeedDataNormalizer
from common_tools.feed_enricher import DSPyFeedEnricher
from common_tools.dedupe_store import RedisDedupeStore
from common_tools.schema_validator import SchemaValidator
from jsonschema import ValidationError as SchemaValidationError
import dspy
# Create a registry for this agent's metrics
METRICS_REGISTRY = CollectorRegistry()
# Deduplication TTL and Redis URL from environment
DEDUP_TTL_SECONDS = int(os.getenv('REDIS_DEDUPE_TTL_SECONDS', 86400))
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379')

# Metrics for Shodan streaming
SHODAN_EVENTS_COUNTER = Counter('shodan_events_total', 'Total number of Shodan events processed', registry=METRICS_REGISTRY)
SHODAN_ERROR_COUNTER = Counter('shodan_errors_total', 'Total Shodan stream errors', registry=METRICS_REGISTRY)

# Helper for Shodan dates (typically ISO8601)
def parse_shodan_date_to_datetime(date_str: str) -> datetime | None:
    if not date_str:
        return None
    try:
        # Shodan timestamps are often ISO8601 strings
        # Example: "2023-10-27T10:20:30.123456Z"
        # Remove Zulu if present and add +00:00 for full ISO compatibility with fromisoformat
        # Also handle potential missing milliseconds
        if '.' in date_str:
            main_part, sec_part = date_str.split('.', 1)
            sec_part = sec_part.rstrip('Z')
            date_str = f"{main_part}.{sec_part.ljust(6, '0')}+00:00" if '+' not in main_part and '-' not in main_part[10:] else f"{main_part}.{sec_part.ljust(6, '0')}"
        else:
            date_str = date_str.rstrip('Z') + "+00:00" if '+' not in date_str and '-' not in date_str[10:] else date_str 
        
        # Corrected logic: fromisoformat handles Z directly or +00:00
        if date_str.endswith('Z'):
            dt = datetime.fromisoformat(date_str[:-1] + '+00:00')
        else:
            dt = datetime.fromisoformat(date_str)
        return dt.astimezone(timezone.utc)
    except ValueError as e:
        # print(f"[Shodan] Could not parse Shodan date {date_str}: {e}", file=sys.stderr)
        pass
    return None

class ShodanStreamAgent(Agent):
    """ADK Agent to stream events from Shodan and publish to Kafka raw.intel."""
    class Config:
        extra = "allow"

    def __init__(self,
                 api_key: str = None,
                 bootstrap_servers: str = 'localhost:9092'):
        super().__init__(name="shodan_stream_agent", description="Streams Shodan events and publishes to Kafka.")
        self.tool = ShodanStreamTool(api_key or os.getenv('SHODAN_API_KEY'))
        
        # --- Kafka Producer Setup ---
        security_protocol = os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
        ssl_cafile = os.getenv('KAFKA_SSL_CAFILE')
        ssl_certfile = os.getenv('KAFKA_SSL_CERTFILE')
        ssl_keyfile = os.getenv('KAFKA_SSL_KEYFILE')
        sasl_mechanism = os.getenv('KAFKA_SASL_MECHANISM')
        sasl_username = os.getenv('KAFKA_SASL_USERNAME')
        sasl_password = os.getenv('KAFKA_SASL_PASSWORD')
        # Setup Kafka producer with error handling for testing
        self.producer = None
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                security_protocol=security_protocol,
                ssl_cafile=ssl_cafile,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
                sasl_mechanism=sasl_mechanism,
                sasl_plain_username=sasl_username,
                sasl_plain_password=sasl_password,
                # Updated value_serializer for FeedRecord
                value_serializer=lambda v: v.model_dump_json().encode('utf-8') if isinstance(v, FeedRecord) else json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            print(f"[Shodan] KafkaProducer init failure: {e}", file=sys.stderr)
            # Continue without exiting for testing purposes

        # --- Initialize new components ---
        self.feed_normalizer = FeedDataNormalizer()
        self.feed_enricher = DSPyFeedEnricher() # Assumes global DSPy LM config
        if dspy.settings.lm is None:
            print("[Shodan] CRITICAL: DSPy LM not configured globally. Enrichment will fail.", file=sys.stderr)

        # --- Schema Validator for message validation ---
        self.schema_validator = SchemaValidator()
        # Validation error metric
        self.validation_error_counter = Counter('shodan_validation_errors_total', 'Total Shodan message schema validation failures', registry=METRICS_REGISTRY)

        # --- Kafka Topics --- (Assuming a unified raw.intel topic)
        # Using os.getenv for topic for Shodan agent simplicity, can be expanded with config.yaml like others
        self.raw_intel_topic = os.getenv('SHODAN_KAFKA_TOPIC', 'raw.intel')
        print(f"[Shodan] Publishing to raw intel topic: {self.raw_intel_topic}", file=sys.stderr)
        # Initialize dedupe store to avoid reprocessing the same event
        self.dedupe_store = RedisDedupeStore(REDIS_URL)

    def run(self):
        print("[Shodan] Starting Shodan stream...", file=sys.stderr)
        # Allow configuring Prometheus port via env, default if not set.
        prom_port_str = os.getenv('PROMETHEUS_PORT_SHODAN', '9466')
        try:
            start_http_server(int(prom_port_str), registry=METRICS_REGISTRY)
            print(f"[Shodan] Prometheus metrics on port {prom_port_str}", file=sys.stderr)
        except Exception as e_prom:
            print(f"[Shodan] Failed to start Prometheus server on port {prom_port_str}: {e_prom}", file=sys.stderr)

        for shodan_event in self.tool.stream():
            self.process_event(shodan_event)

    def process_event(self, shodan_event):
        """Process a single Shodan event and publish it to Kafka if it passes validation.
        Returns True if the event was successfully processed and published, False otherwise."""
        try:
            # Durable dedupe: skip if this event was seen
            ip = shodan_event.get('ip_str')
            port = shodan_event.get('port')
            ts = shodan_event.get('timestamp')
            key = f"dedupe:shodan:{ip}:{port}:{ts}"
            if not self.dedupe_store.set_if_not_exists(key, DEDUP_TTL_SECONDS):
                return False
                
            SHODAN_EVENTS_COUNTER.inc()
         
            # --- Create FeedRecord from shodan_event ---
            ip_str = shodan_event.get('ip_str')
            port = shodan_event.get('port')
            hostnames = shodan_event.get('hostnames', [])
            event_timestamp_str = shodan_event.get('timestamp')

            # Determine URL for FeedRecord
            record_url_str = f"http://{ip_str}" # Default to IP
            if hostnames and isinstance(hostnames, list) and len(hostnames) > 0:
                record_url_str = f"http://{hostnames[0]}" # Use first hostname if available
            # Append port if it's a common web port or if no hostname to make it more unique
            if port and (not hostnames or port in [80, 443, 8000, 8080]): 
                 if not record_url_str.startswith("http://") and not record_url_str.startswith("https://"):
                    record_url_str = f"http://{record_url_str}" # ensure scheme
                 # Avoid double port if hostname already implies it or for default ports
                 if not ( (port == 80 and record_url_str.startswith("http://")) or 
                          (port == 443 and record_url_str.startswith("https://")) ):
                    # Check if port is already in the hostname part (e.g. host:port from shodan field)
                    if str(port) not in urlparse(record_url_str).netloc.split(':')[-1]:
                         record_url_str = f"{record_url_str}:{port}"
            try:
                validated_url = AnyUrl(record_url_str)
            except ValidationError:
                # Fallback if constructed URL is invalid (e.g. bad hostname chars)
                validated_url = AnyUrl(f"shodan://{ip_str}/{port if port else 'unknown_port'}")

            title_parts = []
            if shodan_event.get('devicetype'): title_parts.append(shodan_event.get('devicetype'))
            if shodan_event.get('product'): title_parts.append(shodan_event.get('product'))
            if not title_parts and shodan_event.get('banner'): # First part of banner
                banner_preview = shodan_event.get('banner','').split('\n')[0][:50]
                title_parts.append(f"Banner: {banner_preview}...")
            if not title_parts:
                title_parts.append(f"Event for {ip_str}:{port}")
            title = ' | '.join(filter(None, title_parts))

            # Fix tags field - ensure it's always a list, never None
            tags = shodan_event.get('tags', [])
            if tags is None:
                tags = []
                
            # Add vulnerabilities to tags if present
            if isinstance(shodan_event.get('vulns'), dict):
                tags.extend(shodan_event['vulns'].keys()) # Add CVEs as tags
            elif isinstance(shodan_event.get('vulns'), list): # Older format?
                 tags.extend(shodan_event.get('vulns'))
            
            feed_record = FeedRecord(
                url=validated_url,
                title=title,
                description=shodan_event.get('data') or f"Shodan scan data for {ip_str} on port {port}. Org: {shodan_event.get('org', 'N/A')}. ISP: {shodan_event.get('isp', 'N/A')}.",
                published_at=parse_shodan_date_to_datetime(event_timestamp_str),
                discovered_at=datetime.now(timezone.utc),
                source_name=shodan_event.get('org') or shodan_event.get('isp') or 'shodan.io',
                source_type='shodan_stream',
                tags=tags, # Always a list, never None
                raw_content_type='json',
                raw_content=json.dumps(shodan_event),
                metadata={
                    'shodan_event_original': shodan_event, # Keep full original event
                    'ip': ip_str,
                    'port': port,
                    'asn': shodan_event.get('asn'),
                    'location': shodan_event.get('location'),
                    'transport': shodan_event.get('transport'),
                    'hostnames': hostnames,
                    'vulns': shodan_event.get('vulns') # Store vulnerabilities separately too
                }
            )

            # Normalize
            normalized_record = self.feed_normalizer.normalize_feed_record(feed_record)
            
            # Enrich
            enriched_record = self.feed_enricher.enrich(normalized_record)
            
            # Validate schema before publishing
            try:
                payload = enriched_record.model_dump()
                self.schema_validator.validate(self.raw_intel_topic, payload)
            except SchemaValidationError as e_val:
                # Send to dead-letter queue
                self.validation_error_counter.inc()
                dlq_topic = f"dead-letter.{self.raw_intel_topic}"
                self.producer.send(dlq_topic, {"error_type": "SchemaValidationError", "details": str(e_val), "message": payload})
                print(f"[Shodan] Schema validation failed for {ip}:{port}: {e_val}", file=sys.stderr)
                return False

            # Publish valid message
            self.producer.send(self.raw_intel_topic, enriched_record)
            return True

        except ValidationError as ve:
            print(f"[Shodan] Pydantic validation error creating FeedRecord: {ve}. Event: {shodan_event.get('ip_str', 'N/A')}", file=sys.stderr)
            SHODAN_ERROR_COUNTER.inc()
            return False
        except Exception as e:
            SHODAN_ERROR_COUNTER.inc()
            print(f"[Shodan] Error processing event for IP {shodan_event.get('ip_str', 'N/A')}: {e}", file=sys.stderr)
            # Add a small delay to prevent rapid error loops if stream is problematic
            time.sleep(0.1)
            return False
