from google.adk.agents import Agent
from kafka import KafkaProducer
from common_tools.cti_feed_tools import MispFeedTool
import json
import time
import os
import yaml
import sys
from datetime import datetime, timezone
from prometheus_client import Counter, start_http_server, CollectorRegistry
from urllib.parse import urlparse
from pydantic import AnyUrl, ValidationError
from common_tools.models import FeedRecord
from common_tools.normalizers import FeedDataNormalizer
from common_tools.feed_enricher import DSPyFeedEnricher
from common_tools.dedupe_store import RedisDedupeStore
from common_tools.schema_validator import SchemaValidator, ValidationError as SchemaValidationError
import dspy
# Create a registry for this agent's metrics
METRICS_REGISTRY = CollectorRegistry()
# Metrics for MISP feed collection
MISP_INDEX_FETCH_COUNTER = Counter('misp_index_fetch_total', 'Total MISP index fetch attempts', registry=METRICS_REGISTRY)
MISP_FEED_FETCH_COUNTER = Counter('misp_feed_fetch_total', 'Total MISP feed fetch attempts', registry=METRICS_REGISTRY)
MISP_ERROR_COUNTER = Counter('misp_errors_total', 'Total MISP errors', registry=METRICS_REGISTRY)
MISP_IOCS_PUBLISHED_COUNTER = Counter('misp_iocs_published_total', 'Total IOCs published from MISP feeds', registry=METRICS_REGISTRY)

# Deduplication TTL and Redis URL from environment
DEDUP_TTL_SECONDS = int(os.getenv('REDIS_DEDUPE_TTL_SECONDS', 86400))
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379')

# Helper to parse MISP dates (can be adapted from other agents)
def parse_misp_date_to_datetime(date_input) -> datetime | None:
    if not date_input:
        return None
    if isinstance(date_input, int) or isinstance(date_input, float):
        # Assume Unix timestamp if it's a number
        try:
            return datetime.fromtimestamp(float(date_input), timezone.utc)
        except Exception:
            pass # Fall through to string parsing
    if isinstance(date_input, str):
        try:
            # MISP timestamps can be Unix epoch strings or ISO-like
            if date_input.isdigit():
                dt = datetime.fromtimestamp(int(date_input), timezone.utc)
            else:
                # Similar to STIX/RSS parsing
                if '.' in date_input and ('Z' in date_input.upper() or '+' in date_input or '-' in date_input[10:]):
                     date_input = date_input.split('.')[0] + date_input[date_input.rfind('.')+2:] if date_input.rfind('.') > 18 else date_input
                dt = datetime.fromisoformat(date_input.replace('Z', '+00:00'))
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
    # print(f"[MISP] Could not parse MISP date: {date_input}", file=sys.stderr)
    return None

class MispFeedAgent(Agent):
    """ADK Agent to collect IOCs from MISP OSINT feeds and publish to Kafka raw.intel."""
    class Config:
        extra = "allow"

    def __init__(self, feeds: list[dict], bootstrap_servers: str = 'localhost:9092'):
        super().__init__(name="misp_feed_agent", description="Collects IOCs from MISP OSINT feeds.")
        self.feeds = feeds
        self.tool = MispFeedTool()
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
        # State tracking to only process new entries
        # self.state: dict[str, str] = {}
        # Initialize pluggable dedupe store
        self.dedupe_store = RedisDedupeStore(REDIS_URL)
        # Schema validator for outgoing messages
        self.schema_validator = SchemaValidator()
        # Metric for schema validation failures
        self.validation_error_counter = Counter('misp_validation_errors_total', 'Total MISP message schema validation failures', registry=METRICS_REGISTRY)
         
        # --- Initialize new components ---
        self.feed_normalizer = FeedDataNormalizer()
        self.feed_enricher = DSPyFeedEnricher() # Assumes global DSPy LM config
        if dspy.settings.lm is None:
            print("[MISP] CRITICAL: DSPy LM not configured globally. Enrichment will fail.", file=sys.stderr)

        # --- Kafka Topics --- (Assuming a unified raw.intel topic)
        config_path_env = os.getenv("AGENT_CONFIG_PATH", "agents/config.yaml")
        agent_config = {}
        try:
            with open(config_path_env, 'r') as f:
                agent_config = yaml.safe_load(f) or {}
        except Exception:
            pass # Defaults will be used
        kafka_topics_config = agent_config.get('kafka_topics', {})
        self.raw_intel_topic = kafka_topics_config.get('raw_intel', 'raw.intel')
        print(f"[MISP] Publishing to raw intel topic: {self.raw_intel_topic}", file=sys.stderr)

    def run_once(self):
        for feed_cfg in self.feeds:
            index_url = feed_cfg.get('index_url')
            MISP_INDEX_FETCH_COUNTER.inc()
            try:
                # entries from index typically: [{'url': 'actual_feed_data_url', 'name': '...', 'provider': '...', ...}, ...]
                feed_index_entries = self.tool.get_feed_index(index_url)
            except Exception as e:
                print(f"[MISP] Error fetching index {index_url}: {e}", file=sys.stderr)
                MISP_ERROR_COUNTER.inc()
                continue

            for feed_entry_info in feed_index_entries: # feed_entry_info is from the index
                actual_feed_data_url = feed_entry_info.get('url')
                if not actual_feed_data_url:
                    print(f"[MISP] Skipping entry in index {index_url} with no 'url': {feed_entry_info.get('name')}", file=sys.stderr)
                    continue
                
                # Key for state tracking (currently for the whole feed URL, not per IOC)
                state_key = f"{index_url}|{actual_feed_data_url}"
                # last_processed_ioc_ts = self.state.get(state_key) # TODO: Refine state for per-IOC tracking

                MISP_FEED_FETCH_COUNTER.inc()
                try:
                    # content is the raw MISP feed data (e.g., JSON for a MISP event bundle)
                    feed_content_blob = self.tool.fetch_feed(actual_feed_data_url)
                    # ioc_records are individual IOCs/attributes parsed from feed_content_blob
                    # The structure of ioc_record depends on MispFeedTool.parse_feed
                    ioc_records = self.tool.parse_feed(feed_entry_info, feed_content_blob)
                except Exception as e:
                    print(f"[MISP] Error fetching/parsing feed {actual_feed_data_url}: {e}", file=sys.stderr)
                    MISP_ERROR_COUNTER.inc()
                    continue

                # new_latest_ioc_ts = last_processed_ioc_ts # For per-IOC state update

                for ioc_rec in ioc_records: # ioc_rec is an individual parsed IOC
                    # Deduplication: skip if UUID seen
                    ioc_uuid = ioc_rec.get('uuid')
                    if ioc_uuid:
                        dedupe_key = f"dedupe:misp:{ioc_uuid}"
                        if not self.dedupe_store.set_if_not_exists(dedupe_key, DEDUP_TTL_SECONDS):
                            continue

                    # --- Create FeedRecord from ioc_rec --- 
                    try:
                        # Use actual_feed_data_url as primary FeedRecord.url, details in metadata
                        # Or, if ioc_rec has a direct link to an event/attribute on a MISP instance, use that.
                        record_url = actual_feed_data_url 
                        ioc_value = str(ioc_rec.get('value', ''))
                        ioc_type = str(ioc_rec.get('type', 'unknown'))
                        
                        title = ioc_rec.get('comment') or f"{ioc_type}: {ioc_value}" 
                        if not title and ioc_rec.get('object_relation'): # For MISP objects
                            title = f"MISP Object: {ioc_rec.get('object_relation')} - {ioc_value[:100]}"

                        description = ioc_rec.get('to_ids', False) and "Contains IDs, potentially related to specific malware or threat actor" or ioc_rec.get('comment')
                        if not description and ioc_value and len(ioc_value) > 100:
                             description = f"IOC Value (partial): {ioc_value[:100]}..."
                        elif not description:
                            description = f"MISP IOC of type {ioc_type}"

                        published_ts = parse_misp_date_to_datetime(
                            ioc_rec.get('timestamp') or ioc_rec.get('first_seen') or ioc_rec.get('date')
                        )

                        feed_record = FeedRecord(
                            url=AnyUrl(record_url), # URL of the MISP feed file/event for now
                            title=title,
                            description=description,
                            published_at=published_ts,
                            discovered_at=datetime.now(timezone.utc),
                            source_name=urlparse(index_url).netloc, # Domain of the MISP server/index
                            source_type='misp_feed',
                            tags=[tag.get('name') for tag in ioc_rec.get('Tag', []) if tag.get('name')], # MISP attribute tags
                            raw_content_type='json',
                            raw_content=json.dumps(ioc_rec), # The individual IOC record as JSON
                            metadata={
                                'misp_ioc_record': ioc_rec,
                                'misp_feed_url': actual_feed_data_url,
                                'misp_index_url': index_url,
                                'misp_event_id': ioc_rec.get('event_id') or feed_entry_info.get('event_id'), # from ioc or feed entry
                                'misp_attribute_uuid': ioc_rec.get('uuid')
                            }
                        )
                    except ValidationError as ve:
                        print(f"[MISP] Pydantic validation error for IOC from {actual_feed_data_url}: {ve}", file=sys.stderr)
                        continue 
                    except Exception as e_create:
                        print(f"[MISP] Error creating FeedRecord for IOC from {actual_feed_data_url}: {e_create}", file=sys.stderr)
                        continue

                    # Normalize
                    try:
                        normalized_record = self.feed_normalizer.normalize_feed_record(feed_record)
                    except Exception as e_norm:
                        print(f"[MISP] Error normalizing IOC {ioc_value}: {e_norm}. Using unnormalized.", file=sys.stderr)
                        normalized_record = feed_record
                        normalized_record.metadata.normalization_status = 'error'
                        normalized_record.metadata.normalization_error = str(e_norm)
                    else:
                        normalized_record.metadata.normalization_status = 'success'

                    # Enrich (e.g., classify if IOC implies security focus, standardize vendor if IOC is product-related)
                    # Condition: always for MISP IOCs as they are structured and DSPy can add value.
                    if normalized_record.raw_content: # Ensure we have content for enrichment
                        print(f"[MISP] Enriching IOC: {ioc_value} from {actual_feed_data_url}", file=sys.stderr)
                        try:
                            enriched_record = self.feed_enricher.enrich(normalized_record)
                        except Exception as e_enrich:
                            print(f"[MISP] Error during enrichment call for IOC {ioc_value}: {e_enrich}", file=sys.stderr)
                            enriched_record = normalized_record # Fallback
                    else:
                        enriched_record = normalized_record
                        enriched_record.metadata.dspy_enrichment_status = 'skipped_no_raw_content'

                    # Validate and publish enriched IOCs to Kafka
                    try:
                        payload = enriched_record.model_dump()
                        # Validate against schema
                        self.schema_validator.validate(self.raw_intel_topic, payload)
                    except SchemaValidationError as e_val:
                        # Schema validation failed: send to dead-letter
                        self.validation_error_counter.inc()
                        dlq_topic = f"dead-letter.{self.raw_intel_topic}"
                        self.producer.send(dlq_topic, {"error_type": "SchemaValidationError", "details": str(e_val), "message": payload})
                        continue
                    except Exception as e_val_other:
                        # Unexpected validation error
                        print(f"[MISP] Unexpected validation error: {e_val_other}", file=sys.stderr)
                        MISP_ERROR_COUNTER.inc()
                        continue
                    # Schema valid; send to main topic
                    try:
                        self.producer.send(self.raw_intel_topic, enriched_record)
                        MISP_IOCS_PUBLISHED_COUNTER.inc()
                    except Exception as e_kafka:
                        print(f"[MISP] Kafka send error for IOC {ioc_value}: {e_kafka}", file=sys.stderr)
                        MISP_ERROR_COUNTER.inc()

                    # Update latest timestamp for per-IOC state (if implemented)
                    # if published_ts and (not new_latest_ioc_ts or published_ts > parse_misp_date_to_datetime(new_latest_ioc_ts)):
                    #    new_latest_ioc_ts = published_ts.isoformat()

                # TODO: update self.state[state_key] = new_latest_ioc_ts if per-IOC state is robustly implemented
                # For now, this agent might re-process IOCs if the feed file itself doesn't change its 'modified' time in index often.

    def run(self):
        print(f"[MISP] Starting MispFeedAgent for indices: {[f.get('index_url') for f in self.feeds]}", file=sys.stderr)
        self.run_once()
        if os.getenv('MISP_ONCE') == '1':
            return
        while True:
            interval = next((f.get('poll_interval') for f in self.feeds if f.get('poll_interval')), 3600)
            time.sleep(interval)
            self.run_once()
            if os.getenv('MISP_ONCE') == '1':
                break


if __name__ == '__main__':
    # Start Prometheus metrics server
    port = int(os.getenv('PROMETHEUS_PORT_COLLECTOR', '9464'))
    start_http_server(port, registry=METRICS_REGISTRY)
    print(f"[MISP] Prometheus metrics on port {port}", file=sys.stderr)
    # Load configuration
    config_path = os.getenv('MISP_CONFIG_PATH', os.path.join(os.getcwd(), 'config.yaml'))
    if not os.path.isfile(config_path):
        print(f"Config file not found at {config_path}")
        exit(1)
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    feeds = cfg.get('misp_feeds', [])
    if not feeds:
        print('No MISP feed indices configured. Please add entries to config.yaml under misp_feeds')
        exit(1)
    bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    agent = MispFeedAgent(feeds, bootstrap)
    agent.run()
