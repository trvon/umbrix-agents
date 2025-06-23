import yaml
import json
import os
import sys
from kafka import KafkaProducer
from google.adk.agents import Agent

# --- Add new imports ---
from urllib.parse import urlparse
from datetime import datetime, timezone # Added timezone
from pydantic import AnyUrl, ValidationError
from common_tools.models import FeedRecord
from common_tools.normalizers import FeedDataNormalizer # For normalizing seeded URLs
from common_tools.dedupe_store import RedisDedupeStore

class FeedSeederAgent(Agent):
    """Reads the static feeds list from agents/config.yaml and publishes each to Kafka."""
    class Config:
        extra = "allow"

    def __init__(self,
                 kafka_bootstrap_servers: str = 'localhost:9092',
                 config_path: str = None,
                 topic: str = 'feeds.discovered',
                 name: str = 'feed_seeder',
                 description: str = 'Seed initial feeds to Kafka'):
        super().__init__(name=name, description=description)
        self.bootstrap_servers = kafka_bootstrap_servers
        
        # Determine path to config.yaml
        # Use AGENT_CONFIG_PATH env var if set for Docker, then specific config_path, then default.
        actual_config_path = os.getenv("AGENT_CONFIG_PATH") or config_path or "agents/config.yaml"
        
        cfg = {}
        try:
            with open(actual_config_path, 'r') as f:
                cfg = yaml.safe_load(f) or {}
        except FileNotFoundError:
            print(f"[FEED_SEEDER] Config file not found at {actual_config_path}. Cannot resolve topics or load feeds.", file=sys.stderr)
            sys.exit(1) # Critical if config not found
        except yaml.YAMLError as e:
            print(f"[FEED_SEEDER] Error parsing YAML config at {actual_config_path}: {e}. Cannot resolve topics or load feeds.", file=sys.stderr)
            sys.exit(1) # Critical if config error

        # Resolve the topic name if it's a placeholder
        resolved_topic = topic
        if str(topic).startswith("${") and str(topic).endswith("}"):
            # Example: ${kafka_topics.feeds_discovered}
            parts = topic[2:-1].split('.') # Remove ${} and split by .
            temp_val = cfg
            valid_path = True
            for part in parts:
                if isinstance(temp_val, dict) and part in temp_val:
                    temp_val = temp_val[part]
                else:
                    valid_path = False
                    break
            if valid_path and isinstance(temp_val, str):
                resolved_topic = temp_val
                print(f"[FEED_SEEDER] Resolved topic placeholder '{topic}' to '{resolved_topic}'", file=sys.stderr)
            else:
                print(f"[FEED_SEEDER] Warning: Could not resolve topic placeholder '{topic}' from config. Using literal value.", file=sys.stderr)
        
        self.topic = resolved_topic
        self.config_path = actual_config_path # Store the used path

        # Build list of FeedRecords
        raw_feed_configs = []
        # Consolidate feed reading from config
        for feed_type_key, source_type_val, raw_content_type_val in [
            ('feeds', 'rss_seeded_from_config', 'application/rss+xml'), # Assuming these are RSS/Atom
            ('csv', 'csv_seeded_from_config', 'text/csv'),
            ('txt', 'txt_seeded_from_config', 'text/plain'),
            ('json', 'json_seeded_from_config', 'application/json')
        ]:
            for url_str in cfg.get(feed_type_key, []):
                if isinstance(url_str, str): # Basic check
                    raw_feed_configs.append({
                        'url': url_str,
                        'source_type': source_type_val,
                        'raw_content_type': raw_content_type_val
                    })
                else:
                    print(f"[FEED_SEEDER] Skipping invalid feed entry in config ({feed_type_key}): {url_str}", file=sys.stderr)
        
        temp_feed_records: list[FeedRecord] = []
        for feed_conf in raw_feed_configs:
            try:
                record = FeedRecord(
                    url=AnyUrl(feed_conf['url']),
                    source_name=urlparse(feed_conf['url']).netloc or feed_conf['url'],
                    source_type=feed_conf['source_type'],
                    raw_content_type=feed_conf['raw_content_type'],
                    discovered_at=datetime.now(timezone.utc),
                    # Other fields like title, description, raw_content, published_at are None by default
                )
                temp_feed_records.append(record)
            except ValidationError as ve:
                print(f"[FEED_SEEDER] Invalid URL for configured feed: {feed_conf['url']}. Error: {ve}", file=sys.stderr)
            except Exception as e_create:
                print(f"[FEED_SEEDER] Error creating FeedRecord for {feed_conf['url']}: {e_create}", file=sys.stderr)

        # Normalize the FeedRecords (primarily for URL canonicalization here)
        if temp_feed_records:
            try:
                self.feeds = FeedDataNormalizer.normalize_feed_records(temp_feed_records)
                print(f"[FEED_SEEDER] Successfully created and normalized {len(self.feeds)} FeedRecords from config.", file=sys.stderr)
            except Exception as e_norm:
                print(f"[FEED_SEEDER] Error normalizing seeded FeedRecords: {e_norm}. Using unnormalized.", file=sys.stderr)
                self.feeds = temp_feed_records # Fallback to unnormalized
        else:
            self.feeds = []

        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                # Updated value_serializer for FeedRecord
                value_serializer=lambda v: v.model_dump_json().encode('utf-8') if isinstance(v, FeedRecord) else json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            print(f"[FEED_SEEDER] Failed to connect to Kafka at {self.bootstrap_servers}: {e}", file=sys.stderr)
            sys.exit(1)
        # Initialize dedupe store to avoid reseeding same feed URL
        self.dedupe_store = RedisDedupeStore(os.getenv('REDIS_URL', 'redis://redis:6379'))

    def run_once(self):
        if not self.feeds:
            print("[FEED_SEEDER] No feeds found in config, nothing to seed.", file=sys.stderr)
            return
        print(f"[FEED_SEEDER] Seeding {len(self.feeds)} FeedRecords to topic '{self.topic}'", file=sys.stderr)
        for feed_record in self.feeds:
            dedupe_key = f"dedupe:feed_seeder:{feed_record.url}"
            if not self.dedupe_store.set_if_not_exists(dedupe_key, int(os.getenv('REDIS_DEDUPE_TTL_SECONDS', 86400))):
                continue
            try:
                self.producer.send(self.topic, feed_record)
                print(f"[FEED_SEEDER] Seeded feed record: {feed_record.url}", file=sys.stderr)
            except Exception as e:
                print(f"[FEED_SEEDER] Failed to seed feed record {feed_record.url}: {e}", file=sys.stderr)
        self.producer.flush()
        print(f"[FEED_SEEDER] Completed seeding {len(self.feeds)} FeedRecords to topic '{self.topic}'", file=sys.stderr)
