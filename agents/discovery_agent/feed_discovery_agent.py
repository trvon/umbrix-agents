#!/usr/bin/env python3
"""
Consolidated Feed Discovery Agent: Discovers CTI feeds (RSS/Atom/TAXII) using shared discovery tools.
Consolidates functionality from the original FeedDiscoveryAgent and RssDiscoveryAgent.
"""

import os
import sys
import yaml
import json
import logging
import asyncio
from typing import Dict, List, Optional, Set, Union, Any
from datetime import datetime, timezone
from urllib.parse import urlparse

from google.adk.agents import Agent
from prometheus_client import Counter, Gauge, start_http_server, CollectorRegistry
from kafka import KafkaProducer

from common_tools.schema_validator import SchemaValidator
from common_tools.dedupe_store import RedisDedupeStore
from common_tools.models import FeedRecord
from common_tools.normalizers import FeedDataNormalizer
from discovery_agent.discovery_tools import (
    FeedDiscoveryTools, 
    FeedValidationTools, 
    DiscoveredFeed,
    discover_feeds_from_urls
)
from jsonschema import ValidationError as SchemaValidationError


# Create a registry for this agent's metrics
METRICS_REGISTRY = CollectorRegistry()

# Prometheus metrics
SITES_CRAWLED_COUNTER = Counter(
    'feed_discovery_sites_crawled_total', 
    'Total sites crawled', 
    registry=METRICS_REGISTRY
)
FEEDS_FOUND_COUNTER = Counter(
    'feed_discovery_feeds_found_total', 
    'Total feeds discovered across sites', 
    ['feed_type'], 
    registry=METRICS_REGISTRY
)
DISCOVERY_ERROR_COUNTER = Counter(
    'feed_discovery_errors_total', 
    'Total errors during discovery', 
    ['error_type'], 
    registry=METRICS_REGISTRY
)
FEEDS_PENDING_GAUGE = Gauge(
    'feed_discovery_feeds_pending_approval', 
    'Number of feeds pending approval', 
    registry=METRICS_REGISTRY
)
VALIDATION_ERROR_COUNTER = Counter(
    'feed_discovery_validation_errors_total', 
    'Total feed discovery message schema validation failures', 
    registry=METRICS_REGISTRY
)


class ConsolidatedFeedDiscoveryAgent(Agent):
    """
    Consolidated agent for discovering various threat intelligence feeds.
    
    This agent combines the functionality of the original FeedDiscoveryAgent 
    and RssDiscoveryAgent, using shared discovery tools for better maintainability.
    """
    
    class Config:
        extra = "allow"

    def __init__(self, 
                 kafka_bootstrap_servers: str = None, 
                 topic: str = 'feeds.discovered', 
                 config_path: str = None,
                 name: str = 'consolidated_feed_discovery', 
                 description: str = 'Discovers cyber threat intel feeds using consolidated discovery tools',
                 **kwargs):
        super().__init__(name=name, description=description)
        
        # Setup logging
        logging.basicConfig(
            level=os.getenv("LOG_LEVEL", "INFO"),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            stream=sys.stderr
        )
        self.logger = logging.getLogger("ConsolidatedFeedDiscoveryAgent")
        
        # Configure Kafka
        self.bootstrap_servers = kafka_bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        self.topic = self._resolve_topic(topic, config_path)
        
        # Setup Kafka producer
        self.producer = None
        if self.bootstrap_servers:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=self._serialize_message
                )
                self.logger.info(f"Kafka producer initialized for topic: {self.topic}")
            except Exception as e:
                self.logger.error(f"KafkaProducer init failure: {e}")
                
        # Initialize deduplication store
        redis_url = os.getenv('REDIS_URL', 'redis://redis:6379')
        self.dedupe_store = RedisDedupeStore(redis_url)
        self.dedup_ttl_seconds = int(os.getenv('REDIS_DEDUPE_TTL_SECONDS', 86400))
        
        # Initialize schema validator
        self.schema_validator = SchemaValidator()
        
        # Get seed URLs from environment or use defaults
        self.seed_urls = self._get_seed_urls()
        
        # Configure discovery settings
        self.enable_rss_discovery = os.getenv("ENABLE_RSS_DISCOVERER", "true").lower() == "true"
        self.enable_taxii_discovery = os.getenv("ENABLE_TAXII_DISCOVERER", "true").lower() == "true"
        self.enable_json_discovery = os.getenv("ENABLE_JSON_DISCOVERER", "true").lower() == "true"
        self.cti_relevance_filter = os.getenv("FILTER_CTI_RELEVANT", "true").lower() == "true"
        
        # Track config path for potential feed merging
        self.config_path = self._resolve_config_path(config_path)
        
        # Start Prometheus metrics server if requested
        self._start_metrics_server()
        
        self.logger.info(f"Initialized with {len(self.seed_urls)} seed URLs")
        self.logger.info(f"Discovery settings: RSS={self.enable_rss_discovery}, TAXII={self.enable_taxii_discovery}, JSON={self.enable_json_discovery}")
    
    def _serialize_message(self, message):
        """Serialize message for Kafka producer."""
        if isinstance(message, FeedRecord):
            return message.model_dump_json().encode('utf-8')
        elif isinstance(message, dict):
            return json.dumps(message).encode('utf-8')
        else:
            return json.dumps(message).encode('utf-8')
    
    def _resolve_topic(self, topic: str, config_path: Optional[str]) -> str:
        """Resolve topic name from config if it's a placeholder."""
        actual_config_path = os.getenv("AGENT_CONFIG_PATH") or config_path or "agents/config.yaml"
        
        try:
            with open(actual_config_path, 'r') as f:
                cfg = yaml.safe_load(f) or {}
        except (FileNotFoundError, yaml.YAMLError) as e:
            self.logger.warning(f"Could not load config from {actual_config_path}: {e}")
            return topic

        # Resolve placeholder topics like ${kafka.topics.feeds_discovered}
        if topic.startswith("${") and topic.endswith("}"):
            parts = topic[2:-1].split('.')
            temp_val = cfg
            for part in parts:
                if isinstance(temp_val, dict) and part in temp_val:
                    temp_val = temp_val[part]
                else:
                    self.logger.warning(f"Could not resolve topic placeholder '{topic}'")
                    return topic
            
            if isinstance(temp_val, str):
                self.logger.info(f"Resolved topic placeholder '{topic}' to '{temp_val}'")
                return temp_val
        
        return topic
    
    def _resolve_config_path(self, config_path: Optional[str]) -> str:
        """Resolve configuration file path."""
        return (os.getenv("AGENT_CONFIG_PATH") or 
                config_path or 
                os.path.join(os.getcwd(), "config.yaml"))
    
    def _get_seed_urls(self) -> List[str]:
        """Get seed URLs from environment or defaults."""
        # RSS/General discovery seeds
        seeds_env = os.getenv("FEED_DISCOVERY_SEEDS") or os.getenv("RSS_DISCOVERY_SEEDS")
        if seeds_env:
            seed_urls = [url.strip() for url in seeds_env.split(",") if url.strip()]
        else:
            # Default cybersecurity news and CTI sources
            seed_urls = [
                "https://troyhunt.com",
                "https://thehackernews.com", 
                "https://krebsonsecurity.com",
                "https://www.schneier.com",
                "https://blog.virustotal.com",
                "https://www.us-cert.gov",
                "https://otx.alienvault.com",
                "https://www.misp-project.org",
            ]
        
        # Add TAXII-specific seeds if TAXII discovery is enabled
        taxii_seeds_env = os.getenv("TAXII_DISCOVERY_SEEDS")
        if taxii_seeds_env and self.enable_taxii_discovery:
            taxii_seeds = [url.strip() for url in taxii_seeds_env.split(",") if url.strip()]
            seed_urls.extend(taxii_seeds)
            self.logger.info(f"Added {len(taxii_seeds)} TAXII-specific seed URLs")
        
        return seed_urls
    
    def _start_metrics_server(self):
        """Start Prometheus metrics server if configured."""
        metrics_port = os.getenv("METRICS_PORT")
        if metrics_port:
            try:
                start_http_server(int(metrics_port), registry=METRICS_REGISTRY)
                self.logger.info(f"Started Prometheus metrics server on port {metrics_port}")
            except Exception as e:
                self.logger.error(f"Failed to start Prometheus metrics server: {e}")
    
    async def discover_feeds(self) -> List[DiscoveredFeed]:
        """Discover feeds from all seed URLs using consolidated discovery tools."""
        all_discovered = []
        
        self.logger.info(f"Starting discovery across {len(self.seed_urls)} seed URLs")
        
        # Use the shared discovery tools
        discovered_feeds = await discover_feeds_from_urls(self.seed_urls)
        
        for feed in discovered_feeds:
            SITES_CRAWLED_COUNTER.inc()
            
            # Apply discovery type filters
            if not self._should_include_feed_type(feed.feed_type):
                continue
            
            # Apply CTI relevance filter if enabled
            if self.cti_relevance_filter and not FeedValidationTools.is_cti_relevant(feed):
                self.logger.debug(f"Skipping non-CTI relevant feed: {feed.url}")
                continue
            
            # Calculate quality score
            quality_score = FeedValidationTools.calculate_feed_quality_score(feed)
            feed.confidence_score = quality_score
            
            all_discovered.append(feed)
            FEEDS_FOUND_COUNTER.labels(feed_type=feed.feed_type).inc()
        
        self.logger.info(f"Discovered {len(all_discovered)} feeds after filtering")
        return all_discovered
    
    def _should_include_feed_type(self, feed_type: str) -> bool:
        """Check if a feed type should be included based on configuration."""
        if feed_type in ['rss', 'atom'] and not self.enable_rss_discovery:
            return False
        if feed_type == 'taxii' and not self.enable_taxii_discovery:
            return False
        if feed_type == 'json' and not self.enable_json_discovery:
            return False
        return True
    
    def _convert_to_feed_record(self, discovered_feed: DiscoveredFeed) -> FeedRecord:
        """Convert DiscoveredFeed to FeedRecord for backwards compatibility."""
        return FeedRecord(
            url=discovered_feed.url,
            source_name=urlparse(discovered_feed.source_url or discovered_feed.url).netloc,
            source_type=f'{discovered_feed.feed_type}_discovered',
            raw_content_type='feed_metadata',
            raw_content=json.dumps({
                'title': discovered_feed.title,
                'description': discovered_feed.description,
                'language': discovered_feed.language,
                'last_updated': discovered_feed.last_updated,
                'discovery_method': discovered_feed.discovery_method,
                'confidence_score': discovered_feed.confidence_score
            }),
            discovered_at=datetime.now(timezone.utc)
        )
    
    def _convert_to_legacy_format(self, discovered_feed: DiscoveredFeed) -> Dict[str, Any]:
        """Convert DiscoveredFeed to legacy format for backwards compatibility."""
        return {
            "feed_url": discovered_feed.url,
            "feed_type": discovered_feed.feed_type,
            "title": discovered_feed.title,
            "description": discovered_feed.description,
            "requires_auth": False,  # Will be detected in future versions
            "payment_required": False,  # Will be detected in future versions
            "metadata": {
                "source_page": discovered_feed.source_url,
                "discovery_method": discovered_feed.discovery_method,
                "confidence_score": discovered_feed.confidence_score,
                "language": discovered_feed.language,
                "last_updated": discovered_feed.last_updated
            }
        }
    
    async def publish_discovered_feeds(self, discovered_feeds: List[DiscoveredFeed]):
        """Publish discovered feeds to Kafka, handling both new and legacy formats."""
        if not discovered_feeds or not self.producer:
            self.logger.info("No feeds to publish or producer unavailable")
            return
        
        published_count = 0
        skipped_count = 0
        
        for feed in discovered_feeds:
            # Check for duplicates
            dedupe_key = f"dedupe:feed_discovery:{feed.url}"
            if not self.dedupe_store.set_if_not_exists(dedupe_key, self.dedup_ttl_seconds):
                skipped_count += 1
                continue
            
            try:
                # Support both legacy and new formats
                use_legacy_format = os.getenv("USE_LEGACY_FEED_FORMAT", "false").lower() == "true"
                
                if use_legacy_format:
                    # Legacy format for backwards compatibility
                    message = self._convert_to_legacy_format(feed)
                else:
                    # New FeedRecord format
                    feed_record = self._convert_to_feed_record(feed)
                    message = feed_record.model_dump()
                
                # Validate message
                try:
                    self.schema_validator.validate(self.topic, message)
                except SchemaValidationError as e_val:
                    VALIDATION_ERROR_COUNTER.inc()
                    dlq_topic = f"dead-letter.{self.topic}"
                    self.producer.send(dlq_topic, {
                        "error_type": "SchemaValidationError", 
                        "details": str(e_val), 
                        "message": message
                    })
                    self.logger.error(f"Schema validation failed for {feed.url}: {e_val}")
                    continue
                
                # Send validated message
                if use_legacy_format:
                    self.producer.send(self.topic, message)
                else:
                    self.producer.send(self.topic, feed_record)
                
                published_count += 1
                self.logger.debug(f"Published feed: {feed.url}")
                
            except Exception as e:
                self.logger.error(f"Failed to publish feed {feed.url}: {e}")
                DISCOVERY_ERROR_COUNTER.labels(error_type="publish_error").inc()
        
        if published_count > 0:
            self.producer.flush()
            FEEDS_PENDING_GAUGE.set(published_count)
            
        self.logger.info(f"Published {published_count} feeds, skipped {skipped_count} duplicates")
    
    def update_config_file(self, discovered_feeds: List[DiscoveredFeed]):
        """Update configuration file with discovered feeds (legacy feature)."""
        if not self.config_path or not discovered_feeds:
            return
        
        try:
            # Load existing config
            try:
                with open(self.config_path, 'r') as f:
                    existing_cfg = yaml.safe_load(f) or {}
            except FileNotFoundError:
                existing_cfg = {}
            
            # Get existing feeds list
            feeds_list = existing_cfg.get('feeds', [])
            if not isinstance(feeds_list, list):
                feeds_list = []
            
            # Add new feeds
            added_count = 0
            for feed in discovered_feeds:
                if feed.url not in feeds_list:
                    feeds_list.append(feed.url)
                    added_count += 1
            
            # Update config
            existing_cfg['feeds'] = feeds_list
            
            # Write back to file
            with open(self.config_path, 'w') as f:
                yaml.safe_dump(existing_cfg, f)
            
            self.logger.info(f"Added {added_count} new feeds to config file: {self.config_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to update config file {self.config_path}: {e}")
    
    def run(self):
        """Main execution method - run discovery synchronously."""
        asyncio.run(self.run_async())
    
    async def run_async(self):
        """Main execution method - async version."""
        self.logger.info("Starting consolidated feed discovery")
        
        if not any([self.enable_rss_discovery, self.enable_taxii_discovery, self.enable_json_discovery]):
            self.logger.error("No discovery methods enabled. Enable at least one discovery type.")
            return
        
        try:
            # Discover feeds
            discovered_feeds = await self.discover_feeds()
            
            if not discovered_feeds:
                self.logger.info("No feeds discovered")
                return
            
            # Normalize feeds if using FeedRecord format
            if not os.getenv("USE_LEGACY_FEED_FORMAT", "false").lower() == "true":
                feed_records = [self._convert_to_feed_record(feed) for feed in discovered_feeds]
                try:
                    normalized_records = FeedDataNormalizer.normalize_feed_records(feed_records)
                    # Convert back to DiscoveredFeed format for consistency
                    # (This is a bit circular but maintains the interface)
                    self.logger.info(f"Normalized {len(normalized_records)} feed records")
                except Exception as e:
                    self.logger.warning(f"Normalization failed: {e}. Using original feeds.")
            
            # Publish to Kafka
            await self.publish_discovered_feeds(discovered_feeds)
            
            # Update config file if enabled
            if os.getenv("UPDATE_CONFIG_FILE", "false").lower() == "true":
                self.update_config_file(discovered_feeds)
            
            self.logger.info(f"Discovery completed. Found {len(discovered_feeds)} feeds.")
            
        except Exception as e:
            self.logger.error(f"Error during feed discovery: {e}")
            DISCOVERY_ERROR_COUNTER.labels(error_type="general_error").inc()
            raise
    
    def run_once(self):
        """Compatibility alias for one-shot execution."""
        self.run()


# For backwards compatibility, create aliases
FeedDiscoveryAgent = ConsolidatedFeedDiscoveryAgent
RssDiscoveryAgent = ConsolidatedFeedDiscoveryAgent


# Direct execution entry point
if __name__ == "__main__":
    # Start metrics server if running directly
    if not os.getenv("METRICS_PORT"):
        try:
            start_http_server(8000, registry=METRICS_REGISTRY)
            print("Started Prometheus metrics server on port 8000", file=sys.stderr)
        except Exception as e:
            print(f"Failed to start Prometheus metrics server: {e}", file=sys.stderr)
    
    agent = ConsolidatedFeedDiscoveryAgent()
    agent.run_once()