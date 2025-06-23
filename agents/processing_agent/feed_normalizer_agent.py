import json
import sys
import time # Required for tenacity's wait strategies, though not directly used for sleep
from kafka import KafkaConsumer, KafkaProducer
from google.adk.agents import Agent
from common_tools.dspy_extraction_tool import DSPyExtractionTool
from langdetect import detect, LangDetectException
from googletrans import Translator
from common_tools.retry_framework import retry_with_policy
from datetime import datetime, timezone # ADDED
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from common_tools.schema_validator import SchemaValidator
from jsonschema import ValidationError as SchemaValidationError
from prometheus_client import Counter, CollectorRegistry

# Specific exceptions for retry handling
import httpx # googletrans uses httpx
import requests  # For catching HTTPError during tool enrichment
from google.api_core import exceptions as google_api_exceptions # Assuming DSPyExtractionTool uses Google backend like Gemini
import logging

from auth_client import AgentHttpClient  # For calling CTI-backend tool endpoints
from pydantic import BaseModel, ValidationError
from typing import Optional, List, Dict
import re
# Create a registry for this agent's metrics
METRICS_REGISTRY = CollectorRegistry()
logger = logging.getLogger(__name__) # For tenacity's before_sleep_log

# Define exceptions that should trigger a retry
RETRYABLE_EXCEPTIONS = (
    httpx.HTTPStatusError,  # For HTTP errors like 429 (Too Many Requests), 5xx (Server Errors) from googletrans
    httpx.RequestError,     # For general network issues like timeouts, connection errors with googletrans
    google_api_exceptions.ResourceExhausted,  # Typically HTTP 429 for Google APIs
    google_api_exceptions.ServiceUnavailable, # Typically HTTP 503 for Google APIs
    google_api_exceptions.DeadlineExceeded,   # For timeouts with Google APIs
    google_api_exceptions.InternalServerError, # Typically HTTP 500 for Google APIs
)

# Helper function to parse and normalize timestamps
def normalize_timestamp(timestamp_val) -> str:
    if not timestamp_val:
        return None

    if isinstance(timestamp_val, (int, float)):
        # Assume POSIX timestamp (seconds since epoch)
        # If it's in milliseconds, divide by 1000
        if timestamp_val > 1e12: # A simple heuristic for milliseconds vs seconds
            timestamp_val /= 1000
        try:
            dt_object = datetime.fromtimestamp(timestamp_val, tz=timezone.utc)
            return dt_object.isoformat(timespec='seconds')
        except (TypeError, ValueError) as e:
            print(f"[NORMALIZER] Could not convert numeric timestamp '{timestamp_val}' to datetime: {e}", file=sys.stderr)
            return str(timestamp_val) # Return original if conversion fails

    if isinstance(timestamp_val, str):
        # Attempt to parse common string formats
        # Order matters: try more specific formats first
        formats_to_try = [
            "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO 8601 with milliseconds and Z
            "%Y-%m-%dT%H:%M:%SZ",     # ISO 8601 with Z
            "%Y-%m-%d %H:%M:%S%z",    # e.g., 2023-10-26 10:00:00+0000
            "%Y-%m-%d %H:%M:%S",      # Naive datetime, assume UTC
            "%Y-%m-%dT%H:%M:%S.%f",   # ISO 8601 with microseconds, assume UTC if no tz
            "%Y-%m-%dT%H:%M:%S",      # ISO 8601 without Z, assume UTC if no tz
            "%a, %d %b %Y %H:%M:%S %Z", # RFC 822 / RFC 1123 (e.g., Tue, 15 Nov 1994 12:45:26 GMT)
            "%a, %d %b %Y %H:%M:%S %z", # RFC 822 / RFC 1123 with offset
            "%Y-%m-%d",               # Date only
        ]
        for fmt in formats_to_try:
            try:
                dt_object = datetime.strptime(timestamp_val, fmt)
                # If timezone is naive, assume UTC
                if dt_object.tzinfo is None or dt_object.tzinfo.utcoffset(dt_object) is None:
                    dt_object = dt_object.replace(tzinfo=timezone.utc)
                else:
                    dt_object = dt_object.astimezone(timezone.utc) # Convert to UTC
                return dt_object.isoformat(timespec='seconds')
            except ValueError:
                continue
        print(f"[NORMALIZER] Could not parse timestamp string '{timestamp_val}' with known formats.", file=sys.stderr)
        return timestamp_val # Return original if all parsing attempts fail
    
    # If not a recognized type, return string representation
    print(f"[NORMALIZER] Unknown timestamp type for value '{timestamp_val}'. Returning as string.", file=sys.stderr)
    return str(timestamp_val)

# Add Pydantic models for CTI custom tools
class ThreatActorDetails(BaseModel):
    name: Optional[str]
    description: Optional[str]
    aliases: List[str] = []
    sophistication: Optional[str]
    primary_motivation: Optional[str]
    resource_level: Optional[str]
    first_seen: Optional[str]
    last_seen: Optional[str]
    associated_ttps: List[str] = []
    associated_malware: List[str] = []
    associated_campaigns: List[str] = []

class MalwareDetails(BaseModel):
    name: Optional[str]
    aliases: List[str] = []
    description: Optional[str]
    malware_types: List[str] = []
    is_family: Optional[bool]
    first_seen: Optional[str]
    last_seen: Optional[str]
    capabilities_description: Optional[str]
    associated_ttps: List[Dict] = []
    associated_actors: List[Dict] = []
    associated_campaigns: List[Dict] = []
    message: Optional[str]
    identifier_searched: Optional[str]

class CampaignDetails(BaseModel):
    id: Optional[str]
    name: Optional[str]
    aliases: List[str] = []
    description: Optional[str]
    objective: Optional[str]
    first_seen: Optional[str]
    last_seen: Optional[str]
    targeted_sectors: List[str] = []
    targeted_regions: List[str] = []
    associated_actors: List[Dict] = []
    associated_malware: List[Dict] = []
    associated_ttps: List[Dict] = []
    message: Optional[str]
    identifier_searched: Optional[str]

class VulnerabilityDetails(BaseModel):
    id: Optional[str]
    name: Optional[str]
    description: Optional[str]
    cvss_v2_score: Optional[float]
    cvss_v2_vector: Optional[str]
    cvss_v3_score: Optional[float]
    cvss_v3_vector: Optional[str]
    published_date: Optional[str]
    modified_date: Optional[str]
    references: List[str] = []
    affected_software: List[Dict] = []
    exploited_by_malware: List[Dict] = []
    exploited_by_ttps: List[Dict] = []
    message: Optional[str]
    identifier_searched: Optional[str]

class FeedNormalizationAgent(Agent):
    """Normalizes raw feed/chat/tweet events into a canonical schema and publishes them."""
    class Config:
        extra = "allow"

    def __init__(self,
                 bootstrap_servers: str = 'localhost:9092',
                 input_topic: str = 'raw.intel',
                 output_topic: str = 'normalized.intel',
                 name: str = 'feed_normalizer',
                 description: str = 'Normalize raw events'):
        super().__init__(name=name, description=description)
        self.input_topic = input_topic
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f"{self.name}_group"
        )
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.output_topic = output_topic
        self.extractor = DSPyExtractionTool(model_name="gemini-2.5-flash-preview-04-17")
        # Initialize translator for multilingual feeds
        self.translator = Translator()
        # HTTP client for CTI-backend tool API calls
        self.http_client = AgentHttpClient()
        # Schema validator for outgoing messages
        self.schema_validator = SchemaValidator()
        # Validation error metric
        self.validation_error_counter = Counter('feed_normalizer_validation_errors_total', 'Total feed normalizer message schema validation failures', registry=METRICS_REGISTRY)

    def _translate_with_retry(self, text: str, src_lang: str, dest_lang: str) -> str:
        """Translates text with retry logic using 'external_apis' policy."""
        import sys
        print(f"[NORMALIZER] Attempting translation from {src_lang} for text snippet: '{text[:50]}...'" , file=sys.stderr)
        # Decorate translator.translate with external_apis policy
        translator_fn = retry_with_policy('external_apis')(self.translator.translate)
        return translator_fn(text, src=src_lang, dest=dest_lang).text

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )
    def _extract_with_retry(self, text: str) -> dict:
        """Extracts entities and relations with retry logic."""
        print(f"[NORMALIZER] Attempting extraction for text snippet: '{text[:50]}...'", file=sys.stderr)
        return self.extractor.extract(text=text)

    def run(self):
        print(f"[NORMALIZER] Listening on topic '{self.input_topic}'", file=sys.stderr)
        for msg in self.consumer:
            raw = msg.value
            text = raw.get('full_text') or raw.get('content') or ''
            
            lang = 'unknown'
            if text: # Only attempt language detection if there's text
                try:
                    lang = detect(text)
                except LangDetectException:
                    print(f"[NORMALIZER] Language detection failed for text snippet: '{text[:100]}...'. Defaulting to 'unknown'.", file=sys.stderr)
                    lang = 'unknown' # Explicitly set, though already default
            
            text_en = text # Default to original text
            translated_text_for_output = None # Store the translated text if successful

            # Detect language
            if lang and lang != 'en' and text: # Ensure text is not empty before translating
                try:
                    translated_text = self._translate_with_retry(text, src_lang=lang, dest_lang='en')
                    text_en = translated_text # Use translated text for extraction
                    translated_text_for_output = translated_text
                except Exception as e: # Catch if all retries failed
                    print(f"[NORMALIZER] Translation permanently failed for text (lang: {lang}) after retries: {e}. Proceeding with original text for extraction.", file=sys.stderr)
                    # text_en remains the original non-English text
            
            extracted_data = {'entities': {}, 'relations': []} # Default if extraction fails or text_en is empty
            if text_en: # Only attempt extraction if there's text (original or translated)
                try:
                    extracted_data = self._extract_with_retry(text=text_en)
                except Exception as e: # Catch if all retries failed
                    print(f"[NORMALIZER] Extraction permanently failed for text after retries: {e}. Proceeding with empty entities/relations.", file=sys.stderr)
                    # extracted_data remains the default

            # Normalize timestamp
            original_timestamp = raw.get('retrieved_at', raw.get('timestamp'))
            normalized_ts = normalize_timestamp(original_timestamp)

            normalized = {
                'id': raw.get('id') or raw.get('guid') or raw.get('source_url'),
                'language': lang,
                'translated_text': translated_text_for_output,
                'source_type': raw.get('content_type', raw.get('source_type', 'unknown')),
                'timestamp': normalized_ts, # MODIFIED
                'entities': extracted_data.get('entities', {}),
                'relations': extracted_data.get('relations', []),
                'raw_payload': raw
            }

            # Enrich Attack Pattern entities via CTI backend
            # Expect extracted_data['entities'] to have 'attack_pattern' key with list of IDs
            ttp_ids = extracted_data.get('entities', {}).get('attack_pattern', []) or []
            enriched_ttps: List[Dict] = []
            for ttp in ttp_ids:
                try:
                    data = self.http_client.post(
                        f"/v1/tools/get_attack_pattern_details",
                        json={"attack_pattern_identifier": ttp},
                        timeout=30
                    ).json()
                    # Skip if not found to avoid hallucinating patterns
                    if data.get("message"):
                        continue
                    # Validate with Pydantic model
                    class AttackPatternDetails(BaseModel):
                        id: Optional[str]
                        name: Optional[str]
                        aliases: List[str] = []
                        description: Optional[str]
                        mitre_attack_id: Optional[str]
                        mitre_url: Optional[str]
                        detection: Optional[str]
                        mitigation: Optional[str]
                        platforms: List[str] = []
                        permissions_required: List[str] = []
                        data_sources: List[str] = []
                        associated_actors: List[Dict] = []
                        associated_malware: List[Dict] = []
                        associated_campaigns: List[Dict] = []
                    details = AttackPatternDetails(**data)
                    enriched_ttps.append(details.dict(exclude_none=True))
                except (requests.HTTPError, ValidationError, KeyError) as e:
                    print(f"[NORMALIZER] AttackPattern enrichment failed for {ttp}: {e}", file=sys.stderr)
            if enriched_ttps:
                normalized['attack_pattern_details'] = enriched_ttps

            # Enrich threat actor entities
            actor_ids = extracted_data.get('entities', {}).get('threat_actor', []) or []
            threat_actors = []
            for actor in actor_ids:
                try:
                    resp = self.http_client.post(
                        "/v1/tools/get_threat_actor_summary",
                        json={"actor_name": actor}, timeout=30)
                    data = resp.json()
                    if data.get("message"): continue
                    threat_actors.append(ThreatActorDetails(**data).dict(exclude_none=True))
                except (requests.HTTPError, ValidationError, KeyError) as e:
                    print(f"[NORMALIZER] ThreatActor enrichment failed for {actor}: {e}", file=sys.stderr)
            if threat_actors:
                normalized['threat_actor_details'] = threat_actors

            # Enrich malware entities
            malware_ids = extracted_data.get('entities', {}).get('malware', []) or []
            malwares = []
            for m in malware_ids:
                try:
                    data = self.http_client.post(
                        "/v1/tools/get_malware_details",
                        json={"malware_identifier": m}, timeout=30).json()
                    if data.get("message"): continue
                    malwares.append(MalwareDetails(**data).dict(exclude_none=True))
                except (requests.HTTPError, ValidationError, KeyError) as e:
                    print(f"[NORMALIZER] Malware enrichment failed for {m}: {e}", file=sys.stderr)
            if malwares:
                normalized['malware_details'] = malwares

            # Enrich campaign entities
            campaign_ids = extracted_data.get('entities', {}).get('campaign', []) or []
            campaigns = []
            for c in campaign_ids:
                try:
                    data = self.http_client.post(
                        "/v1/tools/get_campaign_details",
                        json={"campaign_identifier": c}, timeout=30).json()
                    if data.get("message"): continue
                    campaigns.append(CampaignDetails(**data).dict(exclude_none=True))
                except (requests.HTTPError, ValidationError, KeyError) as e:
                    print(f"[NORMALIZER] Campaign enrichment failed for {c}: {e}", file=sys.stderr)
            if campaigns:
                normalized['campaign_details'] = campaigns

            # Enrich vulnerability entities
            vuln_ids = extracted_data.get('entities', {}).get('vulnerability', []) or []
            vulnerabilities = []
            for v in vuln_ids:
                try:
                    data = self.http_client.post(
                        "/v1/tools/get_vulnerability_details",
                        json={"vulnerability_identifier": v}, timeout=30).json()
                    if data.get("message"): continue
                    vulnerabilities.append(VulnerabilityDetails(**data).dict(exclude_none=True))
                except (requests.HTTPError, ValidationError, KeyError) as e:
                    print(f"[NORMALIZER] Vulnerability enrichment failed for {v}: {e}", file=sys.stderr)
            if vulnerabilities:
                normalized['vulnerability_details'] = vulnerabilities

            # Validate against schema before publishing
            try:
                self.schema_validator.validate(self.output_topic, normalized)
            except SchemaValidationError as e_val:
                # Send to dead-letter queue
                self.validation_error_counter.inc()
                dlq_topic = f"dead-letter.{self.output_topic}"
                self.producer.send(dlq_topic, {"error_type": "SchemaValidationError", "details": str(e_val), "message": normalized})
                print(f"[NORMALIZER] Schema validation failed: {e_val}", file=sys.stderr)
                continue

            # Publish validated message
            self.producer.send(self.output_topic, normalized)