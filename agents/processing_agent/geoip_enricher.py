from google.adk.agents import Agent
from kafka import KafkaConsumer, KafkaProducer
from common_tools.network_tools import GeoIpLookupTool
from prometheus_client import Counter, CollectorRegistry
from common_tools.schema_validator import SchemaValidator
from jsonschema.exceptions import ValidationError as SchemaValidationError
import json
import re
import ast
import sys
import time
import os
# Create a registry for this agent's metrics
METRICS_REGISTRY = CollectorRegistry()

class GeoIpEnrichmentAgent(Agent):
    """ADK Agent to enrich raw.intel messages with GeoIP data and publish to enriched.intel."""
    class Config:
        extra = "allow"
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        super().__init__(name="geoip_enricher", description="Enrich raw intel with GeoIP context")
        # Kafka security settings (TLS/SASL)
        security_protocol = os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
        ssl_cafile = os.getenv('KAFKA_SSL_CAFILE')
        ssl_certfile = os.getenv('KAFKA_SSL_CERTFILE')
        ssl_keyfile = os.getenv('KAFKA_SSL_KEYFILE')
        sasl_mechanism = os.getenv('KAFKA_SASL_MECHANISM')
        sasl_plain_username = os.getenv('KAFKA_SASL_USERNAME')
        sasl_plain_password = os.getenv('KAFKA_SASL_PASSWORD')
        # Deserializer: try JSON, fallback to Python literal_eval
        def decode_value(v: bytes):
            # Attempt JSON decode, fallback to literal_eval, else skip message
            s = v.decode('utf-8', errors='ignore').strip()
            try:
                return json.loads(s)
            except json.JSONDecodeError:
                try:
                    return ast.literal_eval(s)
                except Exception:
                    print(f"[GeoIP] Failed to parse message: {s}", file=sys.stderr)
                    return None

        self.consumer = KafkaConsumer(
            'raw.intel', bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            group_id='geoip_enricher_group',
            auto_offset_reset='earliest',
            value_deserializer=decode_value
        )
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.lookup_tool = GeoIpLookupTool()
        # Regex to find IPv4 addresses
        self.ip_regex = re.compile(r"(?:\d{1,3}\.){3}\d{1,3}")
        # Schema validator for enriched.intel topic
        self.schema_validator = SchemaValidator()
        # Validation error metric
        self.validation_error_counter = Counter('geoip_validation_errors_total', 'Total GeoIP message schema validation failures', registry=METRICS_REGISTRY)

    def run(self):
        for message in self.consumer:
            raw = message.value
            if raw is None:
                continue
            print(f"[GeoIP] Consumed raw message: {raw}")
            text = raw.get('raw_data', '')
            ips = self.ip_regex.findall(text)
            if not ips:
                print("[GeoIP] No IP addresses found, skipping message.")
                continue
            geo_results = []
            for ip in set(ips):
                print(f"[GeoIP] Looking up {ip}")
                try:
                    info = self.lookup_tool.call(ip)
                    geo_results.append(info)
                except Exception as e:
                    print(f"[GeoIP] Lookup failed for {ip}: {e}")
            if not geo_results:
                continue
            enriched_message = {
                'original_raw_data_ref': raw,
                'indicators': [{'type': 'ipv4', 'value': ip} for ip in ips],
                'geo_locations': geo_results,
                'confidence_score': 0.7
            }
            print(f"[GeoIP] Publishing enriched message: {enriched_message}")
            # Validate before publishing
            try:
                self.schema_validator.validate('enriched.intel', enriched_message)
            except SchemaValidationError as e_val:
                # Send invalid message to dead-letter topic
                self.validation_error_counter.inc()
                dlq_topic = 'dead-letter.enriched.intel'
                self.producer.send(dlq_topic, {"error_type": "SchemaValidationError", "details": str(e_val), "message": enriched_message})
                continue
            # Publish valid message
            self.producer.send('enriched.intel', enriched_message)
            self.producer.flush()
            time.sleep(0.1)


if __name__ == '__main__':
    agent = GeoIpEnrichmentAgent()
    agent.run()