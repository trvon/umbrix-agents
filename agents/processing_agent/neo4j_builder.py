from google.adk.agents import Agent
from kafka import KafkaConsumer
from common_tools.graph_tools import Neo4jWriterTool
import json


class Neo4jGraphBuilderAgent(Agent):
    """ADK Agent to build and update a Neo4j graph from enriched.intel messages."""
    class Config:
        extra = "allow"
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        super().__init__(name="neo4j_builder", description="Builds Neo4j graph from enriched.intel messages")
        # Consume enriched intelligence
        self.consumer = KafkaConsumer(
            'enriched.intel',
            bootstrap_servers=bootstrap_servers,
            group_id='neo4j_builder_group',
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v)
        )
        self.writer = Neo4jWriterTool()

    def run(self):
        for msg in self.consumer:
            enriched = msg.value
            raw = enriched.get('original_raw_data_ref', {})
            src_url = raw.get('source_url')
            ts = raw.get('retrieved_at')
            # Merge Source node
            cypher = 'MERGE (s:Source {url: $url})'
            self.writer.call(cypher, {'url': src_url})
            # Merge SightingEvent node
            cypher = 'MERGE (e:SightingEvent {timestamp: $ts})'
            self.writer.call(cypher, {'ts': ts})
            # Link event to source
            cypher = (
                'MATCH (s:Source {url: $url}), (e:SightingEvent {timestamp: $ts}) '
                'MERGE (e)-[:REPORTED_BY]->(s)'
            )
            self.writer.call(cypher, {'url': src_url, 'ts': ts})
            # Persist full article text as Article node and link to event
            full_text = raw.get('full_text', '')
            if full_text:
                cypher = 'MERGE (a:Article {url: $url}) SET a.full_text = $full_text'
                self.writer.call(cypher, {'url': src_url, 'full_text': full_text})
                cypher = (
                    'MATCH (a:Article {url: $url}), (e:SightingEvent {timestamp: $ts}) '
                    'MERGE (a)-[:ABOUT]->(e)'
                )
                self.writer.call(cypher, {'url': src_url, 'ts': ts})
            # Merge Indicator nodes and link to event
            for ind in enriched.get('indicators', []):
                value = ind.get('value')
                type_ = ind.get('type')
                cypher = 'MERGE (i:Indicator {value: $value, type: $type})'
                self.writer.call(cypher, {'value': value, 'type': type_})
                cypher = (
                    'MATCH (i:Indicator {value: $value, type: $type}), (e:SightingEvent {timestamp: $ts}) '
                    'MERGE (i)-[:APPEARED_IN]->(e)'
                )
                self.writer.call(cypher, {'value': value, 'type': type_, 'ts': ts})
            # Link Indicators to GeoIP countries
            for geo in enriched.get('geo_locations', []):
                country = geo.get('country')
                ip = geo.get('ip')
                # Ensure Country node
                cypher = 'MERGE (c:Country {name: $country})'
                self.writer.call(cypher, {'country': country})
                # Link indicator to country
                cypher = (
                    'MATCH (i:Indicator {value: $ip, type: "ipv4"}), (c:Country {name: $country}) '
                    'MERGE (i)-[:LOCATED_IN]->(c)'
                )
                self.writer.call(cypher, {'ip': ip, 'country': country})


if __name__ == '__main__':
    agent = Neo4jGraphBuilderAgent()
    agent.run() 