from google.adk.agents import Agent
from google.adk.tools import FunctionTool as Tool
from google.adk.models import Gemini
from kafka import KafkaConsumer
import os
import sys
import json
import requests  # Needed for HTTPError handling
import re
import mitreattack
import asyncio

# Flag to check if ADK model functionality is available
try:
    GOOGLE_ADK_AVAILABLE = True
except ImportError:
    print("Warning: google.adk.models module is not available. LLM functionality will be limited.", file=sys.stderr)
    GOOGLE_ADK_AVAILABLE = False

from pydantic import BaseModel, ValidationError
from typing import Optional, List, Dict
from auth_client import AgentHttpClient  # Provides JWT auth and retry handling


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


class GraphIngestionAgent(Agent):
    """ADK Agent to ingest raw intelligence into Neo4j via cti_backend API"""
    class Config:
        extra = "allow"

    def __init__(self,
                 topics=None,
                 bootstrap_servers: str = 'localhost:9092',
                 name: str = "graph_ingestion",
                 description: str = "Consumes raw intel Kafka topics and POSTs graph changeset to cti_backend",
                 enrich_with_llm: bool = False,
                 gemini_api_key: str = None,
                 gemini_model: str = None,
                 **kwargs):
        super().__init__(
            name=name,
            description=description
        )
        # subscribe to all raw intel topics
        parsed_topics = None
        if isinstance(topics, str):
            try:
                parsed_topics = json.loads(topics)
                if not isinstance(parsed_topics, list):
                    print(f"[GraphIngestion] Warning: 'topics' was a string but not a JSON list: {topics}. Falling back to default.", file=sys.stderr)
                    parsed_topics = None # Fallback to default if not a list after parsing
            except json.JSONDecodeError:
                print(f"[GraphIngestion] Warning: 'topics' string could not be parsed as JSON: {topics}. Falling back to default.", file=sys.stderr)
                parsed_topics = None # Fallback to default
        elif isinstance(topics, list):
            parsed_topics = topics

        self.topics = parsed_topics or ["raw.intel", "raw.intel.taxii", "raw.intel.misp"]
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=bootstrap_servers,
            group_id='graph_ingestion_group',
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v)
        )
        # Backend base URL is implicitly read inside AgentHttpClient via CTI_BACKEND_URL env var.
        if not os.getenv('CTI_BACKEND_URL'):
            print("[GraphIngestion] CTI_BACKEND_URL not set", file=sys.stderr)
            sys.exit(1)

        # Initialize shared HTTP client that manages JWT lifecycle & retries
        self.http_client = AgentHttpClient()

        # Tool to POST to the ingest endpoint
        self.ingest_tool = Tool(func=self._call_ingest_api)
        
        # LLM enrichment setup
        self.enrich_with_llm = enrich_with_llm
        self.llm = None
        
        # Set up Gemini if enrichment is enabled
        if self.enrich_with_llm:
            self.gemini_api_key = gemini_api_key or os.getenv('GOOGLE_API_KEY')
            self.gemini_model = gemini_model or os.getenv('GEMINI_MODEL_NAME', 'gemini-2.5-flash')
            
            if not self.gemini_api_key:
                print("[GraphIngestion] Warning: LLM enrichment enabled but no API key found. Disabling enrichment.", file=sys.stderr)
                self.enrich_with_llm = False
            elif GOOGLE_ADK_AVAILABLE:
                try:
                    self.llm = Gemini(model=self.gemini_model)
                    print(f"[GraphIngestion] LLM enrichment enabled using ADK model: {self.gemini_model}", file=sys.stderr)
                except Exception as e:
                    print(f"[GraphIngestion] Failed to initialize ADK Gemini model: {e}", file=sys.stderr)
                    self.enrich_with_llm = False
            else:
                print("[GraphIngestion] Warning: google.adk.models not available but LLM enrichment enabled. Disabling enrichment.", file=sys.stderr)
                self.enrich_with_llm = False

    def run(self):
        print(f"[GraphIngestion] Starting consumer on topics: {self.topics}", file=sys.stderr)
        for msg in self.consumer:
            record = msg.value
            try:
                changeset = self._transform(record)
                result = self._call_ingest_api(changeset)
                print(f"[GraphIngestion] Ingest result: {result}", file=sys.stderr)
            except Exception as e:
                print(f"[GraphIngestion] Failed to ingest record: {e}", file=sys.stderr)

    def _transform(self, record: dict) -> dict:
        nodes = []
        relationships = []
        # RSS-style records
        if 'source_url' in record and 'retrieved_at' in record:
            src = record['source_url']
            ts = record['retrieved_at']
            # Source node
            nodes.append({
                'id': src,
                'labels': ['Source'],
                'properties': {'url': src}
            })
            # SightingEvent node
            evt_id = f"evt:{src}|{ts}"
            nodes.append({
                'id': evt_id,
                'labels': ['SightingEvent'],
                'properties': {'timestamp': ts}
            })
            relationships.append({'source_id': evt_id, 'target_id': src, 'type': 'REPORTED_BY', 'properties': {}})
            # Article node
            art_id = src
            props = {'url': src, 'full_text': record.get('full_text', ''), 'summary': record.get('summary', ''), 'source_name': record.get('source_name', '')}
            nodes.append({'id': art_id, 'labels': ['Article'], 'properties': props})
            relationships.append({'source_id': art_id, 'target_id': evt_id, 'type': 'ABOUT', 'properties': {}})
        # MISP records
        elif record.get('source') == 'misp' and 'ioc_record' in record:
            idx = record.get('feed_url', '')
            ts = record.get('fetched_at', '')
            node_id = f"misp:{idx}|{ts}"
            nodes.append({'id': node_id, 'labels': ['MispEvent'], 'properties': {'feed_url': idx, 'fetched_at': ts}})
            # IOC node or ThreatActor if APT group
            ioc = record['ioc_record']
            ioc_id = ioc.get('value', '')
            if re.match(r'(?i)^APT\d+', ioc_id):
                # APT group as ThreatActor
                ta_props = {'name': ioc_id}
                ta_props.update(ioc)
                nodes.append({'id': ioc_id, 'labels': ['ThreatActor'], 'properties': ta_props})
            else:
                nodes.append({'id': ioc_id, 'labels': ['Indicator'], 'properties': ioc})
            relationships.append({'source_id': ioc_id, 'target_id': node_id, 'type': 'APPEARED_IN', 'properties': {}})
        # TAXII STIX objects and relationships
        elif record.get('source') == 'taxii' and 'stix_object' in record:
            ts = record.get('fetched_at', '')
            obj = record['stix_object']
            obj_id = obj.get('id', '')
            stix_type = obj.get('type', '').lower()
            # STIX relationship object
            if stix_type == 'relationship':
                rel_type = obj.get('relationship_type', '').upper()
                src_ref = obj.get('source_ref')
                tgt_ref = obj.get('target_ref')
                rel_props = {k: v for k, v in obj.items() if k not in ['type', 'id', 'relationship_type', 'source_ref', 'target_ref']}
                relationships.append({'source_id': src_ref, 'target_id': tgt_ref, 'type': rel_type, 'properties': rel_props})
            else:
                # Map STIX types to internal labels
                label_map = {
                    'attack-pattern': 'AttackPattern',
                    'campaign': 'Campaign',
                    'intrusion-set': 'ThreatActor',
                    'threat-actor': 'ThreatActor',
                    'malware': 'Malware',
                    'vulnerability': 'Vulnerability',
                    'tool': 'Tool',
                    'identity': 'Identity',
                }
                label = label_map.get(stix_type, stix_type.title().replace('-', ''))
                props = dict(obj)
                props['fetched_at'] = ts
                nodes.append({'id': obj_id, 'labels': [label], 'properties': props})
        else:
            # Fallback generic node
            gen_id = record.get('id', '') or json.dumps(record)
            nodes.append({'id': gen_id, 'labels': ['RawIntel'], 'properties': record})

        # Enrich any TTP indicators via CTI backend tool and mitreattack data
        ttp_pattern = re.compile(r"^T\d+(?:\.\d+)*$")
        for node in nodes[:]:
            if 'Indicator' in node['labels'] and ttp_pattern.match(node['id']):
                details = self._enrich_attack_pattern(node['id'])
                if details:
                    nodes.append({
                        'id': details.id or node['id'],
                        'labels': ['AttackPattern'],
                        'properties': details.dict(exclude_none=True)
                    })
                    relationships.append({
                        'source_id': node['id'],
                        'target_id': details.id or node['id'],
                        'type': 'DESCRIBES',
                        'properties': {}
                    })
        # Enrich other CTI entity types
        self._post_transform(nodes, relationships)
        return {'nodes': nodes, 'relationships': relationships}

    def _call_ingest_api(self, changeset: dict) -> dict:
        # Relative URL is appended to CTI_BACKEND_URL by AgentHttpClient
        url = "/v1/graph/ingest"

        # AgentHttpClient already adds Authorization header and retry logic
        resp = self.http_client.post(url, json=changeset, timeout=30)
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            # Capture server error and return structured response
            try:
                error_body = resp.text
            except Exception:
                error_body = str(resp.status_code)
            return {"status": resp.status_code, "error": error_body}
        # Successful response
        try:
            return resp.json()
        except ValueError:
            return {"status": resp.status_code}

    def _call_tool_api(self, tool_name: str, params: dict) -> dict:
        """Call a CTI backend LLM tool by name and return JSON response"""
        url = f"/v1/tools/{tool_name}"
        resp = self.http_client.post(url, json=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _enrich_attack_pattern(self, identifier: str) -> Optional[AttackPatternDetails]:
        """Fetch and validate AttackPattern details via CTI backend tool"""
        try:
            data = self._call_tool_api("get_attack_pattern_details", {"attack_pattern_identifier": identifier})
            # Do not hallucinate: skip enrichment if tool reports not found
            if data.get("message"):
                return None
            return AttackPatternDetails(**data)
        except (requests.HTTPError, ValidationError) as e:
            print(f"[GraphIngestion] AttackPattern enrichment failed for {identifier}: {e}", file=sys.stderr)
            return None

    # Enrichment loops for other CTI entities
    def _enrich_generic(self, identifier: str, tool: str, model: BaseModel, param_key: str) -> Optional[Dict]:
        try:
            data = self._call_tool_api(tool, {param_key: identifier})
            if data.get('message'):
                return None
            return model(**data).dict(exclude_none=True)
        except (requests.HTTPError, ValidationError) as e:
            print(f"[GraphIngestion] {tool} enrichment failed for {identifier}: {e}", file=sys.stderr)
            return None

    def _post_transform(self, nodes: List[dict], relationships: List[dict]) -> None:
        # Enrich threat actors: merge details into existing nodes
        for node in nodes:
            if 'ThreatActor' in node.get('labels', []):
                details = self._enrich_generic(node['id'], 'get_threat_actor_summary', ThreatActorDetails, 'actor_name')
                if details:
                    node['properties'].update(details)
        # Enrich malware entities
        for node in nodes:
            if 'Malware' in node.get('labels', []):
                details = self._enrich_generic(node['id'], 'get_malware_details', MalwareDetails, 'malware_identifier')
                if details:
                    node['properties'].update(details)
        # Enrich campaigns
        for node in nodes:
            if 'Campaign' in node.get('labels', []):
                details = self._enrich_generic(node['id'], 'get_campaign_details', CampaignDetails, 'campaign_identifier')
                if details:
                    node['properties'].update(details)
        # Enrich vulnerabilities
        for node in nodes:
            if 'Vulnerability' in node.get('labels', []):
                details = self._enrich_generic(node['id'], 'get_vulnerability_details', VulnerabilityDetails, 'vulnerability_identifier')
                if details:
                    node['properties'].update(details)

        # LLM-driven finishing: only run on nodes with enough extracted text
        if self.enrich_with_llm:
            for node in nodes:
                try:
                    if self._should_enrich_node(node):
                        # Choose best text field available
                        props = node.get('properties', {})
                        text_input = props.get('full_text') or props.get('description') or ''
                        prompt = {
                            'Article': "Provide a concise one-sentence summary of this article.",
                            'ThreatActor': "Summarize the primary motivation of this threat actor in one sentence.",
                        }.get(node['labels'][0], None)
                        if prompt and text_input:
                            summary = self._llm_process_text(text_input, prompt)
                            props['llm_summary'] = summary
                except Exception as e:
                    print(f"[GraphIngestion] LLM enrichment failed for {node.get('id')}: {e}", file=sys.stderr)

    # New helper: decide if node should be enriched by LLM
    def _should_enrich_node(self, node: dict) -> bool:
        labels = node.get('labels', [])
        props = node.get('properties', {})
        if 'Article' in labels:
            # require a minimum length of full_text
            return len(props.get('full_text', '')) > 300
        if 'ThreatActor' in labels:
            # require detailed description
            return len(props.get('description', '')) > 150
        return False

    # New helper: run a constrained LLM call
    def _llm_process_text(self, text_input: str, prompt: str) -> str:
        if not self.llm or not self.enrich_with_llm:
            return f"LLM summary not available for: {text_input[:50]}..."
            
        try:
            # Format messages for the model
            message_content = f"{prompt}\n\n{text_input}"
            
            # Process with the ADK Gemini model - handle async generator
            async def run_llm():
                import inspect
                response_generator = self.llm.generate_content_async(message_content)
                
                if inspect.isasyncgen(response_generator):
                    # Handle async generator - collect all chunks
                    full_response = ""
                    async for chunk in response_generator:
                        if hasattr(chunk, 'text'):
                            full_response += chunk.text
                        elif hasattr(chunk, 'content'):
                            full_response += chunk.content
                        else:
                            full_response += str(chunk)
                    return full_response
                elif inspect.iscoroutine(response_generator):
                    # Handle regular coroutine
                    response = await response_generator
                    if hasattr(response, 'text'):
                        return response.text
                    elif hasattr(response, 'content'):
                        return response.content
                    else:
                        return str(response)
                else:
                    # Synchronous response
                    response = response_generator
                    if hasattr(response, 'text'):
                        return response.text
                    elif hasattr(response, 'content'):
                        return response.content
                    else:
                        return str(response)
            
            response_text = asyncio.run(run_llm())
            return response_text.strip()
        except Exception as e:
            print(f"[GraphIngestion] LLM processing error: {e}", file=sys.stderr)
            return f"Error processing text: {str(e)[:100]}..."


if __name__ == '__main__':
    agent = GraphIngestionAgent()
    agent.run()