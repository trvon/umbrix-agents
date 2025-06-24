"""
IntelligentCrawlerAgent - Main agent for LLM-guided CTI source discovery.

This agent consumes threat intelligence from Kafka topics, generates intelligent
search queries using LLM, discovers relevant sources, and extracts content.
"""

import asyncio
import json
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import yaml

# Import Google ADK with fallback
try:
    from google.adk.agents import Agent
    from google.adk.models import Gemini
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.adk.tools import google_search
    from google.genai import types
    GOOGLE_ADK_AVAILABLE = True
except ImportError:
    print("Warning: google.adk.models module is not available. LLM functionality will be limited.", file=sys.stderr)
    GOOGLE_ADK_AVAILABLE = False
    # Create a mock base Agent class for testing
    class Agent:
        def __init__(self, name: str, description: str = ""):
            self.name = name
            self.description = description
    
    # Create a mock Gemini model for testing
    class MockGemini:
        def __init__(self, model=None):
            self.model = model
            
        async def generate_content_async(self, contents, **kwargs):
            # Mock response for testing
            mock_response = type('MockResponse', (), {
                'text': f"Mock search queries for: {contents[:100]}...",
                'choices': [type('Choice', (), {'message': type('Msg', (), {'content': "Mock response"})})],
            })
            return mock_response



class IntelligentCrawlerAgent(Agent):
    """
    Intelligent crawler agent that discovers CTI sources using LLM-guided search.
    
    This agent:
    - Consumes normalized intelligence and user submissions from Kafka
    - Uses Gemini LLM to generate contextual search queries
    - Searches across multiple providers for relevant sources
    - Extracts and normalizes content from discovered sources
    - Publishes extracted content to output Kafka topic
    """
    
    # Configuration for Pydantic-based Agent  
    class Config:
        extra = "allow"
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        name: str = "intelligent_crawler_agent",
        description: str = "Intelligent crawler for CTI source discovery",
        topics: List[str] = None,
        output_topic: str = "raw.intel.crawled",
        gemini_api_key: str = None,
        gemini_model: str = None,
        prometheus_port: int = 9465,
        **kwargs
    ):
        super().__init__(name=name, description=description)
        
        # Kafka configuration following existing pattern
        self.topics = topics or ["enriched.intel", "user.submissions", "feeds.discovered"]
        self.output_topic = output_topic
        self.bootstrap_servers = bootstrap_servers
        
        # LLM configuration
        self.gemini_api_key = gemini_api_key or os.getenv("GEMINI_API_KEY", "")
        self.gemini_model = gemini_model or os.getenv("GEMINI_MODEL_NAME", "gemini-2.0-flash-exp")
        
        # Crawler configuration
        self.prometheus_port = prometheus_port
        self.max_sources_per_query = 10
        self.max_queries_per_context = 5
        self.rate_limit_delay = 2.0
        
        self.logger = logging.getLogger(__name__)
        
        # Initialize Prometheus metrics
        self._init_metrics()
        
        # Initialize components
        self.search_orchestrator = None
        self.content_fetcher = None
        self._running = False
        
        # Initialize Gemini model and ADK components
        self.llm = None
        self.search_agent = None
        self.session_service = None
        self.runner = None
        
        if GOOGLE_ADK_AVAILABLE:
            try:
                # Initialize basic LLM for query generation
                self.llm = Gemini(model=self.gemini_model)
                
                # Initialize ADK search agent for intelligent searching
                self.search_agent = Agent(
                    name="cti_search_agent",
                    model=self.llm,  # Use the model object, not string
                    description="Agent specialized in finding cybersecurity threat intelligence sources.",
                    instruction="I am a specialist in finding cybersecurity threat intelligence sources. I search for relevant threat reports, IOCs, malware analysis, and attribution information based on given threat context.",
                    tools=[google_search]
                )
                
                # Initialize session service and runner
                self.session_service = InMemorySessionService()
                self.runner = Runner(agent=self.search_agent, app_name=f"umbrix-cti-crawler-{self.name}", session_service=self.session_service)
                
                self.logger.info(f"Initialized ADK Gemini model and search agent: {self.gemini_model}")
            except Exception as e:
                self.logger.error(f"Failed to initialize ADK components: {e}")
        else:
            self.llm = MockGemini(model=self.gemini_model)
            self.logger.warning("google.adk.models not available, using mock LLM")
        
        # Initialize Kafka consumer and producer
        self._init_kafka()
        
    def _init_metrics(self):
        """Initialize Prometheus metrics."""
        self.metrics = {
            'messages_processed': Counter(
                'crawler_messages_processed_total',
                'Total messages processed by crawler agent',
                ['topic', 'status']
            ),
            'queries_generated': Counter(
                'crawler_queries_generated_total', 
                'Total search queries generated'
            ),
            'sources_discovered': Counter(
                'crawler_sources_discovered_total',
                'Total sources discovered',
                ['provider']
            ),
            'content_extracted': Counter(
                'crawler_content_extracted_total',
                'Total content pieces extracted',
                ['extraction_method']
            ),
            'processing_time': Histogram(
                'crawler_processing_seconds',
                'Time spent processing intelligence context'
            ),
            'llm_calls': Counter(
                'crawler_llm_calls_total',
                'Total LLM API calls',
                ['status']
            ),
            'active_crawlers': Gauge(
                'crawler_active_instances',
                'Number of active crawler instances'
            )
        }
        
        # Start Prometheus metrics server
        try:
            start_http_server(self.prometheus_port)
            self.logger.info(f"Prometheus metrics server started on port {self.prometheus_port}")
        except Exception as e:
            self.logger.warning(f"Failed to start Prometheus server: {e}")
    
    def _init_kafka(self):
        """Initialize Kafka consumer and producer following existing agent patterns."""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=f'{self.name}_group',
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v)
            )
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
            )
            self.logger.info(f"Kafka initialized - consuming from {self.topics}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka: {e}")
            self.consumer = None
            self.producer = None
        
    async def initialize(self):
        """Initialize the crawler agent components."""
        try:
            # Import and initialize components
            from .search_orchestrator import SearchOrchestrator
            from .content_fetcher import ContentFetcher
            
            self.search_orchestrator = SearchOrchestrator()
            self.content_fetcher = ContentFetcher()
            
            self.logger.info("Crawler agent components initialized successfully")
            self.metrics['active_crawlers'].inc()
        except Exception as e:
            self.logger.error(f"Failed to initialize crawler components: {e}")
            raise

    async def generate_search_queries(self, intelligence_context: Dict[str, Any]) -> List[str]:
        """
        Generate intelligent search queries from threat intelligence context using Gemini.
        
        Args:
            intelligence_context: Context containing threat intelligence data
            
        Returns:
            List of search queries optimized for CTI source discovery
        """
        if not self.llm:
            self.metrics['llm_calls'].labels(status='failed').inc()
            return ["generic cybersecurity threat intelligence"]
            
        # Extract key elements from context for better query generation
        threat_actors = intelligence_context.get('threat_actors', [])
        malware_families = intelligence_context.get('malware_families', [])
        attack_techniques = intelligence_context.get('attack_techniques', [])
        indicators = intelligence_context.get('indicators', [])
        campaign_refs = intelligence_context.get('campaign_references', [])
        
        # Create a focused prompt based on available context
        context_summary = {
            'threat_actors': threat_actors[:3],  # Limit for prompt size
            'malware_families': malware_families[:3],
            'attack_techniques': attack_techniques[:3],
            'sample_indicators': indicators[:5],
            'campaigns': campaign_refs[:2]
        }
        
        prompt = f"""
        You are a cybersecurity threat intelligence analyst. Based on the threat intelligence context below, 
        generate {self.max_queries_per_context} specific search queries to discover related CTI sources.
        
        Focus on finding:
        - Additional threat reports and analyses
        - Security vendor publications
        - Research papers and blog posts
        - IOC sharing platforms
        - Attribution and campaign details
        
        Context summary: {json.dumps(context_summary, indent=2)}
        
        Generate queries that would find relevant cybersecurity content. Return only the search queries, 
        one per line, without numbering or explanation.
        """
        
        try:
            self.metrics['llm_calls'].labels(status='started').inc()
            
            # Simplified response handling for Google ADK Gemini
            try:
                response = await self.llm.generate_content_async(prompt)
                
                # Extract text from response - Google ADK typically returns simple response object
                if hasattr(response, 'text'):
                    response_text = response.text
                elif hasattr(response, 'content'):
                    response_text = response.content
                elif hasattr(response, 'candidates') and response.candidates:
                    # Handle candidates structure if present
                    candidate = response.candidates[0]
                    if hasattr(candidate, 'content'):
                        response_text = candidate.content.parts[0].text if candidate.content.parts else str(candidate.content)
                    else:
                        response_text = str(candidate)
                else:
                    response_text = str(response)
                    
            except Exception as llm_error:
                self.logger.error(f"Error generating queries with LLM: {llm_error}")
                response_text = "Error generating queries"
            
            queries = [q.strip() for q in response_text.split('\n') if q.strip()]
            
            # Filter and validate queries
            valid_queries = []
            for query in queries[:self.max_queries_per_context]:
                if len(query) > 10 and len(query) < 200:  # Reasonable length
                    valid_queries.append(query)
            
            self.metrics['llm_calls'].labels(status='success').inc()
            self.metrics['queries_generated'].inc(len(valid_queries))
            
            self.logger.info(f"Generated {len(valid_queries)} search queries from context")
            return valid_queries if valid_queries else ["cybersecurity threat intelligence report"]
            
        except Exception as e:
            self.logger.error(f"Failed to generate search queries: {e}")
            self.metrics['llm_calls'].labels(status='error').inc()
            return ["cybersecurity threat intelligence", "malware analysis report"]
    
    async def intelligent_search_with_adk(self, intelligence_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Use Google ADK search agent to directly find relevant content.
        
        Args:
            intelligence_context: Context containing threat intelligence data
            
        Returns:
            List of search results with metadata
        """
        if not self.search_agent or not self.runner:
            self.logger.warning("ADK search agent not available")
            return []
        
        try:
            # Create a session for this search
            session = self.session_service.create_session(
                app_name=f"umbrix-cti-crawler-{self.name}",
                user_id="cti_system",
                session_id=f"search_{int(time.time())}"
            )
            
            # Create search query from intelligence context
            search_query = self._create_search_query_from_context(intelligence_context)
            
            self.logger.info(f"Performing ADK search with query: {search_query}")
            
            # Create content for the agent
            content = types.Content(role='user', parts=[types.Part(text=search_query)])
            
            # Run the search agent
            events = self.runner.run(
                user_id="cti_system", 
                session_id=session.session_id, 
                new_message=content
            )
            
            # Collect results
            search_results = []
            for event in events:
                if event.is_final_response():
                    response_text = event.content.parts[0].text
                    # Parse the response to extract URLs and descriptions
                    urls = self._extract_urls_from_response(response_text)
                    for url in urls:
                        search_results.append({
                            'url': url,
                            'description': response_text[:200] + "...",
                            'source': 'adk_google_search',
                            'query': search_query,
                            'timestamp': time.time()
                        })
            
            self.logger.info(f"ADK search found {len(search_results)} results")
            self.metrics['llm_calls'].labels(status='success').inc()
            
            return search_results
            
        except Exception as e:
            self.logger.error(f"Error in ADK search: {e}")
            self.metrics['llm_calls'].labels(status='error').inc()
            return []
    
    def _create_search_query_from_context(self, intelligence_context: Dict[str, Any]) -> str:
        """Create a focused search query from threat intelligence context."""
        threat_actors = intelligence_context.get('threat_actors', [])
        malware_families = intelligence_context.get('malware_families', [])
        attack_techniques = intelligence_context.get('attack_techniques', [])
        indicators = intelligence_context.get('indicators', [])
        
        # Create a focused search query
        query_parts = []
        
        if threat_actors:
            query_parts.append(f"threat actor {threat_actors[0]}")
        if malware_families:
            query_parts.append(f"malware {malware_families[0]}")
        if attack_techniques:
            query_parts.append(f"attack technique {attack_techniques[0]}")
        
        # Add CTI-specific terms
        query_parts.extend(["threat intelligence", "cybersecurity report"])
        
        search_query = f"Find recent cybersecurity threat intelligence reports about: {' '.join(query_parts[:3])}"
        
        return search_query
    
    def _extract_urls_from_response(self, response_text: str) -> List[str]:
        """Extract URLs from ADK search response."""
        import re
        # Simple URL extraction - can be enhanced
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        urls = re.findall(url_pattern, response_text)
        return urls[:10]  # Limit to 10 URLs
    
    async def process_intelligence_context(self, context: Dict[str, Any], source_topic: str) -> None:
        """
        Process intelligence context and discover related sources.
        
        Args:
            context: Intelligence context containing threat data
            source_topic: Kafka topic the message came from
        """
        start_time = time.time()
        
        try:
            # Generate search queries from context
            queries = await self.generate_search_queries(context)
            self.logger.info(f"Generated {len(queries)} search queries from {source_topic}")
            
            if not queries:
                self.metrics['messages_processed'].labels(topic=source_topic, status='no_queries').inc()
                return
            
            # Execute searches across providers
            all_search_results = {}
            if self.search_orchestrator:
                for query in queries:
                    try:
                        results = await self.search_orchestrator.search_all_providers(
                            query, self.max_sources_per_query
                        )
                        all_search_results[query] = results
                        
                        # Update metrics per provider
                        for result in results:
                            self.metrics['sources_discovered'].labels(provider=result.provider).inc()
                        
                        # Rate limiting
                        await asyncio.sleep(self.rate_limit_delay)
                        
                    except Exception as e:
                        self.logger.error(f"Search failed for query '{query}': {e}")
                        all_search_results[query] = []
            
            # Collect unique URLs to fetch
            unique_urls = set()
            for results in all_search_results.values():
                for result in results[:self.max_sources_per_query]:
                    if result.url and result.url.startswith(('http://', 'https://')):
                        unique_urls.add(result.url)
            
            self.logger.info(f"Found {len(unique_urls)} unique URLs to fetch")
            
            # Fetch and extract content from discovered sources
            extracted_contents = []
            if self.content_fetcher and unique_urls:
                try:
                    contents = await self.content_fetcher.fetch_multiple_urls(
                        list(unique_urls), max_concurrent=3
                    )
                    
                    for content in contents:
                        if content.success:
                            extracted_contents.append(content)
                            self.metrics['content_extracted'].labels(
                                extraction_method=content.extraction_method
                            ).inc()
                    
                except Exception as e:
                    self.logger.error(f"Content fetching failed: {e}")
            
            # Prepare output message
            discovered_content = {
                "source_type": "intelligent_crawled",
                "original_context": context,
                "source_topic": source_topic,
                "queries_used": queries,
                "search_results_summary": {
                    query: len(results) for query, results in all_search_results.items()
                },
                "extracted_content": [
                    {
                        "url": content.url,
                        "title": content.title,
                        "content": content.content[:5000],  # Limit content size
                        "extraction_method": content.extraction_method,
                        "threat_indicators": content.threat_indicators,
                        "threat_actors": content.threat_actors,
                        "malware_families": content.malware_families,
                        "attack_techniques": content.attack_techniques,
                        "metadata": content.metadata,
                        "timestamp": content.timestamp.isoformat()
                    }
                    for content in extracted_contents
                ],
                "processing_stats": {
                    "queries_generated": len(queries),
                    "sources_found": len(unique_urls),
                    "content_extracted": len(extracted_contents),
                    "processing_time_seconds": time.time() - start_time
                },
                "timestamp": time.time()
            }
            
            # Publish to output topic
            if self.producer:
                try:
                    future = self.producer.send(self.output_topic, discovered_content)
                    future.get(timeout=10)  # Wait for confirmation
                    self.logger.info(
                        f"Published crawled content: {len(extracted_contents)} sources to {self.output_topic}"
                    )
                    self.metrics['messages_processed'].labels(topic=source_topic, status='success').inc()
                except Exception as e:
                    self.logger.error(f"Failed to publish to Kafka: {e}")
                    self.metrics['messages_processed'].labels(topic=source_topic, status='publish_failed').inc()
            
            # Record processing time
            self.metrics['processing_time'].observe(time.time() - start_time)
            
        except Exception as e:
            self.logger.error(f"Error processing intelligence context: {e}")
            self.metrics['messages_processed'].labels(topic=source_topic, status='error').inc()
    
    def run(self):
        """Start the crawler agent processing loop."""
        if not self.consumer:
            self.logger.error("No Kafka consumer available, cannot start processing")
            return
        
        self.logger.info(f"Starting IntelligentCrawlerAgent, listening on topics: {self.topics}")
        self._running = True
        
        # Initialize components if not done
        if not self.search_orchestrator or not self.content_fetcher:
            asyncio.run(self.initialize())
        
        for message in self.consumer:
            if not self._running:
                break
                
            try:
                context = message.value
                topic = message.topic
                
                self.logger.info(f"Received intelligence context from topic '{topic}': {len(str(context))} chars")
                
                # Process the intelligence context asynchronously
                asyncio.run(self.process_intelligence_context(context, topic))
                
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to decode JSON message: {e}")
                self.metrics['messages_processed'].labels(topic=message.topic, status='decode_error').inc()
            except Exception as e:
                self.logger.error(f"Error processing message: {e}")
                self.metrics['messages_processed'].labels(topic=message.topic, status='error').inc()
    
    def stop(self):
        """Stop the crawler agent gracefully."""
        self.logger.info("Stopping IntelligentCrawlerAgent")
        self._running = False
        self.metrics['active_crawlers'].dec()
        
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
    

if __name__ == "__main__":
    # Read configuration from environment, following existing agent patterns
    BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
    
    agent = IntelligentCrawlerAgent(
        bootstrap_servers=BOOTSTRAP,
        gemini_api_key=GEMINI_KEY,
    )
    
    try:
        agent.run()  # blocking loop
    except KeyboardInterrupt:
        agent.logger.info("Received interrupt signal")
        agent.stop()