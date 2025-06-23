"""
GraphAnalyzer: Graph-based analysis for intelligent crawl prioritization.

Analyzes the Neo4j graph to identify content gaps, calculate priorities,
and provide intelligent crawl recommendations.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple
from urllib.parse import urlparse

from neo4j import AsyncGraphDatabase, AsyncDriver
from pydantic import BaseModel


class ContentGap(BaseModel):
    """Represents a gap in the knowledge graph."""
    
    topic: str
    related_urls: List[str]
    priority_score: float
    missing_connections: int
    suggested_search_terms: List[str]


class GraphStats(BaseModel):
    """Graph statistics for analysis."""
    
    total_sources: int = 0
    total_articles: int = 0
    total_indicators: int = 0
    coverage_score: float = 0.0
    last_updated: datetime
    
    
class GraphAnalyzer:
    """
    Analyzes the Neo4j graph to provide intelligent crawl prioritization.
    
    Features:
    - Content gap analysis
    - Priority calculation based on graph structure
    - Duplicate content detection
    - Related content discovery
    - Coverage analysis
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the GraphAnalyzer."""
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Neo4j connection
        self.driver: Optional[AsyncDriver] = None
        
        # Configuration
        self.neo4j_uri = config.get('neo4j_uri', 'bolt://localhost:7687')
        self.neo4j_user = config.get('neo4j_user', 'neo4j')
        self.neo4j_password = config.get('neo4j_password', 'password')
        
        # Analysis parameters
        self.gap_analysis_depth = config.get('gap_analysis_depth', 3)
        self.min_connection_threshold = config.get('min_connection_threshold', 2)
        
    async def initialize(self) -> None:
        """Initialize Neo4j connection."""
        try:
            self.driver = AsyncGraphDatabase.driver(
                self.neo4j_uri,
                auth=(self.neo4j_user, self.neo4j_password)
            )
            
            # Test connection
            async with self.driver.session() as session:
                result = await session.run("RETURN 1 as test")
                await result.single()
            
            self.logger.info("Connected to Neo4j successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    
    async def content_exists(self, url: str) -> bool:
        """
        Check if content for a URL already exists in the graph.
        
        Args:
            url: URL to check
            
        Returns:
            True if content exists, False otherwise
        """
        if not self.driver:
            return False
        
        try:
            async with self.driver.session() as session:
                # Check for existing source or article with this URL
                query = """
                MATCH (n)
                WHERE n.url = $url OR n.source_url = $url
                RETURN count(n) > 0 as exists
                """
                
                result = await session.run(query, url=url)
                record = await result.single()
                
                if record:
                    return record['exists']
                
                return False
                
        except Exception as e:
            self.logger.error(f"Error checking content existence for {url}: {e}")
            # On error, assume content doesn't exist to avoid missing potential content
            return False
    
    async def calculate_content_priority(self, url: str, context: Dict[str, Any] = None) -> float:
        """
        Calculate priority for content based on graph analysis.
        
        Args:
            url: URL to calculate priority for
            context: Additional context for priority calculation
            
        Returns:
            Priority score between 0.0 and 1.0
        """
        if not self.driver:
            return 0.5  # Default priority
        
        try:
            priority = 0.0
            domain = urlparse(url).netloc
            
            async with self.driver.session() as session:
                # Factor 1: Domain authority in graph
                domain_query = """
                MATCH (s:Source)
                WHERE s.url CONTAINS $domain
                RETURN count(s) as domain_sources,
                       avg(s.reputation_score) as avg_reputation
                """
                
                result = await session.run(domain_query, domain=domain)
                record = await result.single()
                
                if record and record['domain_sources'] > 0:
                    # Domain is known in the graph
                    priority += 0.2
                    
                    # High reputation domains get higher priority
                    avg_reputation = record['avg_reputation'] or 0
                    if avg_reputation > 0.7:
                        priority += 0.15
                
                # Factor 2: Related content analysis
                if context and context.get('threat_indicators'):
                    indicators = context['threat_indicators']
                    for indicator in indicators[:5]:  # Limit to first 5 indicators
                        indicator_query = """
                        MATCH (i:Indicator {value: $indicator})
                        RETURN count(i) as indicator_count
                        """
                        
                        result = await session.run(indicator_query, indicator=indicator)
                        record = await result.single()
                        
                        if record and record['indicator_count'] == 0:
                            # New indicator not in graph - high priority
                            priority += 0.1
                
                # Factor 3: Content gap analysis
                gap_score = await self._calculate_gap_score(session, url, context)
                priority += gap_score * 0.3
                
                # Factor 4: Temporal analysis - prioritize recent threat intelligence
                if context and context.get('published_date'):
                    try:
                        published_date = datetime.fromisoformat(context['published_date'].replace('Z', '+00:00'))
                        age_days = (datetime.now(timezone.utc) - published_date).days
                        
                        if age_days <= 1:
                            priority += 0.2  # Very recent
                        elif age_days <= 7:
                            priority += 0.1  # Recent
                    except Exception:
                        pass
                
                # Ensure priority stays within bounds
                priority = max(0.0, min(1.0, priority))
                
            return priority
            
        except Exception as e:
            self.logger.error(f"Error calculating content priority for {url}: {e}")
            return 0.5  # Default on error
    
    async def identify_content_gaps(self, limit: int = 10) -> List[ContentGap]:
        """
        Identify gaps in the knowledge graph that should be prioritized for crawling.
        
        Args:
            limit: Maximum number of gaps to return
            
        Returns:
            List of ContentGap objects
        """
        gaps = []
        
        if not self.driver:
            return gaps
        
        try:
            async with self.driver.session() as session:
                # Find topics with few connections
                gap_query = """
                MATCH (a:Article)-[:MENTIONS]->(i:Indicator)
                WITH i, count(a) as mention_count
                WHERE mention_count < $threshold
                MATCH (i)-[:INDICATES]->(t:ThreatActor)
                WITH t.name as topic, collect(i.value) as indicators, 
                     avg(mention_count) as avg_mentions
                ORDER BY avg_mentions ASC
                LIMIT $limit
                RETURN topic, indicators, avg_mentions
                """
                
                result = await session.run(
                    gap_query,
                    threshold=self.min_connection_threshold,
                    limit=limit
                )
                
                async for record in result:
                    topic = record['topic']
                    indicators = record['indicators']
                    avg_mentions = record['avg_mentions']
                    
                    # Calculate priority score based on gap size
                    priority_score = 1.0 - (avg_mentions / self.min_connection_threshold)
                    
                    # Generate search terms
                    search_terms = await self._generate_search_terms(session, topic, indicators)
                    
                    gap = ContentGap(
                        topic=topic,
                        related_urls=[],  # Would be populated with related URLs
                        priority_score=priority_score,
                        missing_connections=self.min_connection_threshold - int(avg_mentions),
                        suggested_search_terms=search_terms
                    )
                    
                    gaps.append(gap)
                
        except Exception as e:
            self.logger.error(f"Error identifying content gaps: {e}")
        
        return gaps
    
    async def get_related_content(self, url: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get content related to a specific URL based on graph relationships.
        
        Args:
            url: URL to find related content for
            limit: Maximum number of related items to return
            
        Returns:
            List of related content items
        """
        related_content = []
        
        if not self.driver:
            return related_content
        
        try:
            async with self.driver.session() as session:
                # Find related content through shared indicators
                related_query = """
                MATCH (a1:Article {url: $url})-[:MENTIONS]->(i:Indicator)<-[:MENTIONS]-(a2:Article)
                WHERE a1 <> a2
                WITH a2, count(i) as shared_indicators
                ORDER BY shared_indicators DESC
                LIMIT $limit
                RETURN a2.url as url, a2.title as title, shared_indicators
                """
                
                result = await session.run(related_query, url=url, limit=limit)
                
                async for record in result:
                    related_content.append({
                        'url': record['url'],
                        'title': record['title'],
                        'shared_indicators': record['shared_indicators'],
                        'relationship_type': 'shared_indicators'
                    })
                
        except Exception as e:
            self.logger.error(f"Error getting related content for {url}: {e}")
        
        return related_content
    
    async def get_graph_stats(self) -> GraphStats:
        """Get comprehensive graph statistics."""
        if not self.driver:
            return GraphStats(last_updated=datetime.now(timezone.utc))
        
        try:
            async with self.driver.session() as session:
                # Get node counts
                stats_query = """
                MATCH (s:Source) WITH count(s) as sources
                MATCH (a:Article) WITH sources, count(a) as articles
                MATCH (i:Indicator) WITH sources, articles, count(i) as indicators
                RETURN sources, articles, indicators
                """
                
                result = await session.run(stats_query)
                record = await result.single()
                
                if record:
                    total_sources = record['sources']
                    total_articles = record['articles']
                    total_indicators = record['indicators']
                    
                    # Calculate coverage score (simplified)
                    total_content = total_sources + total_articles + total_indicators
                    coverage_score = min(1.0, total_content / 10000)  # Normalize to 10k items
                    
                    return GraphStats(
                        total_sources=total_sources,
                        total_articles=total_articles,
                        total_indicators=total_indicators,
                        coverage_score=coverage_score,
                        last_updated=datetime.now(timezone.utc)
                    )
                
        except Exception as e:
            self.logger.error(f"Error getting graph stats: {e}")
        
        return GraphStats(last_updated=datetime.now(timezone.utc))
    
    async def _calculate_gap_score(self, session, url: str, context: Dict[str, Any] = None) -> float:
        """Calculate content gap score for a URL."""
        gap_score = 0.0
        
        try:
            if context and context.get('threat_actors'):
                for actor in context['threat_actors'][:3]:  # Limit to first 3
                    # Check if this threat actor has enough coverage
                    actor_query = """
                    MATCH (t:ThreatActor {name: $actor})<-[:INDICATES]-(i:Indicator)<-[:MENTIONS]-(a:Article)
                    RETURN count(a) as article_count
                    """
                    
                    result = await session.run(actor_query, actor=actor)
                    record = await result.single()
                    
                    if record:
                        article_count = record['article_count']
                        if article_count < 5:  # Threshold for adequate coverage
                            gap_score += 0.2
            
        except Exception as e:
            self.logger.warning(f"Error calculating gap score: {e}")
        
        return min(1.0, gap_score)
    
    async def _generate_search_terms(self, session, topic: str, indicators: List[str]) -> List[str]:
        """Generate search terms for a topic and its indicators."""
        search_terms = [topic]
        
        try:
            # Add related terms from the graph
            related_query = """
            MATCH (t:ThreatActor {name: $topic})-[:USES]->(m:Malware)
            RETURN m.name as malware_name
            LIMIT 5
            """
            
            result = await session.run(related_query, topic=topic)
            
            async for record in result:
                malware_name = record['malware_name']
                if malware_name:
                    search_terms.append(malware_name)
            
            # Add some indicators as search terms
            search_terms.extend(indicators[:3])
            
        except Exception as e:
            self.logger.warning(f"Error generating search terms: {e}")
        
        return list(set(search_terms))  # Remove duplicates
    
    async def cleanup(self) -> None:
        """Clean up resources."""
        try:
            if self.driver:
                await self.driver.close()
                self.logger.info("Neo4j connection closed")
        except Exception as e:
            self.logger.error(f"Error closing Neo4j connection: {e}")