"""
Package initialization for the agents module.
This file ensures that the agents directory is recognized as a Python package
and provides organized imports for all agent types.
"""

# Import agents by category for clear organization
try:
    from . import discovery_agent
except ImportError as e:
    print(f"Warning: Could not import discovery_agent: {e}")
    discovery_agent = None

try:
    from . import collector_agent
except ImportError as e:
    print(f"Warning: Could not import collector_agent: {e}")
    collector_agent = None

try:  
    from . import processing_agent
except ImportError as e:
    print(f"Warning: Could not import processing_agent: {e}")
    processing_agent = None

try:
    from . import organizer_agent
except ImportError as e:
    print(f"Warning: Could not import organizer_agent: {e}")
    organizer_agent = None

try:
    from . import common_tools
except ImportError as e:
    print(f"Warning: Could not import common_tools: {e}")
    common_tools = None

# Convenience imports for commonly used agents
FeedDiscoveryAgent = None
IntelligentCrawlerAgent = None
FeedSeederAgent = None

if discovery_agent:
    try:
        from .discovery_agent import (
            FeedDiscoveryAgent,
            IntelligentCrawlerAgent, 
            FeedSeederAgent
        )
    except ImportError as e:
        print(f"Warning: Could not import from discovery_agent: {e}")

# Collector agent imports
RssCollectorAgent = None
MispFeedAgent = None
TaxiiPullAgent = None
ShodanStreamAgent = None

if collector_agent:
    try:
        from .collector_agent import (
            RssCollectorAgent,
            MispFeedAgent,
            TaxiiPullAgent, 
            ShodanStreamAgent
        )
    except ImportError as e:
        print(f"Warning: Could not import from collector_agent: {e}")

# Processing agent imports
FeedNormalizationAgent = None
GeoIpEnrichmentAgent = None
Neo4jGraphBuilderAgent = None
GraphIngestionAgent = None

if processing_agent:
    try:
        from .processing_agent import (
            FeedNormalizationAgent,
            GeoIpEnrichmentAgent,
            Neo4jGraphBuilderAgent,
            GraphIngestionAgent
        )
    except ImportError as e:
        print(f"Warning: Could not import from processing_agent: {e}")

# Organizer agent imports
CTIMasterCoordinatorAgent = None
IndicatorContextRetrievalAgent = None
CrawlOrganizerAgent = None

if organizer_agent:
    try:
        from .organizer_agent import (
            CTIMasterCoordinatorAgent,
            IndicatorContextRetrievalAgent,
            CrawlOrganizerAgent
        )
    except ImportError as e:
        print(f"Warning: Could not import from organizer_agent: {e}")

__all__ = [
    # Agent modules
    "discovery_agent",
    "collector_agent", 
    "processing_agent",
    "organizer_agent",
    "common_tools",
    
    # Discovery agents
    "FeedDiscoveryAgent",
    "IntelligentCrawlerAgent",
    "FeedSeederAgent",
    
    # Collection agents
    "RssCollectorAgent", 
    "MispFeedAgent",
    "TaxiiPullAgent",
    "ShodanStreamAgent",
    
    # Processing agents
    "FeedNormalizationAgent",
    "GeoIpEnrichmentAgent", 
    "Neo4jGraphBuilderAgent",
    "GraphIngestionAgent",
    
    # Organizer agents
    "CTIMasterCoordinatorAgent",
    "IndicatorContextRetrievalAgent", 
    "CrawlOrganizerAgent",
]
