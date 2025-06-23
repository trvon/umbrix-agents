"""Coordination and management agents for orchestrating the CTI pipeline."""

try:
    from .master_coordinator import CTIMasterCoordinatorAgent
except ImportError as e:
    print(f"Warning: Could not import CTIMasterCoordinatorAgent: {e}")
    CTIMasterCoordinatorAgent = None

try:
    from .indicator_context_agent import IndicatorContextRetrievalAgent
except ImportError as e:
    print(f"Warning: Could not import IndicatorContextRetrievalAgent: {e}")
    IndicatorContextRetrievalAgent = None

try:
    from .crawl_organizer_agent import CrawlOrganizerAgent, create_crawl_organizer
except ImportError as e:
    print(f"Warning: Could not import CrawlOrganizerAgent: {e}")
    CrawlOrganizerAgent = None
    create_crawl_organizer = None

try:
    from .cache_manager import CacheManager
except ImportError as e:
    print(f"Warning: Could not import CacheManager: {e}")
    CacheManager = None

try:
    from .bloom_filter_manager import BloomFilterManager
except ImportError as e:
    print(f"Warning: Could not import BloomFilterManager: {e}")
    BloomFilterManager = None

try:
    from .crawl_queue import CrawlQueue, CrawlTask
except ImportError as e:
    print(f"Warning: Could not import CrawlQueue/CrawlTask: {e}")
    CrawlQueue = None
    CrawlTask = None

try:
    from .graph_analyzer import GraphAnalyzer
except ImportError as e:
    print(f"Warning: Could not import GraphAnalyzer: {e}")
    GraphAnalyzer = None

__all__ = [
    "CTIMasterCoordinatorAgent",
    "IndicatorContextRetrievalAgent", 
    "CrawlOrganizerAgent",
    "create_crawl_organizer",
    "CacheManager",
    "BloomFilterManager",
    "CrawlQueue",
    "CrawlTask",
    "GraphAnalyzer",
]
