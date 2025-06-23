"""
Collection agents for gathering data from known CTI sources.

This module contains agents that collect threat intelligence data from
known sources including RSS feeds, MISP feeds, TAXII servers, and Shodan.
"""

# Core collection agents
try:
    from .rss_collector import RssCollectorAgent
except ImportError as e:
    print(f"Warning: Could not import RssCollectorAgent: {e}")
    RssCollectorAgent = None

try:
    from .misp_feed_agent import MispFeedAgent
except ImportError as e:
    print(f"Warning: Could not import MispFeedAgent: {e}")
    MispFeedAgent = None

try:  
    from .taxii_pull_agent import TaxiiPullAgent
except ImportError as e:
    print(f"Warning: Could not import TaxiiPullAgent: {e}")
    TaxiiPullAgent = None

try:
    from .shodan_stream_agent import ShodanStreamAgent
except ImportError as e:
    print(f"Warning: Could not import ShodanStreamAgent: {e}")
    ShodanStreamAgent = None

# Backward compatibility imports for moved agents
import warnings

def _deprecated_import_warning(old_location: str, new_location: str):
    """Helper to show deprecation warnings for moved agents."""
    warnings.warn(
        f"Importing from {old_location} is deprecated. "
        f"Use {new_location} instead.",
        DeprecationWarning,
        stacklevel=3
    )

# Backward compatibility for FeedSeederAgent (moved to discovery_agent)
def FeedSeederAgent(*args, **kwargs):
    """Deprecated: FeedSeederAgent has moved to discovery_agent."""
    _deprecated_import_warning(
        "agents.collector_agent.FeedSeederAgent",
        "agents.discovery_agent.FeedSeederAgent"
    )
    # Backward compatibility imports assume flat layout
    from discovery_agent import FeedSeederAgent as _FeedSeederAgent
    return _FeedSeederAgent(*args, **kwargs)

# Backward compatibility for discovery agents (consolidated)
from .rss_discovery_agent import RssDiscoveryAgent, find_feed_links


__all__ = [
    # Core collection agents
    "RssCollectorAgent",
    "MispFeedAgent", 
    "TaxiiPullAgent",
    "ShodanStreamAgent",
    
    # Backward compatibility (deprecated)
    "FeedSeederAgent",  # Moved to discovery_agent
    "RssDiscoveryAgent",  # Consolidated into feed_discovery_agent
    "find_feed_links",  # For test compatibility
]