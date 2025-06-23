"""
Feed Seeder Agent - for backward compatibility with tests.

This module provides a basic feed seeding agent that can be used for testing.
"""

import yaml
import time
from typing import List, Dict, Any, Optional


class FeedSeederAgent:
    """
    A simple feed seeder agent for testing purposes.
    
    This agent can seed initial feed configurations into the system.
    """
    
    def __init__(self, config_path: Optional[str] = None, bootstrap_servers: Optional[str] = None, **kwargs):
        """Initialize the Feed Seeder Agent."""
        self.config_path = config_path
        self.bootstrap_servers = bootstrap_servers
        self.kwargs = kwargs
        self.feeds = []
        
    def load_feeds(self, feeds: List[Dict[str, Any]]) -> None:
        """Load feeds into the agent."""
        self.feeds = feeds
        
    def seed_feeds(self) -> None:
        """Seed feeds into the system."""
        if self.config_path:
            try:
                # Load existing config
                config = {}
                try:
                    with open(self.config_path, 'r') as f:
                        config = yaml.safe_load(f) or {}
                except FileNotFoundError:
                    pass
                
                # Add seeded feeds
                existing_feeds = config.get('feeds', [])
                for feed in self.feeds:
                    if feed not in existing_feeds:
                        existing_feeds.append(feed)
                
                config['feeds'] = existing_feeds
                
                # Write back config
                with open(self.config_path, 'w') as f:
                    yaml.safe_dump(config, f)
                    
            except Exception as e:
                print(f"Error seeding feeds: {e}")
                
    def run(self) -> None:
        """Run the feed seeder agent."""
        self.seed_feeds()
        
    def stop(self) -> None:
        """Stop the feed seeder agent."""
        pass


# For backward compatibility
__all__ = ['FeedSeederAgent']