"""
Test-friendly version of the FeedSeederAgent for testing purposes.
"""
import os
import sys
from agents.collector_agent.feed_seeder_agent import FeedSeederAgent as OriginalFeedSeederAgent

class MockFeedSeederAgent(OriginalFeedSeederAgent):
    """A test-friendly version of FeedSeederAgent that doesn't exit on errors."""
    
    def __init__(self, *args, **kwargs):
        # Override init to avoid sys.exit calls
        try:
            self.testing_mode = True
            super().__init__(*args, **kwargs)
        except SystemExit:
            # Catch sys.exit calls from parent
            pass
