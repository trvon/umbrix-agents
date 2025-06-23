from google.adk.tools import BaseTool
import requests
import feedparser
from .dead_link_integration import dead_link_aware_retry, get_dead_link_detector

class RssFeedFetcherTool(BaseTool):
    def __init__(self):
        super().__init__(
            name="rss_feed_fetcher",
            description="Fetch and parse RSS feed entries with dead link detection"
        )
        self.dead_link_detector = get_dead_link_detector()
    
    """
    Tool to fetch and parse RSS feeds with dead link detection.
    Input: URL of the RSS feed.
    Output: List of parsed feed entries.
    """
    @dead_link_aware_retry('rss_feeds', 'collector', 'rss_fetch')
    def call(self, feed_url: str):
        response = requests.get(feed_url, timeout=10)
        response.raise_for_status()
        parsed = feedparser.parse(response.content)
        if parsed.bozo:
            raise Exception(f"Failed to parse RSS feed: {parsed.bozo_exception}")
        return parsed.entries