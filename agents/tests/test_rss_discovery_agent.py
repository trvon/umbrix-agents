import tempfile
import yaml
import os
import pytest
from agents.collector_agent import rss_discovery_agent
from agents.collector_agent.rss_discovery_agent import find_feed_links, RssDiscoveryAgent

class DummyResponse:
    def __init__(self, html, url):
        self.text = html
        self.url = url
    def raise_for_status(self):
        pass

@ pytest.mark.parametrize("html,base,expected", [
    ("<link type=\"application/rss+xml\" href=\"/feed.xml\" />", "http://example.com", {"http://example.com/feed.xml"}),
    ("<a href=\"rss.xml\">RSS</a>", "https://site.org/path/", {"https://site.org/path/rss.xml"}),
])
def test_find_feed_links(html, base, expected):
    feeds = find_feed_links(html, base)
    # Test for inclusion rather than exact equality, as regex may find additional results
    assert expected.issubset(feeds), f"Expected feeds {expected} not found in {feeds}"

def test_merge_config(tmp_path, monkeypatch):
    # Setup initial config file
    cfg_file = tmp_path / "config.yaml"
    initial = {"feeds": ["http://existing/feed"]}
    cfg_file.write_text(yaml.dump(initial))
    # Monkeypatch HTTP response
    html = '<link type="application/rss+xml" href="new.xml" />'
    monkeypatch.setenv("RSS_DISCOVERY_SEEDS", "https://dummy.test")
    def fake_get(url, timeout):
        assert url == "https://dummy.test"
        return DummyResponse(html, url)
    monkeypatch.setattr(rss_discovery_agent.requests, "get", fake_get)
    # Run agent
    agent = RssDiscoveryAgent(config_path=str(cfg_file))
    agent.run()
    # Verify config updated
    updated = yaml.safe_load(cfg_file.read_text())
    assert "http://existing/feed" in updated["feeds"]
    assert "https://dummy.test/new.xml" in updated["feeds"]