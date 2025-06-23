from google.adk.agents import Agent
from google.adk.tools import FunctionTool as Tool
import os
import sys
from auth_client import AgentHttpClient


class IndicatorContextRetrievalAgent(Agent):
    """ADK Agent to retrieve context for a CTI indicator via cti_backend"""
    class Config:
        extra = "allow"

    def __init__(self,
                 backend_url: str = None,
                 bootstrap_servers: str = None,
                 name: str = "indicator_context_retrieval",
                 description: str = "Retrieves context (neighbors) for an indicator via cti_backend API",
                 **kwargs):
        super().__init__(
            name=name,
            description=description
        )
        self.backend_url = backend_url or os.getenv('CTI_BACKEND_URL')
        if not self.backend_url:
            print("[IndicatorContext] CTI_BACKEND_URL not set", file=sys.stderr)
            sys.exit(1)

        # Shared HTTP client for authenticated requests
        self.http_client = AgentHttpClient()

        # Tool to fetch neighbors
        self.get_neighbors_tool = Tool(func=self._get_neighbors)

    def run(self):
        print(f"[IndicatorContext] Agent initialized. Use get_neighbors_tool.call(indicator, type, depth)", file=sys.stderr)

    def _get_neighbors(self, indicator: str, node_type: str = None, depth: int = 1):
        # Build relative path so AgentHttpClient applies base URL
        url = f"/v1/graph/neighbors/{indicator}"
        params = {'depth': depth}
        if node_type:
            params['type'] = node_type

        resp = self.http_client.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()


if __name__ == '__main__':
    agent = IndicatorContextRetrievalAgent()
    agent.run() 