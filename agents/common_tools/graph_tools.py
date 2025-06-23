from google.adk.tools import BaseTool
from neo4j import GraphDatabase
import os


class Neo4jWriterTool(BaseTool):
    """
    Tool to execute write queries against a Neo4j graph.
    Input: cypher query string and parameters dict.
    Output: True if successful.
    """
    def __init__(self):
        super().__init__(
            name="neo4j_writer",
            description="Executes Cypher write transactions against Neo4j"
        )
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "neo4jpassword")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def call(self, cypher: str, parameters: dict):
        with self.driver.session() as session:
            session.run(cypher, **parameters)
        return True 