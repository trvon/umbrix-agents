# coding: utf-8

"""
    CTI Backend API

    No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)

    The version of the OpenAPI document: 1.0.0
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


import unittest

from openapi_client.models.graph_node import GraphNode

class TestGraphNode(unittest.TestCase):
    """GraphNode unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> GraphNode:
        """Test GraphNode
            include_optional is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `GraphNode`
        """
        model = GraphNode()
        if include_optional:
            return GraphNode(
                id = '',
                labels = [
                    ''
                    ],
                properties = None
            )
        else:
            return GraphNode(
        )
        """

    def testGraphNode(self):
        """Test GraphNode"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
