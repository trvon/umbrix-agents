# coding: utf-8

"""
    CTI Backend API

    No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)

    The version of the OpenAPI document: 1.0.0
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


import unittest

from openapi_client.models.v1_graph_paths_get200_response import V1GraphPathsGet200Response

class TestV1GraphPathsGet200Response(unittest.TestCase):
    """V1GraphPathsGet200Response unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> V1GraphPathsGet200Response:
        """Test V1GraphPathsGet200Response
            include_optional is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `V1GraphPathsGet200Response`
        """
        model = V1GraphPathsGet200Response()
        if include_optional:
            return V1GraphPathsGet200Response(
                nodes = [
                    None
                    ],
                relationships = [
                    ''
                    ]
            )
        else:
            return V1GraphPathsGet200Response(
        )
        """

    def testV1GraphPathsGet200Response(self):
        """Test V1GraphPathsGet200Response"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
