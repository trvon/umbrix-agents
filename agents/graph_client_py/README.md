# openapi-client
No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)

This Python package is automatically generated by the [OpenAPI Generator](https://openapi-generator.tech) project:

- API version: 1.0.0
- Package version: 1.0.0
- Generator version: 7.13.0
- Build package: org.openapitools.codegen.languages.PythonClientCodegen

## Requirements.

Python 3.9+

## Installation & Usage
### pip install

If the python package is hosted on a repository, you can install directly using:

```sh
pip install git+https://github.com/GIT_USER_ID/GIT_REPO_ID.git
```
(you may need to run `pip` with root permission: `sudo pip install git+https://github.com/GIT_USER_ID/GIT_REPO_ID.git`)

Then import the package:
```python
import openapi_client
```

### Setuptools

Install via [Setuptools](http://pypi.python.org/pypi/setuptools).

```sh
python setup.py install --user
```
(or `sudo python setup.py install` to install the package for all users)

Then import the package:
```python
import openapi_client
```

### Tests

Execute `pytest` to run the tests.

## Getting Started

Please follow the [installation procedure](#installation--usage) and then run the following:

```python

import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost:8080/api
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://localhost:8080/api"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: ApiKeyAuth
configuration.api_key['ApiKeyAuth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['ApiKeyAuth'] = 'Bearer'


# Enter a context with an instance of the API client
with openapi_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.DefaultApi(api_client)

    try:
        # List RSS feeds
        api_response = api_instance.v1_feeds_get()
        print("The response of DefaultApi->v1_feeds_get:\n")
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling DefaultApi->v1_feeds_get: %s\n" % e)

```

## Documentation for API Endpoints

All URIs are relative to *http://localhost:8080/api*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*DefaultApi* | [**v1_feeds_get**](docs/DefaultApi.md#v1_feeds_get) | **GET** /v1/feeds | List RSS feeds
*DefaultApi* | [**v1_graph_neighbors_node_value_get**](docs/DefaultApi.md#v1_graph_neighbors_node_value_get) | **GET** /v1/graph/neighbors/{node_value} | Get neighbors of a node
*DefaultApi* | [**v1_graph_paths_get**](docs/DefaultApi.md#v1_graph_paths_get) | **GET** /v1/graph/paths | Find shortest path between two nodes
*DefaultApi* | [**v1_graph_query_post**](docs/DefaultApi.md#v1_graph_query_post) | **POST** /v1/graph/query | Execute a parameterized Cypher query
*DefaultApi* | [**v1_graph_statistics_get**](docs/DefaultApi.md#v1_graph_statistics_get) | **GET** /v1/graph/statistics | Get graph statistics
*DefaultApi* | [**v1_system_settings_waitlist_get**](docs/DefaultApi.md#v1_system_settings_waitlist_get) | **GET** /v1/system/settings/waitlist | Get waitlist mode status


## Documentation For Models

 - [AgentTask](docs/AgentTask.md)
 - [CreateFeedRequest](docs/CreateFeedRequest.md)
 - [CreateFeedResponse](docs/CreateFeedResponse.md)
 - [CreatePaymentIntentRequest](docs/CreatePaymentIntentRequest.md)
 - [CreatePaymentIntentResponse](docs/CreatePaymentIntentResponse.md)
 - [DiscoveredFeed](docs/DiscoveredFeed.md)
 - [Feed](docs/Feed.md)
 - [GenericQueryRequest](docs/GenericQueryRequest.md)
 - [GraphChangeset](docs/GraphChangeset.md)
 - [GraphNode](docs/GraphNode.md)
 - [GraphRelationship](docs/GraphRelationship.md)
 - [LoginRequest](docs/LoginRequest.md)
 - [LoginResponse](docs/LoginResponse.md)
 - [RegisterRequest](docs/RegisterRequest.md)
 - [RegisterResponse](docs/RegisterResponse.md)
 - [SubscriptionRequest](docs/SubscriptionRequest.md)
 - [SubscriptionResponse](docs/SubscriptionResponse.md)
 - [V1GraphIngestPost201Response](docs/V1GraphIngestPost201Response.md)
 - [V1GraphNeighborsNodeValueGet200Response](docs/V1GraphNeighborsNodeValueGet200Response.md)
 - [V1GraphPathsGet200Response](docs/V1GraphPathsGet200Response.md)
 - [V1GraphQueryPost200Response](docs/V1GraphQueryPost200Response.md)
 - [V1GraphStatisticsGet200Response](docs/V1GraphStatisticsGet200Response.md)
 - [V1SystemSettingsWaitlistGet200Response](docs/V1SystemSettingsWaitlistGet200Response.md)


<a id="documentation-for-authorization"></a>
## Documentation For Authorization


Authentication schemes defined for the API:
<a id="ApiKeyAuth"></a>
### ApiKeyAuth

- **Type**: API key
- **API key parameter name**: x-api-key
- **Location**: HTTP header


## Author




