# openapi_client.DefaultApi

All URIs are relative to *http://localhost:8080/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**v1_feeds_get**](DefaultApi.md#v1_feeds_get) | **GET** /v1/feeds | List RSS feeds
[**v1_graph_neighbors_node_value_get**](DefaultApi.md#v1_graph_neighbors_node_value_get) | **GET** /v1/graph/neighbors/{node_value} | Get neighbors of a node
[**v1_graph_paths_get**](DefaultApi.md#v1_graph_paths_get) | **GET** /v1/graph/paths | Find shortest path between two nodes
[**v1_graph_query_post**](DefaultApi.md#v1_graph_query_post) | **POST** /v1/graph/query | Execute a parameterized Cypher query
[**v1_graph_statistics_get**](DefaultApi.md#v1_graph_statistics_get) | **GET** /v1/graph/statistics | Get graph statistics
[**v1_system_settings_waitlist_get**](DefaultApi.md#v1_system_settings_waitlist_get) | **GET** /v1/system/settings/waitlist | Get waitlist mode status


# **v1_feeds_get**
> List[Feed] v1_feeds_get()

List RSS feeds

### Example

* Api Key Authentication (ApiKeyAuth):

```python
import openapi_client
from openapi_client.models.feed import Feed
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
    except Exception as e:
        print("Exception when calling DefaultApi->v1_feeds_get: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**List[Feed]**](Feed.md)

### Authorization

[ApiKeyAuth](../README.md#ApiKeyAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of feeds |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **v1_graph_neighbors_node_value_get**
> V1GraphNeighborsNodeValueGet200Response v1_graph_neighbors_node_value_get(node_value, depth=depth)

Get neighbors of a node

### Example

* Api Key Authentication (ApiKeyAuth):

```python
import openapi_client
from openapi_client.models.v1_graph_neighbors_node_value_get200_response import V1GraphNeighborsNodeValueGet200Response
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
    node_value = 'node_value_example' # str | 
    depth = 56 # int |  (optional)

    try:
        # Get neighbors of a node
        api_response = api_instance.v1_graph_neighbors_node_value_get(node_value, depth=depth)
        print("The response of DefaultApi->v1_graph_neighbors_node_value_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->v1_graph_neighbors_node_value_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **node_value** | **str**|  | 
 **depth** | **int**|  | [optional] 

### Return type

[**V1GraphNeighborsNodeValueGet200Response**](V1GraphNeighborsNodeValueGet200Response.md)

### Authorization

[ApiKeyAuth](../README.md#ApiKeyAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Neighbor nodes |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **v1_graph_paths_get**
> V1GraphPathsGet200Response v1_graph_paths_get(start_node, end_node)

Find shortest path between two nodes

### Example

* Api Key Authentication (ApiKeyAuth):

```python
import openapi_client
from openapi_client.models.v1_graph_paths_get200_response import V1GraphPathsGet200Response
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
    start_node = 'start_node_example' # str | 
    end_node = 'end_node_example' # str | 

    try:
        # Find shortest path between two nodes
        api_response = api_instance.v1_graph_paths_get(start_node, end_node)
        print("The response of DefaultApi->v1_graph_paths_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->v1_graph_paths_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **start_node** | **str**|  | 
 **end_node** | **str**|  | 

### Return type

[**V1GraphPathsGet200Response**](V1GraphPathsGet200Response.md)

### Authorization

[ApiKeyAuth](../README.md#ApiKeyAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Path results |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **v1_graph_query_post**
> V1GraphQueryPost200Response v1_graph_query_post(generic_query_request)

Execute a parameterized Cypher query

### Example

* Api Key Authentication (ApiKeyAuth):

```python
import openapi_client
from openapi_client.models.generic_query_request import GenericQueryRequest
from openapi_client.models.v1_graph_query_post200_response import V1GraphQueryPost200Response
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
    generic_query_request = openapi_client.GenericQueryRequest() # GenericQueryRequest | 

    try:
        # Execute a parameterized Cypher query
        api_response = api_instance.v1_graph_query_post(generic_query_request)
        print("The response of DefaultApi->v1_graph_query_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->v1_graph_query_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **generic_query_request** | [**GenericQueryRequest**](GenericQueryRequest.md)|  | 

### Return type

[**V1GraphQueryPost200Response**](V1GraphQueryPost200Response.md)

### Authorization

[ApiKeyAuth](../README.md#ApiKeyAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Query results |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **v1_graph_statistics_get**
> V1GraphStatisticsGet200Response v1_graph_statistics_get()

Get graph statistics

### Example

* Api Key Authentication (ApiKeyAuth):

```python
import openapi_client
from openapi_client.models.v1_graph_statistics_get200_response import V1GraphStatisticsGet200Response
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
        # Get graph statistics
        api_response = api_instance.v1_graph_statistics_get()
        print("The response of DefaultApi->v1_graph_statistics_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->v1_graph_statistics_get: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**V1GraphStatisticsGet200Response**](V1GraphStatisticsGet200Response.md)

### Authorization

[ApiKeyAuth](../README.md#ApiKeyAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Graph statistics |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **v1_system_settings_waitlist_get**
> V1SystemSettingsWaitlistGet200Response v1_system_settings_waitlist_get()

Get waitlist mode status

### Example

* Api Key Authentication (ApiKeyAuth):

```python
import openapi_client
from openapi_client.models.v1_system_settings_waitlist_get200_response import V1SystemSettingsWaitlistGet200Response
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
        # Get waitlist mode status
        api_response = api_instance.v1_system_settings_waitlist_get()
        print("The response of DefaultApi->v1_system_settings_waitlist_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->v1_system_settings_waitlist_get: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**V1SystemSettingsWaitlistGet200Response**](V1SystemSettingsWaitlistGet200Response.md)

### Authorization

[ApiKeyAuth](../README.md#ApiKeyAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Waitlist enabled/disabled |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

