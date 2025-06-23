# V1GraphStatisticsGet200Response


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**total_nodes** | **int** |  | [optional] 
**source_nodes** | **int** |  | [optional] 

## Example

```python
from openapi_client.models.v1_graph_statistics_get200_response import V1GraphStatisticsGet200Response

# TODO update the JSON string below
json = "{}"
# create an instance of V1GraphStatisticsGet200Response from a JSON string
v1_graph_statistics_get200_response_instance = V1GraphStatisticsGet200Response.from_json(json)
# print the JSON string representation of the object
print(V1GraphStatisticsGet200Response.to_json())

# convert the object into a dict
v1_graph_statistics_get200_response_dict = v1_graph_statistics_get200_response_instance.to_dict()
# create an instance of V1GraphStatisticsGet200Response from a dict
v1_graph_statistics_get200_response_from_dict = V1GraphStatisticsGet200Response.from_dict(v1_graph_statistics_get200_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


