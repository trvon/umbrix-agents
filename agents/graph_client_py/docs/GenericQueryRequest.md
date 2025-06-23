# GenericQueryRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**cypher** | **str** |  | 
**params** | **Dict[str, object]** |  | [optional] 

## Example

```python
from openapi_client.models.generic_query_request import GenericQueryRequest

# TODO update the JSON string below
json = "{}"
# create an instance of GenericQueryRequest from a JSON string
generic_query_request_instance = GenericQueryRequest.from_json(json)
# print the JSON string representation of the object
print(GenericQueryRequest.to_json())

# convert the object into a dict
generic_query_request_dict = generic_query_request_instance.to_dict()
# create an instance of GenericQueryRequest from a dict
generic_query_request_from_dict = GenericQueryRequest.from_dict(generic_query_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


