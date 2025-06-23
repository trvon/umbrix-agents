# GraphRelationship


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**source_id** | **str** |  | [optional] 
**target_id** | **str** |  | [optional] 
**type** | **str** |  | [optional] 
**properties** | **object** |  | [optional] 

## Example

```python
from openapi_client.models.graph_relationship import GraphRelationship

# TODO update the JSON string below
json = "{}"
# create an instance of GraphRelationship from a JSON string
graph_relationship_instance = GraphRelationship.from_json(json)
# print the JSON string representation of the object
print(GraphRelationship.to_json())

# convert the object into a dict
graph_relationship_dict = graph_relationship_instance.to_dict()
# create an instance of GraphRelationship from a dict
graph_relationship_from_dict = GraphRelationship.from_dict(graph_relationship_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


