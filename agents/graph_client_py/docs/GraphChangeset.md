# GraphChangeset


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**nodes** | [**List[GraphNode]**](GraphNode.md) |  | [optional] 
**relationships** | [**List[GraphRelationship]**](GraphRelationship.md) |  | [optional] 

## Example

```python
from openapi_client.models.graph_changeset import GraphChangeset

# TODO update the JSON string below
json = "{}"
# create an instance of GraphChangeset from a JSON string
graph_changeset_instance = GraphChangeset.from_json(json)
# print the JSON string representation of the object
print(GraphChangeset.to_json())

# convert the object into a dict
graph_changeset_dict = graph_changeset_instance.to_dict()
# create an instance of GraphChangeset from a dict
graph_changeset_from_dict = GraphChangeset.from_dict(graph_changeset_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


