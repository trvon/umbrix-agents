# AgentTask


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**correlation_id** | **str** |  | [optional] 
**payload** | **Dict[str, object]** |  | 

## Example

```python
from openapi_client.models.agent_task import AgentTask

# TODO update the JSON string below
json = "{}"
# create an instance of AgentTask from a JSON string
agent_task_instance = AgentTask.from_json(json)
# print the JSON string representation of the object
print(AgentTask.to_json())

# convert the object into a dict
agent_task_dict = agent_task_instance.to_dict()
# create an instance of AgentTask from a dict
agent_task_from_dict = AgentTask.from_dict(agent_task_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


