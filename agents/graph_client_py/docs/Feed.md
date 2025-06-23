# Feed


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**feed_url** | **str** |  | 
**created_at** | **datetime** |  | 

## Example

```python
from openapi_client.models.feed import Feed

# TODO update the JSON string below
json = "{}"
# create an instance of Feed from a JSON string
feed_instance = Feed.from_json(json)
# print the JSON string representation of the object
print(Feed.to_json())

# convert the object into a dict
feed_dict = feed_instance.to_dict()
# create an instance of Feed from a dict
feed_from_dict = Feed.from_dict(feed_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


