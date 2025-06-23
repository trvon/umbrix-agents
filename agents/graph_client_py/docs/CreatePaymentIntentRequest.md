# CreatePaymentIntentRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**amount** | **int** |  | 
**currency** | **str** |  | 

## Example

```python
from openapi_client.models.create_payment_intent_request import CreatePaymentIntentRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreatePaymentIntentRequest from a JSON string
create_payment_intent_request_instance = CreatePaymentIntentRequest.from_json(json)
# print the JSON string representation of the object
print(CreatePaymentIntentRequest.to_json())

# convert the object into a dict
create_payment_intent_request_dict = create_payment_intent_request_instance.to_dict()
# create an instance of CreatePaymentIntentRequest from a dict
create_payment_intent_request_from_dict = CreatePaymentIntentRequest.from_dict(create_payment_intent_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


