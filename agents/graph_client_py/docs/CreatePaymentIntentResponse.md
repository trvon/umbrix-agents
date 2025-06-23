# CreatePaymentIntentResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**client_secret** | **str** |  | 

## Example

```python
from openapi_client.models.create_payment_intent_response import CreatePaymentIntentResponse

# TODO update the JSON string below
json = "{}"
# create an instance of CreatePaymentIntentResponse from a JSON string
create_payment_intent_response_instance = CreatePaymentIntentResponse.from_json(json)
# print the JSON string representation of the object
print(CreatePaymentIntentResponse.to_json())

# convert the object into a dict
create_payment_intent_response_dict = create_payment_intent_response_instance.to_dict()
# create an instance of CreatePaymentIntentResponse from a dict
create_payment_intent_response_from_dict = CreatePaymentIntentResponse.from_dict(create_payment_intent_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


