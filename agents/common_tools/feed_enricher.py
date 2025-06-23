import dspy
from .dspy_signatures import FeedContentInput, EnrichedFeedData # Adjust path as needed
from .models import FeedRecord # For type hinting
from typing import Optional
import sys # For stderr

class DSPyFeedEnricher:
    def __init__(self):
        # DSPy LM (dspy.settings.lm) is expected to be configured globally
        # before this class is instantiated or its methods are called.
        # See agents/common_tools/content_tools.py for an example of global configuration.
        if dspy.settings.lm is None:
            # This is a warning during initialization. The enrich method will also check.
            print("[DSPyFeedEnricher] Warning: DSPy LM is not configured at initialization. Enrichment will fail if not configured before use.", file=sys.stderr)
        
        self.enrich_feed_item = dspy.ChainOfThought(EnrichedFeedData)

    def enrich(self, record: FeedRecord) -> FeedRecord:
        """
        Enriches a FeedRecord using DSPy.
        Assumes dspy.settings.lm is configured globally.
        Assumes record.raw_content is populated.
        The enrichments are added to record.metadata and/or directly to fields.
        """
        if dspy.settings.lm is None:
            # print("[DSPyFeedEnricher] Error: DSPy LM not configured. Cannot enrich.", file=sys.stderr)
            setattr(record.metadata, 'dspy_enrichment_status', 'error_dspy_lm_not_configured')
            return record

        if not record.raw_content:
            # print(f"[DSPyFeedEnricher] Skipping enrichment for record {record.url} due to missing raw_content.", file=sys.stderr)
            setattr(record.metadata, 'dspy_enrichment_status', 'skipped_no_raw_content')
            return record

        try:
            prediction = self.enrich_feed_item(
                raw_content=record.raw_content,
                current_title=record.title,
                current_description=record.description
            )

            enrichment_data = {
                'guessed_title': prediction.guessed_title,
                'guessed_description': prediction.guessed_description,
                'is_security_focused': prediction.is_security_focused,
                'standardized_vendor_name': prediction.standardized_vendor_name,
                'requires_payment': prediction.requires_payment,
            }
            if hasattr(prediction, 'rationale'):
                 enrichment_data['rationale'] = prediction.rationale
            
            setattr(record.metadata, 'dspy_enrichment', enrichment_data)
            setattr(record.metadata, 'dspy_enrichment_status', 'success')

            if prediction.guessed_title and (not record.title or len(record.title) < 10): 
                record.title = prediction.guessed_title
            
            if prediction.guessed_description and (not record.description or len(record.description) < 20): 
                record.description = prediction.guessed_description
            
            record.is_security_focused = prediction.is_security_focused
            record.requires_payment = prediction.requires_payment
            if prediction.standardized_vendor_name:
                record.vendor_name = prediction.standardized_vendor_name

        except Exception as e:
            # print(f"[DSPyFeedEnricher] Error during DSPy enrichment for {record.url}: {e}", file=sys.stderr)
            setattr(record.metadata, 'dspy_enrichment_status', 'error')
            setattr(record.metadata, 'dspy_enrichment_error', str(e))

        return record

# Example usage (ensure DSPy is configured globally first as in content_tools.py):
# from ..common.models import FeedRecord 
# from pydantic import AnyUrl
#
# if __name__ == '__main__':
#     # 1. Global DSPy LM configuration ( mimicking content_tools.py setup manually or by importing it implicitly)
#     # For this example, let's use a MockLLM as before, but assume it's set globally.
#     class MockLLM(dspy.LM):
#         def __init__(self):
#             super().__init__("mock_model")
#             self.provider = "mock"
#         def basic_request(self, prompt, **kwargs):
#             response_data = {
#                 'guessed_title': 'Mocked Title: Important News',
#                 'guessed_description': 'This is a mocked summary. It is crucial.',
#                 'is_security_focused': True,
#                 'standardized_vendor_name': 'MockSecurity Corp.',
#                 'requires_payment': False,
#                 'rationale': 'Mocked rationale: The article discusses security vulnerabilities.'
#             }
#             text_output = f"Rationale: {response_data['rationale']}\n---\n"
#             text_output += f"Guessed Title: {response_data['guessed_title']}\n"
#             text_output += f"Guessed Description: {response_data['guessed_description']}\n"
#             text_output += f"Is Security Focused: {str(response_data['is_security_focused']).lower()}\n"
#             text_output += f"Standardized Vendor Name: {response_data['standardized_vendor_name']}\n"
#             text_output += f"Requires Payment: {str(response_data['requires_payment']).lower()}\n"
#             return {"choices": [{"text": text_output}]}
#         def __call__(self, prompt, only_completed=True, return_sorted=False, **kwargs):
#             return [self.basic_request(prompt, **kwargs)["choices"][0]["text"]]
# 
#     if not dspy.settings.lm: # Check if LM is already configured by other means
#          dspy.settings.configure(lm=MockLLM()) # Configure DSPy globally with the mock
#          print("[DSPyFeedEnricher Example] Configured DSPy with MockLLM.", file=sys.stderr)
#     else:
#          print(f"[DSPyFeedEnricher Example] DSPy LM already configured: {dspy.settings.lm}", file=sys.stderr)
#
#     # 2. Initialize enricher (it no longer takes llm_client)
#     enricher = DSPyFeedEnricher()
# 
#     # 3. Create a sample FeedRecord
#     try:
#         valid_url = AnyUrl("http://example.com/article_for_enrichment")
#     except Exception as e:
#         print(f"URL validation error: {e}", file=sys.stderr)
#         valid_url = AnyUrl("http://default.com")
# 
#     my_record = FeedRecord(
#         url=valid_url, 
#         raw_content="<html><body><h1>Major Security Flaw Found</h1><p>A new CVE has been announced that affects many systems. Experts advise immediate patching.</p></body></html>",
#         title="Flaw Found",
#     )
# 
#     print("\nRecord before enrichment:")
#     print(my_record.model_dump_json(indent=2))
# 
#     enriched_record = enricher.enrich(my_record)
# 
#     print("\nRecord after enrichment:")
#     print(enriched_record.model_dump_json(indent=2))
#
#     # Example with missing raw_content
#     no_content_record = FeedRecord(url=AnyUrl("http://example.com/another_article"))
#     enriched_no_content_record = enricher.enrich(no_content_record)
#     print("\nRecord with no raw_content after attempting enrichment:")
#     print(enriched_no_content_record.model_dump_json(indent=2))
