import dspy
from pydantic import BaseModel, Field
from typing import Optional, List

class FeedContentInput(dspy.Signature):
    """
    Input for DSPy enrichment: raw content and existing (potentially partial) feed data.
    """
    raw_content: str = dspy.InputField(desc="Raw HTML or JSON content of the feed item or page.")
    current_title: Optional[str] = dspy.InputField(desc="Existing title, if any.", prefix="Current Title:")
    current_description: Optional[str] = dspy.InputField(desc="Existing description, if any.", prefix="Current Description:")
    # We could add other fields from FeedRecord if they are useful context for the LLM

    # Ensure the docstring clearly states what the DSPy program should *do*
    # (even though this is an input signature, it sets context for the output)
    __doc__ = """Analyze the provided raw_content (e.g., HTML, JSON) of a web page or feed item.
Based on this content AND the optionally provided current_title and current_description:
1.  If the title is missing or weak, generate a concise and informative title.
2.  If the description is missing or weak, generate a brief summary (1-2 sentences).
3.  Determine if the content is primarily focused on cybersecurity or information security.
4.  Identify the primary vendor or organization name mentioned, if any, and standardize it.
5.  Determine if the content appears to be behind a paywall or requires payment/subscription for full access.
"""

class EnrichedFeedData(dspy.Signature):
    """
    Output of DSPy enrichment: guessed/standardized values.
    These will be placed into the FeedRecord's metadata field or update existing fields.
    """
    # Ensure output fields match the tasks in FeedContentInput.__doc__
    guessed_title: Optional[str] = dspy.OutputField(desc="A concise and informative title for the content, especially if the original was missing or weak.")
    guessed_description: Optional[str] = dspy.OutputField(desc="A brief summary (1-2 sentences) of the content, especially if the original was missing or weak.")
    is_security_focused: bool = dspy.OutputField(desc="True if the content is primarily focused on cybersecurity or information security, False otherwise.")
    standardized_vendor_name: Optional[str] = dspy.OutputField(desc="The standardized name of the primary vendor or organization mentioned (e.g., 'Microsoft', 'CrowdStrike').")
    requires_payment: bool = dspy.OutputField(desc="True if the content appears to be behind a paywall or requires payment/subscription for full access, False otherwise.")
    # We can add a "confidence" or "reasoning" field if needed, similar to summary_note in your llm_tools.md examples

    # This combined signature can then be used by a dspy.Predict or dspy.ChainOfThought module.
    # Example: dspy.Predict(EnrichedFeedData) or a custom DSPy program. 