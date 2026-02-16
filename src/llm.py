"""LLM integration module (placeholder).

LLM calls are the LAST resort for data extraction. Currently all 4 adapters
use deterministic parsing with confidence scores of 0.90-1.0, so this module
is not yet needed.

When needed, this module will:
- Use the Anthropic API (Claude) with structured output prompts
- Cache all LLM responses to avoid redundant API calls
- Log input, output, and confidence score for every call
- Flag any extraction with confidence below 0.85 for human review
"""
