# Agents Implementation Issues Tracker

This document tracks pending tasks and missing implementations in the `agents/` folder.

## Configuration

- [x] Add LLM flags under `MainGraphIngester` in `config.yaml`:
  - `enrich_with_llm: true|false`
  - `gemini_model` (e.g. `${GEMINI_MODEL_NAME}`)
  - `gemini_api_key` or reference to `${GOOGLE_API_KEY}`

- [x] Verify `MasterCoordinator` config entry reads `gemini_model` and `gemini_api_key` correctly.

## Master Coordinator (`coordinator_agent/master_coordinator.py`)

- [x] Move default for `gemini_model` out of method signature into `__init__` body for dynamic env lookup.
- [x] Add unit tests in `tests/test_master_coordinator.py` to assert `genai.chat.create` uses configured model.

## Graph Ingestion Agent (`graph_ingestion_agent/graph_ingestion_agent.py`)

- [ ] Ensure constructor signature (`enrich_with_llm`, `gemini_api_key`, `gemini_model`) matches config loader behavior.
- [ ] Confirm `load_agents_from_config` passes these kwargs through to `GraphIngestionAgent`.
- [ ] Add tests for:
  - `_should_enrich_node` heuristics
  - `_llm_process_text` integration (mock `genai.chat.create`)

## Agent Loader (`agents/main.py`)

- [ ] Update `load_agents_from_config` to accept arbitrary params (including LLM flags) for all agents, not only feed/servers.
- [ ] Validate environment variable substitution for nested structures (lists/dicts) in LLM settings.

## DSPy Extraction Tool (`common_tools/dspy_extraction_tool.py`)

- [ ] Wire `dspy_settings.model_name` and `api_key_env_var` into DSPy client initialization.
- [ ] Add integration test for `DSPyExtractionTool` honoring `model_name` override.

## Documentation

- [ ] Update `docs/llm_tools.md` with:
  - New `enrich_with_llm` toggle
  - Heuristics for triggering enrichment
  - Examples of `gemini_model` overrides

## Testing & CI

- [ ] Add CI checks to ensure no regression in agent startup when LLM flags are present.
- [ ] Mock Kafka messages with `full_text` of varying lengths to validate enrichment triggers.

---
*Last updated: 2025-05-20*
