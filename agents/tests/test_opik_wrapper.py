"""Tests for the optional Comet OPIK integration wrapper.

The goal of these tests is _not_ to verify OPIK’s functionality (that would
require the external service), but rather to ensure that our helper degrades
gracefully when OPIK is **not** present or when the integration is disabled.
"""

from __future__ import annotations

import importlib
import os
import sys
import types


def _reload_wrapper() -> types.ModuleType:  # pragma: no cover – helper
    """Reload the module to re-evaluate the environment conditions."""

    if "agents.common_tools.opik_wrapper" in sys.modules:
        del sys.modules["agents.common_tools.opik_wrapper"]

    # Re-import after manipulating environment / sys.modules.
    return importlib.import_module("agents.common_tools.opik_wrapper")


def test_decorator_is_noop_when_opik_disabled(monkeypatch):
    """If `opik` cannot be imported **or** the API key is missing the decorator
    must act as a transparent pass-through.
    """

    # Ensure the real opik (if installed) is not picked up.
    monkeypatch.setitem(sys.modules, "opik", types.ModuleType("opik"))

    # Ensure the API key is **not** set.
    monkeypatch.delenv("OPIK_API_KEY", raising=False)

    wrapper = _reload_wrapper()

    # A simple function we can decorate.
    @wrapper.trace_llm_call
    def add(a, b):
        return a + b

    assert add(1, 2) == 3
