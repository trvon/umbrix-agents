"""Unit tests for the plugin loader utility.

These tests purposely live **outside** the ``agents`` package so that we can
import the loader in isolation without triggering heavy dependencies pulled in
by ``agents.__init__``.  We stub external libraries that the loader relies on
and create dummy agent classes at runtime.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Stub external libraries that the real agents package may attempt to import.
# We declare them *before* importing the plugin loader so tests remain
# self-contained and fast.
# ---------------------------------------------------------------------------


for _mod in [
    "kafka",
    "kafka.errors",
    "mitreattack",
    "neo4j",
    "sentence_transformers",
    "pyarrow",
    "aiokafka",
]:
    if _mod not in sys.modules:
        sys.modules[_mod] = types.ModuleType(_mod)

# Provide attributes for the kafka stub used by agent modules
if "kafka" in sys.modules:
    kafka_stub = sys.modules["kafka"]

    class _DummyKafkaConsumer:  # noqa: D401
        def __init__(self, *args, **kwargs):
            pass

    class _DummyKafkaProducer:  # noqa: D401
        def __init__(self, *args, **kwargs):
            pass

    setattr(kafka_stub, "KafkaConsumer", _DummyKafkaConsumer)  # type: ignore[attr-defined]
    setattr(kafka_stub, "KafkaProducer", _DummyKafkaProducer)  # type: ignore[attr-defined]

# Add dummy error classes expected by code.
if "kafka.errors" in sys.modules:
    err_mod = sys.modules["kafka.errors"]

    class _KafkaError(Exception):
        pass

    class _CommitFailedError(_KafkaError):
        pass

    setattr(err_mod, "KafkaError", _KafkaError)
    setattr(err_mod, "CommitFailedError", _CommitFailedError)

    # Expose as attribute of parent kafka module for ``from kafka import errors``.
    if "kafka" in sys.modules:
        setattr(sys.modules["kafka"], "errors", err_mod)  # type: ignore[attr-defined]

# Provide a minimal stand-in for ``google.adk.agents.Agent`` so that dynamically
# created dummy agents can inherit from it.

google_mod = types.ModuleType("google")
adk_mod = types.ModuleType("google.adk")
agents_mod = types.ModuleType("google.adk.agents")


class _BaseAgent:  # noqa: D401
    def __init__(self, name: str = "stub", description: str = ""):
        self.name = name
        self.description = description


agents_mod.Agent = _BaseAgent  # type: ignore[attr-defined]
adk_mod.agents = agents_mod  # type: ignore[attr-defined]
google_mod.adk = adk_mod  # type: ignore[attr-defined]

sys.modules["google"] = google_mod
sys.modules["google.adk"] = adk_mod
sys.modules["google.adk.agents"] = agents_mod


# ---------------------------------------------------------------------------
# Helpers to create dummy agent classes and YAML manifests on the fly.
# ---------------------------------------------------------------------------


def _create_dummy_agent_class(class_name: str):
    from google.adk.agents import Agent  # type: ignore  # noqa: WPS433 – runtime import

    class DummyAgent(Agent):  # type: ignore
        def __init__(self):
            super().__init__(name=class_name.lower(), description="dummy")

    return DummyAgent


def _install_dummy_module(module_name: str, class_name: str):
    mod = types.ModuleType(module_name)
    dummy_cls = _create_dummy_agent_class(class_name)
    setattr(mod, class_name, dummy_cls)
    sys.modules[module_name] = mod


# ---------------------------------------------------------------------------
# Test fixtures & utilities
# ---------------------------------------------------------------------------


@pytest.fixture()
def manifest_dir(tmp_path: Path):
    """Return a temporary directory path to store plugin manifests."""

    return tmp_path


def _write_manifest(dir_: Path, name: str, class_path: str):
    dir_.mkdir(parents=True, exist_ok=True)
    (dir_ / f"{name}.yaml").write_text(f"""\
name: {name}
class: {class_path}
""")


# ---------------------------------------------------------------------------
# Actual tests
# ---------------------------------------------------------------------------


def test_manifest_loading(manifest_dir: Path):
    """Loader should instantiate all agents defined in manifests."""

    _install_dummy_module("dummy_pkg.first", "FirstAgent")
    _install_dummy_module("dummy_pkg.second", "SecondAgent")

    _write_manifest(manifest_dir, "first_agent", "dummy_pkg.first.FirstAgent")
    _write_manifest(manifest_dir, "second_agent", "dummy_pkg.second.SecondAgent")

    from agents.common_tools.plugin_loader import load_plugins

    registry = load_plugins(manifests_dir=manifest_dir, load_entry_points=False)

    assert set(registry.keys()) == {"first_agent", "second_agent"}


def test_duplicate_detection(manifest_dir: Path):
    """Duplicate agent names must raise DuplicateAgentError."""

    _install_dummy_module("dummy_pkg.dup", "DupAgent")

    # Write two different manifest files with the same agent name
    (manifest_dir / "dup_agent1.yaml").write_text("name: dup_agent\nclass: dummy_pkg.dup.DupAgent\n")
    (manifest_dir / "dup_agent2.yaml").write_text("name: dup_agent\nclass: dummy_pkg.dup.DupAgent\n")

    from agents.common_tools.plugin_loader import load_plugins, DuplicateAgentError

    with pytest.raises(DuplicateAgentError):
        load_plugins(manifests_dir=manifest_dir, load_entry_points=False)
