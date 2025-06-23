"""Umbrix plugin loader utility.

Allows the Master Coordinator (and potentially other components) to discover
and instantiate sub-agents without hard-coded imports.

Supported discovery mechanisms:

1. **Python entry-points** – third-party packages can expose an entry-point in
   the group ``umbrix_agents`` that returns an ``Agent`` subclass.
2. **YAML manifests** – files placed under a directory (default ``plugins/``)
   declaring the fully-qualified class path to import.

The loader returns a registry ``dict[str, Agent]`` mapping the unique agent
``name`` to an instantiated object.

Duplicate agent names raise ``DuplicateAgentError``.
"""

from __future__ import annotations

import importlib
import os
import sys
import textwrap
from pathlib import Path
from typing import Dict, Optional, Type

import yaml

# Optional dependency – entry-points discovery is only attempted if available.
try:
    from importlib.metadata import entry_points, EntryPoint  # Python ≥3.8

    _ENTRY_POINTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _ENTRY_POINTS_AVAILABLE = False


class DuplicateAgentError(RuntimeError):
    """Raised when two plugins declare the same ``agent.name``."""


def _load_class_from_path(path: str):
    """Import ``module.sub:Class`` or ``module.Class`` path and return the class object."""

    # Support ``module:Class`` or ``module.Class`` delimiters
    if ":" in path:
        module_path, class_name = path.split(":", 1)
    else:
        module_path, class_name = path.rsplit(".", 1)

    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def _instantiate_agent(agent_cls: Type, **kwargs):
    """Instantiate an Agent subclass, allowing kwargs injection for tests."""
    try:
        return agent_cls(**kwargs)  # type: ignore[arg-type]
    except TypeError:
        # Fallback to parameter-less construction if signature mismatch.
        return agent_cls()  # type: ignore[call-arg]


def load_plugins(
    *,
    entry_points_group: str = "umbrix_agents",
    manifests_dir: str | Path = "plugins",
    load_entry_points: bool = True,
    extra_agent_kwargs: Optional[dict] = None,
) -> Dict[str, "Agent"]:
    """Discover and instantiate agents.

    Parameters
    ----------
    entry_points_group: str
        Name of the Python entry-points group to inspect.
    manifests_dir: Union[str, Path]
        Directory containing YAML manifests. Each manifest must at least define
        ``name`` and ``class`` keys.
    load_entry_points: bool
        Disable to speed up tests when not required.
    extra_agent_kwargs: dict | None
        Keyword arguments passed to every agent constructor (useful for
        injecting mocked dependencies in tests).
    """

    try:
        from google.adk.agents import Agent  # type: ignore
    except ImportError:  # pragma: no cover – fallback when ADK not installed
        class Agent:  # type: ignore
            """Minimal stand-in for google.adk.agents.Agent used in tests."""

            def __init__(self, name: str = "anonymous_agent", description: str = ""):
                self.name = name
                self.description = description

    registry: Dict[str, Agent] = {}
    kwargs = extra_agent_kwargs or {}

    # -------- 1. Python entry-points --------
    if load_entry_points and _ENTRY_POINTS_AVAILABLE:
        eps = entry_points(group=entry_points_group)
        for ep in eps:
            agent_cls = ep.load()
            agent: Agent = _instantiate_agent(agent_cls, **kwargs)
            _register_agent(agent, registry)

    # -------- 2. YAML manifests --------
    p = Path(manifests_dir)
    if p.exists() and p.is_dir():
        for file in p.iterdir():
            if file.suffix.lower() not in {".yml", ".yaml"}:
                continue
            with file.open("r", encoding="utf-8") as fh:
                manifest = yaml.safe_load(fh) or {}

            name = manifest.get("name")
            class_path = manifest.get("class") or manifest.get("cls")
            if not name or not class_path:
                # Skip invalid manifests but log to stderr.
                print(
                    textwrap.dedent(
                        f"""
                        [plugin_loader] Warning: manifest '{file}' missing required 'name' or 'class' fields – skipping.
                        """
                    ).strip(),
                    file=sys.stderr,
                )
                continue

            agent_cls = _load_class_from_path(class_path)
            agent: Agent = _instantiate_agent(agent_cls, **kwargs)

            # Allow override of agent.name from manifest if constructor sets different default.
            agent.name = name  # type: ignore[attr-defined]

            _register_agent(agent, registry)

    return registry


def _register_agent(agent, registry):
    if agent.name in registry:
        raise DuplicateAgentError(f"Agent name collision: '{agent.name}' already registered")
    registry[agent.name] = agent
