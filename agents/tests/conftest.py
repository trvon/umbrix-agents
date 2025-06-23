"""Pytest configuration for agents tests."""
import os
import sys

# Add project root (two levels up) to Python path for imports
project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..')
)
sys.path.insert(0, project_root)

# Add current agents directory to path for direct imports
agents_root = os.path.abspath(os.path.dirname(__file__) + '/..')
sys.path.insert(0, agents_root)
