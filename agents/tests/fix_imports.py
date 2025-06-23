#!/usr/bin/env python3
"""
Script to fix imports in test files to use absolute imports.
"""
from pathlib import Path
import re
import sys

def fix_imports(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    
    # Define pattern replacements for imports
    replacements = [
        # Fix relative imports
        (r'from \.\.([\w_]+)\.', r'from agents.\1.'),
        (r'from \.\.([\w_]+) import', r'from agents.\1 import'),
        (r'import \.\.([\w_]+)', r'import agents.\1'),
        
        # Fix absolute imports that were inconsistent
        (r'from common_tools\.', r'from agents.common_tools.'),
        (r'from common\.', r'from agents.common.'),
        (r'import common_tools', r'import agents.common_tools'),
        (r'import common(?!\w)', r'import agents.common'),  # Don't match common in longer words
        
        # Fix imports from agents subdirectories
        (r'from agents\.collector_agent\.', r'from agents.collector_agent.'),
        (r'from agents\.normalization_agent\.', r'from agents.normalization_agent.'),
        (r'from agents\.enricher_agent\.', r'from agents.enricher_agent.'),
    ]
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)
    
    with open(file_path, 'w') as file:
        file.write(content)
    
    print(f"Fixed imports in {file_path}")

def main():
    # Process all Python files in the tests directory
    tests_dir = Path("/Volumes/picaso/work/tools/umbrix/agents/tests")
    for py_file in tests_dir.glob("*.py"):
        if py_file.name not in ["__init__.py", "conftest.py", "fix_imports.py"]:
            fix_imports(py_file)

if __name__ == "__main__":
    main()
