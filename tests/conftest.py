# tests/conftest.py
import sys
from pathlib import Path

# Prepend the project root to sys.path before any test modules are imported
project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))
