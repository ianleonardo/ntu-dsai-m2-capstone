"""
Main entry point for Dagster development server.
"""

import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
os.environ.setdefault("DAGSTER_HOME", str(project_root / ".dagster"))

if __name__ == "__main__":
    from dagster.cli import cli
    
    # Set default repository module
    os.environ.setdefault("DAGSTER_REPOSITORY", "dagster.repository")
    
    cli()
