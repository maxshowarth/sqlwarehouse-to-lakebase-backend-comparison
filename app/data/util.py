from __future__ import annotations

from typing import Literal

from .backends.csv_backend import CsvDataAccess
from .interface import DataAccess

# Import config for getting data directory
try:
    from ..config import get_config
except ImportError:
    # Fallback for relative imports
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from config import get_config


def get_data_access(kind: Literal["csv"] = "csv") -> DataAccess:
    if kind == "csv":
        # Reads from configured CSV folder
        config = get_config()
        return CsvDataAccess(data_dir=config.data_dir)
    raise ValueError(f"Unknown data access kind: {kind}")
s