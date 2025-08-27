from __future__ import annotations

from typing import Literal

from .csv_backend import CsvDataAccess
from .interface import DataAccess


def get_data_access(kind: Literal["csv"] = "csv") -> DataAccess:
    if kind == "csv":
        # Reads from local CSV folder used by our seeding step
        return CsvDataAccess(data_dir="sample_data")
    raise ValueError(f"Unknown data access kind: {kind}")
s