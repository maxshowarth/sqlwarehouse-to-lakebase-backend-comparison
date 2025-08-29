# src/data_access/interface.py
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional, Protocol, Tuple

import pandas as pd
from pydantic import BaseModel


# ---- Types returned to the UI ----

class KpiTotals(BaseModel):
    """Aggregate KPIs computed server-side for the current filter set."""
    orders_distinct: int   # DISTINCT orders in the filtered window
    lines: int             # number of order line rows
    units: int             # SUM(qty)
    revenue: float         # SUM(extended_price)


# ---- Data access protocol ----

class DataAccess(Protocol):
    """
    Backend-agnostic contract for the Streamlit UI.

    IMPORTANT for latency demo:
    - Implementations MUST avoid result caching inside these methods.
      Each call should execute a fresh query against the underlying source.
    """

    # Dimension / metadata calls
    def get_date_bounds(self) -> Tuple[datetime, datetime]:
        ...

    def list_stores(self) -> pd.DataFrame:
        ...

    def list_categories(self) -> pd.DataFrame:
        ...

    # KPI + table + chart queries
    def get_kpis(
        self,
        start_ts: datetime,
        end_ts: datetime,
        store_name: Optional[str] = None,
        category: Optional[str] = None,
        product_search: Optional[str] = None,
    ) -> KpiTotals:
        ...

    def get_orders(
        self,
        start_ts: datetime,
        end_ts: datetime,
        store_name: Optional[str] = None,
        category: Optional[str] = None,
        product_search: Optional[str] = None,
        limit: int = 2000,
        order_by: Literal["order_ts_desc", "order_ts_asc"] = "order_ts_desc",
    ) -> pd.DataFrame:
        ...

    def get_product_counts(
        self,
        start_ts: datetime,
        end_ts: datetime,
        slice_by: Optional[Literal["store", "category", "hour"]] = None,
        store_name: Optional[str] = None,
        category: Optional[str] = None,
        product_search: Optional[str] = None,
        top_n: int = 25,
    ) -> pd.DataFrame:
        ...
