from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional, Tuple

import pandas as pd

from .interface import DataAccess, KpiTotals

# Import config for default data directory
try:
    from ...config import get_config
except ImportError:
    # Fallback for relative imports
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from config import get_config


@dataclass
class _Tables:
    orders: pd.DataFrame
    order_items: pd.DataFrame
    products: pd.DataFrame
    stores: pd.DataFrame
    # Pre-joined “order lines” view to avoid re-joining every call
    lines: pd.DataFrame  # columns after join; see _build_lines()


class CsvDataAccess(DataAccess):
    """
    CSV-backed implementation.
    - Loads CSVs from `data_dir` once at construction.
    - Every method call performs a fresh filter/aggregation pass over the loaded frames
      (so each UI interaction triggers new work, mirroring a DB query).
    """

    def __init__(self, data_dir: str | Path = None) -> None:
        if data_dir is None:
            config = get_config()
            data_dir = config.data_dir
        
        self.data_dir = Path(data_dir)
        
        # If the path is relative, make it relative to the repository root
        if not self.data_dir.is_absolute():
            # Try to find the repository root by looking for characteristic files
            current = Path.cwd()
            repo_root = None
            
            # Look up the directory tree for pyproject.toml or databricks.yml
            for parent in [current] + list(current.parents):
                if (parent / "pyproject.toml").exists() or (parent / "databricks.yml").exists():
                    repo_root = parent
                    break
            
            if repo_root:
                self.data_dir = repo_root / self.data_dir
            else:
                # Fallback to current directory
                self.data_dir = current / self.data_dir
        
        self._tables = self._load_tables(self.data_dir)

    # ---------- loading / join helpers ----------

    @staticmethod
    def _load_tables(data_dir: Path) -> _Tables:
        # Check if data directory exists
        if not data_dir.exists():
            raise FileNotFoundError(
                f"Data directory not found: {data_dir}\n"
                f"Please either:\n"
                f"  1. Generate sample data: python app/backend/seed_data.py\n"
                f"  2. Set DATA_DIR environment variable to point to your data directory\n"
                f"  3. Create a .env file with DATA_DIR=/path/to/your/data"
            )
        
        # Required CSV files
        required_files = ["orders.csv", "order_items.csv", "products.csv", "stores.csv"]
        missing_files = [f for f in required_files if not (data_dir / f).exists()]
        
        if missing_files:
            raise FileNotFoundError(
                f"Required CSV files missing in {data_dir}:\n"
                f"  Missing: {', '.join(missing_files)}\n"
                f"  Expected files: {', '.join(required_files)}\n\n"
                f"Please either:\n"
                f"  1. Generate sample data: python app/backend/seed_data.py\n"
                f"  2. Ensure your data directory contains all required CSV files\n"
                f"  3. Set DATA_DIR environment variable to point to a directory with the required files"
            )
        
        try:
            orders = pd.read_csv(data_dir / "orders.csv", parse_dates=["order_ts"])
            order_items = pd.read_csv(data_dir / "order_items.csv")
            products = pd.read_csv(data_dir / "products.csv")
            stores = pd.read_csv(data_dir / "stores.csv", parse_dates=["opened_date"])
        except Exception as e:
            raise RuntimeError(
                f"Error reading CSV files from {data_dir}: {e}\n"
                f"Please check that the CSV files are valid and readable."
            ) from e

        # Normalize names to avoid collisions
        products = products.rename(columns={"name": "product_name"})
        stores = stores.rename(columns={"name": "store_name"})

        lines = CsvDataAccess._build_lines(orders, order_items, products, stores)

        return _Tables(
            orders=orders,
            order_items=order_items,
            products=products,
            stores=stores,
            lines=lines,
        )

    @staticmethod
    def _build_lines(
        orders: pd.DataFrame,
        order_items: pd.DataFrame,
        products: pd.DataFrame,
        stores: pd.DataFrame,
    ) -> pd.DataFrame:
        df = (
            orders.merge(order_items, on="order_id", how="inner")
                  .merge(products[["product_id", "product_name", "category", "brand"]], on="product_id", how="left")
                  .merge(stores[["store_id", "store_name", "city", "region"]], on="store_id", how="left")
                  .copy()
        )
        # Ensure types
        df["extended_price"] = df["extended_price"].astype(float)
        df["unit_price"] = df["unit_price"].astype(float)
        # Convenience fields
        df["hour"] = df["order_ts"].dt.hour
        return df

    # ---------- contract helpers ----------

    def _filtered_lines(
        self,
        start_ts: datetime,
        end_ts: datetime,
        store_name: Optional[str],
        category: Optional[str],
        product_search: Optional[str],
    ) -> pd.DataFrame:
        df = self._tables.lines

        mask = (df["order_ts"] >= pd.to_datetime(start_ts)) & (df["order_ts"] <= pd.to_datetime(end_ts))
        if store_name:
            mask &= (df["store_name"] == store_name)
        if category:
            mask &= (df["category"] == category)
        if product_search and product_search.strip():
            s = product_search.strip().lower()
            mask &= df["product_name"].str.lower().str.contains(s, na=False)

        return df.loc[mask].copy()

    # ---------- interface implementation ----------

    def get_date_bounds(self) -> Tuple[datetime, datetime]:
        min_ts = self._tables.orders["order_ts"].min()
        max_ts = self._tables.orders["order_ts"].max()
        # Return python datetimes
        return (pd.to_datetime(min_ts).to_pydatetime(), pd.to_datetime(max_ts).to_pydatetime())

    def list_stores(self) -> pd.DataFrame:
        cols = ["store_name", "city", "region"]
        out = self._tables.stores[cols].drop_duplicates().sort_values("store_name").reset_index(drop=True)
        return out

    def list_categories(self) -> pd.DataFrame:
        out = (
            self._tables.products[["category"]]
            .drop_duplicates()
            .sort_values("category")
            .reset_index(drop=True)
        )
        return out

    def get_kpis(
        self,
        start_ts: datetime,
        end_ts: datetime,
        store_name: Optional[str] = None,
        category: Optional[str] = None,
        product_search: Optional[str] = None,
    ) -> KpiTotals:
        flt = self._filtered_lines(start_ts, end_ts, store_name, category, product_search)
        return KpiTotals(
            orders_distinct=int(flt["order_id"].nunique()),
            lines=int(len(flt)),
            units=int(flt["qty"].sum()) if not flt.empty else 0,
            revenue=float(flt["extended_price"].sum()) if not flt.empty else 0.0,
        )

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
        flt = self._filtered_lines(start_ts, end_ts, store_name, category, product_search)

        # Select/rename in the exact order required by the UI
        cols = [
            "order_ts", "order_id", "store_name", "city", "region",
            "product_name", "category", "qty", "unit_price", "extended_price"
        ]
        flt = flt[cols]

        asc = (order_by == "order_ts_asc")
        flt = flt.sort_values("order_ts", ascending=asc)

        # LIMIT (client side, since CSV)
        if limit is not None:
            flt = flt.head(int(limit))

        return flt.reset_index(drop=True)

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
        flt = self._filtered_lines(start_ts, end_ts, store_name, category, product_search)

        if flt.empty:
            if slice_by:
                return pd.DataFrame(columns=["slice_key", "product_name", "item_count"])
            else:
                return pd.DataFrame(columns=["product_name", "item_count"])

        if slice_by is None:
            agg = (
                flt.groupby("product_name", as_index=False)["qty"]
                   .sum()
                   .rename(columns={"qty": "item_count"})
                   .sort_values("item_count", ascending=False)
                   .head(int(top_n))
            )
            return agg.reset_index(drop=True)

        # With slicing: pick top-N products overall, then return long form by slice
        overall = (
            flt.groupby("product_name", as_index=False)["qty"]
               .sum()
               .rename(columns={"qty": "item_count"})
               .sort_values("item_count", ascending=False)
               .head(int(top_n))
        )
        top_products = set(overall["product_name"].tolist())

        key_map = {"store": "store_name", "category": "category", "hour": "hour"}
        key_col = key_map[slice_by]

        sliced = (
            flt[flt["product_name"].isin(top_products)]
            .groupby([key_col, "product_name"], as_index=False)["qty"]
            .sum()
            .rename(columns={"qty": "item_count"})
        )
        sliced = sliced.rename(columns={key_col: "slice_key"})
        return sliced.reset_index(drop=True)
