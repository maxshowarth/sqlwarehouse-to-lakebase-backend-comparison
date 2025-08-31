from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional, Tuple, Union, List

import pandas as pd

from ..interface import DataAccess
from ..models import (
    CustomerFilters, OrderFilters, OrderItemsFilters, ProductFilters, StoreFilters,
    InventoryFilters, PromotionFilters, CustomerResponse, ProductResponse, StoreResponse,
    OrderResponse, OrderItemResponse, InventoryResponse, PromotionResponse,
    StringList, IntList, DateTimeList, DateBounds
)

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
    customers: pd.DataFrame
    inventory: pd.DataFrame
    promotions: pd.DataFrame
    # Pre-joined "order lines" view to avoid re-joining every call
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

            # Load optional tables if they exist
            customers = pd.DataFrame()
            inventory = pd.DataFrame()
            promotions = pd.DataFrame()

            if (data_dir / "customers.csv").exists():
                customers = pd.read_csv(data_dir / "customers.csv", parse_dates=["signup_ts"])
            if (data_dir / "inventory_snapshots.csv").exists():
                inventory = pd.read_csv(data_dir / "inventory_snapshots.csv", parse_dates=["snapshot_ts"])
            if (data_dir / "promotions.csv").exists():
                promotions = pd.read_csv(data_dir / "promotions.csv", parse_dates=["start_date", "end_date"])

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
            customers=customers,
            inventory=inventory,
            promotions=promotions,
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

    def get_date_bounds(self) -> Union[Tuple[datetime, datetime], DateBounds]:
        min_ts = self._tables.orders["order_ts"].min()
        max_ts = self._tables.orders["order_ts"].max()
        # Return python datetimes
        return (pd.to_datetime(min_ts).to_pydatetime(), pd.to_datetime(max_ts).to_pydatetime())

    def list_store_opening_date_bounds(self) -> Union[Tuple[datetime, datetime], DateBounds]:
        if self._tables.stores.empty:
            return (datetime.now(), datetime.now())
        min_ts = self._tables.stores["opened_date"].min()
        max_ts = self._tables.stores["opened_date"].max()
        return (pd.to_datetime(min_ts).to_pydatetime(), pd.to_datetime(max_ts).to_pydatetime())

    def list_store_cities(self) -> Union[pd.DataFrame, StringList]:
        if self._tables.stores.empty:
            return StringList(values=[])
        cities = self._tables.stores["city"].dropna().unique().tolist()
        return StringList(values=sorted(cities))

    def list_store_regions(self) -> Union[pd.DataFrame, StringList]:
        if self._tables.stores.empty:
            return StringList(values=[])
        regions = self._tables.stores["region"].dropna().unique().tolist()
        return StringList(values=sorted(regions))

    def list_product_names(self) -> Union[pd.DataFrame, List[ProductResponse]]:
        if self._tables.products.empty:
            return []
        # Return as DataFrame for now, can be enhanced to return ProductResponse objects
        return self._tables.products[["product_id", "product_name", "category", "brand"]].copy()

    def list_product_categories(self) -> Union[pd.DataFrame, StringList]:
        if self._tables.products.empty:
            return StringList(values=[])
        categories = self._tables.products["category"].dropna().unique().tolist()
        return StringList(values=sorted(categories))

    def list_product_brands(self) -> Union[pd.DataFrame, StringList]:
        if self._tables.products.empty:
            return StringList(values=[])
        brands = self._tables.products["brand"].dropna().unique().tolist()
        return StringList(values=sorted(brands))

    def list_customer_segments(self) -> Union[pd.DataFrame, StringList]:
        if self._tables.customers.empty:
            return StringList(values=[])
        segments = self._tables.customers["segment"].dropna().unique().tolist()
        return StringList(values=sorted(segments))

    def list_customer_home_regions(self) -> Union[pd.DataFrame, StringList]:
        if self._tables.customers.empty:
            return StringList(values=[])
        regions = self._tables.customers["home_region"].dropna().unique().tolist()
        return StringList(values=sorted(regions))

    def list_customer_home_cities(self) -> Union[pd.DataFrame, StringList]:
        if self._tables.customers.empty:
            return StringList(values=[])
        cities = self._tables.customers["home_city"].dropna().unique().tolist()
        return StringList(values=sorted(cities))

    def list_promo_types(self) -> Union[pd.DataFrame, StringList]:
        if self._tables.promotions.empty:
            return StringList(values=[])
        types = self._tables.promotions["promo_type"].dropna().unique().tolist()
        return StringList(values=sorted(types))

    def list_promo_date_bounds(self) -> Union[Tuple[datetime, datetime], DateBounds]:
        if self._tables.promotions.empty:
            return (datetime.now(), datetime.now())
        min_ts = self._tables.promotions["start_date"].min()
        max_ts = self._tables.promotions["end_date"].max()
        return (pd.to_datetime(min_ts).to_pydatetime(), pd.to_datetime(max_ts).to_pydatetime())

    def list_payment_types(self) -> Union[pd.DataFrame, StringList]:
        if self._tables.orders.empty:
            return StringList(values=[])
        types = self._tables.orders["payment_type"].dropna().unique().tolist()
        return StringList(values=sorted(types))

    def list_order_date_bounds(self) -> Union[Tuple[datetime, datetime], DateBounds]:
        return self.get_date_bounds()

    def list_order_payment_types(self) -> Union[pd.DataFrame, StringList]:
        return self.list_payment_types()

    # Customer data queries
    def get_customers(self, filters: CustomerFilters) -> Union[pd.DataFrame, List[CustomerResponse]]:
        if self._tables.customers.empty:
            return pd.DataFrame()

        df = self._tables.customers.copy()

        if filters.start_ts:
            df = df[df["signup_ts"] >= filters.start_ts]
        if filters.end_ts:
            df = df[df["signup_ts"] <= filters.end_ts]
        if filters.segment:
            if isinstance(filters.segment, str):
                df = df[df["segment"] == filters.segment]
            else:
                df = df[df["segment"].isin(filters.segment)]
        if filters.home_region:
            if isinstance(filters.home_region, str):
                df = df[df["home_region"] == filters.home_region]
            else:
                df = df[df["home_region"].isin(filters.home_region)]
        if filters.home_city:
            if isinstance(filters.home_city, str):
                df = df[df["home_city"] == filters.home_city]
            else:
                df = df[df["home_city"].isin(filters.home_city)]

        return df

    # Order data queries
    def get_orders(
        self,
        filters: OrderFilters,
        limit: int = 2000,
        order_by: Literal["order_ts_desc", "order_ts_asc"] = "order_ts_desc",
        store_name: Optional[str] = None,
        category: Optional[str] = None,
        product_search: Optional[str] = None,
    ) -> Union[pd.DataFrame, List[OrderResponse]]:
        df = self._tables.lines.copy()

        if filters.start_ts:
            df = df[df["order_ts"] >= filters.start_ts]
        if filters.end_ts:
            df = df[df["order_ts"] <= filters.end_ts]
        if filters.store_id:
            if isinstance(filters.store_id, int):
                df = df[df["store_id"] == filters.store_id]
            else:
                df = df[df["store_id"].isin(filters.store_id)]
        if filters.customer_id:
            if isinstance(filters.customer_id, int):
                df = df[df["customer_id"] == filters.customer_id]
            else:
                df = df[df["customer_id"].isin(filters.customer_id)]
        if filters.payment_type:
            if isinstance(filters.payment_type, str):
                df = df[df["payment_type"] == filters.payment_type]
            else:
                df = df[df["payment_type"].isin(filters.payment_type)]

        # Additional filters for backward compatibility
        if store_name:
            df = df[df["store_name"] == store_name]
        if category:
            df = df[df["category"] == category]
        if product_search and product_search.strip():
            s = product_search.strip().lower()
            df = df[df["product_name"].str.lower().str.contains(s, na=False)]

        # Select/rename in the exact order required by the UI
        cols = [
            "order_ts", "order_id", "store_name", "city", "region",
            "product_name", "category", "qty", "unit_price", "extended_price"
        ]
        df = df[cols]

        asc = (order_by == "order_ts_asc")
        df = df.sort_values("order_ts", ascending=asc)

        # LIMIT (client side, since CSV)
        if limit is not None:
            df = df.head(int(limit))

        return df.reset_index(drop=True)

    # Order items data queries
    def get_order_items(self, filters: OrderItemsFilters) -> Union[pd.DataFrame, List[OrderItemResponse]]:
        df = self._tables.order_items.copy()

        if filters.start_ts:
            # Join with orders to get order_ts
            orders_subset = self._tables.orders[
                (self._tables.orders["order_ts"] >= filters.start_ts) &
                (self._tables.orders["order_ts"] <= filters.end_ts)
            ]["order_id"]
            df = df[df["order_id"].isin(orders_subset)]
        if filters.end_ts:
            # Already handled above
            pass
        if filters.order_id:
            if isinstance(filters.order_id, int):
                df = df[df["order_id"] == filters.order_id]
            else:
                df = df[df["order_id"].isin(filters.order_id)]
        if filters.product_id:
            if isinstance(filters.product_id, int):
                df = df[df["product_id"] == filters.product_id]
            else:
                df = df[df["product_id"].isin(filters.product_id)]
        if filters.qty_min is not None:
            df = df[df["qty"] >= filters.qty_min]
        if filters.qty_max is not None:
            df = df[df["qty"] <= filters.qty_max]
        if filters.unit_price_min is not None:
            df = df[df["unit_price"] >= filters.unit_price_min]
        if filters.unit_price_max is not None:
            df = df[df["unit_price"] <= filters.unit_price_max]

        return df

    # Product data queries
    def get_products(self, filters: ProductFilters) -> Union[pd.DataFrame, List[ProductResponse]]:
        df = self._tables.products.copy()

        if filters.category:
            if isinstance(filters.category, str):
                df = df[df["category"] == filters.category]
            else:
                df = df[df["category"].isin(filters.category)]
        if filters.brand:
            if isinstance(filters.brand, str):
                df = df[df["brand"] == filters.brand]
            else:
                df = df[df["brand"].isin(filters.brand)]
        if filters.price_min is not None:
            df = df[df["base_price"] >= filters.price_min]
        if filters.price_max is not None:
            df = df[df["base_price"] <= filters.price_max]

        return df

    # Store data queries
    def get_stores(self, filters: StoreFilters) -> Union[pd.DataFrame, List[StoreResponse]]:
        df = self._tables.stores.copy()

        if filters.region:
            if isinstance(filters.region, str):
                df = df[df["region"] == filters.region]
            else:
                df = df[df["region"].isin(filters.region)]
        if filters.city:
            if isinstance(filters.city, str):
                df = df[df["city"] == filters.city]
            else:
                df = df[df["city"].isin(filters.city)]
        if filters.store_id:
            if isinstance(filters.store_id, int):
                df = df[df["store_id"] == filters.store_id]
            else:
                df = df[df["store_id"].isin(filters.store_id)]

        return df

    # Inventory data queries
    def get_inventory(self, filters: InventoryFilters) -> Union[pd.DataFrame, List[InventoryResponse]]:
        if self._tables.inventory.empty:
            return pd.DataFrame()

        df = self._tables.inventory.copy()

        if filters.start_ts:
            df = df[df["snapshot_ts"] >= filters.start_ts]
        if filters.end_ts:
            df = df[df["snapshot_ts"] <= filters.end_ts]
        if filters.store_id:
            if isinstance(filters.store_id, int):
                df = df[df["store_id"] == filters.store_id]
            else:
                df = df[df["store_id"].isin(filters.store_id)]
        if filters.product_id:
            if isinstance(filters.product_id, int):
                df = df[df["product_id"] == filters.product_id]
            else:
                df = df[df["product_id"].isin(filters.product_id)]
        if filters.on_hand_min is not None:
            df = df[df["on_hand"] >= filters.on_hand_min]
        if filters.on_hand_max is not None:
            df = df[df["on_hand"] <= filters.on_hand_max]
        if filters.on_order_min is not None:
            df = df[df["on_order"] >= filters.on_order_min]
        if filters.on_order_max is not None:
            df = df[df["on_order"] <= filters.on_order_max]
        if filters.below_safety is not None:
            df = df[df["below_safety"] == filters.below_safety]

        return df

    # Promotion data queries
    def get_promotions(self, filters: PromotionFilters) -> Union[pd.DataFrame, List[PromotionResponse]]:
        if self._tables.promotions.empty:
            return pd.DataFrame()

        df = self._tables.promotions.copy()

        if filters.start_date:
            df = df[df["start_date"] >= filters.start_date]
        if filters.end_date:
            df = df[df["end_date"] <= filters.end_date]
        if filters.product_id:
            if isinstance(filters.product_id, int):
                df = df[df["product_id"] == filters.product_id]
            else:
                df = df[df["product_id"].isin(filters.product_id)]
        if filters.promo_type:
            if isinstance(filters.promo_type, str):
                df = df[df["promo_type"] == filters.promo_type]
            else:
                df = df[df["promo_type"].isin(filters.promo_type)]
        if filters.discount_pct_min is not None:
            df = df[df["discount_pct"] >= filters.discount_pct_min]
        if filters.discount_pct_max is not None:
            df = df[df["discount_pct"] <= filters.discount_pct_max]

        return df

    def get_product_counts(
        self,
        start_ts: datetime,
        end_ts: datetime,
        slice_by: Optional[Literal["store", "category", "hour"]] = None,
        store_name: Optional[str] = None,
        category: Optional[str] = None,
        product_search: Optional[str] = None,
        top_n: int = 25,
    ) -> Union[pd.DataFrame, List[ProductResponse]]:
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

    # ---------- Individual KPI methods (replacing KpiTotals) ----------

    def get_orders_distinct_count(
        self,
        start_ts: datetime,
        end_ts: datetime,
        store_name: Optional[str] = None,
        category: Optional[str] = None,
        product_search: Optional[str] = None,
    ) -> int:
        """Get distinct order count for the current filter set."""
        flt = self._filtered_lines(start_ts, end_ts, store_name, category, product_search)
        return int(flt["order_id"].nunique())

    def get_order_lines_count(
        self,
        start_ts: datetime,
        end_ts: datetime,
        store_name: Optional[str] = None,
        category: Optional[str] = None,
        product_search: Optional[str] = None,
    ) -> int:
        """Get total order lines count for the current filter set."""
        flt = self._filtered_lines(start_ts, end_ts, store_name, category, product_search)
        return int(len(flt))

    def get_total_units(
        self,
        start_ts: datetime,
        end_ts: datetime,
        store_name: Optional[str] = None,
        category: Optional[str] = None,
        product_search: Optional[str] = None,
    ) -> int:
        """Get total units sold for the current filter set."""
        flt = self._filtered_lines(start_ts, end_ts, store_name, category, product_search)
        return int(flt["qty"].sum()) if not flt.empty else 0

    def get_total_revenue(
        self,
        start_ts: datetime,
        end_ts: datetime,
        store_name: Optional[str] = None,
        category: Optional[str] = None,
        product_search: Optional[str] = None,
    ) -> float:
        """Get total revenue for the current filter set."""
        flt = self._filtered_lines(start_ts, end_ts, store_name, category, product_search)
        return float(flt["extended_price"].sum()) if not flt.empty else 0.0

    # ---------- Legacy methods for backward compatibility ----------

    def list_stores(self) -> pd.DataFrame:
        """Legacy method - use get_stores() instead."""
        cols = ["store_name", "city", "region"]
        out = self._tables.stores[cols].drop_duplicates().sort_values("store_name").reset_index(drop=True)
        return out

    def list_categories(self) -> pd.DataFrame:
        """Legacy method - use list_product_categories() instead."""
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
    ) -> dict:
        """Legacy method - use individual KPI methods instead."""
        return {
            "orders_distinct": self.get_orders_distinct_count(start_ts, end_ts, store_name, category, product_search),
            "lines": self.get_order_lines_count(start_ts, end_ts, store_name, category, product_search),
            "units": self.get_total_units(start_ts, end_ts, store_name, category, product_search),
            "revenue": self.get_total_revenue(start_ts, end_ts, store_name, category, product_search),
        }
