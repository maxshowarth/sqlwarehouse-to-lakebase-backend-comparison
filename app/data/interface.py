# src/data_access/interface.py
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional, Protocol, Tuple, Union, List

import pandas as pd
from pydantic import BaseModel

from .models import (
    # Filter classes
    CustomerFilters,
    OrderFilters,
    OrderItemsFilters,
    ProductFilters,
    StoreFilters,
    InventoryFilters,
    PromotionFilters,
    # Response models
    CustomerResponse,
    ProductResponse,
    StoreResponse,
    OrderResponse,
    OrderItemResponse,
    InventoryResponse,
    PromotionResponse,
    # List response models
    StringList,
    IntList,
    DateTimeList,
    DateBounds,
)


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

    # Queries to populate dropdowns and filters

    def get_date_bounds(self) -> Union[Tuple[datetime, datetime], DateBounds]:
        """Get the start and end date for the data."""
        ...

    def list_store_opening_date_bounds(self) -> Union[Tuple[datetime, datetime], DateBounds]:
        """Get the start and end date for the store opening dates."""
        ...

    def list_store_cities(self) -> Union[pd.DataFrame, StringList]:
        """List all store cities."""
        ...

    def list_store_regions(self) -> Union[pd.DataFrame, StringList]:
        """List all store regions."""
        ...

    def list_product_names(self) -> Union[pd.DataFrame, List[ProductResponse]]:
        """List all product names."""
        ...

    def list_product_categories(self) -> Union[pd.DataFrame, StringList]:
        """List all product categories."""
        ...

    def list_product_brands(self) -> Union[pd.DataFrame, StringList]:
        """List all product brands."""
        ...

    def list_customer_segments(self) -> Union[pd.DataFrame, StringList]:
        """List all customer segments."""
        ...

    def list_customer_home_regions(self) -> Union[pd.DataFrame, StringList]:
        """List all customer home regions."""
        ...

    def list_customer_home_cities(self) -> Union[pd.DataFrame, StringList]:
        """List all customer home cities."""
        ...

    def list_promo_types(self) -> Union[pd.DataFrame, StringList]:
        """List all promotion types."""
        ...

    def list_promo_date_bounds(self) -> Union[Tuple[datetime, datetime], DateBounds]:
        """Get the start and end date for the promotion dates."""
        ...

    def list_payment_types(self) -> Union[pd.DataFrame, StringList]:
        """List all payment types."""
        ...

    def list_order_date_bounds(self) -> Union[Tuple[datetime, datetime], DateBounds]:
        """Get the start and end date for the order dates."""
        ...

    def list_order_payment_types(self) -> Union[pd.DataFrame, StringList]:
        """List all order payment types."""
        ...

    # Customer data queries
    def get_customers(self, filters: CustomerFilters) -> Union[pd.DataFrame, List[CustomerResponse]]:
        """Get customers based on filters."""
        ...

    # Order data queries
    def get_orders(
        self,
        filters: OrderFilters,
        limit: int = 2000,
        order_by: Literal["order_ts_desc", "order_ts_asc"] = "order_ts_desc",
    ) -> Union[pd.DataFrame, List[OrderResponse]]:
        """Get the orders for a given date range, store, category, and product."""
        ...

    # Order items data queries
    def get_order_items(self, filters: OrderItemsFilters) -> Union[pd.DataFrame, List[OrderItemResponse]]:
        """Get order items based on filters."""
        ...

    # Product data queries
    def get_products(self, filters: ProductFilters) -> Union[pd.DataFrame, List[ProductResponse]]:
        """Get products based on filters."""
        ...

    # Store data queries
    def get_stores(self, filters: StoreFilters) -> Union[pd.DataFrame, List[StoreResponse]]:
        """Get stores based on filters."""
        ...

    # Inventory data queries
    def get_inventory(self, filters: InventoryFilters) -> Union[pd.DataFrame, List[InventoryResponse]]:
        """Get inventory snapshots based on filters."""
        ...

    # Promotion data queries
    def get_promotions(self, filters: PromotionFilters) -> Union[pd.DataFrame, List[PromotionResponse]]:
        """Get promotions based on filters."""
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
    ) -> Union[pd.DataFrame, List[ProductResponse]]:
        """Get product counts with optional slicing."""
        ...
