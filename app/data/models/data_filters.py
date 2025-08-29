from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class CustomerFilters(BaseModel):
    """Filters for the customer data."""
    start_ts: Optional[datetime] = Field(default=None, description="Start timestamp for signup date range")
    end_ts: Optional[datetime] = Field(default=None, description="End timestamp for signup date range")
    segment: Optional[str | list[str]] = Field(default=None, description="Segment filter (single segment or list of segments)")
    home_region: Optional[str | list[str]] = Field(default=None, description="Home region filter (single region or list of regions)")
    home_city: Optional[str | list[str]] = Field(default=None, description="Home city filter (single city or list of cities)")


class OrderFilters(BaseModel):
    """Filters for the order data."""
    start_ts: Optional[datetime] = Field(default=None, description="Start timestamp for order date range")
    end_ts: Optional[datetime] = Field(default=None, description="End timestamp for order date range")
    store_id: Optional[int | list[int]] = Field(default=None, description="Store ID filter (single store or list of stores)")
    customer_id: Optional[int | list[int]] = Field(default=None, description="Customer ID filter (single customer or list of customers)")
    payment_type: Optional[str | list[str]] = Field(default=None, description="Payment type filter (single type or list of types)")
    discount_pct_min: Optional[float] = Field(default=0.0, description="Minimum discount percentage")
    discount_pct_max: Optional[float] = Field(default=1.0, description="Maximum discount percentage")


class OrderItemsFilters(BaseModel):
    """Filters for the order items data."""
    start_ts: Optional[datetime] = Field(default=None, description="Start timestamp for order date range")
    end_ts: Optional[datetime] = Field(default=None, description="End timestamp for order date range")
    order_id: Optional[int | list[int]] = Field(default=None, description="Order ID filter (single int or list of ints)")
    product_id: Optional[int | list[int]] = Field(default=None, description="Product ID filter (single int or list of ints)")
    qty_min: Optional[int] = Field(default=0, description="Minimum quantity")
    qty_max: Optional[int] = Field(default=1000, description="Maximum quantity")
    unit_price_min: Optional[float] = Field(default=0.0, description="Minimum unit price")
    unit_price_max: Optional[float] = Field(default=1000.0, description="Maximum unit price")


class ProductFilters(BaseModel):
    """Filters for the product data."""
    category: Optional[str | list[str]] = Field(default=None, description="Category filter (single category or list of categories)")
    brand: Optional[str | list[str]] = Field(default=None, description="Brand filter (single brand or list of brands)")
    price_min: Optional[float] = Field(default=0.0, description="Minimum price")
    price_max: Optional[float] = Field(default=1000.0, description="Maximum price")


class StoreFilters(BaseModel):
    """Filters for the store data."""
    region: Optional[str | list[str]] = Field(default=None, description="Region filter (single region or list of regions)")
    city: Optional[str | list[str]] = Field(default=None, description="City filter (single city or list of cities)")
    store_id: Optional[int | list[int]] = Field(default=None, description="Store ID filter (single store or list of stores)")


class InventoryFilters(BaseModel):
    """Filters for the inventory snapshot data."""
    start_ts: Optional[datetime] = Field(default=None, description="Start timestamp for inventory snapshot range")
    end_ts: Optional[datetime] = Field(default=None, description="End timestamp for inventory snapshot range")
    store_id: Optional[int | list[int]] = Field(default=None, description="Store ID filter (single store or list of stores)")
    product_id: Optional[int | list[int]] = Field(default=None, description="Product ID filter (single product or list of products)")
    on_hand_min: Optional[int] = Field(default=0, description="Minimum on-hand quantity")
    on_hand_max: Optional[int] = Field(default=1000, description="Maximum on-hand quantity")
    on_order_min: Optional[int] = Field(default=0, description="Minimum on-order quantity")
    on_order_max: Optional[int] = Field(default=1000, description="Maximum on-order quantity")
    below_safety: Optional[bool] = Field(default=None, description="Filter for items below safety stock")


class PromotionFilters(BaseModel):
    """Filters for the promotion data."""
    start_date: Optional[datetime] = Field(default=None, description="Start date for promotion range")
    end_date: Optional[datetime] = Field(default=None, description="End date for promotion range")
    product_id: Optional[int | list[int]] = Field(default=None, description="Product ID filter (single product or list of products)")
    promo_type: Optional[str | list[str]] = Field(default=None, description="Promotion type filter (single type or list of types)")
    discount_pct_min: Optional[float] = Field(default=0.0, description="Minimum discount percentage")
    discount_pct_max: Optional[float] = Field(default=1.0, description="Maximum discount percentage")
