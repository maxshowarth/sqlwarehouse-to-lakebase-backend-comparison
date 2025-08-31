from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class OrderResponse(BaseModel):
    """Response model for order data."""
    order_id: str = Field(description="Unique order identifier")
    store_id: int = Field(description="Store where order was placed")
    customer_id: int = Field(description="Customer who placed the order")
    order_ts: datetime = Field(description="Order timestamp")
    payment_type: Literal["card", "cash", "mobile"] = Field(description="Payment method used")
    discount_pct: float = Field(description="Discount percentage applied")
