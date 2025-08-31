from __future__ import annotations

from pydantic import BaseModel, Field


class OrderItemResponse(BaseModel):
    """Response model for order item data."""
    order_id: str = Field(description="Order identifier this item belongs to")
    line_number: int = Field(description="Line number within the order")
    product_id: int = Field(description="Product identifier")
    qty: int = Field(description="Quantity ordered")
    unit_price: float = Field(description="Unit price at time of order")
    extended_price: float = Field(description="Total price for this line (qty * unit_price)")
