from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class InventoryResponse(BaseModel):
    """Response model for inventory snapshot data."""
    snapshot_ts: datetime = Field(description="Inventory snapshot timestamp")
    store_id: int = Field(description="Store identifier")
    product_id: int = Field(description="Product identifier")
    on_hand: int = Field(description="Current on-hand quantity")
    on_order: int = Field(description="Quantity on order")
    safety_stock: int = Field(description="Safety stock level")
    reorder_qty: int = Field(description="Reorder quantity")
