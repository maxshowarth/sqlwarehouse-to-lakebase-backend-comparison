from __future__ import annotations

from pydantic import BaseModel, Field


class ProductResponse(BaseModel):
    """Response model for product data."""
    product_id: int = Field(description="Unique product identifier")
    sku: str = Field(description="Stock keeping unit code")
    name: str = Field(description="Product name")
    category: str = Field(description="Product category")
    brand: str = Field(description="Product brand")
    base_price: float = Field(description="Base product price")
