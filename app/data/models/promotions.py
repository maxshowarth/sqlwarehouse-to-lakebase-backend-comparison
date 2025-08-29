from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field


class PromotionResponse(BaseModel):
    """Response model for promotion data."""
    promo_id: str = Field(description="Unique promotion identifier")
    product_id: int = Field(description="Product being promoted")
    start_date: date = Field(description="Promotion start date")
    end_date: date = Field(description="Promotion end date")
    promo_type: Literal["BOGO-lite", "PercentOff", "PriceDrop"] = Field(description="Type of promotion")
    discount_pct: float = Field(description="Discount percentage")
