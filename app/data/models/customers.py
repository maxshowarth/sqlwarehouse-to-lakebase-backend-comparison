from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class CustomerResponse(BaseModel):
    """Response model for customer data."""
    customer_id: int = Field(description="Unique customer identifier")
    segment: Literal["loyal", "casual", "bargain", "premium"] = Field(description="Customer loyalty segment")
    signup_ts: datetime = Field(description="Customer signup timestamp")
    home_region: Literal["West", "Central", "East"] = Field(description="Customer's home region")
    home_city: str = Field(description="Customer's home city")
