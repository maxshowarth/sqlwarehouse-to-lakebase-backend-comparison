from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field


class StoreResponse(BaseModel):
    """Response model for store data."""
    store_id: int = Field(description="Unique store identifier")
    name: str = Field(description="Store name")
    region: Literal["West", "Central", "East"] = Field(description="Store region")
    city: str = Field(description="Store city")
    latitude: float = Field(description="Store latitude coordinate")
    longitude: float = Field(description="Store longitude coordinate")
    opened_date: date = Field(description="Store opening date")
