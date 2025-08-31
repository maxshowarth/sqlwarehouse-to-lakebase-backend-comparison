from __future__ import annotations

from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


class StringList(BaseModel):
    """Generic container for lists of unique string values."""
    values: List[str] = Field(description="List of unique string values")


class IntList(BaseModel):
    """Generic container for lists of unique integer values."""
    values: List[int] = Field(description="List of unique integer values")


class DateTimeList(BaseModel):
    """Generic container for lists of unique datetime values."""
    values: List[datetime] = Field(description="List of unique datetime values")


class DateBounds(BaseModel):
    """Response model for date bounds data."""
    start_ts: datetime = Field(description="Start timestamp")
    end_ts: datetime = Field(description="End timestamp")
