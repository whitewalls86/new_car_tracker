"""
Pydantic models for search_configs CRUD.
"""
import re
from typing import List, Optional

from pydantic import BaseModel, field_validator

# Valid sort options for Cars.com
SORT_OPTIONS = [
    ("list_price", "Price (Low to High)"),
    ("listed_at_desc", "Newest Listed"),
    ("best_deal", "Best Deal"),
    ("best_match_desc", "Best Match"),
    ("mileage", "Mileage (Low to High)"),
    ("list_price_desc", "Price (High to Low)"),
    ("year_desc", "Year (Newest)"),
]

SORT_KEYS = [k for k, _ in SORT_OPTIONS]


class SearchConfigParams(BaseModel):
    makes: List[str]
    models: List[str]
    zip: str
    radius_miles: int = 150
    scopes: List[str] = ["local", "national"]
    max_listings: int = 2000
    max_safety_pages: int = 50
    sort_order: Optional[str] = "best_match_desc"
    sort_rotation: Optional[List[str]] = None

    @field_validator("zip")
    @classmethod
    def validate_zip(cls, v):
        if not re.match(r"^\d{5}$", v.strip()):
            raise ValueError("Zip code must be exactly 5 digits")
        return v.strip()

    @field_validator("makes", "models")
    @classmethod
    def validate_non_empty(cls, v):
        if not v or all(not item.strip() for item in v):
            raise ValueError("At least one value is required")
        return [item.strip() for item in v if item.strip()]

    @field_validator("scopes")
    @classmethod
    def validate_scopes(cls, v):
        valid = {"local", "national"}
        for s in v:
            if s not in valid:
                raise ValueError(f"Invalid scope: {s}. Must be 'local' or 'national'")
        if not v or all(not item.strip() for item in v):
            raise ValueError("At least one scope is required")
        return v

    @field_validator("radius_miles")
    @classmethod
    def validate_radius(cls, v):
        if v < 1 or v > 5000:
            raise ValueError("Radius must be between 1 and 5000 miles")
        return v

    @field_validator("max_listings")
    @classmethod
    def validate_max_listings(cls, v):
        if v < 1 or v > 10000:
            raise ValueError("Max listings must be between 1 and 10,000")
        return v
