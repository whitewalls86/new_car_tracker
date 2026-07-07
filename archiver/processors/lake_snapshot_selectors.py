"""
Selector registry for CI lake snapshot exports (Plan 120).

Each Selector names a dbt/PySpark branch or guard the snapshot must exercise,
the SQL used to find candidate entities in production, and the minimum
representation required in the snapshot before it can be published.

SQL is placeholder/TODO in this first pass — the exporter does not yet run
selectors against real Parquet. The registry shape and uniqueness constraint
are the load-bearing parts of this slice.
"""
from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class Selector:
    name: str
    min_entities: int
    entity_key: str
    sql: str
    description: str


# name -> (min_entities, entity_key, description)
_SELECTOR_SPECS: List[tuple] = [
    ("stable_state_run", 25, "vin",
     "VINs with multiple detail observations where the business-state "
     "fingerprint is unchanged (gaps-and-islands collapse)."),
    ("state_change_run", 25, "vin",
     "VINs with multiple distinct business-state fingerprints (price, "
     "mileage, dealer, or listing_state changes)."),
    ("relisted_vin", 10, "vin",
     "VINs with more than one listing_id or remap events with "
     "previous_listing_id."),
    ("active_to_unlisted", 10, "listing_id",
     "VIN/listing with an active detail row followed by an unlisted/delete "
     "event."),
    ("price_drop", 25, "listing_id",
     "Consecutive price event where price < prev_price."),
    ("price_increase", 25, "listing_id",
     "Consecutive price event where price > prev_price."),
    ("price_changed_7d", 25, "listing_id",
     "Price change within seven days of the source window end."),
    ("price_changed_30d_only", 25, "listing_id",
     "Price change within thirty days but outside the seven-day window."),
    ("no_price_history", 25, "vin",
     "Observation VIN lacking any matching positive price events."),
    ("detail_beats_srp", 25, "vin",
     "VIN with both detail and SRP observations where detail should win "
     "latest-observation source priority."),
    ("srp_fallback", 25, "vin",
     "VIN with usable SRP attributes and missing/incomplete detail "
     "attributes."),
    ("carousel_only_or_low_priority", 25, "vin",
     "VIN/listing represented only by carousel observations, or where "
     "carousel loses priority to richer sources."),
    ("invalid_or_null_vin", 25, "artifact_id",
     "Rows with null or invalid VINs that must not become vin17."),
    ("benchmark_dense_make_model", 3, "make_model",
     "Make/model groups with enough rows for stable percentile/median "
     "benchmarks."),
    ("benchmark_sparse_make_model", 3, "make_model",
     "Make/model groups with only a few rows, which must not disappear "
     "silently."),
    ("cooldown_blocked", 10, "listing_id",
     "First 403 blocked cooldown event."),
    ("cooldown_incremented", 10, "listing_id",
     "Repeated 403 blocked attempt event."),
    ("cooldown_bucket_3_4", 1, "listing_id",
     "Cooldown attempts between 3 and 4 (bucket boundary)."),
    ("cooldown_bucket_5_10", 1, "listing_id",
     "Cooldown attempts between 5 and 10 (bucket boundary)."),
    ("cooldown_bucket_11_plus", 1, "listing_id",
     "Cooldown attempts >= 11 (high-attempt bucket)."),
    ("fresh_recent_listing", 25, "listing_id",
     "Young/current active listing."),
    ("stale_listing", 25, "listing_id",
     "Old listing, or listing with stale SRP/detail recency."),
]


def _placeholder_sql(name: str) -> str:
    return f"-- TODO: implement selector SQL for '{name}'\nSELECT NULL WHERE FALSE"


def build_selector_registry() -> Dict[str, Selector]:
    """Build the selector registry, keyed by selector name.

    Raises ValueError if any selector name is duplicated in _SELECTOR_SPECS.
    """
    registry: Dict[str, Selector] = {}
    for name, min_entities, entity_key, description in _SELECTOR_SPECS:
        if name in registry:
            raise ValueError(f"Duplicate selector name: {name}")
        registry[name] = Selector(
            name=name,
            min_entities=min_entities,
            entity_key=entity_key,
            sql=_placeholder_sql(name),
            description=description,
        )
    return registry
