import pytest

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Target VINs (500s group — see conftest._seed_all for full data):
#
#   VA0ATTRDETAIL0001 — SRP (2h ago, make='Attr-Make-SRP', msrp=30000 N/A on SRP)
#                       detail (1h ago, make='Attr-Make-DET', msrp=45000)
#                       Detail is fresher AND has lower source_priority (0 vs 1) → detail wins.
#
#   VB0ATTRSRPONLY001 — SRP only (1h ago, make='Attr-Make-SRP')
#                       No detail observation seeded → SRP is the only source.
#
#   VC0ATTRSRPFRESHER — SRP (1h ago, make='Attr-Make-SRP')
#                       detail (2h ago, make='Attr-Make-DET', msrp=45000)
#                       SRP is fresher, but int_vehicle_attributes ranks by source_priority
#                       first (detail=0 beats SRP=1) before recency → detail always wins.
# ---------------------------------------------------------------------------

_VIN_DETAIL_FRESHER  = "VA0ATTRDETAIL0001"
_VIN_SRP_ONLY        = "VB0ATTRSRPONLY001"
_VIN_SRP_FRESHER     = "VC0ATTRSRPFRESHER"


def test_detail_wins_when_fresher(analytics_ci_cur):
    """VA0ATTRDETAIL0001: detail (1h) is fresher than SRP (2h) — detail attributes selected."""
    analytics_ci_cur.execute("""
        SELECT attributes_source, make, msrp
        FROM int_vehicle_attributes
        WHERE vin = %s
    """, (_VIN_DETAIL_FRESHER,))
    row = analytics_ci_cur.fetchone()
    assert row is not None, f"{_VIN_DETAIL_FRESHER} not found in int_vehicle_attributes"
    assert row["attributes_source"] == "detail"
    assert row["make"] == "Attr-Make-DET"
    assert row["msrp"] == 45000


def test_srp_only_attributes_source(analytics_ci_cur):
    """VB0ATTRSRPONLY001: no detail observation — attributes_source must be 'srp'."""
    analytics_ci_cur.execute("""
        SELECT attributes_source, make
        FROM int_vehicle_attributes
        WHERE vin = %s
    """, (_VIN_SRP_ONLY,))
    row = analytics_ci_cur.fetchone()
    assert row is not None, f"{_VIN_SRP_ONLY} not found in int_vehicle_attributes"
    assert row["attributes_source"] == "srp"
    assert row["make"] == "Attr-Make-SRP"


def test_detail_wins_even_when_srp_fresher(analytics_ci_cur):
    """VC0ATTRSRPFRESHER: SRP (1h) is fresher than detail (2h), but detail source_priority always wins."""
    analytics_ci_cur.execute("""
        SELECT attributes_source, make, msrp
        FROM int_vehicle_attributes
        WHERE vin = %s
    """, (_VIN_SRP_FRESHER,))
    row = analytics_ci_cur.fetchone()
    assert row is not None, f"{_VIN_SRP_FRESHER} not found in int_vehicle_attributes"
    assert row["attributes_source"] == "detail"
    assert row["make"] == "Attr-Make-DET"
    assert row["msrp"] == 45000
