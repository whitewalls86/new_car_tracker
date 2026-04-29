"""Unit tests for processors/results_page_cards.py — v3 parser only (v1/v2 deprecated)."""
import json

from scraper.processors.results_page_cards import _digits_to_int, parse_cars_results_page_html_v3

LISTING_FIELDS = {
    "listing_id",
    "canonical_detail_url",
    "year",
    "make",
    "model",
    "trim",
    "stockType",
    "price",
    "msrp",
    "mileage",
    "vin",
    "fuelType",
    "bodyStyle",
    "financingType",
    "seller_zip",
    "seller_customerId",
    "page_number",
    "position_on_page",
    "trid",
    "isaContext",
}


def _make_v3_card(
    tag: str,
    listing_id: str = "abc-1111-2222-3333-444444444444",
    **overrides,
) -> str:
    """Build a minimal <spark-card> or <fuse-card> with data-vehicle-details JSON."""
    data = {
        "listingId": listing_id,
        "make": "Toyota",
        "model": "RAV4",
        "trim": "XLE",
        "year": 2025,
        "price": 35000,
        "msrp": 36000,
        "mileage": 0,
        "vin": "1HGCM82633A123456",
        "stockType": "new",
        "fuelType": "Gasoline",
        "bodyStyle": "SUV",
        "financingType": "cash",
        "trid": "abc-trid-123",
        "isaContext": "organic",
        "seller": {"zip": "77002", "customerId": "cust-99"},
        "metadata": {"page_number": 1, "position_on_page": 3},
    }
    data.update(overrides)
    escaped = json.dumps(data).replace("'", "&#39;")
    return f"<{tag} data-vehicle-details='{escaped}'></{tag}>"


class TestV3ParserSparkCard:
    def test_parses_spark_card(self):
        html = _make_v3_card("spark-card")
        listings, meta = parse_cars_results_page_html_v3(html)
        assert len(listings) == 1
        assert listings[0]["listing_id"] == "abc-1111-2222-3333-444444444444"
        assert listings[0]["make"] == "Toyota"

    def test_parses_fuse_card(self):
        html = _make_v3_card("fuse-card")
        listings, meta = parse_cars_results_page_html_v3(html)
        assert len(listings) == 1
        assert listings[0]["listing_id"] == "abc-1111-2222-3333-444444444444"

    def test_both_card_types_in_same_page(self):
        html = (
            _make_v3_card("spark-card", listing_id="aaaa-0000-0000-0000-000000000001")
            + _make_v3_card("fuse-card", listing_id="bbbb-0000-0000-0000-000000000002")
        )
        listings, meta = parse_cars_results_page_html_v3(html)
        assert len(listings) == 2
        ids = {listing["listing_id"] for listing in listings}
        assert "aaaa-0000-0000-0000-000000000001" in ids
        assert "bbbb-0000-0000-0000-000000000002" in ids

    def test_empty_page_returns_empty_list(self):
        listings, meta = parse_cars_results_page_html_v3("<html><body></body></html>")
        assert listings == []
        assert meta["cards_found"] == 0
        assert meta["listing_ids_extracted"] == 0

    def test_missing_listing_id_skipped(self):
        data = {"make": "Honda", "model": "CR-V"}  # no listingId
        html = f"<spark-card data-vehicle-details='{json.dumps(data)}'></spark-card>"
        listings, meta = parse_cars_results_page_html_v3(html)
        assert listings == []
        assert meta["cards_found"] == 1
        assert meta["listing_ids_extracted"] == 0

    def test_malformed_json_increments_failures(self):
        html = "<spark-card data-vehicle-details='not valid json'></spark-card>"
        listings, meta = parse_cars_results_page_html_v3(html)
        assert listings == []
        assert meta["json_failures"] == 1

    def test_malformed_and_valid_card(self):
        bad = "<spark-card data-vehicle-details='broken'></spark-card>"
        good = _make_v3_card("fuse-card", listing_id="good-0000-0000-0000-000000000001")
        listings, meta = parse_cars_results_page_html_v3(bad + good)
        assert len(listings) == 1
        assert meta["json_failures"] == 1


class TestV3FieldMapping:
    def test_canonical_url_built_from_listing_id(self):
        html = _make_v3_card("spark-card", listing_id="xyz-0000-0000-0000-000000000099")
        listings, _ = parse_cars_results_page_html_v3(html)
        assert listings[0]["canonical_detail_url"] == "https://www.cars.com/vehicledetail/xyz-0000-0000-0000-000000000099/"

    def test_seller_zip_extracted(self):
        html = _make_v3_card("spark-card", seller={"zip": "90210", "customerId": "c1"})
        listings, _ = parse_cars_results_page_html_v3(html)
        assert listings[0]["seller_zip"] == "90210"

    def test_seller_customer_id_extracted(self):
        html = _make_v3_card("spark-card", seller={"zip": "77002", "customerId": "cust-42"})
        listings, _ = parse_cars_results_page_html_v3(html)
        assert listings[0]["seller_customerId"] == "cust-42"

    def test_page_number_from_metadata(self):
        html = _make_v3_card("spark-card", metadata={"page_number": 4, "position_on_page": 7})
        listings, _ = parse_cars_results_page_html_v3(html)
        assert listings[0]["page_number"] == 4
        assert listings[0]["position_on_page"] == 7

    def test_isa_context_uppercased(self):
        html = _make_v3_card("spark-card", isaContext="sponsored")
        listings, _ = parse_cars_results_page_html_v3(html)
        assert listings[0]["isaContext"] == "SPONSORED"

    def test_isa_context_none_when_missing(self):
        data = {
            "listingId": "isa-0000-0000-0000-000000000001",
            "make": "Honda",
        }
        html = f"<spark-card data-vehicle-details='{json.dumps(data)}'></spark-card>"
        listings, _ = parse_cars_results_page_html_v3(html)
        assert listings[0]["isaContext"] is None

    def test_last_seen_price_alias_equals_price(self):
        html = _make_v3_card("spark-card", price=29999)
        listings, _ = parse_cars_results_page_html_v3(html)
        assert listings[0]["last_seen_price"] == listings[0]["price"] == 29999

    def test_year_coerced_to_int(self):
        html = _make_v3_card("spark-card", year=2024)
        listings, _ = parse_cars_results_page_html_v3(html)
        assert listings[0]["year"] == 2024
        assert isinstance(listings[0]["year"], int)

    def test_price_coerced_to_int_from_string(self):
        html = _make_v3_card("spark-card", price="$35,000")
        listings, _ = parse_cars_results_page_html_v3(html)
        assert listings[0]["price"] == 35000


class TestV3Meta:
    def test_parser_name(self):
        listings, meta = parse_cars_results_page_html_v3("<html></html>")
        assert meta["parser"] == "cars_results_page__listings_v3_spark_card"

    def test_cards_found_count(self):
        html = (
            _make_v3_card("spark-card", listing_id="a-0000-0000-0000-000000000001")
            + _make_v3_card("fuse-card", listing_id="b-0000-0000-0000-000000000002")
        )
        _, meta = parse_cars_results_page_html_v3(html)
        assert meta["cards_found"] == 2
        assert meta["listing_ids_extracted"] == 2

    def test_json_failures_zero_on_clean_page(self):
        html = _make_v3_card("spark-card")
        _, meta = parse_cars_results_page_html_v3(html)
        assert meta["json_failures"] == 0


class TestListingContract:
    """Assert that every expected listing field is present in output."""

    def test_all_listing_fields_present(self):
        html = _make_v3_card("spark-card")
        listings, _ = parse_cars_results_page_html_v3(html)
        assert len(listings) == 1
        missing = LISTING_FIELDS - listings[0].keys()
        assert missing == set(), f"Missing listing fields: {missing}"


# ---------------------------------------------------------------------------
# _digits_to_int
# ---------------------------------------------------------------------------
class TestDigitsToInt:
    def test_none_returns_none(self):
        assert _digits_to_int(None) is None

    def test_int_passthrough(self):
        assert _digits_to_int(42) == 42

    def test_float_truncated(self):
        assert _digits_to_int(12.9) == 12

    def test_string_with_digits(self):
        assert _digits_to_int("$12,345") == 12345

    def test_string_no_digits(self):
        assert _digits_to_int("N/A") is None
