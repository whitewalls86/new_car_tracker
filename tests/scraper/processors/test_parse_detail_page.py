"""Unit tests for processors/parse_detail_page.py"""
import json

from scraper.processors.parse_detail_page import (
    _parse_carousel_cards,
    _parse_dealer_card,
    parse_cars_detail_page_html_v1,
)

# Fields that n8n reads from `primary` (from Parse Detail Pages.json workflow)
N8N_PRIMARY_FIELDS = {
    "listing_id",
    "vin",
    "listing_state",
    "make",
    "model",
    "trim",
    "year",
    "price",
    "mileage",
    "msrp",
    "stock_type",
    "fuel_type",
    "body_style",
    "dealer_name",
    "dealer_zip",
    "dealer_street",
    "dealer_city",
    "dealer_state",
    "dealer_phone",
    "dealer_website",
    "dealer_cars_com_url",
    "dealer_rating",
    "customer_id",
}


def _activity_script(data: dict) -> str:
    return f'<script id="initial-activity-data" type="application/json">{json.dumps(data)}</script>'


def _dealer_card(name="Best Auto", address="123 Main St, Houston, TX 77002", rating=None):
    rating_html = f'<fuse-rating rating="{rating}"></fuse-rating>' if rating else ""
    return f"""
    <div class="dealer-card">
      <h3>{name}</h3>
      <div class="map-link"><a href="#">{address}</a></div>
      {rating_html}
    </div>"""


ACTIVE_DETAIL_HTML = (
    _activity_script({
        "listing_id": "11111111-aaaa-bbbb-cccc-000000000001",
        "vin": "1HGCM82633A123456",
        "make": "Honda",
        "model": "Accord",
        "trim": "Sport",
        "year": 2024,
        "price": 28000,
        "mileage": 0,
        "msrp": 29500,
        "stock_type": "new",
        "fuel_type": "Gasoline",
        "bodystyle": "Sedan",
        "customer_id": "cust-77",
        "seller_id": "seller-42",
    })
    + _dealer_card(rating=4.5)
)

UNLISTED_SPARK_HTML = """
<spark-notification class="unlisted-notification" title="No longer listed">
  This vehicle is no longer available
</spark-notification>
"""

UNLISTED_TEXT_HTML = """
<html><body><p>This vehicle is no longer available for purchase.</p></body></html>
"""


class TestReturnType:
    def test_returns_three_tuple(self):
        result = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_primary_is_dict(self):
        primary, _, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert isinstance(primary, dict)

    def test_carousel_is_list(self):
        _, carousel, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert isinstance(carousel, list)

    def test_meta_is_dict(self):
        _, _, meta = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert isinstance(meta, dict)


class TestUnlistedDetection:
    def test_detect_unlisted_via_spark_notification(self):
        primary, _, _ = parse_cars_detail_page_html_v1(UNLISTED_SPARK_HTML)
        assert primary["listing_state"] == "unlisted"
        assert primary["unlisted_title"] == "No longer listed"

    def test_detect_unlisted_via_fallback_text(self):
        primary, _, _ = parse_cars_detail_page_html_v1(UNLISTED_TEXT_HTML)
        assert primary["listing_state"] == "unlisted"
        assert primary["unlisted_title"] is None

    def test_active_listing_state(self):
        primary, _, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert primary["listing_state"] == "active"

    def test_unlisted_message_present_for_spark_notification(self):
        primary, _, _ = parse_cars_detail_page_html_v1(UNLISTED_SPARK_HTML)
        assert primary["unlisted_message"] is not None


class TestPrimaryJsonExtraction:
    def test_parses_listing_id_from_activity_data(self):
        primary, _, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert primary["listing_id"] == "11111111-aaaa-bbbb-cccc-000000000001"

    def test_parses_vin(self):
        primary, _, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert primary["vin"] == "1HGCM82633A123456"

    def test_parses_make_model_trim(self):
        primary, _, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert primary["make"] == "Honda"
        assert primary["model"] == "Accord"
        assert primary["trim"] == "Sport"

    def test_parses_price_and_mileage(self):
        primary, _, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert primary["price"] == 28000
        assert primary["mileage"] == 0

    def test_listing_id_source_is_activity_data(self):
        primary, _, meta = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert primary["listing_id_source"] == "initial-activity-data"
        assert meta["listing_id_source"] == "initial-activity-data"

    def test_listing_id_falls_back_to_url(self):
        """No activity data script — listing_id should come from URL."""
        html = "<html><body><p>simple page</p></body></html>"
        url = "https://www.cars.com/vehicledetail/99999999-0000-0000-0000-000000000099/"
        primary, _, _ = parse_cars_detail_page_html_v1(html, url=url)
        assert primary["listing_id"] == "99999999-0000-0000-0000-000000000099"
        assert primary["listing_id_source"] == "url"

    def test_empty_html_all_fields_none(self):
        primary, carousel, _ = parse_cars_detail_page_html_v1("<html></html>")
        assert primary["listing_id"] is None
        assert primary["vin"] is None
        assert carousel == []


class TestDealerCardParsing:
    def test_dealer_name_from_h3(self):
        primary, _, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert primary["dealer_name"] == "Best Auto"

    def test_dealer_address_all_four_components(self):
        primary, _, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert primary["dealer_street"] == "123 Main St"
        assert primary["dealer_city"] == "Houston"
        assert primary["dealer_state"] == "TX"
        assert primary["dealer_zip"] == "77002"

    def test_dealer_address_missing_gracefully(self):
        html = (
            _activity_script({"listing_id": "a"})
            + "<div class='dealer-card'><h3>No Address</h3></div>"
        )
        primary, _, _ = parse_cars_detail_page_html_v1(html)
        assert primary.get("dealer_street") is None

    def test_dealer_rating_parsed_as_float(self):
        primary, _, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert primary["dealer_rating"] == 4.5
        assert isinstance(primary["dealer_rating"], float)

    def test_dealer_rating_malformed_no_crash(self):
        html = (
            _activity_script({"listing_id": "a"})
            + '<div class="dealer-card"><fuse-rating rating="bad"></fuse-rating></div>'
        )
        primary, _, _ = parse_cars_detail_page_html_v1(html)
        # Should not raise; dealer_rating simply absent
        assert primary.get("dealer_rating") is None

    def test_dealer_website_strips_utm(self):
        html = (
            _activity_script({"listing_id": "a"})
            + '<div class="dealer-card"><div class="website"><a href="https://dealer.com?utm_source=cars">site</a></div></div>'
        )
        primary, _, _ = parse_cars_detail_page_html_v1(html)
        assert primary["dealer_website"] == "https://dealer.com"

    def test_dealer_cars_com_url_extracted(self):
        html = (
            _activity_script({"listing_id": "a"})
            + '<div class="dealer-card"><a href="/dealers/best-auto-123/">view</a></div>'
        )
        primary, _, _ = parse_cars_detail_page_html_v1(html)
        assert primary["dealer_cars_com_url"] == "/dealers/best-auto-123/"


class TestCarouselParsing:
    def test_carousel_found_flag_true(self):
        html = (
            ACTIVE_DETAIL_HTML
            + '<div class="listings-carousel"></div>'
        )
        _, _, meta = parse_cars_detail_page_html_v1(html)
        assert meta["carousel_found"] is True

    def test_carousel_no_container_flag_false(self):
        _, carousel, meta = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert meta["carousel_found"] is False
        assert carousel == []

    def test_carousel_fuse_card_parsed(self):
        carousel_html = """
        <div class="listings-carousel">
          <fuse-card>
            <fuse-save data-listing-id="cc-0000-0000-0000-000000000001"></fuse-save>
            <span class="price">$25,000</span>
            <span class="body">New 2025 Toyota RAV4 XLE</span>
            <span slot="footer">10 mi</span>
          </fuse-card>
        </div>"""
        _, carousel, meta = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML + carousel_html)
        assert len(carousel) == 1
        assert carousel[0]["listing_id"] == "cc-0000-0000-0000-000000000001"
        assert carousel[0]["price"] == 25000
        assert carousel[0]["mileage"] == 10

    def test_carousel_spark_card_parsed(self):
        carousel_html = """
        <div class="listings-carousel">
          <spark-card>
            <spark-save data-listing-id="sc-0000-0000-0000-000000000002"></spark-save>
            <span class="price">$31,000</span>
            <span class="body">Used 2023 Honda CR-V EX</span>
            <span slot="footer">55 mi</span>
          </spark-card>
        </div>"""
        _, carousel, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML + carousel_html)
        assert len(carousel) == 1
        assert carousel[0]["listing_id"] == "sc-0000-0000-0000-000000000002"

    def test_carousel_body_condition_year_extracted(self):
        carousel_html = """
        <div class="listings-carousel">
          <fuse-card>
            <fuse-save data-listing-id="cond-0000-0000-0000-000000000001"></fuse-save>
            <span class="body">New 2026 Ford Bronco Sport</span>
            <span slot="footer">0 mi</span>
          </fuse-card>
        </div>"""
        _, carousel, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML + carousel_html)
        assert carousel[0]["condition"] == "New"
        assert carousel[0]["year"] == 2026

    def test_carousel_missing_listing_id_counted(self):
        carousel_html = """
        <div class="listings-carousel">
          <fuse-card>
            <span class="price">$20,000</span>
          </fuse-card>
        </div>"""
        _, carousel, meta = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML + carousel_html)
        assert carousel == []
        assert meta["missing_listing_id"] == 1


class TestMetaKeys:
    def test_meta_has_parser_key(self):
        _, _, meta = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert meta["parser"] == "cars_detail_page__v1"

    def test_meta_has_html_len(self):
        _, _, meta = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert isinstance(meta["html_len"], int)
        assert meta["html_len"] > 0

    def test_meta_carousel_found_key_present(self):
        _, _, meta = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        assert "carousel_found" in meta


class TestN8nContract:
    def test_all_n8n_primary_fields_present(self):
        primary, _, _ = parse_cars_detail_page_html_v1(ACTIVE_DETAIL_HTML)
        missing = N8N_PRIMARY_FIELDS - primary.keys()
        assert missing == set(), f"Missing n8n-consumed primary fields: {missing}"


# ---------------------------------------------------------------------------
# _parse_dealer_card — seller JSON fallback
# ---------------------------------------------------------------------------
class TestDealerCardSellerJson:
    def test_malformed_seller_json_does_not_crash(self):
        """When the seller regex matches but the JSON is invalid, gracefully skip."""
        from bs4 import BeautifulSoup
        html = """
        <div class="dealer-card"><h3>Test Dealer</h3></div>
        <script>"seller": {not valid json at all}</script>
        """
        soup = BeautifulSoup(html, "lxml")
        info = _parse_dealer_card(soup)
        assert info["dealer_card_name"] == "Test Dealer"
        assert "dealer_phone" not in info

    def test_valid_seller_json_extracts_phone(self):
        from bs4 import BeautifulSoup
        html = """
        <div class="dealer-card"><h3>Good Dealer</h3></div>
        <script>"seller": {"phoneNumber": "555-1234", "zipcode": "90210"}</script>
        """
        soup = BeautifulSoup(html, "lxml")
        info = _parse_dealer_card(soup)
        assert info["dealer_phone"] == "555-1234"
        assert info["dealer_zip_parsed"] == "90210"


# ---------------------------------------------------------------------------
# _parse_carousel_cards — listing_id from href fallback
# ---------------------------------------------------------------------------
class TestCarouselListingIdFallback:
    def test_listing_id_from_href_when_no_data_attribute(self):
        """When fuse-save has no data-listing-id, extract UUID from href."""
        from bs4 import BeautifulSoup
        uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        html = f"""
        <div class="listings-carousel">
          <fuse-card-carousel>
            <fuse-card>
              <fuse-save></fuse-save>
              <a href="/vehicledetail/{uuid}/">Details</a>
              <span class="price">$25,000</span>
              <span class="body">LE</span>
              <span slot="footer">15,000 mi.</span>
            </fuse-card>
          </fuse-card-carousel>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        cards, meta = _parse_carousel_cards(soup)
        assert len(cards) == 1
        assert cards[0]["listing_id"] == uuid
