"""Unit tests for processing/html_sections.py (Plan 114, Stages 0-2).

Stage 2 is the gate: extract -> reconstruct -> parse -> compare, on every
checked-in fixture, before any MinIO work happens.

Sectioning is lossless by design. The "why we never drop a section" tests near
the bottom are deliberately kept even though nothing in the module can drop a
section any more -- they pin the two whole-document parser scans that make
pruning unsafe, so that re-adding pruning fails loudly rather than silently.
"""
import gzip
import json
import re
from pathlib import Path

import pytest

from processing.html_sections import (
    PARSER_CRITICAL_SECTION_NAMES,
    PARSER_VERSION,
    Section,
    build_manifest,
    extract_sections,
    parse_outputs_equivalent,
    reconstruct,
    section_sha256,
    serialize_manifest,
)
from processing.processors.parse_detail_page import (
    _parse_dealer_card,
    parse_cars_detail_page_html_v1,
)

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "html"

REAL_FIXTURES = ["real_detail_crv", "real_detail_2"]
ALL_FIXTURES = REAL_FIXTURES + ["challenge_just_a_moment"]

# The 12 id'd <script> blocks both real fixtures carry, as section names.
EXPECTED_SCRIPT_SECTIONS = {
    "script_datadog_config",
    "script_datadog_global_context",
    "script_datadog_logs_config",
    "script_payment_calculator_initial_state",
    "vehicle_activity_json",
    "als_json",
    "script_event_stream_config",
    "script_third_party_flags",
    "script_carsweb",
    "script_carsweb_vehicledetailcontroller",
    "vehicle_controller_json",
    "script_graphql_config",
}

# The sections a rejected earlier draft would have discarded as "volatile".
# Kept only so the tests below can demonstrate what discarding them costs.
_REJECTED_DROP_CANDIDATES = frozenset(
    {
        "script_datadog_config",
        "script_datadog_global_context",
        "script_datadog_logs_config",
        "als_json",
        "script_event_stream_config",
        "script_third_party_flags",
        "script_graphql_config",
    }
)

_SELLER_BLOB_RE = re.compile(r'"seller"\s*:\s*\{([^}]+)\}')


def _load_html_fixture(name: str) -> str:
    """Load a gzip-compressed captured HTML artifact from tests/fixtures/html."""
    return gzip.decompress((_FIXTURE_DIR / f"{name}.html.gz").read_bytes()).decode(
        "utf-8", errors="replace"
    )


def _section_by_name(sections, name: str) -> Section:
    matches = [section for section in sections if section.name == name]
    assert len(matches) == 1, f"expected exactly one {name!r}, got {len(matches)}"
    return matches[0]


def _replace_section_json(html: str, section_name: str, mutate) -> str:
    """Rebuild ``html`` with one script section's decoded JSON body mutated."""
    parts = []
    replaced = False
    for section in extract_sections(html):
        if section.name != section_name:
            parts.append(section.text)
            continue
        body_start = section.text.find(">") + 1
        body_end = section.text.rfind("</script")
        payload = json.loads(section.text[body_start:body_end])
        mutate(payload)
        parts.append(section.text[:body_start] + json.dumps(payload) + section.text[body_end:])
        replaced = True
    assert replaced, f"section {section_name!r} not found"
    return "".join(parts)


def _drop_sections(sections, names) -> str:
    """Reconstruct while discarding named sections. Only tests may do this."""
    return "".join(s.text for s in sections if s.name not in names)


# --------------------------------------------------------------------------
# Slicing invariants
# --------------------------------------------------------------------------


@pytest.mark.parametrize("fixture", ALL_FIXTURES)
def test_sections_reconstruct_byte_identically(fixture):
    """The core guarantee: reconstruction is exact, by construction."""
    html = _load_html_fixture(fixture)
    assert reconstruct(extract_sections(html)) == html


@pytest.mark.parametrize("fixture", ALL_FIXTURES)
def test_sections_are_contiguous_and_cover_the_document(fixture):
    html = _load_html_fixture(fixture)
    sections = extract_sections(html)

    assert sections[0].start == 0
    assert sections[-1].end == len(html)
    for earlier, later in zip(sections, sections[1:]):
        assert earlier.end == later.start, "sections must not gap or overlap"
    for section in sections:
        assert section.text == html[section.start : section.end]
        assert section.length == len(section.text)


@pytest.mark.parametrize("fixture", ALL_FIXTURES)
def test_section_names_are_deterministic_and_unique(fixture):
    html = _load_html_fixture(fixture)

    first = [(s.name, s.start, s.end, s.kind) for s in extract_sections(html)]
    second = [(s.name, s.start, s.end, s.kind) for s in extract_sections(html)]

    assert first == second
    names = [name for name, _, _, _ in first]
    assert len(names) == len(set(names)), "section names must be unique within a document"


def test_empty_document_yields_no_sections():
    assert extract_sections("") == []


def test_document_without_anchors_is_a_single_filler_section():
    """A Cloudflare challenge page anchors nothing, and must still round-trip."""
    html = _load_html_fixture("challenge_just_a_moment")
    sections = extract_sections(html)

    assert len(sections) == 1
    assert sections[0].name == "document_prefix"
    assert sections[0].kind == "filler"
    assert reconstruct(sections) == html


def test_malformed_markup_still_covers_the_whole_document():
    """Stray closers and unclosed tags must not drop or duplicate characters."""
    html = (
        "<html><body><p>unclosed"
        "<script id='initial-activity-data'>{\"listing_id\": \"x\"}</script>"
        "</div>"  # stray close tag with no matching open
        "<div class='listings-carousel'><fuse-card>c</fuse-card></div>"
        "</body></html>"
    )
    sections = extract_sections(html)

    assert reconstruct(sections) == html
    assert {"vehicle_activity_json", "carousel_block"} <= {s.name for s in sections}


def test_repeated_anchor_names_are_disambiguated():
    html = (
        "<html><body>"
        "<div class='dealer-card'><h3>First</h3></div>"
        "<div class='dealer-card'><h3>Second</h3></div>"
        "</body></html>"
    )
    names = [s.name for s in extract_sections(html)]

    assert "dealer_contact_block" in names
    assert "dealer_contact_block__2" in names
    assert reconstruct(extract_sections(html)) == html


def test_nested_anchor_does_not_overlap_its_parent():
    """Outermost-first selection: a nested anchor is absorbed, not double-counted."""
    html = (
        "<html><body><div class='listings-carousel'>"
        "<div class='dealer-card'><h3>Nested</h3></div>"
        "</div></body></html>"
    )
    sections = extract_sections(html)
    names = [s.name for s in sections]

    assert "carousel_block" in names
    assert not any(name.startswith("dealer_contact_block") for name in names)
    assert reconstruct(sections) == html


# --------------------------------------------------------------------------
# Sectioning is structural: nothing reads inside a block
# --------------------------------------------------------------------------


def test_section_boundaries_do_not_depend_on_block_contents():
    """Rewriting a block's payload must not move any section boundary."""
    html = _load_html_fixture("real_detail_crv")
    mutated = _replace_section_json(
        html, "vehicle_activity_json", lambda payload: payload.clear()
    )

    original = [(s.name, s.kind) for s in extract_sections(html)]
    after = [(s.name, s.kind) for s in extract_sections(mutated)]
    assert original == after


def test_non_json_script_body_is_sectioned_normally():
    """A payload that is not JSON at all must still anchor and round-trip."""
    html = (
        "<html><body>"
        "<script id='initial-activity-data'>var x = 1; // not JSON</script>"
        "</body></html>"
    )
    section = _section_by_name(extract_sections(html), "vehicle_activity_json")

    assert section.kind == "script"
    assert reconstruct(extract_sections(html)) == html


def test_kind_is_structural_only():
    """kind is decided by tag and class, never by parsing a payload."""
    html = _load_html_fixture("real_detail_crv")
    sections = extract_sections(html)

    assert {s.kind for s in sections} == {"script", "dom", "filler"}
    for section in sections:
        if section.name in {"dealer_contact_block", "carousel_block"}:
            assert section.kind == "dom"
        elif section.name.startswith(("filler_", "document_")):
            assert section.kind == "filler"
        else:
            assert section.kind == "script"
            assert section.text.startswith("<script")


# --------------------------------------------------------------------------
# Taxonomy
# --------------------------------------------------------------------------


@pytest.mark.parametrize("fixture", REAL_FIXTURES)
def test_real_fixtures_carry_the_expected_script_sections(fixture):
    html = _load_html_fixture(fixture)
    names = {s.name for s in extract_sections(html)}

    assert EXPECTED_SCRIPT_SECTIONS <= names
    assert PARSER_CRITICAL_SECTION_NAMES <= names


@pytest.mark.parametrize("fixture", REAL_FIXTURES)
def test_parser_critical_sections_are_never_filler(fixture):
    html = _load_html_fixture(fixture)
    sections = extract_sections(html)

    for name in PARSER_CRITICAL_SECTION_NAMES:
        assert _section_by_name(sections, name).kind in {"script", "dom"}


# --------------------------------------------------------------------------
# Content addressing
# --------------------------------------------------------------------------


def test_section_sha256_is_content_addressed():
    assert section_sha256("abc") == section_sha256("abc")
    assert section_sha256("abc") != section_sha256("abd")


@pytest.mark.parametrize("fixture", REAL_FIXTURES)
def test_price_change_moves_only_the_activity_section_hash(fixture):
    """A price edit must show up in vehicle_activity_json and nowhere else."""
    html = _load_html_fixture(fixture)
    mutated = _replace_section_json(
        html, "vehicle_activity_json", lambda payload: payload.__setitem__("price", 999999)
    )

    before = {s.name: section_sha256(s.text) for s in extract_sections(html)}
    after = {s.name: section_sha256(s.text) for s in extract_sections(mutated)}

    assert before["vehicle_activity_json"] != after["vehicle_activity_json"]
    changed = {name for name in before if before[name] != after.get(name)}
    assert changed == {"vehicle_activity_json"}

    # And it reaches the parser, so the section really is the carrier.
    assert parse_cars_detail_page_html_v1(mutated)[0]["price"] == 999999


def test_identical_sections_share_a_hash_across_different_listings():
    """Dedup is content-addressed, so it is not scoped to one listing.

    The two real fixtures are different listings. Any section whose bytes match
    collapses to a single stored object, with no extra machinery.
    """
    a = extract_sections(_load_html_fixture("real_detail_crv"))
    b = extract_sections(_load_html_fixture("real_detail_2"))

    a_hashes = {s.name: section_sha256(s.text) for s in a}
    b_hashes = {s.name: section_sha256(s.text) for s in b}
    shared = {name for name, h in a_hashes.items() if b_hashes.get(name) == h}

    assert shared, "expected at least some cross-listing section reuse"
    # Page-shell config blocks are identical across unrelated listings.
    assert "script_datadog_config" in shared
    assert "script_graphql_config" in shared
    # Listing-specific content must NOT collide.
    assert "vehicle_activity_json" not in shared
    assert "dealer_contact_block" not in shared


# --------------------------------------------------------------------------
# Manifest
# --------------------------------------------------------------------------


def test_manifest_serialization_is_stable():
    html = _load_html_fixture("real_detail_crv")
    sections = extract_sections(html)

    kwargs = dict(
        artifact_id=4171890,
        listing_id="49953eaa-2c73-4841-b419-7d77f81a534e",
        source_minio_path="s3://bronze/html/x.zst",
        source_raw_sha256=section_sha256(html),
        parser_equivalent_verified=True,
        verified_at="2026-07-01T00:00:00Z",
    )
    first = build_manifest(sections, **kwargs)
    second = build_manifest(extract_sections(html), **kwargs)

    assert serialize_manifest(first) == serialize_manifest(second)
    assert first["manifest_version"] == 1
    assert first["parser_version"] == PARSER_VERSION
    assert [s["name"] for s in first["sections"]] == [s.name for s in sections]
    assert first["reconstruction"] == {
        "mode": "ordered_sections",
        "parser_equivalent_verified": True,
        "verified_at": "2026-07-01T00:00:00Z",
    }


def test_manifest_never_claims_a_section_was_normalized():
    """The module does not normalize; the field exists only for schema shape."""
    sections = extract_sections(_load_html_fixture("real_detail_crv"))
    manifest = build_manifest(sections)

    assert all(entry["normalized"] is False for entry in manifest["sections"])


def test_manifest_hashes_address_the_verbatim_slice():
    sections = extract_sections(_load_html_fixture("real_detail_crv"))
    manifest = build_manifest(sections)

    by_name = {entry["name"]: entry for entry in manifest["sections"]}
    for section in sections:
        entry = by_name[section.name]
        assert entry["content_sha256"] == section_sha256(section.text)
        assert entry["content_path"].endswith(entry["content_sha256"])
        assert entry["length"] == len(section.text)


def test_unverified_manifest_carries_no_verification_timestamp():
    """Failed reconstruction must not leave a manifest looking verified."""
    sections = extract_sections(_load_html_fixture("real_detail_crv"))
    manifest = build_manifest(
        sections, parser_equivalent_verified=False, verified_at="2026-07-01T00:00:00Z"
    )

    assert manifest["reconstruction"]["parser_equivalent_verified"] is False
    assert manifest["reconstruction"]["verified_at"] is None


# --------------------------------------------------------------------------
# Stage 2 gate
# --------------------------------------------------------------------------


@pytest.mark.parametrize("fixture", ALL_FIXTURES)
def test_reconstruction_is_parser_equivalent(fixture):
    """The gate. Must pass on every fixture before any MinIO work."""
    html = _load_html_fixture(fixture)
    rebuilt = reconstruct(extract_sections(html))

    equivalent, differences = parse_outputs_equivalent(html, rebuilt)
    assert equivalent, differences


@pytest.mark.parametrize("fixture", REAL_FIXTURES)
def test_gate_is_not_vacuous(fixture):
    """Guard the gate itself: these fixtures carry the fields it compares."""
    primary, carousel, meta = parse_cars_detail_page_html_v1(_load_html_fixture(fixture))

    assert meta["primary_json_present"] is True
    assert meta["dealer_card_found"] is True
    assert meta["carousel_found"] is True
    assert meta["cards_parsed"] > 0
    assert primary["listing_state"] == "active"
    assert primary["vin"] and primary["price"] and primary["dealer_phone"]


def test_parse_outputs_equivalent_detects_a_real_difference():
    """A changed price must fail the gate, and be named in the differences."""
    html = _load_html_fixture("real_detail_crv")
    mutated = _replace_section_json(
        html, "vehicle_activity_json", lambda payload: payload.__setitem__("price", 12345)
    )

    equivalent, differences = parse_outputs_equivalent(html, mutated)
    assert not equivalent
    assert any(diff.startswith("primary.price") for diff in differences)


def test_html_len_alone_does_not_fail_the_gate():
    """The one relaxation in the contract: raw input length is excluded."""
    html = _load_html_fixture("real_detail_crv")
    padded = html + "\n<!-- padding that changes html_len only -->\n"

    _, _, meta_a = parse_cars_detail_page_html_v1(html)
    _, _, meta_b = parse_cars_detail_page_html_v1(padded)
    assert meta_a["html_len"] != meta_b["html_len"]

    equivalent, differences = parse_outputs_equivalent(html, padded)
    assert equivalent, differences


# --------------------------------------------------------------------------
# Why sectioning stays lossless: the two whole-document parser scans
# --------------------------------------------------------------------------


@pytest.mark.parametrize("fixture", REAL_FIXTURES)
def test_dealer_card_slice_reparses_to_the_same_card_fields(fixture):
    """Validates the offset scanner: the slice is a faithful .dealer-card subtree.

    ``dealer_phone`` is deliberately excluded. It does not come from the card at
    all -- it comes from the whole-document ``"seller"`` regex
    (parse_detail_page.py:166) -- so it is asserted separately below as the
    demonstration that the scan really is document-wide.
    """
    from bs4 import BeautifulSoup

    html = _load_html_fixture(fixture)
    card_section = _section_by_name(extract_sections(html), "dealer_contact_block")

    from_slice = _parse_dealer_card(BeautifulSoup(card_section.text, "lxml"))
    from_document = _parse_dealer_card(BeautifulSoup(html, "lxml"))

    card_fields = set(from_slice) | set(from_document)
    card_fields.discard("dealer_phone")
    assert card_fields, "dealer card produced no fields"
    for field in sorted(card_fields):
        assert from_slice.get(field) == from_document.get(field), field

    assert from_document.get("dealer_phone")
    assert from_slice.get("dealer_phone") is None


@pytest.mark.parametrize("fixture", REAL_FIXTURES)
def test_seller_blob_lives_only_in_the_vehicle_controller_section(fixture):
    """Landmine 1: first-match-wins seller scan. One match, one section."""
    sections = extract_sections(_load_html_fixture(fixture))

    holders = [s.name for s in sections if _SELLER_BLOB_RE.search(s.text)]
    assert holders == ["vehicle_controller_json"]


def test_dropping_a_section_holding_an_unlisted_marker_would_break_the_parse():
    """Landmine 2, tested positively with a synthetic fixture.

    No checked-in fixture contains an unlisted marker, so the whole-document
    scan at parse_detail_page.py:72 cannot be exercised by real pages. Inject
    one into a page-shell section: lossless reconstruction survives it, and
    discarding that section flips listing_state back to 'active'.

    This is why the module offers no pruning. If pruning is ever reintroduced,
    this test is the one that must keep passing.
    """
    html = _replace_section_json(
        _load_html_fixture("real_detail_crv"),
        "script_third_party_flags",
        lambda payload: payload.__setitem__("_synthetic", "this listing is no longer available"),
    )

    # The marker is armed: the page now parses as unlisted.
    assert parse_cars_detail_page_html_v1(html)[0]["listing_state"] == "unlisted"

    sections = extract_sections(html)

    # Lossless reconstruction is immune, because it discards nothing.
    equivalent, differences = parse_outputs_equivalent(html, reconstruct(sections))
    assert equivalent, differences

    # Discarding the section silently flips listing_state back to active.
    pruned = _drop_sections(sections, _REJECTED_DROP_CANDIDATES)
    equivalent, differences = parse_outputs_equivalent(html, pruned)
    assert not equivalent
    assert any(diff.startswith("primary.listing_state") for diff in differences)


def test_dropping_a_section_holding_a_seller_blob_would_break_the_parse():
    """Landmine 1: a decoy seller blob ahead of the real one changes the answer."""
    html = _replace_section_json(
        _load_html_fixture("real_detail_crv"),
        "script_datadog_config",
        lambda payload: payload.__setitem__("seller", {"phoneNumber": "(000) 000-0000"}),
    )
    sections = extract_sections(html)

    # Lossless reconstruction is immune.
    equivalent, differences = parse_outputs_equivalent(html, reconstruct(sections))
    assert equivalent, differences

    # The decoy wins the first-match scan, so discarding it changes dealer_phone.
    pruned = _drop_sections(sections, _REJECTED_DROP_CANDIDATES)
    equivalent, differences = parse_outputs_equivalent(html, pruned)
    assert not equivalent
    assert any(diff.startswith("primary.dealer_phone") for diff in differences)


def test_module_exposes_no_way_to_drop_or_rewrite_a_section():
    """Guards the design decision itself against quiet reintroduction."""
    import processing.html_sections as html_sections

    for banned in ("normalize_section", "tier_b_html", "volatile_drop_risks"):
        assert not hasattr(html_sections, banned), (
            f"{banned} reintroduces lossy sectioning; see the module docstring"
        )
    assert not any(f.name == "volatile" for f in Section.__dataclass_fields__.values())
