"""Sectioned HTML artifact support for Plan 114 (Stages 0-2).

Splits a raw cars.com detail page into named, content-addressed sections so
that section-level reuse can be measured, and so a manifest can describe how
to reconstruct the artifact later.

Sectioning is lossless. Always.
------------------------------
Sections are contiguous, non-overlapping character slices covering the
*entire* document, so ``reconstruct(extract_sections(html)) == html``
byte-for-byte **by construction** -- not by testing.

This module deliberately offers no way to drop, reorder, or rewrite a
section. An earlier draft had a second "normalized" tier that key-sorted JSON
script bodies and discarded sections believed to be per-request noise. It was
rejected: the equivalence gate can only ever validate against the *current*
parser, while the entire point of retaining artifacts is reprocessing them
under *future* parser versions. Any lossy transform is a bet that no future
parser will want what it discarded, and a bet that cannot be re-run once the
original bytes are gone. Measured reuse gain for that tier was ~1.8%, which
did not come close to justifying the exposure.

A consequence worth keeping: nothing here reads *inside* a block. Section
boundaries come from tag offsets, the ``id`` attribute, and class names only.
There is no JSON parsing anywhere in this module, so an upstream change to a
payload's shape cannot move a boundary or corrupt a slice.

We also do **not** extract via BeautifulSoup and re-join ``str(element)``:
lxml re-serialization introduces byte differences everywhere, which would
conflate "our sectioning is wrong" with "lxml normalized an attribute".

Why dropping sections is unsafe (the two landmines)
---------------------------------------------------
Byte-identical reconstruction disarms two whole-document scans in the parser.
Both are invisible until something is dropped or reordered, which is why they
are recorded here and pinned by tests:

* parse_detail_page.py:166 searches ``"seller"\\s*:\\s*\\{([^}]+)\\}`` over the
  whole serialized document, **first match wins**. In both real fixtures the
  seller blob occurs exactly once, inside
  ``script#CarsWeb.VehicleDetailController.show`` (``vehicle_controller_json``),
  in the last ~2% of the document. Dropping any *earlier* section that happens
  to contain a ``"seller"`` object would silently change ``dealer_phone`` and
  the fallback ``dealer_zip``.
* parse_detail_page.py:72 searches the raw string for "no longer
  available"/"no longer listed". Dropping an analytics or anti-bot section
  that happens to contain that text flips ``listing_state`` from ``unlisted``
  to ``active``.

Equivalence contract (Stage 0)
------------------------------
Reconstruction is guaranteed by construction, but :func:`parse_outputs_equivalent`
still exists to verify it -- it catches bugs in the extractor itself, and it is
the gate any future change to this module must pass. Two HTML strings are
"parser equivalent" when ``parse_cars_detail_page_html_v1`` returns:

* ``primary``  -- compared **exactly**;
* ``carousel`` -- compared **exactly**;
* ``meta``     -- compared exactly **after excluding ``html_len``**, with
  ``primary_json_present``, ``dealer_card_found``, ``carousel_found``,
  ``cards_found`` and ``cards_parsed`` all required to match.

``html_len`` is excluded because the parser records raw input length
(parse_detail_page.py:380), so a padded-but-equivalent input would fail a naive
dict comparison for a reason that says nothing about correctness. Nothing else
is relaxed.

Dedup is content-addressed, so it is not listing-scoped
-------------------------------------------------------
Section identity is ``sha256(text)`` in a flat global namespace, so identical
slices collapse to one stored object no matter which listing or which capture
they came from. Cross-listing sharing needs no extra machinery.

Measured on the two real fixtures (two different listings), whole-section
hashes shared across them cover only **2.2%** of the document -- but a
line-level diff of the four large filler sections shows **~52%** of the
document is actually common between the two listings. The gap is section
*granularity*: the big fillers mix static page shell with listing-specific
content, so a single differing byte spoils the whole slice. Two concrete
examples: ``document_suffix`` (1146 chars) differs only by an 8-hex-character
build token, and ``filler_3`` is 86% common line-for-line.

Refining the taxonomy inside those fillers is therefore where the remaining
reuse lives. The plan explicitly permits refining the taxonomy against real
pages; that work belongs in Stage 3 with a real sample behind it, not here.

Parser-critical sections
------------------------
``vehicle_activity_json``, ``vehicle_controller_json``,
``dealer_contact_block`` and ``carousel_block`` are the sections the parser
actually reads. They are named in :data:`PARSER_CRITICAL_SECTION_NAMES` for
Stage 4's parse-input-projection *measurement* only -- storing just those four
is a number that brackets the storage answer, never a productionization path,
for exactly the future-parser reason given above.

Known gap, stated explicitly: the ``<title>`` element (read only when the
activity JSON is absent, parse_detail_page.py:101) and
``spark-notification.unlisted-notification`` (parse_detail_page.py:61) are not
anchored -- they land inside filler sections. Harmless while sectioning stays
lossless, but it is a precondition anyone adding pruning would have to revisit.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from processing.processors.parse_detail_page import parse_cars_detail_page_html_v1

PARSER_VERSION = "cars_detail_page__v1"
MANIFEST_VERSION = 1

DEFAULT_SECTION_PREFIX = "s3://bronze/html_sections/sha256"

# Elements that never nest, so they must not push onto the tag stack.
_VOID_ELEMENTS = frozenset(
    {
        "area", "base", "br", "col", "embed", "hr", "img", "input",
        "link", "meta", "param", "source", "track", "wbr",
    }
)

# script[id] -> section name. Any other id'd script becomes script_<slug(id)>.
_SCRIPT_ID_SECTION_NAMES = {
    "initial-activity-data": "vehicle_activity_json",
    "initial-als-data": "als_json",
    "CarsWeb.VehicleDetailController.show": "vehicle_controller_json",
}

#: Sections the parser actually reads. See the module docstring: this exists
#: for measurement, not to license discarding anything else.
PARSER_CRITICAL_SECTION_NAMES = frozenset(
    {
        "vehicle_activity_json",
        "vehicle_controller_json",
        "dealer_contact_block",
        "carousel_block",
    }
)

#: meta keys that must match for two parses to be equivalent, beyond the
#: whole-dict comparison that excludes html_len.
_REQUIRED_META_KEYS = (
    "primary_json_present",
    "dealer_card_found",
    "carousel_found",
    "cards_found",
    "cards_parsed",
)

_META_EXCLUDED_KEYS = frozenset({"html_len"})

_SLUG_RE = re.compile(r"[^0-9a-z]+")


@dataclass(frozen=True)
class Section:
    """One contiguous character slice of the source document.

    ``text`` is always ``html[start:end]`` for the document it came from.
    ``kind`` is structural only -- ``script``, ``dom`` or ``filler`` -- and is
    never derived from a block's contents.
    """

    name: str
    start: int
    end: int
    text: str
    kind: str

    @property
    def length(self) -> int:
        return self.end - self.start


@dataclass(frozen=True)
class _Element:
    tag: str
    attrs: Dict[str, Optional[str]]
    start: int
    end: int


def _slug(value: str) -> str:
    return _SLUG_RE.sub("_", value.lower()).strip("_")


def _line_start_offsets(html: str) -> List[int]:
    """Absolute index of the first character of each line."""
    offsets = [0]
    for index, char in enumerate(html):
        if char == "\n":
            offsets.append(index + 1)
    return offsets


class _ElementSpanScanner(HTMLParser):
    """Records absolute ``[start, end)`` character spans for every element.

    ``HTMLParser.getpos()`` reports a 1-based line and 0-based column, which a
    precomputed line-start table converts to an absolute index into the source
    string. ``convert_charrefs=False`` keeps the parser from rewriting entity
    references, which is safer when positions are what we care about;
    ``<script>`` bodies are handled as CDATA by HTMLParser itself, so script
    contents never register as markup.
    """

    def __init__(self, html: str) -> None:
        super().__init__(convert_charrefs=False)
        self._html = html
        self._line_starts = _line_start_offsets(html)
        self._stack: List[Tuple[str, int, Dict[str, Optional[str]]]] = []
        self.elements: List[_Element] = []

    def _absolute_position(self) -> int:
        line, column = self.getpos()
        return self._line_starts[line - 1] + column

    def handle_starttag(self, tag: str, attrs: Sequence[Tuple[str, Optional[str]]]) -> None:
        if tag in _VOID_ELEMENTS:
            return
        self._stack.append((tag, self._absolute_position(), dict(attrs)))

    def handle_startendtag(self, tag: str, attrs: Sequence[Tuple[str, Optional[str]]]) -> None:
        # Self-closing: no depth change, and nothing can be anchored inside it.
        return

    def handle_endtag(self, tag: str) -> None:
        close_start = self._absolute_position()
        for index in range(len(self._stack) - 1, -1, -1):
            if self._stack[index][0] != tag:
                continue
            # Anything still open above this tag was never closed; record it as
            # implicitly ending where the enclosing close tag begins.
            for open_tag, open_start, open_attrs in self._stack[index + 1:]:
                self.elements.append(_Element(open_tag, open_attrs, open_start, close_start))
            matched_tag, matched_start, matched_attrs = self._stack[index]
            close_end = self._html.find(">", close_start)
            end = len(self._html) if close_end == -1 else close_end + 1
            self.elements.append(_Element(matched_tag, matched_attrs, matched_start, end))
            del self._stack[index:]
            return
        # No matching open tag — stray close tag in malformed markup. Ignore.

    def finish(self) -> List[_Element]:
        self.close()
        for open_tag, open_start, open_attrs in self._stack:
            self.elements.append(_Element(open_tag, open_attrs, open_start, len(self._html)))
        self._stack.clear()
        return self.elements


def _class_names(element: _Element) -> frozenset:
    raw = element.attrs.get("class") or ""
    return frozenset(raw.split())


def _anchor_name(element: _Element) -> Optional[str]:
    """Section name for an anchorable element, or None if it is not an anchor."""
    if element.tag == "script":
        script_id = element.attrs.get("id")
        if script_id:
            return _SCRIPT_ID_SECTION_NAMES.get(script_id) or f"script_{_slug(script_id)}"
        return None
    classes = _class_names(element)
    if "dealer-card" in classes:
        return "dealer_contact_block"
    if element.tag == "div" and "listings-carousel" in classes:
        return "carousel_block"
    return None


def _select_anchors(elements: Iterable[_Element]) -> List[Tuple[int, int, str]]:
    """Non-overlapping anchor spans, outermost-first, in document order."""
    candidates = [
        (element.start, element.end, name)
        for element in elements
        if (name := _anchor_name(element)) is not None
    ]
    # (start asc, end desc) makes the outermost element win a tie on start.
    candidates.sort(key=lambda item: (item[0], -item[1]))

    selected: List[Tuple[int, int, str]] = []
    last_end = 0
    for start, end, name in candidates:
        if start >= last_end:
            selected.append((start, end, name))
            last_end = end
    return selected


def _deduplicate_names(names: List[str]) -> List[str]:
    """Suffix repeated section names so every name in a document is unique."""
    counts: Dict[str, int] = {}
    out: List[str] = []
    for name in names:
        counts[name] = counts.get(name, 0) + 1
        out.append(name if counts[name] == 1 else f"{name}__{counts[name]}")
    return out


def extract_sections(html: str) -> List[Section]:
    """Split ``html`` into contiguous, non-overlapping sections covering it all.

    ``"".join(section.text for section in extract_sections(html)) == html`` for
    any input. Anchored sections are the id'd ``<script>`` blocks plus the
    ``.dealer-card`` and ``div.listings-carousel`` subtrees; everything between
    them becomes honest filler (``document_prefix`` / ``filler_N`` /
    ``document_suffix``) rather than invented ``global_header`` /
    ``static_assets`` boundaries that the pages do not actually mark.

    A document with no anchors at all (for example a Cloudflare challenge page)
    yields a single ``document_prefix`` section spanning the whole input.
    """
    if not html:
        return []

    scanner = _ElementSpanScanner(html)
    scanner.feed(html)
    anchors = _select_anchors(scanner.finish())

    if not anchors:
        return [Section("document_prefix", 0, len(html), html, "filler")]

    spans: List[Tuple[int, int, str, str]] = []  # (start, end, name, kind)
    filler_index = 0
    cursor = 0
    for start, end, name in anchors:
        if start > cursor:
            if not spans:
                filler_name = "document_prefix"
            else:
                filler_index += 1
                filler_name = f"filler_{filler_index}"
            spans.append((cursor, start, filler_name, "filler"))
        kind = "dom" if name in {"dealer_contact_block", "carousel_block"} else "script"
        spans.append((start, end, name, kind))
        cursor = end

    if cursor < len(html):
        spans.append((cursor, len(html), "document_suffix", "filler"))

    unique_names = _deduplicate_names([name for _, _, name, _ in spans])
    return [
        Section(unique_name, start, end, html[start:end], kind)
        for (start, end, _, kind), unique_name in zip(spans, unique_names)
    ]


def reconstruct(sections: Iterable[Section]) -> str:
    """Rejoin sections into HTML. Exact inverse of :func:`extract_sections`."""
    return "".join(section.text for section in sections)


def section_sha256(text: str) -> str:
    """Content address for a section. Global: not scoped to listing or capture."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_manifest(
    sections: Sequence[Section],
    *,
    artifact_id: Optional[int] = None,
    listing_id: Optional[str] = None,
    source_minio_path: Optional[str] = None,
    source_raw_sha256: Optional[str] = None,
    parser_equivalent_verified: bool = False,
    verified_at: Optional[str] = None,
    mode: str = "ordered_sections",
    content_path_for: Optional[Callable[[str], str]] = None,
) -> Dict[str, Any]:
    """Build the prototype manifest described in the plan doc.

    ``normalized`` is present on every section entry for schema compatibility
    with the plan's manifest shape, and is always ``False``: this module does
    not normalize. See the module docstring for why.
    """
    path_for = content_path_for or (lambda sha: f"{DEFAULT_SECTION_PREFIX}/{sha}")

    entries: List[Dict[str, Any]] = []
    for section in sections:
        content_sha256 = section_sha256(section.text)
        entries.append(
            {
                "name": section.name,
                "content_sha256": content_sha256,
                "content_path": path_for(content_sha256),
                "encoding": "utf-8",
                "normalized": False,
                "kind": section.kind,
                "length": section.length,
            }
        )

    return {
        "manifest_version": MANIFEST_VERSION,
        "artifact_id": artifact_id,
        "listing_id": listing_id,
        "source_minio_path": source_minio_path,
        "parser_version": PARSER_VERSION,
        "source_raw_sha256": source_raw_sha256,
        "sections": entries,
        "reconstruction": {
            "mode": mode,
            "parser_equivalent_verified": parser_equivalent_verified,
            "verified_at": verified_at if parser_equivalent_verified else None,
        },
    }


def serialize_manifest(manifest: Dict[str, Any]) -> str:
    """Stable serialization: sorted keys, fixed separators, section order kept."""
    return json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _compare_meta(meta_a: Dict[str, Any], meta_b: Dict[str, Any]) -> List[str]:
    diffs: List[str] = []
    for key in _REQUIRED_META_KEYS:
        if meta_a.get(key) != meta_b.get(key):
            diffs.append(f"meta.{key}: {meta_a.get(key)!r} != {meta_b.get(key)!r}")

    reported = {diff.split(":", 1)[0] for diff in diffs}
    keys = (set(meta_a) | set(meta_b)) - _META_EXCLUDED_KEYS
    for key in sorted(keys):
        if f"meta.{key}" in reported:
            continue
        if meta_a.get(key) != meta_b.get(key):
            diffs.append(f"meta.{key}: {meta_a.get(key)!r} != {meta_b.get(key)!r}")
    return diffs


def parse_outputs_equivalent(
    html_a: str,
    html_b: str,
    url: Optional[str] = None,
) -> Tuple[bool, List[str]]:
    """Check the Stage 0 equivalence contract between two HTML strings.

    Returns ``(equivalent, differences)``; ``differences`` names every field
    that failed, so a failing gate says what broke rather than just that it
    broke.
    """
    primary_a, carousel_a, meta_a = parse_cars_detail_page_html_v1(html_a, url)
    primary_b, carousel_b, meta_b = parse_cars_detail_page_html_v1(html_b, url)

    diffs: List[str] = []
    for key in sorted(set(primary_a) | set(primary_b)):
        if primary_a.get(key) != primary_b.get(key):
            diffs.append(f"primary.{key}: {primary_a.get(key)!r} != {primary_b.get(key)!r}")

    if len(carousel_a) != len(carousel_b):
        diffs.append(f"carousel length: {len(carousel_a)} != {len(carousel_b)}")
    else:
        for index, (card_a, card_b) in enumerate(zip(carousel_a, carousel_b)):
            if card_a != card_b:
                diffs.append(f"carousel[{index}]: {card_a!r} != {card_b!r}")

    diffs.extend(_compare_meta(meta_a, meta_b))
    return not diffs, diffs
