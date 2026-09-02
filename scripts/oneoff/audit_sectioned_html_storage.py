"""Plan 114 Stage 3: audit sectioned HTML storage over a real MinIO sample.

Stages 0-2 built the lossless sectioner (``processing/html_sections.py``) and
measured it on two fixtures. Those two fixtures said something specific and
uncomfortable: whole-section hashes shared across two listings covered only
**2.2%** of the document, while a line-level diff showed **~52%** of it was
actually common. Roughly 50 percentage points of real redundancy existed but
was unreachable, because the large filler sections mix static page shell with
listing-specific content and one differing byte spoils a 60KB slice.

So this script does not ask "is section reuse high enough?". It asks:

    Can the filler sections be split finely enough to reach the redundancy
    that demonstrably exists -- and is that reachable losslessly?

It answers with five measurements, in the order they inform the decision:

1. **Whole-section reuse, split by scope.** Within a listing (across captures)
   and across listings are reported as separate numbers. They have different
   implications and earlier drafts of the plan conflated them.
2. **Sub-section redundancy.** Line-level *and* character-level, because the
   units decide the answer. ``document_suffix`` is the cautionary case: 19%
   common line-for-line but 99% common character-for-character, the whole
   difference being an 8-hex-character build token. Reporting only line-level
   would have understated that section by a factor of five.
3. **A granularity bound.** Content-defined chunking over the whole sample,
   which is what a *maximally* fine lossless splitter could reach. This is a
   bound to compare the taxonomy against, **not a proposed design** -- see the
   caveat on :func:`chunk_dedup_bound`.
4. **Storage accounting, compressed, against the baseline that is hard to
   beat.** Three ways this measurement could flatter itself, all closed:
   uncompressed character counts credit dedup with savings zstd already finds;
   a plain per-object zstd baseline credits it with savings a *trained
   dictionary* would find far more cheaply; and a bytes-only model ignores that
   every MinIO object costs ~8 KB and ~2.24 inodes whatever its size. So the
   report carries object and inode counts, and scores the sectioned layout
   against a dictionary baseline as well as a plain one.
5. **The volatile/stable labels, re-tested.** Stages 0-2 found six of seven
   hypothesised "per-request volatile" sections were byte-identical across two
   listings -- static page shell, not noise. n=2 is a hint, not a result. This
   re-runs it against the real sample.

Read-only. This script never writes to MinIO, and never deletes anything. It
does not write section objects either: Stage 3 is measurement, and writing a
parallel object layout is a Stage 5 decision that should be made with these
numbers in hand.

Sample bias, stated up front
----------------------------
Sampling reuses ``audit_semantic_duplicate_html_hashes.fetch_sample``, which
selects the groups with the **highest** duplicate artifact counts. That is the
right sample for "when the parsed state is unchanged, what do the bytes do?",
and the wrong sample for extrapolating a corpus-wide storage number: repeat
captures of the same listing are over-represented by construction. The report
labels the within-listing figure as an upper bound for that reason. The
cross-listing figure is much less affected, since those artifacts come from
different listings regardless of how often each was captured.

Where it runs
-------------
Needs DuckDB (lake sampling), boto3 + zstandard (object reads), and
bs4 + lxml (the parser gate), plus network reach to MinIO -- so it is intended
to run inside the compose network, not from a laptop.
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Sequence

from processing.html_sections import (
    PARSER_CRITICAL_SECTION_NAMES,
    build_manifest,
    extract_sections,
    parse_outputs_equivalent,
    reconstruct,
    section_sha256,
    serialize_manifest,
)

LOG = logging.getLogger("sectioned_html_audit")

#: zstd level the write path uses (shared/minio.py). Section objects and
#: manifests are costed at the same level so the comparison is like-for-like.
DEFAULT_ZSTD_LEVEL = 9

#: Content-defined chunking targets, in bytes. 1 KiB is the headline; the
#: others bracket it so the granularity/overhead tradeoff is visible rather
#: than implied.
DEFAULT_CHUNK_TARGETS = (256, 1024, 4096)

#: Per-object storage floor, measured on the production VM 2026-08-08 (see the
#: "Storage Accounting" section of the plan doc). MinIO stores every object as
#: a directory plus an ``xl.meta`` file, inlining payloads under 128 KB, so on
#: this single-drive backend an object costs ~8 KB (4 KB directory + 4 KB
#: rounded file) no matter how small its content is.
#:
#: This is not a rounding detail. It is what makes fine granularity a trap:
#: 256-byte chunks "save" nearly everything and then spend it all back, 32x
#: over, on object overhead.
DEFAULT_OBJECT_OVERHEAD_BYTES = 8192

#: Inodes consumed per MinIO object, measured the same day (8,774,058 inodes
#: for 3,918,760 objects). Inode headroom is tighter than byte headroom, so the
#: audit reports the inode cost of a section store rather than only its bytes.
INODES_PER_OBJECT = 2.24

#: Size of the trained zstd dictionary used for the honest compression
#: baseline. 112 KB is the zstd default for `--train`.
DEFAULT_DICT_SIZE_BYTES = 112 * 1024


# ── Records ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SectionRecord:
    """One section of one artifact, reduced to what the measurements need."""

    name: str
    sha256: str
    chars: int
    nbytes: int


@dataclass(frozen=True)
class ArtifactRecord:
    """One sampled artifact, sectioned and verified."""

    artifact_id: int
    listing_id: str
    group_key: str
    minio_path: str
    fetched_at: str
    raw_chars: int
    raw_bytes: int
    stored_compressed_bytes: int
    manifest_json: str
    sections: tuple[SectionRecord, ...]
    reconstructed_exactly: bool
    parser_equivalent: Optional[bool]
    parse_differences: tuple[str, ...] = ()
    decode_was_lossy: bool = False

    @property
    def parser_critical_chars(self) -> int:
        return sum(s.chars for s in self.sections if _base_name(s.name) in
                   PARSER_CRITICAL_SECTION_NAMES)


@dataclass
class Failure:
    artifact_id: Optional[int]
    minio_path: Optional[str]
    stage: str
    error: str


@dataclass
class Totals:
    sampled_rows: int = 0
    fetched: int = 0
    skipped_no_path: int = 0
    failures: list[Failure] = field(default_factory=list)


# ── Section-name helpers ──────────────────────────────────────────────────────

def _base_name(name: str) -> str:
    """Strip the ``__2`` disambiguation suffix ``extract_sections`` may add.

    Two artifacts can disagree on how many times a name repeats, so grouping by
    the raw name would split a section's statistics across ``filler_3`` and
    ``filler_3__2``. Comparisons are done on the base name.
    """
    base, sep, tail = name.rpartition("__")
    return base if sep and tail.isdigit() else name


# ── Measurement: whole-section reuse, split by scope ───────────────────────────

@dataclass
class ReuseReport:
    artifacts: int
    listings: int
    groups: int
    raw_chars: int
    unique_chars_global: int
    unique_chars_per_listing_sum: int
    unique_chars_per_group_sum: int
    unique_sections_global: int

    def as_dict(self) -> dict[str, Any]:
        raw = self.raw_chars or 1
        within_group = self.raw_chars - self.unique_chars_per_group_sum
        within_listing = self.raw_chars - self.unique_chars_per_listing_sum
        cross_listing = self.unique_chars_per_listing_sum - self.unique_chars_global
        total = self.raw_chars - self.unique_chars_global
        return {
            "artifacts": self.artifacts,
            "listings": self.listings,
            "semantic_duplicate_groups": self.groups,
            "raw_chars": self.raw_chars,
            "unique_chars_global": self.unique_chars_global,
            "unique_sections_global": self.unique_sections_global,
            # Each of these is a share of the same raw total, so they add up:
            # within-group <= within-listing, and within-listing + cross-listing
            # == total.
            "within_group_saving_pct": round(100.0 * within_group / raw, 4),
            "within_listing_saving_pct": round(100.0 * within_listing / raw, 4),
            "cross_listing_saving_pct": round(100.0 * cross_listing / raw, 4),
            "total_saving_pct": round(100.0 * total / raw, 4),
        }


def compute_reuse(artifacts: Sequence[ArtifactRecord]) -> ReuseReport:
    """Whole-section hash reuse at three scopes.

    Content addressing is a flat global namespace, so a section shared between
    two listings collapses with no extra machinery. Splitting the saving by
    scope is therefore purely a reporting choice -- but a necessary one, since
    "88% of repeat captures of one listing are redundant" and "88% of unrelated
    listings are redundant" are wildly different claims about the corpus.
    """
    by_listing: dict[str, dict[str, int]] = defaultdict(dict)
    by_group: dict[str, dict[str, int]] = defaultdict(dict)
    global_unique: dict[str, int] = {}
    raw_chars = 0

    for artifact in artifacts:
        raw_chars += artifact.raw_chars
        for section in artifact.sections:
            global_unique[section.sha256] = section.chars
            by_listing[artifact.listing_id][section.sha256] = section.chars
            by_group[artifact.group_key][section.sha256] = section.chars

    return ReuseReport(
        artifacts=len(artifacts),
        listings=len(by_listing),
        groups=len(by_group),
        raw_chars=raw_chars,
        unique_chars_global=sum(global_unique.values()),
        unique_chars_per_listing_sum=sum(sum(d.values()) for d in by_listing.values()),
        unique_chars_per_group_sum=sum(sum(d.values()) for d in by_group.values()),
        unique_sections_global=len(global_unique),
    )


# ── Measurement: per-section-name stability (re-tests the volatile labels) ─────

def section_name_stats(artifacts: Sequence[ArtifactRecord]) -> list[dict[str, Any]]:
    """Per section name: how much it varies, and at which scope.

    The label answers the Stage 0-2 question directly:

    * ``seen_once`` -- the name occurs in exactly one artifact, so it carries
      no evidence either way. This is not a footnote: cars.com emits inline
      scripts with random numeric ids (``script_1033207580``), and the
      ``script[id]`` anchor rule mints a fresh section name for each one. They
      would otherwise be scored ``identical_corpus_wide`` on a sample of one,
      which is exactly backwards -- a name seen once is the *least* evidence of
      stability, not the most.
    * ``identical_corpus_wide`` -- one hash across every artifact that has it,
      seen in more than one. Stored once for the whole corpus. Six of the seven
      sections previously hypothesised to be per-request volatile landed here
      on n=2.
    * ``stable_per_listing`` -- one hash within each listing, differing between
      listings. Genuinely listing-specific content, deduped across captures.
    * ``varies_within_listing`` -- differs between captures of the same
      listing. Only these are per-request volatile in any real sense.
    """
    occurrences: dict[str, int] = defaultdict(int)
    hashes: dict[str, set[str]] = defaultdict(set)
    listings: dict[str, set[str]] = defaultdict(set)
    hashes_by_listing: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    sizes: dict[str, list[int]] = defaultdict(list)
    present_in: dict[str, set[int]] = defaultdict(set)

    for artifact in artifacts:
        for section in artifact.sections:
            name = _base_name(section.name)
            occurrences[name] += 1
            hashes[name].add(section.sha256)
            listings[name].add(artifact.listing_id)
            hashes_by_listing[name][artifact.listing_id].add(section.sha256)
            sizes[name].append(section.chars)
            present_in[name].add(artifact.artifact_id)

    rows: list[dict[str, Any]] = []
    for name in sorted(occurrences):
        stable_within_every_listing = all(
            len(shas) == 1 for shas in hashes_by_listing[name].values()
        )
        if occurrences[name] == 1:
            label = "seen_once"
        elif len(hashes[name]) == 1:
            label = "identical_corpus_wide"
        elif stable_within_every_listing:
            label = "stable_per_listing"
        else:
            label = "varies_within_listing"
        rows.append(
            {
                "section": name,
                "occurrences": occurrences[name],
                "artifacts_present": len(present_in[name]),
                "listings": len(listings[name]),
                "distinct_hashes": len(hashes[name]),
                "median_chars": int(statistics.median(sizes[name])),
                "total_chars": sum(sizes[name]),
                "stability": label,
            }
        )
    return rows


# ── Measurement: sub-section redundancy (line-level and character-level) ───────

def _common_affix_chars(a: str, b: str) -> int:
    """Chars shared as a common prefix plus a common suffix, without overlap."""
    limit = min(len(a), len(b))
    prefix = 0
    while prefix < limit and a[prefix] == b[prefix]:
        prefix += 1
    suffix = 0
    while suffix < limit - prefix and a[-1 - suffix] == b[-1 - suffix]:
        suffix += 1
    return prefix + suffix


def common_content(a: str, b: str) -> dict[str, int]:
    """Common content between two texts, at line and character granularity.

    ``line_common_chars`` counts the characters in whole lines that match --
    the granularity Stages 0-2 reported.

    ``char_common_chars`` refines it: for every region the line diff calls
    changed, it adds the common prefix and suffix *within* that region. That
    is what turns ``document_suffix`` from 19% to 99% -- nine of its ten lines
    match, and the tenth differs only by an 8-character build token buried in
    ~927 characters that are identical on both sides.

    Both are **lower bounds** on the true common content (a full character LCS
    is quadratic and not worth its cost here), and ``char_common_chars`` is
    always the tighter of the two. Neither is achievable savings: reaching them
    needs boundaries a real splitter can find without reading the other
    document. They bound what refining the taxonomy could win.
    """
    lines_a = a.splitlines(keepends=True)
    lines_b = b.splitlines(keepends=True)
    matcher = SequenceMatcher(None, lines_a, lines_b, autojunk=False)

    line_common = 0
    char_common = 0
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            matched = sum(len(line) for line in lines_a[i1:i2])
            line_common += matched
            char_common += matched
        else:
            char_common += _common_affix_chars(
                "".join(lines_a[i1:i2]), "".join(lines_b[j1:j2])
            )

    return {
        "chars_a": len(a),
        "chars_b": len(b),
        "line_common_chars": line_common,
        "char_common_chars": char_common,
    }


def pairwise_section_redundancy(
    artifacts: Sequence[ArtifactRecord],
    texts: dict[int, dict[str, str]],
    *,
    max_pairs_per_section: int = 8,
) -> list[dict[str, Any]]:
    """Line- vs character-level common content per section name, across listings.

    Only cross-listing pairs are compared, and only where the two sections
    already differ: identical slices are the whole-section reuse number and
    would just dilute this one. The point is to size the redundancy that
    whole-section hashing *misses*.
    """
    by_name: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for artifact in artifacts:
        for section in artifact.sections:
            text = texts.get(artifact.artifact_id, {}).get(section.name)
            if text is not None:
                by_name[_base_name(section.name)].append((artifact.listing_id, text))

    rows: list[dict[str, Any]] = []
    for name in sorted(by_name):
        samples = by_name[name]
        pairs = 0
        totals = {"chars_a": 0, "line_common_chars": 0, "char_common_chars": 0}
        for index_a in range(len(samples)):
            if pairs >= max_pairs_per_section:
                break
            listing_a, text_a = samples[index_a]
            for listing_b, text_b in samples[index_a + 1:]:
                if pairs >= max_pairs_per_section:
                    break
                if listing_a == listing_b or text_a == text_b:
                    continue
                result = common_content(text_a, text_b)
                totals["chars_a"] += result["chars_a"]
                totals["line_common_chars"] += result["line_common_chars"]
                totals["char_common_chars"] += result["char_common_chars"]
                pairs += 1
        if not pairs:
            continue
        base = totals["chars_a"] or 1
        rows.append(
            {
                "section": name,
                "pairs_compared": pairs,
                "mean_chars": totals["chars_a"] // pairs,
                "line_common_pct": round(100.0 * totals["line_common_chars"] / base, 2),
                "char_common_pct": round(100.0 * totals["char_common_chars"] / base, 2),
            }
        )
    rows.sort(key=lambda row: row["mean_chars"], reverse=True)
    return rows


# ── Measurement: granularity bound via content-defined chunking ────────────────

def _gear_table() -> list[int]:
    """Deterministic 256-entry gear table for the rolling hash.

    Fixed seed, so two runs of this audit chunk identically and their numbers
    are comparable.
    """
    import random

    rng = random.Random(0x114)
    return [rng.getrandbits(64) for _ in range(256)]


_GEAR = _gear_table()
_MASK64 = (1 << 64) - 1


def content_defined_chunks(
    data: bytes, *, target: int, min_size: Optional[int] = None,
    max_size: Optional[int] = None,
) -> list[tuple[int, int]]:
    """Split ``data`` at content-defined boundaries (gear-hash CDC).

    Boundaries depend only on a rolling window of the local bytes, so inserting
    or deleting bytes in one region does not shift the boundaries of any other
    region. That is the property whole-section hashing lacks and the reason one
    changed byte currently spoils a 60KB slice.
    """
    min_size = min_size if min_size is not None else max(1, target // 4)
    max_size = max_size if max_size is not None else target * 8
    mask = (1 << max(1, target.bit_length() - 1)) - 1

    chunks: list[tuple[int, int]] = []
    start = 0
    digest = 0
    for index, byte in enumerate(data):
        digest = ((digest << 1) + _GEAR[byte]) & _MASK64
        size = index - start + 1
        if size < min_size:
            continue
        if size >= max_size or (digest & mask) == 0:
            chunks.append((start, index + 1))
            start = index + 1
            digest = 0
    if start < len(data):
        chunks.append((start, len(data)))
    return chunks


def chunk_dedup_bound(
    payloads: Iterable[bytes],
    *,
    target: int,
    object_overhead_bytes: int = DEFAULT_OBJECT_OVERHEAD_BYTES,
) -> dict[str, Any]:
    """Dedup reachable at ``target``-byte granularity, ignoring taxonomy.

    **This is a bound, not a design.** Content-defined chunking finds every
    shared run in the corpus, including ones no anchor could name, and it does
    so by storing thousands of anonymous chunks per document -- which is a
    different system with different failure modes, not a refinement of the
    section taxonomy. Its value here is as the number the taxonomy is graded
    against: if refining ``filler_3`` recovers most of this, the taxonomy is
    good enough; if it recovers a fraction, the whole approach is capped well
    below what the redundancy suggested.

    ``net_saving_pct`` charges each stored chunk ``object_overhead_bytes``,
    because at small targets the raw dedup figure is a mirage that per-object
    overhead spends straight back.
    """
    total = 0
    unique: dict[bytes, int] = {}
    chunk_count = 0
    for payload in payloads:
        total += len(payload)
        for start, end in content_defined_chunks(payload, target=target):
            chunk = payload[start:end]
            chunk_count += 1
            unique.setdefault(_digest(chunk), end - start)

    stored = sum(unique.values())
    overhead = len(unique) * object_overhead_bytes
    base = total or 1
    return {
        "target_bytes": target,
        "total_bytes": total,
        "chunks": chunk_count,
        "unique_chunks": len(unique),
        "mean_chunk_bytes": (total // chunk_count) if chunk_count else 0,
        "stored_bytes": stored,
        "gross_saving_pct": round(100.0 * (total - stored) / base, 4),
        "overhead_bytes": overhead,
        "net_saving_pct": round(100.0 * (total - stored - overhead) / base, 4),
    }


def _digest(data: bytes) -> bytes:
    import hashlib

    return hashlib.sha256(data).digest()


# ── Measurement: storage accounting, compressed ───────────────────────────────

def _compressor(level: int) -> Callable[[bytes], bytes]:
    import zstandard as zstd

    return zstd.ZstdCompressor(level=level).compress


def dictionary_baseline(
    documents: Sequence[bytes],
    *,
    zstd_level: int = DEFAULT_ZSTD_LEVEL,
    dict_size: int = DEFAULT_DICT_SIZE_BYTES,
) -> Optional[dict[str, Any]]:
    """Compress each document against a dictionary trained on the sample.

    This is the baseline sectioning actually has to beat, and it is a harder
    bar than today's per-object zstd. Every page is currently its own
    independent zstd frame (``shared/minio.py:write_html``), so the shared page
    shell is re-encoded from scratch in all ~3.9M objects. A trained dictionary
    targets exactly that redundancy while keeping each artifact independently
    decompressable -- no manifests, no section objects, no reconstruction step.

    It matters for gate honesty: the six byte-identical ``script_*`` config
    blocks that dedup corpus-wide "for free" are redundancy a dictionary also
    captures. Scoring sectioning against naive per-object zstd would credit it
    with a win a far cheaper change already delivers.

    The two are complementary, not competing -- a dictionary attacks
    boilerplate shared across *different* listings, sectioning attacks reuse
    across *repeated captures of the same* listing -- which is exactly why both
    numbers belong in the report.

    The dictionary is trained on **half** the sample and measured on the other
    half. Training and measuring on the same documents overstates the win --
    the dictionary memorises the very pages it is then scored against -- and
    since this number is the bar sectioning has to clear, overstating it would
    bias the audit *against* sectioning just as surely as understating it would
    flatter it. The held-out saving is then projected onto the full sample so
    it shares a denominator with the other storage figures.

    Returns ``None`` if there is too little sample to split, since a dictionary
    trained on a handful of documents says nothing about the corpus.
    """
    import zstandard as zstd

    if len(documents) < 8:
        return None

    train = list(documents[::2])
    held_out = list(documents[1::2])

    try:
        trained = zstd.train_dictionary(dict_size, train)
    except Exception as exc:  # noqa: BLE001 - an untrainable sample is a result.
        LOG.warning("Dictionary training failed, skipping the baseline: %s", exc)
        return None

    with_dict = zstd.ZstdCompressor(level=zstd_level, dict_data=trained).compress
    without_dict = zstd.ZstdCompressor(level=zstd_level).compress

    held_out_with = sum(len(with_dict(document)) for document in held_out)
    held_out_without = sum(len(without_dict(document)) for document in held_out)
    ratio = held_out_with / (held_out_without or 1)

    return {
        "documents_trained_on": len(train),
        "documents_held_out": len(held_out),
        "dictionary_bytes": len(trained.as_bytes()),
        "held_out_plain_bytes": held_out_without,
        "held_out_with_dictionary_bytes": held_out_with,
        # Projected onto the whole sample using the held-out ratio, so it is
        # comparable with baseline_compressed_bytes.
        "compressed_bytes": round(
            sum(len(without_dict(document)) for document in documents) * ratio
        ),
    }


@dataclass
class StorageReport:
    artifacts: int
    stored_compressed_bytes: int
    baseline_compressed_bytes: int
    sectioned_no_dedup_compressed_bytes: int
    section_store_compressed_bytes: int
    manifest_compressed_bytes: int
    raw_chars: int
    unique_section_chars: int
    parser_critical_chars: int
    unique_sections: int
    object_overhead_bytes: int
    zstd_level: int
    dictionary: Optional[dict[str, Any]] = None

    def as_dict(self) -> dict[str, Any]:
        baseline = self.baseline_compressed_bytes or 1
        overhead = self.unique_sections * self.object_overhead_bytes
        # Manifests are costed at content size only, on the assumption they are
        # batched (one object per source per day, as flush_silver_observations
        # already does) rather than stored one-per-artifact. Stored
        # individually they would cost `manifest_objects_unbatched` * 8 KB --
        # ~800% overhead on their own content -- which is why the plan doc says
        # not to.
        sectioned = (
            self.section_store_compressed_bytes + self.manifest_compressed_bytes + overhead
        )
        result = {
            "zstd_level": self.zstd_level,
            "stored_compressed_bytes_today": self.stored_compressed_bytes,
            "baseline_compressed_bytes": self.baseline_compressed_bytes,
            "sectioned_no_dedup_compressed_bytes": self.sectioned_no_dedup_compressed_bytes,
            "section_store_compressed_bytes": self.section_store_compressed_bytes,
            "manifest_compressed_bytes": self.manifest_compressed_bytes,
            "object_overhead_bytes_each": self.object_overhead_bytes,
            "object_overhead_bytes_total": overhead,
            "sectioned_total_compressed_bytes": sectioned,
            "compressed_saving_pct": round(100.0 * (baseline - sectioned) / baseline, 4),
            "uncompressed_saving_pct": round(
                100.0 * (self.raw_chars - self.unique_section_chars) / (self.raw_chars or 1), 4
            ),
            # What splitting costs before dedup pays anything back: the same
            # bytes, compressed per-section instead of per-document.
            "per_section_compression_penalty_pct": round(
                100.0
                * (self.sectioned_no_dedup_compressed_bytes - self.baseline_compressed_bytes)
                / baseline,
                4,
            ),
            "parser_input_projection_pct_of_raw": round(
                100.0 * self.parser_critical_chars / (self.raw_chars or 1), 4
            ),
            # Object count is a cost in its own right, and inodes run out
            # before bytes do on this backend.
            "objects": {
                "baseline_objects": self.artifacts,
                "section_objects": self.unique_sections,
                "section_objects_per_artifact": round(
                    self.unique_sections / (self.artifacts or 1), 3
                ),
                "manifest_objects_unbatched": self.artifacts,
                "manifest_unbatched_overhead_bytes": self.artifacts * self.object_overhead_bytes,
                "baseline_inodes": round(self.artifacts * INODES_PER_OBJECT),
                "section_store_inodes": round(self.unique_sections * INODES_PER_OBJECT),
            },
        }

        if self.dictionary:
            dictionary_bytes = self.dictionary["compressed_bytes"]
            result["dictionary_baseline"] = {
                **self.dictionary,
                "saving_vs_plain_zstd_pct": round(
                    100.0 * (baseline - dictionary_bytes) / baseline, 4
                ),
                "sectioned_saving_vs_dictionary_pct": round(
                    100.0 * (dictionary_bytes - sectioned) / (dictionary_bytes or 1), 4
                ),
            }
        return result


def compute_storage(
    artifacts: Sequence[ArtifactRecord],
    texts: dict[int, dict[str, str]],
    *,
    zstd_level: int = DEFAULT_ZSTD_LEVEL,
    object_overhead_bytes: int = DEFAULT_OBJECT_OVERHEAD_BYTES,
) -> StorageReport:
    """Cost the sectioned layout against the layout it would replace.

    Two traps this deliberately avoids:

    * **Comparing against what is stored today.** Existing objects were written
      at zstd level 3; costing section objects at level 9 against them would
      credit sectioning with savings that a plain recompression pass (Plan 116)
      already gets on its own. So the baseline is the same documents
      recompressed at ``zstd_level`` -- the level the section store is costed
      at. ``stored_compressed_bytes_today`` is reported alongside as context,
      never as the denominator.
    * **Comparing uncompressed character counts.** zstd exploits redundancy
      across a whole document, so slicing a page into ~20 separately-compressed
      objects throws away context the current single-object layout gets free.
      That penalty is real, is reported as
      ``per_section_compression_penalty_pct``, and is invisible in a
      character-count comparison.
    """
    compress = _compressor(zstd_level)

    unique_sections: dict[str, str] = {}
    documents: list[bytes] = []
    baseline = 0
    sectioned_no_dedup = 0
    manifests = 0
    for artifact in artifacts:
        artifact_texts = texts.get(artifact.artifact_id, {})
        document = "".join(
            artifact_texts.get(section.name, "") for section in artifact.sections
        ).encode("utf-8")
        documents.append(document)
        baseline += len(compress(document))
        manifests += len(compress(artifact.manifest_json.encode("utf-8")))
        for section in artifact.sections:
            text = artifact_texts.get(section.name)
            if text is None:
                continue
            encoded = text.encode("utf-8")
            sectioned_no_dedup += len(compress(encoded))
            unique_sections.setdefault(section.sha256, text)

    section_store = sum(
        len(compress(text.encode("utf-8"))) for text in unique_sections.values()
    )
    return StorageReport(
        artifacts=len(artifacts),
        dictionary=dictionary_baseline(documents, zstd_level=zstd_level),
        stored_compressed_bytes=sum(a.stored_compressed_bytes for a in artifacts),
        baseline_compressed_bytes=baseline,
        sectioned_no_dedup_compressed_bytes=sectioned_no_dedup,
        section_store_compressed_bytes=section_store,
        manifest_compressed_bytes=manifests,
        raw_chars=sum(a.raw_chars for a in artifacts),
        unique_section_chars=sum(len(text) for text in unique_sections.values()),
        parser_critical_chars=sum(a.parser_critical_chars for a in artifacts),
        unique_sections=len(unique_sections),
        object_overhead_bytes=object_overhead_bytes,
        zstd_level=zstd_level,
    )


# ── Fetch + section one artifact ──────────────────────────────────────────────

def _compressed_size(minio_path: str) -> int:
    """Stored object size, from metadata. Mirrors shared.minio.object_exists."""
    from shared.minio import _split_s3_path, get_boto3_client

    bucket, key = _split_s3_path(minio_path)
    response = get_boto3_client().head_object(Bucket=bucket, Key=key)
    return int(response["ContentLength"])


def section_artifact(
    row: dict[str, Any],
    html: str,
    compressed_bytes: int,
    *,
    verify_parse: bool = True,
    decode_was_lossy: bool = False,
) -> tuple[ArtifactRecord, dict[str, str]]:
    """Section one artifact and run both reconstruction gates.

    Byte equality is the real gate and it is free. The parser gate is run
    anyway because it is what the plan's Audit Algorithm step 9 specifies, and
    because running the parser at all tells us the sampled artifact is a
    parseable detail page rather than a challenge page -- which is itself an
    audit result, since parse failures must keep their full raw HTML.
    """
    sections = extract_sections(html)
    records = tuple(
        SectionRecord(
            name=section.name,
            sha256=section_sha256(section.text),
            chars=section.length,
            nbytes=len(section.text.encode("utf-8")),
        )
        for section in sections
    )

    rebuilt = reconstruct(sections)
    reconstructed_exactly = rebuilt == html

    parser_equivalent: Optional[bool] = None
    differences: tuple[str, ...] = ()
    if verify_parse:
        equivalent, diffs = parse_outputs_equivalent(html, rebuilt)
        parser_equivalent = equivalent
        differences = tuple(diffs[:5])

    manifest = build_manifest(
        sections,
        artifact_id=_as_int(row.get("artifact_id")),
        listing_id=str(row.get("listing_id")),
        source_minio_path=str(row.get("minio_path")),
        source_raw_sha256=section_sha256(html),
        parser_equivalent_verified=bool(reconstructed_exactly and parser_equivalent is not False),
        verified_at=str(row.get("fetched_at")),
    )

    record = ArtifactRecord(
        artifact_id=_as_int(row.get("artifact_id")) or 0,
        listing_id=str(row.get("listing_id")),
        group_key=f"{row.get('listing_id')}::{row.get('parsed_fingerprint')}",
        minio_path=str(row.get("minio_path")),
        fetched_at=str(row.get("fetched_at")),
        raw_chars=len(html),
        raw_bytes=len(html.encode("utf-8")),
        stored_compressed_bytes=compressed_bytes,
        manifest_json=serialize_manifest(manifest),
        sections=records,
        reconstructed_exactly=reconstructed_exactly,
        parser_equivalent=parser_equivalent,
        parse_differences=differences,
        decode_was_lossy=decode_was_lossy,
    )
    return record, {section.name: section.text for section in sections}


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def collect_artifacts(
    rows: Sequence[dict[str, Any]],
    totals: Totals,
    *,
    max_artifacts: int = 0,
    verify_parse: bool = True,
    progress_every: int = 10,
) -> tuple[list[ArtifactRecord], dict[int, dict[str, str]]]:
    """Fetch, decompress and section every sampled artifact."""
    from shared.minio import read_html

    artifacts: list[ArtifactRecord] = []
    texts: dict[int, dict[str, str]] = {}

    for row in rows:
        if max_artifacts and len(artifacts) >= max_artifacts:
            break
        totals.sampled_rows += 1
        minio_path = row.get("minio_path")
        artifact_id = _as_int(row.get("artifact_id"))
        if not minio_path:
            totals.skipped_no_path += 1
            continue
        try:
            payload = read_html(str(minio_path))
        except Exception as exc:  # noqa: BLE001 - one bad object must not end the audit.
            totals.failures.append(Failure(artifact_id, str(minio_path), "fetch", str(exc)))
            continue

        try:
            compressed_bytes = _compressed_size(str(minio_path))
        except Exception as exc:  # noqa: BLE001 - context only; not worth losing an artifact.
            LOG.debug("head_object failed for %s: %s", minio_path, exc)
            compressed_bytes = 0

        html = payload.decode("utf-8", errors="replace")
        decode_was_lossy = html.encode("utf-8") != payload

        try:
            record, section_texts = section_artifact(
                row, html, compressed_bytes,
                verify_parse=verify_parse, decode_was_lossy=decode_was_lossy,
            )
        except Exception as exc:  # noqa: BLE001 - a section failure is a result, not a crash.
            totals.failures.append(Failure(artifact_id, str(minio_path), "section", str(exc)))
            continue

        artifacts.append(record)
        texts[record.artifact_id] = section_texts
        totals.fetched += 1
        if progress_every and totals.fetched % progress_every == 0:
            LOG.info(
                "PROGRESS | fetched=%d listings=%d failures=%d",
                totals.fetched,
                len({a.listing_id for a in artifacts}),
                len(totals.failures),
            )

    return artifacts, texts


# ── Report ────────────────────────────────────────────────────────────────────

def build_report(
    artifacts: Sequence[ArtifactRecord],
    texts: dict[int, dict[str, str]],
    totals: Totals,
    args: argparse.Namespace,
) -> dict[str, Any]:
    reuse = compute_reuse(artifacts)
    storage = compute_storage(
        artifacts, texts,
        zstd_level=args.zstd_level,
        object_overhead_bytes=args.object_overhead_bytes,
    )
    chunk_bounds = []
    chunk_artifacts = 0
    if not args.no_chunk_bound:
        # The rolling hash is a pure-Python byte loop, so it is capped: dedup
        # rate on a subsample is a fair relative signal, and the alternative is
        # a measurement nobody waits for.
        subsample = artifacts[: args.chunk_sample] if args.chunk_sample else artifacts
        payloads = [
            "".join(texts[a.artifact_id][s.name] for s in a.sections).encode("utf-8")
            for a in subsample
            if a.artifact_id in texts
        ]
        chunk_artifacts = len(payloads)
        for target in args.chunk_targets:
            LOG.info("Chunking %d artifacts at target=%dB...", chunk_artifacts, target)
            chunk_bounds.append(
                chunk_dedup_bound(
                    payloads, target=target,
                    object_overhead_bytes=args.object_overhead_bytes,
                )
            )

    failed_reconstruction = [a.artifact_id for a in artifacts if not a.reconstructed_exactly]
    failed_equivalence = [a.artifact_id for a in artifacts if a.parser_equivalent is False]

    return {
        "sample": {
            "groups_requested": args.groups,
            "artifacts_per_group": args.artifacts_per_group,
            "rows_returned": totals.sampled_rows,
            "artifacts_measured": totals.fetched,
            "skipped_no_minio_path": totals.skipped_no_path,
            "failures": [vars(f) for f in totals.failures],
            "lossy_utf8_decodes": sum(1 for a in artifacts if a.decode_was_lossy),
            "bias": (
                "Groups are the highest-duplicate-count semantic groups, so "
                "within_listing_saving_pct is an upper bound for the corpus; "
                "cross_listing_saving_pct is largely unaffected."
            ),
        },
        "gates": {
            "byte_identical_reconstruction_pct": round(
                100.0 * (len(artifacts) - len(failed_reconstruction)) / (len(artifacts) or 1), 4
            ),
            "parser_equivalent_pct": round(
                100.0 * (len(artifacts) - len(failed_equivalence)) / (len(artifacts) or 1), 4
            ),
            "failed_reconstruction_artifact_ids": failed_reconstruction[:20],
            "failed_equivalence_artifact_ids": failed_equivalence[:20],
            "first_parse_differences": [
                {"artifact_id": a.artifact_id, "differences": list(a.parse_differences)}
                for a in artifacts
                if a.parser_equivalent is False
            ][:5],
        },
        "reuse": reuse.as_dict(),
        "storage": storage.as_dict(),
        "section_stability": section_name_stats(artifacts),
        "sub_section_redundancy": pairwise_section_redundancy(
            artifacts, texts, max_pairs_per_section=args.max_pairs_per_section
        ),
        "granularity_bound": {
            "artifacts_chunked": chunk_artifacts,
            "targets": chunk_bounds,
        },
    }


def _fmt_bytes(value: int) -> str:
    if value >= 1024 ** 3:
        return f"{value / 1024 ** 3:.2f} GiB"
    if value >= 1024 ** 2:
        return f"{value / 1024 ** 2:.1f} MiB"
    if value >= 1024:
        return f"{value / 1024:.1f} KiB"
    return f"{value} B"


def print_report(report: dict[str, Any]) -> None:
    sample = report["sample"]
    gates = report["gates"]
    reuse = report["reuse"]
    storage = report["storage"]

    lines = [
        "",
        "=== Plan 114 Stage 3: Sectioned HTML Storage Audit ===",
        f"Artifacts measured:   {sample['artifacts_measured']:>8,}"
        f"  ({reuse['listings']} listings, {reuse['semantic_duplicate_groups']} groups)",
        f"Fetch/section failures:{len(sample['failures']):>8,}",
        f"Lossy utf-8 decodes:  {sample['lossy_utf8_decodes']:>8,}",
        "",
        "--- Gates ---",
        f"Byte-identical reconstruction: {gates['byte_identical_reconstruction_pct']:>8.2f}%",
        f"Parser-equivalent:             {gates['parser_equivalent_pct']:>8.2f}%",
    ]
    if gates["failed_reconstruction_artifact_ids"]:
        lines.append(f"  FAILED reconstruction: {gates['failed_reconstruction_artifact_ids']}")
    if gates["first_parse_differences"]:
        lines.append(f"  FAILED equivalence: {gates['first_parse_differences']}")

    lines += [
        "",
        "--- Whole-section reuse (share of sampled raw chars) ---",
        f"Within a duplicate group: {reuse['within_group_saving_pct']:>8.2f}%",
        f"Within a listing:         {reuse['within_listing_saving_pct']:>8.2f}%   (upper bound)",
        f"Across listings (extra):  {reuse['cross_listing_saving_pct']:>8.2f}%",
        f"Total:                    {reuse['total_saving_pct']:>8.2f}%",
        "",
        f"--- Storage at zstd level {storage['zstd_level']} (the comparison that decides) ---",
        f"Stored today (level 3):   {_fmt_bytes(storage['stored_compressed_bytes_today']):>12}"
        "  [context only]",
        f"Baseline full raw:        {_fmt_bytes(storage['baseline_compressed_bytes']):>12}",
        f"Section store (deduped):  {_fmt_bytes(storage['section_store_compressed_bytes']):>12}",
        f"Manifests:                {_fmt_bytes(storage['manifest_compressed_bytes']):>12}",
        f"Per-object overhead:      {_fmt_bytes(storage['object_overhead_bytes_total']):>12}",
        f"Sectioned total:          {_fmt_bytes(storage['sectioned_total_compressed_bytes']):>12}",
        f"Compressed saving:        {storage['compressed_saving_pct']:>11.2f}%",
        f"  uncompressed chars:     {storage['uncompressed_saving_pct']:>11.2f}%  (flattering)",
        f"  split penalty pre-dedup:{storage['per_section_compression_penalty_pct']:>11.2f}%",
        f"Parse-input projection:   {storage['parser_input_projection_pct_of_raw']:>11.2f}%"
        " of raw chars  [measurement only]",
    ]

    dictionary = storage.get("dictionary_baseline")
    if dictionary:
        lines += [
            "",
            "--- vs trained-dictionary baseline (the bar that matters) ---",
            f"Dictionary baseline:      {_fmt_bytes(dictionary['compressed_bytes']):>12}"
            f"  (dict {_fmt_bytes(dictionary['dictionary_bytes'])},"
            f" trained on {dictionary['documents_trained_on']},"
            f" scored on {dictionary['documents_held_out']} held out)",
            f"Dictionary alone saves:   {dictionary['saving_vs_plain_zstd_pct']:>11.2f}%"
            "  vs plain per-object zstd",
            f"Sectioning then adds:     {dictionary['sectioned_saving_vs_dictionary_pct']:>11.2f}%"
            "  on top of the dictionary",
        ]
    else:
        lines.append("  (dictionary baseline skipped: sample too small to train on)")

    objects = storage["objects"]
    lines += [
        "",
        "--- Object count and inodes (a cost bytes-only models miss) ---",
        f"Baseline objects:         {objects['baseline_objects']:>12,}"
        f"   inodes {objects['baseline_inodes']:,}",
        f"Section objects:          {objects['section_objects']:>12,}"
        f"   inodes {objects['section_store_inodes']:,}"
        f"  ({objects['section_objects_per_artifact']} per artifact)",
        f"Manifests if unbatched:   "
        f"{_fmt_bytes(objects['manifest_unbatched_overhead_bytes']):>12}"
        "   of padding alone -- batch them",
        "",
        "--- Section stability (re-test of the volatile hypothesis) ---",
    ]
    for row in sorted(
        report["section_stability"], key=lambda r: r["total_chars"], reverse=True
    )[:20]:
        lines.append(
            f"  {row['section']:<34} {row['median_chars']:>8,}ch  "
            f"hashes={row['distinct_hashes']:<4} listings={row['listings']:<4} "
            f"{row['stability']}"
        )

    if report["sub_section_redundancy"]:
        lines += [
            "",
            "--- Sub-section redundancy across listings (bounds, not savings) ---",
            f"  {'section':<34} {'mean chars':>10} {'line %':>8} {'char %':>8}",
        ]
        for row in report["sub_section_redundancy"][:12]:
            lines.append(
                f"  {row['section']:<34} {row['mean_chars']:>10,} "
                f"{row['line_common_pct']:>8.1f} {row['char_common_pct']:>8.1f}"
            )

    if report["granularity_bound"]["targets"]:
        chunked = report["granularity_bound"]["artifacts_chunked"]
        lines += [
            "",
            "--- Granularity bound: content-defined chunking (a bound, not a design) ---",
            f"    over {chunked:,} artifacts",
            f"  {'target':>8} {'chunks':>10} {'unique':>10} {'gross %':>9} {'net %':>9}",
        ]
        for row in report["granularity_bound"]["targets"]:
            lines.append(
                f"  {row['target_bytes']:>8,} {row['chunks']:>10,} {row['unique_chunks']:>10,} "
                f"{row['gross_saving_pct']:>9.2f} {row['net_saving_pct']:>9.2f}"
            )

    lines += ["", sample["bias"], ""]
    print("\n".join(lines))


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Plan 114 Stage 3: measure section-level reuse, sub-section "
            "redundancy and sectioned storage cost over a real MinIO sample. "
            "Read-only: never writes to or deletes from MinIO."
        )
    )
    parser.add_argument("--groups", type=int, default=8,
                        help="Semantic duplicate groups to sample.")
    parser.add_argument("--artifacts-per-group", type=int, default=4,
                        help="Artifacts sampled per group.")
    parser.add_argument("--source-pattern", default="%detail%",
                        help="SQL ILIKE pattern for detail-source observations.")
    parser.add_argument("--max-artifacts", type=int, default=0,
                        help="Hard cap on artifacts fetched (0 = no cap).")
    parser.add_argument("--skip-parse", action="store_true",
                        help="Skip the parser equivalence gate (byte equality still runs).")
    parser.add_argument("--zstd-level", type=int, default=DEFAULT_ZSTD_LEVEL,
                        help="zstd level used to cost section objects and manifests.")
    parser.add_argument("--object-overhead-bytes", type=int,
                        default=DEFAULT_OBJECT_OVERHEAD_BYTES,
                        help="Per-object storage floor for a stored section/chunk. Default "
                             f"{DEFAULT_OBJECT_OVERHEAD_BYTES} B, measured on the production "
                             "MinIO backend (directory + xl.meta).")
    parser.add_argument("--chunk-targets", type=int, nargs="+", default=list(
        DEFAULT_CHUNK_TARGETS), help="Target chunk sizes for the granularity bound.")
    parser.add_argument("--no-chunk-bound", action="store_true",
                        help="Skip content-defined chunking (the slowest measurement).")
    parser.add_argument("--chunk-sample", type=int, default=40,
                        help="Artifacts included in the chunking bound (0 = all).")
    parser.add_argument("--max-pairs-per-section", type=int, default=8,
                        help="Cross-listing pairs compared per section name.")
    parser.add_argument("--progress-every", type=int, default=10,
                        help="Log a progress line every N artifacts.")
    parser.add_argument("--sample-out", type=Path, default=None,
                        help="Write the DuckDB sample rows to this JSON file.")
    parser.add_argument("--sample-in", type=Path, default=None,
                        help="Read sample rows from JSON instead of querying DuckDB.")
    parser.add_argument("--json-out", type=Path, default=None,
                        help="Write the full report to this JSON file.")
    parser.add_argument("--log-level", default="INFO", help="DEBUG | INFO | WARNING")
    return parser.parse_args(argv)


def load_sample(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Sample rows, either from a cached JSON file or from the lake.

    ``--sample-in`` exists because the DuckDB query scans the whole silver
    observations dataset, which is by far the most expensive part of a run and
    has nothing to do with the measurements being iterated on.
    """
    if args.sample_in:
        rows = json.loads(Path(args.sample_in).read_text(encoding="utf-8"))
        LOG.info("Loaded %d sample rows from %s", len(rows), args.sample_in)
        return rows

    from scripts.oneoff.audit_semantic_duplicate_html_hashes import connect_duckdb, fetch_sample

    LOG.info("Querying the lake for %d duplicate groups...", args.groups)
    rows = fetch_sample(connect_duckdb(), args)
    LOG.info("Sample: %d rows", len(rows))
    if args.sample_out:
        Path(args.sample_out).write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
        LOG.info("Wrote sample rows to %s", args.sample_out)
    return rows


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    rows = load_sample(args)
    if not rows:
        print("No semantic duplicate groups found; nothing to audit.")
        return 1

    totals = Totals()
    artifacts, texts = collect_artifacts(
        rows, totals,
        max_artifacts=args.max_artifacts,
        verify_parse=not args.skip_parse,
        progress_every=args.progress_every,
    )
    if not artifacts:
        print("No artifacts could be fetched and sectioned.")
        for failure in totals.failures[:10]:
            print(f"  {failure.stage}: {failure.minio_path}: {failure.error}")
        return 1

    report = build_report(artifacts, texts, totals, args)
    print_report(report)

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        LOG.info("Wrote report to %s", args.json_out)

    # A reconstruction miss is an extractor bug, not a finding: fail loudly.
    return 0 if report["gates"]["byte_identical_reconstruction_pct"] == 100.0 else 2


if __name__ == "__main__":
    sys.exit(main())
