"""Unit tests for scripts/audit_sectioned_html_storage.py (Plan 114, Stage 3).

The audit's job is to produce numbers a retention decision will be made from,
so these tests are mostly about the measurements being *honest* rather than
merely running:

  A - section name normalization
  B - reuse split by scope (within group / within listing / across listings)
  C - section stability labels (re-test of the volatile hypothesis)
  D - common_content: line vs character granularity
  E - content-defined chunking: the boundary-stability property that matters
  F - storage accounting: the two ways this measurement could flatter itself
  G - per-artifact sectioning and its gates
  H - fetch loop: failures are results, and the audit never writes
  I - CLI defaults
"""
from __future__ import annotations

import argparse
import gzip
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.audit_sectioned_html_storage import (
    DEFAULT_OBJECT_OVERHEAD_BYTES,
    INODES_PER_OBJECT,
    ArtifactRecord,
    SectionRecord,
    Totals,
    _base_name,
    build_report,
    chunk_dedup_bound,
    collect_artifacts,
    common_content,
    compute_reuse,
    compute_storage,
    content_defined_chunks,
    dictionary_baseline,
    pairwise_section_redundancy,
    parse_args,
    section_artifact,
    section_name_stats,
)

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "html"


# ── Shared helpers ────────────────────────────────────────────────────────────

def _section(name: str, sha: str, chars: int = 100) -> SectionRecord:
    return SectionRecord(name=name, sha256=sha, chars=chars, nbytes=chars)


def _artifact(
    artifact_id: int,
    listing_id: str,
    sections: list[SectionRecord],
    *,
    group: str | None = None,
    stored_compressed_bytes: int = 0,
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        listing_id=listing_id,
        group_key=group or f"{listing_id}::f1",
        minio_path=f"s3://bronze/html/{artifact_id}.html.zst",
        fetched_at="2026-08-01T00:00:00Z",
        raw_chars=sum(s.chars for s in sections),
        raw_bytes=sum(s.nbytes for s in sections),
        stored_compressed_bytes=stored_compressed_bytes,
        manifest_json="{}",
        sections=tuple(sections),
        reconstructed_exactly=True,
        parser_equivalent=True,
    )


def _texts(artifact: ArtifactRecord, filler: str = "x") -> dict[str, str]:
    """Section texts consistent with an artifact's recorded hashes and sizes."""
    return {s.name: (s.sha256 * s.chars)[: s.chars] for s in artifact.sections}


def _load_fixture(name: str) -> str:
    raw = gzip.decompress((_FIXTURE_DIR / f"{name}.html.gz").read_bytes())
    return raw.decode("utf-8", errors="replace")


def _row(artifact_id: int = 1, listing_id: str = "L1") -> dict:
    return {
        "artifact_id": artifact_id,
        "listing_id": listing_id,
        "parsed_fingerprint": "fp",
        "minio_path": f"s3://bronze/html/{artifact_id}.html.zst",
        "fetched_at": "2026-08-01T00:00:00Z",
    }


def _audit_args(**overrides) -> argparse.Namespace:
    defaults = dict(
        groups=2,
        artifacts_per_group=2,
        zstd_level=9,
        object_overhead_bytes=0,
        chunk_targets=[1024],
        no_chunk_bound=True,
        chunk_sample=0,
        max_pairs_per_section=4,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


# ── Group A: section name normalization ───────────────────────────────────────

class TestBaseName:
    def test_plain_name_is_unchanged(self):
        assert _base_name("filler_3") == "filler_3"

    def test_dedup_suffix_is_stripped(self):
        assert _base_name("filler_3__2") == "filler_3"

    def test_non_numeric_double_underscore_is_not_a_suffix(self):
        assert _base_name("script_some__thing") == "script_some__thing"

    def test_two_artifacts_disagreeing_on_repeat_count_share_statistics(self):
        """The reason _base_name exists: one document repeating a section must
        not split that section's stats across two rows."""
        a = _artifact(1, "L1", [_section("filler_3", "aa"), _section("filler_3__2", "bb")])
        b = _artifact(2, "L2", [_section("filler_3", "cc")])
        names = {row["section"] for row in section_name_stats([a, b])}
        assert names == {"filler_3"}


# ── Group B: reuse split by scope ─────────────────────────────────────────────

class TestComputeReuse:
    def test_identical_captures_of_one_listing_dedup_within_the_group(self):
        sections = [_section("a", "sha_a", 100), _section("b", "sha_b", 100)]
        report = compute_reuse([_artifact(1, "L1", sections), _artifact(2, "L1", sections)])
        result = report.as_dict()
        assert result["within_group_saving_pct"] == pytest.approx(50.0)
        assert result["cross_listing_saving_pct"] == pytest.approx(0.0)

    def test_cross_listing_sharing_is_reported_separately(self):
        shared = _section("shell", "sha_shell", 100)
        a = _artifact(1, "L1", [shared, _section("body", "sha_a", 100)])
        b = _artifact(2, "L2", [shared, _section("body", "sha_b", 100)])
        result = compute_reuse([a, b]).as_dict()

        # Nothing repeats inside either listing, so all of the saving is cross.
        assert result["within_listing_saving_pct"] == pytest.approx(0.0)
        assert result["cross_listing_saving_pct"] == pytest.approx(25.0)
        assert result["total_saving_pct"] == pytest.approx(25.0)

    def test_scope_shares_sum_to_the_total(self):
        """The three scopes are shares of one raw total, and the report is only
        readable if they actually add up."""
        shared = _section("shell", "sha_shell", 100)
        a = _artifact(1, "L1", [shared, _section("body", "sha_a", 100)])
        b = _artifact(2, "L1", [shared, _section("body", "sha_a", 100)])
        c = _artifact(3, "L2", [shared, _section("body", "sha_c", 100)])
        result = compute_reuse([a, b, c]).as_dict()

        assert result["within_listing_saving_pct"] + result["cross_listing_saving_pct"] == (
            pytest.approx(result["total_saving_pct"])
        )
        assert result["within_group_saving_pct"] <= result["within_listing_saving_pct"]

    def test_within_group_never_exceeds_within_listing(self):
        shared = _section("shell", "sha_shell", 100)
        a = _artifact(1, "L1", [shared], group="L1::f1")
        b = _artifact(2, "L1", [shared], group="L1::f2")
        result = compute_reuse([a, b]).as_dict()

        # Same listing, different semantic groups: the listing dedups, the
        # groups cannot.
        assert result["within_group_saving_pct"] == pytest.approx(0.0)
        assert result["within_listing_saving_pct"] == pytest.approx(50.0)

    def test_empty_sample_does_not_divide_by_zero(self):
        assert compute_reuse([]).as_dict()["total_saving_pct"] == 0.0


# ── Group C: section stability labels ─────────────────────────────────────────

class TestSectionNameStats:
    def test_one_hash_everywhere_is_identical_corpus_wide(self):
        a = _artifact(1, "L1", [_section("script_datadog_config", "sha_x")])
        b = _artifact(2, "L2", [_section("script_datadog_config", "sha_x")])
        row = section_name_stats([a, b])[0]
        assert row["stability"] == "identical_corpus_wide"
        assert row["listings"] == 2

    def test_stable_within_listing_but_differing_across_is_labelled_per_listing(self):
        a = _artifact(1, "L1", [_section("dealer_contact_block", "sha_1")])
        b = _artifact(2, "L1", [_section("dealer_contact_block", "sha_1")])
        c = _artifact(3, "L2", [_section("dealer_contact_block", "sha_2")])
        row = section_name_stats([a, b, c])[0]
        assert row["stability"] == "stable_per_listing"

    def test_differing_between_captures_of_one_listing_is_volatile(self):
        a = _artifact(1, "L1", [_section("als_json", "sha_1")])
        b = _artifact(2, "L1", [_section("als_json", "sha_2")])
        row = section_name_stats([a, b])[0]
        assert row["stability"] == "varies_within_listing"

    def test_a_name_seen_in_only_one_artifact_is_not_called_stable(self):
        """cars.com emits inline scripts with random numeric ids, so the
        script[id] anchor mints a fresh name per capture. Scoring those as
        'identical corpus-wide' on a sample of one is exactly backwards."""
        a = _artifact(1, "L1", [_section("script_1033207580", "sha_1")])
        b = _artifact(2, "L2", [_section("script_998877", "sha_2")])
        labels = {row["section"]: row["stability"] for row in section_name_stats([a, b])}
        assert labels == {"script_1033207580": "seen_once", "script_998877": "seen_once"}

    def test_a_section_missing_from_some_artifacts_is_still_counted_once_each(self):
        a = _artifact(1, "L1", [_section("carousel_block", "sha_1")])
        b = _artifact(2, "L1", [_section("document_prefix", "sha_2")])
        rows = {row["section"]: row for row in section_name_stats([a, b])}
        assert rows["carousel_block"]["artifacts_present"] == 1
        assert rows["document_prefix"]["artifacts_present"] == 1


# ── Group D: line vs character granularity ────────────────────────────────────

class TestCommonContent:
    def test_identical_text_is_fully_common_at_both_granularities(self):
        text = "alpha\nbeta\ngamma\n"
        result = common_content(text, text)
        assert result["line_common_chars"] == len(text)
        assert result["char_common_chars"] == len(text)

    def test_char_granularity_beats_line_granularity_on_a_one_token_difference(self):
        """The document_suffix case: a long line differing by a short token is
        ~0% common line-wise and ~99% common character-wise."""
        prefix = "x" * 500
        suffix = "y" * 400
        a = f"{prefix}68d40bf0{suffix}"
        b = f"{prefix}ba4cf7d2{suffix}"

        result = common_content(a, b)
        assert result["line_common_chars"] == 0
        assert result["char_common_chars"] == 900
        assert result["char_common_chars"] / result["chars_a"] > 0.99

    def test_char_common_is_never_below_line_common(self):
        a = "shared\nunique-a-longer\nshared2\n"
        b = "shared\nunique-b\nshared2\n"
        result = common_content(a, b)
        assert result["char_common_chars"] >= result["line_common_chars"] > 0

    def test_disjoint_text_is_not_credited_with_common_content(self):
        result = common_content("aaaa\n", "bbbb\n")
        assert result["line_common_chars"] == 0
        # Only the shared trailing newline.
        assert result["char_common_chars"] <= 1

    def test_real_fixture_pair_reproduces_the_stage_2_measurement(self):
        """document_suffix was the finding that reframed Stage 3; if this drifts,
        the character-level claim in the plan doc is no longer backed."""
        crv = _load_fixture("real_detail_crv")
        other = _load_fixture("real_detail_2")

        rec_a, texts_a = section_artifact(_row(1, "L1"), crv, 0, verify_parse=False)
        rec_b, texts_b = section_artifact(_row(2, "L2"), other, 0, verify_parse=False)

        result = common_content(texts_a["document_suffix"], texts_b["document_suffix"])
        line_pct = 100.0 * result["line_common_chars"] / result["chars_a"]
        char_pct = 100.0 * result["char_common_chars"] / result["chars_a"]
        assert line_pct == pytest.approx(19.1, abs=1.0)
        assert char_pct == pytest.approx(99.3, abs=1.0)


class TestPairwiseSectionRedundancy:
    def test_only_cross_listing_pairs_are_compared(self):
        a = _artifact(1, "L1", [_section("filler_3", "sha_a", 10)])
        b = _artifact(2, "L1", [_section("filler_3", "sha_b", 10)])
        texts = {1: {"filler_3": "aaaaaaaaaa"}, 2: {"filler_3": "bbbbbbbbbb"}}
        assert pairwise_section_redundancy([a, b], texts) == []

    def test_identical_sections_are_excluded_as_already_counted(self):
        a = _artifact(1, "L1", [_section("filler_3", "sha_a", 10)])
        b = _artifact(2, "L2", [_section("filler_3", "sha_a", 10)])
        texts = {1: {"filler_3": "aaaaaaaaaa"}, 2: {"filler_3": "aaaaaaaaaa"}}
        assert pairwise_section_redundancy([a, b], texts) == []

    def test_pair_count_is_capped(self):
        artifacts = [
            _artifact(i, f"L{i}", [_section("filler_3", f"sha_{i}", 4)]) for i in range(6)
        ]
        texts = {i: {"filler_3": f"a{i}bc"} for i in range(6)}
        rows = pairwise_section_redundancy(artifacts, texts, max_pairs_per_section=3)
        assert rows[0]["pairs_compared"] == 3


# ── Group E: content-defined chunking ─────────────────────────────────────────

def _entropy(n: int, seed: int = 0) -> bytes:
    """Pseudorandom bytes with a fixed seed.

    Deliberately not an arithmetic sequence like ``(i * 7 + 3) % 251``: that is
    strictly periodic, the rolling hash cycles through the same values forever,
    and every chunk degenerates to ``max_size``. Real HTML is nothing like it
    (the fixtures chunk to ~1.1 KiB at a 1 KiB target), so testing against it
    would only measure the pathological case.
    """
    import random

    return random.Random(seed).randbytes(n)


class TestContentDefinedChunks:
    def test_chunks_tile_the_input_exactly(self):
        chunks = content_defined_chunks(_entropy(10000), target=256)
        assert chunks[0][0] == 0
        assert chunks[-1][1] == 10000
        assert all(a[1] == b[0] for a, b in zip(chunks, chunks[1:]))

    def test_boundaries_are_content_derived_not_positional(self):
        """If boundaries were positional, mean chunk size would pin to max_size."""
        chunks = content_defined_chunks(_entropy(60000), target=1024)
        sizes = [e - s for s, e in chunks]
        assert 512 < (sum(sizes) / len(sizes)) < 2048

    def test_boundaries_survive_an_insertion_elsewhere(self):
        """The property whole-section hashing lacks: one changed byte must not
        invalidate the rest of the document."""
        body = _entropy(20000)
        modified = body[:5000] + b"INSERTED-TOKEN" + body[5000:]

        original = {body[s:e] for s, e in content_defined_chunks(body, target=512)}
        after = {modified[s:e] for s, e in content_defined_chunks(modified, target=512)}

        shared = sum(len(chunk) for chunk in original & after)
        assert shared / len(body) > 0.8

    def test_chunk_sizes_respect_the_configured_bounds(self):
        chunks = content_defined_chunks(
            _entropy(30000), target=1024, min_size=256, max_size=4096
        )
        sizes = [e - s for s, e in chunks]
        assert all(size <= 4096 for size in sizes)
        # Only the final chunk may fall below min_size.
        assert all(size >= 256 for size in sizes[:-1])

    def test_periodic_input_still_terminates_via_max_size(self):
        """Known limitation, pinned rather than hidden: on strictly periodic
        input the hash may never hit a boundary, and max_size is the only thing
        keeping chunks bounded."""
        periodic = bytes((i * 7 + 3) % 251 for i in range(20000))
        chunks = content_defined_chunks(periodic, target=512, max_size=4096)
        assert all(e - s <= 4096 for s, e in chunks)
        assert chunks[-1][1] == len(periodic)

    def test_empty_input_yields_no_chunks(self):
        assert content_defined_chunks(b"", target=1024) == []


class TestChunkDedupBound:
    def test_identical_payloads_dedup_almost_entirely(self):
        payload = _entropy(40000)
        result = chunk_dedup_bound([payload, payload], target=1024, object_overhead_bytes=0)
        assert result["gross_saving_pct"] > 49.0

    def test_per_object_overhead_is_charged_against_the_saving(self):
        payload = _entropy(40000)
        free = chunk_dedup_bound([payload, payload], target=256, object_overhead_bytes=0)
        costed = chunk_dedup_bound([payload, payload], target=256, object_overhead_bytes=8192)
        assert costed["net_saving_pct"] < free["net_saving_pct"]
        assert costed["gross_saving_pct"] == free["gross_saving_pct"]

    def test_finer_targets_produce_more_chunks(self):
        payload = _entropy(40000)
        coarse = chunk_dedup_bound([payload], target=4096)
        fine = chunk_dedup_bound([payload], target=256)
        assert fine["chunks"] > coarse["chunks"]

    def test_unrelated_payloads_show_little_saving(self):
        result = chunk_dedup_bound(
            [_entropy(20000, seed=1), _entropy(20000, seed=2)],
            target=1024,
            object_overhead_bytes=0,
        )
        assert result["gross_saving_pct"] < 10.0

    def test_chunking_is_deterministic_across_calls(self):
        payload = _entropy(20000)
        assert chunk_dedup_bound([payload], target=1024) == chunk_dedup_bound(
            [payload], target=1024
        )


# ── Group F: storage accounting ───────────────────────────────────────────────

class TestComputeStorage:
    def _sample(self):
        section = _section("body", "sha_body", 2000)
        artifact = _artifact(1, "L1", [section], stored_compressed_bytes=999_999)
        return [artifact], {1: _texts(artifact)}

    def test_baseline_is_recompressed_not_what_is_stored_today(self):
        """Objects on disk are level 3. Costing sections at level 9 against them
        would credit sectioning with Plan 116's recompression savings."""
        artifacts, texts = self._sample()
        result = compute_storage(artifacts, texts, zstd_level=9).as_dict()

        assert result["stored_compressed_bytes_today"] == 999_999
        assert result["baseline_compressed_bytes"] != 999_999
        assert result["compressed_saving_pct"] < 100.0

    def test_split_penalty_is_reported_and_positive_when_splitting_hurts(self):
        text = "".join(f"<div>row {i} of repeated markup</div>\n" for i in range(400))
        sections = [_section(f"s{i}", f"sha_{i}", 40) for i in range(20)]
        artifact = _artifact(1, "L1", sections)
        chunk = len(text) // 20
        texts = {1: {f"s{i}": text[i * chunk:(i + 1) * chunk] for i in range(20)}}

        result = compute_storage([artifact], texts, zstd_level=9).as_dict()
        # Compressing 20 slices separately loses the cross-slice context that a
        # single-object layout gets for free.
        assert result["per_section_compression_penalty_pct"] > 0

    def test_uncompressed_saving_is_reported_alongside_and_is_the_larger_number(self):
        shared = _section("shell", "sha_shell", 4000)
        a = _artifact(1, "L1", [shared])
        b = _artifact(2, "L2", [shared])
        texts = {1: _texts(a), 2: _texts(b)}
        result = compute_storage([a, b], texts, zstd_level=9).as_dict()
        assert result["uncompressed_saving_pct"] == pytest.approx(50.0)

    def test_object_overhead_is_charged_per_unique_section(self):
        artifacts, texts = self._sample()
        free = compute_storage(artifacts, texts, object_overhead_bytes=0).as_dict()
        costed = compute_storage(artifacts, texts, object_overhead_bytes=4096).as_dict()
        assert costed["object_overhead_bytes_total"] == 4096
        assert costed["compressed_saving_pct"] < free["compressed_saving_pct"]

    def test_parser_input_projection_counts_only_the_four_critical_sections(self):
        sections = [
            _section("vehicle_activity_json", "sha_1", 100),
            _section("filler_3", "sha_2", 300),
        ]
        artifact = _artifact(1, "L1", sections)
        result = compute_storage([artifact], {1: _texts(artifact)}).as_dict()
        assert result["parser_input_projection_pct_of_raw"] == pytest.approx(25.0)

    def test_object_and_inode_counts_are_reported(self):
        """Object count is a cost: ~8 KB and ~2.24 inodes each, whatever the
        content size. A bytes-only model would miss it."""
        a = _artifact(1, "L1", [_section("x", "sha_1", 100), _section("y", "sha_2", 100)])
        b = _artifact(2, "L2", [_section("x", "sha_1", 100), _section("z", "sha_3", 100)])
        objects = compute_storage([a, b], {1: _texts(a), 2: _texts(b)}).as_dict()["objects"]

        assert objects["baseline_objects"] == 2
        assert objects["section_objects"] == 3  # sha_1 is shared
        assert objects["section_objects_per_artifact"] == 1.5
        assert objects["section_store_inodes"] == round(3 * INODES_PER_OBJECT)

    def test_unbatched_manifest_overhead_is_surfaced_as_a_warning_number(self):
        artifacts, texts = self._sample()
        objects = compute_storage(
            artifacts, texts, object_overhead_bytes=8192
        ).as_dict()["objects"]
        assert objects["manifest_unbatched_overhead_bytes"] == 8192

    def test_default_object_overhead_matches_the_measured_minio_floor(self):
        assert DEFAULT_OBJECT_OVERHEAD_BYTES == 8192

    def test_dictionary_baseline_is_skipped_on_a_sample_too_small_to_train(self):
        artifacts, texts = self._sample()
        assert "dictionary_baseline" not in compute_storage(artifacts, texts).as_dict()

    def test_dictionary_baseline_is_wired_into_the_storage_report(self):
        """Gate honesty: a dictionary captures the shared page shell too, so
        sectioning must be scored against it, not only against naive zstd."""
        artifacts, texts = [], {}
        for index in range(16):
            shell = _section("shell", "sha_shell", 12000)
            body = _section("body", f"sha_body_{index}", 4000)
            artifact = _artifact(index, f"L{index}", [shell, body])
            artifacts.append(artifact)
            texts[index] = _texts(artifact)

        result = compute_storage(artifacts, texts, object_overhead_bytes=0).as_dict()
        dictionary = result["dictionary_baseline"]

        assert dictionary["compressed_bytes"] > 0
        assert "saving_vs_plain_zstd_pct" in dictionary
        assert "sectioned_saving_vs_dictionary_pct" in dictionary


class TestDictionaryBaseline:
    def _documents(self, count: int, size: int = 20000) -> list[bytes]:
        shell = ("<div class='page-shell'>boilerplate every listing carries</div>\n" * 200)
        return [
            (shell + f"<span id='listing-{index}'>{_entropy(size, seed=index).hex()}</span>")
            .encode("utf-8")
            for index in range(count)
        ]

    def test_dictionary_is_scored_on_documents_it_was_not_trained_on(self):
        """Training and scoring on the same documents would overstate the bar
        sectioning has to clear."""
        result = dictionary_baseline(self._documents(16), dict_size=16 * 1024)
        assert result is not None
        assert result["documents_trained_on"] == 8
        assert result["documents_held_out"] == 8
        assert result["held_out_plain_bytes"] > 0

    def test_shared_boilerplate_is_captured_on_held_out_documents(self):
        result = dictionary_baseline(self._documents(16), dict_size=16 * 1024)
        # The page shell is common to every document, so a dictionary trained on
        # half of them must still shrink the other half.
        assert result["held_out_with_dictionary_bytes"] < result["held_out_plain_bytes"]

    def test_too_small_a_sample_returns_none_rather_than_a_meaningless_number(self):
        assert dictionary_baseline(self._documents(4)) is None

    def test_degenerate_sample_does_not_raise(self):
        """zstd will happily train on nearly nothing; the audit must not crash
        on whatever it returns."""
        result = dictionary_baseline([b"tiny"] * 16)
        assert result is None or result["held_out_plain_bytes"] > 0


# ── Group G: per-artifact sectioning and gates ────────────────────────────────

class TestSectionArtifact:
    def test_real_fixture_round_trips_and_passes_the_parser_gate(self):
        html = _load_fixture("real_detail_crv")
        record, texts = section_artifact(_row(), html, 1234)

        assert record.reconstructed_exactly is True
        assert record.parser_equivalent is True
        assert "".join(texts[s.name] for s in record.sections) == html
        assert record.raw_chars == len(html)
        assert record.stored_compressed_bytes == 1234

    def test_skipping_the_parse_leaves_equivalence_unknown_not_false(self):
        html = _load_fixture("real_detail_crv")
        record, _ = section_artifact(_row(), html, 0, verify_parse=False)
        assert record.parser_equivalent is None
        assert record.reconstructed_exactly is True

    def test_challenge_page_still_sections_and_round_trips(self):
        """A Cloudflare challenge has no anchors at all. It must not be a
        failure -- parse failures are an audit result, and the artifact still
        has to reconstruct."""
        html = _load_fixture("challenge_just_a_moment")
        record, _ = section_artifact(_row(), html, 0)
        assert record.reconstructed_exactly is True

    def test_manifest_records_the_source_hash_and_section_order(self):
        import json

        html = _load_fixture("real_detail_crv")
        record, _ = section_artifact(_row(7, "L7"), html, 0, verify_parse=False)
        manifest = json.loads(record.manifest_json)

        assert manifest["artifact_id"] == 7
        assert manifest["listing_id"] == "L7"
        assert [entry["name"] for entry in manifest["sections"]] == [
            s.name for s in record.sections
        ]
        assert all(entry["normalized"] is False for entry in manifest["sections"])

    def test_parser_critical_chars_sums_only_critical_sections(self):
        html = _load_fixture("real_detail_crv")
        record, texts = section_artifact(_row(), html, 0, verify_parse=False)
        expected = sum(
            len(texts[s.name])
            for s in record.sections
            if _base_name(s.name)
            in {
                "vehicle_activity_json",
                "vehicle_controller_json",
                "dealer_contact_block",
                "carousel_block",
            }
        )
        assert record.parser_critical_chars == expected
        assert 0 < record.parser_critical_chars < record.raw_chars


# ── Group H: fetch loop behaviour and the read-only contract ──────────────────

class TestCollectArtifacts:
    def _patched(self, html: str):
        return patch(
            "shared.minio.read_html", return_value=html.encode("utf-8")
        ), patch(
            "scripts.audit_sectioned_html_storage._compressed_size", return_value=5000
        )

    def test_fetches_sections_and_records_each_artifact(self):
        html = _load_fixture("real_detail_crv")
        read, size = self._patched(html)
        totals = Totals()
        with read, size:
            artifacts, texts = collect_artifacts(
                [_row(1, "L1"), _row(2, "L2")], totals, verify_parse=False
            )
        assert len(artifacts) == 2
        assert totals.fetched == 2
        assert set(texts) == {1, 2}

    def test_rows_without_a_minio_path_are_skipped_not_failed(self):
        totals = Totals()
        row = _row()
        row["minio_path"] = None
        artifacts, _ = collect_artifacts([row], totals, verify_parse=False)
        assert artifacts == []
        assert totals.skipped_no_path == 1
        assert totals.failures == []

    def test_a_fetch_error_is_recorded_and_the_audit_continues(self):
        html = _load_fixture("real_detail_crv")
        with patch(
            "shared.minio.read_html",
            side_effect=[RuntimeError("boom"), html.encode("utf-8")],
        ), patch("scripts.audit_sectioned_html_storage._compressed_size", return_value=1):
            totals = Totals()
            artifacts, _ = collect_artifacts(
                [_row(1, "L1"), _row(2, "L2")], totals, verify_parse=False
            )
        assert len(artifacts) == 1
        assert len(totals.failures) == 1
        assert totals.failures[0].stage == "fetch"

    def test_a_head_object_failure_does_not_lose_the_artifact(self):
        """Compressed size is context, not a measurement the audit depends on."""
        html = _load_fixture("real_detail_crv")
        with patch("shared.minio.read_html", return_value=html.encode("utf-8")), patch(
            "scripts.audit_sectioned_html_storage._compressed_size",
            side_effect=RuntimeError("no such key"),
        ):
            totals = Totals()
            artifacts, _ = collect_artifacts([_row()], totals, verify_parse=False)
        assert len(artifacts) == 1
        assert artifacts[0].stored_compressed_bytes == 0
        assert totals.failures == []

    def test_max_artifacts_caps_the_fetch(self):
        html = _load_fixture("real_detail_crv")
        read, size = self._patched(html)
        with read, size:
            artifacts, _ = collect_artifacts(
                [_row(i, f"L{i}") for i in range(5)],
                Totals(),
                max_artifacts=2,
                verify_parse=False,
            )
        assert len(artifacts) == 2

    def test_lossy_utf8_decode_is_flagged(self):
        with patch("shared.minio.read_html", return_value=b"<html>\xff\xfe</html>"), patch(
            "scripts.audit_sectioned_html_storage._compressed_size", return_value=1
        ):
            artifacts, _ = collect_artifacts([_row()], Totals(), verify_parse=False)
        assert artifacts[0].decode_was_lossy is True

    def test_audit_never_writes_to_minio(self):
        """Read-only is a contract, not an intention."""
        html = _load_fixture("real_detail_crv")
        client = MagicMock()
        with patch("shared.minio.read_html", return_value=html.encode("utf-8")), patch(
            "shared.minio.get_boto3_client", return_value=client
        ):
            collect_artifacts([_row()], Totals(), verify_parse=False)

        client.put_object.assert_not_called()
        client.delete_object.assert_not_called()
        client.delete_objects.assert_not_called()
        client.copy_object.assert_not_called()


# ── Group I: report assembly and CLI ──────────────────────────────────────────

class TestBuildReport:
    def test_report_carries_the_gates_reuse_storage_and_stability(self):
        html = _load_fixture("real_detail_crv")
        record, texts = section_artifact(_row(), html, 4000, verify_parse=False)
        report = build_report([record], {record.artifact_id: texts}, Totals(fetched=1),
                              _audit_args())

        assert report["gates"]["byte_identical_reconstruction_pct"] == 100.0
        assert report["reuse"]["artifacts"] == 1
        assert report["storage"]["baseline_compressed_bytes"] > 0
        assert report["section_stability"]
        assert "bias" in report["sample"]

    def test_a_reconstruction_failure_shows_up_in_the_gates(self):
        record = _artifact(1, "L1", [_section("a", "sha_a", 10)])
        broken = ArtifactRecord(**{**vars(record), "reconstructed_exactly": False})
        report = build_report(
            [broken], {1: _texts(broken)}, Totals(fetched=1), _audit_args()
        )
        assert report["gates"]["byte_identical_reconstruction_pct"] == 0.0
        assert report["gates"]["failed_reconstruction_artifact_ids"] == [1]

    def test_chunk_bound_is_skipped_when_disabled(self):
        record = _artifact(1, "L1", [_section("a", "sha_a", 10)])
        report = build_report(
            [record], {1: _texts(record)}, Totals(fetched=1), _audit_args(no_chunk_bound=True)
        )
        assert report["granularity_bound"]["targets"] == []


class TestCli:
    def test_defaults_are_read_only_and_bounded(self):
        args = parse_args([])
        assert args.groups > 0
        assert args.artifacts_per_group > 0
        assert args.chunk_sample > 0
        assert args.skip_parse is False
        assert args.json_out is None

    def test_sample_in_and_sample_out_are_available_for_iterating(self):
        args = parse_args(["--sample-in", "/tmp/s.json", "--sample-out", "/tmp/o.json"])
        # Both are type=Path, so compare as paths — str() renders separators
        # per platform and the assertion is about what the flag parsed to.
        assert args.sample_in == Path("/tmp/s.json")
        assert args.sample_out == Path("/tmp/o.json")

    def test_chunk_targets_accept_multiple_values(self):
        args = parse_args(["--chunk-targets", "256", "1024"])
        assert args.chunk_targets == [256, 1024]
