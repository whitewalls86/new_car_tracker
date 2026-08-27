# ruff: noqa: E501
import hashlib

from scripts.build_april_ledger import build_ledger, canonical_fingerprint


def test_ledger_joins_only_on_recomputed_content_hash_and_is_deterministic():
    content = b"detail html"
    digest = hashlib.sha256(content).hexdigest()
    legacy = [
        {
            "legacy_key": "z",
            "row_group": 0,
            "row_offset": 2,
            "artifact_id": 99,
            "content_bytes": content,
            "sha256": digest,
            "http_status": 200,
            "orphan": True,
        },
        {
            "legacy_key": "a",
            "row_group": 0,
            "row_offset": 1,
            "artifact_id": 99,
            "content_bytes": b"wrong",
            "sha256": digest,
            "http_status": 200,
        },
    ]
    sidecars = [
        {
            "raw_sha256": digest,
            "source_key": "html/source",
            "pack_key": "pack.zpack",
            "sidecar_key": "pack.idx.parquet",
        }
    ]

    rows = build_ledger(legacy, sidecars)

    assert [row["legacy_key"] for row in rows] == ["a", "z"]
    assert rows[1]["source_key"] == "html/source"
    assert rows[1]["disposition"] == "recover_car19"
    assert rows[0]["disposition"] == "unresolved"
    assert canonical_fingerprint(rows) == canonical_fingerprint(list(rows))
