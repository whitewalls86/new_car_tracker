"""
Plan 112 Gate B: unit tests for the MLflow provenance bridge
(shared/mlflow_provenance.py) and the CLI field-collection logic
(scripts/log_lakehouse_experiment_provenance.py).

No MLflow server, no MLflow install, no Docker required: mlflow is imported
only inside log_provenance_run, never at module import, so these run in the
existing `unit-tests` job. They pin the exact params/tags/artifact contract
the plan (Sec 3.5/3.6) promises.
"""
import json

import pytest

from shared.mlflow_provenance import (
    OPTIONAL_FIELDS,
    REQUIRED_FIELDS,
    build_provenance_payload,
    provenance_fields_from_iceberg_info,
    provenance_fields_from_manifest,
)


def _full_fields():
    return {
        "snapshot_id": "adaptive-refresh-2026-07-07-174500",
        "iceberg_catalog": "cartracker",
        "iceberg_table": "cartracker_experiments.volatility_features_snapshot",
        "iceberg_snapshot_id": 2630085324796564860,
        "feature_table_name": "int_listing_volatility_features",
        "row_count": 250790,
        "distinct_vin17": 250790,
        "max_latest_fetched_at": "2026-07-14 23:00:00",
        "export_fingerprint": "abc123",
        "archive_sha256": "deadbeef",
        "archive_key": "snapshot_archives/fingerprints/abc123/snapshot.tar.zst",
        "archive_manifest_key": "snapshot_archives/fingerprints/abc123/archive_manifest.json",
    }


class TestRequiredFields:
    def test_builds_with_only_required_fields(self):
        fields = {
            "snapshot_id": "snap-1",
            "iceberg_catalog": "cartracker",
            "iceberg_table": "cartracker_experiments.t",
            "feature_table_name": "int_listing_volatility_features",
            "row_count": 100,
        }
        payload = build_provenance_payload(fields)
        assert payload.params["snapshot_id"] == "snap-1"
        assert payload.params["row_count"] == "100"
        # No optional field leaked in.
        for name in OPTIONAL_FIELDS:
            assert name not in payload.params

    @pytest.mark.parametrize("missing", REQUIRED_FIELDS)
    def test_raises_naming_each_missing_required_field(self, missing):
        fields = _full_fields()
        del fields[missing]
        with pytest.raises(ValueError, match=missing):
            build_provenance_payload(fields)

    def test_none_required_field_is_treated_as_missing(self):
        fields = _full_fields()
        fields["snapshot_id"] = None
        with pytest.raises(ValueError, match="snapshot_id"):
            build_provenance_payload(fields)

    def test_rejects_unknown_field(self):
        fields = _full_fields()
        fields["totally_bogus"] = "x"
        with pytest.raises(ValueError, match="totally_bogus"):
            build_provenance_payload(fields)


class TestTypeNormalization:
    def test_all_param_and_tag_values_are_strings(self):
        payload = build_provenance_payload(_full_fields())
        for value in payload.params.values():
            assert isinstance(value, str)
        for value in payload.tags.values():
            assert isinstance(value, str)

    def test_int_fields_stringified(self):
        payload = build_provenance_payload(_full_fields())
        assert payload.params["row_count"] == "250790"
        assert payload.params["iceberg_snapshot_id"] == "2630085324796564860"

    def test_optional_none_values_dropped(self):
        fields = _full_fields()
        fields["distinct_vin17"] = None
        fields["max_latest_fetched_at"] = None
        payload = build_provenance_payload(fields)
        assert "distinct_vin17" not in payload.params
        assert "max_latest_fetched_at" not in payload.params
        # A present optional still flows through.
        assert payload.params["export_fingerprint"] == "abc123"


class TestExactContract:
    """Pin the exact params/tags dicts the plan Sec 3.5 promises MLflow."""

    def test_exact_params(self):
        payload = build_provenance_payload(_full_fields())
        assert payload.params == {
            "snapshot_id": "adaptive-refresh-2026-07-07-174500",
            "iceberg_catalog": "cartracker",
            "iceberg_table": "cartracker_experiments.volatility_features_snapshot",
            "feature_table_name": "int_listing_volatility_features",
            "row_count": "250790",
            "export_fingerprint": "abc123",
            "archive_sha256": "deadbeef",
            "archive_key": "snapshot_archives/fingerprints/abc123/snapshot.tar.zst",
            "archive_manifest_key":
                "snapshot_archives/fingerprints/abc123/archive_manifest.json",
            "iceberg_snapshot_id": "2630085324796564860",
            "distinct_vin17": "250790",
            "max_latest_fetched_at": "2026-07-14 23:00:00",
        }

    def test_exact_tags(self):
        payload = build_provenance_payload(_full_fields(), env="vm", code_sha="a1b2c3d")
        assert payload.tags == {
            "plan": "112",
            "gate": "B",
            "kind": "lakehouse_provenance",
            "entity_grain": "vin17",
            "env": "vm",
            "snapshot_id": "adaptive-refresh-2026-07-07-174500",
            "iceberg.table": "cartracker_experiments.volatility_features_snapshot",
            "code_sha": "a1b2c3d",
        }

    def test_code_sha_tag_omitted_when_absent(self):
        payload = build_provenance_payload(_full_fields())
        assert "code_sha" not in payload.tags


class TestManifestArtifact:
    def test_no_manifest_means_no_artifact(self):
        payload = build_provenance_payload(_full_fields())
        assert payload.manifest_artifact_path is None

    def test_existing_manifest_is_attached(self, tmp_path):
        manifest = tmp_path / "archive_manifest.json"
        manifest.write_text("{}")
        payload = build_provenance_payload(
            _full_fields(), manifest_artifact_path=str(manifest)
        )
        assert payload.manifest_artifact_path == str(manifest)

    def test_missing_manifest_path_raises(self):
        with pytest.raises(ValueError, match="manifest artifact not found"):
            build_provenance_payload(
                _full_fields(), manifest_artifact_path="/no/such/manifest.json"
            )


class TestProvenanceFieldsFromManifest:
    def test_extracts_rich_archive_shape(self):
        manifest = {
            "snapshot_id": "snap-9",
            "export_fingerprint": "fp-9",
            "archive": {
                "path": "snapshot_archives/fingerprints/fp-9/snapshot.tar.zst",
                "sha256": "cafebabe",
                "bytes": 123,
            },
        }
        fields = provenance_fields_from_manifest(
            manifest, manifest_key="snapshot_archives/fingerprints/fp-9/archive_manifest.json"
        )
        assert fields == {
            "snapshot_id": "snap-9",
            "export_fingerprint": "fp-9",
            "archive_sha256": "cafebabe",
            "archive_key": "snapshot_archives/fingerprints/fp-9/snapshot.tar.zst",
            "archive_manifest_key":
                "snapshot_archives/fingerprints/fp-9/archive_manifest.json",
        }

    def test_tolerates_flat_archive_shape(self):
        manifest = {"snapshot_id": "snap-x", "archive_sha256": "aa", "archive_path": "p.tar.zst"}
        fields = provenance_fields_from_manifest(manifest)
        assert fields["archive_sha256"] == "aa"
        assert fields["archive_key"] == "p.tar.zst"
        assert "archive_manifest_key" not in fields  # not provided

    def test_partial_manifest_never_raises(self):
        # No archive checksum at all -- get_archive_meta would raise; we swallow it.
        fields = provenance_fields_from_manifest({"snapshot_id": "only-id"})
        assert fields == {"snapshot_id": "only-id"}


class TestProvenanceFieldsFromIcebergInfo:
    def test_maps_info_dict_onto_provenance_names(self):
        info = {
            "catalog": "cartracker",
            "table": "cartracker_experiments.volatility_features_snapshot",
            "current_snapshot_id": 42,
            "snapshots": [1, 42],
            "row_count": 500,
            "distinct_vin17": 500,
            "max_latest_fetched_at": "2026-07-14 23:00:00",
            "location": "s3://bronze/lakehouse_spike/warehouse/x",
        }
        fields = provenance_fields_from_iceberg_info(info)
        assert fields == {
            "iceberg_catalog": "cartracker",
            "iceberg_table": "cartracker_experiments.volatility_features_snapshot",
            "iceberg_snapshot_id": 42,
            "row_count": 500,
            "distinct_vin17": 500,
            "max_latest_fetched_at": "2026-07-14 23:00:00",
        }

    def test_drops_absent_optional_keys(self):
        info = {"catalog": "cartracker", "table": "cartracker_experiments.t", "row_count": 1}
        fields = provenance_fields_from_iceberg_info(info)
        assert "max_latest_fetched_at" not in fields
        assert "distinct_vin17" not in fields
        assert fields["iceberg_catalog"] == "cartracker"


class TestCliFieldCollection:
    """The layered-input precedence in the CLI: metadata-json < iceberg-info
    < manifest < explicit flags."""

    @staticmethod
    def _run_collect(argv):
        from scripts.log_lakehouse_experiment_provenance import _collect_fields, _parse_args

        return _collect_fields(_parse_args(argv))

    def test_flags_override_manifest_and_metadata(self, tmp_path):
        metadata = tmp_path / "meta.json"
        metadata.write_text(json.dumps({
            "snapshot_id": "from-metadata",
            "iceberg_catalog": "cartracker",
            "iceberg_table": "cartracker_experiments.t",
            "feature_table_name": "int_listing_volatility_features",
            "row_count": 1,
        }))
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({
            "snapshot_id": "from-manifest",
            "archive": {"path": "a.tar.zst", "sha256": "sh", "bytes": 1},
        }))

        fields = self._run_collect([
            "--metadata-json", str(metadata),
            "--manifest", str(manifest),
            "--snapshot-id", "from-flag",
        ])

        # manifest overrides metadata, flag overrides manifest.
        assert fields["snapshot_id"] == "from-flag"
        assert fields["archive_key"] == "a.tar.zst"
        assert fields["iceberg_table"] == "cartracker_experiments.t"

    def test_int_flags_are_coerced(self, tmp_path):
        fields = self._run_collect([
            "--snapshot-id", "s", "--iceberg-catalog", "cartracker",
            "--iceberg-table", "cartracker_experiments.t",
            "--feature-table-name", "int_listing_volatility_features",
            "--row-count", "250790",
        ])
        assert fields["row_count"] == 250790  # int, not "250790"

    def test_collected_fields_build_a_valid_payload(self, tmp_path):
        fields = self._run_collect([
            "--snapshot-id", "s", "--iceberg-catalog", "cartracker",
            "--iceberg-table", "cartracker_experiments.t",
            "--feature-table-name", "int_listing_volatility_features",
            "--row-count", "5",
        ])
        payload = build_provenance_payload(fields)
        assert payload.params["row_count"] == "5"


class TestCliCleanErrors:
    """A user-facing smoke CLI must return a clean nonzero on bad input, never
    a raw traceback (missing/invalid input files, missing required fields)."""

    @staticmethod
    def _main(argv):
        from scripts.log_lakehouse_experiment_provenance import main

        return main(argv)

    def test_missing_manifest_returns_clean_error(self, capsys):
        rc = self._main([
            "--manifest", "/no/such/manifest.json",
            "--feature-table-name", "int_listing_volatility_features",
            "--dry-run",
        ])
        assert rc == 2
        err = capsys.readouterr().err
        assert "--manifest not found" in err
        assert "Traceback" not in err

    def test_missing_metadata_json_returns_clean_error(self, capsys):
        rc = self._main(["--metadata-json", "/no/such/meta.json", "--dry-run"])
        assert rc == 2
        assert "--metadata-json not found" in capsys.readouterr().err

    def test_invalid_json_returns_clean_error(self, tmp_path, capsys):
        bad = tmp_path / "bad.json"
        bad.write_text("{not valid json")
        rc = self._main(["--iceberg-info-json", str(bad), "--dry-run"])
        assert rc == 2
        assert "not valid JSON" in capsys.readouterr().err

    def test_missing_required_field_returns_clean_error(self, capsys):
        # Only a snapshot id -- missing iceberg_catalog/table/feature/row_count.
        rc = self._main(["--snapshot-id", "s", "--dry-run"])
        assert rc == 2
        err = capsys.readouterr().err
        assert "missing required provenance field" in err
        assert "Traceback" not in err

    def test_valid_dry_run_returns_zero(self, capsys):
        rc = self._main([
            "--snapshot-id", "s", "--iceberg-catalog", "cartracker",
            "--iceberg-table", "cartracker_experiments.t",
            "--feature-table-name", "int_listing_volatility_features",
            "--row-count", "5", "--dry-run",
        ])
        assert rc == 0
        assert '"row_count": "5"' in capsys.readouterr().out
