"""Plan 138 Stage 3c: the fingerprint that makes a one-year cache safe.

``/static_ops/*`` is served with ``max-age=31536000, immutable`` when the URL
carries a ``?v=``. That is a promise the bytes behind the URL never change, and
:func:`ops.static_assets.asset_url` is the only thing keeping it. If it stopped
varying with content, a deploy would reach nobody who had already visited -- for
a year, with no error anywhere.
"""
import pytest

from ops.static_assets import STATIC_DIR, asset_url


class TestAssetUrl:
    def test_it_names_the_file_and_a_hash_of_its_bytes(self):
        url = asset_url("info.css")
        path, _, query = url.partition("?")
        assert path == "/static_ops/info.css"
        assert query.startswith("v=")
        assert len(query) == len("v=") + 12

    def test_two_different_files_get_two_different_hashes(self):
        assert asset_url("info.css") != asset_url("info.js")

    def test_the_hash_follows_the_content(self, tmp_path, mocker):
        """The whole point, and the one property a stale cache would violate."""
        mocker.patch("ops.static_assets.STATIC_DIR", tmp_path)
        asset_url.cache_clear()
        asset = tmp_path / "probe.css"

        asset.write_text("a{}", encoding="utf-8")
        before = asset_url("probe.css")
        asset.write_text("b{}", encoding="utf-8")
        asset_url.cache_clear()
        after = asset_url("probe.css")

        assert before != after
        asset_url.cache_clear()

    def test_it_refuses_the_side_of_the_mount_seam_that_git_pull_publishes(self):
        """Stage 7 bind-mounts ``generated/`` read-only from the checkout, so a
        recap or a roadmap update publishes without an image build. A hash for
        one of those would be computed once at startup and then cached past
        every republish, which is the failure that would look exactly like the
        generator having stopped running."""
        with pytest.raises(ValueError, match="git pull"):
            asset_url("generated/project-updates.json")

    def test_a_missing_asset_is_not_papered_over(self):
        with pytest.raises(FileNotFoundError):
            asset_url("nothing-here.css")

    def test_every_vendored_icon_can_be_addressed(self):
        icons = sorted((STATIC_DIR / "vendor" / "icons").glob("*.svg"))
        assert len(icons) == 8, f"expected 8 vendored icons, found {len(icons)}"
        for icon in icons:
            assert asset_url(f"vendor/icons/{icon.name}").startswith(
                f"/static_ops/vendor/icons/{icon.name}?v="
            )
