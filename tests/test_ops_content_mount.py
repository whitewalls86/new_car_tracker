"""Plan 138 Stage 7: generated public content is served from the checkout.

Publishing a recap or a roadmap update used to mean rebuilding the ops image and
recreating the container -- a coordination drain with every in-scope Airflow DAG
parked, to publish prose. Stage 7 bind-mounts the generated artifacts read-only
from the checkout instead, so ``git pull`` publishes them.

That only holds while the mount does, and the ways it can stop holding are all
silent: remove it and the site quietly freezes at the last image build; narrow it
to a single file and ``git pull`` leaves the container reading a deleted inode
while reporting success; add a generator that writes outside it and only that
one artifact freezes. None of those raise anything at runtime, so they are
asserted here.
"""
from __future__ import annotations

import ast
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).parent.parent

# The host side of the mount, repository-relative. Every generated public
# artifact must live under this directory.
CONTENT_DIR = "ops/static_ops/generated"
MOUNT = f"./{CONTENT_DIR}:/app/{CONTENT_DIR}:ro"


def _compose():
    return yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


def _ops_volumes():
    return _compose()["services"]["ops"]["volumes"]


def _generator_outputs():
    """Every ``OUTPUT``/``OUTPUT_DIR`` a public generator declares.

    Read out of the source rather than imported, so this stays a static property
    of the tree and a new generator's dependencies cannot make it unreadable.
    """
    found = {}
    for script in sorted((_REPO_ROOT / "scripts").glob("build_public_*.py")):
        tree = ast.parse(script.read_text(encoding="utf-8"))
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            for target in node.targets:
                if (
                    isinstance(target, ast.Name)
                    and target.id in {"OUTPUT", "OUTPUT_DIR"}
                    and isinstance(node.value, ast.Constant)
                    and isinstance(node.value.value, str)
                ):
                    found[f"{script.name}:{target.id}"] = node.value.value
    return found


class TestGeneratedContentMount:
    def test_ops_bind_mounts_the_generated_content_from_the_checkout(self):
        assert MOUNT in _ops_volumes()

    def test_the_mount_is_read_only(self):
        """`ops` serves this content; the generators write it in the checkout."""
        spec = next(v for v in _ops_volumes() if v.startswith(f"./{CONTENT_DIR}:"))
        assert spec.endswith(":ro"), f"{spec} is writable from inside the container"

    def test_the_mount_is_a_directory_and_never_a_single_file(self):
        """redeploy.sh decision 4: a single-file bind mount pins the inode, and
        `git pull` replaces the file rather than editing it, so the container
        goes on serving a deleted copy while reporting success. A directory
        resolves names on every access and cannot fail that way."""
        source = _REPO_ROOT / CONTENT_DIR
        assert source.is_dir(), f"{CONTENT_DIR} must exist as a directory"
        for spec in _ops_volumes():
            host = spec.split(":")[0]
            if not host.startswith("./ops/static_ops"):
                continue
            assert (_REPO_ROOT / host).is_dir(), (
                f"{spec} mounts a single file; mount its parent directory instead"
            )

    def test_every_public_generator_writes_inside_the_mount(self):
        """A generator writing outside it lands on the image-only side, and its
        artifact then freezes at the last build with nothing to show for it."""
        outputs = _generator_outputs()
        assert outputs, "no public generator output paths found to check"
        for where, path in outputs.items():
            assert path.startswith(f"{CONTENT_DIR}/"), (
                f"{where} writes {path}, outside the mounted {CONTENT_DIR}/"
            )

    def test_both_known_generators_are_covered(self):
        """Guards the discovery above: if a rename made the glob match nothing,
        the check before this one would pass on an empty set."""
        assert set(_generator_outputs()) == {
            "build_public_recaps.py:OUTPUT_DIR",
            "build_public_roadmap.py:OUTPUT",
        }

    def test_the_image_still_carries_the_content_so_it_stays_self_contained(self):
        """The mount overlays the image, it does not replace it. A host with no
        checkout still serves, and rollback is removing the mount."""
        dockerfile = (_REPO_ROOT / "ops/Dockerfile").read_text(encoding="utf-8")
        assert "COPY . ." in dockerfile

    def test_authored_assets_stay_on_the_image_side(self):
        """Stage 3c extracts CSS and JavaScript into `ops/static_ops/`. The seam
        is generated data on the mounted side and authored assets on the image
        side -- a wider mount would make that code `git pull`-deployable."""
        mounted_hosts = {
            v.split(":")[0] for v in _ops_volumes() if v.startswith("./ops/static_ops")
        }
        assert mounted_hosts == {f"./{CONTENT_DIR}"}
