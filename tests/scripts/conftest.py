"""Shared helpers for the script suites.

Plan 162 Stage T: ``make_tar_zst`` was defined three times across the
lake-snapshot test modules — one superset (this one) and two subsets.
"""
import io
import tarfile

import zstandard as zstd


def make_tar_zst(archive_path, files=None, raw_members=None):
    """Build a .tar.zst archive. raw_members lets tests add unsafe entries
    (path traversal, symlinks) that Path-based construction couldn't express."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, content in (files or {}).items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
        for info, content in raw_members or []:
            tar.addfile(info, io.BytesIO(content) if content is not None else None)
    compressed = zstd.ZstdCompressor(level=3).compress(buf.getvalue())
    archive_path.write_bytes(compressed)
    return archive_path
