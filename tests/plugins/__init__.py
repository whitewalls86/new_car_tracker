"""Pytest plugins this repository loads into every run.

Registered through ``[tool.pytest.ini_options] addopts`` rather than a
conftest, because ``docs-tests`` runs its two steps with ``--noconftest`` -- it
installs three packages and cannot import ``tests/conftest.py`` at all. A
conftest hook therefore *cannot* cover every job, and one of the two skips this
repository actually produces lives in that job.

``-p`` loads through ``--noconftest``, and ``pythonpath = ["."]`` is what makes
the module importable before collection starts. Anything here is imported in
the minimal job too, so it may import the standard library and pytest, and
nothing else.
"""
