"""The CI snapshot manifest's format version: what is written, what is served.

Plan 162 Stage AA, gap G31.

**One definition, two roles, one file.** ``archiver`` writes the manifest and
``ops`` serves it back over HTTP, and the version has to mean the same thing to
both. An earlier draft of this stage restated the integers in each service and
added a test that they matched, on the reasoning that importing across services
would trade a checkable disagreement for a shared deploy. That reasoning was
wrong twice over: ``shared/`` is *already* imported by both, so the deploy
coupling exists whether or not these two integers live here, and the repository
already has a procedure for it -- a change under ``shared/`` redeploys
``archiver``, ``pack-worker``, ``processing``, ``scraper``, ``dbt_runner`` and
``dashboard``, then ``ops`` last and alone. Two copies plus a test was a
mechanism invented to solve a problem the tree had already solved.

**Written and readable are still separate, and that is not duplication.**
``ops`` refuses a manifest whose version it does not recognise, which means a
single shared integer would make bumping the format impossible: the moment
``archiver`` wrote the new version, every read would 409, and there would be no
ordering of deploys that avoided it. Two names is what makes a bump *operable*
-- add the new version to the readable set, deploy ``ops``, then move the
written version and deploy ``archiver``. The reader leads, the writer follows,
and nothing 409s in between.

So the invariant that matters is not "these are equal" but "the version being
written is one the reader accepts", and
``tests/shared/test_lake_snapshot_schema.py`` asserts exactly that.

**Why not a table.** Storing the versions -- and the field list for each -- in
Postgres was considered and rejected, and the decisive reason is specific to
this repository rather than general. ``contracts/ops.json`` is generated from
the running app at build time by ``scripts/generate_service_contracts.py``, so
the response shape has to be a Python type that exists at import. A table could
not replace ``ops/api_models.py``'s ``ArchiveManifest``; it could only sit
beside it, which adds a drift axis instead of removing one -- and the new axis
is the worse kind, because a row can disagree with the deployed code without
anything failing at build time. Two further reasons stand behind that one: a
version is a fact about which image is running, not about data, so a table can
claim a version nothing writes; and a rule that lives in a table cannot turn CI
red for a code change that forgot its counterpart, which is the entire job here.
"""
from __future__ import annotations

# What `archiver` stamps into every manifest it writes.
ARCHIVE_CACHE_SCHEMA_VERSION = 1
EXPORT_CACHE_SCHEMA_VERSION = 3
